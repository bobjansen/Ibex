// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_format.hpp>
#include <ibex/ui/server.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <optional>
#include <random>
#include <ratio>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__linux__)
#include <fcntl.h>
#include <linux/landlock.h>
#include <linux/prctl.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#endif

#ifdef _WIN32
#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace ibex::ui {
namespace {

using json = nlohmann::json;

#ifdef _WIN32
using Socket = SOCKET;
constexpr Socket kInvalidSocket = INVALID_SOCKET;
auto close_socket(Socket socket) -> void {
    closesocket(socket);
}
#else
using Socket = int;
constexpr Socket kInvalidSocket = -1;
auto close_socket(Socket socket) -> void {
    close(socket);
}
#endif

struct HttpRequest {
    std::string method;
    std::string target;
    std::map<std::string, std::string, std::less<>> headers;
    std::string body;
};

struct Session {
    explicit Session(const repl::ReplConfig& config, runtime::ExternRegistry& registry)
        : repl(config, registry) {}

    repl::ReplSession repl;
    std::map<std::string, runtime::Table, std::less<>> results;
    std::uint64_t next_result_id = 1;
};

// Seeded into each new browser session when the server runs with `--demo`.
// Draws through the RNG bridge so `seed_rng` makes the tables reproducible.
// `trades` columns are timestamp/symbol/price/volume; `reference` has one row
// per symbol (symbol/name/sector/currency/lot_size/tick_size) and joins to
// `trades` on `symbol`; `prices` and `samples` each expose a `value` column.
constexpr std::string_view kDemoBootstrap = R"(import data_gen;
seed_rng(20240115);
let trades = gen_ticks(50000, "AAPL,MSFT,GOOG,AMZN,NVDA");
let reference = gen_reference("AAPL,MSFT,GOOG,AMZN,NVDA");
let prices = gen_walk(2000, 100.0, 1.0);
let samples = gen_normal(10000, 0.0, 1.0);
)";

struct StaticAsset {
    std::filesystem::path path;
    std::string contents;
};

using StaticAssets = std::map<std::string, StaticAsset, std::less<>>;

auto lower(std::string text) -> std::string {
    std::ranges::transform(text, text.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return text;
}

auto trim(std::string_view value) -> std::string_view {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
        value.remove_suffix(1);
    }
    return value;
}

auto read_request(Socket socket) -> std::optional<HttpRequest> {
    std::string raw;
    std::array<char, 8192> buffer{};
    while (raw.find("\r\n\r\n") == std::string::npos) {
        const auto count = recv(socket, buffer.data(), static_cast<int>(buffer.size()), 0);
        if (count <= 0) {
            return std::nullopt;
        }
        raw.append(buffer.data(), static_cast<std::size_t>(count));
        if (raw.size() > static_cast<std::size_t>(1024) * 1024) {
            return std::nullopt;
        }
    }
    const auto header_end = raw.find("\r\n\r\n");
    std::istringstream stream(raw.substr(0, header_end));
    HttpRequest request;
    std::string version;
    if (!(stream >> request.method >> request.target >> version)) {
        return std::nullopt;
    }
    std::string line;
    std::getline(stream, line);
    while (std::getline(stream, line) && line != "\r") {
        const auto colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        auto value = trim(std::string_view(line).substr(colon + 1));
        request.headers.insert_or_assign(lower(line.substr(0, colon)), std::string(value));
    }
    std::size_t content_length = 0;
    if (const auto it = request.headers.find("content-length"); it != request.headers.end()) {
        try {
            content_length = static_cast<std::size_t>(std::stoull(it->second));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    }
    if (content_length > static_cast<std::size_t>(8) * 1024 * 1024) {
        return std::nullopt;
    }
    request.body = raw.substr(header_end + 4);
    while (request.body.size() < content_length) {
        const auto count = recv(socket, buffer.data(), static_cast<int>(buffer.size()), 0);
        if (count <= 0) {
            return std::nullopt;
        }
        request.body.append(buffer.data(), static_cast<std::size_t>(count));
    }
    request.body.resize(content_length);
    return request;
}

auto mime_type(const std::filesystem::path& path) -> std::string_view {
    const auto extension = path.extension().string();
    if (extension == ".js")
        return "text/javascript; charset=utf-8";
    if (extension == ".css")
        return "text/css; charset=utf-8";
    if (extension == ".svg")
        return "image/svg+xml";
    if (extension == ".ico")
        return "image/x-icon";
    return "text/html; charset=utf-8";
}

auto send_response(Socket socket, int status, std::string_view status_text, const std::string& body,
                   std::string_view content_type = "application/json; charset=utf-8",
                   std::string_view cookie = {}) -> void {
    std::ostringstream response;
    response << "HTTP/1.1 " << status << ' ' << status_text << "\r\n"
             << "Content-Type: " << content_type << "\r\n"
             << "Content-Length: " << body.size() << "\r\n"
             << "Cache-Control: no-store\r\n"
             << "Connection: close\r\n";
    if (!cookie.empty()) {
        response << "Set-Cookie: " << cookie << "; Path=/; SameSite=Strict\r\n";
    }
    response << "\r\n" << body;
    const std::string text = response.str();
    std::size_t sent = 0;
    while (sent < text.size()) {
#ifdef _WIN32
        const auto remaining = static_cast<int>(text.size() - sent);
#else
        const auto remaining = text.size() - sent;
#endif
        const auto count = send(socket, text.data() + sent, remaining, 0);
        if (count <= 0)
            return;
        sent += static_cast<std::size_t>(count);
    }
}

auto session_id(const HttpRequest& request) -> std::optional<std::string> {
    const auto it = request.headers.find("cookie");
    if (it == request.headers.end())
        return std::nullopt;
    constexpr std::string_view prefix = "ibex_session=";
    const auto start = it->second.find(prefix);
    if (start == std::string::npos)
        return std::nullopt;
    const auto value_start = start + prefix.size();
    const auto end = it->second.find(';', value_start);
    return it->second.substr(value_start, end - value_start);
}

auto new_session_id() -> std::string {
    std::random_device device;
    std::mt19937_64 engine(device());
    std::ostringstream out;
    out << std::hex << engine() << engine();
    return out.str();
}

auto print_performance_environment() -> void {
    constexpr std::array<std::string_view, 13> variables = {"IBEX_CORES",
                                                            "IBEX_DECODE_THREADS",
                                                            "IBEX_DECODE_SATURATION",
                                                            "IBEX_PARALLEL",
                                                            "IBEX_CHUNK_ROWS",
                                                            "IBEX_MORSEL_ROWS",
                                                            "IBEX_STREAM_SCAN",
                                                            "IBEX_JOIN_PROBE",
                                                            "IBEX_NO_MALLOC_TUNING",
                                                            "IBEX_PROFILE_OPERATORS",
                                                            "IBEX_PARALLEL_STATS",
                                                            "IBEX_UNIQUE_KEY_STATS",
                                                            "IBEX_THREADS (deprecated)"};
    std::cout << "Ibex UI performance environment:\n";
    for (const auto variable : variables) {
        const auto name = variable.substr(0, variable.find(' '));
        const char* value = std::getenv(std::string(name).c_str());
        std::cout << "  " << variable << '='
                  << (value != nullptr && value[0] != '\0' ? value : "<unset>") << '\n';
    }
}

auto column_type(const runtime::ColumnValue& value) -> std::string {
    if (std::holds_alternative<Column<std::int64_t>>(value))
        return "Int64";
    if (std::holds_alternative<Column<double>>(value))
        return "Float64";
    if (std::holds_alternative<Column<std::string>>(value))
        return "String";
    if (std::holds_alternative<Column<Categorical>>(value))
        return "Categorical";
    if (std::holds_alternative<Column<Date>>(value))
        return "Date";
    if (std::holds_alternative<Column<Timestamp>>(value))
        return "Timestamp";
    return "Bool";
}

auto cell_json(const runtime::ColumnEntry& entry, std::size_t row) -> json {
    if (runtime::is_null(entry, row))
        return nullptr;
    return std::visit(
        [row](const auto& column) -> json {
            using T = typename std::decay_t<decltype(column)>::value_type;
            if constexpr (std::same_as<T, Date>) {
                return runtime::format_date(column[row]);
            } else if constexpr (std::same_as<T, Timestamp>) {
                return runtime::format_timestamp(column[row]);
            } else if constexpr (std::same_as<T, Categorical>) {
                return std::string(column[row]);
            } else {
                return column[row];
            }
        },
        *entry.column);
}

auto table_page(const runtime::Table& table, std::size_t offset, std::size_t limit) -> json {
    const std::size_t start = std::min(offset, table.rows());
    const std::size_t end = std::min(table.rows(), start + limit);
    json columns = json::array();
    for (const auto& entry : table.columns) {
        columns.push_back({{"name", entry.name}, {"type", column_type(*entry.column)}});
    }
    json rows = json::array();
    for (std::size_t row = start; row < end; ++row) {
        json values = json::array();
        for (const auto& entry : table.columns)
            values.push_back(cell_json(entry, row));
        rows.push_back(std::move(values));
    }
    return {{"kind", "table"},         {"columns", std::move(columns)},
            {"rows", std::move(rows)}, {"offset", start},
            {"limit", limit},          {"total_rows", table.rows()}};
}

auto bounded_limit(const json& body) -> std::size_t {
    const auto requested = body.value("limit", 200U);
    return std::clamp<std::size_t>(requested, 1, 1000);
}

auto query_size(std::string_view target, std::string_view key, std::size_t fallback)
    -> std::size_t {
    const auto question = target.find('?');
    if (question == std::string_view::npos)
        return fallback;
    std::string_view query = target.substr(question + 1);
    while (!query.empty()) {
        const auto ampersand = query.find('&');
        const auto item = query.substr(0, ampersand);
        const auto equals = item.find('=');
        if (equals != std::string_view::npos && item.substr(0, equals) == key) {
            try {
                return static_cast<std::size_t>(std::stoull(std::string(item.substr(equals + 1))));
            } catch (const std::exception&) {
                return fallback;
            }
        }
        if (ampersand == std::string_view::npos)
            break;
        query.remove_prefix(ampersand + 1);
    }
    return fallback;
}

auto url_decode(std::string_view encoded) -> std::optional<std::string> {
    std::string decoded;
    decoded.reserve(encoded.size());
    const auto hex_value = [](char character) -> std::optional<unsigned char> {
        if (character >= '0' && character <= '9')
            return static_cast<unsigned char>(character - '0');
        if (character >= 'a' && character <= 'f')
            return static_cast<unsigned char>(character - 'a' + 10);
        if (character >= 'A' && character <= 'F')
            return static_cast<unsigned char>(character - 'A' + 10);
        return std::nullopt;
    };
    for (std::size_t index = 0; index < encoded.size(); ++index) {
        if (encoded[index] != '%') {
            decoded.push_back(encoded[index]);
            continue;
        }
        if (index + 2 >= encoded.size())
            return std::nullopt;
        const auto high = hex_value(encoded[index + 1]);
        const auto low = hex_value(encoded[index + 2]);
        if (!high || !low)
            return std::nullopt;
        decoded.push_back(static_cast<char>((*high << 4U) | *low));
        index += 2;
    }
    return decoded;
}

auto query_value(std::string_view target, std::string_view key) -> std::optional<std::string> {
    const auto question = target.find('?');
    if (question == std::string_view::npos)
        return std::nullopt;
    std::string_view query = target.substr(question + 1);
    while (!query.empty()) {
        const auto ampersand = query.find('&');
        const auto item = query.substr(0, ampersand);
        const auto equals = item.find('=');
        if (equals != std::string_view::npos && item.substr(0, equals) == key) {
            return url_decode(item.substr(equals + 1));
        }
        if (ampersand == std::string_view::npos)
            break;
        query.remove_prefix(ampersand + 1);
    }
    return std::nullopt;
}

auto scalar_json(const runtime::ScalarValue& value) -> json {
    return std::visit(
        [](const auto& scalar) -> json {
            using T = std::decay_t<decltype(scalar)>;
            if constexpr (std::same_as<T, Date>) {
                return runtime::format_date(scalar);
            } else if constexpr (std::same_as<T, Timestamp>) {
                return runtime::format_timestamp(scalar);
            } else {
                return scalar;
            }
        },
        value);
}

auto environment_json(const Session& session) -> json {
    json tables = json::array();
    for (const auto& table : session.repl.environment()) {
        json columns = json::array();
        for (const auto& [name, type] : table.columns) {
            columns.push_back({{"name", name}, {"type", type}});
        }
        tables.push_back({{"name", table.name},
                          {"rows", table.rows},
                          {"lazy", table.lazy},
                          {"columns", std::move(columns)}});
    }
    return {{"tables", std::move(tables)}};
}

auto is_beneath(const std::filesystem::path& path, const std::filesystem::path& root) -> bool {
    auto path_it = path.begin();
    for (auto root_it = root.begin(); root_it != root.end(); ++root_it, ++path_it) {
        if (path_it == path.end() || *path_it != *root_it)
            return false;
    }
    return true;
}

auto data_directory_json(const std::filesystem::path& root, std::string_view requested_path)
    -> std::optional<json> {
    std::filesystem::path relative(requested_path.empty() ? "." : requested_path);
    relative = relative.lexically_normal();
    if (relative.is_absolute() || std::ranges::find(relative, "..") != relative.end()) {
        return std::nullopt;
    }
    std::error_code ec;
    const auto directory = std::filesystem::weakly_canonical(root / relative, ec);
    if (ec || !is_beneath(directory, root) || !std::filesystem::is_directory(directory, ec)) {
        return std::nullopt;
    }
    std::vector<json> entries;
    for (std::filesystem::directory_iterator it(directory, ec), end; !ec && it != end;
         it.increment(ec)) {
        const auto canonical = std::filesystem::weakly_canonical(it->path(), ec);
        if (ec || !is_beneath(canonical, root))
            continue;
        const bool is_directory = it->is_directory(ec);
        if (ec)
            continue;
        const auto entry_relative = std::filesystem::relative(canonical, root, ec);
        if (ec)
            continue;
        entries.push_back({{"name", canonical.filename().string()},
                           {"path", canonical.string()},
                           {"relative_path", entry_relative.generic_string()},
                           {"directory", is_directory}});
    }
    if (ec)
        return std::nullopt;
    std::ranges::sort(entries, [](const json& lhs, const json& rhs) {
        if (lhs["directory"] != rhs["directory"])
            return lhs["directory"] > rhs["directory"];
        return lhs["name"] < rhs["name"];
    });
    const auto directory_relative = std::filesystem::relative(directory, root, ec);
    if (ec)
        return std::nullopt;
    const auto directory_path =
        directory_relative == "." ? "" : directory_relative.generic_string();
    return json{{"path", directory_path}, {"entries", std::move(entries)}};
}

auto load_static_assets(const std::filesystem::path& root) -> std::optional<StaticAssets> {
    StaticAssets assets;
    std::error_code ec;
    for (std::filesystem::recursive_directory_iterator it(root, ec), end; !ec && it != end;
         it.increment(ec)) {
        if (!it->is_regular_file(ec))
            continue;
        const auto relative = std::filesystem::relative(it->path(), root, ec);
        if (ec)
            return std::nullopt;
        std::ifstream input(it->path(), std::ios::binary);
        if (!input)
            return std::nullopt;
        assets.emplace(
            '/' + relative.generic_string(),
            StaticAsset{.path = it->path(),
                        .contents = std::string(std::istreambuf_iterator<char>(input), {})});
    }
    if (ec || !assets.contains("/index.html"))
        return std::nullopt;
    return assets;
}

auto static_file(const StaticAssets& assets, std::string_view target) -> const StaticAsset* {
    const auto query = target.find('?');
    const std::string relative(target.substr(0, query));
    std::filesystem::path requested = relative == "/" ? "index.html" : relative.substr(1);
    requested = requested.lexically_normal();
    if (requested.empty() || requested.is_absolute() || requested.string().contains("..")) {
        return nullptr;
    }
    const auto found = assets.find('/' + requested.generic_string());
    if (found != assets.end())
        return &found->second;
    return &assets.at("/index.html");  // SPA navigation fallback.
}

#if defined(__linux__)
constexpr std::uint64_t kLandlockFilesystemAccess =
    LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_WRITE_FILE | LANDLOCK_ACCESS_FS_READ_FILE |
    LANDLOCK_ACCESS_FS_READ_DIR | LANDLOCK_ACCESS_FS_REMOVE_DIR | LANDLOCK_ACCESS_FS_REMOVE_FILE |
    LANDLOCK_ACCESS_FS_MAKE_CHAR | LANDLOCK_ACCESS_FS_MAKE_DIR | LANDLOCK_ACCESS_FS_MAKE_REG |
    LANDLOCK_ACCESS_FS_MAKE_SOCK | LANDLOCK_ACCESS_FS_MAKE_FIFO | LANDLOCK_ACCESS_FS_MAKE_BLOCK |
    LANDLOCK_ACCESS_FS_MAKE_SYM | LANDLOCK_ACCESS_FS_REFER | LANDLOCK_ACCESS_FS_TRUNCATE;

constexpr std::uint64_t kLandlockReadExecuteAccess =
    LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR;

auto add_landlock_path_rule(int ruleset_fd, const std::filesystem::path& directory,
                            std::uint64_t allowed_access) -> std::optional<std::string> {
    // POSIX open(2).
    const int directory_fd = open(directory.c_str(), O_PATH | O_CLOEXEC);
    if (directory_fd < 0) {
        return "could not open a Landlock allow-list directory";
    }
    landlock_path_beneath_attr rule{.allowed_access = allowed_access, .parent_fd = directory_fd};
    const int add_result = static_cast<int>(
        // Linux syscall(2).
        syscall(SYS_landlock_add_rule, ruleset_fd, LANDLOCK_RULE_PATH_BENEATH, &rule, 0));
    close(directory_fd);
    if (add_result != 0) {
        return "could not add a directory to the Landlock ruleset";
    }
    return std::nullopt;
}

auto enable_landlock(const std::filesystem::path& data_directory,
                     const std::vector<std::filesystem::path>& read_only_directories)
    -> std::optional<std::string> {
    const int abi = static_cast<int>(
        // Linux syscall(2).
        syscall(SYS_landlock_create_ruleset, nullptr, 0, LANDLOCK_CREATE_RULESET_VERSION));
    if (abi < 3) {
        return "Landlock ABI 3 or newer is required to confine reads and writes";
    }
    landlock_ruleset_attr ruleset_attr{};
    ruleset_attr.handled_access_fs = kLandlockFilesystemAccess;
    const int ruleset_fd = static_cast<int>(
        // Linux syscall(2).
        syscall(SYS_landlock_create_ruleset, &ruleset_attr, sizeof(ruleset_attr), 0));
    if (ruleset_fd < 0)
        return "could not create the Landlock ruleset";
    if (const auto error =
            add_landlock_path_rule(ruleset_fd, data_directory, kLandlockFilesystemAccess)) {
        close(ruleset_fd);
        return error;
    }
    for (const auto& directory : read_only_directories) {
        if (const auto error =
                add_landlock_path_rule(ruleset_fd, directory, kLandlockReadExecuteAccess)) {
            close(ruleset_fd);
            return error;
        }
    }
    // Linux prctl(2) and syscall(2).
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 ||
        // Linux syscall(2).
        syscall(SYS_landlock_restrict_self, ruleset_fd, 0) != 0) {
        close(ruleset_fd);
        return "could not enforce the Landlock ruleset";
    }
    close(ruleset_fd);
    return std::nullopt;
}

auto readable_plugin_directories(const repl::ReplConfig& config)
    -> std::optional<std::vector<std::filesystem::path>> {
    std::vector<std::filesystem::path> directories;
    const auto add_directories = [&directories](const std::vector<std::string>& paths) -> bool {
        std::error_code ec;
        for (const auto& path : paths) {
            const auto canonical = std::filesystem::weakly_canonical(path, ec);
            if (ec || !std::filesystem::is_directory(canonical))
                return false;
            directories.push_back(canonical);
        }
        return true;
    };
    if (!add_directories(config.plugin_search_paths) ||
        !add_directories(config.import_search_paths)) {
        return std::nullopt;
    }
    std::ranges::sort(directories);
    directories.erase(std::ranges::unique(directories).begin(), directories.end());
    return directories;
}
#endif

}  // namespace

auto run_server(const ServerConfig& config, runtime::ExternRegistry& registry) -> int {
    if (!std::filesystem::is_regular_file(config.web_root / "index.html")) {
        std::cerr << "error: Ibex UI assets are missing from '" << config.web_root.string()
                  << "'\n";
        return 1;
    }
    const auto assets = load_static_assets(config.web_root);
    if (!assets) {
        std::cerr << "error: could not load Ibex UI assets from '" << config.web_root.string()
                  << "'\n";
        return 1;
    }

    std::error_code ec;
    const auto requested_data_directory =
        config.data_directory.empty() ? std::filesystem::current_path(ec) : config.data_directory;
    const auto data_directory = std::filesystem::weakly_canonical(requested_data_directory, ec);
    if (ec || !std::filesystem::is_directory(data_directory)) {
        std::cerr << "error: UI data directory is not an existing directory: '"
                  << requested_data_directory.string() << "'\n";
        return 1;
    }
#if defined(__linux__)
    const auto plugin_directories = readable_plugin_directories(config.repl);
    if (!plugin_directories) {
        std::cerr << "error: could not resolve a configured UI plugin directory\n";
        return 1;
    }
    if (const auto error = enable_landlock(data_directory, *plugin_directories)) {
        std::cerr << "error: " << *error << "\n";
        return 1;
    }
#endif
    if (!config.data_directory.empty()) {
        std::filesystem::current_path(data_directory, ec);
        if (ec) {
            std::cerr << "error: could not change to UI data directory: '"
                      << data_directory.string() << "'\n";
            return 1;
        }
    }
#ifdef _WIN32
    WSADATA data{};
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0)
        return 1;
#endif
    const Socket listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener == kInvalidSocket)
        return 1;
    int reuse = 1;
#ifdef _WIN32
    static_cast<void>(setsockopt(listener, SOL_SOCKET, SO_REUSEADDR,
                                 reinterpret_cast<const char*>(&reuse), sizeof(reuse)));
#else
    static_cast<void>(setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));
#endif
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(config.port);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): socket API requires sockaddr*.
    if (bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
        listen(listener, 16) != 0) {
        std::cerr << "error: unable to listen on http://127.0.0.1:" << config.port << "\n";
        close_socket(listener);
        return 1;
    }
    print_performance_environment();
    std::cout << "Ibex UI: http://127.0.0.1:" << config.port << "\n";
    std::cout << "Press Ctrl+C to stop.\n";

    std::map<std::string, std::unique_ptr<Session>, std::less<>> sessions;
    while (true) {
        const Socket client = accept(listener, nullptr, nullptr);
        if (client == kInvalidSocket)
            continue;
        const auto request = read_request(client);
        if (!request) {
            close_socket(client);
            continue;
        }
        std::string created_cookie;
        auto id = session_id(*request);
        if (!id || !sessions.contains(*id)) {
            id = new_session_id();
            sessions.insert_or_assign(*id, std::make_unique<Session>(config.repl, registry));
            created_cookie = "ibex_session=" + *id;
            if (config.demo) {
                const auto seeded = sessions.at(*id)->repl.execute(kDemoBootstrap);
                if (!seeded.ok) {
                    static bool warned = false;
                    if (!warned) {
                        warned = true;
                        std::cerr << "warning: --demo seeding failed: " << seeded.error
                                  << " (is the data_gen plugin on the library path?)\n";
                    }
                }
            }
        }
        auto& session = *sessions.at(*id);
        try {
            if (request->method == "GET" && request->target == "/api/v1/config") {
                send_response(client, 200, "OK", json({{"demo", config.demo}}).dump(),
                              "application/json; charset=utf-8", created_cookie);
            } else if (request->method == "GET" && request->target == "/api/v1/environment") {
                send_response(client, 200, "OK", environment_json(session).dump(),
                              "application/json; charset=utf-8", created_cookie);
            } else if (request->method == "GET" && request->target.starts_with("/api/v1/files")) {
                const auto relative_path = query_value(request->target, "path");
                if (!relative_path) {
                    send_response(client, 400, "Bad Request", R"({"error":"invalid file path"})");
                } else if (const auto files = data_directory_json(data_directory, *relative_path)) {
                    send_response(client, 200, "OK", files->dump(),
                                  "application/json; charset=utf-8", created_cookie);
                } else {
                    send_response(client, 404, "Not Found", R"({"error":"directory not found"})");
                }
            } else if (request->method == "POST" && request->target == "/api/v1/execute") {
                const json body = json::parse(request->body);
                const auto started_at = std::chrono::steady_clock::now();
                auto execution = session.repl.execute(body.value("source", ""));
                const auto elapsed = std::chrono::steady_clock::now() - started_at;
                const double elapsed_ms =
                    std::chrono::duration<double, std::milli>(elapsed).count();
                json response = {{"ok", execution.ok},
                                 {"elapsed_ms", elapsed_ms},
                                 {"environment", environment_json(session)}};
                if (!execution.ok) {
                    response["error"] = {{"message", execution.error}};
                    if (execution.error_line.has_value()) {
                        response["error"]["line"] = *execution.error_line;
                    }
                    if (execution.error_column.has_value()) {
                        response["error"]["column"] = *execution.error_column;
                    }
                } else if (!execution.tables.empty()) {
                    json results = json::array();
                    for (auto& table : execution.tables) {
                        const std::string result_id = std::to_string(session.next_result_id++);
                        session.results.insert_or_assign(result_id, std::move(table));
                        while (session.results.size() > 16)
                            session.results.erase(session.results.begin());
                        results.push_back({{"result_id", result_id},
                                           {"result", table_page(session.results.at(result_id), 0,
                                                                 bounded_limit(body))}});
                    }
                    response["results"] = std::move(results);
                    response["result_id"] = response["results"][0]["result_id"];
                    response["result"] = response["results"][0]["result"];
                } else if (execution.table.has_value()) {
                    const std::string result_id = std::to_string(session.next_result_id++);
                    session.results.insert_or_assign(result_id, std::move(*execution.table));
                    response["result_id"] = result_id;
                    response["result"] =
                        table_page(session.results.at(result_id), 0, bounded_limit(body));
                } else if (execution.scalar.has_value()) {
                    response["result"] = {{"kind", "scalar"},
                                          {"value", scalar_json(*execution.scalar)}};
                } else {
                    response["result"] = {{"kind", "none"}};
                }
                send_response(client, 200, "OK", response.dump(), "application/json; charset=utf-8",
                              created_cookie);
            } else if (request->method == "GET" &&
                       request->target.starts_with("/api/v1/results/")) {
                const auto raw =
                    request->target.substr(std::string_view("/api/v1/results/").size());
                const auto question = raw.find('?');
                const std::string result_id = raw.substr(0, question);
                const auto result = session.results.find(result_id);
                if (result == session.results.end()) {
                    send_response(client, 404, "Not Found", R"({"error":"result expired"})");
                } else {
                    const auto offset = query_size(request->target, "offset", 0);
                    const auto limit = std::clamp(query_size(request->target, "limit", 200),
                                                  std::size_t{1}, std::size_t{1000});
                    send_response(client, 200, "OK",
                                  table_page(result->second, offset, limit).dump());
                }
            } else if (request->method == "DELETE" &&
                       request->target.starts_with("/api/v1/environment/")) {
                const auto name =
                    request->target.substr(std::string_view("/api/v1/environment/").size());
                const bool removed = session.repl.erase(name);
                send_response(client, removed ? 200 : 404, removed ? "OK" : "Not Found",
                              environment_json(session).dump(), "application/json; charset=utf-8",
                              created_cookie);
            } else if (request->method == "GET") {
                const auto* const asset = static_file(*assets, request->target);
                if (!asset) {
                    send_response(client, 404, "Not Found", "Not found",
                                  "text/plain; charset=utf-8");
                } else {
                    send_response(client, 200, "OK", asset->contents, mime_type(asset->path),
                                  created_cookie);
                }
            } else {
                send_response(client, 404, "Not Found", R"({"error":"unknown endpoint"})");
            }
        } catch (const std::exception& error) {
            send_response(client, 400, "Bad Request", json({{"error", error.what()}}).dump());
        }
        close_socket(client);
    }
}

}  // namespace ibex::ui
