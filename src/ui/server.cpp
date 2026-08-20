// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ui/server.hpp>

#include <ibex/runtime/table_format.hpp>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

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
auto close_socket(Socket socket) -> void { closesocket(socket); }
#else
using Socket = int;
constexpr Socket kInvalidSocket = -1;
auto close_socket(Socket socket) -> void { close(socket); }
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

auto lower(std::string text) -> std::string {
    std::ranges::transform(text, text.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
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
        if (raw.size() > 1024 * 1024) {
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
    if (content_length > 8 * 1024 * 1024) {
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
    if (extension == ".js") return "text/javascript; charset=utf-8";
    if (extension == ".css") return "text/css; charset=utf-8";
    if (extension == ".svg") return "image/svg+xml";
    if (extension == ".ico") return "image/x-icon";
    return "text/html; charset=utf-8";
}

auto send_response(Socket socket, int status, std::string_view status_text, std::string body,
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
        const auto count = send(socket, text.data() + sent,
                                remaining, 0);
        if (count <= 0) return;
        sent += static_cast<std::size_t>(count);
    }
}

auto session_id(const HttpRequest& request) -> std::optional<std::string> {
    const auto it = request.headers.find("cookie");
    if (it == request.headers.end()) return std::nullopt;
    constexpr std::string_view prefix = "ibex_session=";
    const auto start = it->second.find(prefix);
    if (start == std::string::npos) return std::nullopt;
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

auto column_type(const runtime::ColumnValue& value) -> std::string {
    return std::visit(
        [](const auto& column) -> std::string {
            using T = typename std::decay_t<decltype(column)>::value_type;
            if constexpr (std::same_as<T, std::int64_t>) return "Int64";
            if constexpr (std::same_as<T, double>) return "Float64";
            if constexpr (std::same_as<T, std::string>) return "String";
            if constexpr (std::same_as<T, Categorical>) return "Categorical";
            if constexpr (std::same_as<T, Date>) return "Date";
            if constexpr (std::same_as<T, Timestamp>) return "Timestamp";
            return "Bool";
        },
        value);
}

auto cell_json(const runtime::ColumnEntry& entry, std::size_t row) -> json {
    if (runtime::is_null(entry, row)) return nullptr;
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
        for (const auto& entry : table.columns) values.push_back(cell_json(entry, row));
        rows.push_back(std::move(values));
    }
    return {{"kind", "table"}, {"columns", std::move(columns)}, {"rows", std::move(rows)},
            {"offset", start}, {"limit", limit}, {"total_rows", table.rows()}};
}

auto bounded_limit(const json& body) -> std::size_t {
    const auto requested = body.value("limit", 200U);
    return std::clamp<std::size_t>(requested, 1, 1000);
}

auto query_size(std::string_view target, std::string_view key, std::size_t fallback) -> std::size_t {
    const auto question = target.find('?');
    if (question == std::string_view::npos) return fallback;
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
        if (ampersand == std::string_view::npos) break;
        query.remove_prefix(ampersand + 1);
    }
    return fallback;
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
        tables.push_back({{"name", table.name}, {"rows", table.rows}, {"lazy", table.lazy},
                          {"columns", std::move(columns)}});
    }
    return {{"tables", std::move(tables)}};
}

auto static_file(const std::filesystem::path& root, std::string_view target)
    -> std::optional<std::pair<std::filesystem::path, std::string>> {
    const auto query = target.find('?');
    const std::string relative(target.substr(0, query));
    std::filesystem::path requested = relative == "/" ? "index.html" : relative.substr(1);
    requested = requested.lexically_normal();
    if (requested.empty() || requested.is_absolute() || requested.string().contains("..")) {
        return std::nullopt;
    }
    auto path = root / requested;
    if (!std::filesystem::is_regular_file(path)) {
        path = root / "index.html";  // SPA navigation fallback.
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) return std::nullopt;
    return std::pair{path, std::string(std::istreambuf_iterator<char>(input), {})};
}

}  // namespace

auto run_server(const ServerConfig& config, runtime::ExternRegistry& registry) -> int {
    if (!std::filesystem::is_regular_file(config.web_root / "index.html")) {
        std::cerr << "error: Ibex UI assets are missing from '" << config.web_root.string()
                  << "'\n";
        return 1;
    }
#ifdef _WIN32
    WSADATA data{};
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 1;
#endif
    const Socket listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener == kInvalidSocket) return 1;
    int reuse = 1;
    static_cast<void>(setsockopt(listener, SOL_SOCKET, SO_REUSEADDR,
                                 reinterpret_cast<const char*>(&reuse), sizeof(reuse)));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(config.port);
    if (bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
        listen(listener, 16) != 0) {
        std::cerr << "error: unable to listen on http://127.0.0.1:" << config.port << "\n";
        close_socket(listener);
        return 1;
    }
    std::cout << "Ibex UI: http://127.0.0.1:" << config.port << "\n";
    std::cout << "Press Ctrl+C to stop.\n";

    std::map<std::string, std::unique_ptr<Session>, std::less<>> sessions;
    while (true) {
        const Socket client = accept(listener, nullptr, nullptr);
        if (client == kInvalidSocket) continue;
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
        }
        auto& session = *sessions.at(*id);
        try {
            if (request->method == "GET" && request->target == "/api/v1/environment") {
                send_response(client, 200, "OK", environment_json(session).dump(),
                              "application/json; charset=utf-8", created_cookie);
            } else if (request->method == "POST" && request->target == "/api/v1/execute") {
                const json body = json::parse(request->body);
                const auto execution = session.repl.execute(body.value("source", ""));
                json response = {{"ok", execution.ok}, {"environment", environment_json(session)}};
                if (!execution.ok) {
                    response["error"] = {{"message", execution.error}};
                    if (execution.error_line.has_value()) {
                        response["error"]["line"] = *execution.error_line;
                    }
                    if (execution.error_column.has_value()) {
                        response["error"]["column"] = *execution.error_column;
                    }
                } else if (execution.table.has_value()) {
                    const std::string result_id = std::to_string(session.next_result_id++);
                    session.results.insert_or_assign(result_id, std::move(*execution.table));
                    while (session.results.size() > 16) session.results.erase(session.results.begin());
                    response["result_id"] = result_id;
                    response["result"] = table_page(session.results.at(result_id), 0, bounded_limit(body));
                } else if (execution.scalar.has_value()) {
                    response["result"] = {{"kind", "scalar"}, {"value", scalar_json(*execution.scalar)}};
                } else {
                    response["result"] = {{"kind", "none"}};
                }
                send_response(client, 200, "OK", response.dump(), "application/json; charset=utf-8",
                              created_cookie);
            } else if (request->method == "GET" && request->target.starts_with("/api/v1/results/")) {
                const auto raw = request->target.substr(std::string_view("/api/v1/results/").size());
                const auto question = raw.find('?');
                const std::string result_id = raw.substr(0, question);
                const auto result = session.results.find(result_id);
                if (result == session.results.end()) {
                    send_response(client, 404, "Not Found", R"({"error":"result expired"})");
                } else {
                    const auto offset = query_size(request->target, "offset", 0);
                    const auto limit = std::clamp(query_size(request->target, "limit", 200),
                                                  std::size_t{1}, std::size_t{1000});
                    send_response(client, 200, "OK", table_page(result->second, offset, limit).dump());
                }
            } else if (request->method == "DELETE" && request->target.starts_with("/api/v1/environment/")) {
                const auto name = request->target.substr(std::string_view("/api/v1/environment/").size());
                const bool removed = session.repl.erase(name);
                send_response(client, removed ? 200 : 404, removed ? "OK" : "Not Found",
                              environment_json(session).dump(), "application/json; charset=utf-8",
                              created_cookie);
            } else if (request->method == "GET") {
                const auto asset = static_file(config.web_root, request->target);
                if (!asset) {
                    send_response(client, 404, "Not Found", "Not found", "text/plain; charset=utf-8");
                } else {
                    send_response(client, 200, "OK", asset->second, mime_type(asset->first), created_cookie);
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
