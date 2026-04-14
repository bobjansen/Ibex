#pragma once
// Ibex CSV library — RFC 4180 compliant CSV reading and writing.
//
// Reading:
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv");
// Optional null controls:
//   extern fn read_csv(path: String, nulls: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv", "<empty>,NA");
// Optional delimiter and header controls:
//   extern fn read_csv(path: String, nulls: String, delimiter: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv", "", ";");
//   extern fn read_csv(path: String, nulls: String, delimiter: String, has_header: Bool)
//       -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv", "", ";", false);
//
// Writing:
//   extern fn write_csv(df: DataFrame, path: String) -> Int from "csv.hpp";
//   let rows = write_csv(df, "data/out.csv");
//
// The reader streams the file via mmap (on POSIX) and parses it in a single
// pass into views over the backing buffer; only fields that contain escaped
// `""` sequences allocate a heap string.  Typed column construction then
// performs at most one numeric-probe scan per column and fuses the final
// parse into the build loop, so each value is parsed at most twice.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fast_float/fast_float.h>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#if __has_include(<sys/mman.h>)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#define IBEX_CSV_HAVE_MMAP 1
#else
#define IBEX_CSV_HAVE_MMAP 0
#endif

namespace {

enum class CsvColumnKind : std::uint8_t {
    Infer,
    Int,
    Double,
    String,
    Categorical,
};

struct CsvSchemaEntry {
    std::optional<std::string> name;  // set if the entry was written as `name:type`
    CsvColumnKind kind = CsvColumnKind::Infer;
};

struct CsvSchemaHint {
    std::vector<CsvSchemaEntry> entries;
    [[nodiscard]] auto empty() const noexcept -> bool { return entries.empty(); }
};

struct CsvReadOptions {
    bool null_if_empty = false;
    std::unordered_set<std::string> null_tokens;
    char delimiter = ',';
    bool has_header = true;
    CsvSchemaHint schema;
};

inline auto csv_trim(std::string_view text) -> std::string_view {
    auto begin = text.find_first_not_of(" \t\r\n");
    if (begin == std::string_view::npos) {
        return {};
    }
    auto end = text.find_last_not_of(" \t\r\n");
    return text.substr(begin, end - begin + 1);
}

inline auto csv_parse_null_spec(std::string_view spec) -> CsvReadOptions {
    CsvReadOptions options;
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        std::size_t comma = spec.find(',', pos);
        if (comma == std::string_view::npos) {
            comma = spec.size();
        }
        auto token = csv_trim(spec.substr(pos, comma - pos));
        if (!token.empty()) {
            if (token == "<empty>") {
                options.null_if_empty = true;
            } else {
                options.null_tokens.emplace(token);
            }
        }
        if (comma == spec.size()) {
            break;
        }
        pos = comma + 1;
    }
    return options;
}

inline auto csv_parse_delimiter(std::string_view spec) -> char {
    if (spec.size() != 1) {
        throw std::runtime_error("read_csv: delimiter must be a single character");
    }
    return spec.front();
}

inline auto csv_parse_column_kind(std::string_view type_str) -> CsvColumnKind {
    if (type_str == "i64" || type_str == "int") {
        return CsvColumnKind::Int;
    }
    if (type_str == "f64" || type_str == "double" || type_str == "float") {
        return CsvColumnKind::Double;
    }
    if (type_str == "str" || type_str == "string") {
        return CsvColumnKind::String;
    }
    if (type_str == "cat" || type_str == "categorical") {
        return CsvColumnKind::Categorical;
    }
    throw std::runtime_error("read_csv: unknown schema type '" + std::string(type_str) + "'");
}

inline auto csv_parse_schema(std::string_view spec) -> CsvSchemaHint {
    CsvSchemaHint hint;
    if (spec.empty()) {
        return hint;
    }
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        std::size_t comma = spec.find(',', pos);
        if (comma == std::string_view::npos) {
            comma = spec.size();
        }
        auto token = csv_trim(spec.substr(pos, comma - pos));
        if (!token.empty()) {
            CsvSchemaEntry entry;
            const auto colon = token.find(':');
            std::string_view type_str;
            if (colon != std::string_view::npos) {
                entry.name = std::string(csv_trim(token.substr(0, colon)));
                type_str = csv_trim(token.substr(colon + 1));
            } else {
                type_str = token;
            }
            entry.kind = csv_parse_column_kind(type_str);
            hint.entries.push_back(std::move(entry));
        }
        if (comma == spec.size()) {
            break;
        }
        pos = comma + 1;
    }
    return hint;
}

/// Owns the source buffer for a CSV read.  Uses mmap on POSIX when the input
/// is a regular file; otherwise reads the file into a heap-allocated buffer.
class CsvSource {
   public:
    explicit CsvSource(const std::string& path) {
#if IBEX_CSV_HAVE_MMAP
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat st{};
            if (::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0) {
                size_ = static_cast<std::size_t>(st.st_size);
                if (size_ == 0) {
                    ::close(fd);
                    data_ = "";
                    return;
                }
                void* addr = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
                if (addr != MAP_FAILED) {
                    ::madvise(addr, size_, MADV_SEQUENTIAL);
                    fd_ = fd;
                    mapped_ = addr;
                    data_ = static_cast<const char*>(addr);
                    return;
                }
            }
            ::close(fd);
        }
#endif
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("read_csv: failed to open '" + path + "'");
        }
        in.seekg(0, std::ios::end);
        const auto sz = in.tellg();
        if (sz > 0) {
            fallback_.resize(static_cast<std::size_t>(sz));
            in.seekg(0, std::ios::beg);
            in.read(fallback_.data(), static_cast<std::streamsize>(sz));
            size_ = static_cast<std::size_t>(in.gcount());
            data_ = fallback_.data();
        } else {
            data_ = "";
            size_ = 0;
        }
    }

    ~CsvSource() {
#if IBEX_CSV_HAVE_MMAP
        if (mapped_ != nullptr) {
            ::munmap(mapped_, size_);
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
#endif
    }

    CsvSource(const CsvSource&) = delete;
    auto operator=(const CsvSource&) -> CsvSource& = delete;
    CsvSource(CsvSource&&) = delete;
    auto operator=(CsvSource&&) -> CsvSource& = delete;

    [[nodiscard]] auto data() const noexcept -> const char* { return data_; }
    [[nodiscard]] auto size() const noexcept -> std::size_t { return size_; }

   private:
    const char* data_ = nullptr;
    std::size_t size_ = 0;
#if IBEX_CSV_HAVE_MMAP
    int fd_ = -1;
    void* mapped_ = nullptr;
#endif
    std::vector<char> fallback_;
};

/// Parse one logical row out of [pos, end).  On return `pos` is advanced past
/// the row's terminating newline (if any).  Views for ordinary fields point
/// into the caller-owned source buffer; views for fields that contained a
/// `""` escape point into strings held by `escape_storage`.  A std::deque is
/// used so that subsequent push_backs do not invalidate earlier references.
///
/// Returns true iff a non-empty row was produced.  Empty lines are skipped
/// (matching typical CSV reader behavior); a trailing newline does not
/// synthesize an extra empty row.
inline auto csv_parse_row(const char*& pos, const char* end, char delim,
                          std::vector<std::string_view>& out_fields,
                          std::deque<std::string>& escape_storage) -> bool {
    while (pos < end && (*pos == '\n' || *pos == '\r')) {
        if (*pos == '\r' && pos + 1 < end && *(pos + 1) == '\n') {
            pos += 2;
        } else {
            ++pos;
        }
    }
    if (pos >= end) {
        return false;
    }

    out_fields.clear();
    while (true) {
        if (*pos == '"') {
            ++pos;
            const char* field_start = pos;
            bool has_escape = false;
            const char* content_end = nullptr;
            while (pos < end) {
                if (*pos == '"') {
                    if (pos + 1 < end && *(pos + 1) == '"') {
                        has_escape = true;
                        pos += 2;
                        continue;
                    }
                    content_end = pos;
                    ++pos;
                    break;
                }
                ++pos;
            }
            if (content_end == nullptr) {
                content_end = pos;
            }
            if (!has_escape) {
                out_fields.emplace_back(field_start,
                                        static_cast<std::size_t>(content_end - field_start));
            } else {
                auto& buf = escape_storage.emplace_back();
                buf.reserve(static_cast<std::size_t>(content_end - field_start));
                for (const char* p = field_start; p < content_end;) {
                    if (*p == '"' && p + 1 < content_end && *(p + 1) == '"') {
                        buf.push_back('"');
                        p += 2;
                    } else {
                        buf.push_back(*p);
                        ++p;
                    }
                }
                out_fields.emplace_back(buf);
            }
            if (pos < end && *pos != delim && *pos != '\n' && *pos != '\r') {
                while (pos < end && *pos != delim && *pos != '\n' && *pos != '\r') {
                    ++pos;
                }
            }
        } else {
            const char* field_start = pos;
            while (pos < end && *pos != delim && *pos != '\n' && *pos != '\r') {
                ++pos;
            }
            out_fields.emplace_back(field_start, static_cast<std::size_t>(pos - field_start));
        }

        if (pos >= end) {
            return true;
        }
        if (*pos == delim) {
            ++pos;
            continue;
        }
        if (*pos == '\r') {
            ++pos;
            if (pos < end && *pos == '\n') {
                ++pos;
            }
            return true;
        }
        if (*pos == '\n') {
            ++pos;
            return true;
        }
    }
}

inline auto csv_try_int(std::string_view sv, std::int64_t& out) -> bool {
    if (sv.empty()) {
        return false;
    }
    const char* begin = sv.data();
    const char* end = sv.data() + sv.size();
    auto result = std::from_chars(begin, end, out);
    return result.ec == std::errc() && result.ptr == end;
}

inline auto csv_try_double(std::string_view sv, double& out) -> bool {
    if (sv.empty()) {
        return false;
    }
    const char* begin = sv.data();
    const char* end = sv.data() + sv.size();
    auto result = fast_float::from_chars(begin, end, out);
    return result.ec == std::errc() && result.ptr == end;
}

}  // namespace

inline auto read_csv_with_options(std::string_view path, const CsvReadOptions& options)
    -> ibex::runtime::Table {
    const std::string path_string(path);
    std::error_code ec;
    const bool exists = std::filesystem::exists(path_string, ec);
    if (ec) {
        throw std::runtime_error("read_csv: failed to inspect path '" + path_string +
                                 "': " + ec.message());
    }
    if (!exists) {
        throw std::runtime_error("read_csv: file not found: '" + path_string + "'");
    }

    CsvSource source(path_string);
    const char* pos = source.data();
    const char* end = source.data() + source.size();

    std::deque<std::string> escape_storage;
    std::vector<std::string_view> row_buf;
    row_buf.reserve(16);

    std::vector<std::string> col_names;
    if (options.has_header) {
        if (!csv_parse_row(pos, end, options.delimiter, row_buf, escape_storage)) {
            return ibex::runtime::Table{};
        }
        col_names.reserve(row_buf.size());
        for (auto sv : row_buf) {
            col_names.emplace_back(sv);
        }
    }

    // Cheap newline count for row reservation.  Quoted newlines are rare
    // enough that this is a fine upper bound.
    std::size_t row_estimate = 0;
    {
        const char* scan = pos;
        while (scan < end) {
            const char* nl = static_cast<const char*>(
                std::memchr(scan, '\n', static_cast<std::size_t>(end - scan)));
            if (nl == nullptr) {
                ++row_estimate;
                break;
            }
            ++row_estimate;
            scan = nl + 1;
        }
    }

    // Resolve a schema hint for column `c`/`name`: match by name if any
    // entries were written as `name:type`, otherwise by position.
    auto resolve_hint = [&](std::size_t idx, const std::string& name) -> CsvColumnKind {
        if (options.schema.empty()) {
            return CsvColumnKind::Infer;
        }
        for (const auto& entry : options.schema.entries) {
            if (entry.name && *entry.name == name) {
                return entry.kind;
            }
        }
        if (idx < options.schema.entries.size() && !options.schema.entries[idx].name) {
            return options.schema.entries[idx].kind;
        }
        return CsvColumnKind::Infer;
    };

    std::vector<std::string_view> first_row;
    bool has_first_row = csv_parse_row(pos, end, options.delimiter, row_buf, escape_storage);
    if (has_first_row) {
        first_row = row_buf;
        if (!options.has_header) {
            col_names.reserve(first_row.size());
            for (std::size_t i = 0; i < first_row.size(); ++i) {
                col_names.push_back("col" + std::to_string(i + 1));
            }
        }
    }

    std::size_t n_cols = col_names.size();
    std::vector<CsvColumnKind> resolved_hints;
    resolved_hints.reserve(n_cols);
    bool all_hints_known = has_first_row;
    for (std::size_t c = 0; c < n_cols; ++c) {
        const auto hint = resolve_hint(c, col_names[c]);
        resolved_hints.push_back(hint);
        if (hint == CsvColumnKind::Infer) {
            all_hints_known = false;
        }
    }

    const bool need_null_check = options.null_if_empty || !options.null_tokens.empty();

    if (has_first_row && all_hints_known && !need_null_check) {
        struct CsvSchemaColumnBuilder {
            CsvColumnKind kind;
            ibex::Column<std::int64_t> ints;
            ibex::Column<double> doubles;
            ibex::Column<std::string> strings;
            using code_type = ibex::Column<ibex::Categorical>::code_type;
            std::vector<std::string> dict;
            ibex::Column<ibex::Categorical>::index_map cat_index;
            std::vector<code_type> cat_codes;

            explicit CsvSchemaColumnBuilder(CsvColumnKind kind_in) : kind(kind_in) {}

            void reserve(std::size_t n) {
                switch (kind) {
                    case CsvColumnKind::Int:
                        ints.reserve(n);
                        break;
                    case CsvColumnKind::Double:
                        doubles.reserve(n);
                        break;
                    case CsvColumnKind::String:
                        strings.reserve(n);
                        break;
                    case CsvColumnKind::Categorical:
                        cat_codes.reserve(n);
                        dict.reserve(std::max<std::size_t>(8, n / 100));
                        cat_index.reserve(std::max<std::size_t>(8, n / 100));
                        break;
                    case CsvColumnKind::Infer:
                        break;
                }
            }

            void append(std::string_view name, std::size_t row_index, std::string_view sv) {
                if (kind == CsvColumnKind::Int) {
                    std::int64_t iv{};
                    if (!csv_try_int(sv, iv)) {
                        throw std::runtime_error("read_csv: column '" + std::string(name) +
                                                 "' row " + std::to_string(row_index) +
                                                 " failed to parse as i64");
                    }
                    ints.push_back(iv);
                    return;
                }
                if (kind == CsvColumnKind::Double) {
                    double dv{};
                    if (!csv_try_double(sv, dv)) {
                        throw std::runtime_error("read_csv: column '" + std::string(name) +
                                                 "' row " + std::to_string(row_index) +
                                                 " failed to parse as f64");
                    }
                    doubles.push_back(dv);
                    return;
                }
                if (kind == CsvColumnKind::String) {
                    strings.push_back(sv);
                    return;
                }
                if (kind == CsvColumnKind::Categorical) {
                    const auto it = cat_index.find(sv);
                    if (it != cat_index.end()) {
                        cat_codes.push_back(it->second);
                        return;
                    }
                    const auto code = static_cast<code_type>(dict.size());
                    dict.emplace_back(sv);
                    cat_index.emplace(dict.back(), code);
                    cat_codes.push_back(code);
                    return;
                }
            }

            [[nodiscard]] auto finish() && -> ibex::runtime::ColumnValue {
                switch (kind) {
                    case CsvColumnKind::Int:
                        return std::move(ints);
                    case CsvColumnKind::Double:
                        return std::move(doubles);
                    case CsvColumnKind::String:
                        return std::move(strings);
                    case CsvColumnKind::Categorical:
                        return ibex::Column<ibex::Categorical>(std::move(dict),
                                                               std::move(cat_codes));
                    case CsvColumnKind::Infer:
                        break;
                }
                throw std::runtime_error("read_csv: internal error - unresolved schema hint");
            }
        };

        std::vector<CsvSchemaColumnBuilder> builders;
        builders.reserve(n_cols);
        for (const auto hint : resolved_hints) {
            builders.emplace_back(hint);
            builders.back().reserve(row_estimate);
        }

        std::size_t row_index = 0;
        auto append_row = [&](const std::vector<std::string_view>& fields) {
            const std::size_t row_cols = fields.size();
            for (std::size_t c = 0; c < n_cols; ++c) {
                const auto sv = c < row_cols ? fields[c] : std::string_view{};
                builders[c].append(col_names[c], row_index, sv);
            }
            ++row_index;
        };

        append_row(first_row);
        while (csv_parse_row(pos, end, options.delimiter, row_buf, escape_storage)) {
            append_row(row_buf);
        }

        ibex::runtime::Table direct_table;
        for (std::size_t c = 0; c < n_cols; ++c) {
            direct_table.add_column(col_names[c], std::move(builders[c]).finish());
        }
        return direct_table;
    }

    std::vector<std::vector<std::string_view>> columns(n_cols);
    for (auto& c : columns) {
        c.reserve(row_estimate);
    }
    if (has_first_row) {
        const std::size_t row_cols = first_row.size();
        for (std::size_t c = 0; c < n_cols; ++c) {
            columns[c].push_back(c < row_cols ? first_row[c] : std::string_view{});
        }
    }
    while (csv_parse_row(pos, end, options.delimiter, row_buf, escape_storage)) {
        const std::size_t row_cols = row_buf.size();
        for (std::size_t c = 0; c < n_cols; ++c) {
            if (c < row_cols) {
                columns[c].push_back(row_buf[c]);
            } else {
                columns[c].emplace_back();
            }
        }
    }

    ibex::runtime::Table table;
    constexpr double kMaxCategoricalRatio = 0.10;

    for (std::size_t c = 0; c < columns.size(); ++c) {
        const auto& vals = columns[c];
        const auto& name = col_names[c];
        const std::size_t n = vals.size();
        const CsvColumnKind hint = resolve_hint(c, name);

        ibex::runtime::ValidityBitmap validity;
        bool has_nulls = false;
        if (need_null_check) {
            validity.assign(n, true);
            for (std::size_t i = 0; i < n; ++i) {
                const auto sv = vals[i];
                bool row_is_null = false;
                if (options.null_if_empty && sv.empty()) {
                    row_is_null = true;
                } else if (!options.null_tokens.empty() &&
                           options.null_tokens.contains(std::string(sv))) {
                    row_is_null = true;
                }
                if (row_is_null) {
                    validity.set(i, false);
                    has_nulls = true;
                }
            }
        }

        auto is_null = [&](std::size_t i) -> bool {
            return need_null_check && !validity[i];
        };

        // Schema-hinted fast path: parse directly to the declared type, skip
        // inference probes, and throw on parse failure.
        if (hint != CsvColumnKind::Infer) {
            if (hint == CsvColumnKind::Int) {
                ibex::Column<std::int64_t> col;
                col.reserve(n);
                for (std::size_t i = 0; i < n; ++i) {
                    if (is_null(i)) {
                        col.push_back(0);
                        continue;
                    }
                    std::int64_t iv{};
                    if (!csv_try_int(vals[i], iv)) {
                        throw std::runtime_error("read_csv: column '" + name + "' row " +
                                                 std::to_string(i) + " failed to parse as i64");
                    }
                    col.push_back(iv);
                }
                if (has_nulls) {
                    table.add_column(name, std::move(col), std::move(validity));
                } else {
                    table.add_column(name, std::move(col));
                }
                continue;
            }
            if (hint == CsvColumnKind::Double) {
                ibex::Column<double> col;
                col.reserve(n);
                for (std::size_t i = 0; i < n; ++i) {
                    if (is_null(i)) {
                        col.push_back(0.0);
                        continue;
                    }
                    double dv{};
                    if (!csv_try_double(vals[i], dv)) {
                        throw std::runtime_error("read_csv: column '" + name + "' row " +
                                                 std::to_string(i) + " failed to parse as f64");
                    }
                    col.push_back(dv);
                }
                if (has_nulls) {
                    table.add_column(name, std::move(col), std::move(validity));
                } else {
                    table.add_column(name, std::move(col));
                }
                continue;
            }
            if (hint == CsvColumnKind::Categorical) {
                std::vector<ibex::Column<ibex::Categorical>::code_type> codes;
                codes.reserve(n);
                std::vector<std::string> dict;
                std::unordered_map<std::string_view, ibex::Column<ibex::Categorical>::code_type>
                    index;
                for (std::size_t i = 0; i < n; ++i) {
                    if (is_null(i)) {
                        codes.push_back(0);
                        continue;
                    }
                    const auto sv = vals[i];
                    auto it = index.find(sv);
                    if (it != index.end()) {
                        codes.push_back(it->second);
                        continue;
                    }
                    auto code =
                        static_cast<ibex::Column<ibex::Categorical>::code_type>(dict.size());
                    dict.emplace_back(sv);
                    index.emplace(dict.back(), code);
                    codes.push_back(code);
                }
                ibex::Column<ibex::Categorical> col(std::move(dict), std::move(codes));
                if (has_nulls) {
                    table.add_column(name, std::move(col), std::move(validity));
                } else {
                    table.add_column(name, std::move(col));
                }
                continue;
            }
            // CsvColumnKind::String — skip numeric probes and categorical
            // promotion entirely.
            ibex::Column<std::string> col;
            col.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                if (is_null(i)) {
                    col.push_back(std::string_view{});
                } else {
                    col.push_back(vals[i]);
                }
            }
            if (has_nulls) {
                table.add_column(name, std::move(col), std::move(validity));
            } else {
                table.add_column(name, std::move(col));
            }
            continue;
        }

        // Try int64 with a fused build pass: if every non-null value parses,
        // we keep the built column directly.
        if (n > 0) {
            ibex::Column<std::int64_t> int_col;
            int_col.reserve(n);
            bool all_int = true;
            bool any_valid = false;
            for (std::size_t i = 0; i < n; ++i) {
                if (is_null(i)) {
                    int_col.push_back(0);
                    continue;
                }
                std::int64_t iv{};
                if (!csv_try_int(vals[i], iv)) {
                    all_int = false;
                    break;
                }
                int_col.push_back(iv);
                any_valid = true;
            }
            if (all_int && any_valid) {
                if (has_nulls) {
                    table.add_column(name, std::move(int_col), std::move(validity));
                } else {
                    table.add_column(name, std::move(int_col));
                }
                continue;
            }
        }

        // Try double with a fused build pass.
        if (n > 0) {
            ibex::Column<double> dbl_col;
            dbl_col.reserve(n);
            bool all_double = true;
            bool any_valid = false;
            for (std::size_t i = 0; i < n; ++i) {
                if (is_null(i)) {
                    dbl_col.push_back(0.0);
                    continue;
                }
                double dv{};
                if (!csv_try_double(vals[i], dv)) {
                    all_double = false;
                    break;
                }
                dbl_col.push_back(dv);
                any_valid = true;
            }
            if (all_double && any_valid) {
                if (has_nulls) {
                    table.add_column(name, std::move(dbl_col), std::move(validity));
                } else {
                    table.add_column(name, std::move(dbl_col));
                }
                continue;
            }
        }

        // String fallback.  When nulls are present, skip categorical detection
        // and keep plain string + validity bitmap (matching historical
        // behavior).
        if (has_nulls) {
            ibex::Column<std::string> col;
            col.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                if (!validity[i]) {
                    col.push_back(std::string_view{});
                } else {
                    col.push_back(vals[i]);
                }
            }
            table.add_column(name, std::move(col), std::move(validity));
            continue;
        }

        // Pure-string column: try categorical promotion.
        if (n > 0) {
            const std::size_t max_uniques = std::max<std::size_t>(
                1, static_cast<std::size_t>(static_cast<double>(n) * kMaxCategoricalRatio));
            std::vector<ibex::Column<ibex::Categorical>::code_type> codes;
            codes.reserve(n);
            std::vector<std::string> dict;
            std::unordered_map<std::string_view, ibex::Column<ibex::Categorical>::code_type> index;
            bool ok = true;
            for (auto sv : vals) {
                auto it = index.find(sv);
                if (it != index.end()) {
                    codes.push_back(it->second);
                    continue;
                }
                if (index.size() + 1 > max_uniques) {
                    ok = false;
                    break;
                }
                auto code = static_cast<ibex::Column<ibex::Categorical>::code_type>(dict.size());
                dict.push_back(std::string(sv));
                index.emplace(dict.back(), code);
                codes.push_back(code);
            }
            if (ok) {
                table.add_column(
                    name, ibex::Column<ibex::Categorical>(std::move(dict), std::move(codes)));
                continue;
            }
        }

        ibex::Column<std::string> col;
        col.reserve(n);
        for (auto sv : vals) {
            col.push_back(sv);
        }
        table.add_column(name, std::move(col));
    }

    return table;
}

inline auto read_csv(std::string_view path) -> ibex::runtime::Table {
    return read_csv_with_options(path, CsvReadOptions{});
}

inline auto read_csv(std::string_view path, std::string_view null_spec) -> ibex::runtime::Table {
    return read_csv_with_options(path, csv_parse_null_spec(null_spec));
}

inline auto read_csv(std::string_view path, std::string_view null_spec, std::string_view delimiter)
    -> ibex::runtime::Table {
    auto options = csv_parse_null_spec(null_spec);
    options.delimiter = csv_parse_delimiter(delimiter);
    return read_csv_with_options(path, options);
}

inline auto read_csv(std::string_view path, std::string_view null_spec, std::string_view delimiter,
                     bool has_header) -> ibex::runtime::Table {
    auto options = csv_parse_null_spec(null_spec);
    options.delimiter = csv_parse_delimiter(delimiter);
    options.has_header = has_header;
    return read_csv_with_options(path, options);
}

inline auto read_csv(std::string_view path, std::string_view null_spec, std::string_view delimiter,
                     bool has_header, std::string_view schema) -> ibex::runtime::Table {
    auto options = csv_parse_null_spec(null_spec);
    options.delimiter = csv_parse_delimiter(delimiter);
    options.has_header = has_header;
    options.schema = csv_parse_schema(schema);
    return read_csv_with_options(path, options);
}

namespace {

/// Write a single CSV field, quoting it when necessary (RFC 4180).
inline void csv_write_field(std::ostream& out, std::string_view value) {
    const bool needs_quoting = value.find_first_of(",\"\r\n") != std::string_view::npos;
    if (!needs_quoting) {
        out.write(value.data(), static_cast<std::streamsize>(value.size()));
        return;
    }
    out.put('"');
    for (char c : value) {
        if (c == '"') {
            out.put('"');  // double the quote (RFC 4180)
        }
        out.put(c);
    }
    out.put('"');
}

/// Write one cell of a column at row `r`, dispatching on the column variant.
inline void csv_write_cell(std::ostream& out, const ibex::runtime::ColumnEntry& entry,
                           std::size_t r) {
    if (ibex::runtime::is_null(entry, r)) {
        return;  // empty field for nulls
    }
    std::visit(
        [&](const auto& col) {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, ibex::Column<std::int64_t>>) {
                out << col[r];
            } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                out << col[r];
            } else if constexpr (std::is_same_v<ColT, ibex::Column<std::string>>) {
                csv_write_field(out, col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Categorical>>) {
                csv_write_field(out, col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                out << col[r].days;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>) {
                out << col[r].nanos;
            }
        },
        *entry.column);
}

}  // namespace

/// Write `table` to a CSV file at `path`.
///
/// Returns the number of data rows written (excluding the header line).
/// Null values are written as empty fields.  String fields containing commas,
/// double-quotes, or newlines are quoted per RFC 4180.
inline auto write_csv(const ibex::runtime::Table& table, std::string_view path) -> std::int64_t {
    std::string path_str{path};
    std::ofstream ofs{path_str};
    if (!ofs) {
        throw std::runtime_error("write_csv: cannot open for writing: " + path_str);
    }

    const auto& cols = table.columns;
    const std::size_t n_cols = cols.size();
    const std::size_t n_rows = table.rows();

    // Header row
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (c > 0) {
            ofs.put(',');
        }
        csv_write_field(ofs, cols[c].name);
    }
    ofs.put('\n');

    // Data rows
    for (std::size_t r = 0; r < n_rows; ++r) {
        for (std::size_t c = 0; c < n_cols; ++c) {
            if (c > 0) {
                ofs.put(',');
            }
            csv_write_cell(ofs, cols[c], r);
        }
        ofs.put('\n');
    }

    ofs.flush();
    if (!ofs) {
        throw std::runtime_error("write_csv: I/O error writing: " + path_str);
    }

    return static_cast<std::int64_t>(n_rows);
}
