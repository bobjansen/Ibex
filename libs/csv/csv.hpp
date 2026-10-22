// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once
// On Windows, keep the SDK's min/max macros from breaking fast_float's
// use of std::numeric_limits<T>::max().
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#endif
// Ibex CSV library — RFC 4180 compliant CSV reading and writing.
//
// Reading:
//   extern fn csv::read(path: String) -> DataFrame from "csv.hpp";
//   let df = csv::read("data/myfile.csv");
// Optional null controls:
//   extern fn csv::read(path: String, nulls: String) -> DataFrame from "csv.hpp";
//   let df = csv::read("data/myfile.csv", "<empty>,NA");
// Optional delimiter and header controls:
//   extern fn csv::read(path: String, nulls: String, delimiter: String) -> DataFrame from
//   "csv.hpp"; let df = csv::read("data/myfile.csv", "", ";"); extern fn csv::read(path: String,
//   nulls: String, delimiter: String, has_header: Bool)
//       -> DataFrame from "csv.hpp";
//   let df = csv::read("data/myfile.csv", "", ";", false);
//
// Writing:
//   extern fn csv::write(df: DataFrame, path: String) -> Int from "csv.hpp";
//   let rows = csv::write(df, "data/out.csv");
//
// The reader maps the file (mmap on POSIX) and parses it into views over the
// mapped bytes; only fields that contain escaped `""` sequences allocate a
// heap string. On more than one core a large file is parsed in parallel byte
// ranges ("Parallel eager read" below), with results identical to the serial
// reader, which remains the fallback and the reference.
//
// With a schema that types every column, `csv::read` is also a lazy source
// ("Lazy read with a declared schema" below): binding counts rows, and each
// query parses only the columns it references.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#ifdef IBEX_CSV_HAVE_CURL
#include <curl/curl.h>

#include "../common/curl_api.hpp"
#endif
#include <deque>
#include <fast_float/fast_float.h>
#include <filesystem>
#include <fstream>
#include <optional>
#include <random>
#include <robin_hood.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
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

namespace ibex::csv::detail {

enum class CsvColumnKind : std::uint8_t {
    Infer,
    Int,
    Double,
    String,
    Categorical,
    Date,
    /// `decimal(p,s)`: parsed exactly from the text, never through a double.
    Decimal,
};

struct CsvSchemaEntry {
    std::optional<std::string> name;  // set if the entry was written as `name:type`
    CsvColumnKind kind = CsvColumnKind::Infer;
    ibex::DecimalType decimal{};  // meaningful only when kind == Decimal
};

struct CsvSchemaHint {
    std::vector<CsvSchemaEntry> entries;
    [[nodiscard]] auto empty() const noexcept -> bool { return entries.empty(); }
};

struct CsvReadOptions {
    bool null_if_empty = false;
    robin_hood::unordered_set<std::string> null_tokens;
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
        throw std::runtime_error("csv::read: delimiter must be a single character");
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
    if (type_str == "date") {
        return CsvColumnKind::Date;
    }
    if (type_str == "decimal") {
        throw std::runtime_error(
            "csv::read: decimal needs a precision and scale, e.g. decimal(12,2)");
    }
    throw std::runtime_error("csv::read: unknown schema type '" + std::string(type_str) + "'");
}

/// Parse `decimal(p,s)` (spaces allowed). Returns nullopt for any other type
/// name; throws for a malformed or unsupported decimal.
inline auto csv_parse_decimal_type(std::string_view type_str) -> std::optional<ibex::DecimalType> {
    if (!type_str.starts_with("decimal(")) {
        return std::nullopt;
    }
    const auto fail = [&]() -> std::optional<ibex::DecimalType> {
        throw std::runtime_error("csv::read: invalid decimal type '" + std::string(type_str) +
                                 "' (expected decimal(precision, scale) with precision 1..38 "
                                 "and scale 0..precision)");
    };
    if (!type_str.ends_with(')')) {
        return fail();
    }
    const auto body = type_str.substr(8, type_str.size() - 9);
    const auto comma = body.find(',');
    if (comma == std::string_view::npos) {
        return fail();
    }
    const auto p_text = csv_trim(body.substr(0, comma));
    const auto s_text = csv_trim(body.substr(comma + 1));
    int precision = 0;
    int scale = 0;
    const auto [p_end, p_ec] =
        std::from_chars(p_text.data(), p_text.data() + p_text.size(), precision);
    const auto [s_end, s_ec] = std::from_chars(s_text.data(), s_text.data() + s_text.size(), scale);
    if (p_ec != std::errc{} || s_ec != std::errc{} || p_end != p_text.data() + p_text.size() ||
        s_end != s_text.data() + s_text.size() || precision < 1 ||
        precision > ibex::decimal::kMaxPrecision || scale < 0 || scale > precision) {
        return fail();
    }
    return ibex::DecimalType{.precision = static_cast<std::uint8_t>(precision),
                             .scale = static_cast<std::uint8_t>(scale)};
}

inline auto csv_parse_schema(std::string_view spec) -> CsvSchemaHint {
    CsvSchemaHint hint;
    if (spec.empty()) {
        return hint;
    }
    // The next entry separator: a comma outside parentheses, so that
    // `price:decimal(12,2)` stays one entry.
    const auto next_separator = [&](std::size_t from) {
        int depth = 0;
        for (std::size_t i = from; i < spec.size(); ++i) {
            if (spec[i] == '(') {
                ++depth;
            } else if (spec[i] == ')') {
                --depth;
            } else if (spec[i] == ',' && depth == 0) {
                return i;
            }
        }
        return spec.size();
    };
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        const std::size_t comma = next_separator(pos);
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
            if (auto dec = csv_parse_decimal_type(type_str)) {
                entry.kind = CsvColumnKind::Decimal;
                entry.decimal = *dec;
            } else {
                entry.kind = csv_parse_column_kind(type_str);
            }
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
                // MAP_POPULATE maps every page in one kernel call instead of
                // one fault per 4 KB page as the parse first touches it. The
                // parallel parse is neutral either way; the lazy source's row
                // count -- a bare byte scan -- halved (139 -> 60 ms on 500 MB).
#ifdef MAP_POPULATE
                constexpr int kMapFlags = MAP_PRIVATE | MAP_POPULATE;
#else
                constexpr int kMapFlags = MAP_PRIVATE;
#endif
                void* addr = ::mmap(nullptr, size_, PROT_READ, kMapFlags, fd, 0);
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
            throw std::runtime_error("csv::read: failed to open '" + path + "'");
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

    void advise_dontneed(const char* from, const char* to) const noexcept {
#if IBEX_CSV_HAVE_MMAP
        if (mapped_ == nullptr || from >= to) {
            return;
        }
        const auto page_size = static_cast<std::uintptr_t>(::sysconf(_SC_PAGESIZE));
        auto start = reinterpret_cast<std::uintptr_t>(from);
        auto aligned = start & ~(page_size - 1);
        auto end_addr = reinterpret_cast<std::uintptr_t>(to);
        auto aligned_end = end_addr & ~(page_size - 1);
        if (aligned_end > aligned + page_size) {
            // Keep the last page (may still be read by the next chunk).
            ::madvise(reinterpret_cast<void*>(aligned), aligned_end - aligned - page_size,
                      MADV_DONTNEED);
        }
#else
        (void)from;
        (void)to;
#endif
    }

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

/// Parses a "YYYY-MM-DD" calendar date into days-since-epoch, matching the
/// `date"YYYY-MM-DD"` literal semantics in the parser (proleptic Gregorian,
/// signed days since 1970-01-01).
inline auto csv_try_date(std::string_view sv, std::int32_t& out) -> bool {
    if (sv.size() != 10 || sv[4] != '-' || sv[7] != '-') {
        return false;
    }
    auto parse_int = [](std::string_view part) -> std::optional<int> {
        int value = 0;
        const char* end = part.data() + part.size();
        auto result = std::from_chars(part.data(), end, value);
        if (result.ec != std::errc() || result.ptr != end) {
            return std::nullopt;
        }
        return value;
    };
    auto year = parse_int(sv.substr(0, 4));
    auto month = parse_int(sv.substr(5, 2));
    auto day = parse_int(sv.substr(8, 2));
    if (!year || !month || !day) {
        return false;
    }
    using namespace std::chrono;
    const year_month_day ymd{std::chrono::year{*year},
                             std::chrono::month{static_cast<unsigned>(*month)},
                             std::chrono::day{static_cast<unsigned>(*day)}};
    if (!ymd.ok()) {
        return false;
    }
    const sys_days days_since = sys_days{ymd};
    const auto days = days_since.time_since_epoch().count();
    if (days < std::numeric_limits<std::int32_t>::min() ||
        days > std::numeric_limits<std::int32_t>::max()) {
        return false;
    }
    out = static_cast<std::int32_t>(days);
    return true;
}

/// Pull-based chunked CSV source. Requires a fully-specified schema with
/// no Infer columns, no null handling, no header. Parses rows from the
/// mmapped source into per-chunk builders; Categorical columns share a
/// single dictionary + index across all chunks so codes are consistent
/// when `MaterializeOperator` concatenates them.
class ChunkedCsvSourceOperator final : public ibex::runtime::Operator {
   public:
    ChunkedCsvSourceOperator(const std::string& path, std::vector<std::string> col_names,
                             std::vector<CsvColumnKind> col_kinds, char delimiter,
                             std::size_t rows_per_chunk,
                             std::vector<ibex::DecimalType> col_decimals = {})
        : source_(std::make_unique<CsvSource>(path)),
          pos_(source_->data()),
          end_(source_->data() + source_->size()),
          col_names_(std::move(col_names)),
          col_kinds_(std::move(col_kinds)),
          col_decimals_(std::move(col_decimals)),
          delimiter_(delimiter),
          rows_per_chunk_(rows_per_chunk) {
        col_decimals_.resize(col_kinds_.size());
        shared_dicts_.resize(col_kinds_.size());
        shared_indices_.resize(col_kinds_.size());
        for (std::size_t c = 0; c < col_kinds_.size(); ++c) {
            if (col_kinds_[c] == CsvColumnKind::Categorical) {
                shared_dicts_[c] = std::make_shared<std::vector<std::string>>();
                shared_indices_[c] = std::make_shared<ibex::Column<ibex::Categorical>::index_map>();
            }
        }
    }

    auto next() -> std::expected<std::optional<ibex::runtime::Chunk>, std::string> override {
        if (pos_ >= end_ && emitted_) {
            return std::optional<ibex::runtime::Chunk>{};
        }

        const std::size_t n_cols = col_names_.size();
        std::vector<ibex::Column<std::int64_t>> int_cols(n_cols);
        std::vector<ibex::Column<double>> double_cols(n_cols);
        std::vector<ibex::Column<std::string>> string_cols(n_cols);
        std::vector<ibex::Column<ibex::Categorical>::codes_storage> cat_codes(n_cols);
        std::vector<ibex::Column<ibex::Date>> date_cols(n_cols);
        std::vector<ibex::Column<ibex::Decimal>> decimal_cols(n_cols);

        for (std::size_t c = 0; c < n_cols; ++c) {
            switch (col_kinds_[c]) {
                case CsvColumnKind::Decimal: {
                    ibex::ColumnMeta meta;
                    meta.decimal = col_decimals_[c];
                    decimal_cols[c].set_meta(meta);
                    decimal_cols[c].reserve(rows_per_chunk_);
                    break;
                }
                case CsvColumnKind::Int:
                    int_cols[c].reserve(rows_per_chunk_);
                    break;
                case CsvColumnKind::Double:
                    double_cols[c].reserve(rows_per_chunk_);
                    break;
                case CsvColumnKind::String:
                    string_cols[c].reserve(rows_per_chunk_);
                    break;
                case CsvColumnKind::Categorical:
                    cat_codes[c].reserve(rows_per_chunk_);
                    break;
                case CsvColumnKind::Date:
                    date_cols[c].reserve(rows_per_chunk_);
                    break;
                case CsvColumnKind::Infer:
                    return std::unexpected(
                        "ChunkedCsvSourceOperator: schema must not contain Infer columns");
            }
        }

        std::vector<std::string_view> row_buf;
        row_buf.reserve(n_cols);
        std::deque<std::string> escape_storage;
        std::size_t rows_read = 0;
        const char* chunk_start = pos_;

        while (rows_read < rows_per_chunk_ &&
               csv_parse_row(pos_, end_, delimiter_, row_buf, escape_storage)) {
            const std::size_t row_cols = row_buf.size();
            for (std::size_t c = 0; c < n_cols; ++c) {
                const auto sv = c < row_cols ? row_buf[c] : std::string_view{};
                switch (col_kinds_[c]) {
                    case CsvColumnKind::Decimal: {
                        auto units = ibex::decimal::parse(sv, col_decimals_[c]);
                        if (!units) {
                            return std::unexpected(
                                "csv::read: column '" + col_names_[c] + "' row " +
                                std::to_string(total_rows_ + rows_read) + ": " + units.error());
                        }
                        decimal_cols[c].push_back(ibex::Decimal{*units});
                        break;
                    }
                    case CsvColumnKind::Int: {
                        std::int64_t iv{};
                        if (!csv_try_int(sv, iv)) {
                            return std::unexpected("csv::read: column '" + col_names_[c] +
                                                   "' row " +
                                                   std::to_string(total_rows_ + rows_read) +
                                                   " failed to parse as i64");
                        }
                        int_cols[c].push_back(iv);
                        break;
                    }
                    case CsvColumnKind::Double: {
                        double dv{};
                        if (!csv_try_double(sv, dv)) {
                            return std::unexpected("csv::read: column '" + col_names_[c] +
                                                   "' row " +
                                                   std::to_string(total_rows_ + rows_read) +
                                                   " failed to parse as f64");
                        }
                        double_cols[c].push_back(dv);
                        break;
                    }
                    case CsvColumnKind::String:
                        string_cols[c].push_back(sv);
                        break;
                    case CsvColumnKind::Categorical: {
                        auto& idx = *shared_indices_[c];
                        auto& dict = *shared_dicts_[c];
                        using code_type = ibex::Column<ibex::Categorical>::code_type;
                        code_type code{};
                        auto it = idx.find(sv);
                        if (it != idx.end()) {
                            code = it->second;
                        } else {
                            code = static_cast<code_type>(dict.size());
                            dict.emplace_back(sv);
                            idx.emplace(dict.back(), code);
                        }
                        cat_codes[c].push_back(code);
                        break;
                    }
                    case CsvColumnKind::Date: {
                        std::int32_t dv{};
                        if (!csv_try_date(sv, dv)) {
                            return std::unexpected(
                                "csv::read: column '" + col_names_[c] + "' row " +
                                std::to_string(total_rows_ + rows_read) +
                                " failed to parse as date (expected YYYY-MM-DD)");
                        }
                        date_cols[c].push_back(ibex::Date{dv});
                        break;
                    }
                    case CsvColumnKind::Infer:
                        break;
                }
            }
            ++rows_read;
            escape_storage.clear();
        }

        source_->advise_dontneed(chunk_start, pos_);

        if (rows_read == 0 && emitted_) {
            return std::optional<ibex::runtime::Chunk>{};
        }

        ibex::runtime::Chunk chunk;
        chunk.columns.reserve(n_cols);
        for (std::size_t c = 0; c < n_cols; ++c) {
            ibex::runtime::ColumnValue column;
            switch (col_kinds_[c]) {
                case CsvColumnKind::Decimal:
                    column = std::move(decimal_cols[c]);
                    break;
                case CsvColumnKind::Int:
                    column = std::move(int_cols[c]);
                    break;
                case CsvColumnKind::Double:
                    column = std::move(double_cols[c]);
                    break;
                case CsvColumnKind::String:
                    column = std::move(string_cols[c]);
                    break;
                case CsvColumnKind::Categorical: {
                    ibex::Column<ibex::Categorical> col{shared_dicts_[c], shared_indices_[c],
                                                        std::move(cat_codes[c])};
                    column = std::move(col);
                    break;
                }
                case CsvColumnKind::Date:
                    column = std::move(date_cols[c]);
                    break;
                case CsvColumnKind::Infer:
                    break;
            }
            chunk.add_column(col_names_[c], std::move(column));
        }
        // The first chunk carries the declared schema even for an empty file.
        emitted_ = true;
        total_rows_ += rows_read;
        return std::optional<ibex::runtime::Chunk>{std::move(chunk)};
    }

   private:
    std::unique_ptr<CsvSource> source_;
    const char* pos_ = nullptr;
    const char* end_ = nullptr;
    std::vector<std::string> col_names_;
    std::vector<CsvColumnKind> col_kinds_;
    std::vector<ibex::DecimalType> col_decimals_;
    char delimiter_;
    std::size_t rows_per_chunk_;
    std::size_t total_rows_ = 0;
    bool emitted_ = false;
    std::vector<std::shared_ptr<std::vector<std::string>>> shared_dicts_;
    std::vector<std::shared_ptr<ibex::Column<ibex::Categorical>::index_map>> shared_indices_;
};

}  // namespace ibex::csv::detail

namespace ibex::csv::detail {

inline auto is_https_uri(std::string_view path) -> bool {
    return path.starts_with("https://") || path.starts_with("http://");
}

#ifdef IBEX_CSV_HAVE_CURL
inline auto write_http_chunk_csv(char* ptr, std::size_t size, std::size_t nmemb, void* userdata)
    -> std::size_t {
    if (size != 0 && nmemb > static_cast<std::size_t>(-1) / size) {
        return 0;
    }
    auto* fp = static_cast<std::FILE*>(userdata);
    // Element size 1 so the return value is the byte count curl compares
    // against `size * nmemb`; a short write signals failure and aborts.
    return std::fwrite(ptr, 1, size * nmemb, fp);
}

/// Open a uniquely-named temp file under `dir` and return it together with its
/// path. Portable replacement for POSIX mkstemp: the C11 "x" mode makes the
/// open fail if the candidate already exists, so the random name + retry loop
/// is race-free. "b" keeps curl's bytes verbatim on Windows.
inline auto csv_open_unique_temp(const std::filesystem::path& dir, std::string& out_path)
    -> std::FILE* {
    std::random_device rd;
    std::mt19937_64 rng(rd());
    for (int attempt = 0; attempt < 100; ++attempt) {
        auto candidate = (dir / ("ibex-csv-" + std::to_string(rng()) + ".tmp")).string();
        if (std::FILE* fp = std::fopen(candidate.c_str(), "wbx"); fp != nullptr) {
            out_path = std::move(candidate);
            return fp;
        }
    }
    return nullptr;
}

/// Download `url` to a fresh temp file and return its path. Caller owns the
/// file and is responsible for removing it. Mirrors the parquet plugin's
/// helper; both resolve libcurl through libs/common/curl_api.hpp.
inline auto csv_download_https_to_temp(std::string_view url) -> std::string {
    const auto& api = ibex::lazy_curl::curl_api("csv::read");
    static const bool curl_initialized = [&api] {
        return api.global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK;
    }();
    if (!curl_initialized) {
        throw std::runtime_error("csv::read: failed to initialize HTTPS client");
    }

    std::error_code ec;
    auto temp_dir = std::filesystem::temp_directory_path(ec);
    if (ec) {
        throw std::runtime_error("csv::read: failed to locate temp directory: " + ec.message());
    }

    std::string temp_path;
    std::FILE* fp = csv_open_unique_temp(temp_dir, temp_path);
    if (fp == nullptr) {
        throw std::runtime_error("csv::read: failed to create temp file in " + temp_dir.string());
    }

    bool fp_open = true;
    auto cleanup = [&] {
        if (fp_open) {
            (void)std::fclose(fp);
            fp_open = false;
        }
        std::error_code rm_ec;
        std::filesystem::remove(temp_path, rm_ec);
    };

    CURL* curl = api.easy_init();
    if (curl == nullptr) {
        cleanup();
        throw std::runtime_error("csv::read: failed to create HTTPS client");
    }

    std::string url_string{url};
    std::array<char, CURL_ERROR_SIZE> error_buffer{};
    api.easy_setopt(curl, CURLOPT_URL, url_string.c_str());
    api.easy_setopt(curl, CURLOPT_ERRORBUFFER, error_buffer.data());
    api.easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_http_chunk_csv);
    api.easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    api.easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    api.easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    api.easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    api.easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    api.easy_setopt(curl, CURLOPT_USERAGENT, "ibex-csv/1");
    // With the Schannel backend (the default on Windows) curl validates against
    // the system certificate store, so these overrides are only consulted when
    // the caller explicitly points at a bundle.
    if (const char* ca_info = std::getenv("CURL_CA_BUNDLE");
        ca_info != nullptr && ca_info[0] != '\0') {
        api.easy_setopt(curl, CURLOPT_CAINFO, ca_info);
    } else if (const char* ssl_cert_file = std::getenv("SSL_CERT_FILE");
               ssl_cert_file != nullptr && ssl_cert_file[0] != '\0') {
        api.easy_setopt(curl, CURLOPT_CAINFO, ssl_cert_file);
    }

    const CURLcode rc = api.easy_perform(curl);
    long response_code = 0;
    api.easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    api.easy_cleanup(curl);

    // fclose flushes any buffered bytes; a non-zero result means the download
    // didn't fully land on disk.
    const int close_result = std::fclose(fp);
    fp_open = false;

    if (rc != CURLE_OK) {
        std::error_code rm_ec;
        std::filesystem::remove(temp_path, rm_ec);
        const std::string detail =
            error_buffer[0] != '\0' ? error_buffer.data() : api.easy_strerror(rc);
        throw std::runtime_error("csv::read: failed to download '" + url_string + "' (" + detail +
                                 ", HTTP " + std::to_string(response_code) + ")");
    }
    if (close_result != 0) {
        std::error_code rm_ec;
        std::filesystem::remove(temp_path, rm_ec);
        throw std::runtime_error("csv::read: failed to finish temp download for '" + url_string +
                                 "'");
    }
    return temp_path;
}
#endif  // IBEX_CSV_HAVE_CURL

struct CsvLocalPath {
    std::string path;
    bool is_temp = false;
    ~CsvLocalPath() {
        if (is_temp) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }
    CsvLocalPath() = default;
    CsvLocalPath(const CsvLocalPath&) = delete;
    auto operator=(const CsvLocalPath&) -> CsvLocalPath& = delete;
    CsvLocalPath(CsvLocalPath&& other) noexcept
        : path(std::move(other.path)), is_temp(other.is_temp) {
        other.is_temp = false;
    }
    auto operator=(CsvLocalPath&& other) noexcept -> CsvLocalPath& {
        if (this != &other) {
            if (is_temp) {
                std::error_code ec;
                std::filesystem::remove(path, ec);
            }
            path = std::move(other.path);
            is_temp = other.is_temp;
            other.is_temp = false;
        }
        return *this;
    }
};

inline auto resolve_csv_path(std::string_view path) -> CsvLocalPath {
    CsvLocalPath out;
    if (is_https_uri(path)) {
#ifdef IBEX_CSV_HAVE_CURL
        out.path = csv_download_https_to_temp(path);
        out.is_temp = true;
        return out;
#else
        throw std::runtime_error(
            "csv::read: HTTPS URLs are not supported in this build (compiled without libcurl)");
#endif
    }
    out.path.assign(path);
    return out;
}

// ── Layout ───────────────────────────────────────────────────────────────────

/// The schema hint for column `idx` named `name`: an entry written
/// `name:type` matches by name, an unnamed entry by position.
inline auto csv_resolve_hint(const CsvReadOptions& options, std::size_t idx,
                             const std::string& name) -> CsvColumnKind {
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
}

/// The Decimal(p, s) of a column whose hint is Decimal, resolved the same way
/// as `csv_resolve_hint`.
inline auto csv_resolve_decimal(const CsvReadOptions& options, std::size_t idx,
                                const std::string& name) -> ibex::DecimalType {
    for (const auto& entry : options.schema.entries) {
        if (entry.name && *entry.name == name) {
            return entry.decimal;
        }
    }
    if (idx < options.schema.entries.size() && !options.schema.entries[idx].name) {
        return options.schema.entries[idx].decimal;
    }
    return ibex::DecimalType{};
}

/// What every read of a CSV file starts from: the column names (from the
/// header, or `col1`, `col2`, ...), each column's declared type, and the first
/// data row, which fixes the column count when there is no header. Shared by
/// the eager reader and the lazy source so both resolve a file identically.
struct CsvLayout {
    /// `has_header` was set but the file holds no row at all.
    bool no_header_row = false;
    std::vector<std::string> names;
    std::vector<CsvColumnKind> hints;
    std::vector<ibex::DecimalType> decimals;
    bool has_first_row = false;
    /// Views into the source buffer, or into `escapes`.
    std::vector<std::string_view> first_row;
    std::size_t first_row_bytes = 0;
    /// Just past the first data row (past the header when there is none).
    const char* body = nullptr;
    /// Unescaped `""` fields of the header and first row. A deque, so the
    /// views into it stay valid as later parses append.
    std::deque<std::string> escapes;
};

inline void csv_read_layout(const char* data, const char* end, const CsvReadOptions& options,
                            CsvLayout& out) {
    const char* pos = data;
    std::vector<std::string_view> row_buf;
    row_buf.reserve(16);
    if (options.has_header) {
        if (!csv_parse_row(pos, end, options.delimiter, row_buf, out.escapes)) {
            out.no_header_row = true;
            out.body = pos;
            return;
        }
        out.names.reserve(row_buf.size());
        for (auto sv : row_buf) {
            out.names.emplace_back(sv);
        }
    }
    const char* first_row_start = pos;
    out.has_first_row = csv_parse_row(pos, end, options.delimiter, row_buf, out.escapes);
    out.first_row_bytes = static_cast<std::size_t>(pos - first_row_start);
    out.body = pos;
    if (out.has_first_row) {
        out.first_row = row_buf;
        if (!options.has_header) {
            out.names.reserve(out.first_row.size());
            for (std::size_t i = 0; i < out.first_row.size(); ++i) {
                out.names.push_back("col" + std::to_string(i + 1));
            }
        }
    } else if (!options.has_header) {
        // With no row to discover columns from, the supplied schema is the
        // layout.
        for (std::size_t i = 0; i < options.schema.entries.size(); ++i) {
            out.names.push_back(
                options.schema.entries[i].name.value_or("col" + std::to_string(i + 1)));
        }
    }
    out.hints.reserve(out.names.size());
    out.decimals.reserve(out.names.size());
    for (std::size_t c = 0; c < out.names.size(); ++c) {
        out.hints.push_back(csv_resolve_hint(options, c, out.names[c]));
        out.decimals.push_back(csv_resolve_decimal(options, c, out.names[c]));
    }
}

// ── Parallel eager read ──────────────────────────────────────────────────────
//
// The serial reader below parses every row on one thread, so `csv::read` took
// the same ~2 s on a 500 MB file at 1 core and at 8 while Polars and DuckDB
// scale to 0.25-0.5 s. This splits the body into byte ranges at newlines and
// parses each range on its own worker, then builds every column from the
// per-range field views, a range per task.
//
// A cut at a newline is only a row boundary if that newline is not inside a
// quoted field. Rather than track quote state up front, the cuts are taken on
// trust and checked afterwards: range 0 starts at a true row boundary, so its
// worker's rows are true rows, and if its last row ends at or before the cut,
// the cut is a true boundary too -- and so on by induction. Any range whose
// last row runs past its cut fails the check, and the whole read falls back to
// the serial path. So does any value a declared type cannot parse, which lets
// the serial path raise its exact error. The parallel path declines; it never
// guesses.
//
// The result is identical to the serial reader's: same types, same nulls, and
// Categorical dictionaries in first-appearance order (each range's dictionary
// is merged in range order).

/// Below this many body bytes the serial reader is fast enough that fanning
/// out costs more than it saves.
inline constexpr std::size_t kCsvParallelMinBytes = std::size_t{8} << 20;
/// The smallest byte range one worker is given.
inline constexpr std::size_t kCsvParallelMinRangeBytes = std::size_t{2} << 20;

/// Reads that took the parallel path, for tests that must prove they did.
inline std::atomic<std::size_t> csv_parallel_reads{0};

/// A size from the environment, or `fallback`. The thresholds are overridable
/// so tests can drive the parallel path with small files and tiny ranges.
inline auto csv_env_size(const char* name, std::size_t fallback) -> std::size_t {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') {
        return fallback;
    }
    std::size_t value = 0;
    const char* raw_end = raw + std::strlen(raw);
    const auto result = std::from_chars(raw, raw_end, value);
    return result.ec == std::errc{} && result.ptr == raw_end ? value : fallback;
}

inline auto csv_skip_blank_lines(const char* pos, const char* end) -> const char* {
    while (pos < end && (*pos == '\n' || *pos == '\r')) {
        ++pos;
    }
    return pos;
}

/// Run `body(i)` for i in [0, tasks) on up to `workers` pool threads.
template <typename Body>
void csv_run_parallel(std::size_t tasks, std::size_t workers, const Body& body) {
    if (workers <= 1 || tasks <= 1) {
        for (std::size_t i = 0; i < tasks; ++i) {
            body(i);
        }
        return;
    }
    std::atomic<std::size_t> next{0};
    ibex::runtime::process_worker_pool()
        .submit(std::min(workers, tasks),
                [&](std::size_t) {
                    for (std::size_t i = next.fetch_add(1, std::memory_order_relaxed); i < tasks;
                         i = next.fetch_add(1, std::memory_order_relaxed)) {
                        body(i);
                    }
                })
        .wait();
}

/// Cut [begin, end) into at most `parts` ranges, each but the first starting
/// right after a '\n'. The cuts are candidates only; see above.
inline auto csv_split_ranges(const char* begin, const char* end, std::size_t parts)
    -> std::vector<std::pair<const char*, const char*>> {
    std::vector<const char*> cuts{begin};
    const auto bytes = static_cast<std::size_t>(end - begin);
    for (std::size_t k = 1; k < parts; ++k) {
        const char* target = begin + (bytes / parts) * k;
        if (target <= cuts.back()) {
            continue;
        }
        const auto* nl = static_cast<const char*>(
            std::memchr(target, '\n', static_cast<std::size_t>(end - target)));
        if (nl == nullptr || nl + 1 >= end) {
            break;
        }
        cuts.push_back(nl + 1);
    }
    std::vector<std::pair<const char*, const char*>> ranges;
    ranges.reserve(cuts.size());
    for (std::size_t k = 0; k < cuts.size(); ++k) {
        ranges.emplace_back(cuts[k], k + 1 < cuts.size() ? cuts[k + 1] : end);
    }
    return ranges;
}

/// The null spec, resolved once for a read: `csv_parse_null_spec` keeps owned
/// strings, which a per-field lookup would have to build a `std::string` to
/// probe -- one allocation per field per row.
struct CsvNullSet {
    bool if_empty = false;
    robin_hood::unordered_flat_set<std::string_view> tokens;

    explicit CsvNullSet(const CsvReadOptions& options) : if_empty(options.null_if_empty) {
        for (const auto& token : options.null_tokens) {
            tokens.insert(std::string_view{token});
        }
    }
    [[nodiscard]] auto active() const noexcept -> bool { return if_empty || !tokens.empty(); }
    [[nodiscard]] auto is_null(std::string_view sv) const -> bool {
        return (if_empty && sv.empty()) || (!tokens.empty() && tokens.contains(sv));
    }
};

/// Where one column's fields go while a range is parsed. Numeric columns --
/// declared, or inferred from a sample -- parse straight into a typed buffer;
/// everything else keeps its field views and is built afterwards exactly as
/// the serial reader builds it.
enum class CsvSinkMode : std::uint8_t {
    Views,
    InferInt,     ///< inferred: Int64 so far; may widen to InferDouble
    InferDouble,  ///< inferred: Double
    Int,          ///< declared i64
    Double,       ///< declared f64
    Date,         ///< declared date
    Decimal,      ///< declared decimal(p,s)
};

struct CsvColumnSink {
    CsvSinkMode mode = CsvSinkMode::Views;
    std::vector<std::string_view> views;
    ibex::Column<std::int64_t> ints;
    ibex::Column<double> doubles;
    ibex::Column<ibex::Date> dates;
    ibex::Column<ibex::Decimal> decimals;
    ibex::DecimalType decimal_type{};
    /// Range-local rows that are null (or, for an inferred column, empty).
    std::vector<std::uint32_t> invalid;
    bool any_valid = false;
    /// An inferred numeric column met a value that is no number at all; the
    /// column is re-read as views in a second pass.
    bool needs_text = false;
    /// A declared type rejected a value; the whole read falls back to the
    /// serial reader, which raises the error.
    bool rejected = false;

    void reserve(std::size_t rows) {
        switch (mode) {
            case CsvSinkMode::Views:
                views.reserve(rows);
                break;
            case CsvSinkMode::InferInt:
            case CsvSinkMode::Int:
                ints.reserve(rows);
                break;
            case CsvSinkMode::InferDouble:
            case CsvSinkMode::Double:
                doubles.reserve(rows);
                break;
            case CsvSinkMode::Date:
                dates.reserve(rows);
                break;
            case CsvSinkMode::Decimal:
                decimals.reserve(rows);
                break;
        }
    }

    void push_invalid(std::uint32_t row) {
        invalid.push_back(row);
        switch (mode) {
            case CsvSinkMode::InferInt:
            case CsvSinkMode::Int:
                ints.push_back(0);
                break;
            case CsvSinkMode::InferDouble:
            case CsvSinkMode::Double:
                doubles.push_back(0.0);
                break;
            case CsvSinkMode::Date:
                dates.push_back(ibex::Date{});
                break;
            case CsvSinkMode::Decimal:
                decimals.push_back(ibex::Decimal{});
                break;
            case CsvSinkMode::Views:
                break;
        }
    }

    /// Int64 -> Double in place. Exact with respect to the serial reader,
    /// which would parse the same text as a double: both round the integer to
    /// the nearest double.
    void widen_to_double() {
        doubles.reserve(ints.capacity());
        for (std::size_t i = 0; i < ints.size(); ++i) {
            doubles.push_back(static_cast<double>(ints[i]));
        }
        ints = {};
        mode = CsvSinkMode::InferDouble;
    }

    void push(std::string_view sv, std::uint32_t row, const CsvNullSet& nulls) {
        if (mode == CsvSinkMode::Views) {
            views.push_back(sv);
            return;
        }
        if (needs_text || rejected) {
            return;
        }
        if (nulls.active() && nulls.is_null(sv)) {
            push_invalid(row);
            return;
        }
        switch (mode) {
            case CsvSinkMode::InferInt: {
                if (sv.empty()) {
                    push_invalid(row);
                    return;
                }
                std::int64_t iv{};
                if (csv_try_int(sv, iv)) {
                    ints.push_back(iv);
                    any_valid = true;
                    return;
                }
                double dv{};
                if (csv_try_double(sv, dv)) {
                    widen_to_double();
                    doubles.push_back(dv);
                    any_valid = true;
                    return;
                }
                needs_text = true;
                return;
            }
            case CsvSinkMode::InferDouble: {
                if (sv.empty()) {
                    push_invalid(row);
                    return;
                }
                double dv{};
                if (csv_try_double(sv, dv)) {
                    doubles.push_back(dv);
                    any_valid = true;
                    return;
                }
                needs_text = true;
                return;
            }
            case CsvSinkMode::Int: {
                std::int64_t iv{};
                rejected = !csv_try_int(sv, iv);
                ints.push_back(iv);
                return;
            }
            case CsvSinkMode::Double: {
                double dv{};
                rejected = !csv_try_double(sv, dv);
                doubles.push_back(dv);
                return;
            }
            case CsvSinkMode::Date: {
                std::int32_t days{};
                rejected = !csv_try_date(sv, days);
                dates.push_back(ibex::Date{days});
                return;
            }
            case CsvSinkMode::Decimal: {
                auto units = ibex::decimal::parse(sv, decimal_type);
                rejected = !units.has_value();
                decimals.push_back(ibex::Decimal{units.value_or(ibex::Int128{})});
                return;
            }
            case CsvSinkMode::Views:
                return;
        }
    }
};

/// One range's rows, one sink per column. Views point into the source buffer,
/// or into `escapes` for fields that held a `""`.
struct CsvRangeRows {
    std::vector<CsvColumnSink> sinks;
    std::deque<std::string> escapes;
    std::deque<std::string> reread_escapes;
    std::size_t rows = 0;
    bool consistent = true;
};

/// Parse the rows that START in [begin, range_end) into `out.sinks`. A row
/// may run past `range_end` -- that is what `consistent` reports. Only the
/// columns with `take[c]` set are stored.
inline void csv_parse_range(const char* begin, const char* range_end, const char* file_end,
                            char delim, std::size_t reserve_rows, const CsvNullSet& nulls,
                            const std::vector<std::uint8_t>& take, CsvRangeRows& out) {
    const std::size_t n_cols = out.sinks.size();
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (take[c] != 0) {
            out.sinks[c].reserve(reserve_rows);
        }
    }
    std::vector<std::string_view> fields;
    fields.reserve(n_cols);
    const char* pos = begin;
    const char* last_end = begin;
    auto row = static_cast<std::uint32_t>(out.rows);
    while (true) {
        pos = csv_skip_blank_lines(pos, file_end);
        if (pos >= range_end || !csv_parse_row(pos, file_end, delim, fields, out.escapes)) {
            break;
        }
        last_end = pos;
        const std::size_t row_cols = fields.size();
        for (std::size_t c = 0; c < n_cols; ++c) {
            if (take[c] != 0) {
                out.sinks[c].push(c < row_cols ? fields[c] : std::string_view{}, row, nulls);
            }
        }
        ++row;
    }
    out.rows = row;
    out.consistent = last_end <= range_end;
}

/// Pick each column's sink from its declared type, or, for an inferred
/// column, from the first rows: Int64 if every non-null, non-empty sampled
/// value is an integer, Double if every one is a number, Views otherwise.
/// A wrong guess costs time, never correctness: a numeric sink that meets
/// text sends its column back through the views path.
inline auto csv_choose_sinks(const char* body, const char* end, char delim,
                             const std::vector<std::string_view>& first_row,
                             const std::vector<CsvColumnKind>& hints, const CsvNullSet& nulls)
    -> std::vector<CsvSinkMode> {
    constexpr std::size_t kSampleRows = 1000;
    const std::size_t n_cols = hints.size();
    std::vector<std::uint8_t> all_int(n_cols, 1);
    std::vector<std::uint8_t> all_double(n_cols, 1);
    std::vector<std::uint8_t> any_valid(n_cols, 0);
    const auto look = [&](const std::vector<std::string_view>& fields) {
        for (std::size_t c = 0; c < n_cols; ++c) {
            const auto sv = c < fields.size() ? fields[c] : std::string_view{};
            if (sv.empty() || (nulls.active() && nulls.is_null(sv))) {
                continue;
            }
            any_valid[c] = 1;
            std::int64_t iv{};
            double dv{};
            if (all_int[c] != 0 && !csv_try_int(sv, iv)) {
                all_int[c] = 0;
            }
            if (all_double[c] != 0 && !csv_try_double(sv, dv)) {
                all_double[c] = 0;
            }
        }
    };
    look(first_row);
    std::deque<std::string> escapes;
    std::vector<std::string_view> fields;
    const char* pos = body;
    for (std::size_t r = 0; r < kSampleRows && csv_parse_row(pos, end, delim, fields, escapes);
         ++r) {
        look(fields);
    }
    std::vector<CsvSinkMode> modes(n_cols, CsvSinkMode::Views);
    for (std::size_t c = 0; c < n_cols; ++c) {
        switch (hints[c]) {
            case CsvColumnKind::Int:
                modes[c] = CsvSinkMode::Int;
                break;
            case CsvColumnKind::Double:
                modes[c] = CsvSinkMode::Double;
                break;
            case CsvColumnKind::Date:
                modes[c] = CsvSinkMode::Date;
                break;
            case CsvColumnKind::Decimal:
                modes[c] = CsvSinkMode::Decimal;
                break;
            case CsvColumnKind::String:
            case CsvColumnKind::Categorical:
                break;
            case CsvColumnKind::Infer:
                if (any_valid[c] != 0 && all_int[c] != 0) {
                    modes[c] = CsvSinkMode::InferInt;
                } else if (any_valid[c] != 0 && all_double[c] != 0) {
                    modes[c] = CsvSinkMode::InferDouble;
                }
                break;
        }
    }
    return modes;
}

template <typename T>
void csv_size_for_overwrite(ibex::Column<T>& column, std::size_t n) {
    if constexpr (ibex::detail::overwrite_safe_v<T>) {
        column.resize_for_overwrite(n);
    } else {
        column.resize(n);
    }
}

/// Everything a parallel column build reads: per-range views and where each
/// range starts in the column.
struct CsvSegments {
    std::vector<const std::vector<std::string_view>*> views;
    std::vector<std::size_t> offsets;
    std::size_t rows = 0;
    std::size_t workers = 1;
};

/// A Categorical column built range by range: each range interns its own
/// values in first-appearance order, then the dictionaries are merged in
/// range order, which reproduces the serial first-appearance order exactly.
/// `limit` caps the distinct count (0 = none); exceeding it returns nullopt.
/// Rows flagged in `null_flags` take code 0 and add nothing, as in the serial
/// reader.
inline auto csv_build_categorical(const CsvSegments& seg, const std::uint8_t* null_flags,
                                  std::size_t limit)
    -> std::optional<ibex::Column<ibex::Categorical>> {
    using code_type = ibex::Column<ibex::Categorical>::code_type;
    // One dictionary per WORKER, over a contiguous run of ranges, not one per
    // range: the merge below is serial and costs one insert per local entry,
    // and a high-cardinality column repeats most of its values in every range.
    // h2o's id3 (100k values) merged ~3M entries from 32 ranges; from 8
    // groups it merges ~0.8M.
    const std::size_t k = seg.views.size();
    const std::size_t groups = std::min(k, std::max<std::size_t>(seg.workers, 1));
    const auto group_begin = [&](std::size_t g) { return (k * g) / groups; };
    ibex::Column<ibex::Categorical>::codes_storage codes;
    codes.resize(seg.rows);
    std::vector<std::vector<std::string_view>> local_dicts(groups);
    std::atomic<bool> over{false};
    csv_run_parallel(groups, seg.workers, [&](std::size_t g) {
        robin_hood::unordered_flat_map<std::string_view, code_type> index;
        auto& dict = local_dicts[g];
        for (std::size_t r = group_begin(g); r < group_begin(g + 1); ++r) {
            const auto& vals = *seg.views[r];
            const std::size_t off = seg.offsets[r];
            for (std::size_t i = 0; i < vals.size(); ++i) {
                if (null_flags != nullptr && null_flags[off + i] != 0) {
                    codes[off + i] = -1;
                    continue;
                }
                const auto [it, inserted] =
                    index.try_emplace(vals[i], static_cast<code_type>(dict.size()));
                if (inserted) {
                    dict.push_back(vals[i]);
                    if (limit != 0 && dict.size() > limit) {
                        over.store(true, std::memory_order_relaxed);
                        return;
                    }
                }
                codes[off + i] = it->second;
            }
        }
    });
    if (over.load()) {
        return std::nullopt;
    }
    std::vector<std::string> dict;
    robin_hood::unordered_flat_map<std::string_view, code_type> global;
    global.reserve(local_dicts[0].size());
    std::vector<std::vector<code_type>> remap(groups);
    for (std::size_t g = 0; g < groups; ++g) {
        remap[g].reserve(local_dicts[g].size());
        for (const auto value : local_dicts[g]) {
            const auto [it, inserted] =
                global.try_emplace(value, static_cast<code_type>(dict.size()));
            if (inserted) {
                dict.emplace_back(value);
                if (limit != 0 && dict.size() > limit) {
                    return std::nullopt;
                }
            }
            remap[g].push_back(it->second);
        }
    }
    csv_run_parallel(k, seg.workers, [&](std::size_t r) {
        std::size_t g = 0;
        while (group_begin(g + 1) <= r) {
            ++g;
        }
        const std::size_t off = seg.offsets[r];
        const std::size_t n = seg.views[r]->size();
        const auto& map = remap[g];
        for (std::size_t i = 0; i < n; ++i) {
            const code_type local = codes[off + i];
            codes[off + i] = local < 0 ? 0 : map[static_cast<std::size_t>(local)];
        }
    });
    return ibex::Column<ibex::Categorical>(std::move(dict), std::move(codes));
}

/// A String column built range by range and concatenated in order. Rows
/// flagged in `null_flags` read as "".
inline auto csv_build_strings(const CsvSegments& seg, const std::uint8_t* null_flags)
    -> ibex::Column<std::string> {
    const std::size_t k = seg.views.size();
    std::vector<ibex::Column<std::string>> parts(k);
    csv_run_parallel(k, seg.workers, [&](std::size_t r) {
        const auto& vals = *seg.views[r];
        const std::size_t off = seg.offsets[r];
        auto& part = parts[r];
        part.reserve(vals.size());
        for (std::size_t i = 0; i < vals.size(); ++i) {
            part.push_back(null_flags != nullptr && null_flags[off + i] != 0 ? std::string_view{}
                                                                             : vals[i]);
        }
    });
    ibex::Column<std::string> out = std::move(parts[0]);
    for (std::size_t r = 1; r < k; ++r) {
        out.append(parts[r]);
    }
    return out;
}

/// Parse every non-null row of a column straight into a pre-sized buffer.
/// `parse(view, slot) -> bool`; a null row gets `null_value`. Returns false if
/// any value fails, leaving the error to the serial reader.
template <typename T, typename Parse>
auto csv_build_fixed(const CsvSegments& seg, const std::uint8_t* null_flags, T null_value,
                     ibex::Column<T>& out, const Parse& parse) -> bool {
    csv_size_for_overwrite(out, seg.rows);
    T* data = out.data();
    std::atomic<bool> bad{false};
    csv_run_parallel(seg.views.size(), seg.workers, [&](std::size_t r) {
        const auto& vals = *seg.views[r];
        const std::size_t off = seg.offsets[r];
        for (std::size_t i = 0; i < vals.size(); ++i) {
            if (null_flags != nullptr && null_flags[off + i] != 0) {
                data[off + i] = null_value;
                continue;
            }
            if (!parse(vals[i], data[off + i])) {
                bad.store(true, std::memory_order_relaxed);
                return;
            }
        }
    });
    return !bad.load();
}

/// The inference probe for one numeric type: every non-null value parses, or
/// is empty (an empty field reads as null in an otherwise-numeric column).
/// Mirrors the serial probe; `empties` gets the global rows that were empty.
template <typename T, typename Parse>
auto csv_probe_numeric(const CsvSegments& seg, const std::uint8_t* null_flags, ibex::Column<T>& out,
                       std::vector<std::size_t>& empties, bool& any_valid, const Parse& parse)
    -> bool {
    const std::size_t k = seg.views.size();
    csv_size_for_overwrite(out, seg.rows);
    T* data = out.data();
    std::atomic<bool> bad{false};
    std::vector<std::uint8_t> valid_in(k, 0);
    std::vector<std::vector<std::size_t>> empty_in(k);
    csv_run_parallel(k, seg.workers, [&](std::size_t r) {
        const auto& vals = *seg.views[r];
        const std::size_t off = seg.offsets[r];
        for (std::size_t i = 0; i < vals.size(); ++i) {
            if (bad.load(std::memory_order_relaxed)) {
                return;
            }
            if (null_flags != nullptr && null_flags[off + i] != 0) {
                data[off + i] = T{};
                continue;
            }
            if (vals[i].empty()) {
                empty_in[r].push_back(off + i);
                data[off + i] = T{};
                continue;
            }
            if (!parse(vals[i], data[off + i])) {
                bad.store(true, std::memory_order_relaxed);
                return;
            }
            valid_in[r] = 1;
        }
    });
    if (bad.load()) {
        return false;
    }
    any_valid = std::ranges::any_of(valid_in, [](std::uint8_t v) { return v != 0; });
    for (auto& e : empty_in) {
        empties.insert(empties.end(), e.begin(), e.end());
    }
    return true;
}

/// Build one column from its per-range field views, with the serial
/// reader's semantics, and add it to `table`. Returns false when a declared
/// type rejects a value.
inline auto csv_build_column_from_views(ibex::runtime::Table& table, const CsvSegments& seg,
                                        const std::string& name, CsvColumnKind hint,
                                        ibex::DecimalType decimal, const CsvNullSet& null_set)
    -> bool {
    const std::size_t n = seg.rows;
    std::vector<std::uint8_t> null_flags;
    ibex::runtime::ValidityBitmap validity;
    bool has_nulls = false;
    if (null_set.active()) {
        null_flags.assign(n, 0);
        std::atomic<bool> any{false};
        csv_run_parallel(seg.views.size(), seg.workers, [&](std::size_t r) {
            const auto& vals = *seg.views[r];
            const std::size_t off = seg.offsets[r];
            bool seen = false;
            for (std::size_t i = 0; i < vals.size(); ++i) {
                if (null_set.is_null(vals[i])) {
                    null_flags[off + i] = 1;
                    seen = true;
                }
            }
            if (seen) {
                any.store(true, std::memory_order_relaxed);
            }
        });
        has_nulls = any.load();
        if (has_nulls) {
            validity.assign(n, true);
            for (std::size_t i = 0; i < n; ++i) {
                if (null_flags[i] != 0) {
                    validity.set(i, false);
                }
            }
        }
    }
    const std::uint8_t* nulls = has_nulls ? null_flags.data() : nullptr;
    auto add = [&](auto&& column) {
        if (has_nulls) {
            table.add_column(name, std::forward<decltype(column)>(column), validity);
        } else {
            table.add_column(name, std::forward<decltype(column)>(column));
        }
    };

    if (hint != CsvColumnKind::Infer) {
        switch (hint) {
            case CsvColumnKind::Int: {
                ibex::Column<std::int64_t> col;
                if (!csv_build_fixed<std::int64_t>(seg, nulls, 0, col, csv_try_int)) {
                    return false;
                }
                add(std::move(col));
                return true;
            }
            case CsvColumnKind::Double: {
                ibex::Column<double> col;
                if (!csv_build_fixed<double>(seg, nulls, 0.0, col, csv_try_double)) {
                    return false;
                }
                add(std::move(col));
                return true;
            }
            case CsvColumnKind::Decimal: {
                ibex::Column<ibex::Decimal> col;
                ibex::ColumnMeta meta;
                meta.decimal = decimal;
                col.set_meta(meta);
                const bool ok = csv_build_fixed<ibex::Decimal>(
                    seg, nulls, ibex::Decimal{}, col,
                    [&](std::string_view sv, ibex::Decimal& slot) {
                        auto units = ibex::decimal::parse(sv, decimal);
                        if (!units) {
                            return false;
                        }
                        slot = ibex::Decimal{*units};
                        return true;
                    });
                if (!ok) {
                    return false;
                }
                add(std::move(col));
                return true;
            }
            case CsvColumnKind::Date: {
                ibex::Column<ibex::Date> col;
                const bool ok = csv_build_fixed<ibex::Date>(
                    seg, nulls, ibex::Date{}, col, [](std::string_view sv, ibex::Date& slot) {
                        std::int32_t days{};
                        if (!csv_try_date(sv, days)) {
                            return false;
                        }
                        slot = ibex::Date{days};
                        return true;
                    });
                if (!ok) {
                    return false;
                }
                add(std::move(col));
                return true;
            }
            case CsvColumnKind::Categorical:
                add(*csv_build_categorical(seg, nulls, 0));
                return true;
            case CsvColumnKind::String:
            case CsvColumnKind::Infer:
                add(csv_build_strings(seg, nulls));
                return true;
        }
    }

    // Inference, in the serial reader's order: Int64, then Double, then
    // String -- Categorical when the column has no nulls and few enough
    // distinct values.
    const auto numeric = [&](auto& column, const auto& parse) -> bool {
        std::vector<std::size_t> empties;
        bool any_valid = false;
        using T = typename std::decay_t<decltype(column)>::value_type;
        if (!csv_probe_numeric<T>(seg, nulls, column, empties, any_valid, parse) || !any_valid) {
            return false;
        }
        if (!empties.empty()) {
            ibex::runtime::ValidityBitmap probe =
                has_nulls ? validity : ibex::runtime::ValidityBitmap{};
            if (!has_nulls) {
                probe.assign(n, true);
            }
            for (const std::size_t row : empties) {
                probe.set(row, false);
            }
            table.add_column(name, std::move(column), std::move(probe));
        } else if (has_nulls) {
            table.add_column(name, std::move(column), validity);
        } else {
            table.add_column(name, std::move(column));
        }
        return true;
    };
    if (n > 0) {
        ibex::Column<std::int64_t> ints;
        if (numeric(ints, csv_try_int)) {
            return true;
        }
        ibex::Column<double> doubles;
        if (numeric(doubles, csv_try_double)) {
            return true;
        }
    }
    if (has_nulls) {
        add(csv_build_strings(seg, nulls));
        return true;
    }
    if (n > 0) {
        if (auto cat = csv_build_categorical(seg, nullptr, ibex::categorical_promotion_limit(n))) {
            table.add_column(name, std::move(*cat));
            return true;
        }
    }
    table.add_column(name, csv_build_strings(seg, nullptr));
    return true;
}

/// Concatenate one typed column out of the ranges' sinks into its final
/// buffer, a range per task. `get(sink)` names the range's buffer.
template <typename T, typename Get>
auto csv_concat_sinks(const std::vector<CsvRangeRows>& ranges, std::size_t c,
                      const std::vector<std::size_t>& offsets, std::size_t rows,
                      std::size_t workers, const Get& get) -> ibex::Column<T> {
    ibex::Column<T> out;
    csv_size_for_overwrite(out, rows);
    T* data = out.data();
    csv_run_parallel(ranges.size(), workers, [&](std::size_t r) {
        const CsvColumnSink& sink = ranges[r].sinks[c];
        const auto& src = get(sink);
        std::copy_n(src.data(), src.size(), data + offsets[r]);
    });
    return out;
}

/// The parallel read of `layout.body .. end`, after `csv_read_layout` has
/// parsed the header and the first data row.
///
/// The eager reader calls this with no `take` and no `forced_workers`: it then
/// declines (nullopt) whenever the serial reader should run instead -- too
/// small, one core, a cut inside a quoted field, or a value a declared type
/// rejects.
///
/// The lazy source calls it with `take` (only those columns are stored and
/// returned; the rest are tokenized past) and `forced_workers` (its query's
/// budget, possibly 1). It never wants the serial reader, which builds every
/// column: a failed boundary check retries as one range instead, so the only
/// nullopt it can see is a rejected value.
inline auto csv_read_parallel(const char* end, const CsvLayout& layout,
                              const CsvReadOptions& options,
                              const std::vector<std::uint8_t>* take = nullptr,
                              std::size_t forced_workers = 0, bool single_range = false)
    -> std::optional<ibex::runtime::Table> {
    const char* body = layout.body;
    const auto& first_row = layout.first_row;
    const auto first_row_bytes = layout.first_row_bytes;
    const auto& col_names = layout.names;
    const auto& hints = layout.hints;
    const auto& decimals = layout.decimals;
    const auto bytes = static_cast<std::size_t>(end - body);
    std::size_t workers = forced_workers;
    if (forced_workers == 0) {
        if (bytes < csv_env_size("IBEX_CSV_PARALLEL_MIN_BYTES", kCsvParallelMinBytes) ||
            std::getenv("IBEX_CSV_SERIAL") != nullptr) {
            return std::nullopt;
        }
        workers = std::min(ibex::runtime::compute_thread_count(),
                           ibex::runtime::process_worker_pool().size());
        if (workers < 2) {
            return std::nullopt;
        }
    }
    const std::size_t n_cols = col_names.size();
    const std::vector<std::uint8_t> take_mask =
        take != nullptr ? *take : std::vector<std::uint8_t>(n_cols, 1);
    const CsvNullSet null_set(options);
    const auto modes = csv_choose_sinks(body, end, options.delimiter, first_row, hints, null_set);

    // A few ranges per worker, so one dense range cannot strand the rest.
    const std::size_t range_bytes = std::max<std::size_t>(
        csv_env_size("IBEX_CSV_PARALLEL_RANGE_BYTES", kCsvParallelMinRangeBytes), 1);
    const std::size_t parts =
        single_range ? 1 : std::clamp<std::size_t>(bytes / range_bytes, 1, workers * 4);
    const auto cuts = csv_split_ranges(body, end, parts);
    std::vector<CsvRangeRows> ranges(cuts.size());
    for (auto& range : ranges) {
        range.sinks.resize(n_cols);
        for (std::size_t c = 0; c < n_cols; ++c) {
            range.sinks[c].mode = modes[c];
            range.sinks[c].decimal_type = decimals[c];
        }
    }
    // The first data row was parsed to learn the column count; it leads range 0.
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (take_mask[c] != 0) {
            ranges[0].sinks[c].push(c < first_row.size() ? first_row[c] : std::string_view{}, 0,
                                    null_set);
        }
    }
    ranges[0].rows = 1;
    const std::size_t row_bytes = std::max<std::size_t>(first_row_bytes, 1);
    const auto reserve_for = [&](std::size_t r) {
        const auto span = static_cast<std::size_t>(cuts[r].second - cuts[r].first) / row_bytes;
        return span + span / 8 + 16;
    };
    csv_run_parallel(cuts.size(), workers, [&](std::size_t r) {
        csv_parse_range(cuts[r].first, cuts[r].second, end, options.delimiter, reserve_for(r),
                        null_set, take_mask, ranges[r]);
    });

    std::vector<std::size_t> offsets(ranges.size());
    std::size_t rows = 0;
    for (std::size_t r = 0; r < ranges.size(); ++r) {
        if (!ranges[r].consistent) {
            if (forced_workers != 0 && !single_range) {
                return csv_read_parallel(end, layout, options, take, forced_workers, true);
            }
            return std::nullopt;
        }
        offsets[r] = rows;
        rows += ranges[r].rows;
    }

    // Per column: a declared type that rejected a value sends the whole read
    // to the serial path; an inferred numeric that met text, or that saw no
    // value at all, is re-read as views and built like any text column.
    std::vector<std::uint8_t> reread(n_cols, 0);
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (take_mask[c] == 0 || modes[c] == CsvSinkMode::Views) {
            continue;
        }
        bool any_valid = false;
        for (const auto& range : ranges) {
            const auto& sink = range.sinks[c];
            if (sink.rejected) {
                return std::nullopt;
            }
            reread[c] |= sink.needs_text ? 1U : 0U;
            any_valid = any_valid || sink.any_valid;
        }
        const bool inferred =
            modes[c] == CsvSinkMode::InferInt || modes[c] == CsvSinkMode::InferDouble;
        if (inferred && !any_valid) {
            reread[c] = 1;
        }
    }
    if (std::ranges::any_of(reread, [](std::uint8_t v) { return v != 0; })) {
        for (auto& range : ranges) {
            for (std::size_t c = 0; c < n_cols; ++c) {
                if (reread[c] != 0) {
                    range.sinks[c] = CsvColumnSink{};  // Views
                }
            }
        }
        csv_run_parallel(cuts.size(), workers, [&](std::size_t r) {
            auto& range = ranges[r];
            CsvRangeRows views;
            views.sinks.resize(n_cols);
            if (r == 0) {
                for (std::size_t c = 0; c < n_cols; ++c) {
                    if (reread[c] != 0) {
                        views.sinks[c].views.push_back(c < first_row.size() ? first_row[c]
                                                                            : std::string_view{});
                    }
                }
                views.rows = 1;
            }
            csv_parse_range(cuts[r].first, cuts[r].second, end, options.delimiter, reserve_for(r),
                            null_set, reread, views);
            for (std::size_t c = 0; c < n_cols; ++c) {
                if (reread[c] != 0) {
                    range.sinks[c].views = std::move(views.sinks[c].views);
                }
            }
            // Escaped fields of the re-read point into its own storage. Moving
            // the deque keeps every element where it is; moving the strings out
            // one by one would not (a short string's bytes live inside it).
            range.reread_escapes = std::move(views.escapes);
        });
    }

    ibex::runtime::Table table;
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (take_mask[c] == 0) {
            continue;
        }
        const auto mode = ranges[0].sinks[c].mode;
        const auto& name = col_names[c];
        if (mode == CsvSinkMode::Views) {
            CsvSegments seg;
            seg.workers = workers;
            for (std::size_t r = 0; r < ranges.size(); ++r) {
                seg.views.push_back(&ranges[r].sinks[c].views);
                seg.offsets.push_back(offsets[r]);
            }
            seg.rows = rows;
            if (!csv_build_column_from_views(table, seg, name, hints[c], decimals[c], null_set)) {
                return std::nullopt;
            }
            continue;
        }
        // Typed sinks: the invalid rows (nulls, and an inferred column's
        // empties) are the validity, exactly as the serial reader assembles it.
        ibex::runtime::ValidityBitmap validity;
        bool any_invalid = false;
        for (std::size_t r = 0; r < ranges.size(); ++r) {
            const auto& invalid = ranges[r].sinks[c].invalid;
            if (invalid.empty()) {
                continue;
            }
            if (!any_invalid) {
                validity.assign(rows, true);
                any_invalid = true;
            }
            for (const auto local : invalid) {
                validity.set(offsets[r] + local, false);
            }
        }
        auto add = [&](auto&& column) {
            if (any_invalid) {
                table.add_column(name, std::forward<decltype(column)>(column), std::move(validity));
            } else {
                table.add_column(name, std::forward<decltype(column)>(column));
            }
        };
        const bool widened = std::ranges::any_of(ranges, [&](const CsvRangeRows& r) {
            return r.sinks[c].mode == CsvSinkMode::InferDouble;
        });
        switch (mode) {
            case CsvSinkMode::InferInt:
            case CsvSinkMode::InferDouble:
                if (widened) {
                    for (auto& range : ranges) {
                        if (range.sinks[c].mode == CsvSinkMode::InferInt) {
                            range.sinks[c].widen_to_double();
                        }
                    }
                    add(csv_concat_sinks<double>(
                        ranges, c, offsets, rows, workers,
                        [](const CsvColumnSink& s) -> const auto& { return s.doubles; }));
                } else {
                    add(csv_concat_sinks<std::int64_t>(
                        ranges, c, offsets, rows, workers,
                        [](const CsvColumnSink& s) -> const auto& { return s.ints; }));
                }
                break;
            case CsvSinkMode::Int:
                add(csv_concat_sinks<std::int64_t>(
                    ranges, c, offsets, rows, workers,
                    [](const CsvColumnSink& s) -> const auto& { return s.ints; }));
                break;
            case CsvSinkMode::Double:
                add(csv_concat_sinks<double>(
                    ranges, c, offsets, rows, workers,
                    [](const CsvColumnSink& s) -> const auto& { return s.doubles; }));
                break;
            case CsvSinkMode::Date:
                add(csv_concat_sinks<ibex::Date>(
                    ranges, c, offsets, rows, workers,
                    [](const CsvColumnSink& s) -> const auto& { return s.dates; }));
                break;
            case CsvSinkMode::Decimal: {
                auto col = csv_concat_sinks<ibex::Decimal>(
                    ranges, c, offsets, rows, workers,
                    [](const CsvColumnSink& s) -> const auto& { return s.decimals; });
                ibex::ColumnMeta meta;
                meta.decimal = decimals[c];
                col.set_meta(meta);
                add(std::move(col));
                break;
            }
            case CsvSinkMode::Views:
                break;
        }
        // Free this column's range buffers before building the next one.
        for (auto& range : ranges) {
            range.sinks[c] = CsvColumnSink{};
        }
    }
    csv_parallel_reads.fetch_add(1, std::memory_order_relaxed);
    return table;
}

inline auto read_csv_with_options(std::string_view path, const CsvReadOptions& options)
    -> ibex::runtime::Table {
    auto local = resolve_csv_path(path);
    const std::string path_string = local.path;
    std::error_code ec;
    const bool exists = std::filesystem::exists(path_string, ec);
    if (ec) {
        throw std::runtime_error("csv::read: failed to inspect path '" + path_string +
                                 "': " + ec.message());
    }
    if (!exists) {
        throw std::runtime_error("csv::read: file not found: '" + std::string{path} + "'");
    }

    CsvSource source(path_string);
    const char* end = source.data() + source.size();
    CsvLayout layout;
    csv_read_layout(source.data(), end, options, layout);
    if (layout.no_header_row) {
        return ibex::runtime::Table{};
    }
    const char* pos = layout.body;
    std::deque<std::string>& escape_storage = layout.escapes;
    std::vector<std::string_view> row_buf;
    row_buf.reserve(16);
    const std::vector<std::string>& col_names = layout.names;
    const std::vector<std::string_view>& first_row = layout.first_row;
    const bool has_first_row = layout.has_first_row;
    const auto resolve_decimal = [&](std::size_t idx, const std::string& name) {
        return csv_resolve_decimal(options, idx, name);
    };
    const auto resolve_hint = [&](std::size_t idx, const std::string& name) {
        return csv_resolve_hint(options, idx, name);
    };

    std::size_t n_cols = col_names.size();
    const std::vector<CsvColumnKind>& resolved_hints = layout.hints;
    bool all_hints_known = has_first_row;
    for (const auto hint : resolved_hints) {
        if (hint == CsvColumnKind::Infer) {
            all_hints_known = false;
        }
    }

    const bool need_null_check = options.null_if_empty || !options.null_tokens.empty();

    if (has_first_row) {
        if (auto table = csv_read_parallel(end, layout, options)) {
            return std::move(*table);
        }
    }

    // Cheap newline count for row reservation.  Quoted newlines are rare
    // enough that this is a fine upper bound. Counted only here, after the
    // parallel path declined: it is a serial pass over the whole file.
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

    if (has_first_row && all_hints_known && !need_null_check) {
        struct CsvSchemaColumnBuilder {
            CsvColumnKind kind;
            ibex::Column<std::int64_t> ints;
            ibex::Column<double> doubles;
            ibex::Column<std::string> strings;
            ibex::Column<ibex::Date> dates;
            ibex::DecimalType decimal_type{};
            ibex::Column<ibex::Decimal> decimals;
            using code_type = ibex::Column<ibex::Categorical>::code_type;
            std::vector<std::string> dict;
            ibex::Column<ibex::Categorical>::index_map cat_index;
            ibex::Column<ibex::Categorical>::codes_storage cat_codes;

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
                    case CsvColumnKind::Date:
                        dates.reserve(n);
                        break;
                    case CsvColumnKind::Decimal:
                        decimals.reserve(n);
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
                        throw std::runtime_error("csv::read: column '" + std::string(name) +
                                                 "' row " + std::to_string(row_index) +
                                                 " failed to parse as i64");
                    }
                    ints.push_back(iv);
                    return;
                }
                if (kind == CsvColumnKind::Double) {
                    double dv{};
                    if (!csv_try_double(sv, dv)) {
                        throw std::runtime_error("csv::read: column '" + std::string(name) +
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
                if (kind == CsvColumnKind::Date) {
                    std::int32_t dv{};
                    if (!csv_try_date(sv, dv)) {
                        throw std::runtime_error("csv::read: column '" + std::string(name) +
                                                 "' row " + std::to_string(row_index) +
                                                 " failed to parse as date (expected YYYY-MM-DD)");
                    }
                    dates.push_back(ibex::Date{dv});
                    return;
                }
                if (kind == CsvColumnKind::Decimal) {
                    auto units = ibex::decimal::parse(sv, decimal_type);
                    if (!units) {
                        throw std::runtime_error("csv::read: column '" + std::string(name) +
                                                 "' row " + std::to_string(row_index) + ": " +
                                                 units.error());
                    }
                    decimals.push_back(ibex::Decimal{*units});
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
                    case CsvColumnKind::Date:
                        return std::move(dates);
                    case CsvColumnKind::Decimal: {
                        ibex::ColumnMeta meta;
                        meta.decimal = decimal_type;
                        decimals.set_meta(meta);
                        return std::move(decimals);
                    }
                    case CsvColumnKind::Categorical:
                        return ibex::Column<ibex::Categorical>(std::move(dict),
                                                               std::move(cat_codes));
                    case CsvColumnKind::Infer:
                        break;
                }
                throw std::runtime_error("csv::read: internal error - unresolved schema hint");
            }
        };

        std::vector<CsvSchemaColumnBuilder> builders;
        builders.reserve(n_cols);
        for (std::size_t c = 0; c < resolved_hints.size(); ++c) {
            builders.emplace_back(resolved_hints[c]);
            builders.back().decimal_type = resolve_decimal(c, col_names[c]);
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

        auto is_null = [&](std::size_t i) -> bool { return need_null_check && !validity[i]; };

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
                        throw std::runtime_error("csv::read: column '" + name + "' row " +
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
                        throw std::runtime_error("csv::read: column '" + name + "' row " +
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
            if (hint == CsvColumnKind::Decimal) {
                // Exact: the text is parsed straight to units, rounding any
                // extra fractional digits half away from zero.
                const ibex::DecimalType type = resolve_decimal(c, name);
                ibex::Column<ibex::Decimal> col;
                ibex::ColumnMeta meta;
                meta.decimal = type;
                col.set_meta(meta);
                col.reserve(n);
                for (std::size_t i = 0; i < n; ++i) {
                    if (is_null(i)) {
                        col.push_back(ibex::Decimal{});
                        continue;
                    }
                    auto units = ibex::decimal::parse(vals[i], type);
                    if (!units) {
                        throw std::runtime_error("csv::read: column '" + name + "' row " +
                                                 std::to_string(i) + ": " + units.error());
                    }
                    col.push_back(ibex::Decimal{*units});
                }
                if (has_nulls) {
                    table.add_column(name, std::move(col), std::move(validity));
                } else {
                    table.add_column(name, std::move(col));
                }
                continue;
            }
            if (hint == CsvColumnKind::Date) {
                ibex::Column<ibex::Date> col;
                col.reserve(n);
                for (std::size_t i = 0; i < n; ++i) {
                    if (is_null(i)) {
                        col.push_back(ibex::Date{});
                        continue;
                    }
                    std::int32_t dv{};
                    if (!csv_try_date(vals[i], dv)) {
                        throw std::runtime_error("csv::read: column '" + name + "' row " +
                                                 std::to_string(i) +
                                                 " failed to parse as date (expected YYYY-MM-DD)");
                    }
                    col.push_back(ibex::Date{dv});
                }
                if (has_nulls) {
                    table.add_column(name, std::move(col), std::move(validity));
                } else {
                    table.add_column(name, std::move(col));
                }
                continue;
            }
            if (hint == CsvColumnKind::Categorical) {
                ibex::Column<ibex::Categorical>::codes_storage codes;
                codes.reserve(n);
                std::vector<std::string> dict;
                // Keys are owned `std::string`. Storing `std::string_view`
                // would dangle when `dict.push_back` reallocates the dict
                // buffer, causing repeat values to be assigned fresh codes.
                robin_hood::unordered_map<std::string, ibex::Column<ibex::Categorical>::code_type,
                                          ibex::detail::StringHash, std::equal_to<>>
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
        // we keep the built column directly. A bare empty field reads as
        // null in an otherwise-numeric column, even without an explicit
        // null spec ("<empty>"), rather than forcing the whole column to
        // String; a column that instead resolves to String below leaves
        // empty fields as empty strings, unaffected by this. The probe
        // works on a local copy of the validity bitmap so a failed probe
        // (or the double probe that follows) never leaks empty-as-null
        // bits into a column that ends up String.
        if (n > 0) {
            ibex::Column<std::int64_t> int_col;
            int_col.reserve(n);
            bool all_int = true;
            bool any_valid = false;
            bool any_empty = false;
            ibex::runtime::ValidityBitmap probe_validity;
            for (std::size_t i = 0; i < n; ++i) {
                if (is_null(i)) {
                    int_col.push_back(0);
                    continue;
                }
                if (vals[i].empty()) {
                    if (!any_empty) {
                        probe_validity = has_nulls ? validity : ibex::runtime::ValidityBitmap{};
                        if (!has_nulls) {
                            probe_validity.assign(n, true);
                        }
                        any_empty = true;
                    }
                    probe_validity.set(i, false);
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
                if (has_nulls || any_empty) {
                    table.add_column(name, std::move(int_col),
                                     any_empty ? std::move(probe_validity) : std::move(validity));
                } else {
                    table.add_column(name, std::move(int_col));
                }
                continue;
            }
        }

        // Try double with a fused build pass. Same empty-as-null treatment
        // as the int64 probe above, also on a local validity copy.
        if (n > 0) {
            ibex::Column<double> dbl_col;
            dbl_col.reserve(n);
            bool all_double = true;
            bool any_valid = false;
            bool any_empty = false;
            ibex::runtime::ValidityBitmap probe_validity;
            for (std::size_t i = 0; i < n; ++i) {
                if (is_null(i)) {
                    dbl_col.push_back(0.0);
                    continue;
                }
                if (vals[i].empty()) {
                    if (!any_empty) {
                        probe_validity = has_nulls ? validity : ibex::runtime::ValidityBitmap{};
                        if (!has_nulls) {
                            probe_validity.assign(n, true);
                        }
                        any_empty = true;
                    }
                    probe_validity.set(i, false);
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
                if (has_nulls || any_empty) {
                    table.add_column(name, std::move(dbl_col),
                                     any_empty ? std::move(probe_validity) : std::move(validity));
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
            const std::size_t max_uniques = ibex::categorical_promotion_limit(n);
            ibex::Column<ibex::Categorical>::codes_storage codes;
            codes.reserve(n);
            std::vector<std::string> dict;
            // Keys are owned `std::string`. Storing `std::string_view` here
            // would dangle when `dict.push_back` reallocates the dict buffer
            // (or moves SSO strings during reallocation), causing repeat
            // values to be assigned fresh codes — which downstream looks like
            // group-by producing more groups than distinct keys.
            robin_hood::unordered_map<std::string, ibex::Column<ibex::Categorical>::code_type,
                                      ibex::detail::StringHash, std::equal_to<>>
                index;
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

// ── Lazy read with a declared schema ─────────────────────────────────────────
//
// `let t = csv::read(path, ..., schema = "...")` with a type for EVERY column
// binds like `parquet::read`: binding learns the schema and the row count and
// decodes nothing, and each query parses only the columns it references. Those
// columns are cached in the `LazyTable`, so a later query that needs them again
// does not touch the file.
//
// Why only a declared schema. A lazy source must know every column's type when
// it is bound. Parquet reads that from its footer; a CSV file has none, and the
// eager reader infers a type from the WHOLE column (one "1.5" anywhere makes it
// Double; Categorical vs String depends on the distinct count over all rows).
// Learning that is a full pass -- about as costly as the eager read itself --
// and the first query would then tokenize the file again. With every type
// declared there is nothing to learn: binding is a parallel row count.
//
// What the user sees differs from the eager read in exactly two ways, both
// documented in SPEC.md (Section 12.1):
//   * A value its declared type rejects is reported when a query first reads
//     that column, not at the `let`, and a column no query reads is never
//     checked. The message is the eager reader's own.
//   * Each query that needs a column not yet cached re-reads the file; the
//     file must not change while it is bound (a changed row count or header
//     is an error, not a silently different answer).
//
// Every other case declines (`csv_lazy_read` returns null) and the eager
// reader runs as before: no schema or a partial one, an HTTPS path, an empty
// file, or a file that cannot be opened -- whose error the eager reader then
// reports in its usual words.

/// Rows in `[body, end)` exactly as `csv_parse_row` counts them.
///
/// Without a quote anywhere in the body a row is a maximal run of bytes that
/// are neither '\r' nor '\n', which a byte scan counts in parallel with no
/// tokenizing. A quote may hide a newline inside a field, so then the rows are
/// tokenized with the same cuts and boundary check as the parallel read (a
/// failed check recounts as one range).
inline auto csv_count_rows(const char* body, const char* end, char delim, std::size_t workers)
    -> std::size_t {
    const auto bytes = static_cast<std::size_t>(end - body);
    if (bytes == 0) {
        return 0;
    }
    workers = std::max<std::size_t>(workers, 1);
    const std::size_t range_bytes = std::max<std::size_t>(
        csv_env_size("IBEX_CSV_PARALLEL_RANGE_BYTES", kCsvParallelMinRangeBytes), 1);
    const std::size_t parts = std::clamp<std::size_t>(bytes / range_bytes, 1, workers * 4);
    const auto bound = [&](std::size_t k) { return k == parts ? end : body + (bytes / parts) * k; };
    std::vector<std::size_t> counts(parts, 0);
    std::atomic<bool> quoted{false};
    csv_run_parallel(parts, workers, [&](std::size_t k) {
        const char* p = bound(k);
        const char* q = bound(k + 1);
        if (std::memchr(p, '"', static_cast<std::size_t>(q - p)) != nullptr) {
            quoted.store(true, std::memory_order_relaxed);
            return;
        }
        bool after_break = p == body || p[-1] == '\n' || p[-1] == '\r';
        std::size_t n = 0;
        for (; p < q; ++p) {
            const bool is_break = *p == '\n' || *p == '\r';
            n += (!is_break && after_break) ? 1 : 0;
            after_break = is_break;
        }
        counts[k] = n;
    });
    if (!quoted.load()) {
        std::size_t total = 0;
        for (const auto n : counts) {
            total += n;
        }
        return total;
    }
    const auto tokenized = [&](std::size_t pieces) -> std::optional<std::size_t> {
        const auto cuts = csv_split_ranges(body, end, pieces);
        std::vector<CsvRangeRows> ranges(cuts.size());
        const CsvReadOptions none;
        const CsvNullSet no_nulls(none);
        const std::vector<std::uint8_t> nothing;
        csv_run_parallel(cuts.size(), workers, [&](std::size_t r) {
            csv_parse_range(cuts[r].first, cuts[r].second, end, delim, 0, no_nulls, nothing,
                            ranges[r]);
        });
        std::size_t total = 0;
        for (const auto& range : ranges) {
            if (!range.consistent) {
                return std::nullopt;
            }
            total += range.rows;
        }
        return total;
    };
    if (auto total = tokenized(parts)) {
        return *total;
    }
    return *tokenized(1);
}

/// `table`'s rows at the ascending positions `selection`, for a decode asked
/// to materialize only some rows. Codes keep their dictionary.
inline auto csv_gather_rows(const ibex::runtime::Table& table,
                            const ibex::runtime::Selection& selection) -> ibex::runtime::Table {
    ibex::runtime::Table out;
    const std::size_t n = selection.size();
    for (const auto& entry : table.columns) {
        auto gathered = std::visit(
            [&](const auto& column) -> ibex::runtime::ColumnValue {
                using C = std::decay_t<decltype(column)>;
                if constexpr (std::is_same_v<C, ibex::Column<ibex::Categorical>>) {
                    ibex::Column<ibex::Categorical>::codes_storage codes;
                    codes.reserve(n);
                    for (const auto row : selection) {
                        codes.push_back(column.code_at(row));
                    }
                    return ibex::Column<ibex::Categorical>(column.dictionary_ptr(),
                                                           column.index_ptr(), std::move(codes));
                } else {
                    C result;
                    result.reserve(n);
                    for (const auto row : selection) {
                        result.push_back(column[row]);
                    }
                    result.set_meta(column.meta());
                    return result;
                }
            },
            *entry.column);
        if (entry.validity.has_value()) {
            ibex::runtime::ValidityBitmap validity;
            validity.assign(n, true);
            for (std::size_t i = 0; i < n; ++i) {
                if (!(*entry.validity)[selection[i]]) {
                    validity.set(i, false);
                }
            }
            out.add_column(entry.name, std::move(gathered), std::move(validity));
        } else {
            out.add_column(entry.name, std::move(gathered));
        }
    }
    return out;
}

/// What a bound lazy CSV source remembers; shared by every reader it makes.
struct CsvLazyState {
    std::string path;
    CsvReadOptions options;
    std::vector<std::string> names;
    std::size_t rows = 0;
};

/// Decodes the requested columns of a bound CSV file. One per concurrent
/// decode (`LazyTable` pools them); it holds no cursor, so it reopens the file
/// for each decode.
class CsvLazyReader final : public ibex::runtime::LazySourceReader {
   public:
    explicit CsvLazyReader(std::shared_ptr<const CsvLazyState> state) : state_(std::move(state)) {}

    auto decode(const std::vector<std::string>& names, const ibex::runtime::Selection* selection,
                const ibex::runtime::SourceUnit* /*unit: the source has no units*/,
                const ibex::runtime::ExecutionContext& exec)
        -> std::expected<ibex::runtime::Table, std::string> override {
        const auto changed = [&] {
            return std::unexpected("csv::read: '" + state_->path +
                                   "' changed after it was bound; bind it again");
        };
        try {
            CsvSource source(state_->path);
            const char* end = source.data() + source.size();
            CsvLayout layout;
            csv_read_layout(source.data(), end, state_->options, layout);
            if (!layout.has_first_row || layout.names != state_->names) {
                return changed();
            }
            std::vector<std::uint8_t> take(layout.names.size(), 0);
            for (const auto& name : names) {
                for (std::size_t c = 0; c < layout.names.size(); ++c) {
                    if (layout.names[c] == name) {
                        take[c] = 1;
                    }
                }
            }
            const std::size_t workers = std::max<std::size_t>(
                1, std::min(exec.compute_budget(), ibex::runtime::process_worker_pool().size()));
            auto table = csv_read_parallel(end, layout, state_->options, &take, workers);
            if (!table.has_value()) {
                // A declared type rejected a value. The eager reader raises
                // the error in its own words (column and row).
                (void)read_csv_with_options(state_->path, state_->options);
                return std::unexpected("csv::read: '" + state_->path +
                                       "': a value does not match its declared type");
            }
            if (table->rows() != state_->rows) {
                return changed();
            }
            if (selection != nullptr) {
                return csv_gather_rows(*table, *selection);
            }
            return std::move(*table);
        } catch (const std::exception& e) {
            return std::unexpected(std::string(e.what()));
        }
    }

   private:
    std::shared_ptr<const CsvLazyState> state_;
};

/// `csv::read` as a lazy source, or null to decline (see the section comment):
/// the caller then reads eagerly.
inline auto csv_lazy_read(std::string_view path, const CsvReadOptions& options)
    -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
    if (options.schema.empty() || is_https_uri(path)) {
        return ibex::runtime::LazyTablePtr{};
    }
    const std::string path_string{path};
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path_string, ec)) {
        return ibex::runtime::LazyTablePtr{};
    }
    try {
        CsvSource source(path_string);
        const char* end = source.data() + source.size();
        CsvLayout layout;
        csv_read_layout(source.data(), end, options, layout);
        if (!layout.has_first_row || std::ranges::any_of(layout.hints, [](CsvColumnKind k) {
                return k == CsvColumnKind::Infer;
            })) {
            return ibex::runtime::LazyTablePtr{};
        }
        const std::size_t workers = std::min(ibex::runtime::compute_thread_count(),
                                             ibex::runtime::process_worker_pool().size());
        const std::size_t rows = 1 + csv_count_rows(layout.body, end, options.delimiter, workers);

        ibex::runtime::Table schema;
        for (std::size_t c = 0; c < layout.names.size(); ++c) {
            switch (layout.hints[c]) {
                case CsvColumnKind::Int:
                    schema.add_column(layout.names[c], ibex::Column<std::int64_t>{});
                    break;
                case CsvColumnKind::Double:
                    schema.add_column(layout.names[c], ibex::Column<double>{});
                    break;
                case CsvColumnKind::String:
                    schema.add_column(layout.names[c], ibex::Column<std::string>{});
                    break;
                case CsvColumnKind::Categorical:
                    schema.add_column(layout.names[c],
                                      ibex::Column<ibex::Categorical>(std::vector<std::string>{}));
                    break;
                case CsvColumnKind::Date:
                    schema.add_column(layout.names[c], ibex::Column<ibex::Date>{});
                    break;
                case CsvColumnKind::Decimal: {
                    ibex::Column<ibex::Decimal> column;
                    ibex::ColumnMeta meta;
                    meta.decimal = layout.decimals[c];
                    column.set_meta(meta);
                    schema.add_column(layout.names[c], std::move(column));
                    break;
                }
                case CsvColumnKind::Infer:
                    return ibex::runtime::LazyTablePtr{};
            }
        }
        auto state = std::make_shared<const CsvLazyState>(CsvLazyState{
            .path = path_string, .options = options, .names = layout.names, .rows = rows});
        ibex::runtime::LazySourceReaderFactory factory =
            [state]() -> std::expected<ibex::runtime::LazySourceReaderPtr, std::string> {
            return std::make_unique<CsvLazyReader>(state);
        };
        return std::make_shared<ibex::runtime::LazyTable>(std::move(schema), rows,
                                                          std::move(factory));
    } catch (const std::exception&) {
        // Unreadable: the eager reader reports it in its own words.
        return ibex::runtime::LazyTablePtr{};
    }
}

}  // namespace ibex::csv::detail

namespace ibex::ext::csv {

inline auto read(std::string_view path) -> ibex::runtime::Table {
    return ibex::csv::detail::read_csv_with_options(path, ibex::csv::detail::CsvReadOptions{});
}

inline auto read(std::string_view path, std::string_view null_spec) -> ibex::runtime::Table {
    return ibex::csv::detail::read_csv_with_options(
        path, ibex::csv::detail::csv_parse_null_spec(null_spec));
}

inline auto read(std::string_view path, std::string_view null_spec, std::string_view delimiter)
    -> ibex::runtime::Table {
    auto options = ibex::csv::detail::csv_parse_null_spec(null_spec);
    options.delimiter = ibex::csv::detail::csv_parse_delimiter(delimiter);
    return ibex::csv::detail::read_csv_with_options(path, options);
}

inline auto read(std::string_view path, std::string_view null_spec, std::string_view delimiter,
                 bool has_header) -> ibex::runtime::Table {
    auto options = ibex::csv::detail::csv_parse_null_spec(null_spec);
    options.delimiter = ibex::csv::detail::csv_parse_delimiter(delimiter);
    options.has_header = has_header;
    return ibex::csv::detail::read_csv_with_options(path, options);
}

inline auto read(std::string_view path, std::string_view null_spec, std::string_view delimiter,
                 bool has_header, std::string_view schema) -> ibex::runtime::Table {
    auto options = ibex::csv::detail::csv_parse_null_spec(null_spec);
    options.delimiter = ibex::csv::detail::csv_parse_delimiter(delimiter);
    options.has_header = has_header;
    options.schema = ibex::csv::detail::csv_parse_schema(schema);
    return ibex::csv::detail::read_csv_with_options(path, options);
}

}  // namespace ibex::ext::csv

namespace ibex::csv::detail {

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

/// Writes `value` using the shortest decimal representation that round-trips
/// back to the same double (std::to_chars' default float format). Plain
/// `out << value` truncates to the stream's default precision (6 significant
/// digits), silently dropping precision on anything larger than ~5-6 digits
/// (e.g. a financial sum like 3083843.06 would print as 3.08384e+06).
inline void csv_write_double(std::ostream& out, double value) {
    std::array<char, 32> buf{};
    auto result = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    out.write(buf.data(), static_cast<std::streamsize>(result.ptr - buf.data()));
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
                csv_write_double(out, col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<std::string>>) {
                csv_write_field(out, col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Categorical>>) {
                csv_write_field(out, col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                out << col[r].days;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>) {
                out << col[r].nanos;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Decimal>>) {
                // Exactly `scale` fractional digits: the text csv::read's
                // decimal(p,s) hint parses back to the same units.
                out << ibex::decimal::to_string(col[r].units,
                                                ibex::runtime::decimal_type_of(col).scale);
            }
        },
        *entry.column);
}

}  // namespace ibex::csv::detail

namespace ibex::ext::csv {

/// Write `table` to a CSV file at `path`.
///
/// Returns the number of data rows written (excluding the header line).
/// Null values are written as empty fields.  String fields containing commas,
/// double-quotes, or newlines are quoted per RFC 4180.
inline auto write(const ibex::runtime::Table& table, std::string_view path) -> std::int64_t {
    std::string path_str{path};
    if (const std::filesystem::path parent = std::filesystem::path(path_str).parent_path();
        !parent.empty()) {
        std::error_code mkdir_ec;
        std::filesystem::create_directories(parent, mkdir_ec);
    }
    std::ofstream ofs{path_str};
    if (!ofs) {
        throw std::runtime_error("csv::write: cannot open for writing: " + path_str);
    }

    const auto& cols = table.columns;
    const std::size_t n_cols = cols.size();
    const std::size_t n_rows = table.rows();

    // Header row
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (c > 0) {
            ofs.put(',');
        }
        ibex::csv::detail::csv_write_field(ofs, cols[c].name);
    }
    ofs.put('\n');

    // Data rows
    for (std::size_t r = 0; r < n_rows; ++r) {
        for (std::size_t c = 0; c < n_cols; ++c) {
            if (c > 0) {
                ofs.put(',');
            }
            ibex::csv::detail::csv_write_cell(ofs, cols[c], r);
        }
        ofs.put('\n');
    }

    ofs.flush();
    if (!ofs) {
        throw std::runtime_error("csv::write: I/O error writing: " + path_str);
    }

    return static_cast<std::int64_t>(n_rows);
}

}  // namespace ibex::ext::csv
