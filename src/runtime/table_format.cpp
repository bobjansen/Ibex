#include <ibex/runtime/table_format.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <string>
#include <vector>

namespace ibex::runtime {

namespace {

auto normalize_float_text(std::string text) -> std::string {
    auto trim_mantissa = [](std::string& mantissa) {
        auto dot = mantissa.find('.');
        if (dot != std::string::npos) {
            while (!mantissa.empty() && mantissa.back() == '0') {
                mantissa.pop_back();
            }
            if (!mantissa.empty() && mantissa.back() == '.') {
                mantissa.pop_back();
            }
        }
        if (mantissa == "-0") {
            mantissa = "0";
        }
    };

    auto exp_pos = text.find_first_of("eE");
    if (exp_pos == std::string::npos) {
        trim_mantissa(text);
        return text;
    }

    std::string mantissa = text.substr(0, exp_pos);
    trim_mantissa(mantissa);

    std::string exponent = text.substr(exp_pos + 1);
    char sign = '\0';
    std::size_t idx = 0;
    if (!exponent.empty() && (exponent[0] == '+' || exponent[0] == '-')) {
        sign = exponent[0];
        idx = 1;
    }
    while (idx < exponent.size() && exponent[idx] == '0') {
        ++idx;
    }
    std::string digits = idx < exponent.size() ? exponent.substr(idx) : "0";

    std::string out = std::move(mantissa);
    out.push_back('e');
    if (sign == '-') {
        out.push_back('-');
    }
    out.append(digits);
    return out;
}

}  // namespace

auto format_date(Date date) -> std::string {
    using namespace std::chrono;
    sys_days day = sys_days{days{date.days}};
    year_month_day ymd{day};
    return fmt::format("{:04}-{:02}-{:02}", static_cast<int>(ymd.year()),
                       static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()));
}

auto format_timestamp(Timestamp ts) -> std::string {
    using namespace std::chrono;
    sys_time<nanoseconds> tp{nanoseconds{ts.nanos}};
    auto day = floor<days>(tp);
    year_month_day ymd{day};
    auto tod = tp - day;
    hh_mm_ss<nanoseconds> hms{tod};
    return fmt::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}", static_cast<int>(ymd.year()),
                       static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()),
                       hms.hours().count(), hms.minutes().count(), hms.seconds().count(),
                       hms.subseconds().count());
}

auto format_float_mixed(double value) -> std::string {
    if (std::isnan(value)) {
        return "nan";
    }
    if (std::isinf(value)) {
        return value > 0 ? "inf" : "-inf";
    }
    std::array<char, 128> buffer{};
    auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value,
                                   std::chars_format::general, 7);
    if (ec == std::errc{}) {
        return normalize_float_text(std::string(buffer.data(), ptr));
    }
    return normalize_float_text(fmt::format("{:.7g}", value));
}

auto quote_and_escape(std::string_view text) -> std::string {
    std::string out;
    out.reserve(text.size() + 2);
    out.push_back('"');
    for (char ch : text) {
        switch (ch) {
            case '\\':
                out.append("\\\\");
                break;
            case '"':
                out.append("\\\"");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\t':
                out.append("\\t");
                break;
            case '\r':
                out.append("\\r");
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    out.push_back('"');
    return out;
}

auto format_cell(const ColumnEntry& entry, std::size_t row) -> std::string {
    if (is_null(entry, row)) {
        return "null";
    }
    const auto& column = *entry.column;
    return std::visit(
        [row](const auto& col) -> std::string {
            using T = typename std::decay_t<decltype(col)>::value_type;
            if constexpr (std::is_same_v<T, Date>) {
                return format_date(col[row]);
            } else if constexpr (std::is_same_v<T, Timestamp>) {
                return format_timestamp(col[row]);
            } else if constexpr (std::is_same_v<T, std::string_view>) {
                return quote_and_escape(col[row]);
            } else if constexpr (std::is_same_v<T, double>) {
                return format_float_mixed(col[row]);
            } else {
                return fmt::format("{}", col[row]);
            }
        },
        column);
}

void format_table(const Table& table, std::ostream& out, std::size_t max_rows) {
    if (table.columns.empty()) {
        // A column-less frame may still carry a logical row count (e.g. from
        // `Table(n)`); report it rather than the bare "<empty>".
        if (table.rows() > 0) {
            fmt::print(out, "rows: {}\n(no columns)\n", table.rows());
        } else {
            fmt::print(out, "<empty>\n");
        }
        return;
    }
    fmt::print(out, "rows: {}\n", table.rows());

    const std::size_t col_count = table.columns.size();
    const std::size_t shown_rows = std::min(table.rows(), max_rows);

    std::vector<std::size_t> widths(col_count);
    std::vector<std::vector<std::string>> cells(col_count);
    for (std::size_t c = 0; c < col_count; ++c) {
        widths[c] = table.columns[c].name.size();
        cells[c].reserve(shown_rows);
        for (std::size_t r = 0; r < shown_rows; ++r) {
            auto cell = format_cell(table.columns[c], r);
            widths[c] = std::max(widths[c], cell.size());
            cells[c].push_back(std::move(cell));
        }
    }

    auto print_sep = [&]() {
        fmt::print(out, "+");
        for (std::size_t c = 0; c < col_count; ++c) {
            fmt::print(out, "{:-<{}}+", "", widths[c] + 2);
        }
        fmt::print(out, "\n");
    };

    print_sep();
    fmt::print(out, "|");
    for (std::size_t c = 0; c < col_count; ++c) {
        fmt::print(out, " {:<{}} |", table.columns[c].name, widths[c]);
    }
    fmt::print(out, "\n");
    print_sep();

    for (std::size_t r = 0; r < shown_rows; ++r) {
        fmt::print(out, "|");
        for (std::size_t c = 0; c < col_count; ++c) {
            fmt::print(out, " {:<{}} |", cells[c][r], widths[c]);
        }
        fmt::print(out, "\n");
    }
    print_sep();

    if (table.rows() > shown_rows) {
        fmt::print(out, "... ({} more rows)\n", table.rows() - shown_rows);
    }
}

}  // namespace ibex::runtime
