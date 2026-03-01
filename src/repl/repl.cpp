#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_set>
#include <variant>

#ifdef IBEX_HAS_READLINE
#include <readline/history.h>
#include <readline/readline.h>
#endif

namespace ibex::repl {

namespace {

using FunctionRegistry = std::unordered_map<std::string, parser::FunctionDecl>;
using ExternDeclRegistry = std::unordered_map<std::string, parser::ExternDecl>;
using ColumnRegistry = std::unordered_map<std::string, runtime::ColumnValue>;
using EvalValue = std::variant<runtime::Table, runtime::ScalarValue, runtime::ColumnValue>;

#ifdef IBEX_HAS_READLINE
constexpr std::array<std::string_view, 12> kColonCommands = {
    ":q",    ":quit",     ":exit", ":tables", ":scalars", ":schema",
    ":head", ":describe", ":load", ":timing", ":time",    ":comments",
};

auto colon_command_generator(const char* text, int state) -> char* {
    static std::size_t index = 0;
    static std::string prefix;
    if (state == 0) {
        index = 0;
        prefix = text != nullptr ? text : "";
    }
    while (index < kColonCommands.size()) {
        const auto command = kColonCommands[index++];
        if (command.starts_with(prefix)) {
            return ::strdup(std::string(command).c_str());
        }
    }
    return nullptr;
}

auto repl_completion(const char* text, int start, int /*end*/) -> char** {
    if (start != 0 || text == nullptr || text[0] != ':') {
        return nullptr;
    }
    rl_attempted_completion_over = 1;
    return rl_completion_matches(text, colon_command_generator);
}

void configure_line_editing() {
    rl_attempted_completion_function = repl_completion;
}

auto read_repl_line(const std::string& prompt, std::string& out) -> bool {
    char* raw = ::readline(prompt.c_str());
    if (raw == nullptr) {
        return false;
    }
    out.assign(raw);
    if (!out.empty()) {
        ::add_history(raw);
    }
    std::free(raw);
    return true;
}
#else
void configure_line_editing() {}

auto read_repl_line(const std::string& prompt, std::string& out) -> bool {
    fmt::print("{}", prompt);
    return static_cast<bool>(std::getline(std::cin, out));
}
#endif

std::string_view trim(std::string_view text);

struct ScriptCommentLine {
    std::size_t line = 0;
    std::string text;
};

void append_comment_line(std::vector<ScriptCommentLine>& comments, std::size_t line,
                         std::string_view raw_text) {
    auto text = trim(raw_text);
    if (!text.empty() && text.front() == '*') {
        text = trim(text.substr(1));
    }
    if (text.empty()) {
        return;
    }
    comments.push_back(ScriptCommentLine{
        .line = line,
        .text = std::string(text),
    });
}

auto collect_script_comment_lines(std::string_view source) -> std::vector<ScriptCommentLine> {
    std::vector<ScriptCommentLine> comments;
    std::size_t i = 0;
    std::size_t line = 1;
    while (i < source.size()) {
        if (source[i] == '\n') {
            ++line;
            ++i;
            continue;
        }
        if (source[i] == '"' || source[i] == '\'') {
            const char quote = source[i];
            ++i;
            while (i < source.size()) {
                if (source[i] == '\\' && (i + 1) < source.size()) {
                    if (source[i + 1] == '\n') {
                        ++line;
                    }
                    i += 2;
                    continue;
                }
                if (source[i] == '\n') {
                    ++line;
                }
                if (source[i] == quote) {
                    ++i;
                    break;
                }
                ++i;
            }
            continue;
        }
        if (source[i] == '/' && (i + 1) < source.size() && source[i + 1] == '/') {
            const std::size_t start = i + 2;
            std::size_t end = source.find('\n', start);
            if (end == std::string_view::npos) {
                end = source.size();
            }
            append_comment_line(comments, line, source.substr(start, end - start));
            i = end;
            continue;
        }
        if (source[i] == '/' && (i + 1) < source.size() && source[i + 1] == '*') {
            i += 2;
            std::size_t comment_line = line;
            std::size_t segment_start = i;
            bool closed = false;
            while (i < source.size()) {
                if (source[i] == '*' && (i + 1) < source.size() && source[i + 1] == '/') {
                    append_comment_line(comments, comment_line,
                                        source.substr(segment_start, i - segment_start));
                    i += 2;
                    closed = true;
                    break;
                }
                if (source[i] == '\n') {
                    append_comment_line(comments, comment_line,
                                        source.substr(segment_start, i - segment_start));
                    ++line;
                    ++i;
                    comment_line = line;
                    segment_start = i;
                    continue;
                }
                ++i;
            }
            if (!closed) {
                append_comment_line(comments, comment_line, source.substr(segment_start));
            }
            continue;
        }
        ++i;
    }
    return comments;
}

auto statement_start_line(const parser::Stmt& stmt) -> std::size_t {
    return std::visit([](const auto& s) { return s.start_line; }, stmt);
}

auto statement_end_line(const parser::Stmt& stmt) -> std::size_t {
    return std::visit([](const auto& s) { return s.end_line; }, stmt);
}

auto build_statement_comment_groups(const std::vector<parser::Stmt>& statements,
                                    const std::vector<ScriptCommentLine>& comments)
    -> std::vector<std::vector<std::string>> {
    std::vector<std::vector<std::string>> groups(statements.size());
    std::size_t comment_index = 0;
    std::size_t prev_end_line = 0;
    for (std::size_t i = 0; i < statements.size(); ++i) {
        const std::size_t start_line = statement_start_line(statements[i]);
        const std::size_t end_line = std::max(statement_end_line(statements[i]), start_line);

        while (comment_index < comments.size() && comments[comment_index].line < start_line) {
            if (comments[comment_index].line > prev_end_line) {
                groups[i].push_back(comments[comment_index].text);
            }
            ++comment_index;
        }
        while (comment_index < comments.size() && comments[comment_index].line <= end_line) {
            if (comments[comment_index].line >= start_line) {
                groups[i].push_back(comments[comment_index].text);
            }
            ++comment_index;
        }
        prev_end_line = end_line;
    }
    return groups;
}

void print_comment_group(const std::vector<std::string>& comments) {
    if (comments.empty()) {
        return;
    }
    fmt::print("script comments:\n");
    for (const auto& line : comments) {
        fmt::print("  {}\n", line);
    }
    fmt::print("\n");
}

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

auto format_scalar(const runtime::ScalarValue& value) -> std::string {
    return std::visit(
        [](const auto& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, Date>) {
                return format_date(v);
            } else if constexpr (std::is_same_v<T, Timestamp>) {
                return format_timestamp(v);
            } else if constexpr (std::is_same_v<T, double>) {
                return format_float_mixed(v);
            } else {
                return fmt::format("{}", v);
            }
        },
        value);
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

auto format_cell(const runtime::ColumnEntry& entry, std::size_t row) -> std::string {
    if (runtime::is_null(entry, row)) {
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

auto build_builtin_tables() -> runtime::TableRegistry {
    runtime::TableRegistry registry;
    runtime::Table trades;
    trades.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    trades.add_column("symbol", Column<std::string>{"A", "B", "A", "C"});
    registry.emplace("trades", std::move(trades));
    return registry;
}

void print_table(const runtime::Table& table, std::size_t max_rows = 10) {
    if (table.columns.empty()) {
        fmt::print("<empty>\n");
        return;
    }
    fmt::print("rows: {}\n", table.rows());

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
        fmt::print("+");
        for (std::size_t c = 0; c < col_count; ++c) {
            fmt::print("{:-<{}}+", "", widths[c] + 2);
        }
        fmt::print("\n");
    };

    print_sep();
    fmt::print("|");
    for (std::size_t c = 0; c < col_count; ++c) {
        fmt::print(" {:<{}} |", table.columns[c].name, widths[c]);
    }
    fmt::print("\n");
    print_sep();

    for (std::size_t r = 0; r < shown_rows; ++r) {
        fmt::print("|");
        for (std::size_t c = 0; c < col_count; ++c) {
            fmt::print(" {:<{}} |", cells[c][r], widths[c]);
        }
        fmt::print("\n");
    }
    print_sep();

    if (table.rows() > shown_rows) {
        fmt::print("... ({} more rows)\n", table.rows() - shown_rows);
    }
}

void print_tables(const runtime::TableRegistry& tables) {
    if (tables.empty()) {
        fmt::print("tables: <none>\n");
        return;
    }
    std::vector<std::string> names;
    names.reserve(tables.size());
    for (const auto& entry : tables) {
        names.push_back(entry.first);
    }
    std::ranges::sort(names);
    fmt::print("tables:");
    for (const auto& name : names) {
        fmt::print(" {}", name);
    }
    fmt::print("\n");
}

void print_scalars(const runtime::ScalarRegistry& scalars) {
    if (scalars.empty()) {
        fmt::print("scalars: <none>\n");
        return;
    }
    std::vector<std::string> names;
    names.reserve(scalars.size());
    for (const auto& entry : scalars) {
        names.push_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    fmt::print("scalars:\n");
    for (const auto& name : names) {
        fmt::print("  {} = ", name);
        const auto& value = scalars.at(name);
        fmt::print("{}", format_scalar(value));
        fmt::print("\n");
    }
}

std::string format_scalar_names(const runtime::ScalarRegistry& scalars) {
    if (scalars.empty()) {
        return "<none>";
    }
    std::vector<std::string> names;
    names.reserve(scalars.size());
    for (const auto& entry : scalars) {
        names.push_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

std::string format_table_names(const runtime::TableRegistry& tables) {
    if (tables.empty()) {
        return "<none>";
    }
    std::vector<std::string> names;
    names.reserve(tables.size());
    for (const auto& entry : tables) {
        names.push_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

std::string format_function_names(const FunctionRegistry& functions,
                                  const ExternDeclRegistry& extern_decls) {
    std::vector<std::string> names;
    names.reserve(functions.size() + extern_decls.size());
    for (const auto& entry : functions) {
        names.push_back(entry.first);
    }
    for (const auto& entry : extern_decls) {
        names.push_back(entry.first);
    }
    if (names.empty()) {
        return "<none>";
    }
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

std::string column_type_name(const runtime::ColumnValue& column) {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return "Int64";
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return "Float64";
    }
    if (std::holds_alternative<Column<std::string>>(column)) {
        return "String";
    }
    if (std::holds_alternative<Column<Categorical>>(column)) {
        return "Categorical";
    }
    if (std::holds_alternative<Column<Date>>(column)) {
        return "Date";
    }
    if (std::holds_alternative<Column<Timestamp>>(column)) {
        return "Timestamp";
    }
    return "Unknown";
}

void print_schema(const runtime::Table& table) {
    fmt::print("columns:\n");
    for (const auto& entry : table.columns) {
        fmt::print("  {}: {}\n", entry.name, column_type_name(*entry.column));
    }
}

void describe_table(const runtime::Table& table, std::size_t max_rows = 10) {
    print_schema(table);
    print_table(table, max_rows);
}

std::string_view ltrim(std::string_view text) {
    auto pos = text.find_first_not_of(" \t\r\n");
    if (pos == std::string_view::npos) {
        return {};
    }
    return text.substr(pos);
}

std::string_view rtrim(std::string_view text) {
    auto pos = text.find_last_not_of(" \t\r\n");
    if (pos == std::string_view::npos) {
        return {};
    }
    return text.substr(0, pos + 1);
}

std::string_view trim(std::string_view text) {
    return rtrim(ltrim(text));
}

bool starts_with_command(std::string_view text, std::string_view command) {
    if (!text.starts_with(command)) {
        return false;
    }
    if (text.size() == command.size()) {
        return true;
    }
    auto next = static_cast<unsigned char>(text[command.size()]);
    return std::isspace(next) != 0;
}

std::string parse_load_path(std::string_view text) {
    std::string_view view = trim(text);
    if (view.empty()) {
        return {};
    }
    if (view.front() == '"' || view.front() == '\'') {
        char quote = view.front();
        auto end = view.find(quote, 1);
        if (end != std::string_view::npos) {
            return std::string(view.substr(1, end - 1));
        }
    }
    return std::string(view);
}

std::size_t parse_optional_size(std::string_view text, std::size_t default_value) {
    text = trim(text);
    if (text.empty()) {
        return default_value;
    }
    std::size_t value = 0;
    auto result = std::from_chars(text.begin(), text.end(), value);
    if (result.ec != std::errc()) {
        return default_value;
    }
    return value;
}

void print_elapsed(std::chrono::steady_clock::duration elapsed) {
    using namespace std::chrono;
    auto micros = duration_cast<microseconds>(elapsed).count();
    if (micros < 1000) {
        fmt::print("time: {} us\n", micros);
        return;
    }
    if (micros < 1000 * 1000) {
        fmt::print("time: {:.3f} ms\n", static_cast<double>(micros) / 1000.0);
        return;
    }
    fmt::print("time: {:.3f} s\n", static_cast<double>(micros) / 1'000'000.0);
}

auto make_temp_table_name() -> std::string {
    static std::size_t counter = 0;
    return "_fn_tmp" + std::to_string(counter++);
}

auto column_name_from_expr(const parser::Expr& expr) -> std::optional<std::string> {
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        return ident->name;
    }
    if (const auto* literal = std::get_if<parser::LiteralExpr>(&expr.node)) {
        if (const auto* str_value = std::get_if<std::string>(&literal->value)) {
            return *str_value;
        }
    }
    return std::nullopt;
}

auto eval_table_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                     runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                     const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                     const runtime::ExternRegistry& externs)
    -> std::expected<runtime::Table, std::string>;

auto eval_scalar_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                      runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                      const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                      const runtime::ExternRegistry& externs)
    -> std::expected<runtime::ScalarValue, std::string>;

auto eval_function_call(parser::CallExpr& call, runtime::TableRegistry& tables,
                        runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                        const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                        const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string>;

auto eval_expr_value(parser::Expr& expr, runtime::TableRegistry& tables,
                     runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                     const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                     const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string> {
    if (std::holds_alternative<parser::LiteralExpr>(expr.node) ||
        std::holds_alternative<parser::BinaryExpr>(expr.node) ||
        std::holds_alternative<parser::UnaryExpr>(expr.node) ||
        std::holds_alternative<parser::GroupExpr>(expr.node)) {
        auto scalar =
            eval_scalar_expr(expr, tables, scalars, columns, functions, extern_decls, externs);
        if (!scalar) {
            return std::unexpected(scalar.error());
        }
        return EvalValue{std::move(scalar.value())};
    }
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        if (auto it = scalars.find(ident->name); it != scalars.end()) {
            return EvalValue{it->second};
        }
        if (auto it = columns.find(ident->name); it != columns.end()) {
            return EvalValue{it->second};
        }
    }
    if (auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (call->callee == "scalar") {
            auto scalar =
                eval_scalar_expr(expr, tables, scalars, columns, functions, extern_decls, externs);
            if (!scalar) {
                return std::unexpected(scalar.error());
            }
            return EvalValue{std::move(scalar.value())};
        }
        if (functions.contains(call->callee)) {
            return eval_function_call(*call, tables, scalars, columns, functions, extern_decls,
                                      externs);
        }
        if (extern_decls.contains(call->callee)) {
            const auto& decl = extern_decls.at(call->callee);
            if (decl.return_type.kind == parser::Type::Kind::Scalar) {
                auto scalar = eval_scalar_expr(expr, tables, scalars, columns, functions,
                                               extern_decls, externs);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                return EvalValue{std::move(scalar.value())};
            }
            auto table =
                eval_table_expr(expr, tables, scalars, columns, functions, extern_decls, externs);
            if (!table) {
                return std::unexpected(table.error());
            }
            return EvalValue{std::move(table.value())};
        }
    }
    auto table = eval_table_expr(expr, tables, scalars, columns, functions, extern_decls, externs);
    if (!table) {
        return std::unexpected(table.error());
    }
    return EvalValue{std::move(table.value())};
}

auto eval_scalar_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                      runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                      const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                      const runtime::ExternRegistry& externs)
    -> std::expected<runtime::ScalarValue, std::string> {
    if (const auto* literal = std::get_if<parser::LiteralExpr>(&expr.node)) {
        if (const auto* int_value = std::get_if<std::int64_t>(&literal->value)) {
            return runtime::ScalarValue{*int_value};
        }
        if (const auto* double_value = std::get_if<double>(&literal->value)) {
            return runtime::ScalarValue{*double_value};
        }
        if (const auto* str_value = std::get_if<std::string>(&literal->value)) {
            return runtime::ScalarValue{*str_value};
        }
        return std::unexpected("unsupported scalar literal");
    }
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        if (auto it = scalars.find(ident->name); it != scalars.end()) {
            return it->second;
        }
        if (columns.contains(ident->name)) {
            return std::unexpected("expected scalar expression");
        }
        return std::unexpected("unknown scalar: " + ident->name);
    }
    if (const auto* group = std::get_if<parser::GroupExpr>(&expr.node)) {
        return eval_scalar_expr(*group->expr, tables, scalars, columns, functions, extern_decls,
                                externs);
    }
    if (const auto* unary = std::get_if<parser::UnaryExpr>(&expr.node)) {
        auto value = eval_scalar_expr(*unary->expr, tables, scalars, columns, functions,
                                      extern_decls, externs);
        if (!value) {
            return std::unexpected(value.error());
        }
        if (unary->op != parser::UnaryOp::Negate) {
            return std::unexpected("unsupported unary operator in scalar expression");
        }
        if (std::holds_alternative<Date>(value.value()) ||
            std::holds_alternative<Timestamp>(value.value())) {
            return std::unexpected("date/time arithmetic not supported");
        }
        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
            return runtime::ScalarValue{-(*int_value)};
        }
        if (const auto* double_value = std::get_if<double>(&value.value())) {
            return runtime::ScalarValue{-(*double_value)};
        }
        return std::unexpected("unsupported unary operand type");
    }
    if (const auto* binary = std::get_if<parser::BinaryExpr>(&expr.node)) {
        auto left = eval_scalar_expr(*binary->left, tables, scalars, columns, functions,
                                     extern_decls, externs);
        if (!left) {
            return std::unexpected(left.error());
        }
        auto right = eval_scalar_expr(*binary->right, tables, scalars, columns, functions,
                                      extern_decls, externs);
        if (!right) {
            return std::unexpected(right.error());
        }
        if (std::holds_alternative<Date>(left.value()) ||
            std::holds_alternative<Date>(right.value()) ||
            std::holds_alternative<Timestamp>(left.value()) ||
            std::holds_alternative<Timestamp>(right.value())) {
            return std::unexpected("date/time arithmetic not supported");
        }
        bool left_double = std::holds_alternative<double>(left.value());
        bool right_double = std::holds_alternative<double>(right.value());
        if (left_double || right_double) {
            double lhs = left_double ? std::get<double>(left.value())
                                     : static_cast<double>(std::get<std::int64_t>(left.value()));
            double rhs = right_double ? std::get<double>(right.value())
                                      : static_cast<double>(std::get<std::int64_t>(right.value()));
            switch (binary->op) {
                case parser::BinaryOp::Add:
                    return runtime::ScalarValue{lhs + rhs};
                case parser::BinaryOp::Sub:
                    return runtime::ScalarValue{lhs - rhs};
                case parser::BinaryOp::Mul:
                    return runtime::ScalarValue{lhs * rhs};
                case parser::BinaryOp::Div:
                    return runtime::ScalarValue{lhs / rhs};
                case parser::BinaryOp::Mod:
                    return std::unexpected("mod not supported for float scalars");
                default:
                    return std::unexpected("unsupported operator in scalar expression");
            }
        }
        auto lhs = std::get<std::int64_t>(left.value());
        auto rhs = std::get<std::int64_t>(right.value());
        switch (binary->op) {
            case parser::BinaryOp::Add:
                return runtime::ScalarValue{lhs + rhs};
            case parser::BinaryOp::Sub:
                return runtime::ScalarValue{lhs - rhs};
            case parser::BinaryOp::Mul:
                return runtime::ScalarValue{lhs * rhs};
            case parser::BinaryOp::Div:
                return runtime::ScalarValue{lhs / rhs};
            case parser::BinaryOp::Mod:
                return runtime::ScalarValue{lhs % rhs};
            default:
                return std::unexpected("unsupported operator in scalar expression");
        }
    }
    if (auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (call->callee == "scalar") {
            if (call->args.size() != 2) {
                return std::unexpected("scalar() expects (table, column)");
            }
            auto column_name = column_name_from_expr(*call->args[1]);
            if (!column_name.has_value()) {
                return std::unexpected("scalar() column must be identifier or string");
            }
            auto table = eval_table_expr(*call->args[0], tables, scalars, columns, functions,
                                         extern_decls, externs);
            if (!table) {
                return std::unexpected(table.error());
            }
            auto scalar = runtime::extract_scalar(table.value(), *column_name);
            if (!scalar) {
                return std::unexpected(scalar.error());
            }
            return scalar.value();
        }
        if (functions.contains(call->callee)) {
            auto value = eval_function_call(*call, tables, scalars, columns, functions,
                                            extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                return *scalar;
            }
            return std::unexpected("function returned table where scalar expected");
        }
        if (extern_decls.contains(call->callee)) {
            const auto& decl = extern_decls.at(call->callee);
            if (decl.return_type.kind != parser::Type::Kind::Scalar) {
                return std::unexpected("extern function returns table: " + call->callee);
            }
            const auto* fn = externs.find(call->callee);
            if (fn == nullptr) {
                return std::unexpected("extern function not registered: " + call->callee);
            }
            if (fn->kind != runtime::ExternReturnKind::Scalar) {
                return std::unexpected("extern function returns table: " + call->callee);
            }
            if (fn->first_arg_is_table) {
                // First argument is a DataFrame; remaining arguments are scalars.
                if (call->args.empty()) {
                    return std::unexpected(call->callee + "() requires a DataFrame first argument");
                }
                auto table = eval_table_expr(*call->args[0], tables, scalars, columns, functions,
                                             extern_decls, externs);
                if (!table) {
                    return std::unexpected(table.error());
                }
                runtime::ExternArgs scalar_args;
                scalar_args.reserve(call->args.size() - 1);
                for (std::size_t i = 1; i < call->args.size(); ++i) {
                    auto value = eval_scalar_expr(*call->args[i], tables, scalars, columns,
                                                  functions, extern_decls, externs);
                    if (!value) {
                        return std::unexpected(value.error());
                    }
                    scalar_args.push_back(std::move(value.value()));
                }
                auto result = fn->table_consumer_func(table.value(), scalar_args);
                if (!result) {
                    return std::unexpected(result.error());
                }
                if (auto* scalar = std::get_if<runtime::ScalarValue>(&result.value())) {
                    return *scalar;
                }
                return std::unexpected("extern function returned table: " + call->callee);
            }
            runtime::ExternArgs args;
            args.reserve(call->args.size());
            for (const auto& arg : call->args) {
                auto value = eval_scalar_expr(*arg, tables, scalars, columns, functions,
                                              extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                args.push_back(std::move(value.value()));
            }
            auto result = fn->func(args);
            if (!result) {
                return std::unexpected(result.error());
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&result.value())) {
                return *scalar;
            }
            return std::unexpected("extern function returned table: " + call->callee);
        }
        return std::unexpected("unknown function: " + call->callee + " (available: " +
                               format_function_names(functions, extern_decls) + ")");
    }
    return std::unexpected("expected scalar expression");
}

auto eval_table_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                     runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                     const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                     const runtime::ExternRegistry& externs)
    -> std::expected<runtime::Table, std::string> {
    auto eval_extern_table_call =
        [&](parser::CallExpr& call) -> std::expected<runtime::Table, std::string> {
        const auto& decl = extern_decls.at(call.callee);
        if (decl.return_type.kind == parser::Type::Kind::Scalar) {
            return std::unexpected("extern function returns scalar: " + call.callee);
        }
        if (decl.return_type.kind == parser::Type::Kind::Series) {
            return std::unexpected("extern function returns column: " + call.callee);
        }
        const auto* fn = externs.find(call.callee);
        if (fn == nullptr) {
            return std::unexpected("extern function not registered: " + call.callee);
        }
        if (fn->kind != runtime::ExternReturnKind::Table) {
            return std::unexpected("extern function returns scalar: " + call.callee);
        }
        runtime::ExternArgs args;
        args.reserve(call.args.size());
        for (const auto& arg : call.args) {
            auto value =
                eval_scalar_expr(*arg, tables, scalars, columns, functions, extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            args.push_back(std::move(value.value()));
        }
        auto result = fn->func(args);
        if (!result) {
            return std::unexpected(result.error());
        }
        if (auto* table = std::get_if<runtime::Table>(&result.value())) {
            return std::move(*table);
        }
        return std::unexpected("extern function returned scalar: " + call.callee);
    };

    if (auto* block = std::get_if<parser::BlockExpr>(&expr.node)) {
        if (block->base && std::holds_alternative<parser::CallExpr>(block->base->node)) {
            auto* call = std::get_if<parser::CallExpr>(&block->base->node);
            if (call != nullptr && functions.contains(call->callee)) {
                auto value = eval_function_call(*call, tables, scalars, columns, functions,
                                                extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (!std::holds_alternative<runtime::Table>(*value)) {
                    return std::unexpected("function returned scalar where table expected");
                }
                auto temp_name = make_temp_table_name();
                tables.insert_or_assign(temp_name,
                                        std::get<runtime::Table>(std::move(value.value())));
                block->base =
                    std::make_unique<parser::Expr>(parser::Expr{parser::IdentifierExpr{temp_name}});
            } else if (call != nullptr && extern_decls.contains(call->callee)) {
                auto table = eval_extern_table_call(*call);
                if (!table) {
                    return std::unexpected(table.error());
                }
                auto temp_name = make_temp_table_name();
                tables.insert_or_assign(temp_name, std::move(table.value()));
                block->base =
                    std::make_unique<parser::Expr>(parser::Expr{parser::IdentifierExpr{temp_name}});
            }
        }
    }
    if (auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (functions.contains(call->callee)) {
            auto value = eval_function_call(*call, tables, scalars, columns, functions,
                                            extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                return std::move(*table);
            }
            return std::unexpected("function returned scalar where table expected");
        }
        if (extern_decls.contains(call->callee)) {
            return eval_extern_table_call(*call);
        }
    }
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        if (scalars.contains(ident->name)) {
            return std::unexpected(
                "expected table expression (known scalars: " + format_scalar_names(scalars) + ")");
        }
        if (columns.contains(ident->name)) {
            return std::unexpected("expected table expression (name refers to column)");
        }
        if (auto it = tables.find(ident->name); it != tables.end()) {
            return it->second;
        }
        return std::unexpected("unknown table: " + ident->name +
                               " (available: " + format_table_names(tables) + ")");
    }
    parser::LowerContext context;
    auto lowered = parser::lower_expr(expr, context);
    if (!lowered) {
        return std::unexpected(lowered.error().message);
    }
    auto evaluated = runtime::interpret(*lowered.value(), tables, &scalars, &externs);
    if (!evaluated) {
        return std::unexpected(evaluated.error());
    }
    return std::move(evaluated.value());
}

auto eval_function_call(parser::CallExpr& call, runtime::TableRegistry& tables,
                        runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                        const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
                        const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string> {
    auto it = functions.find(call.callee);
    if (it == functions.end()) {
        return std::unexpected("unknown function: " + call.callee + " (available: " +
                               format_function_names(functions, extern_decls) + ")");
    }
    const auto& fn = it->second;
    if (call.args.size() != fn.params.size()) {
        return std::unexpected("function argument count mismatch");
    }

    runtime::TableRegistry local_tables = tables;
    runtime::ScalarRegistry local_scalars = scalars;
    ColumnRegistry local_columns = columns;

    for (std::size_t i = 0; i < fn.params.size(); ++i) {
        const auto& param = fn.params[i];
        auto& arg = *call.args[i];
        switch (param.type.kind) {
            case parser::Type::Kind::Scalar: {
                auto value = eval_scalar_expr(arg, tables, scalars, columns, functions,
                                              extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_scalars.insert_or_assign(param.name, std::move(value.value()));
                break;
            }
            case parser::Type::Kind::DataFrame:
            case parser::Type::Kind::TimeFrame: {
                auto value = eval_table_expr(arg, tables, scalars, columns, functions, extern_decls,
                                             externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_tables.insert_or_assign(param.name, std::move(value.value()));
                break;
            }
            case parser::Type::Kind::Series:
                if (auto value = eval_expr_value(arg, tables, scalars, columns, functions,
                                                 extern_decls, externs)) {
                    if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                        local_columns.insert_or_assign(param.name, std::move(*col));
                        break;
                    }
                    if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                        if (table->columns.size() != 1) {
                            return std::unexpected("Column argument must have exactly one column");
                        }
                        local_columns.insert_or_assign(param.name, *table->columns.front().column);
                        break;
                    }
                    return std::unexpected("Column argument must be a column or table");
                } else {
                    return std::unexpected(value.error());
                }
        }
    }

    std::optional<EvalValue> last_value;
    for (const auto& stmt : fn.body) {
        if (std::holds_alternative<parser::LetStmt>(stmt)) {
            const auto& let_stmt = std::get<parser::LetStmt>(stmt);
            bool type_is_scalar =
                let_stmt.type.has_value() && let_stmt.type->kind == parser::Type::Kind::Scalar;
            bool type_is_table = let_stmt.type.has_value() &&
                                 (let_stmt.type->kind == parser::Type::Kind::DataFrame ||
                                  let_stmt.type->kind == parser::Type::Kind::TimeFrame);
            if (type_is_scalar) {
                auto value = eval_scalar_expr(*let_stmt.value, local_tables, local_scalars,
                                              local_columns, functions, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_scalars.insert_or_assign(let_stmt.name, std::move(value.value()));
            } else if (type_is_table) {
                auto value = eval_table_expr(*let_stmt.value, local_tables, local_scalars,
                                             local_columns, functions, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_tables.insert_or_assign(let_stmt.name, std::move(value.value()));
            } else if (let_stmt.type.has_value() &&
                       let_stmt.type->kind == parser::Type::Kind::Series) {
                auto value = eval_expr_value(*let_stmt.value, local_tables, local_scalars,
                                             local_columns, functions, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                    local_columns.insert_or_assign(let_stmt.name, std::move(*col));
                } else if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                    if (table->columns.size() != 1) {
                        return std::unexpected("Column binding must have exactly one column");
                    }
                    local_columns.insert_or_assign(let_stmt.name, *table->columns.front().column);
                } else {
                    return std::unexpected("Column binding must be a column or table");
                }
            } else {
                auto value = eval_expr_value(*let_stmt.value, local_tables, local_scalars,
                                             local_columns, functions, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                    local_scalars.insert_or_assign(let_stmt.name, std::move(*scalar));
                } else if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                    local_columns.insert_or_assign(let_stmt.name, std::move(*col));
                } else {
                    local_tables.insert_or_assign(
                        let_stmt.name, std::get<runtime::Table>(std::move(value.value())));
                }
            }
            continue;
        }
        const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
        auto value = eval_expr_value(*expr_stmt.expr, local_tables, local_scalars, local_columns,
                                     functions, extern_decls, externs);
        if (!value) {
            return std::unexpected(value.error());
        }
        last_value = std::move(value.value());
    }

    if (!last_value.has_value()) {
        return std::unexpected("function has no return expression");
    }

    if (fn.return_type.kind == parser::Type::Kind::Scalar) {
        if (!std::holds_alternative<runtime::ScalarValue>(*last_value)) {
            return std::unexpected("function return type mismatch (expected scalar)");
        }
        return EvalValue{std::get<runtime::ScalarValue>(std::move(*last_value))};
    }
    if (fn.return_type.kind == parser::Type::Kind::DataFrame ||
        fn.return_type.kind == parser::Type::Kind::TimeFrame) {
        if (!std::holds_alternative<runtime::Table>(*last_value)) {
            return std::unexpected("function return type mismatch (expected table)");
        }
        return EvalValue{std::get<runtime::Table>(std::move(*last_value))};
    }

    if (fn.return_type.kind == parser::Type::Kind::Series) {
        if (auto* col = std::get_if<runtime::ColumnValue>(&last_value.value())) {
            return EvalValue{std::move(*col)};
        }
        if (auto* table = std::get_if<runtime::Table>(&last_value.value())) {
            if (table->columns.size() != 1) {
                return std::unexpected("Column return must have exactly one column");
            }
            return EvalValue{*table->columns.front().column};
        }
        return std::unexpected("function return type mismatch (expected column)");
    }

    return std::unexpected("unsupported return type");
}

/// Derive the plugin filename stem from a source_path like "csv.hpp" -> "csv".
auto plugin_stem(const std::string& source_path) -> std::string {
    std::filesystem::path p(source_path);
    return p.stem().string();
}

/// Try to load a plugin shared library for the given stem from the search paths.
/// Returns true if the plugin was loaded (or was already loaded), false on failure.
enum class PluginLoadStatus : std::uint8_t { Loaded, NotFound, LoadError };

struct PluginLoadResult {
    PluginLoadStatus status;
    std::string message;
};

auto try_load_plugin(const std::string& stem, const std::vector<std::string>& search_paths,
                     std::unordered_set<std::string>& loaded_plugins,
                     runtime::ExternRegistry& externs) -> PluginLoadResult {
    if (loaded_plugins.contains(stem)) {
        return {PluginLoadStatus::Loaded, ""};
    }
    std::string filename = stem + ".so";
    std::string last_error;
    std::string last_candidate;
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
        void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            if (std::filesystem::exists(full_path)) {
                if (const char* err = dlerror()) {
                    last_error = err;
                }
                last_candidate = full_path.string();
            }
            continue;
        }
        using RegisterFn = void (*)(runtime::ExternRegistry*);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        auto* fn = reinterpret_cast<RegisterFn>(dlsym(handle, "ibex_register"));
        if (fn == nullptr) {
            dlclose(handle);
            fmt::print("warning: plugin '{}' has no ibex_register symbol\n", full_path.string());
            continue;
        }
        fn(&externs);
        loaded_plugins.insert(stem);
        spdlog::debug("loaded plugin: {}", full_path.string());
        return {PluginLoadStatus::Loaded, ""};
    }
    if (!last_candidate.empty()) {
        return {PluginLoadStatus::LoadError,
                fmt::format("failed to load '{}': {}", last_candidate,
                            last_error.empty() ? "unknown error" : last_error)};
    }
    return {PluginLoadStatus::NotFound, ""};
}

/// Locate <name>.ibex in the plugin search paths and return its contents.
/// Returns std::nullopt when the file is not found in any search path.
auto find_library_source(const std::string& name,
                         const std::vector<std::string>& search_paths)
    -> std::optional<std::string> {
    std::string filename = name + ".ibex";
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
        std::ifstream in{full_path};
        if (!in) {
            continue;
        }
        std::string source((std::istreambuf_iterator<char>(in)),
                           std::istreambuf_iterator<char>());
        spdlog::debug("import: found library '{}' at {}", name, full_path.string());
        return source;
    }
    return std::nullopt;
}

auto execute_statements(std::vector<parser::Stmt>& statements, runtime::TableRegistry& tables,
                        runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                        FunctionRegistry& functions, ExternDeclRegistry& extern_decls,
                        runtime::ExternRegistry& externs,
                        const std::vector<std::string>& plugin_search_paths,
                        std::unordered_set<std::string>& loaded_plugins,
                        const std::vector<std::string>& import_search_paths = {},
                        const std::vector<std::vector<std::string>>* comment_groups = nullptr)
    -> bool {
    for (std::size_t stmt_index = 0; stmt_index < statements.size(); ++stmt_index) {
        auto& stmt = statements[stmt_index];
        if (comment_groups != nullptr && stmt_index < comment_groups->size()) {
            print_comment_group((*comment_groups)[stmt_index]);
        }
        if (std::holds_alternative<parser::ExternDecl>(stmt)) {
            const auto& decl = std::get<parser::ExternDecl>(stmt);
            extern_decls.insert_or_assign(decl.name, decl);
            if (!decl.source_path.empty()) {
                auto stem = plugin_stem(decl.source_path);
                auto result = try_load_plugin(stem, plugin_search_paths, loaded_plugins, externs);
                if (result.status == PluginLoadStatus::NotFound) {
                    fmt::print("warning: could not find plugin '{}.so' in search path\n", stem);
                } else if (result.status == PluginLoadStatus::LoadError) {
                    fmt::print("warning: {}\n", result.message);
                }
            }
            continue;
        }
        if (std::holds_alternative<parser::ImportDecl>(stmt)) {
            const auto& imp = std::get<parser::ImportDecl>(stmt);
            // Prefer explicit import_search_paths; fall back to plugin_search_paths so
            // that .ibex stubs can live alongside their .so files in the same directory.
            const auto& primary_paths =
                import_search_paths.empty() ? plugin_search_paths : import_search_paths;
            auto source = find_library_source(imp.name, primary_paths);
            // If not found in primary, also check plugin_search_paths when import_search_paths
            // were explicitly provided.
            if (!source.has_value() && !import_search_paths.empty()) {
                source = find_library_source(imp.name, plugin_search_paths);
            }
            if (!source.has_value()) {
                fmt::print("error: import '{}': could not find '{}.ibex' in search path\n",
                           imp.name, imp.name);
                return false;
            }
            auto parsed = parser::parse(*source);
            if (!parsed) {
                fmt::print("error: import '{}': {}\n", imp.name, parsed.error().format());
                return false;
            }
            // Recursively execute the imported file's statements (which will typically
            // only contain extern fn and fn declarations).
            if (!execute_statements(parsed->statements, tables, scalars, columns, functions,
                                    extern_decls, externs, plugin_search_paths, loaded_plugins,
                                    import_search_paths)) {
                return false;
            }
            continue;
        }
        if (std::holds_alternative<parser::FunctionDecl>(stmt)) {
            auto fn = std::get<parser::FunctionDecl>(std::move(stmt));
            functions.insert_or_assign(fn.name, std::move(fn));
            continue;
        }
        if (std::holds_alternative<parser::LetStmt>(stmt)) {
            const auto& let_stmt = std::get<parser::LetStmt>(stmt);
            if (let_stmt.type.has_value()) {
                bool expect_scalar = let_stmt.type->kind == parser::Type::Kind::Scalar;
                bool expect_table = let_stmt.type->kind == parser::Type::Kind::DataFrame ||
                                    let_stmt.type->kind == parser::Type::Kind::TimeFrame;
                bool expect_column = let_stmt.type->kind == parser::Type::Kind::Series;
                if (expect_scalar) {
                    auto value = eval_scalar_expr(*let_stmt.value, tables, scalars, columns,
                                                  functions, extern_decls, externs);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        return false;
                    }
                    scalars.insert_or_assign(let_stmt.name, std::move(value.value()));
                    continue;
                }
                if (expect_table) {
                    auto value = eval_table_expr(*let_stmt.value, tables, scalars, columns,
                                                 functions, extern_decls, externs);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        return false;
                    }
                    tables.insert_or_assign(let_stmt.name, std::move(value.value()));
                    continue;
                }
                if (expect_column) {
                    auto value = eval_expr_value(*let_stmt.value, tables, scalars, columns,
                                                 functions, extern_decls, externs);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        return false;
                    }
                    if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                        columns.insert_or_assign(let_stmt.name, std::move(*col));
                        continue;
                    }
                    if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                        if (table->columns.size() != 1) {
                            fmt::print("error: column return must have exactly one column\n");
                            return false;
                        }
                        columns.insert_or_assign(let_stmt.name, *table->columns.front().column);
                        continue;
                    }
                    fmt::print("error: expected column return for {}\n", let_stmt.name);
                    return false;
                }
            }

            auto value = eval_expr_value(*let_stmt.value, tables, scalars, columns, functions,
                                         extern_decls, externs);
            if (!value) {
                fmt::print("error: {}\n", value.error());
                return false;
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                scalars.insert_or_assign(let_stmt.name, std::move(*scalar));
            } else if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                columns.insert_or_assign(let_stmt.name, std::move(*col));
            } else {
                tables.insert_or_assign(let_stmt.name,
                                        std::get<runtime::Table>(std::move(value.value())));
            }
            continue;
        }
        if (std::holds_alternative<parser::ExprStmt>(stmt)) {
            const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
            auto value = eval_expr_value(*expr_stmt.expr, tables, scalars, columns, functions,
                                         extern_decls, externs);
            if (!value) {
                fmt::print("error: {}\n", value.error());
                return false;
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                fmt::print("{}\n", format_scalar(*scalar));
            } else if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                runtime::Table temp;
                temp.add_column("column", *col);
                print_table(temp);
            } else {
                print_table(std::get<runtime::Table>(std::move(value.value())));
            }
        }
    }
    return true;
}

}  // namespace

auto normalize_input(std::string_view input) -> std::string {
    std::string normalized(input);
    auto last_non_space = normalized.find_last_not_of(" \t\r\n");
    if (last_non_space != std::string::npos && normalized[last_non_space] != ';') {
        normalized.push_back(';');
    }
    return normalized;
}

auto execute_script(std::string_view source, runtime::ExternRegistry& registry) -> bool {
    auto parsed = parser::parse(source);
    if (!parsed) {
        fmt::print("error: {}\n", parsed.error().format());
        return false;
    }
    auto tables = build_builtin_tables();
    runtime::ScalarRegistry scalars;
    ColumnRegistry columns;
    FunctionRegistry functions;
    ExternDeclRegistry extern_decls;
    std::vector<std::string> no_paths;
    std::unordered_set<std::string> loaded_plugins;
    return execute_statements(parsed->statements, tables, scalars, columns, functions, extern_decls,
                              registry, no_paths, loaded_plugins);
}

void run(const ReplConfig& config, runtime::ExternRegistry& registry) {
    if (config.verbose) {
        spdlog::info("Ibex REPL started (verbose={})", config.verbose);
    }

    auto tables = build_builtin_tables();
    runtime::ScalarRegistry scalars;
    ColumnRegistry columns;
    FunctionRegistry functions;
    ExternDeclRegistry extern_decls;
    std::unordered_set<std::string> loaded_plugins;
    bool timing_enabled = false;
    bool load_comments_enabled = false;
    configure_line_editing();

    std::string line;
    while (true) {
        if (!read_repl_line(config.prompt, line)) {
            fmt::print("\n");
            break;
        }

        if (line.empty()) {
            continue;
        }

        std::string_view line_view(line);

        if (starts_with_command(line_view, ":timing")) {
            auto arg = trim(line_view.substr(std::string_view(":timing").size()));
            if (arg.empty()) {
                timing_enabled = !timing_enabled;
            } else if (arg == "on") {
                timing_enabled = true;
            } else if (arg == "off") {
                timing_enabled = false;
            } else {
                fmt::print("usage: :timing [on|off]\n");
                continue;
            }
            fmt::print("timing: {}\n", timing_enabled ? "on" : "off");
            continue;
        }
        if (starts_with_command(line_view, ":comments")) {
            auto arg = trim(line_view.substr(std::string_view(":comments").size()));
            if (arg.empty()) {
                load_comments_enabled = !load_comments_enabled;
            } else if (arg == "on") {
                load_comments_enabled = true;
            } else if (arg == "off") {
                load_comments_enabled = false;
            } else {
                fmt::print("usage: :comments [on|off]\n");
                continue;
            }
            fmt::print("load comments: {}\n", load_comments_enabled ? "on" : "off");
            continue;
        }

        bool one_shot_timing = false;
        if (starts_with_command(line_view, ":time")) {
            auto timed_input = trim(line_view.substr(std::string_view(":time").size()));
            if (timed_input.empty()) {
                fmt::print("usage: :time <command>\n");
                continue;
            }
            line = std::string(timed_input);
            line_view = std::string_view(line);
            one_shot_timing = true;
        }

        const bool should_time = timing_enabled || one_shot_timing;
        std::optional<std::chrono::steady_clock::time_point> timing_start;
        if (should_time) {
            timing_start = std::chrono::steady_clock::now();
        }
        auto report_timing = [&]() {
            if (timing_start.has_value()) {
                print_elapsed(std::chrono::steady_clock::now() - *timing_start);
            }
        };

        if (line == ":q" || line == ":quit" || line == ":exit") {
            break;
        }
        if (line == ":tables") {
            print_tables(tables);
            continue;
        }
        if (line == ":scalars") {
            print_scalars(scalars);
            continue;
        }
        if (line_view.starts_with(":schema")) {
            auto arg = trim(line_view.substr(std::string_view(":schema").size()));
            if (arg.empty()) {
                fmt::print("usage: :schema <table>\n");
                continue;
            }
            auto it = tables.find(std::string(arg));
            if (it == tables.end()) {
                fmt::print("error: unknown table '{}'\n", arg);
                continue;
            }
            print_schema(it->second);
            continue;
        }
        if (line_view.starts_with(":head")) {
            auto rest = trim(line_view.substr(std::string_view(":head").size()));
            auto space = rest.find(' ');
            std::string_view name = rest;
            std::string_view count_text;
            if (space != std::string_view::npos) {
                name = trim(rest.substr(0, space));
                count_text = trim(rest.substr(space + 1));
            }
            if (name.empty()) {
                fmt::print("usage: :head <table> [n]\n");
                continue;
            }
            auto it = tables.find(std::string(name));
            if (it == tables.end()) {
                fmt::print("error: unknown table '{}'\n", name);
                continue;
            }
            std::size_t count = parse_optional_size(count_text, 10);
            print_table(it->second, count);
            continue;
        }
        if (line_view.starts_with(":describe")) {
            auto rest = trim(line_view.substr(std::string_view(":describe").size()));
            auto space = rest.find(' ');
            std::string_view name = rest;
            std::string_view count_text;
            if (space != std::string_view::npos) {
                name = trim(rest.substr(0, space));
                count_text = trim(rest.substr(space + 1));
            }
            if (name.empty()) {
                fmt::print("usage: :describe <table> [n]\n");
                continue;
            }
            auto it = tables.find(std::string(name));
            if (it == tables.end()) {
                fmt::print("error: unknown table '{}'\n", name);
                continue;
            }
            std::size_t count = parse_optional_size(count_text, 10);
            describe_table(it->second, count);
            continue;
        }
        if (line_view.starts_with(":load")) {
            auto arg_view = trim(line_view.substr(std::string_view(":load").size()));
            if (arg_view.empty()) {
                fmt::print("usage: :load <file>\n");
                report_timing();
                continue;
            }
            std::string path = parse_load_path(arg_view);
            if (path.empty()) {
                fmt::print("usage: :load <file>\n");
                report_timing();
                continue;
            }
            std::ifstream input{path};
            if (!input) {
                fmt::print("error: failed to open '{}'\n", path);
                report_timing();
                continue;
            }
            std::string source((std::istreambuf_iterator<char>(input)),
                               std::istreambuf_iterator<char>());
            auto parsed = parser::parse(source);
            if (!parsed) {
                fmt::print("error: {}\n", parsed.error().format());
                report_timing();
                continue;
            }
            std::vector<std::vector<std::string>> comment_groups;
            const std::vector<std::vector<std::string>>* comment_groups_ptr = nullptr;
            if (load_comments_enabled) {
                auto comments = collect_script_comment_lines(source);
                comment_groups = build_statement_comment_groups(parsed->statements, comments);
                comment_groups_ptr = &comment_groups;
            }
            if (!execute_statements(parsed->statements, tables, scalars, columns, functions,
                                    extern_decls, registry, config.plugin_search_paths,
                                    loaded_plugins, config.import_search_paths,
                                    comment_groups_ptr)) {
                report_timing();
                continue;
            }
            report_timing();
            continue;
        }

        auto normalized = normalize_input(line);
        auto parsed = parser::parse(normalized);
        if (!parsed) {
            fmt::print("error: {}\n", parsed.error().format());
            continue;
        }

        execute_statements(parsed->statements, tables, scalars, columns, functions, extern_decls,
                           registry, config.plugin_search_paths, loaded_plugins,
                           config.import_search_paths);
        report_timing();
    }

    spdlog::info("Ibex REPL exiting");
}

}  // namespace ibex::repl
