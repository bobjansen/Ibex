#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/required_columns.hpp>
#include <ibex/ir/scan_predicates.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/rng.hpp>
#include <ibex/runtime/safe_arith.hpp>
#include <ibex/runtime/table_format.hpp>

#include <fmt/base.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <iterator>
#include <memory>
#include <string.h>
#include <string_view>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>
#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <dlfcn.h>
#include <signal.h>
#endif
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
#include <system_error>
#include <variant>

#ifdef IBEX_HAS_READLINE
#include <readline/history.h>
#include <readline/readline.h>
#endif

namespace ibex::repl {

namespace {

using FunctionRegistry = robin_hood::unordered_map<std::string, parser::FunctionDecl>;
using ExternDeclRegistry = robin_hood::unordered_map<std::string, parser::ExternDecl>;
using ColumnRegistry = robin_hood::unordered_map<std::string, runtime::ColumnValue>;
/// Table bindings whose source can decode columns selectively, held unread.
///
/// Kept beside `TableRegistry` rather than inside it: a lookup site that has
/// not been taught about laziness then fails loudly with "unknown table"
/// instead of quietly receiving a table missing the columns nobody asked for.
/// A binding lives in exactly one of the two registries.
using LazyTableRegistry = robin_hood::unordered_map<std::string, runtime::LazyTablePtr>;
using ModelRegistry = robin_hood::unordered_map<std::string, runtime::ModelResult>;
using CompileTimeListRegistry = robin_hood::unordered_map<std::string, std::vector<std::string>>;
using FunctionSourceRegistry = robin_hood::unordered_map<std::string, std::string>;
using DeclarationDocRegistry = robin_hood::unordered_map<std::string, std::string>;
using ImportRegistry = robin_hood::unordered_set<std::string>;
using EvalValue = std::variant<runtime::Table, runtime::ScalarValue, runtime::ColumnValue>;

struct BoundCallArg {
    const parser::Param* param = nullptr;
    parser::Expr* expr = nullptr;
    bool is_default = false;
};

struct CompletionContext {
    const runtime::TableRegistry* tables = nullptr;
    const runtime::ScalarRegistry* scalars = nullptr;
    const ColumnRegistry* columns = nullptr;
    const ModelRegistry* models = nullptr;
    const FunctionRegistry* functions = nullptr;
    const CompileTimeListRegistry* compile_time_lists = nullptr;
    const ExternDeclRegistry* extern_decls = nullptr;
    const ImportRegistry* imports = nullptr;
};

/// Result of one prompt read. `Interrupted` means the user pressed Ctrl+C
/// at the prompt: the line is discarded and the loop shows a fresh prompt.
enum class ReadLineStatus : std::uint8_t { Line, Eof, Interrupted };

#ifdef IBEX_HAS_READLINE
constexpr std::string_view kColonCommands[] = {
    ":q",         ":quit",    ":exit",   ":help",     ":tables", ":scalars",
    ":functions", ":imports", ":schema", ":head",     ":peek",   ":describe",
    ":load",      ":timing",  ":time",   ":comments", ":doc",    ":source",
};

constexpr std::string_view kCompletionBuiltins[] = {
    "Bool",
    "Date",
    "Float32",
    "Float64",
    "Int",
    "Int32",
    "Int64",
    "String",
    "Timestamp",
    "abs",
    "as_timeframe",
    "bankers",
    "ceil",
    "coef",
    "columns",
    "count",
    "cumprod",
    "cumsum",
    "ewma",
    "exp",
    "fill_backward",
    "fill_forward",
    "fill_null",
    "fitted",
    "floor",
    "first",
    "get",
    "is_nan",
    "is_not_null",
    "is_null",
    "kurtosis",
    "lag",
    "last",
    "lead",
    "log",
    "matmul",
    "max",
    "mean",
    "median",
    "min",
    "model_coef",
    "model_fitted",
    "model_importance",
    "model_predict",
    "model_residuals",
    "model_r_squared",
    "model_summary",
    "nearest",
    "nrow",
    "null_if_nan",
    "null_if_not_finite",
    "pmax",
    "pmin",
    "print",
    "quantile",
    "r_squared",
    "rand_bernoulli",
    "rand_exponential",
    "rand_gamma",
    "rand_int",
    "rand_normal",
    "rand_poisson",
    "rand_student_t",
    "rand_uniform",
    "rbind",
    "rep",
    "residuals",
    "rolling_count",
    "rolling_ewma",
    "rolling_kurtosis",
    "rolling_max",
};

constexpr std::string_view kMoreCompletionBuiltins[] = {
    "rolling_mean", "rolling_median",
    "rolling_min",  "rolling_quantile",
    "rolling_skew", "rolling_std",
    "rolling_sum",  "round",
    "scalar",       "seed_rng",
    "skew",         "sqrt",
    "std",          "sum",
    "summary",      "trunc",
    "filter",       "select",
    "update",       "by",
    "window",       "order",
    "rename",       "distinct",
    "head",         "tail",
    "top",          "melt",
    "dcast",        "join",
    "on",           "sin",
    "cos",          "tan",
    "asin",         "acos",
    "atan",         "sinh",
    "cosh",         "tanh",
    "log2",         "log10",
};

CompletionContext g_completion_context;
std::vector<std::string> g_completion_candidates;

void set_completion_context(CompletionContext context) {
    g_completion_context = context;
}

auto is_ident_char(char c) -> bool {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
}

auto trim_left(std::string_view text) -> std::string_view {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    return text;
}

auto command_matches(std::string_view line, std::string_view command) -> bool {
    line = trim_left(line);
    return line.starts_with(command) &&
           (line.size() == command.size() ||
            std::isspace(static_cast<unsigned char>(line[command.size()])) != 0);
}

auto identifier_before(std::string_view line, std::size_t pos) -> std::string {
    pos = std::min(pos, line.size());
    while (pos > 0 && std::isspace(static_cast<unsigned char>(line[pos - 1])) != 0) {
        --pos;
    }
    const auto end = pos;
    while (pos > 0 && is_ident_char(line[pos - 1])) {
        --pos;
    }
    if (pos == end) {
        return {};
    }
    return std::string(line.substr(pos, end - pos));
}

auto table_context_for_line(std::string_view line, std::size_t cursor) -> const runtime::Table* {
    if (g_completion_context.tables == nullptr) {
        return nullptr;
    }
    cursor = std::min(cursor, line.size());
    std::size_t active_bracket = std::string_view::npos;
    int depth = 0;
    for (std::size_t i = 0; i < cursor; ++i) {
        if (line[i] == '[') {
            ++depth;
            active_bracket = i;
        } else if (line[i] == ']' && depth > 0) {
            --depth;
            if (depth == 0) {
                active_bracket = std::string_view::npos;
            }
        }
    }
    if (active_bracket == std::string_view::npos) {
        return nullptr;
    }
    auto table_name = identifier_before(line, active_bracket);
    if (table_name.empty()) {
        return nullptr;
    }
    auto it = g_completion_context.tables->find(table_name);
    return it == g_completion_context.tables->end() ? nullptr : &it->second;
}

void add_candidate(std::vector<std::string>& candidates, std::string_view candidate) {
    if (!candidate.empty()) {
        candidates.emplace_back(candidate);
    }
}

template <typename Map>
void add_map_keys(std::vector<std::string>& candidates, const Map* map) {
    if (map == nullptr) {
        return;
    }
    candidates.reserve(candidates.size() + map->size());
    for (const auto& entry : *map) {
        candidates.push_back(entry.first);
    }
}

void add_set_values(std::vector<std::string>& candidates, const ImportRegistry* set) {
    if (set == nullptr) {
        return;
    }
    candidates.reserve(candidates.size() + set->size());
    for (const auto& value : *set) {
        candidates.push_back(value);
    }
}

void add_table_columns(std::vector<std::string>& candidates, const runtime::Table* table) {
    if (table == nullptr) {
        return;
    }
    candidates.reserve(candidates.size() + table->columns.size());
    for (const auto& column : table->columns) {
        candidates.push_back(column.name);
    }
}

void add_static_candidates(std::vector<std::string>& candidates) {
    for (auto value : kCompletionBuiltins) {
        add_candidate(candidates, value);
    }
    for (auto value : kMoreCompletionBuiltins) {
        add_candidate(candidates, value);
    }
}

auto unique_sorted(std::vector<std::string> candidates) -> std::vector<std::string> {
    std::sort(candidates.begin(), candidates.end());
    candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());
    return candidates;
}

auto any_prefix_match(const std::vector<std::string>& candidates, std::string_view prefix) -> bool {
    if (prefix.empty()) {
        return false;
    }
    return std::any_of(candidates.begin(), candidates.end(),
                       [&](const auto& candidate) { return candidate.starts_with(prefix); });
}

auto completion_generator(const char* text, int state) -> char* {
    static std::size_t index = 0;
    static std::string prefix;
    if (state == 0) {
        index = 0;
        prefix = text != nullptr ? text : "";
    }
    while (index < g_completion_candidates.size()) {
        const auto& candidate = g_completion_candidates[index++];
        if (candidate.starts_with(prefix)) {
            return ::strdup(candidate.c_str());
        }
    }
    return nullptr;
}

auto matches_from(std::vector<std::string> candidates, const char* text) -> char** {
    g_completion_candidates = unique_sorted(std::move(candidates));
    rl_attempted_completion_over = 1;
    return rl_completion_matches(text, completion_generator);
}

auto expression_completion_candidates(std::string_view line, std::size_t cursor,
                                      std::string_view prefix) -> std::vector<std::string> {
    std::vector<std::string> candidates;
    if (const auto* table = table_context_for_line(line, cursor); table != nullptr) {
        add_table_columns(candidates, table);
        if (any_prefix_match(candidates, prefix)) {
            return candidates;
        }
        add_static_candidates(candidates);
        add_map_keys(candidates, g_completion_context.scalars);
        add_map_keys(candidates, g_completion_context.columns);
        add_map_keys(candidates, g_completion_context.functions);
        add_map_keys(candidates, g_completion_context.extern_decls);
        add_map_keys(candidates, g_completion_context.compile_time_lists);
        add_set_values(candidates, g_completion_context.imports);
        return candidates;
    }

    add_map_keys(candidates, g_completion_context.tables);
    add_map_keys(candidates, g_completion_context.scalars);
    add_map_keys(candidates, g_completion_context.columns);
    add_map_keys(candidates, g_completion_context.models);
    add_map_keys(candidates, g_completion_context.functions);
    add_map_keys(candidates, g_completion_context.extern_decls);
    add_map_keys(candidates, g_completion_context.compile_time_lists);
    add_set_values(candidates, g_completion_context.imports);
    if (any_prefix_match(candidates, prefix)) {
        return candidates;
    }
    add_static_candidates(candidates);
    add_candidate(candidates, "let");
    add_candidate(candidates, "fn");
    add_candidate(candidates, "extern");
    add_candidate(candidates, "import");
    add_candidate(candidates, "model");
    add_candidate(candidates, "Table");
    add_candidate(candidates, "true");
    add_candidate(candidates, "false");
    return candidates;
}

auto default_history_path() -> std::string {
    if (const char* env = std::getenv("IBEX_HISTORY_FILE"); env != nullptr && env[0] != '\0') {
        return env;
    }
    if (const char* home = std::getenv("HOME"); home != nullptr && home[0] != '\0') {
        return (std::filesystem::path(home) / ".ibex_history").string();
    }
#ifdef _WIN32
    if (const char* profile = std::getenv("USERPROFILE");
        profile != nullptr && profile[0] != '\0') {
        return (std::filesystem::path(profile) / ".ibex_history").string();
    }
#endif
    return {};
}

auto resolve_history_path(const ReplConfig& config) -> std::string {
    if (!config.persistent_history) {
        return {};
    }
    if (!config.history_path.empty()) {
        return config.history_path;
    }
    return default_history_path();
}

void load_history_file(const std::string& path, std::size_t limit) {
    if (path.empty()) {
        return;
    }
    ::using_history();
    if (limit > 0) {
        ::stifle_history(static_cast<int>(std::min<std::size_t>(
            limit, static_cast<std::size_t>(std::numeric_limits<int>::max()))));
    }
    const int rc = ::read_history(path.c_str());
    if (rc != 0 && rc != ENOENT) {
        spdlog::debug("failed to read REPL history '{}': {}", path, std::strerror(rc));
    }
}

void save_history_file(const std::string& path, std::size_t limit) {
    if (path.empty()) {
        return;
    }
    std::error_code ec;
    if (auto parent = std::filesystem::path(path).parent_path(); !parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            spdlog::debug("failed to create REPL history directory '{}': {}", parent.string(),
                          ec.message());
            return;
        }
    }
    const int write_rc = ::write_history(path.c_str());
    if (write_rc != 0) {
        spdlog::debug("failed to write REPL history '{}': {}", path, std::strerror(write_rc));
        return;
    }
    if (limit > 0) {
        const int truncate_rc = ::history_truncate_file(
            path.c_str(), static_cast<int>(std::min<std::size_t>(
                              limit, static_cast<std::size_t>(std::numeric_limits<int>::max()))));
        if (truncate_rc != 0) {
            spdlog::debug("failed to truncate REPL history '{}': {}", path,
                          std::strerror(truncate_rc));
        }
    }
}

auto repl_completion(const char* text, int start, int end) -> char** {
    const std::string_view line = rl_line_buffer != nullptr ? std::string_view(rl_line_buffer) : "";
    if (start == 0 && text != nullptr && text[0] == ':') {
        std::vector<std::string> commands;
        commands.reserve(std::size(kColonCommands));
        for (auto command : kColonCommands) {
            commands.emplace_back(command);
        }
        return matches_from(std::move(commands), text);
    }

    if (command_matches(line, ":load")) {
        rl_attempted_completion_over = 1;
        return rl_completion_matches(text, rl_filename_completion_function);
    }

    if (command_matches(line, ":schema") || command_matches(line, ":head") ||
        command_matches(line, ":describe")) {
        std::vector<std::string> tables;
        add_map_keys(tables, g_completion_context.tables);
        return matches_from(std::move(tables), text);
    }

    if (command_matches(line, ":source")) {
        std::vector<std::string> functions;
        add_map_keys(functions, g_completion_context.functions);
        return matches_from(std::move(functions), text);
    }

    if (command_matches(line, ":doc") || command_matches(line, ":help")) {
        return matches_from(expression_completion_candidates(
                                line, static_cast<std::size_t>(end),
                                text != nullptr ? std::string_view(text) : std::string_view{}),
                            text);
    }

    if (line.starts_with(':')) {
        return nullptr;
    }

    return matches_from(expression_completion_candidates(
                            line, static_cast<std::size_t>(end),
                            text != nullptr ? std::string_view(text) : std::string_view{}),
                        text);
}

/// Polled by readline roughly 10x/second while it waits for input (and right
/// after a signal EINTRs the wait). On Ctrl+C, discard the pending line and
/// set rl_done, which makes rl_read_key() hand back a synthetic newline so
/// readline() returns the (emptied) line to the caller. rl_done is only
/// honored on the rl_event_hook path — rl_signal_event_hook does not unblock
/// rl_getc's internal read loop.
auto interrupt_event_hook() -> int {
    if (runtime::interrupt_requested()) {
        rl_replace_line("", 0);
        rl_done = 1;
    }
    return 0;
}

void configure_line_editing() {
    rl_attempted_completion_function = repl_completion;

    // The event hook is installed ONLY for an interactive terminal, and that is
    // load-bearing rather than an optimization.
    //
    // readline's event-hook wait loop calls `rl_gather_tyi()`, which returns 0
    // both for "no input has arrived yet" and for "the input is at EOF" — it does
    // not distinguish them. With a hook installed, readline therefore never
    // reports EOF on a non-terminal stdin: it spins, calling the hook forever, at
    // 100% CPU. A script piped or redirected into the REPL (a supported mode)
    // would hang on its last line instead of exiting, burning a core.
    //
    // Nothing is lost by skipping it: the hook exists solely to make Ctrl+C
    // interrupt an interactive *prompt*, and a redirected stdin has no prompt to
    // interrupt. Ctrl+C during evaluation is handled elsewhere, by the
    // interpreter's cooperative interrupt checks.
    if (::isatty(STDIN_FILENO) != 0) {
        rl_event_hook = interrupt_event_hook;
    }

#if defined(RL_READLINE_VERSION) && RL_READLINE_VERSION >= 0x0500
    // The REPL owns SIGINT (see install_interrupt_handler): GNU readline must
    // not install its own handlers, or it swallows the Ctrl+C and resumes the
    // read. Guarded to real GNU readline — libedit's shim (macOS) reports 4.2;
    // there Ctrl+C surfaces as a null return, which read_repl_line already
    // treats as an interruption when the flag is set.
    rl_catch_signals = 0;
#endif
}

auto should_record_history(std::string_view line) -> bool {
    line = trim_left(line);
    while (!line.empty() && std::isspace(static_cast<unsigned char>(line.back())) != 0) {
        line.remove_suffix(1);
    }
    return !line.empty() && line != ":q" && line != ":quit" && line != ":exit";
}

auto read_repl_line(const std::string& prompt, std::string& out) -> ReadLineStatus {
    // Drop any Ctrl+C that landed between the end of the previous evaluation
    // and this prompt — it has already done its job (or arrived too late).
    runtime::clear_interrupt();
    const std::unique_ptr<char, decltype(&std::free)> raw{::readline(prompt.c_str()), &std::free};
    if (runtime::consume_interrupt()) {
        // Covers both the event-hook path (emptied line) and readline
        // variants that surface the EINTR as a null return.
        fmt::print("^C\n");
        return ReadLineStatus::Interrupted;
    }
    if (raw == nullptr) {
        return ReadLineStatus::Eof;
    }

    out.assign(raw.get());
    if (should_record_history(out)) {
        HIST_ENTRY const* previous = history_length > 0 ? ::history_get(history_length) : nullptr;
        if (previous == nullptr || previous->line == nullptr || out != previous->line) {
            ::add_history(raw.get());
        }
    }
    return ReadLineStatus::Line;
}
#else
void configure_line_editing() {}

void set_completion_context(CompletionContext) {}

auto read_repl_line(const std::string& prompt, std::string& out) -> ReadLineStatus {
    runtime::clear_interrupt();
    fmt::print("{}", prompt);
    if (std::getline(std::cin, out)) {
        return ReadLineStatus::Line;
    }
    if (runtime::consume_interrupt()) {
        // The read was EINTR'd by Ctrl+C: clear the stream error and hand
        // control back to the loop instead of treating it as EOF.
        std::cin.clear();
        fmt::print("^C\n");
        return ReadLineStatus::Interrupted;
    }
    return ReadLineStatus::Eof;
}

auto resolve_history_path(const ReplConfig& /*config*/) -> std::string {
    return {};
}

void load_history_file(const std::string& /*path*/, std::size_t /*limit*/) {}

void save_history_file(const std::string& /*path*/, std::size_t /*limit*/) {}
#endif

#ifndef _WIN32
/// Only sets the atomic flag; everything else is polled cooperatively at
/// evaluation boundaries (see ibex/runtime/interrupt.hpp).
void handle_sigint(int /*signum*/) {
    runtime::request_interrupt();
}

void install_interrupt_handler() {
    struct sigaction action{};
    action.sa_handler = handle_sigint;
    sigemptyset(&action.sa_mask);
    // No SA_RESTART: blocking reads (the prompt, blocking externs) must
    // return EINTR so a Ctrl+C is noticed promptly.
    action.sa_flags = 0;
    sigaction(SIGINT, &action, nullptr);
}
#else
// Windows keeps default Ctrl+C behavior for now (process exit).
void install_interrupt_handler() {}
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

auto source_for_lines(std::string_view source, std::size_t start_line, std::size_t end_line)
    -> std::string {
    if (source.empty() || start_line == 0 || end_line < start_line) {
        return {};
    }
    std::size_t line = 1;
    std::size_t start = std::string_view::npos;
    std::size_t end = source.size();
    for (std::size_t i = 0; i <= source.size(); ++i) {
        if (line == start_line && start == std::string_view::npos) {
            start = i;
        }
        if (i == source.size()) {
            break;
        }
        if (source[i] == '\n') {
            if (line == end_line) {
                end = i;
                break;
            }
            ++line;
        }
    }
    if (start == std::string_view::npos) {
        return {};
    }
    return std::string(source.substr(start, end - start));
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

auto doc_from_comment_group(const std::vector<std::string>& comments) -> std::string {
    std::string doc;
    for (const auto& line : comments) {
        if (line.empty()) {
            continue;
        }
        if (!doc.empty()) {
            doc.push_back('\n');
        }
        doc += line;
    }
    return doc;
}

auto bind_call_arguments(const std::string& callee, parser::CallExpr& call,
                         const std::vector<parser::Param>& params)
    -> std::expected<std::vector<BoundCallArg>, std::string> {
    std::vector<BoundCallArg> bound(params.size());
    robin_hood::unordered_map<std::string, std::size_t> param_index;
    param_index.reserve(params.size());
    for (std::size_t i = 0; i < params.size(); ++i) {
        param_index.emplace(params[i].name, i);
        bound[i].param = &params[i];
    }

    if (call.args.size() > params.size()) {
        return std::unexpected(callee + ": too many positional arguments");
    }

    for (std::size_t i = 0; i < call.args.size(); ++i) {
        bound[i].expr = call.args[i].get();
    }

    for (auto& named_arg : call.named_args) {
        auto it = param_index.find(named_arg.name);
        if (it == param_index.end()) {
            return std::unexpected(callee + ": unknown named argument '" + named_arg.name + "'");
        }
        auto& slot = bound[it->second];
        if (slot.expr != nullptr) {
            return std::unexpected(callee + ": duplicate argument for parameter '" +
                                   slot.param->name + "'");
        }
        slot.expr = named_arg.value.get();
    }

    for (auto& slot : bound) {
        if (slot.expr != nullptr) {
            continue;
        }
        if (slot.param->default_value != nullptr) {
            slot.expr = slot.param->default_value.get();
            slot.is_default = true;
            continue;
        }
        return std::unexpected(callee + ": missing required argument '" + slot.param->name + "'");
    }

    return bound;
}

auto format_scalar(const runtime::ScalarValue& value) -> std::string {
    return std::visit(
        [](const auto& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, Date>) {
                return runtime::format_date(v);
            } else if constexpr (std::is_same_v<T, Timestamp>) {
                return runtime::format_timestamp(v);
            } else if constexpr (std::is_same_v<T, double>) {
                return runtime::format_float_mixed(v);
            } else {
                return fmt::format("{}", v);
            }
        },
        value);
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
    runtime::format_table(table, std::cout, max_rows);
}

// Render any evaluated value (table, scalar, or column) to stdout using the
// same formatting the REPL applies to a bare expression statement. Shared by
// the top-level statement printer and the `print(...)` builtin.
void render_eval_value(const EvalValue& value) {
    if (const auto* scalar = std::get_if<runtime::ScalarValue>(&value)) {
        fmt::print("{}\n", format_scalar(*scalar));
    } else if (const auto* col = std::get_if<runtime::ColumnValue>(&value)) {
        runtime::Table temp;
        temp.add_column("column", *col);
        print_table(temp);
    } else {
        print_table(std::get<runtime::Table>(value));
    }
}

void print_tables(const runtime::TableRegistry& tables, const LazyTableRegistry& lazy_tables) {
    if (tables.empty() && lazy_tables.empty()) {
        fmt::print("tables: <none>\n");
        return;
    }
    std::vector<std::string> names;
    names.reserve(tables.size() + lazy_tables.size());
    for (const auto& entry : tables) {
        names.push_back(entry.first);
    }
    for (const auto& entry : lazy_tables) {
        names.push_back(entry.first);
    }
    std::ranges::sort(names);
    fmt::print("tables:");
    for (const auto& name : names) {
        fmt::print(" {}", name);
    }
    fmt::print("\n");
}

/// Resolve a table name for a caller that needs the rows, not just the schema.
/// A lazy binding is materialized in full: nothing about the caller bounds which
/// columns it will look at.
auto resolve_table(const std::string& name, const runtime::TableRegistry& tables,
                   const LazyTableRegistry& lazy_tables)
    -> std::expected<runtime::Table, std::string> {
    if (auto it = tables.find(name); it != tables.end()) {
        return it->second;
    }
    if (auto it = lazy_tables.find(name); it != lazy_tables.end()) {
        return it->second->materialize();
    }
    return std::unexpected("unknown table '" + name + "'");
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

std::string format_table_names(const runtime::TableRegistry& tables,
                               const LazyTableRegistry& lazy_tables) {
    if (tables.empty() && lazy_tables.empty()) {
        return "<none>";
    }
    std::vector<std::string> names;
    names.reserve(tables.size() + lazy_tables.size());
    for (const auto& entry : tables) {
        names.push_back(entry.first);
    }
    for (const auto& entry : lazy_tables) {
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

auto scalar_type_name(parser::ScalarType st) -> std::string_view {
    switch (st) {
        case parser::ScalarType::Int32:
            return "Int32";
        case parser::ScalarType::Int64:
            return "Int64";
        case parser::ScalarType::Float32:
            return "Float32";
        case parser::ScalarType::Float64:
            return "Float64";
        case parser::ScalarType::Bool:
            return "Bool";
        case parser::ScalarType::String:
            return "String";
        case parser::ScalarType::Date:
            return "Date";
        case parser::ScalarType::Timestamp:
            return "Timestamp";
    }
    return "Unknown";
}

auto scalar_value_type_name(const runtime::ScalarValue& val) -> std::string_view {
    if (std::holds_alternative<std::int64_t>(val))
        return "Int64";
    if (std::holds_alternative<double>(val))
        return "Float64";
    if (std::holds_alternative<bool>(val))
        return "Bool";
    if (std::holds_alternative<std::string>(val))
        return "String";
    if (std::holds_alternative<Date>(val))
        return "Date";
    if (std::holds_alternative<Timestamp>(val))
        return "Timestamp";
    return "Unknown";
}

auto type_to_string(const parser::Type& type) -> std::string {
    auto schema_to_string = [](const parser::SchemaType& schema) {
        if (schema.fields.empty()) {
            return std::string{};
        }
        std::string out = "<{ ";
        for (std::size_t i = 0; i < schema.fields.size(); ++i) {
            if (i > 0) {
                out += ", ";
            }
            out += schema.fields[i].name;
            out += ": ";
            out += scalar_type_name(schema.fields[i].type);
        }
        out += " }>";
        return out;
    };

    if (type.kind == parser::Type::Kind::Scalar) {
        if (const auto* scalar = std::get_if<parser::ScalarType>(&type.arg)) {
            return std::string(scalar_type_name(*scalar));
        }
        return "Unknown";
    }
    if (type.kind == parser::Type::Kind::Series) {
        std::string out = "Series";
        if (const auto* scalar = std::get_if<parser::ScalarType>(&type.arg)) {
            out += "<";
            out += scalar_type_name(*scalar);
            out += ">";
        }
        return out;
    }
    const char* base = type.kind == parser::Type::Kind::TimeFrame ? "TimeFrame" : "DataFrame";
    if (const auto* schema = std::get_if<parser::SchemaType>(&type.arg)) {
        return std::string(base) + schema_to_string(*schema);
    }
    return base;
}

auto param_to_string(const parser::Param& param) -> std::string {
    std::string out;
    if (param.effect == parser::Param::Effect::Mutable) {
        out += "mutable ";
    } else if (param.effect == parser::Param::Effect::Consume) {
        out += "consume ";
    }
    out += param.name;
    out += ": ";
    out += type_to_string(param.type);
    if (param.default_value != nullptr) {
        out += " = <default>";
    }
    return out;
}

auto effects_to_string(const std::optional<std::vector<parser::EffectSpec>>& effects)
    -> std::string {
    if (!effects.has_value()) {
        return {};
    }
    std::string out = " effects {";
    for (std::size_t i = 0; i < effects->size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        switch ((*effects)[i].kind) {
            case parser::EffectKind::IoRead:
                out += "io_read";
                break;
            case parser::EffectKind::IoWrite:
                out += "io_write";
                break;
            case parser::EffectKind::Nondet:
                out += "nondet";
                break;
            case parser::EffectKind::State:
                out += "state";
                break;
            case parser::EffectKind::Blocking:
                out += "blocking";
                break;
            case parser::EffectKind::MayFail:
                out += "may_fail";
                break;
        }
        if ((*effects)[i].resource.has_value()) {
            out += "(\"";
            out += *(*effects)[i].resource;
            out += "\")";
        }
    }
    out += "}";
    return out;
}

auto function_signature(const parser::FunctionDecl& fn) -> std::string {
    std::string out = "fn " + fn.name + "(";
    for (std::size_t i = 0; i < fn.params.size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        out += param_to_string(fn.params[i]);
    }
    out += ") -> ";
    out += type_to_string(fn.return_type);
    out += effects_to_string(fn.effects);
    return out;
}

auto extern_signature(const parser::ExternDecl& decl) -> std::string {
    std::string out = "extern fn " + decl.name + "(";
    for (std::size_t i = 0; i < decl.params.size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        out += param_to_string(decl.params[i]);
    }
    out += ") -> ";
    out += type_to_string(decl.return_type);
    out += effects_to_string(decl.effects);
    if (!decl.source_path.empty()) {
        out += " from \"";
        out += decl.source_path;
        out += "\"";
    }
    return out;
}

auto scalar_type_matches(const runtime::ScalarValue& val, parser::ScalarType expected) -> bool {
    switch (expected) {
        case parser::ScalarType::Int32:
        case parser::ScalarType::Int64:
            return std::holds_alternative<std::int64_t>(val);
        case parser::ScalarType::Float32:
        case parser::ScalarType::Float64:
            return std::holds_alternative<double>(val);
        case parser::ScalarType::Bool:
            return std::holds_alternative<bool>(val);
        case parser::ScalarType::String:
            return std::holds_alternative<std::string>(val);
        case parser::ScalarType::Date:
            return std::holds_alternative<Date>(val);
        case parser::ScalarType::Timestamp:
            return std::holds_alternative<Timestamp>(val);
    }
    return false;
}

/// Widen an integer scalar to Float when a let binding (or function argument)
/// declares a Float type. Avoids the `let x: Float64 = Float64(42)` ceremony
/// that nothing in the language gains from. This is the only implicit
/// conversion accepted; other type mismatches still error.
auto try_widen_int_to_float(runtime::ScalarValue& val, parser::ScalarType expected) -> bool {
    if (expected != parser::ScalarType::Float32 && expected != parser::ScalarType::Float64) {
        return false;
    }
    const auto* iv = std::get_if<std::int64_t>(&val);
    if (iv == nullptr) {
        return false;
    }
    val = static_cast<double>(*iv);
    return true;
}

auto column_type_matches(const runtime::ColumnValue& col, parser::ScalarType expected) -> bool {
    switch (expected) {
        case parser::ScalarType::Int32:
        case parser::ScalarType::Int64:
            return std::holds_alternative<Column<std::int64_t>>(col);
        case parser::ScalarType::Float32:
        case parser::ScalarType::Float64:
            return std::holds_alternative<Column<double>>(col);
        case parser::ScalarType::Bool:
            return std::holds_alternative<Column<bool>>(col);
        case parser::ScalarType::String:
            return std::holds_alternative<Column<std::string>>(col);
        case parser::ScalarType::Date:
            return std::holds_alternative<Column<Date>>(col);
        case parser::ScalarType::Timestamp:
            return std::holds_alternative<Column<Timestamp>>(col);
    }
    return false;
}

/// IR column type of a runtime column, for building static schemas from
/// concrete tables. Categorical columns report as String; anything unrecognised
/// is left untyped (nullopt) — the column is still known to exist.
auto column_ir_type(const runtime::ColumnValue& col) -> std::optional<ir::ColumnType> {
    if (std::holds_alternative<Column<std::int64_t>>(col)) {
        return ir::ColumnType::Int64;
    }
    if (std::holds_alternative<Column<double>>(col)) {
        return ir::ColumnType::Float64;
    }
    if (std::holds_alternative<Column<bool>>(col)) {
        return ir::ColumnType::Bool;
    }
    if (std::holds_alternative<Column<std::string>>(col) ||
        std::holds_alternative<Column<Categorical>>(col)) {
        return ir::ColumnType::String;
    }
    if (std::holds_alternative<Column<Date>>(col)) {
        return ir::ColumnType::Date;
    }
    if (std::holds_alternative<Column<Timestamp>>(col)) {
        return ir::ColumnType::Timestamp;
    }
    return std::nullopt;
}

/// Exact (closed) schema of a concrete table — used to carry let-bound table
/// schemas into the static checks of later statements.
auto table_schema_info(const runtime::Table& table) -> ir::SchemaInfo {
    std::vector<ir::SchemaField> fields;
    fields.reserve(table.columns.size());
    for (const auto& entry : table.columns) {
        fields.push_back(
            ir::SchemaField{.name = entry.name, .type = column_ir_type(*entry.column)});
    }
    return ir::SchemaInfo::known(std::move(fields), /*open=*/false);
}

/// Validates that `column` satisfies the scalar element type declared in `type`.
/// Returns nullopt on success, or an error message on failure.
auto validate_column_type(const runtime::ColumnValue& column, const parser::Type& type)
    -> std::optional<std::string> {
    const auto* scalar = std::get_if<parser::ScalarType>(&type.arg);
    if (scalar == nullptr) {
        return std::nullopt;
    }
    if (!column_type_matches(column, *scalar)) {
        return "column has wrong type (expected " + std::string(scalar_type_name(*scalar)) + ")";
    }
    return std::nullopt;
}

auto empty_series_for_type(parser::ScalarType type) -> runtime::ColumnValue {
    switch (type) {
        case parser::ScalarType::Int32:
        case parser::ScalarType::Int64:
            return Column<std::int64_t>{};
        case parser::ScalarType::Float32:
        case parser::ScalarType::Float64:
            return Column<double>{};
        case parser::ScalarType::Bool:
            return Column<bool>{};
        case parser::ScalarType::String:
            return Column<std::string>{};
        case parser::ScalarType::Date:
            return Column<Date>{};
        case parser::ScalarType::Timestamp:
            return Column<Timestamp>{};
    }
    return Column<std::int64_t>{};
}

auto eval_series_literal(const parser::ArrayLiteralExpr& array,
                         std::optional<parser::ScalarType> expected = std::nullopt)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (array.elements.empty()) {
        if (!expected.has_value()) {
            return std::unexpected("empty series literal requires a Series<T> annotation");
        }
        return empty_series_for_type(*expected);
    }

    const auto* first_lit = std::get_if<parser::LiteralExpr>(&array.elements.front()->node);
    if (first_lit == nullptr) {
        return std::unexpected("series literal elements must be literals");
    }

    const std::size_t type_index = first_lit->value.index();
    if (type_index == 4) {
        return std::unexpected("duration literals are not valid series elements");
    }
    for (const auto& element : array.elements) {
        const auto* lit = std::get_if<parser::LiteralExpr>(&element->node);
        if (lit == nullptr) {
            return std::unexpected("series literal elements must be literals");
        }
        if (lit->value.index() == 4) {
            return std::unexpected("duration literals are not valid series elements");
        }
        if (lit->value.index() != type_index) {
            return std::unexpected("series literal has mixed element types");
        }
    }

    runtime::ColumnValue out;
    switch (type_index) {
        case 0: {
            Column<std::int64_t> col;
            col.reserve(array.elements.size());
            for (const auto& element : array.elements) {
                col.push_back(
                    std::get<std::int64_t>(std::get<parser::LiteralExpr>(element->node).value));
            }
            out = std::move(col);
            break;
        }
        case 1: {
            Column<double> col;
            col.reserve(array.elements.size());
            for (const auto& element : array.elements) {
                col.push_back(std::get<double>(std::get<parser::LiteralExpr>(element->node).value));
            }
            out = std::move(col);
            break;
        }
        case 2: {
            Column<bool> col;
            col.reserve(array.elements.size());
            for (const auto& element : array.elements) {
                col.push_back(std::get<bool>(std::get<parser::LiteralExpr>(element->node).value));
            }
            out = std::move(col);
            break;
        }
        case 3: {
            Column<std::string> col;
            col.reserve(array.elements.size());
            for (const auto& element : array.elements) {
                const auto& value =
                    std::get<std::string>(std::get<parser::LiteralExpr>(element->node).value);
                col.push_back(std::string_view{value});
            }
            out = std::move(col);
            break;
        }
        case 5: {
            Column<Date> col;
            col.reserve(array.elements.size());
            for (const auto& element : array.elements) {
                col.push_back(std::get<Date>(std::get<parser::LiteralExpr>(element->node).value));
            }
            out = std::move(col);
            break;
        }
        case 6: {
            Column<Timestamp> col;
            col.reserve(array.elements.size());
            for (const auto& element : array.elements) {
                col.push_back(
                    std::get<Timestamp>(std::get<parser::LiteralExpr>(element->node).value));
            }
            out = std::move(col);
            break;
        }
        default:
            return std::unexpected("unsupported series literal element type");
    }

    if (expected.has_value() && !column_type_matches(out, *expected)) {
        return std::unexpected("series literal has wrong type (expected " +
                               std::string(scalar_type_name(*expected)) + ")");
    }
    return out;
}

/// Validates that `table` satisfies the schema declared in `type`.
/// All declared fields must be present with the correct type; extra columns are allowed.
/// Returns nullopt on success, or an error message on failure.
/// Skips validation if no schema fields are declared (bare DataFrame / DataFrame<{}>).
auto validate_table_type(const runtime::Table& table, const parser::Type& type)
    -> std::optional<std::string> {
    const auto* schema = std::get_if<parser::SchemaType>(&type.arg);
    if (schema == nullptr || schema->fields.empty()) {
        return std::nullopt;
    }
    for (const auto& field : schema->fields) {
        const auto* col = table.find(field.name);
        if (col == nullptr) {
            return "schema mismatch: missing column '" + field.name + "'";
        }
        if (!column_type_matches(*col, field.type)) {
            return "schema mismatch: column '" + field.name + "' has wrong type (expected " +
                   std::string(scalar_type_name(field.type)) + ")";
        }
    }
    return std::nullopt;
}

/// Returns true if `callee` is one of the type-name cast functions.
auto is_cast_callee(std::string_view callee) -> bool {
    return callee == "Int64" || callee == "Int32" || callee == "Int" || callee == "Float64" ||
           callee == "Float32";
}

/// Applies a scalar type cast. `callee` must be a cast name (checked by is_cast_callee).
auto apply_scalar_cast(const runtime::ScalarValue& val, std::string_view callee)
    -> std::expected<runtime::ScalarValue, std::string> {
    const bool to_int = (callee == "Int64" || callee == "Int32" || callee == "Int");
    if (to_int) {
        if (std::holds_alternative<std::int64_t>(val)) {
            return val;
        }
        if (std::holds_alternative<double>(val)) {
            double v = std::get<double>(val);
            if (v != std::trunc(v)) {
                return std::unexpected(std::string(callee) + "(): cannot cast non-integer Float " +
                                       std::to_string(v) +
                                       " to Int (use floor(), ceil(), or round())");
            }
            return runtime::ScalarValue{static_cast<std::int64_t>(v)};
        }
        return std::unexpected(std::string(callee) + "(): cannot cast " +
                               std::string(scalar_value_type_name(val)) + " to Int");
    }
    // Float64 / Float32
    if (std::holds_alternative<double>(val)) {
        return val;
    }
    if (std::holds_alternative<std::int64_t>(val)) {
        return runtime::ScalarValue{static_cast<double>(std::get<std::int64_t>(val))};
    }
    return std::unexpected(std::string(callee) + "(): cannot cast " +
                           std::string(scalar_value_type_name(val)) + " to Float");
}

/// Applies an element-wise column type cast.
auto apply_column_cast(const runtime::ColumnValue& col, std::string_view callee)
    -> std::expected<runtime::ColumnValue, std::string> {
    const bool to_int = (callee == "Int64" || callee == "Int32" || callee == "Int");
    if (to_int) {
        if (std::holds_alternative<Column<std::int64_t>>(col)) {
            return col;
        }
        if (std::holds_alternative<Column<double>>(col)) {
            const auto& src = std::get<Column<double>>(col);
            for (const double v : src) {
                if (v != std::trunc(v)) {
                    return std::unexpected(std::string(callee) +
                                           "(): cannot cast non-integer Float column to Int (use "
                                           "floor(), ceil(), or round())");
                }
            }
            Column<std::int64_t> dst;
            dst.reserve(src.size());
            for (double v : src) {
                dst.push_back(static_cast<std::int64_t>(v));
            }
            return runtime::ColumnValue{std::move(dst)};
        }
        return std::unexpected(std::string(callee) + "(): cannot cast column to Int");
    }
    // Float64 / Float32
    if (std::holds_alternative<Column<double>>(col)) {
        return col;
    }
    if (std::holds_alternative<Column<std::int64_t>>(col)) {
        const auto& src = std::get<Column<std::int64_t>>(col);
        Column<double> dst;
        dst.reserve(src.size());
        for (std::int64_t v : src) {
            dst.push_back(static_cast<double>(v));
        }
        return runtime::ColumnValue{std::move(dst)};
    }
    return std::unexpected(std::string(callee) + "(): cannot cast column to Float");
}

/// Validates that an expression is a bare mode identifier and returns its name.
auto extract_round_mode(const parser::Expr& arg) -> std::expected<std::string_view, std::string> {
    const auto* ident = std::get_if<parser::IdentifierExpr>(&arg.node);
    if (!ident) {
        return std::unexpected(
            "round(): second argument must be a bare mode identifier (nearest, bankers, floor, "
            "ceil, trunc)");
    }
    if (ident->name != "nearest" && ident->name != "bankers" && ident->name != "floor" &&
        ident->name != "ceil" && ident->name != "trunc") {
        return std::unexpected("round(): unknown mode '" + ident->name +
                               "' (expected: nearest, bankers, floor, ceil, trunc)");
    }
    return std::string_view{ident->name};
}

auto apply_scalar_round(double v, std::string_view mode)
    -> std::expected<runtime::ScalarValue, std::string> {
    std::int64_t result{};
    if (mode == "nearest") {
        result = static_cast<std::int64_t>(std::llround(v));
    } else if (mode == "bankers") {
        result =
            static_cast<std::int64_t>(std::llrint(v));  // uses FE_TONEAREST (round-half-to-even)
    } else if (mode == "floor") {
        result = static_cast<std::int64_t>(std::floor(v));
    } else if (mode == "ceil") {
        result = static_cast<std::int64_t>(std::ceil(v));
    } else {  // trunc
        result = static_cast<std::int64_t>(std::trunc(v));
    }
    return runtime::ScalarValue{result};
}

auto apply_column_round(const Column<double>& src, std::string_view mode) -> Column<std::int64_t> {
    Column<std::int64_t> dst;
    dst.reserve(src.size());
    for (const double v : src) {
        std::int64_t r{};
        if (mode == "nearest") {
            r = static_cast<std::int64_t>(std::llround(v));
        } else if (mode == "bankers") {
            r = static_cast<std::int64_t>(
                std::llrint(v));  // uses FE_TONEAREST (round-half-to-even)
        } else if (mode == "floor") {
            r = static_cast<std::int64_t>(std::floor(v));
        } else if (mode == "ceil") {
            r = static_cast<std::int64_t>(std::ceil(v));
        } else {  // trunc
            r = static_cast<std::int64_t>(std::trunc(v));
        }
        dst.push_back(r);
    }
    return dst;
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

struct BuiltinDoc {
    std::string_view name;
    std::string_view signature;
    std::string_view summary;
    std::string_view example;
};

constexpr BuiltinDoc kBuiltinDocs[] = {
    {.name = "mean",
     .signature = "mean(col) -> Float64",
     .summary = "Aggregate mean over each group.",
     .example = "trades[select { avg = mean(price) }, by symbol]"},
    {.name = "sum",
     .signature = "sum(col) -> same numeric type",
     .summary = "Aggregate sum over each group.",
     .example = "trades[select { total = sum(price) }, by symbol]"},
    {.name = "count",
     .signature = "count() -> Int64",
     .summary = "Count rows in the current group.",
     .example = "trades[select { n = count() }, by symbol]"},
    {.name = "min",
     .signature = "min(col) -> same type",
     .summary = "Aggregate minimum.",
     .example = "trades[select { lo = min(price) }]"},
    {.name = "max",
     .signature = "max(col) -> same type",
     .summary = "Aggregate maximum.",
     .example = "trades[select { hi = max(price) }]"},
    {.name = "first",
     .signature = "first(col) -> same type",
     .summary = "First value in group encounter order.",
     .example = "trades[select { first_px = first(price) }, by symbol]"},
    {.name = "last",
     .signature = "last(col) -> same type",
     .summary = "Last value in group encounter order.",
     .example = "trades[select { last_px = last(price) }, by symbol]"},
    {.name = "median",
     .signature = "median(col) -> Float64",
     .summary = "Aggregate median.",
     .example = "trades[select { med = median(price) }, by symbol]"},
    {.name = "std",
     .signature = "std(col) -> Float64",
     .summary = "Sample standard deviation.",
     .example = "trades[select { vol = std(price) }, by symbol]"},
    {.name = "quantile",
     .signature = "quantile(col, p) -> Float64",
     .summary = "Linear interpolated aggregate quantile.",
     .example = "trades[select { p95 = quantile(price, 0.95) }, by symbol]"},
    {.name = "scalar",
     .signature = "scalar(table, column) -> scalar",
     .summary = "Extract a scalar value from a one-row table.",
     .example = "scalar(summary, \"avg\")"},
    {.name = "columns",
     .signature = "columns(table) -> DataFrame",
     .summary = "Return a metadata table of column names.",
     .example = "let cols = columns(trades);"},
    {.name = "as_timeframe",
     .signature = "as_timeframe(table, \"timestamp_col\") -> TimeFrame",
     .summary = "Mark a table as time-indexed and sorted by its timestamp/date column.",
     .example = "let tf = as_timeframe(trades, \"ts\");"},
    {.name = "lag",
     .signature = "lag(col, n) -> Series<T>",
     .summary = "Value n rows before the current row.",
     .example = "trades[update { prev = lag(price, 1) }, by symbol]"},
    {.name = "lead",
     .signature = "lead(col, n) -> Series<T>",
     .summary = "Value n rows after the current row.",
     .example = "trades[update { next = lead(price, 1) }, by symbol]"},
    {.name = "rank",
     .signature = "rank(expr, method = dense, ascending = true) -> Series<Int64>",
     .summary = "Row rank, optionally partitioned by surrounding by clause.",
     .example = "scores[update { r = rank(score, method = dense, ascending = false) }, by dept]"},
    {.name = "round",
     .signature = "round(value, mode) -> Int64",
     .summary = "Round Float64 using nearest/bankers/floor/ceil/trunc.",
     .example = "round(2.5, bankers)"},
    {.name = "print",
     .signature = "print(value) -> value",
     .summary = "Print a human-readable value in scripts and REPL sessions.",
     .example = "print(trades[select { n = count() }])"},
};

auto find_builtin_doc(std::string_view name) -> const BuiltinDoc* {
    for (const auto& doc : kBuiltinDocs) {
        if (doc.name == name) {
            return &doc;
        }
    }
    return nullptr;
}

void print_help() {
    fmt::print("Ibex REPL help\n");
    fmt::print("  :help                 Show this help\n");
    fmt::print("  :tables               List table bindings\n");
    fmt::print("  :scalars              List scalar bindings\n");
    fmt::print("  :functions            List user and extern functions\n");
    fmt::print("  :imports              List imported libraries and extern origins\n");
    fmt::print("  :schema <table>       Show column names and types\n");
    fmt::print("  :head <table> [n]     Show first n rows\n");
    fmt::print("  :peek <expr>          Evaluate and compactly display an expression\n");
    fmt::print("  :describe <table>     Schema + first rows\n");
    fmt::print("  :doc <name>           Show docs/signature for a binding or built-in\n");
    fmt::print("  ?name                 Shorthand for :doc name\n");
    fmt::print("  :source <fn>          Show source for a user-defined function\n");
    fmt::print("  :load <file>          Load and execute an .ibex script\n");
    fmt::print("  :time <command>       Time exactly one command\n");
    fmt::print("  :timing [on|off]      Toggle command timing\n");
    fmt::print("  :comments [on|off]    Toggle script comment echo during :load\n");
    fmt::print("  :quit                 Exit\n");
}

void print_functions(const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls) {
    if (functions.empty() && extern_decls.empty()) {
        fmt::print("functions: <none>\n");
        return;
    }
    std::vector<std::string> names;
    names.reserve(functions.size());
    for (const auto& entry : functions) {
        names.push_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    if (!names.empty()) {
        fmt::print("user functions:\n");
        for (const auto& name : names) {
            fmt::print("  {}\n", function_signature(functions.at(name)));
        }
    }
    names.clear();
    names.reserve(extern_decls.size());
    for (const auto& entry : extern_decls) {
        names.push_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    if (!names.empty()) {
        fmt::print("extern functions:\n");
        for (const auto& name : names) {
            fmt::print("  {}\n", extern_signature(extern_decls.at(name)));
        }
    }
}

void print_imports(const ImportRegistry& imports, const ExternDeclRegistry& extern_decls) {
    if (imports.empty()) {
        fmt::print("imports: <none>\n");
    } else {
        std::vector<std::string> names(imports.begin(), imports.end());
        std::sort(names.begin(), names.end());
        fmt::print("imports:");
        for (const auto& name : names) {
            fmt::print(" {}", name);
        }
        fmt::print("\n");
    }

    robin_hood::unordered_map<std::string, std::vector<std::string>> by_source;
    for (const auto& [name, decl] : extern_decls) {
        if (!decl.source_path.empty()) {
            by_source[decl.source_path].push_back(name);
        }
    }
    if (by_source.empty()) {
        return;
    }
    std::vector<std::string> sources;
    sources.reserve(by_source.size());
    for (const auto& [source, _] : by_source) {
        sources.push_back(source);
    }
    std::sort(sources.begin(), sources.end());
    fmt::print("extern origins:\n");
    for (const auto& source : sources) {
        auto& names = by_source[source];
        std::sort(names.begin(), names.end());
        fmt::print("  {}:", source);
        for (const auto& name : names) {
            fmt::print(" {}", name);
        }
        fmt::print("\n");
    }
}

void print_doc_text(const DeclarationDocRegistry& declaration_docs, const std::string& key) {
    auto doc_it = declaration_docs.find(key);
    if (doc_it == declaration_docs.end() || doc_it->second.empty()) {
        return;
    }
    fmt::print("doc:\n");
    std::string_view text = doc_it->second;
    while (!text.empty()) {
        auto newline = text.find('\n');
        auto line = newline == std::string_view::npos ? text : text.substr(0, newline);
        fmt::print("  {}\n", line);
        if (newline == std::string_view::npos) {
            break;
        }
        text.remove_prefix(newline + 1);
    }
}

void print_doc(std::string_view name, const runtime::TableRegistry& tables,
               const LazyTableRegistry& lazy_tables, const runtime::ScalarRegistry& scalars,
               const ColumnRegistry& columns, const ModelRegistry& models,
               const FunctionRegistry& functions, const ExternDeclRegistry& extern_decls,
               const DeclarationDocRegistry& declaration_docs, const ImportRegistry& imports) {
    if (name.empty()) {
        fmt::print("usage: :doc <name>\n");
        return;
    }
    const std::string key{name};
    if (auto it = tables.find(key); it != tables.end()) {
        fmt::print("table {}\n", key);
        print_schema(it->second);
        fmt::print("rows: {}\n", it->second.rows());
        return;
    }
    if (auto it = lazy_tables.find(key); it != lazy_tables.end()) {
        fmt::print("table {}\n", key);
        print_schema(it->second->schema());
        fmt::print("rows: {}\n", it->second->rows());
        return;
    }
    if (auto it = scalars.find(key); it != scalars.end()) {
        fmt::print("scalar {}: {} = {}\n", key, scalar_value_type_name(it->second),
                   format_scalar(it->second));
        return;
    }
    if (auto it = columns.find(key); it != columns.end()) {
        fmt::print("column {}: {}\n", key, column_type_name(it->second));
        return;
    }
    if (auto it = models.find(key); it != models.end()) {
        fmt::print("model {}\n", key);
        fmt::print("  method: {}\n", it->second.method);
        fmt::print("  observations: {}\n", it->second.n_obs);
        fmt::print("  r_squared: {}\n", runtime::format_float_mixed(it->second.r_squared));
        return;
    }
    if (auto it = functions.find(key); it != functions.end()) {
        fmt::print("{}\n", function_signature(it->second));
        print_doc_text(declaration_docs, key);
        fmt::print("source: use :source {}\n", key);
        return;
    }
    if (auto it = extern_decls.find(key); it != extern_decls.end()) {
        fmt::print("{}\n", extern_signature(it->second));
        print_doc_text(declaration_docs, key);
        if (!it->second.source_path.empty()) {
            fmt::print("origin: {}\n", it->second.source_path);
        }
        return;
    }
    if (imports.contains(key)) {
        fmt::print("import \"{}\"\n", key);
        return;
    }
    if (const auto* doc = find_builtin_doc(name); doc != nullptr) {
        fmt::print("{}\n", doc->signature);
        fmt::print("{}\n", doc->summary);
        if (!doc->example.empty()) {
            fmt::print("example: {}\n", doc->example);
        }
        return;
    }
    fmt::print("no documentation for '{}'\n", name);
}

void print_source(std::string_view name, const FunctionRegistry& functions,
                  const FunctionSourceRegistry& sources) {
    if (name.empty()) {
        fmt::print("usage: :source <function>\n");
        return;
    }
    const std::string key{name};
    auto fn_it = functions.find(key);
    if (fn_it == functions.end()) {
        fmt::print("error: unknown user function '{}'\n", name);
        return;
    }
    auto source_it = sources.find(key);
    if (source_it != sources.end() && !source_it->second.empty()) {
        fmt::print("{}\n", source_it->second);
        return;
    }
    fmt::print("{} {{\n", function_signature(fn_it->second));
    fmt::print("  <source unavailable>\n");
    fmt::print("}}\n");
}

auto column_bytes(const runtime::ColumnEntry& entry) -> std::size_t {
    std::size_t bytes = std::visit(
        [](const auto& col) -> std::size_t {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                return (col.size() * sizeof(std::uint32_t))  // offsets
                       + (col.size() == 0 ? 0 : col.offsets_data()[col.size()]);
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                std::size_t dict_chars = 0;
                for (const auto& s : col.dictionary()) {
                    dict_chars += s.size();
                }
                return (col.size() * sizeof(Column<Categorical>::code_type)) + dict_chars;
            } else if constexpr (std::is_same_v<ColType, Column<bool>>) {
                return (col.size() + 7) / 8;
            } else {
                using V = typename ColType::value_type;
                return col.size() * sizeof(V);
            }
        },
        *entry.column);
    if (entry.validity.has_value()) {
        bytes += (entry.validity->size() + 7) / 8;
    }
    return bytes;
}

auto format_bytes(std::size_t bytes) -> std::string {
    constexpr double kKiB = 1024.0;
    constexpr double kMiB = kKiB * 1024.0;
    constexpr double kGiB = kMiB * 1024.0;
    auto b = static_cast<double>(bytes);
    if (b >= kGiB) {
        return fmt::format("{:.2f} GiB", b / kGiB);
    }
    if (b >= kMiB) {
        return fmt::format("{:.1f} MiB", b / kMiB);
    }
    if (b >= kKiB) {
        return fmt::format("{:.1f} KiB", b / kKiB);
    }
    return fmt::format("{} B", bytes);
}

/// Compact one-shot inspection: types + first rows + total row count + an
/// estimated in-memory footprint. Replaces the `:schema` / `:head` /
/// length-query loop a typical exploratory session leans on.
void peek_table(const runtime::Table& table, std::size_t max_rows = 5) {
    fmt::print("rows: {}", table.rows());
    if (!table.columns.empty()) {
        std::size_t total = 0;
        for (const auto& entry : table.columns) {
            total += column_bytes(entry);
        }
        fmt::print("  cols: {}  ~{}", table.columns.size(), format_bytes(total));
        if (table.time_index.has_value()) {
            fmt::print("  time_index: {}", *table.time_index);
        }
    }
    fmt::print("\n");
    if (table.columns.empty()) {
        fmt::print("<empty>\n");
        return;
    }

    const std::size_t shown_rows = std::min(table.rows(), max_rows);
    const std::size_t col_count = table.columns.size();

    std::vector<std::string> type_row(col_count);
    std::vector<std::size_t> widths(col_count);
    std::vector<std::vector<std::string>> cells(col_count);
    for (std::size_t c = 0; c < col_count; ++c) {
        type_row[c] = std::string("<") + column_type_name(*table.columns[c].column) + ">";
        widths[c] = std::max(table.columns[c].name.size(), type_row[c].size());
        cells[c].reserve(shown_rows);
        for (std::size_t r = 0; r < shown_rows; ++r) {
            auto cell = runtime::format_cell(table.columns[c], r);
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
    fmt::print("\n|");
    for (std::size_t c = 0; c < col_count; ++c) {
        fmt::print(" {:<{}} |", type_row[c], widths[c]);
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
    const std::string_view view = trim(text);
    if (view.empty()) {
        return {};
    }
    if (view.front() == '"' || view.front() == '\'') {
        const char quote = view.front();
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
    auto result = std::from_chars(text.data(), text.data() + text.size(), value);
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
    if (micros < 1000L * 1000L) {
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

auto has_model_result(const runtime::ModelResult& model) -> bool {
    return !model.method.empty();
}

auto is_model_table_accessor(std::string_view callee) -> bool {
    return callee == "coef" || callee == "model_coef" || callee == "summary" ||
           callee == "model_summary" || callee == "fitted" || callee == "model_fitted" ||
           callee == "residuals" || callee == "model_residuals" || callee == "importance" ||
           callee == "model_importance";
}

auto is_model_scalar_accessor(std::string_view callee) -> bool {
    return callee == "r_squared" || callee == "model_r_squared";
}

auto extract_compile_time_string_list(const parser::Expr& expr)
    -> std::optional<std::vector<std::string>> {
    const auto* array = std::get_if<parser::ArrayLiteralExpr>(&expr.node);
    if (array == nullptr) {
        return std::nullopt;
    }
    std::vector<std::string> values;
    values.reserve(array->elements.size());
    for (const auto& element : array->elements) {
        const auto* literal = std::get_if<parser::LiteralExpr>(&element->node);
        if (literal == nullptr || !std::holds_alternative<std::string>(literal->value)) {
            return std::nullopt;
        }
        values.push_back(std::get<std::string>(literal->value));
    }
    return values;
}

auto extract_compile_time_string_list(const runtime::Table& table)
    -> std::optional<std::vector<std::string>> {
    if (table.columns.size() != 1 || table.columns.front().name != "name") {
        return std::nullopt;
    }
    const auto& entry = table.columns.front();
    const auto* names = std::get_if<ibex::Column<std::string>>(entry.column.get());
    if (names == nullptr) {
        return std::nullopt;
    }
    if (entry.validity.has_value()) {
        for (std::size_t row = 0; row < names->size(); ++row) {
            if (!(*entry.validity)[row]) {
                return std::nullopt;
            }
        }
    }
    std::vector<std::string> values;
    values.reserve(names->size());
    for (const auto& value : *names) {
        values.push_back(std::string(value));
    }
    return values;
}

auto lookup_model_binding(const parser::CallExpr& call, const ModelRegistry& models)
    -> std::expected<const runtime::ModelResult*, std::string> {
    if (call.args.size() != 1 || !call.named_args.empty()) {
        return std::unexpected(call.callee + ": expected exactly one model binding argument");
    }
    const auto* ident = std::get_if<parser::IdentifierExpr>(&call.args[0]->node);
    if (ident == nullptr) {
        return std::unexpected(call.callee + ": argument must be a model binding name");
    }
    auto it = models.find(ident->name);
    if (it == models.end()) {
        return std::unexpected(call.callee + ": unknown model binding '" + ident->name + "'");
    }
    return &it->second;
}

auto eval_model_table_accessor(const parser::CallExpr& call, const ModelRegistry& models)
    -> std::expected<runtime::Table, std::string> {
    auto model = lookup_model_binding(call, models);
    if (!model) {
        return std::unexpected(model.error());
    }
    if (call.callee == "coef" || call.callee == "model_coef") {
        return model.value()->coefficients;
    }
    if (call.callee == "summary" || call.callee == "model_summary") {
        return model.value()->summary;
    }
    if (call.callee == "fitted" || call.callee == "model_fitted") {
        return model.value()->fitted_values;
    }
    if (call.callee == "importance" || call.callee == "model_importance") {
        return model.value()->importance;
    }
    return model.value()->residuals;
}

auto eval_model_scalar_accessor(const parser::CallExpr& call, const ModelRegistry& models)
    -> std::expected<runtime::ScalarValue, std::string> {
    auto model = lookup_model_binding(call, models);
    if (!model) {
        return std::unexpected(model.error());
    }
    return runtime::ScalarValue{model.value()->r_squared};
}

auto eval_table_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                     LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                     ColumnRegistry& columns, ModelRegistry& models,
                     const FunctionRegistry& functions, CompileTimeListRegistry& compile_time_lists,
                     const ExternDeclRegistry& extern_decls, const runtime::ExternRegistry& externs,
                     runtime::ModelResult* model_out = nullptr)
    -> std::expected<runtime::Table, std::string>;

auto eval_scalar_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                      LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                      ColumnRegistry& columns, ModelRegistry& models,
                      const FunctionRegistry& functions,
                      CompileTimeListRegistry& compile_time_lists,
                      const ExternDeclRegistry& extern_decls,
                      const runtime::ExternRegistry& externs)
    -> std::expected<runtime::ScalarValue, std::string>;

// Evaluate a call to a "series function": an aggregate (max/sum/mean/… reducing a
// series to a scalar) or a row-wise scalar builtin (abs/sqrt/pmax/… applied
// element-wise when any argument is a series, else to scalars). Returns a
// ScalarValue or, for an element-wise result, a ColumnValue. Precondition:
// ir::is_aggregate_func(callee) || runtime::is_scalar_builtin(callee).
auto eval_series_call(parser::CallExpr& call, runtime::TableRegistry& tables,
                      LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                      ColumnRegistry& columns, ModelRegistry& models,
                      const FunctionRegistry& functions,
                      CompileTimeListRegistry& compile_time_lists,
                      const ExternDeclRegistry& extern_decls,
                      const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string>;

auto eval_function_call(parser::CallExpr& call, runtime::TableRegistry& tables,
                        LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                        ColumnRegistry& columns, ModelRegistry& models,
                        const FunctionRegistry& functions,
                        CompileTimeListRegistry& compile_time_lists,
                        const ExternDeclRegistry& extern_decls,
                        const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string>;

auto try_bind_lazy_source(parser::Expr& expr, runtime::TableRegistry& tables,
                          LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                          ColumnRegistry& columns, ModelRegistry& models,
                          const FunctionRegistry& functions,
                          CompileTimeListRegistry& compile_time_lists,
                          const ExternDeclRegistry& extern_decls,
                          const runtime::ExternRegistry& externs)
    -> std::optional<std::expected<runtime::LazyTablePtr, std::string>>;

/// Undoes the inline-source rewrites made while evaluating one expression:
/// restores the replaced sub-expressions and retires the temp lazy bindings.
/// The AST must come back unchanged because the same Expr can be evaluated again
/// (a user function called twice), and the second evaluation must not reference a
/// temp name the first one dropped.
struct InlineSourceRewrites {
    LazyTableRegistry* lazy_tables = nullptr;
    std::vector<std::pair<parser::ExprPtr*, parser::ExprPtr>> replaced;
    std::vector<std::string> temp_names;

    explicit InlineSourceRewrites(LazyTableRegistry* registry) : lazy_tables(registry) {}
    InlineSourceRewrites(const InlineSourceRewrites&) = delete;
    auto operator=(const InlineSourceRewrites&) -> InlineSourceRewrites& = delete;
    InlineSourceRewrites(InlineSourceRewrites&&) = delete;
    auto operator=(InlineSourceRewrites&&) -> InlineSourceRewrites& = delete;

    ~InlineSourceRewrites() {
        for (auto& [slot, original] : replaced) {
            *slot = std::move(original);
        }
        if (lazy_tables != nullptr) {
            for (const auto& name : temp_names) {
                lazy_tables->erase(name);
            }
        }
    }
};

auto rewrite_inline_sources(parser::Expr& expr, InlineSourceRewrites& rewrites,
                            runtime::TableRegistry& tables, LazyTableRegistry& lazy_tables,
                            runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                            ModelRegistry& models, const FunctionRegistry& functions,
                            CompileTimeListRegistry& compile_time_lists,
                            const ExternDeclRegistry& extern_decls,
                            const runtime::ExternRegistry& externs) -> std::optional<std::string>;

auto is_model_predict(std::string_view callee) -> bool {
    return callee == "predict" || callee == "model_predict";
}

// model_predict(m, newdata): look up the model binding `m`, evaluate `newdata`
// as a table, and predict by reusing the model's native handle.
auto eval_model_predict(parser::CallExpr& call, runtime::TableRegistry& tables,
                        LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                        ColumnRegistry& columns, ModelRegistry& models,
                        const FunctionRegistry& functions,
                        CompileTimeListRegistry& compile_time_lists,
                        const ExternDeclRegistry& extern_decls,
                        const runtime::ExternRegistry& externs)
    -> std::expected<runtime::Table, std::string> {
    if (call.args.size() != 2 || !call.named_args.empty()) {
        return std::unexpected(call.callee + ": expected (model, newdata)");
    }
    const auto* ident = std::get_if<parser::IdentifierExpr>(&call.args[0]->node);
    if (ident == nullptr) {
        return std::unexpected(call.callee + ": first argument must be a model binding name");
    }
    auto it = models.find(ident->name);
    if (it == models.end()) {
        return std::unexpected(call.callee + ": unknown model binding '" + ident->name + "'");
    }
    auto newdata = eval_table_expr(*call.args[1], tables, lazy_tables, scalars, columns, models,
                                   functions, compile_time_lists, extern_decls, externs);
    if (!newdata) {
        return std::unexpected(newdata.error());
    }
    return runtime::predict_model(it->second, newdata.value(), externs);
}

auto eval_expr_value(parser::Expr& expr, runtime::TableRegistry& tables,
                     LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                     ColumnRegistry& columns, ModelRegistry& models,
                     const FunctionRegistry& functions, CompileTimeListRegistry& compile_time_lists,
                     const ExternDeclRegistry& extern_decls, const runtime::ExternRegistry& externs,
                     runtime::ModelResult* model_out = nullptr)
    -> std::expected<EvalValue, std::string> {
    if (const auto* group = std::get_if<parser::GroupExpr>(&expr.node)) {
        return eval_expr_value(*group->expr, tables, lazy_tables, scalars, columns, models,
                               functions, compile_time_lists, extern_decls, externs, model_out);
    }
    if (const auto* array = std::get_if<parser::ArrayLiteralExpr>(&expr.node)) {
        auto series = eval_series_literal(*array);
        if (!series) {
            return std::unexpected(series.error());
        }
        return EvalValue{std::move(series.value())};
    }
    // String interpolation (`...${expr}...`) lowers to a __interp call; it always
    // produces a scalar string.
    if (const auto* call = std::get_if<parser::CallExpr>(&expr.node);
        call != nullptr && call->callee == "__interp") {
        auto scalar = eval_scalar_expr(expr, tables, lazy_tables, scalars, columns, models,
                                       functions, compile_time_lists, extern_decls, externs);
        if (!scalar) {
            return std::unexpected(scalar.error());
        }
        return EvalValue{std::move(scalar.value())};
    }
    if (std::holds_alternative<parser::LiteralExpr>(expr.node) ||
        std::holds_alternative<parser::BinaryExpr>(expr.node) ||
        std::holds_alternative<parser::UnaryExpr>(expr.node)) {
        auto scalar = eval_scalar_expr(expr, tables, lazy_tables, scalars, columns, models,
                                       functions, compile_time_lists, extern_decls, externs);
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
        if (is_model_table_accessor(call->callee)) {
            auto table = eval_model_table_accessor(*call, models);
            if (!table) {
                return std::unexpected(table.error());
            }
            return EvalValue{std::move(table.value())};
        }
        if (is_model_predict(call->callee)) {
            auto table = eval_model_predict(*call, tables, lazy_tables, scalars, columns, models,
                                            functions, compile_time_lists, extern_decls, externs);
            if (!table) {
                return std::unexpected(table.error());
            }
            return EvalValue{std::move(table.value())};
        }
        if (is_model_scalar_accessor(call->callee)) {
            auto scalar = eval_model_scalar_accessor(*call, models);
            if (!scalar) {
                return std::unexpected(scalar.error());
            }
            return EvalValue{std::move(scalar.value())};
        }
        if (is_cast_callee(call->callee) && call->args.size() == 1) {
            auto inner =
                eval_expr_value(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs);
            if (!inner) {
                return std::unexpected(inner.error());
            }
            if (auto* col = std::get_if<runtime::ColumnValue>(&inner.value())) {
                auto result = apply_column_cast(*col, call->callee);
                if (!result) {
                    return std::unexpected(result.error());
                }
                return EvalValue{std::move(result.value())};
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&inner.value())) {
                auto result = apply_scalar_cast(*scalar, call->callee);
                if (!result) {
                    return std::unexpected(result.error());
                }
                return EvalValue{std::move(result.value())};
            }
            return std::unexpected(std::string(call->callee) + "(): cannot cast a table");
        }
        if (call->callee == "round" && call->args.size() == 2) {
            auto mode = extract_round_mode(*call->args[1]);
            if (!mode) {
                return std::unexpected(mode.error());
            }
            auto inner =
                eval_expr_value(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs);
            if (!inner) {
                return std::unexpected(inner.error());
            }
            if (auto* col = std::get_if<runtime::ColumnValue>(&inner.value())) {
                if (!std::holds_alternative<Column<double>>(*col)) {
                    return std::unexpected("round(): column must be Float");
                }
                return EvalValue{runtime::ColumnValue{
                    apply_column_round(std::get<Column<double>>(*col), *mode)}};
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&inner.value())) {
                if (!std::holds_alternative<double>(*scalar)) {
                    return std::unexpected("round(): first argument must be Float");
                }
                return EvalValue{apply_scalar_round(std::get<double>(*scalar), *mode).value()};
            }
            return std::unexpected("round(): cannot round a table");
        }
        if (call->callee == "print") {
            if (call->args.size() != 1 || !call->named_args.empty()) {
                return std::unexpected("print: expected exactly one argument");
            }
            auto inner =
                eval_expr_value(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs);
            if (!inner) {
                return std::unexpected(inner.error());
            }
            // Display as a side effect and pass the value through unchanged, so
            // `print(x)` composes (e.g. `let y = print(x);` shows and binds).
            render_eval_value(inner.value());
            return inner;
        }
        if (call->callee == "seed_rng") {
            if (call->args.size() != 1 || !call->named_args.empty()) {
                return std::unexpected("seed_rng: expected exactly one argument (seed: Int)");
            }
            auto seed_val =
                eval_scalar_expr(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                 functions, compile_time_lists, extern_decls, externs);
            if (!seed_val) {
                return std::unexpected(seed_val.error());
            }
            if (!std::holds_alternative<std::int64_t>(*seed_val)) {
                return std::unexpected("seed_rng: argument must be an integer");
            }
            const auto seed = static_cast<std::uint64_t>(std::get<std::int64_t>(*seed_val));
            runtime::reseed(seed);
            // Return the seed so the REPL can confirm what was set.
            return EvalValue{static_cast<std::int64_t>(seed)};
        }
        if (call->callee == "scalar") {
            auto scalar = eval_scalar_expr(expr, tables, lazy_tables, scalars, columns, models,
                                           functions, compile_time_lists, extern_decls, externs);
            if (!scalar) {
                return std::unexpected(scalar.error());
            }
            return EvalValue{std::move(scalar.value())};
        }
        if (functions.contains(call->callee)) {
            return eval_function_call(*call, tables, lazy_tables, scalars, columns, models,
                                      functions, compile_time_lists, extern_decls, externs);
        }
        if (extern_decls.contains(call->callee)) {
            const auto& decl = extern_decls.at(call->callee);
            if (decl.return_type.kind == parser::Type::Kind::Scalar) {
                auto scalar =
                    eval_scalar_expr(expr, tables, lazy_tables, scalars, columns, models, functions,
                                     compile_time_lists, extern_decls, externs);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                return EvalValue{std::move(scalar.value())};
            }
            auto table =
                eval_table_expr(expr, tables, lazy_tables, scalars, columns, models, functions,
                                compile_time_lists, extern_decls, externs, model_out);
            if (!table) {
                return std::unexpected(table.error());
            }
            return EvalValue{std::move(table.value())};
        }
        // Series functions: aggregates over a series (max/sum/mean/… -> scalar)
        // and row-wise scalar builtins (sqrt/abs/pmax/… -> scalar, or a series
        // when applied element-wise to series arguments). Routed here rather than
        // treated as a table function.
        if (ir::is_aggregate_func(call->callee) || runtime::is_scalar_builtin(call->callee)) {
            return eval_series_call(*call, tables, lazy_tables, scalars, columns, models, functions,
                                    compile_time_lists, extern_decls, externs);
        }
    }
    auto table = eval_table_expr(expr, tables, lazy_tables, scalars, columns, models, functions,
                                 compile_time_lists, extern_decls, externs, model_out);
    if (!table) {
        return std::unexpected(table.error());
    }
    return EvalValue{std::move(table.value())};
}

auto eval_scalar_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                      LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                      ColumnRegistry& columns, ModelRegistry& models,
                      const FunctionRegistry& functions,
                      CompileTimeListRegistry& compile_time_lists,
                      const ExternDeclRegistry& extern_decls,
                      const runtime::ExternRegistry& externs)
    -> std::expected<runtime::ScalarValue, std::string> {
    if (const auto* literal = std::get_if<parser::LiteralExpr>(&expr.node)) {
        if (const auto* int_value = std::get_if<std::int64_t>(&literal->value)) {
            return runtime::ScalarValue{*int_value};
        }
        if (const auto* double_value = std::get_if<double>(&literal->value)) {
            return runtime::ScalarValue{*double_value};
        }
        if (const auto* bool_value = std::get_if<bool>(&literal->value)) {
            return runtime::ScalarValue{*bool_value};
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
        return eval_scalar_expr(*group->expr, tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs);
    }
    if (const auto* unary = std::get_if<parser::UnaryExpr>(&expr.node)) {
        auto value = eval_scalar_expr(*unary->expr, tables, lazy_tables, scalars, columns, models,
                                      functions, compile_time_lists, extern_decls, externs);
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
        if (std::holds_alternative<bool>(value.value())) {
            return std::unexpected("boolean arithmetic not supported");
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
        auto left = eval_scalar_expr(*binary->left, tables, lazy_tables, scalars, columns, models,
                                     functions, compile_time_lists, extern_decls, externs);
        if (!left) {
            return std::unexpected(left.error());
        }
        auto right = eval_scalar_expr(*binary->right, tables, lazy_tables, scalars, columns, models,
                                      functions, compile_time_lists, extern_decls, externs);
        if (!right) {
            return std::unexpected(right.error());
        }
        if (std::holds_alternative<Date>(left.value()) ||
            std::holds_alternative<Date>(right.value()) ||
            std::holds_alternative<Timestamp>(left.value()) ||
            std::holds_alternative<Timestamp>(right.value())) {
            return std::unexpected("date/time arithmetic not supported");
        }
        if (std::holds_alternative<bool>(left.value()) ||
            std::holds_alternative<bool>(right.value())) {
            return std::unexpected("boolean arithmetic not supported");
        }
        const bool left_double = std::holds_alternative<double>(left.value());
        const bool right_double = std::holds_alternative<double>(right.value());
        if (left_double || right_double) {
            const double lhs = left_double
                                   ? std::get<double>(left.value())
                                   : static_cast<double>(std::get<std::int64_t>(left.value()));
            const double rhs = right_double
                                   ? std::get<double>(right.value())
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
                return runtime::ScalarValue{runtime::safe_idiv(lhs, rhs)};
            case parser::BinaryOp::Mod:
                return runtime::ScalarValue{runtime::safe_imod(lhs, rhs)};
            default:
                return std::unexpected("unsupported operator in scalar expression");
        }
    }
    if (auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (call->callee == "__interp") {
            // String interpolation: stringify each argument (literal segments and
            // embedded value expressions alike) and concatenate. format_scalar
            // renders strings unquoted, so the literals read verbatim.
            std::string out;
            for (auto& arg : call->args) {
                auto value = eval_scalar_expr(*arg, tables, lazy_tables, scalars, columns, models,
                                              functions, compile_time_lists, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                out += format_scalar(*value);
            }
            return runtime::ScalarValue{std::move(out)};
        }
        if (is_model_scalar_accessor(call->callee)) {
            return eval_model_scalar_accessor(*call, models);
        }
        if (call->callee == "scalar") {
            if (call->args.size() != 2) {
                return std::unexpected("scalar() expects (table, column)");
            }
            auto column_name = column_name_from_expr(*call->args[1]);
            if (!column_name.has_value()) {
                return std::unexpected("scalar() column must be identifier or string");
            }
            auto table =
                eval_table_expr(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs);
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
            auto value = eval_function_call(*call, tables, lazy_tables, scalars, columns, models,
                                            functions, compile_time_lists, extern_decls, externs);
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
            auto bound_args = bind_call_arguments(call->callee, *call, decl.params);
            if (!bound_args) {
                return std::unexpected(bound_args.error());
            }
            if (fn->first_arg_is_table) {
                // First argument is a DataFrame; remaining arguments are scalars.
                if (bound_args->empty()) {
                    return std::unexpected(call->callee + "() requires a DataFrame first argument");
                }
                auto table =
                    eval_table_expr(*(*bound_args)[0].expr, tables, lazy_tables, scalars, columns,
                                    models, functions, compile_time_lists, extern_decls, externs);
                if (!table) {
                    return std::unexpected(table.error());
                }
                runtime::ExternArgs scalar_args;
                scalar_args.reserve(bound_args->size() - 1);
                for (std::size_t i = 1; i < bound_args->size(); ++i) {
                    auto value = eval_scalar_expr(*(*bound_args)[i].expr, tables, lazy_tables,
                                                  scalars, columns, models, functions,
                                                  compile_time_lists, extern_decls, externs);
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
            args.reserve(bound_args->size());
            for (const auto& arg : *bound_args) {
                auto value =
                    eval_scalar_expr(*arg.expr, tables, lazy_tables, scalars, columns, models,
                                     functions, compile_time_lists, extern_decls, externs);
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
        if (is_cast_callee(call->callee)) {
            if (call->args.size() != 1) {
                return std::unexpected(call->callee + "(): expects exactly one argument");
            }
            auto value =
                eval_scalar_expr(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                 functions, compile_time_lists, extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            return apply_scalar_cast(value.value(), call->callee);
        }
        if (call->callee == "round") {
            if (call->args.size() != 2) {
                return std::unexpected(
                    "round(): expects (value, mode) — mode: nearest, bankers, floor, ceil, trunc");
            }
            auto mode = extract_round_mode(*call->args[1]);
            if (!mode) {
                return std::unexpected(mode.error());
            }
            auto value =
                eval_scalar_expr(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                 functions, compile_time_lists, extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            if (!std::holds_alternative<double>(*value)) {
                return std::unexpected("round(): first argument must be Float");
            }
            return apply_scalar_round(std::get<double>(*value), *mode);
        }
        // Aggregates over a series (max/sum/mean/…) and row-wise scalar builtins
        // (sqrt/sin/abs/pmax/…) both flow through eval_series_call. In scalar
        // position the result must be a scalar — an element-wise series result
        // (e.g. pmax(s1, s2)) is an error here, but works via eval_expr_value.
        if (ir::is_aggregate_func(call->callee) || runtime::is_scalar_builtin(call->callee)) {
            auto value = eval_series_call(*call, tables, lazy_tables, scalars, columns, models,
                                          functions, compile_time_lists, extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            if (auto* s = std::get_if<runtime::ScalarValue>(&value.value())) {
                return *s;
            }
            return std::unexpected(call->callee +
                                   "(): produced a series where a scalar was expected");
        }
        return std::unexpected("unknown function: " + call->callee + " (available: " +
                               format_function_names(functions, extern_decls) + ")");
    }
    return std::unexpected("expected scalar expression");
}

// Extract element `i` of a column as a scalar (Categorical/String -> std::string).
auto column_element(const runtime::ColumnValue& col, std::size_t i) -> runtime::ScalarValue {
    return std::visit(
        [i](const auto& c) -> runtime::ScalarValue {
            auto value = c[i];
            using V = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<V, std::string_view>) {
                return runtime::ScalarValue{std::string(value)};
            } else {
                return runtime::ScalarValue{value};
            }
        },
        col);
}

// Build a homogeneous column from a sequence of scalar values (all the same
// variant alternative). Used to materialize an element-wise builtin result.
auto column_from_scalars(const std::vector<runtime::ScalarValue>& vals)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (vals.empty()) {
        return std::unexpected("internal: empty element-wise result");
    }
    return std::visit(
        [&](const auto& first) -> std::expected<runtime::ColumnValue, std::string> {
            using T = std::decay_t<decltype(first)>;
            Column<T> col;
            col.reserve(vals.size());
            for (const auto& v : vals) {
                const auto* p = std::get_if<T>(&v);
                if (p == nullptr) {
                    return std::unexpected("element-wise result has inconsistent types");
                }
                col.push_back(*p);
            }
            return runtime::ColumnValue{std::move(col)};
        },
        vals.front());
}

auto eval_series_call(parser::CallExpr& call, runtime::TableRegistry& tables,
                      LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                      ColumnRegistry& columns, ModelRegistry& models,
                      const FunctionRegistry& functions,
                      CompileTimeListRegistry& compile_time_lists,
                      const ExternDeclRegistry& extern_decls,
                      const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string> {
    // Evaluate every positional argument as a value (scalar or series).
    std::vector<EvalValue> arg_values;
    arg_values.reserve(call.args.size());
    for (auto& arg : call.args) {
        auto v = eval_expr_value(*arg, tables, lazy_tables, scalars, columns, models, functions,
                                 compile_time_lists, extern_decls, externs);
        if (!v) {
            return std::unexpected(v.error());
        }
        if (std::holds_alternative<runtime::Table>(v.value())) {
            return std::unexpected(call.callee + "(): argument must be a scalar or a series");
        }
        arg_values.push_back(std::move(v.value()));
    }

    // Aggregate: reduce a single series to a scalar (max/sum/mean/...).
    if (ir::is_aggregate_func(call.callee)) {
        if (arg_values.empty()) {
            return std::unexpected(call.callee + "(): expected a series argument");
        }
        const auto* col = std::get_if<runtime::ColumnValue>(&arg_values[0]);
        if (col == nullptr) {
            return std::unexpected(call.callee + "(): argument must be a series (column)");
        }
        double param = 0.0;
        if (call.callee == "ewma" || call.callee == "quantile") {
            if (arg_values.size() != 2) {
                return std::unexpected(call.callee + "(): expected (series, number)");
            }
            const auto* p = std::get_if<runtime::ScalarValue>(&arg_values[1]);
            if (p == nullptr) {
                return std::unexpected(call.callee + "(): second argument must be a number");
            }
            if (const auto* d = std::get_if<double>(p)) {
                param = *d;
            } else if (const auto* iv = std::get_if<std::int64_t>(p)) {
                param = static_cast<double>(*iv);
            } else {
                return std::unexpected(call.callee + "(): second argument must be a number");
            }
        }
        auto result = runtime::aggregate_series(call.callee, *col, param);
        if (!result) {
            return std::unexpected(result.error());
        }
        return EvalValue{std::move(result.value())};
    }

    // Scalar builtin (abs/sqrt/sin/pmax/...): element-wise when any argument is a
    // series (broadcasting scalars), otherwise a plain scalar result.
    std::size_t n = 0;
    bool any_series = false;
    for (const auto& v : arg_values) {
        if (const auto* c = std::get_if<runtime::ColumnValue>(&v)) {
            any_series = true;
            const std::size_t len = runtime::column_size(*c);
            if (!any_series || n == 0) {
                n = len;
            }
            if (len != n) {
                return std::unexpected(call.callee + "(): series arguments have different lengths");
            }
        }
    }
    if (!any_series) {
        std::vector<runtime::ScalarValue> scalar_args;
        scalar_args.reserve(arg_values.size());
        for (auto& v : arg_values) {
            scalar_args.push_back(std::get<runtime::ScalarValue>(v));
        }
        auto result = runtime::eval_scalar_builtin(call.callee, scalar_args);
        if (!result) {
            return std::unexpected(result.error());
        }
        return EvalValue{std::move(result.value())};
    }
    if (n == 0) {
        return std::unexpected(call.callee + "(): cannot apply to an empty series");
    }
    std::vector<runtime::ScalarValue> results;
    results.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        std::vector<runtime::ScalarValue> row_args;
        row_args.reserve(arg_values.size());
        for (const auto& v : arg_values) {
            if (const auto* c = std::get_if<runtime::ColumnValue>(&v)) {
                row_args.push_back(column_element(*c, i));
            } else {
                row_args.push_back(std::get<runtime::ScalarValue>(v));
            }
        }
        auto r = runtime::eval_scalar_builtin(call.callee, row_args);
        if (!r) {
            return std::unexpected(r.error());
        }
        results.push_back(std::move(r.value()));
    }
    auto col = column_from_scalars(results);
    if (!col) {
        return std::unexpected(col.error());
    }
    return EvalValue{std::move(col.value())};
}

auto eval_table_expr(parser::Expr& expr, runtime::TableRegistry& tables,
                     LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                     ColumnRegistry& columns, ModelRegistry& models,
                     const FunctionRegistry& functions, CompileTimeListRegistry& compile_time_lists,
                     const ExternDeclRegistry& extern_decls, const runtime::ExternRegistry& externs,
                     runtime::ModelResult* model_out)
    -> std::expected<runtime::Table, std::string> {
    if (auto* table_expr = std::get_if<parser::TableExpr>(&expr.node);
        table_expr != nullptr && table_expr->row_count == nullptr) {
        runtime::Table out;
        for (const auto& col_def : table_expr->columns) {
            std::expected<EvalValue, std::string> value = std::unexpected("");
            if (const auto* array = std::get_if<parser::ArrayLiteralExpr>(&col_def.expr->node);
                array != nullptr && array->elements.empty()) {
                auto series = eval_series_literal(*array, parser::ScalarType::Int64);
                if (!series) {
                    return std::unexpected(series.error());
                }
                value = EvalValue{std::move(series.value())};
            } else {
                value =
                    eval_expr_value(*col_def.expr, tables, lazy_tables, scalars, columns, models,
                                    functions, compile_time_lists, extern_decls, externs);
            }
            if (!value) {
                return std::unexpected(value.error());
            }

            runtime::ColumnValue column;
            if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                column = std::move(*col);
            } else if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                if (table->columns.size() == 1) {
                    column = *table->columns.front().column;
                } else if (const auto* found = table->find(col_def.name); found != nullptr) {
                    column = *found;
                } else {
                    return std::unexpected("Table constructor: expression for column '" +
                                           col_def.name +
                                           "' produced a table with no matching column");
                }
            } else {
                return std::unexpected("Table constructor: expression for column '" + col_def.name +
                                       "' must evaluate to a Series or DataFrame");
            }

            if (!out.columns.empty() && runtime::column_size(column) != out.rows()) {
                return std::unexpected("Table constructor: column '" + col_def.name +
                                       "' length does not match previous columns");
            }
            out.add_column(col_def.name, std::move(column));
        }
        return out;
    }

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
        auto bound_args = bind_call_arguments(call.callee, call, decl.params);
        if (!bound_args) {
            return std::unexpected(bound_args.error());
        }
        runtime::ExternArgs args;
        args.reserve(bound_args->size());
        for (const auto& arg : *bound_args) {
            auto value = eval_scalar_expr(*arg.expr, tables, lazy_tables, scalars, columns, models,
                                          functions, compile_time_lists, extern_decls, externs);
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
                auto value =
                    eval_function_call(*call, tables, lazy_tables, scalars, columns, models,
                                       functions, compile_time_lists, extern_decls, externs);
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
            }
        }
    }

    // Every inline source in this expression — `read_parquet(p)` wherever it
    // appears, not merely as the outermost base — is bound to a temp lazy binding
    // and replaced by that name, so it reaches `interpret` as an ordinary Scan and
    // picks up the same projection pushdown a `let`-bound source gets.
    //
    // Walking the whole tree is the point. A query written as one expression puts
    // its sources inside join operands:
    //
    //     (read_parquet(a)[select …] join read_parquet(b)[select …] on k)[filter …]
    //
    // Rewriting only the outermost base would leave those two reads eager, and the
    // single-expression form — the one the optimizer can actually see through —
    // would read every column of both tables and end up SLOWER than the same query
    // split across `let`s.
    //
    // Every rewrite is undone on the way out: this Expr may be evaluated more than
    // once (a function body called twice), and the second evaluation must not find
    // a name the first one retired.
    InlineSourceRewrites inline_sources{&lazy_tables};
    if (auto err =
            rewrite_inline_sources(expr, inline_sources, tables, lazy_tables, scalars, columns,
                                   models, functions, compile_time_lists, extern_decls, externs)) {
        return std::unexpected(*err);
    }
    if (auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (is_model_table_accessor(call->callee)) {
            return eval_model_table_accessor(*call, models);
        }
        if (is_model_predict(call->callee)) {
            return eval_model_predict(*call, tables, lazy_tables, scalars, columns, models,
                                      functions, compile_time_lists, extern_decls, externs);
        }
        if (call->callee == "print") {
            if (call->args.size() != 1 || !call->named_args.empty()) {
                return std::unexpected("print: expected exactly one argument");
            }
            auto inner =
                eval_table_expr(*call->args[0], tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs, model_out);
            if (!inner) {
                return std::unexpected(inner.error());
            }
            print_table(inner.value());
            return inner;
        }
        if (functions.contains(call->callee)) {
            auto value = eval_function_call(*call, tables, lazy_tables, scalars, columns, models,
                                            functions, compile_time_lists, extern_decls, externs);
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
        // A lazy binding named on its own — printed, passed to a function, written
        // out — is consumed whole, so there is nothing to push down. Force it.
        if (auto it = lazy_tables.find(ident->name); it != lazy_tables.end()) {
            return it->second->materialize();
        }
        return std::unexpected("unknown table: " + ident->name +
                               " (available: " + format_table_names(tables, lazy_tables) + ")");
    }
    parser::LowerContext context;
    context.compile_time_lists = compile_time_lists;
    for (const auto& [name, decl] : extern_decls) {
        if (decl.return_type.kind == parser::Type::Kind::DataFrame ||
            decl.return_type.kind == parser::Type::Kind::TimeFrame) {
            context.table_externs.insert(name);
            context.table_extern_decls.insert_or_assign(name, &decl);
        }
        if (!decl.params.empty() && decl.params[0].type.kind == parser::Type::Kind::DataFrame) {
            context.sink_externs.insert(name);
        }
    }
    // Supply every in-scope binding name so the lowerer can statically validate
    // column references in filter/computed expressions without false positives.
    // A superset is safe; under-inclusion is not, so include all registries.
    for (const auto& entry : scalars) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : columns) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : models) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : functions) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : compile_time_lists) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : tables) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : lazy_tables) {
        context.lexical_names.insert(entry.first);
    }
    // Carry the exact schema of each in-scope table binding into the lowerer so
    // references to a let-bound table are checked statically in this expression.
    // A lazy binding knows its schema without having decoded anything, so it is
    // checked just as strictly as a materialized one.
    for (const auto& [name, table] : tables) {
        context.source_schemas.insert_or_assign(name, table_schema_info(table));
    }
    for (const auto& [name, lazy] : lazy_tables) {
        context.source_schemas.insert_or_assign(name, table_schema_info(lazy->schema()));
    }
    // Scalar user functions are inlined when called inside clause expressions.
    for (const auto& [name, decl] : functions) {
        context.functions.insert_or_assign(name, &decl);
    }
    auto lowered = parser::lower_expr(expr, context);
    if (!lowered) {
        return std::unexpected(lowered.error().message);
    }

    // Projection pushdown. Ask the plan which columns it actually reads from
    // each source, and materialize only those from any lazy binding it scans.
    // `interpret` then runs against a registry in which every scanned source is
    // an ordinary Table — it never learns that a read was deferred.
    const runtime::TableRegistry* eval_tables = &tables;
    runtime::TableRegistry projected;
    std::set<std::string> applied_scan_filters;
    if (!lazy_tables.empty()) {
        auto demand = ir::required_columns(*lowered.value());
        auto predicates = ir::scan_predicates(*lowered.value());
        projected = tables;
        for (const auto& [name, lazy] : lazy_tables) {
            auto needed = demand.find(name);
            if (needed == demand.end()) {
                continue;  // this plan never scans it
            }
            std::expected<runtime::Table, std::string> table;
            if (auto pushed = predicates.find(name); pushed != predicates.end()) {
                std::set<std::string> names = needed->second.names;
                if (needed->second.all) {
                    for (const auto& field : lazy->schema().columns) {
                        names.insert(field.name);
                    }
                }
                table = lazy->project_where(names, pushed->second, &scalars);
                applied_scan_filters.insert(name);
            } else {
                table =
                    needed->second.all ? lazy->materialize() : lazy->project(needed->second.names);
            }
            if (!table) {
                return std::unexpected(table.error());
            }
            projected.insert_or_assign(name, std::move(table.value()));
        }
        eval_tables = &projected;
    }

    if (!applied_scan_filters.empty()) {
        lowered.value() =
            ir::remove_applied_scan_filters(std::move(lowered.value()), applied_scan_filters);
    }

    runtime::ModelResult captured_model;
    auto evaluated = runtime::interpret(*lowered.value(), *eval_tables, &scalars, &externs,
                                        model_out != nullptr ? &captured_model : nullptr);
    if (!evaluated) {
        return std::unexpected(evaluated.error());
    }
    if (model_out != nullptr) {
        *model_out = std::move(captured_model);
    }
    return std::move(evaluated.value());
}

auto eval_function_call(parser::CallExpr& call, runtime::TableRegistry& tables,
                        LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                        ColumnRegistry& columns, ModelRegistry& models,
                        const FunctionRegistry& functions,
                        CompileTimeListRegistry& compile_time_lists,
                        const ExternDeclRegistry& extern_decls,
                        const runtime::ExternRegistry& externs)
    -> std::expected<EvalValue, std::string> {
    auto it = functions.find(call.callee);
    if (it == functions.end()) {
        return std::unexpected("unknown function: " + call.callee + " (available: " +
                               format_function_names(functions, extern_decls) + ")");
    }
    const auto& fn = it->second;
    auto bound_args = bind_call_arguments(call.callee, call, fn.params);
    if (!bound_args) {
        return std::unexpected(bound_args.error());
    }

    runtime::TableRegistry local_tables = tables;
    // Lazy bindings are visible inside a function body too; the handles are
    // shared, so a column the caller already decoded is not decoded again.
    LazyTableRegistry local_lazy_tables = lazy_tables;
    runtime::ScalarRegistry local_scalars = scalars;
    ColumnRegistry local_columns = columns;
    ModelRegistry local_models = models;
    CompileTimeListRegistry local_compile_time_lists = compile_time_lists;

    for (std::size_t i = 0; i < fn.params.size(); ++i) {
        const auto& param = fn.params[i];
        auto& arg = *(*bound_args)[i].expr;
        switch (param.type.kind) {
            case parser::Type::Kind::Scalar: {
                auto value = eval_scalar_expr(arg, tables, lazy_tables, scalars, columns, models,
                                              functions, compile_time_lists, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                const auto* st = std::get_if<parser::ScalarType>(&param.type.arg);
                if (st != nullptr) {
                    try_widen_int_to_float(value.value(), *st);
                }
                if (st != nullptr && !scalar_type_matches(value.value(), *st)) {
                    return std::unexpected(call.callee + ": type mismatch for parameter '" +
                                           param.name + "': expected " +
                                           std::string(scalar_type_name(*st)) + " but got " +
                                           std::string(scalar_value_type_name(value.value())));
                }
                local_scalars.insert_or_assign(param.name, std::move(value.value()));
                break;
            }
            case parser::Type::Kind::DataFrame:
            case parser::Type::Kind::TimeFrame: {
                auto value = eval_table_expr(arg, tables, lazy_tables, scalars, columns, models,
                                             functions, compile_time_lists, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto err = validate_table_type(value.value(), param.type)) {
                    return std::unexpected(call.callee + ": type mismatch for parameter '" +
                                           param.name + "': " + *err);
                }
                local_tables.insert_or_assign(param.name, std::move(value.value()));
                break;
            }
            case parser::Type::Kind::Series:
                if (auto value =
                        eval_expr_value(arg, tables, lazy_tables, scalars, columns, models,
                                        functions, compile_time_lists, extern_decls, externs)) {
                    if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                        if (auto err = validate_column_type(*col, param.type)) {
                            return std::unexpected(call.callee + ": type mismatch for parameter '" +
                                                   param.name + "': " + *err);
                        }
                        local_columns.insert_or_assign(param.name, std::move(*col));
                        break;
                    }
                    if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                        if (table->columns.size() != 1) {
                            return std::unexpected("Column argument must have exactly one column");
                        }
                        if (auto err =
                                validate_column_type(*table->columns.front().column, param.type)) {
                            return std::unexpected(call.callee + ": type mismatch for parameter '" +
                                                   param.name + "': " + *err);
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
            const bool type_is_scalar =
                let_stmt.type.has_value() && let_stmt.type->kind == parser::Type::Kind::Scalar;
            const bool type_is_table = let_stmt.type.has_value() &&
                                       (let_stmt.type->kind == parser::Type::Kind::DataFrame ||
                                        let_stmt.type->kind == parser::Type::Kind::TimeFrame);
            if (type_is_scalar) {
                auto value = eval_scalar_expr(*let_stmt.value, local_tables, local_lazy_tables,
                                              local_scalars, local_columns, local_models, functions,
                                              local_compile_time_lists, extern_decls, externs);
                if (!value) {
                    return std::unexpected(value.error());
                }
                const auto* st = std::get_if<parser::ScalarType>(&let_stmt.type->arg);
                if (st != nullptr) {
                    try_widen_int_to_float(value.value(), *st);
                }
                if (st != nullptr && !scalar_type_matches(value.value(), *st)) {
                    return std::unexpected("type error: '" + let_stmt.name + "' declared as " +
                                           std::string(scalar_type_name(*st)) + " but value is " +
                                           std::string(scalar_value_type_name(value.value())));
                }
                local_scalars.insert_or_assign(let_stmt.name, std::move(value.value()));
                local_compile_time_lists.erase(let_stmt.name);
                local_models.erase(let_stmt.name);
            } else if (type_is_table) {
                runtime::ModelResult model_value;
                auto value =
                    eval_table_expr(*let_stmt.value, local_tables, local_lazy_tables, local_scalars,
                                    local_columns, local_models, functions,
                                    local_compile_time_lists, extern_decls, externs, &model_value);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto err = validate_table_type(value.value(), *let_stmt.type)) {
                    return std::unexpected("type error: '" + let_stmt.name + "': " + *err);
                }
                auto table_value = std::move(value.value());
                auto compile_time_list = extract_compile_time_string_list(table_value);
                local_tables.insert_or_assign(let_stmt.name, std::move(table_value));
                if (compile_time_list.has_value()) {
                    local_compile_time_lists.insert_or_assign(let_stmt.name,
                                                              std::move(*compile_time_list));
                } else {
                    local_compile_time_lists.erase(let_stmt.name);
                }
                if (has_model_result(model_value)) {
                    local_models.insert_or_assign(let_stmt.name, std::move(model_value));
                } else {
                    local_models.erase(let_stmt.name);
                }
            } else if (let_stmt.type.has_value() &&
                       let_stmt.type->kind == parser::Type::Kind::Series) {
                std::expected<EvalValue, std::string> value = std::unexpected("");
                if (const auto* array =
                        std::get_if<parser::ArrayLiteralExpr>(&let_stmt.value->node)) {
                    const auto* st = std::get_if<parser::ScalarType>(&let_stmt.type->arg);
                    auto series = eval_series_literal(
                        *array, st != nullptr ? std::optional{*st} : std::nullopt);
                    if (!series) {
                        return std::unexpected(series.error());
                    }
                    value = EvalValue{std::move(series.value())};
                } else {
                    value = eval_expr_value(*let_stmt.value, local_tables, local_lazy_tables,
                                            local_scalars, local_columns, local_models, functions,
                                            local_compile_time_lists, extern_decls, externs);
                }
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                    if (auto err = validate_column_type(*col, *let_stmt.type)) {
                        return std::unexpected("type error: '" + let_stmt.name + "': " + *err);
                    }
                    local_columns.insert_or_assign(let_stmt.name, std::move(*col));
                } else if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                    if (table->columns.size() != 1) {
                        return std::unexpected("Series binding must have exactly one column");
                    }
                    if (auto err =
                            validate_column_type(*table->columns.front().column, *let_stmt.type)) {
                        return std::unexpected("type error: '" + let_stmt.name + "': " + *err);
                    }
                    local_columns.insert_or_assign(let_stmt.name, *table->columns.front().column);
                } else {
                    return std::unexpected("Series binding must be a Series or DataFrame");
                }
                local_compile_time_lists.erase(let_stmt.name);
                local_models.erase(let_stmt.name);
            } else {
                runtime::ModelResult model_value;
                auto value =
                    eval_expr_value(*let_stmt.value, local_tables, local_lazy_tables, local_scalars,
                                    local_columns, local_models, functions,
                                    local_compile_time_lists, extern_decls, externs, &model_value);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                    local_scalars.insert_or_assign(let_stmt.name, std::move(*scalar));
                    local_compile_time_lists.erase(let_stmt.name);
                    local_models.erase(let_stmt.name);
                } else if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                    local_columns.insert_or_assign(let_stmt.name, std::move(*col));
                    if (auto string_list = extract_compile_time_string_list(*let_stmt.value);
                        string_list.has_value()) {
                        local_compile_time_lists.insert_or_assign(let_stmt.name,
                                                                  std::move(*string_list));
                    } else {
                        local_compile_time_lists.erase(let_stmt.name);
                    }
                    local_models.erase(let_stmt.name);
                } else {
                    auto table_value = std::get<runtime::Table>(std::move(value.value()));
                    auto compile_time_list = extract_compile_time_string_list(table_value);
                    local_tables.insert_or_assign(let_stmt.name, std::move(table_value));
                    if (compile_time_list.has_value()) {
                        local_compile_time_lists.insert_or_assign(let_stmt.name,
                                                                  std::move(*compile_time_list));
                    } else {
                        local_compile_time_lists.erase(let_stmt.name);
                    }
                    if (has_model_result(model_value)) {
                        local_models.insert_or_assign(let_stmt.name, std::move(model_value));
                    } else {
                        local_models.erase(let_stmt.name);
                    }
                }
            }
            continue;
        }
        if (std::holds_alternative<parser::TupleLetStmt>(stmt)) {
            const auto& tlet = std::get<parser::TupleLetStmt>(stmt);
            auto value = eval_expr_value(*tlet.value, local_tables, local_lazy_tables,
                                         local_scalars, local_columns, local_models, functions,
                                         local_compile_time_lists, extern_decls, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            auto* table = std::get_if<runtime::Table>(&value.value());
            if (table == nullptr) {
                return std::unexpected("tuple binding requires a DataFrame on the right-hand side");
            }
            if (table->columns.size() != tlet.names.size()) {
                return std::unexpected(fmt::format("tuple binding expects {} column(s), got {}",
                                                   tlet.names.size(), table->columns.size()));
            }
            for (std::size_t i = 0; i < tlet.names.size(); ++i) {
                local_columns.insert_or_assign(tlet.names[i], *table->columns[i].column);
                local_compile_time_lists.erase(tlet.names[i]);
            }
            continue;
        }
        const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
        auto value = eval_expr_value(*expr_stmt.expr, local_tables, local_lazy_tables,
                                     local_scalars, local_columns, local_models, functions,
                                     local_compile_time_lists, extern_decls, externs);
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
            return std::unexpected(call.callee + ": return type mismatch (expected scalar)");
        }
        return EvalValue{std::get<runtime::ScalarValue>(std::move(*last_value))};
    }
    if (fn.return_type.kind == parser::Type::Kind::DataFrame ||
        fn.return_type.kind == parser::Type::Kind::TimeFrame) {
        if (!std::holds_alternative<runtime::Table>(*last_value)) {
            return std::unexpected(call.callee + ": return type mismatch (expected table)");
        }
        auto table = std::get<runtime::Table>(std::move(*last_value));
        if (auto err = validate_table_type(table, fn.return_type)) {
            return std::unexpected(call.callee + ": return type mismatch: " + *err);
        }
        return EvalValue{std::move(table)};
    }

    if (fn.return_type.kind == parser::Type::Kind::Series) {
        if (auto* col = std::get_if<runtime::ColumnValue>(&last_value.value())) {
            if (auto err = validate_column_type(*col, fn.return_type)) {
                return std::unexpected(call.callee + ": return type mismatch: " + *err);
            }
            return EvalValue{std::move(*col)};
        }
        if (auto* table = std::get_if<runtime::Table>(&last_value.value())) {
            if (table->columns.size() != 1) {
                return std::unexpected("Column return must have exactly one column");
            }
            if (auto err = validate_column_type(*table->columns.front().column, fn.return_type)) {
                return std::unexpected(call.callee + ": return type mismatch: " + *err);
            }
            return EvalValue{*table->columns.front().column};
        }
        return std::unexpected(call.callee + ": return type mismatch (expected column)");
    }

    return std::unexpected("unsupported return type");
}

/// Derive the plugin filename stem from a source_path like "csv.hpp" -> "csv".
auto plugin_stem(const std::string& source_path) -> std::string {
    const std::filesystem::path p(source_path);
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
                     robin_hood::unordered_set<std::string>& loaded_plugins,
                     runtime::ExternRegistry& externs) -> PluginLoadResult {
    if (loaded_plugins.contains(stem)) {
        return {.status = PluginLoadStatus::Loaded, .message = ""};
    }
#ifdef _WIN32
    std::string filename = stem + ".dll";
#else
    const std::string filename = stem + ".so";
#endif
    std::string last_error;
    std::string last_candidate;
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
#ifdef _WIN32
        HMODULE handle = LoadLibraryA(full_path.string().c_str());
        if (handle == nullptr) {
            if (std::filesystem::exists(full_path)) {
                last_error = fmt::format("error code {}", GetLastError());
                last_candidate = full_path.string();
            }
            continue;
        }
        using RegisterFn = void (*)(runtime::ExternRegistry*);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        auto* fn = reinterpret_cast<RegisterFn>(
            reinterpret_cast<void*>(GetProcAddress(handle, "ibex_register")));
        if (fn == nullptr) {
            FreeLibrary(handle);
            fmt::print("warning: plugin '{}' has no ibex_register symbol\n", full_path.string());
            continue;
        }
#else
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
#endif
        fn(&externs);
        loaded_plugins.insert(stem);
        spdlog::debug("loaded plugin: {}", full_path.string());
        return {.status = PluginLoadStatus::Loaded, .message = ""};
    }
    if (!last_candidate.empty()) {
        return {.status = PluginLoadStatus::LoadError,
                .message = fmt::format("failed to load '{}': {}", last_candidate,
                                       last_error.empty() ? "unknown error" : last_error)};
    }
    return {.status = PluginLoadStatus::NotFound, .message = ""};
}

/// Locate <name>.ibex in the plugin search paths and return its contents.
/// Returns std::nullopt when the file is not found in any search path.
auto find_library_source(const std::string& name, const std::vector<std::string>& search_paths)
    -> std::optional<std::string> {
    const std::string filename = name + ".ibex";
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
        std::ifstream in{full_path};
        if (!in) {
            continue;
        }
        std::string source((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        spdlog::debug("import: found library '{}' at {}", name, full_path.string());
        return source;
    }
    return std::nullopt;
}

/// Bind `expr` lazily if it is a bare call to an extern table source that can
/// decode its columns selectively (`read_parquet`). Reads the source's schema
/// and nothing else; the columns are decoded later, per query, by whatever
/// subset that query references.
///
/// Returns nullopt when `expr` is not such a call — the caller then evaluates it
/// eagerly, exactly as before. Only an un-annotated `let` takes this path; a
/// `let t: DataFrame<{...}> = ...` is checked against the declared schema first,
/// and validating that is the eager path's job.
auto try_bind_lazy_source(parser::Expr& expr, runtime::TableRegistry& tables,
                          LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                          ColumnRegistry& columns, ModelRegistry& models,
                          const FunctionRegistry& functions,
                          CompileTimeListRegistry& compile_time_lists,
                          const ExternDeclRegistry& extern_decls,
                          const runtime::ExternRegistry& externs)
    -> std::optional<std::expected<runtime::LazyTablePtr, std::string>> {
    auto* call = std::get_if<parser::CallExpr>(&expr.node);
    if (call == nullptr) {
        return std::nullopt;
    }
    const auto* fn = externs.find(call->callee);
    if (fn == nullptr || !fn->lazy_table_func) {
        return std::nullopt;
    }
    auto decl = extern_decls.find(call->callee);
    if (decl == extern_decls.end()) {
        return std::nullopt;
    }

    auto bound_args = bind_call_arguments(call->callee, *call, decl->second.params);
    if (!bound_args) {
        return std::unexpected(bound_args.error());
    }
    runtime::ExternArgs args;
    args.reserve(bound_args->size());
    for (const auto& arg : *bound_args) {
        auto value = eval_scalar_expr(*arg.expr, tables, lazy_tables, scalars, columns, models,
                                      functions, compile_time_lists, extern_decls, externs);
        if (!value) {
            return std::unexpected(value.error());
        }
        args.push_back(std::move(value.value()));
    }
    return fn->lazy_table_func(args);
}

/// Replace every inline lazy source in `expr` — `read_parquet(p)` in any table
/// position — with a temp lazy binding, so the whole expression lowers to Scans
/// and projection pushdown reaches all of them.
///
/// Recursion is over the positions where a *table* can appear: a block's base, a
/// join's operands, a parenthesised group, an ascription, and a call's arguments
/// (`write_csv(read_parquet(p)[…], out)`). A source nested in a join operand is
/// the case that matters — see the caller.
///
/// Returns an error message on failure; nullopt on success. Every replacement is
/// recorded in `rewrites`, which restores the AST when it goes out of scope.
auto rewrite_inline_sources(parser::Expr& expr, InlineSourceRewrites& rewrites,
                            runtime::TableRegistry& tables, LazyTableRegistry& lazy_tables,
                            runtime::ScalarRegistry& scalars, ColumnRegistry& columns,
                            ModelRegistry& models, const FunctionRegistry& functions,
                            CompileTimeListRegistry& compile_time_lists,
                            const ExternDeclRegistry& extern_decls,
                            const runtime::ExternRegistry& externs) -> std::optional<std::string> {
    // Rewrite the expression `slot` points at, if it is a bare call to a lazy
    // source; otherwise recurse into it.
    auto visit_slot = [&](parser::ExprPtr& slot) -> std::optional<std::string> {
        if (!slot) {
            return std::nullopt;
        }
        if (std::holds_alternative<parser::CallExpr>(slot->node)) {
            auto lazy = try_bind_lazy_source(*slot, tables, lazy_tables, scalars, columns, models,
                                             functions, compile_time_lists, extern_decls, externs);
            if (lazy.has_value()) {
                if (!*lazy) {
                    return lazy->error();
                }
                auto temp_name = make_temp_table_name();
                lazy_tables.insert_or_assign(temp_name, std::move(lazy->value()));
                rewrites.temp_names.push_back(temp_name);
                rewrites.replaced.emplace_back(&slot, std::move(slot));
                slot = std::make_unique<parser::Expr>(
                    parser::Expr{parser::IdentifierExpr{std::move(temp_name)}});
                return std::nullopt;
            }
        }
        return rewrite_inline_sources(*slot, rewrites, tables, lazy_tables, scalars, columns,
                                      models, functions, compile_time_lists, extern_decls, externs);
    };

    if (auto* block = std::get_if<parser::BlockExpr>(&expr.node)) {
        return visit_slot(block->base);
    }
    if (auto* join = std::get_if<parser::JoinExpr>(&expr.node)) {
        if (auto err = visit_slot(join->left)) {
            return err;
        }
        return visit_slot(join->right);
    }
    if (auto* group = std::get_if<parser::GroupExpr>(&expr.node)) {
        return visit_slot(group->expr);
    }
    if (auto* ascribe = std::get_if<parser::AscribeExpr>(&expr.node)) {
        return visit_slot(ascribe->base);
    }
    if (auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        for (auto& arg : call->args) {
            if (auto err = visit_slot(arg)) {
                return err;
            }
        }
        for (auto& named : call->named_args) {
            if (auto err = visit_slot(named.value)) {
                return err;
            }
        }
    }
    return std::nullopt;
}

auto execute_statements(std::vector<parser::Stmt>& statements, runtime::TableRegistry& tables,
                        LazyTableRegistry& lazy_tables, runtime::ScalarRegistry& scalars,
                        ColumnRegistry& columns, ModelRegistry& models, FunctionRegistry& functions,
                        CompileTimeListRegistry& compile_time_lists,
                        ExternDeclRegistry& extern_decls, runtime::ExternRegistry& externs,
                        const std::vector<std::string>& plugin_search_paths,
                        robin_hood::unordered_set<std::string>& loaded_plugins,
                        const std::vector<std::string>& import_search_paths = {},
                        const std::vector<std::vector<std::string>>* print_comment_groups = nullptr,
                        const std::vector<std::vector<std::string>>* doc_comment_groups = nullptr,
                        FunctionSourceRegistry* function_sources = nullptr,
                        DeclarationDocRegistry* declaration_docs = nullptr,
                        ImportRegistry* imports = nullptr, std::string_view source_text = {})
    -> bool {
    // Pre-pass: register every top-level `fn` declaration in this batch so that
    // function bodies can reference functions declared later in the same script
    // or REPL submission. We move the decls into the registry; the main loop
    // below treats FunctionDecl as a no-op since the pre-pass owns it.
    for (std::size_t stmt_index = 0; stmt_index < statements.size(); ++stmt_index) {
        auto& stmt = statements[stmt_index];
        if (std::holds_alternative<parser::FunctionDecl>(stmt)) {
            auto fn = std::get<parser::FunctionDecl>(std::move(stmt));
            if (function_sources != nullptr) {
                auto source = source_for_lines(source_text, fn.start_line, fn.end_line);
                if (!source.empty()) {
                    function_sources->insert_or_assign(fn.name, std::move(source));
                }
            }
            if (declaration_docs != nullptr && doc_comment_groups != nullptr &&
                stmt_index < doc_comment_groups->size()) {
                auto doc = doc_from_comment_group((*doc_comment_groups)[stmt_index]);
                if (!doc.empty()) {
                    declaration_docs->insert_or_assign(fn.name, std::move(doc));
                }
            }
            functions.insert_or_assign(fn.name, std::move(fn));
        }
    }
    for (std::size_t stmt_index = 0; stmt_index < statements.size(); ++stmt_index) {
        auto& stmt = statements[stmt_index];
        // Statement-level interruption boundary: a Ctrl+C during a multi
        // statement batch (:load, script) stops before the next statement.
        if (runtime::interrupt_requested()) {
            fmt::print("error: interrupted\n");
            return false;
        }
        if (print_comment_groups != nullptr && stmt_index < print_comment_groups->size()) {
            print_comment_group((*print_comment_groups)[stmt_index]);
        }
        if (std::holds_alternative<parser::ExternDecl>(stmt)) {
            auto decl = std::get<parser::ExternDecl>(std::move(stmt));
            auto decl_name = decl.name;
            if (declaration_docs != nullptr && doc_comment_groups != nullptr &&
                stmt_index < doc_comment_groups->size()) {
                auto doc = doc_from_comment_group((*doc_comment_groups)[stmt_index]);
                if (!doc.empty()) {
                    declaration_docs->insert_or_assign(decl_name, std::move(doc));
                }
            }
            extern_decls.insert_or_assign(decl_name, std::move(decl));
            const auto& stored_decl = extern_decls.at(decl_name);
            if (!stored_decl.source_path.empty()) {
                auto stem = plugin_stem(stored_decl.source_path);
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
            auto plugin_result =
                try_load_plugin(imp.name, plugin_search_paths, loaded_plugins, externs);
            if (plugin_result.status == PluginLoadStatus::LoadError) {
                fmt::print("warning: {}\n", plugin_result.message);
            }
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
            if (imports != nullptr) {
                imports->insert(imp.name);
            }
            auto parsed = parser::parse(*source);
            if (!parsed) {
                fmt::print("error: import '{}': {}\n", imp.name, parsed.error().format());
                return false;
            }
            auto import_comments = collect_script_comment_lines(*source);
            auto import_doc_groups =
                build_statement_comment_groups(parsed->statements, import_comments);
            // Recursively execute the imported file's statements (which will typically
            // only contain extern fn and fn declarations).
            if (!execute_statements(parsed->statements, tables, lazy_tables, scalars, columns,
                                    models, functions, compile_time_lists, extern_decls, externs,
                                    plugin_search_paths, loaded_plugins, import_search_paths,
                                    nullptr, &import_doc_groups, function_sources, declaration_docs,
                                    imports, *source)) {
                return false;
            }
            continue;
        }
        if (std::holds_alternative<parser::FunctionDecl>(stmt)) {
            // Already registered by the pre-pass above.
            continue;
        }
        if (std::holds_alternative<parser::LetStmt>(stmt)) {
            const auto& let_stmt = std::get<parser::LetStmt>(stmt);
            if (!let_stmt.type.has_value()) {
                if (auto lazy = try_bind_lazy_source(*let_stmt.value, tables, lazy_tables, scalars,
                                                     columns, models, functions, compile_time_lists,
                                                     extern_decls, externs)) {
                    if (!*lazy) {
                        fmt::print("error: {}\n", lazy->error());
                        return false;
                    }
                    lazy_tables.insert_or_assign(let_stmt.name, std::move(lazy->value()));
                    // A name lives in exactly one registry, so drop any binding
                    // this one shadows.
                    tables.erase(let_stmt.name);
                    scalars.erase(let_stmt.name);
                    columns.erase(let_stmt.name);
                    models.erase(let_stmt.name);
                    compile_time_lists.erase(let_stmt.name);
                    continue;
                }
            }
            if (let_stmt.type.has_value()) {
                const bool expect_scalar = let_stmt.type->kind == parser::Type::Kind::Scalar;
                const bool expect_table = let_stmt.type->kind == parser::Type::Kind::DataFrame ||
                                          let_stmt.type->kind == parser::Type::Kind::TimeFrame;
                const bool expect_column = let_stmt.type->kind == parser::Type::Kind::Series;
                if (expect_scalar) {
                    auto value = eval_scalar_expr(*let_stmt.value, tables, lazy_tables, scalars,
                                                  columns, models, functions, compile_time_lists,
                                                  extern_decls, externs);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        return false;
                    }
                    const auto* st = std::get_if<parser::ScalarType>(&let_stmt.type->arg);
                    if (st != nullptr) {
                        try_widen_int_to_float(value.value(), *st);
                    }
                    if (st != nullptr && !scalar_type_matches(value.value(), *st)) {
                        fmt::print("error: '{}' declared as {} but value is {}\n", let_stmt.name,
                                   scalar_type_name(*st), scalar_value_type_name(value.value()));
                        return false;
                    }
                    lazy_tables.erase(let_stmt.name);
                    scalars.insert_or_assign(let_stmt.name, std::move(value.value()));
                    compile_time_lists.erase(let_stmt.name);
                    models.erase(let_stmt.name);
                    continue;
                }
                if (expect_table) {
                    runtime::ModelResult model_value;
                    auto value = eval_table_expr(*let_stmt.value, tables, lazy_tables, scalars,
                                                 columns, models, functions, compile_time_lists,
                                                 extern_decls, externs, &model_value);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        return false;
                    }
                    if (auto err = validate_table_type(value.value(), *let_stmt.type)) {
                        fmt::print("error: '{}': {}\n", let_stmt.name, *err);
                        return false;
                    }
                    auto table_value = std::move(value.value());
                    auto compile_time_list = extract_compile_time_string_list(table_value);
                    lazy_tables.erase(let_stmt.name);
                    tables.insert_or_assign(let_stmt.name, std::move(table_value));
                    if (compile_time_list.has_value()) {
                        compile_time_lists.insert_or_assign(let_stmt.name,
                                                            std::move(*compile_time_list));
                    } else {
                        compile_time_lists.erase(let_stmt.name);
                    }
                    if (has_model_result(model_value)) {
                        models.insert_or_assign(let_stmt.name, std::move(model_value));
                    } else {
                        models.erase(let_stmt.name);
                    }
                    continue;
                }
                if (expect_column) {
                    std::expected<EvalValue, std::string> value = std::unexpected("");
                    if (const auto* array =
                            std::get_if<parser::ArrayLiteralExpr>(&let_stmt.value->node)) {
                        const auto* st = std::get_if<parser::ScalarType>(&let_stmt.type->arg);
                        auto series = eval_series_literal(
                            *array, st != nullptr ? std::optional{*st} : std::nullopt);
                        if (!series) {
                            fmt::print("error: {}\n", series.error());
                            return false;
                        }
                        value = EvalValue{std::move(series.value())};
                    } else {
                        value = eval_expr_value(*let_stmt.value, tables, lazy_tables, scalars,
                                                columns, models, functions, compile_time_lists,
                                                extern_decls, externs);
                    }
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        return false;
                    }
                    if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                        if (auto err = validate_column_type(*col, *let_stmt.type)) {
                            fmt::print("error: '{}': {}\n", let_stmt.name, *err);
                            return false;
                        }
                        lazy_tables.erase(let_stmt.name);
                        columns.insert_or_assign(let_stmt.name, std::move(*col));
                        compile_time_lists.erase(let_stmt.name);
                        models.erase(let_stmt.name);
                        continue;
                    }
                    if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                        if (table->columns.size() != 1) {
                            fmt::print("error: Series return must have exactly one column\n");
                            return false;
                        }
                        if (auto err = validate_column_type(*table->columns.front().column,
                                                            *let_stmt.type)) {
                            fmt::print("error: '{}': {}\n", let_stmt.name, *err);
                            return false;
                        }
                        lazy_tables.erase(let_stmt.name);
                        columns.insert_or_assign(let_stmt.name, *table->columns.front().column);
                        compile_time_lists.erase(let_stmt.name);
                        models.erase(let_stmt.name);
                        continue;
                    }
                    fmt::print("error: expected Series return for {}\n", let_stmt.name);
                    return false;
                }
            }

            runtime::ModelResult model_value;
            auto value =
                eval_expr_value(*let_stmt.value, tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs, &model_value);
            if (!value) {
                fmt::print("error: {}\n", value.error());
                return false;
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                lazy_tables.erase(let_stmt.name);
                scalars.insert_or_assign(let_stmt.name, std::move(*scalar));
                compile_time_lists.erase(let_stmt.name);
                models.erase(let_stmt.name);
            } else if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                lazy_tables.erase(let_stmt.name);
                columns.insert_or_assign(let_stmt.name, std::move(*col));
                if (auto string_list = extract_compile_time_string_list(*let_stmt.value);
                    string_list.has_value()) {
                    compile_time_lists.insert_or_assign(let_stmt.name, std::move(*string_list));
                } else {
                    compile_time_lists.erase(let_stmt.name);
                }
                models.erase(let_stmt.name);
            } else {
                auto table_value = std::get<runtime::Table>(std::move(value.value()));
                auto compile_time_list = extract_compile_time_string_list(table_value);
                tables.insert_or_assign(let_stmt.name, std::move(table_value));
                if (compile_time_list.has_value()) {
                    compile_time_lists.insert_or_assign(let_stmt.name,
                                                        std::move(*compile_time_list));
                } else {
                    compile_time_lists.erase(let_stmt.name);
                }
                if (has_model_result(model_value)) {
                    models.insert_or_assign(let_stmt.name, std::move(model_value));
                } else {
                    models.erase(let_stmt.name);
                }
            }
            continue;
        }
        if (std::holds_alternative<parser::TupleLetStmt>(stmt)) {
            const auto& tlet = std::get<parser::TupleLetStmt>(stmt);
            auto value = eval_expr_value(*tlet.value, tables, lazy_tables, scalars, columns, models,
                                         functions, compile_time_lists, extern_decls, externs);
            if (!value) {
                fmt::print("error: {}\n", value.error());
                return false;
            }
            auto* table = std::get_if<runtime::Table>(&value.value());
            if (table == nullptr) {
                fmt::print("error: tuple binding requires a DataFrame on the right-hand side\n");
                return false;
            }
            if (table->columns.size() != tlet.names.size()) {
                fmt::print("error: tuple binding expects {} column(s), got {}\n", tlet.names.size(),
                           table->columns.size());
                return false;
            }
            for (std::size_t i = 0; i < tlet.names.size(); ++i) {
                columns.insert_or_assign(tlet.names[i], *table->columns[i].column);
                compile_time_lists.erase(tlet.names[i]);
            }
            continue;
        }
        if (std::holds_alternative<parser::ExprStmt>(stmt)) {
            const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
            auto value =
                eval_expr_value(*expr_stmt.expr, tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, externs);
            if (!value) {
                fmt::print("error: {}\n", value.error());
                return false;
            }
            // A top-level `print(...)` already rendered its argument; rendering
            // the passed-through return value here too would print it twice.
            const auto* call = std::get_if<parser::CallExpr>(&expr_stmt.expr->node);
            const bool already_printed = call != nullptr && call->callee == "print";
            if (!already_printed) {
                render_eval_value(value.value());
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

auto execute_script(std::string_view source, runtime::ExternRegistry& registry,
                    const ReplConfig& config) -> bool {
    auto parsed = parser::parse(source);
    if (!parsed) {
        fmt::print("error: {}\n", parsed.error().format());
        return false;
    }
    auto tables = build_builtin_tables();
    LazyTableRegistry lazy_tables;
    runtime::ScalarRegistry scalars;
    ColumnRegistry columns;
    ModelRegistry models;
    FunctionRegistry functions;
    CompileTimeListRegistry compile_time_lists;
    ExternDeclRegistry extern_decls;
    FunctionSourceRegistry function_sources;
    DeclarationDocRegistry declaration_docs;
    ImportRegistry imports;
    auto comments = collect_script_comment_lines(source);
    auto doc_comment_groups = build_statement_comment_groups(parsed->statements, comments);
    robin_hood::unordered_set<std::string> loaded_plugins;
    return execute_statements(parsed->statements, tables, lazy_tables, scalars, columns, models,
                              functions, compile_time_lists, extern_decls, registry,
                              config.plugin_search_paths, loaded_plugins,
                              config.import_search_paths, nullptr, &doc_comment_groups,
                              &function_sources, &declaration_docs, &imports, source);
}

auto execute_script(std::string_view source, runtime::ExternRegistry& registry) -> bool {
    return execute_script(source, registry, ReplConfig{});
}

auto run_file(const std::string& path, const ReplConfig& config, runtime::ExternRegistry& registry)
    -> bool {
    std::ifstream input{path};
    if (!input) {
        fmt::print("error: failed to open '{}'\n", path);
        return false;
    }
    const std::string source((std::istreambuf_iterator<char>(input)),
                             std::istreambuf_iterator<char>());
    return execute_script(source, registry, config);
}

namespace {

/// Net open-delimiter depth of `src`, counting `()[]{}` while skipping anything
/// inside string literals and `//` / `/* */` comments. Used by the interactive
/// loop to decide whether an input is a complete statement (depth <= 0) or
/// whether it should keep reading continuation lines (depth > 0). An unclosed
/// block comment is treated as incomplete.
auto delimiter_depth(std::string_view src) -> int {
    int depth = 0;
    std::size_t i = 0;
    while (i < src.size()) {
        const char c = src[i];
        if (c == '"' || c == '\'') {
            const char quote = c;
            ++i;
            while (i < src.size()) {
                if (src[i] == '\\' && i + 1 < src.size()) {
                    i += 2;
                    continue;
                }
                if (src[i] == quote) {
                    ++i;
                    break;
                }
                ++i;
            }
            continue;
        }
        if (c == '/' && i + 1 < src.size() && src[i + 1] == '/') {
            const std::size_t nl = src.find('\n', i + 2);
            if (nl == std::string_view::npos) {
                return depth;  // line comment runs to end of buffer
            }
            i = nl;
            continue;
        }
        if (c == '/' && i + 1 < src.size() && src[i + 1] == '*') {
            const std::size_t close = src.find("*/", i + 2);
            if (close == std::string_view::npos) {
                return depth + 1;  // unterminated block comment: incomplete
            }
            i = close + 2;
            continue;
        }
        if (c == '(' || c == '[' || c == '{') {
            ++depth;
        } else if (c == ')' || c == ']' || c == '}') {
            --depth;
        }
        ++i;
    }
    return depth;
}

}  // namespace

void run(const ReplConfig& config, runtime::ExternRegistry& registry) {
    if (config.verbose) {
        spdlog::info("Ibex REPL started (verbose={})", config.verbose);
    }

    auto tables = build_builtin_tables();
    LazyTableRegistry lazy_tables;
    runtime::ScalarRegistry scalars;
    ColumnRegistry columns;
    ModelRegistry models;
    FunctionRegistry functions;
    CompileTimeListRegistry compile_time_lists;
    ExternDeclRegistry extern_decls;
    FunctionSourceRegistry function_sources;
    DeclarationDocRegistry declaration_docs;
    ImportRegistry imports;
    robin_hood::unordered_set<std::string> loaded_plugins;
    bool timing_enabled = false;
    bool load_comments_enabled = false;
    configure_line_editing();
    // Interactive sessions own SIGINT: Ctrl+C interrupts the running
    // evaluation (or clears the prompt) instead of killing the process.
    // Scripts (run_file) keep the default disposition.
    install_interrupt_handler();
    const std::string history_path = resolve_history_path(config);
    load_history_file(history_path, config.history_limit);

    // Parse and execute one complete statement buffer (possibly spanning
    // several physical lines). Shared by the normal path and the multi-line
    // continuation path.
    auto submit_buffer = [&](const std::string& buffer) {
        auto normalized = normalize_input(buffer);
        auto parsed = parser::parse(normalized);
        if (!parsed) {
            fmt::print("error: {}\n", parsed.error().format());
            return;
        }
        execute_statements(parsed->statements, tables, lazy_tables, scalars, columns, models,
                           functions, compile_time_lists, extern_decls, registry,
                           config.plugin_search_paths, loaded_plugins, config.import_search_paths,
                           nullptr, nullptr, &function_sources, &declaration_docs, &imports,
                           normalized);
    };

    // Accumulates a statement whose delimiters span multiple input lines. Empty
    // while at a statement boundary; non-empty while awaiting continuation lines.
    std::string pending;
    const std::string continuation_prompt =
        config.prompt.empty() ? std::string{} : std::string(config.prompt.size() - 1, '.') + " ";

    std::string line;
    while (true) {
        set_completion_context(CompletionContext{
            .tables = &tables,
            .scalars = &scalars,
            .columns = &columns,
            .models = &models,
            .functions = &functions,
            .compile_time_lists = &compile_time_lists,
            .extern_decls = &extern_decls,
            .imports = &imports,
        });
        const auto status =
            read_repl_line(pending.empty() ? config.prompt : continuation_prompt, line);
        if (status == ReadLineStatus::Interrupted) {
            // Ctrl+C at the prompt: discard the line and any multi-line
            // continuation in progress, then show a fresh prompt.
            pending.clear();
            continue;
        }
        if (status == ReadLineStatus::Eof) {
            fmt::print("\n");
            if (!pending.empty()) {
                submit_buffer(pending);  // surface the error in the unterminated buffer
            }
            break;
        }

        // Mid statement: keep accumulating continuation lines (including blank
        // ones) until the open delimiters balance, then execute. Colon commands
        // are recognised only at a statement boundary, so they pass through here.
        if (!pending.empty()) {
            pending.push_back('\n');
            pending += line;
            if (delimiter_depth(pending) > 0) {
                continue;
            }
            submit_buffer(pending);
            pending.clear();
            continue;
        }

        if (line.empty()) {
            continue;
        }

        std::string_view line_view(line);

        if (starts_with_command(line_view, ":help")) {
            auto arg = trim(line_view.substr(std::string_view(":help").size()));
            if (arg.empty()) {
                print_help();
            } else {
                print_doc(arg, tables, lazy_tables, scalars, columns, models, functions,
                          extern_decls, declaration_docs, imports);
            }
            continue;
        }
        if (line_view.size() > 1 && line_view.front() == '?') {
            auto arg = trim(line_view.substr(1));
            print_doc(arg, tables, lazy_tables, scalars, columns, models, functions, extern_decls,
                      declaration_docs, imports);
            continue;
        }
        if (starts_with_command(line_view, ":functions")) {
            auto arg = trim(line_view.substr(std::string_view(":functions").size()));
            if (!arg.empty()) {
                fmt::print("usage: :functions\n");
                continue;
            }
            print_functions(functions, extern_decls);
            continue;
        }
        if (starts_with_command(line_view, ":imports")) {
            auto arg = trim(line_view.substr(std::string_view(":imports").size()));
            if (!arg.empty()) {
                fmt::print("usage: :imports\n");
                continue;
            }
            print_imports(imports, extern_decls);
            continue;
        }
        if (starts_with_command(line_view, ":doc")) {
            auto arg = trim(line_view.substr(std::string_view(":doc").size()));
            print_doc(arg, tables, lazy_tables, scalars, columns, models, functions, extern_decls,
                      declaration_docs, imports);
            continue;
        }
        if (starts_with_command(line_view, ":source")) {
            auto arg = trim(line_view.substr(std::string_view(":source").size()));
            print_source(arg, functions, function_sources);
            continue;
        }
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

        if (starts_with_command(line_view, ":q") || starts_with_command(line_view, ":quit") ||
            starts_with_command(line_view, ":exit")) {
            break;
        }
        if (starts_with_command(line_view, ":tables")) {
            auto arg = trim(line_view.substr(std::string_view(":tables").size()));
            if (!arg.empty()) {
                fmt::print("usage: :tables\n");
                continue;
            }
            print_tables(tables, lazy_tables);
            continue;
        }
        if (starts_with_command(line_view, ":scalars")) {
            auto arg = trim(line_view.substr(std::string_view(":scalars").size()));
            if (!arg.empty()) {
                fmt::print("usage: :scalars\n");
                continue;
            }
            print_scalars(scalars);
            continue;
        }
        if (line_view.starts_with(":schema")) {
            auto arg = trim(line_view.substr(std::string_view(":schema").size()));
            if (arg.empty()) {
                fmt::print("usage: :schema <table>\n");
                continue;
            }
            if (auto lazy = lazy_tables.find(std::string(arg)); lazy != lazy_tables.end()) {
                // Known from the source's metadata — no column is decoded to answer this.
                print_schema(lazy->second->schema());
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
            auto table = resolve_table(std::string(name), tables, lazy_tables);
            if (!table) {
                fmt::print("error: {}\n", table.error());
                continue;
            }
            const std::size_t count = parse_optional_size(count_text, 10);
            print_table(table.value(), count);
            continue;
        }
        // Accept the obvious typo `:peak` as an alias for `:peek`.
        const bool is_peek = line_view.starts_with(":peek") &&
                             (line_view.size() == 5 || line_view[5] == ' ' || line_view[5] == '\t');
        const bool is_peak = line_view.starts_with(":peak") &&
                             (line_view.size() == 5 || line_view[5] == ' ' || line_view[5] == '\t');
        if (is_peek || is_peak) {
            auto rest = std::string(trim(line_view.substr(5)));
            if (rest.empty()) {
                fmt::print("usage: :peek <expr>\n");
                continue;
            }
            if (tables.contains(rest) || lazy_tables.contains(rest)) {
                auto table = resolve_table(rest, tables, lazy_tables);
                if (!table) {
                    fmt::print("error: {}\n", table.error());
                    continue;
                }
                peek_table(table.value());
                continue;
            }
            auto normalized = normalize_input(rest);
            auto parsed = parser::parse(normalized);
            if (!parsed) {
                fmt::print("error: {}\n", parsed.error().format());
                continue;
            }
            if (parsed->statements.size() != 1 ||
                !std::holds_alternative<parser::ExprStmt>(parsed->statements.front())) {
                fmt::print("error: :peek expects a single expression\n");
                continue;
            }
            auto& expr_stmt = std::get<parser::ExprStmt>(parsed->statements.front());
            auto value =
                eval_expr_value(*expr_stmt.expr, tables, lazy_tables, scalars, columns, models,
                                functions, compile_time_lists, extern_decls, registry);
            if (!value) {
                fmt::print("error: {}\n", value.error());
                continue;
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                fmt::print("{}\n", format_scalar(*scalar));
            } else if (auto* col = std::get_if<runtime::ColumnValue>(&value.value())) {
                runtime::Table temp;
                temp.add_column("column", *col);
                peek_table(temp);
            } else {
                peek_table(std::get<runtime::Table>(std::move(value.value())));
            }
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
            auto table = resolve_table(std::string(name), tables, lazy_tables);
            if (!table) {
                fmt::print("error: {}\n", table.error());
                continue;
            }
            const std::size_t count = parse_optional_size(count_text, 10);
            describe_table(table.value(), count);
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
            const std::string source((std::istreambuf_iterator<char>(input)),
                                     std::istreambuf_iterator<char>());
            auto parsed = parser::parse(source);
            if (!parsed) {
                fmt::print("error: {}\n", parsed.error().format());
                report_timing();
                continue;
            }
            auto comments = collect_script_comment_lines(source);
            auto doc_comment_groups = build_statement_comment_groups(parsed->statements, comments);
            std::vector<std::vector<std::string>> print_comment_groups;
            const std::vector<std::vector<std::string>>* print_comment_groups_ptr = nullptr;
            if (load_comments_enabled) {
                print_comment_groups = doc_comment_groups;
                print_comment_groups_ptr = &print_comment_groups;
            }
            if (!execute_statements(
                    parsed->statements, tables, lazy_tables, scalars, columns, models, functions,
                    compile_time_lists, extern_decls, registry, config.plugin_search_paths,
                    loaded_plugins, config.import_search_paths, print_comment_groups_ptr,
                    &doc_comment_groups, &function_sources, &declaration_docs, &imports, source)) {
                report_timing();
                continue;
            }
            report_timing();
            continue;
        }

        // Catch typo'd colon commands so the user gets a clear error instead
        // of falling through to the parser — `:peak trades` would otherwise
        // surface as `error: 1:1: expected expression`.
        if (line_view.starts_with(':')) {
            auto cmd_end = line_view.find_first_of(" \t");
            auto cmd = cmd_end == std::string_view::npos ? line_view : line_view.substr(0, cmd_end);
            fmt::print("error: unknown REPL command '{}'\n", cmd);
            fmt::print(
                "known: :help, :tables, :scalars, :functions, :imports, :schema, :head, "
                ":peek, :describe, :doc, :source, :load, :timing, :time, :comments, :quit\n");
            continue;
        }

        // Start of a statement. If its delimiters don't balance on this line,
        // hold it and read continuation lines before parsing.
        pending = line;
        if (delimiter_depth(pending) > 0) {
            continue;
        }
        pending.clear();
        submit_buffer(line);
        report_timing();
    }

    save_history_file(history_path, config.history_limit);

    spdlog::info("Ibex REPL exiting");
}

}  // namespace ibex::repl
