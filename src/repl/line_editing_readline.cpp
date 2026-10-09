// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The interactive REPL's line editor on GNU readline (or libedit): history,
// tab completion over the session's bindings, and Ctrl+C at the prompt.
// See line_editing.hpp.

#include <ibex/format.hpp>
#include <ibex/parser/names.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <readline/history.h>
#include <readline/readline.h>
#include <string.h>  // NOLINT(modernize-deprecated-headers): strdup is POSIX, not <cstring>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

#include "line_editing.hpp"

namespace ibex::repl::line_editing {

namespace {

template <typename... Args>
void log_failure(ibex::formatting::format_string<Args...> format, Args&&... args) {
    if (verbose()) {
        std::clog << ibex::formatting::format(format, std::forward<Args>(args)...) << '\n';
    }
}

constexpr auto kColonCommands = std::to_array<std::string_view>({
    ":q",       ":quit",     ":exit", ":help",   ":tables",   ":scalars", ":functions",
    ":imports", ":schema",   ":head", ":peek",   ":describe", ":load",    ":timing",
    ":time",    ":comments", ":doc",  ":source", ":run",      ":explain",
});

constexpr auto kCompletionBuiltins = std::to_array<std::string_view>({
    "Bankers",
    "Bool",
    "Ceil",
    "Date",
    "Float32",
    "Float64",
    "Floor",
    "Int",
    "Int32",
    "Int64",
    "Nearest",
    "RoundMode",
    "String",
    "Timestamp",
    "Trunc",
    "abs",
    "as_timeframe",
    "ceil",
    "coef",
    "columns",
    "count",
    "cumprod",
    "cumsum",
    "ewma",
    "exists",
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
    "like",
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
});

constexpr auto kMoreCompletionBuiltins = std::to_array<std::string_view>({
    "rolling_mean", "rolling_median",
    "rolling_min",  "rolling_quantile",
    "rolling_skew", "rolling_std",
    "rolling_sum",  "round",
    "scalar",       "seed_rng",
    "seq",          "skew",
    "sqrt",         "std",
    "sum",          "summary",
    "trunc",        "filter",
    "select",       "update",
    "by",           "window",
    "order",        "rename",
    "distinct",     "head",
    "tail",         "top",
    "melt",         "dcast",
    "join",         "on",
    "sin",          "cos",
    "tan",          "asin",
    "acos",         "atan",
    "sinh",         "cosh",
    "tanh",         "log2",
    "log10",
});

CompletionContext g_completion_context;
std::vector<std::string> g_completion_candidates;

void set_completion_context_impl(CompletionContext context) {
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
        // `adbc::query` also offers `adbc::`, so the namespace completes on
        // its own before a name in it is chosen.
        if (const auto ns = parser::namespace_of(entry.first); !ns.empty()) {
            candidates.push_back(std::string(ns) + "::");
        }
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
    std::ranges::sort(candidates);
    candidates.erase(std::ranges::unique(candidates).begin(), candidates.end());
    return candidates;
}

auto any_prefix_match(const std::vector<std::string>& candidates, std::string_view prefix) -> bool {
    if (prefix.empty()) {
        return false;
    }
    return std::ranges::any_of(
        candidates, [&](const auto& candidate) { return candidate.starts_with(prefix); });
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

auto resolve_history_path_impl(const ReplConfig& config) -> std::string {
    if (!config.persistent_history) {
        return {};
    }
    if (!config.history_path.empty()) {
        return config.history_path;
    }
    return default_history_path();
}

void load_history_file_impl(const std::string& path, std::size_t limit) {
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
        log_failure("failed to read REPL history '{}': {}", path, std::strerror(rc));
    }
}

void save_history_file_impl(const std::string& path, std::size_t limit) {
    if (path.empty()) {
        return;
    }
    std::error_code ec;
    if (auto parent = std::filesystem::path(path).parent_path(); !parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            log_failure("failed to create REPL history directory '{}': {}", parent.string(),
                        ec.message());
            return;
        }
    }
    const int write_rc = ::write_history(path.c_str());
    if (write_rc != 0) {
        log_failure("failed to write REPL history '{}': {}", path, std::strerror(write_rc));
        return;
    }
    if (limit > 0) {
        const int truncate_rc = ::history_truncate_file(
            path.c_str(), static_cast<int>(std::min<std::size_t>(
                              limit, static_cast<std::size_t>(std::numeric_limits<int>::max()))));
        if (truncate_rc != 0) {
            log_failure("failed to truncate REPL history '{}': {}", path,
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

    if (command_matches(line, ":load") || command_matches(line, ":run")) {
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

#if defined(RL_READLINE_VERSION) && RL_READLINE_VERSION >= 0x0500
/// Polled by GNU readline roughly 10x/second while it waits for input (and
/// right after a signal EINTRs the wait). On Ctrl+C, discard the pending line
/// and set rl_done, which makes rl_read_key() hand back a synthetic newline so
/// readline() returns the (emptied) line to the caller. rl_done is only
/// honored on the rl_event_hook path — rl_signal_event_hook does not unblock
/// rl_getc's internal read loop.
///
/// macOS's libedit compatibility headers report Readline 4.2 but do not expose
/// this hook API; there Ctrl+C surfaces as a null return from readline().
auto interrupt_event_hook() -> int {
    if (runtime::interrupt_requested()) {
        rl_replace_line("", 0);
        rl_done = 1;
    }
    return 0;
}
#endif

void configure_line_editing_impl() {
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
    // libedit's macOS compatibility API lacks rl_event_hook (and the related
    // rl_replace_line / rl_done helpers used by the hook). It reports 4.2, so
    // use its null-on-EINTR behavior instead; read_repl_line handles that.
#if defined(RL_READLINE_VERSION) && RL_READLINE_VERSION >= 0x0500
    if (::isatty(STDIN_FILENO) != 0) {
        rl_event_hook = interrupt_event_hook;
    }
#endif

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

auto read_repl_line_impl(const std::string& prompt, std::string& out) -> ReadLineStatus {
    // Drop any Ctrl+C that landed between the end of the previous evaluation
    // and this prompt — it has already done its job (or arrived too late).
    runtime::clear_interrupt();
    const std::unique_ptr<char, decltype(&std::free)> raw{::readline(prompt.c_str()), &std::free};
    if (runtime::consume_interrupt()) {
        // Covers both the event-hook path (emptied line) and readline
        // variants that surface the EINTR as a null return.
        ibex::formatting::print("^C\n");
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

}  // namespace

void configure_line_editing() {
    configure_line_editing_impl();
}

void set_completion_context(CompletionContext context) {
    set_completion_context_impl(context);
}

auto read_repl_line(const std::string& prompt, std::string& out) -> ReadLineStatus {
    return read_repl_line_impl(prompt, out);
}

auto resolve_history_path(const ReplConfig& config) -> std::string {
    return resolve_history_path_impl(config);
}

void load_history_file(const std::string& path, std::size_t limit) {
    load_history_file_impl(path, limit);
}

void save_history_file(const std::string& path, std::size_t limit) {
    save_history_file_impl(path, limit);
}

}  // namespace ibex::repl::line_editing
