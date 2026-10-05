// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::repl {

/// Configuration for the REPL session.
struct ReplConfig {
    bool verbose = false;
    /// Print `planner: whole-script` / `planner: statements (<reason>)` to
    /// stderr per script. Deliberately NOT folded into `verbose`, which also
    /// enables REPL diagnostic logging: the benchmark harness reads this line
    /// to record which engine path it measured, and must not pay for logging
    /// someone later adds to a hot path.
    bool report_planner = false;
    std::string prompt = "ibex> ";
    bool persistent_history = true;
    /// History file used when readline is available. Empty means
    /// `$IBEX_HISTORY_FILE`, then `$HOME/.ibex_history` where possible.
    std::string history_path;
    /// Maximum number of entries kept in the persistent history file.
    std::size_t history_limit = 10000;
    /// Directories searched (in order) for plugin shared libraries (*.so).
    /// When a script declares `extern fn foo(...) from "bar.hpp"`, the REPL
    /// looks for `bar.so` in each of these directories and loads it via dlopen.
    std::vector<std::string> plugin_search_paths;
    /// Directories searched (in order) for library stub files (<name>.ibex).
    /// Used by `import "name";` declarations.  When empty, the plugin_search_paths
    /// are used as a fallback so that plugins and their accompanying .ibex stubs
    /// can live in the same directory.
    std::vector<std::string> import_search_paths;
};

/// A structured value produced by evaluating an expression in a REPL session.
/// Terminal callers continue to use the existing formatter; programmatic
/// callers (such as the local UI server) can render tables themselves.
struct ExecutionResult {
    bool ok = false;
    std::optional<runtime::Table> table;
    /// Every table rendered by this execution, in statement order. `table`
    /// remains the last table for compatibility with existing callers.
    std::vector<runtime::Table> tables;
    std::optional<runtime::ScalarValue> scalar;
    /// Everything the execution wrote to stdout: `print(...)` output, warnings,
    /// and (when rendering) the formatted results -- but not the error, which
    /// is in `error`.
    std::string output;
    std::string error;
    std::optional<std::size_t> error_line;
    std::optional<std::size_t> error_column;
};

struct EnvironmentTable {
    std::string name;
    std::vector<std::pair<std::string, std::string>> columns;
    std::size_t rows = 0;
    bool lazy = false;
};

/// Stateful programmatic facade over the same evaluator used by an interactive
/// REPL. Each instance owns its bindings, while the supplied extern registry
/// and configuration retain the normal plugin/import behavior.
class ReplSession {
   public:
    ReplSession(const ReplConfig& config, runtime::ExternRegistry& registry);
    ~ReplSession();
    ReplSession(ReplSession&&) noexcept;
    auto operator=(ReplSession&&) noexcept -> ReplSession&;
    ReplSession(const ReplSession&) = delete;
    auto operator=(const ReplSession&) -> ReplSession& = delete;

    /// How one `execute` call runs.
    struct ExecuteOptions {
        /// Tables and scalars visible to this call only: bound under their
        /// names for its duration, shadowing any session binding of the same
        /// name, and removed again afterwards -- unless the call itself rebinds
        /// the name, in which case its binding stays.
        runtime::TableRegistry tables;
        runtime::ScalarRegistry scalars;
        /// Format each value to text as the terminal REPL does. A caller that
        /// reads `ExecutionResult::table` / `scalar` itself can skip it.
        bool render = true;
    };

    [[nodiscard]] auto execute(std::string_view source) -> ExecutionResult;
    [[nodiscard]] auto execute(std::string_view source, ExecuteOptions options) -> ExecutionResult;
    [[nodiscard]] auto environment() const -> std::vector<EnvironmentTable>;
    [[nodiscard]] auto erase(std::string_view name) -> bool;

    /// The table bound to `name`, decoding a lazy binding in full.
    [[nodiscard]] auto table_binding(std::string_view name)
        -> std::expected<runtime::Table, std::string>;

    /// The schema of one expression, inferred by lowering it against the
    /// session's bindings without running it. Nullopt when it does not lower
    /// or the schema is not fully known. `extra_names` are names the caller
    /// will bind for the call that runs it (they are not columns).
    [[nodiscard]] auto infer_schema(std::string_view expression,
                                    const std::vector<std::string>& extra_names) const
        -> std::optional<ir::SchemaInfo>;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

/// Run the interactive REPL loop.
///
/// Reads lines from stdin, parses and evaluates them.
void run(const ReplConfig& config, runtime::ExternRegistry& registry);

/// Execute a script in a fresh REPL context (useful for tests).
[[nodiscard]] auto execute_script(std::string_view source, runtime::ExternRegistry& registry)
    -> bool;

/// Execute a script with the same plugin / import search paths the
/// interactive REPL would use. Lets non-interactive callers (`ibex_eval`)
/// run scripts that declare `extern fn ... from "csv.hpp"` etc., and use
/// the REPL's full vocabulary including model accessors.
[[nodiscard]] auto execute_script(std::string_view source, runtime::ExternRegistry& registry,
                                  const ReplConfig& config) -> bool;

/// Execute an Ibex script file by path. The whole file is parsed at once, so
/// statements may span multiple physical lines (e.g. a multi-line `model { ... }`
/// clause). Honors the config's plugin / import search paths. Returns false if
/// the file cannot be read or a statement fails.
[[nodiscard]] auto run_file(const std::string& path, const ReplConfig& config,
                            runtime::ExternRegistry& registry) -> bool;

/// Normalize a single REPL input line (e.g., inject implicit semicolon).
[[nodiscard]] auto normalize_input(std::string_view input) -> std::string;

}  // namespace ibex::repl
