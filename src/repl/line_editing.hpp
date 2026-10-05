// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Line editing for the interactive REPL loop (`repl::run`), split from the
// evaluator so the evaluator carries no terminal dependency. Two
// implementations: `line_editing_readline.cpp` (GNU readline / libedit:
// history, completion, Ctrl+C at the prompt) in `ibex_repl_cli`, and
// `line_editing_plain.cpp` (std::getline) in `ibex_repl`, which everything
// embedding the evaluator links -- the R package among them, where a second
// readline in R's own process would be a liability.

#pragma once

#include <ibex/parser/ast.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <cstdint>
#include <robin_hood.h>
#include <string>
#include <vector>

namespace ibex::repl {

using FunctionRegistry = robin_hood::unordered_map<std::string, parser::FunctionDecl>;
using ExternDeclRegistry = robin_hood::unordered_map<std::string, parser::ExternDecl>;
using ColumnRegistry = robin_hood::unordered_map<std::string, runtime::ColumnValue>;
using ModelRegistry = robin_hood::unordered_map<std::string, runtime::ModelResult>;
using CompileTimeListRegistry = robin_hood::unordered_map<std::string, std::vector<std::string>>;
using ImportRegistry = robin_hood::unordered_set<std::string>;

namespace line_editing {

/// What completion may offer: the session's current bindings.
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

void configure_line_editing();
void set_completion_context(CompletionContext context);
auto read_repl_line(const std::string& prompt, std::string& out) -> ReadLineStatus;
auto resolve_history_path(const ReplConfig& config) -> std::string;
void load_history_file(const std::string& path, std::size_t limit);
void save_history_file(const std::string& path, std::size_t limit);

/// Whether the REPL runs with `verbose`: history I/O failures are logged then.
[[nodiscard]] auto verbose() -> bool;

}  // namespace line_editing

}  // namespace ibex::repl
