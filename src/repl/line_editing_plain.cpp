// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The REPL loop's line reader without a terminal library: std::getline, no
// history or completion. Linked by everything that embeds the evaluator; the
// interactive `ibex` binary links the readline editor instead. See
// line_editing.hpp.

#include <ibex/format.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/interrupt.hpp>

#include <cstddef>
#include <iostream>
#include <string>

#include "line_editing.hpp"

namespace ibex::repl::line_editing {

void configure_line_editing() {}

void set_completion_context([[maybe_unused]] CompletionContext context) {}

auto read_repl_line(const std::string& prompt, std::string& out) -> ReadLineStatus {
    runtime::clear_interrupt();
    ibex::formatting::print("{}", prompt);
    if (std::getline(std::cin, out)) {
        return ReadLineStatus::Line;
    }
    if (runtime::consume_interrupt()) {
        // The read was EINTR'd by Ctrl+C: clear the stream error and hand
        // control back to the loop instead of treating it as EOF.
        std::cin.clear();
        ibex::formatting::print("^C\n");
        return ReadLineStatus::Interrupted;
    }
    return ReadLineStatus::Eof;
}

auto resolve_history_path(const ReplConfig& /*config*/) -> std::string {
    return {};
}

void load_history_file(const std::string& /*path*/, std::size_t /*limit*/) {}

void save_history_file(const std::string& /*path*/, std::size_t /*limit*/) {}

}  // namespace ibex::repl::line_editing
