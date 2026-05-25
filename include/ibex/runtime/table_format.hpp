#pragma once

// Canonical table/cell formatting shared by the interpreter REPL (and the
// `ibex_eval` batch path that wraps it) and the transpiled `ops::print` path.
// Both must render identically — the `ibex_parity_interpreter_vs_transpiled`
// test diffs their stdout byte-for-byte — so the formatter lives here, in the
// runtime layer, rather than being duplicated per consumer.

#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>

namespace ibex::runtime {

[[nodiscard]] auto format_date(Date date) -> std::string;
[[nodiscard]] auto format_timestamp(Timestamp ts) -> std::string;
[[nodiscard]] auto format_float_mixed(double value) -> std::string;
[[nodiscard]] auto quote_and_escape(std::string_view text) -> std::string;
[[nodiscard]] auto format_cell(const ColumnEntry& entry, std::size_t row) -> std::string;

// Render `table` as a boxed table: a `rows: N` header, then column headers and
// data framed with `+`/`-`/`|`. Shows at most `max_rows` rows, followed by a
// "... (N more rows)" line when truncated. Empty tables print "<empty>".
void format_table(const Table& table, std::ostream& out, std::size_t max_rows = 10);

}  // namespace ibex::runtime
