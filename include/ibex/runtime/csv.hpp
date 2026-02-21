#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <string>
#include <string_view>

namespace ibex::runtime {

/// Simple CSV reader (comma-separated, no quotes/escapes).
[[nodiscard]] auto read_csv_simple(std::string_view path) -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
