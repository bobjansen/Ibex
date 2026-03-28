#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <string>

namespace ibex::lightbm {

auto fit(const runtime::Table& design_matrix, const std::string& response_col,
         std::int64_t iterations, double learning_rate)
    -> std::expected<runtime::Table, std::string>;

}  // namespace ibex::lightbm
