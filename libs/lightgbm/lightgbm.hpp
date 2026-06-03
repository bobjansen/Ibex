#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <string>

namespace ibex::lightgbm {

// Trains a real LightGBM gradient-boosted tree model on the design matrix
// (every Float64 column except `response_col`, which is the label) and returns
// a long-format wire table consumed by the interpreter's predictive-model path:
//
//   kind : String   -- "fitted" or "importance"
//   term : String   -- feature name (importance rows); "" for fitted rows
//   value: Float64  -- prediction (fitted rows) / gain importance (importance rows)
//
// There are `n` rows with kind="fitted" (in-sample predictions, input order)
// followed by `p` rows with kind="importance" (one per feature).
auto fit(const runtime::Table& design_matrix, const std::string& response_col,
         std::int64_t iterations, double learning_rate)
    -> std::expected<runtime::Table, std::string>;

}  // namespace ibex::lightgbm
