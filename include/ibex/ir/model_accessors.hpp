// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <string_view>

namespace ibex::ir {

/// The accessors of a fitted model that give a table (`coef(m)`, `model_summary(m)`,
/// ...), under either name.
[[nodiscard]] constexpr auto is_model_table_accessor(std::string_view callee) -> bool {
    return callee == "coef" || callee == "model_coef" || callee == "summary" ||
           callee == "model_summary" || callee == "fitted" || callee == "model_fitted" ||
           callee == "residuals" || callee == "model_residuals" || callee == "importance" ||
           callee == "model_importance";
}

/// The accessor that gives a scalar (`r_squared(m)`), under either name.
[[nodiscard]] constexpr auto is_model_scalar_accessor(std::string_view callee) -> bool {
    return callee == "r_squared" || callee == "model_r_squared";
}

}  // namespace ibex::ir
