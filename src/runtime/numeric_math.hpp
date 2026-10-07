// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace ibex::runtime {

/// The row-wise double -> double math builtins, named by an enum rather than a
/// function pointer so a column kernel can pick its loop once per block instead
/// of making an indirect call per row (which keeps even `abs` scalar).
enum class UnaryMath : std::uint8_t {
    Sqrt,
    Log,
    Exp,
    Log2,
    Log10,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Abs,
    Floor,
    Ceil,
    Trunc,
};

[[nodiscard]] auto lookup_unary_math(std::string_view name) -> std::optional<UnaryMath>;

/// abs/floor/ceil/trunc keep an Int argument Int; the rest widen to Double.
[[nodiscard]] auto unary_math_is_type_preserving(UnaryMath fn) -> bool;

/// One value, through the same kernel `apply_unary_math` uses for a column, so
/// a scalar operand and a column row with the same value agree bit for bit.
[[nodiscard]] auto apply_unary_math(UnaryMath fn, double value) -> double;

/// dst[i] = fn(src[i]) for i < n; `src == dst` is allowed. On an AVX2 build
/// with libmvec the transcendentals run 4-wide, and the n % 4 tail goes through
/// the same vector kernel, so a row's result never depends on where a range
/// boundary fell (and so never on the worker count).
void apply_unary_math(UnaryMath fn, const double* src, double* dst, std::size_t n);

}  // namespace ibex::runtime
