#pragma once

// The `min` / `max` / `sql_max` rule for Float64 values, shared by every
// kernel that folds them so that no path disagrees about NaN.
//
// `min` and `max` skip NaN: a number replaces a NaN accumulator and a NaN never
// replaces a number, so the result is NaN only when every present value is
// NaN. That is Polars' rule, and for `min` also SQL's (NaN ranks above every
// number). `sql_max` ranks NaN above every number too, so any NaN wins.
//
// Each fold is order-independent, so partial results merge with the same
// function. An accumulator may start at NaN to mean "no number seen yet".

#include <algorithm>
#include <type_traits>

namespace ibex::runtime {

[[nodiscard]] constexpr auto float_min(double acc, double value) noexcept -> double {
    // NOLINTNEXTLINE(misc-redundant-expression): acc != acc tests for NaN.
    return (value < acc || acc != acc) ? value : acc;
}

[[nodiscard]] constexpr auto float_max(double acc, double value) noexcept -> double {
    // NOLINTNEXTLINE(misc-redundant-expression): acc != acc tests for NaN.
    return (value > acc || acc != acc) ? value : acc;
}

[[nodiscard]] constexpr auto float_sql_max(double acc, double value) noexcept -> double {
    // NOLINTNEXTLINE(misc-redundant-expression): value != value tests for NaN.
    return (value > acc || value != value) ? value : acc;
}

/// `min` / `max` over a numeric type: the NaN rule above for floating point,
/// plain comparison otherwise.
template <typename T>
[[nodiscard]] constexpr auto fold_min(T acc, T value) noexcept -> T {
    if constexpr (std::is_floating_point_v<T>) {
        return float_min(acc, value);
    } else {
        return std::min(acc, value);
    }
}

template <typename T>
[[nodiscard]] constexpr auto fold_max(T acc, T value) noexcept -> T {
    if constexpr (std::is_floating_point_v<T>) {
        return float_max(acc, value);
    } else {
        return std::max(acc, value);
    }
}

}  // namespace ibex::runtime
