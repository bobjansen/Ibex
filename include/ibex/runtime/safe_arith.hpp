#pragma once

#include <limits>
#include <type_traits>

namespace ibex::runtime {

// Integer division / modulo guarded against the two cases that are undefined
// behaviour in C++ (and trap with SIGFPE on x86): division by zero, and the
// INT_MIN / -1 overflow that a bare `b == 0` check misses. Ibex's defined result
// is `x / 0 == 0`, `x % 0 == 0`, `INT_MIN / -1 == INT_MIN` (the two's-complement
// wrap), and `INT_MIN % -1 == 0`.
//
// Single source of truth for guarded integer division: the vectorized
// (`arith_into`), per-row (`eval_expr`), scalar-broadcast (`apply_int_op`), and
// REPL scalar evaluators all route through here so they cannot disagree. Float
// division is total (IEEE inf/nan, no trap) and is never routed through these.
template <typename T>
constexpr auto safe_idiv(T a, T b) noexcept -> T {
    static_assert(std::is_integral_v<T>, "safe_idiv is for integer types only");
    if (b == 0) {
        return T{0};
    }
    if constexpr (std::is_signed_v<T>) {
        if (b == T{-1} && a == std::numeric_limits<T>::min()) {
            return std::numeric_limits<T>::min();
        }
    }
    return a / b;
}

template <typename T>
constexpr auto safe_imod(T a, T b) noexcept -> T {
    static_assert(std::is_integral_v<T>, "safe_imod is for integer types only");
    if (b == 0) {
        return T{0};
    }
    if constexpr (std::is_signed_v<T>) {
        if (b == T{-1} && a == std::numeric_limits<T>::min()) {
            return T{0};
        }
    }
    return a % b;
}

}  // namespace ibex::runtime
