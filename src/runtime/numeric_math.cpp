// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "numeric_math.hpp"

#include <array>
#include <cmath>
#include <cstddef>
#include <optional>
#include <string_view>
#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace ibex::runtime {

namespace {

constexpr std::array<std::pair<std::string_view, UnaryMath>, 18> kUnaryMathNames = {{
    {"sqrt", UnaryMath::Sqrt},
    {"log", UnaryMath::Log},
    {"exp", UnaryMath::Exp},
    {"log2", UnaryMath::Log2},
    {"log10", UnaryMath::Log10},
    {"sin", UnaryMath::Sin},
    {"cos", UnaryMath::Cos},
    {"tan", UnaryMath::Tan},
    {"asin", UnaryMath::Asin},
    {"acos", UnaryMath::Acos},
    {"atan", UnaryMath::Atan},
    {"sinh", UnaryMath::Sinh},
    {"cosh", UnaryMath::Cosh},
    {"tanh", UnaryMath::Tanh},
    {"abs", UnaryMath::Abs},
    {"floor", UnaryMath::Floor},
    {"ceil", UnaryMath::Ceil},
    {"trunc", UnaryMath::Trunc},
}};

// The scalar libm function: the whole kernel on a build without libmvec, and
// the exact kernels (sqrt is correctly rounded; abs/floor/ceil/trunc are exact)
// everywhere.
auto scalar_unary_math(UnaryMath fn, double x) -> double {
    switch (fn) {
        case UnaryMath::Sqrt:
            return std::sqrt(x);
        case UnaryMath::Log:
            return std::log(x);
        case UnaryMath::Exp:
            return std::exp(x);
        case UnaryMath::Log2:
            return std::log2(x);
        case UnaryMath::Log10:
            return std::log10(x);
        case UnaryMath::Sin:
            return std::sin(x);
        case UnaryMath::Cos:
            return std::cos(x);
        case UnaryMath::Tan:
            return std::tan(x);
        case UnaryMath::Asin:
            return std::asin(x);
        case UnaryMath::Acos:
            return std::acos(x);
        case UnaryMath::Atan:
            return std::atan(x);
        case UnaryMath::Sinh:
            return std::sinh(x);
        case UnaryMath::Cosh:
            return std::cosh(x);
        case UnaryMath::Tanh:
            return std::tanh(x);
        case UnaryMath::Abs:
            return std::fabs(x);
        case UnaryMath::Floor:
            return std::floor(x);
        case UnaryMath::Ceil:
            return std::ceil(x);
        case UnaryMath::Trunc:
            return std::trunc(x);
    }
    return x;  // exhaustive switch; keeps strict compilers aware.
}

// One loop per kernel, chosen outside it: a lambda the compiler can inline,
// so floor/ceil/trunc become vroundpd and abs a mask.
template <typename Kernel>
void map_rows(const double* src, double* dst, std::size_t n, Kernel kernel) {
    for (std::size_t i = 0; i < n; ++i) {
        dst[i] = kernel(src[i]);
    }
}

}  // namespace

// Vectorised transcendentals via libmvec, the same mechanism zorro uses for the
// RNG normal/exponential paths: 4-wide AVX2, ~5-10x scalar libm.
#if defined(__AVX2__) && defined(ZORRO_USE_LIBMVEC)
// glibc AVX2 packed-double symbols (one arg).
// NOLINTBEGIN(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp) — these
// are glibc's fixed vector-ABI symbol names; the spelling is not ours to choose.
extern "C" {
__m256d _ZGVdN4v_log(__m256d) noexcept;
__m256d _ZGVdN4v_log2(__m256d) noexcept;
__m256d _ZGVdN4v_log10(__m256d) noexcept;
__m256d _ZGVdN4v_exp(__m256d) noexcept;
__m256d _ZGVdN4v_sin(__m256d) noexcept;
__m256d _ZGVdN4v_cos(__m256d) noexcept;
__m256d _ZGVdN4v_tan(__m256d) noexcept;
__m256d _ZGVdN4v_asin(__m256d) noexcept;
__m256d _ZGVdN4v_acos(__m256d) noexcept;
__m256d _ZGVdN4v_atan(__m256d) noexcept;
__m256d _ZGVdN4v_sinh(__m256d) noexcept;
__m256d _ZGVdN4v_cosh(__m256d) noexcept;
__m256d _ZGVdN4v_tanh(__m256d) noexcept;
}
// NOLINTEND(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)

namespace {

using VectorKernel = __m256d (*)(__m256d) noexcept;

auto vector_kernel(UnaryMath fn) -> VectorKernel {
    switch (fn) {
        case UnaryMath::Log:
            return _ZGVdN4v_log;
        case UnaryMath::Exp:
            return _ZGVdN4v_exp;
        case UnaryMath::Log2:
            return _ZGVdN4v_log2;
        case UnaryMath::Log10:
            return _ZGVdN4v_log10;
        case UnaryMath::Sin:
            return _ZGVdN4v_sin;
        case UnaryMath::Cos:
            return _ZGVdN4v_cos;
        case UnaryMath::Tan:
            return _ZGVdN4v_tan;
        case UnaryMath::Asin:
            return _ZGVdN4v_asin;
        case UnaryMath::Acos:
            return _ZGVdN4v_acos;
        case UnaryMath::Atan:
            return _ZGVdN4v_atan;
        case UnaryMath::Sinh:
            return _ZGVdN4v_sinh;
        case UnaryMath::Cosh:
            return _ZGVdN4v_cosh;
        case UnaryMath::Tanh:
            return _ZGVdN4v_tanh;
        default:
            return nullptr;
    }
}

// libmvec is not bit-identical to scalar libm, so the tail is padded into one
// more vector call rather than finished with the scalar function: every row
// gets the vector kernel's answer wherever it sits in a range.
void apply_vector_kernel(VectorKernel kernel, const double* src, double* dst, std::size_t n) {
    std::size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        _mm256_storeu_pd(dst + i, kernel(_mm256_loadu_pd(src + i)));
    }
    if (i < n) {
        alignas(32) std::array<double, 4> lanes{};
        for (std::size_t j = i; j < n; ++j) {
            lanes[j - i] = src[j];
        }
        _mm256_store_pd(lanes.data(), kernel(_mm256_load_pd(lanes.data())));
        for (std::size_t j = i; j < n; ++j) {
            dst[j] = lanes[j - i];
        }
    }
}

}  // namespace

auto apply_unary_math(UnaryMath fn, double value) -> double {
    if (const VectorKernel kernel = vector_kernel(fn); kernel != nullptr) {
        double out = 0.0;
        apply_vector_kernel(kernel, &value, &out, 1);
        return out;
    }
    return scalar_unary_math(fn, value);
}
#else
namespace {

using VectorKernel = void*;

auto vector_kernel(UnaryMath /*fn*/) -> VectorKernel {
    return nullptr;
}

void apply_vector_kernel(VectorKernel /*kernel*/, const double* /*src*/, double* /*dst*/,
                         std::size_t /*n*/) {}

}  // namespace

auto apply_unary_math(UnaryMath fn, double value) -> double {
    return scalar_unary_math(fn, value);
}
#endif

auto lookup_unary_math(std::string_view name) -> std::optional<UnaryMath> {
    for (const auto& [spelling, fn] : kUnaryMathNames) {
        if (spelling == name) {
            return fn;
        }
    }
    return std::nullopt;
}

auto unary_math_is_type_preserving(UnaryMath fn) -> bool {
    return fn == UnaryMath::Abs || fn == UnaryMath::Floor || fn == UnaryMath::Ceil ||
           fn == UnaryMath::Trunc;
}

void apply_unary_math(UnaryMath fn, const double* src, double* dst, std::size_t n) {
    switch (fn) {
        case UnaryMath::Abs:
            map_rows(src, dst, n, [](double x) { return std::fabs(x); });
            return;
        case UnaryMath::Floor:
            map_rows(src, dst, n, [](double x) { return std::floor(x); });
            return;
        case UnaryMath::Ceil:
            map_rows(src, dst, n, [](double x) { return std::ceil(x); });
            return;
        case UnaryMath::Trunc:
            map_rows(src, dst, n, [](double x) { return std::trunc(x); });
            return;
        case UnaryMath::Sqrt: {
            // std::sqrt sets errno on a negative argument, so without
            // -fno-math-errno the auto-vectorizer keeps the loop on scalar
            // vsqrtsd plus an errno branch. The packed form is bit-identical
            // for every input; only errno differs, which ibex never reads.
            std::size_t i = 0;
#ifdef __AVX2__
            for (; i + 4 <= n; i += 4) {
                _mm256_storeu_pd(dst + i, _mm256_sqrt_pd(_mm256_loadu_pd(src + i)));
            }
#endif
            for (; i < n; ++i) {
                dst[i] = std::sqrt(src[i]);
            }
            return;
        }
        default:
            break;
    }
    if (const VectorKernel kernel = vector_kernel(fn); kernel != nullptr) {
        apply_vector_kernel(kernel, src, dst, n);
        return;
    }
    for (std::size_t i = 0; i < n; ++i) {
        dst[i] = scalar_unary_math(fn, src[i]);
    }
}

}  // namespace ibex::runtime
