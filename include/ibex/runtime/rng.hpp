#pragma once
// Ibex RNG layer — delegates to zorro.hpp for SIMD-dispatched xoshiro256++.
//
// Scalar engine (zorro::Xoshiro256pp)
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//   · Used by std::<dist> paths (student_t, gamma, poisson)
//
// SIMD engine (zorro::Rng)
//   · Automatic compile-time dispatch: AVX-512 → AVX2 → portable
//   · AMD Zen 4: integer kernels use AVX-512, FP-heavy kernels fall back to AVX2
//   · Used for fill_uniform, fill_normal, fill_exponential
//
// Integer fills (bernoulli → int64, rand_int)
//   · Use zorro::Xoshiro256pp_x4_portable directly (integer-domain comparison)
//
// Seeding: reseed() resets all three thread-local engines to the given seed.

#include <cstdint>
#include <random>
#include <zorro.hpp>

namespace ibex::runtime {

// Re-export scalar primitives from zorro.
using zorro::bits_to_01;
using zorro::bits_to_pm1;
using zorro::Xoshiro256pp;

// ─── Thread-local scalar engine (for std::distribution) ──────────────────────

inline auto get_rng() noexcept -> Xoshiro256pp& {
    static thread_local Xoshiro256pp rng{std::random_device{}()};
    return rng;
}

// ─── Thread-local SIMD-dispatched engine (for bulk double fills) ─────────────

inline auto get_rng_simd() noexcept -> zorro::Rng& {
    static thread_local zorro::Rng rng{std::random_device{}()};
    return rng;
}

// ─── Thread-local x4 portable engine (for integer fills) ─────────────────────

using zorro::Xoshiro256pp_x4_portable;

inline auto get_rng_x4() noexcept -> Xoshiro256pp_x4_portable& {
    static thread_local Xoshiro256pp_x4_portable rng{std::random_device{}()};
    return rng;
}

// ─── Seeding ─────────────────────────────────────────────────────────────────

inline void reseed(std::uint64_t seed) noexcept {
    get_rng() = Xoshiro256pp{seed};
    get_rng_simd() = zorro::Rng{seed};
    get_rng_x4() = Xoshiro256pp_x4_portable{seed};
}

// ─── Bulk fills: double output (delegate to zorro::Rng) ──────────────────────

inline void fill_uniform(double* __restrict__ out, std::size_t rows, double low,
                         double high) noexcept {
    get_rng_simd().fill_uniform(out, rows, low, high);
}

inline void fill_normal(double* __restrict__ out, std::size_t rows, double mean,
                        double stddev) noexcept {
    get_rng_simd().fill_normal(out, rows, mean, stddev);
}

inline void fill_exponential(double* __restrict__ out, std::size_t rows, double lambda) noexcept {
    get_rng_simd().fill_exponential(out, rows, lambda);
}

// ─── Bulk fills: int64 output (zorro has no int64 bernoulli/int) ─────────────

inline void fill_bernoulli(std::int64_t* __restrict__ out, std::size_t rows, double p) noexcept {
    constexpr double kScale53 = 9007199254740992.0;  // 2^53
    const auto threshold = static_cast<std::uint64_t>(p * kScale53);
    auto& rng4 = get_rng_x4();
    std::size_t i = 0;
    while (i + 4 <= rows) {
        const auto bits = rng4();
        out[i] = ((bits[0] >> 11) < threshold) ? 1 : 0;
        out[i + 1] = ((bits[1] >> 11) < threshold) ? 1 : 0;
        out[i + 2] = ((bits[2] >> 11) < threshold) ? 1 : 0;
        out[i + 3] = ((bits[3] >> 11) < threshold) ? 1 : 0;
        i += 4;
    }
    if (i < rows) {
        const auto bits = rng4();
        for (std::size_t lane = 0; i < rows; ++lane, ++i) {
            out[i] = ((bits[lane] >> 11) < threshold) ? 1 : 0;
        }
    }
}

inline void fill_int(std::int64_t* __restrict__ out, std::size_t rows, std::int64_t lo,
                     std::uint64_t span) noexcept {
    // span = hi - lo + 1 as uint64 (caller ensures span != 0).
    // Multiply-shift: map a 53-bit uniform word into [0, span) without rejection.
    auto& rng4 = get_rng_x4();
    std::size_t i = 0;
    while (i + 4 <= rows) {
        const auto bits = rng4();
        out[i] =
            lo + static_cast<std::int64_t>((static_cast<__uint128_t>(bits[0] >> 11) * span) >> 53);
        out[i + 1] =
            lo + static_cast<std::int64_t>((static_cast<__uint128_t>(bits[1] >> 11) * span) >> 53);
        out[i + 2] =
            lo + static_cast<std::int64_t>((static_cast<__uint128_t>(bits[2] >> 11) * span) >> 53);
        out[i + 3] =
            lo + static_cast<std::int64_t>((static_cast<__uint128_t>(bits[3] >> 11) * span) >> 53);
        i += 4;
    }
    if (i < rows) {
        const auto bits = rng4();
        for (std::size_t lane = 0; i < rows; ++lane, ++i) {
            out[i] = lo + static_cast<std::int64_t>(
                              (static_cast<__uint128_t>(bits[lane] >> 11) * span) >> 53);
        }
    }
}

}  // namespace ibex::runtime
