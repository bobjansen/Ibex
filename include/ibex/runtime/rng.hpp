#pragma once
// High-throughput PRNG for Ibex's vectorized RNG functions.
//
// Engine: xoshiro256++ by Blackman & Vigna (https://prng.di.unimi.it/)
//   · 256-bit state → fits entirely in L1 cache (vs 2496-byte MT state)
//   · ~3-4× faster than std::mt19937_64 in the scalar generation loop
//   · Period 2^256 - 1; passes all known statistical test suites
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//
// Seeding: splitmix64 expands a single 64-bit seed into the four state words.

#include <cstdint>
#include <limits>
#include <numbers>
#include <random>

namespace ibex::runtime {

// ─── splitmix64 ───────────────────────────────────────────────────────────────
// Used to expand a single seed into the four xoshiro state words.
// Reference: https://prng.di.unimi.it/splitmix64.c
inline auto splitmix64(std::uint64_t& state) noexcept -> std::uint64_t {
    state += 0x9e3779b97f4a7c15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

// ─── xoshiro256++ ─────────────────────────────────────────────────────────────
// Reference: https://prng.di.unimi.it/xoshiro256plusplus.c
struct Xoshiro256pp {
    using result_type = std::uint64_t;

    static constexpr auto min() noexcept -> result_type { return 0; }
    static constexpr auto max() noexcept -> result_type {
        return std::numeric_limits<result_type>::max();
    }

    explicit Xoshiro256pp(std::uint64_t seed) noexcept {
        s[0] = splitmix64(seed);
        s[1] = splitmix64(seed);
        s[2] = splitmix64(seed);
        s[3] = splitmix64(seed);
    }

    auto operator()() noexcept -> result_type {
        const std::uint64_t result = rotl(s[0] + s[3], 23) + s[0];
        const std::uint64_t t      = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = rotl(s[3], 45);
        return result;
    }

private:
    std::uint64_t s[4];

    static constexpr auto rotl(std::uint64_t x, int k) noexcept -> std::uint64_t {
        return (x << k) | (x >> (64 - k));
    }
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Convert a raw 64-bit word to a double in [0, 1) using the top 53 bits.
// This avoids any division and is exact (no rounding artefacts above the ULP).
inline auto bits_to_01(std::uint64_t x) noexcept -> double {
    return static_cast<double>(x >> 11) * 0x1.0p-53;
}

// Box-Muller transform: produce two standard-normal variates from two
// independent uniforms u1 ∈ (0,1) and u2 ∈ [0,1).
// Caller must ensure u1 > 0 (add a tiny epsilon before calling if needed).
inline void box_muller(double u1, double u2, double& z0, double& z1) noexcept {
    // r = sqrt(-2 ln u1),  theta = 2π u2
    const double r     = std::sqrt(-2.0 * std::log(u1));
    const double theta = 2.0 * std::numbers::pi * u2;
    z0 = r * std::cos(theta);
    z1 = r * std::sin(theta);
}

// Thread-local xoshiro256++ instance seeded from std::random_device.
inline auto get_rng() noexcept -> Xoshiro256pp& {
    static thread_local Xoshiro256pp rng{std::random_device{}()};
    return rng;
}

} // namespace ibex::runtime
