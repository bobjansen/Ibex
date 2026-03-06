#pragma once
// High-throughput PRNG for Ibex's vectorized RNG functions.
//
// Scalar engine (Xoshiro256pp)
//   · 256-bit state → fits entirely in L1 cache (vs 2496-byte MT state)
//   · Period 2^256 - 1; passes all known statistical test suites
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//   · Used by rand_uniform, rand_student_t, rand_bernoulli, rand_poisson, rand_int
//
// 4-wide engine (Xoshiro256pp_x4_portable)
//   · Four independent xoshiro256++ streams in SoA layout (s[word][lane])
//   · SoA lets the compiler auto-vectorize inner loops with -mavx2
//   · Used by fill_normal_x4 (Marsaglia Polar method)
//
// Normal generation: Marsaglia Polar method
//   · Generate (u1, u2) ∈ (−1,1)²; accept if s = u1²+u2² < 1  (~78.5%)
//   · z0 = u1·√(−2·log(s)/s),  z1 = u2·√(−2·log(s)/s)
//   · ~2× faster than scalar Box-Muller: no cos/sin, only log+sqrt
//
// Seeding: splitmix64 expands a single 64-bit seed into the four state words.

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>

namespace ibex::runtime {

// ─── splitmix64 ───────────────────────────────────────────────────────────────
inline auto splitmix64(std::uint64_t& state) noexcept -> std::uint64_t {
    state += 0x9e3779b97f4a7c15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

// ─── xoshiro256++ (scalar single-stream) ─────────────────────────────────────
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

// ─── Scalar helpers ───────────────────────────────────────────────────────────

// Convert a raw 64-bit word to a double in [0, 1) using the top 53 bits.
inline auto bits_to_01(std::uint64_t x) noexcept -> double {
    return static_cast<double>(x >> 11) * 0x1.0p-53;
}

// Convert a raw 64-bit word to a double in (-1, 1).  Used by the Polar method.
inline auto bits_to_pm1(std::uint64_t x) noexcept -> double {
    return static_cast<double>(static_cast<std::int64_t>(x) >> 11) * 0x1.0p-52;
}

// Thread-local xoshiro256++ instance seeded from std::random_device.
inline auto get_rng() noexcept -> Xoshiro256pp& {
    static thread_local Xoshiro256pp rng{std::random_device{}()};
    return rng;
}

inline void reseed_rng(std::uint64_t seed) noexcept {
    get_rng() = Xoshiro256pp{seed};
}

// ─── xoshiro256++ × 4 (portable) ─────────────────────────────────────────────
namespace detail {
    inline constexpr std::uint64_t stream_offsets[4] = {
        0x0000000000000000ULL,
        0x9e3779b97f4a7c15ULL,
        0x6c62272e07bb0142ULL,
        0xd2a98b26625eee7bULL,
    };
} // namespace detail

struct Xoshiro256pp_x4_portable {
    std::uint64_t s[4][4]; // s[word][lane]

    explicit Xoshiro256pp_x4_portable(std::uint64_t seed) noexcept {
        for (int lane = 0; lane < 4; ++lane) {
            std::uint64_t lseed = seed ^ detail::stream_offsets[lane];
            s[0][lane] = splitmix64(lseed);
            s[1][lane] = splitmix64(lseed);
            s[2][lane] = splitmix64(lseed);
            s[3][lane] = splitmix64(lseed);
        }
    }

    [[nodiscard]] std::array<std::uint64_t, 4> operator()() noexcept {
        std::uint64_t result[4];
        for (int i = 0; i < 4; ++i)
            result[i] = std::rotl(s[0][i] + s[3][i], 23) + s[0][i];

        std::uint64_t t[4];
        for (int i = 0; i < 4; ++i) t[i] = s[1][i] << 17;

        for (int i = 0; i < 4; ++i) s[2][i] ^= s[0][i];
        for (int i = 0; i < 4; ++i) s[3][i] ^= s[1][i];
        for (int i = 0; i < 4; ++i) s[1][i] ^= s[2][i];
        for (int i = 0; i < 4; ++i) s[0][i] ^= s[3][i];
        for (int i = 0; i < 4; ++i) s[2][i] ^= t[i];
        for (int i = 0; i < 4; ++i) s[3][i] = std::rotl(s[3][i], 45);

        return {result[0], result[1], result[2], result[3]};
    }
};

inline auto get_rng_x4_portable() noexcept -> Xoshiro256pp_x4_portable& {
    static thread_local Xoshiro256pp_x4_portable rng{std::random_device{}()};
    return rng;
}

// ─── fill_normal_x4 ───────────────────────────────────────────────────────────
// Marsaglia Polar method: generates `rows` N(mean, stddev²) samples into `out`.
inline void fill_normal_x4(double* __restrict__ out, std::size_t rows,
                            double mean, double stddev) noexcept {
    auto& rng4 = get_rng_x4_portable();
    std::size_t i = 0;
    while (i < rows) {
        const auto r1 = rng4();
        const auto r2 = rng4();
        for (int lane = 0; lane < 4 && i < rows; ++lane) {
            const double u1 = bits_to_pm1(r1[lane]);
            const double u2 = bits_to_pm1(r2[lane]);
            const double s  = u1 * u1 + u2 * u2;
            if (s >= 1.0 || s == 0.0) [[unlikely]] continue;
            const double scale = std::sqrt(-2.0 * std::log(s) / s);
            out[i++] = mean + stddev * u1 * scale;
            if (i < rows) out[i++] = mean + stddev * u2 * scale;
        }
    }
}

// ─── reseed_rng_x4 ────────────────────────────────────────────────────────────
inline void reseed_rng_x4(std::uint64_t seed) noexcept {
    get_rng_x4_portable() = Xoshiro256pp_x4_portable{seed};
}

} // namespace ibex::runtime
