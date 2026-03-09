#pragma once
// High-throughput PRNG for Ibex's vectorized RNG functions.
//
// Scalar engine (Xoshiro256pp)
//   · 256-bit state → fits entirely in L1 cache (vs 2496-byte MT state)
//   · Period 2^256 - 1; passes all known statistical test suites
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//   · Used by std::<dist> paths (student_t, gamma, poisson, int)
//
// 2-wide engine (Xoshiro256pp_x2)
//   · Two independent xoshiro256++ streams in SoA layout (s[word][lane])
//   · s[4][2] = 8 × u64 = 64 bytes — fits in GP registers; no stack spills
//   · Compiler maps s[word] to one SSE2 XMM register (2 × u64); auto-vectorizes
//     with -msse2 which is always available on x86-64
//   · Drop-in scalar replacement: benefits from ILP and SSE2 without AVX2
//
// 4-wide engine (Xoshiro256pp_x4_portable)
//   · Four independent xoshiro256++ streams in SoA layout (s[word][lane])
//   · SoA lets the compiler auto-vectorize inner loops with -mavx2
//   · Used by fill_uniform_x4, fill_exponential_x4, fill_bernoulli_x4, fill_normal_x4
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
        const std::uint64_t t = s[1] << 17;
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
}  // namespace detail

struct Xoshiro256pp_x4_portable {
    std::uint64_t s[4][4];  // s[word][lane]

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
        for (int i = 0; i < 4; ++i)
            t[i] = s[1][i] << 17;

        for (int i = 0; i < 4; ++i)
            s[2][i] ^= s[0][i];
        for (int i = 0; i < 4; ++i)
            s[3][i] ^= s[1][i];
        for (int i = 0; i < 4; ++i)
            s[1][i] ^= s[2][i];
        for (int i = 0; i < 4; ++i)
            s[0][i] ^= s[3][i];
        for (int i = 0; i < 4; ++i)
            s[2][i] ^= t[i];
        for (int i = 0; i < 4; ++i)
            s[3][i] = std::rotl(s[3][i], 45);

        return {result[0], result[1], result[2], result[3]};
    }
};

inline auto get_rng_x4_portable() noexcept -> Xoshiro256pp_x4_portable& {
    static thread_local Xoshiro256pp_x4_portable rng{std::random_device{}()};
    return rng;
}

// ─── fill_normal_x4 ───────────────────────────────────────────────────────────
// Marsaglia Polar method: generates `rows` N(mean, stddev²) samples into `out`.
inline void fill_normal_x4(double* __restrict__ out, std::size_t rows, double mean,
                           double stddev) noexcept {
    auto& rng4 = get_rng_x4_portable();
    std::size_t i = 0;
    while (i < rows) {
        const auto r1 = rng4();
        const auto r2 = rng4();
        for (std::size_t lane = 0; lane < 4 && i < rows; ++lane) {
            const double u1 = bits_to_pm1(r1[lane]);
            const double u2 = bits_to_pm1(r2[lane]);
            const double s = u1 * u1 + u2 * u2;
            if (s >= 1.0 || s == 0.0) [[unlikely]]
                continue;
            const double scale = std::sqrt(-2.0 * std::log(s) / s);
            out[i++] = mean + stddev * u1 * scale;
            if (i < rows)
                out[i++] = mean + stddev * u2 * scale;
        }
    }
}

inline void fill_uniform_x4(double* __restrict__ out, std::size_t rows, double low,
                            double high) noexcept {
    const double range = high - low;
    auto& rng4 = get_rng_x4_portable();
    std::size_t i = 0;
    while (i + 4 <= rows) {
        const auto bits = rng4();
        out[i] = low + bits_to_01(bits[0]) * range;
        out[i + 1] = low + bits_to_01(bits[1]) * range;
        out[i + 2] = low + bits_to_01(bits[2]) * range;
        out[i + 3] = low + bits_to_01(bits[3]) * range;
        i += 4;
    }
    if (i < rows) {
        const auto bits = rng4();
        for (std::size_t lane = 0; i < rows; ++lane, ++i) {
            out[i] = low + bits_to_01(bits[lane]) * range;
        }
    }
}

inline void fill_exponential_x4(double* __restrict__ out, std::size_t rows,
                                double lambda) noexcept {
    const double inv_lambda = 1.0 / lambda;
    auto& rng4 = get_rng_x4_portable();
    std::size_t i = 0;
    while (i + 4 <= rows) {
        const auto bits = rng4();
        out[i] = -std::log(bits_to_01(bits[0]) + 1e-300) * inv_lambda;
        out[i + 1] = -std::log(bits_to_01(bits[1]) + 1e-300) * inv_lambda;
        out[i + 2] = -std::log(bits_to_01(bits[2]) + 1e-300) * inv_lambda;
        out[i + 3] = -std::log(bits_to_01(bits[3]) + 1e-300) * inv_lambda;
        i += 4;
    }
    if (i < rows) {
        const auto bits = rng4();
        for (std::size_t lane = 0; i < rows; ++lane, ++i) {
            out[i] = -std::log(bits_to_01(bits[lane]) + 1e-300) * inv_lambda;
        }
    }
}

inline void fill_bernoulli_x4(std::int64_t* __restrict__ out, std::size_t rows, double p) noexcept {
    constexpr double kScale53 = 9007199254740992.0;  // 2^53
    const auto threshold = static_cast<std::uint64_t>(p * kScale53);
    auto& rng4 = get_rng_x4_portable();
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

inline void fill_int_x4(std::int64_t* __restrict__ out, std::size_t rows, std::int64_t lo,
                        std::uint64_t span) noexcept {
    // span = hi - lo + 1 as uint64 (caller ensures span != 0).
    // Multiply-shift: map a 53-bit uniform word into [0, span) without rejection.
    // Uses __uint128_t so the product never overflows regardless of span size.
    auto& rng4 = get_rng_x4_portable();
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

// ─── reseed_rng_x4 ────────────────────────────────────────────────────────────
inline void reseed_rng_x4(std::uint64_t seed) noexcept {
    get_rng_x4_portable() = Xoshiro256pp_x4_portable{seed};
}

// ─── xoshiro256++ × 2 ────────────────────────────────────────────────────────
// s[4][2] = 8 × u64 = 64 bytes: fits in GP registers without spills.
// With -msse2 (baseline x86-64) each s[word] maps to one XMM register,
// so the inner loops auto-vectorize as 128-bit SSE2 ops.
struct Xoshiro256pp_x2 {
    std::uint64_t s[4][2];  // s[word][lane]

    explicit Xoshiro256pp_x2(std::uint64_t seed) noexcept {
        for (int lane = 0; lane < 2; ++lane) {
            std::uint64_t lseed = seed ^ detail::stream_offsets[lane];
            s[0][lane] = splitmix64(lseed);
            s[1][lane] = splitmix64(lseed);
            s[2][lane] = splitmix64(lseed);
            s[3][lane] = splitmix64(lseed);
        }
    }

    [[nodiscard]] std::array<std::uint64_t, 2> operator()() noexcept {
        std::uint64_t result[2];
        for (int i = 0; i < 2; ++i)
            result[i] = std::rotl(s[0][i] + s[3][i], 23) + s[0][i];

        std::uint64_t t[2];
        for (int i = 0; i < 2; ++i)
            t[i] = s[1][i] << 17;

        for (int i = 0; i < 2; ++i)
            s[2][i] ^= s[0][i];
        for (int i = 0; i < 2; ++i)
            s[3][i] ^= s[1][i];
        for (int i = 0; i < 2; ++i)
            s[1][i] ^= s[2][i];
        for (int i = 0; i < 2; ++i)
            s[0][i] ^= s[3][i];
        for (int i = 0; i < 2; ++i)
            s[2][i] ^= t[i];
        for (int i = 0; i < 2; ++i)
            s[3][i] = std::rotl(s[3][i], 45);

        return {result[0], result[1]};
    }
};

inline auto get_rng_x2() noexcept -> Xoshiro256pp_x2& {
    static thread_local Xoshiro256pp_x2 rng{std::random_device{}()};
    return rng;
}

inline void reseed_rng_x2(std::uint64_t seed) noexcept {
    get_rng_x2() = Xoshiro256pp_x2{seed};
}

// ─── fill_*_x2 ───────────────────────────────────────────────────────────────

inline void fill_uniform_x2(double* __restrict__ out, std::size_t rows, double low,
                            double high) noexcept {
    const double range = high - low;
    auto& rng2 = get_rng_x2();
    std::size_t i = 0;
    while (i + 2 <= rows) {
        const auto bits = rng2();
        out[i] = low + bits_to_01(bits[0]) * range;
        out[i + 1] = low + bits_to_01(bits[1]) * range;
        i += 2;
    }
    if (i < rows) {
        const auto bits = rng2();
        out[i] = low + bits_to_01(bits[0]) * range;
    }
}

inline void fill_exponential_x2(double* __restrict__ out, std::size_t rows,
                                double lambda) noexcept {
    const double inv_lambda = 1.0 / lambda;
    auto& rng2 = get_rng_x2();
    std::size_t i = 0;
    while (i + 2 <= rows) {
        const auto bits = rng2();
        out[i] = -std::log(bits_to_01(bits[0]) + 1e-300) * inv_lambda;
        out[i + 1] = -std::log(bits_to_01(bits[1]) + 1e-300) * inv_lambda;
        i += 2;
    }
    if (i < rows) {
        const auto bits = rng2();
        out[i] = -std::log(bits_to_01(bits[0]) + 1e-300) * inv_lambda;
    }
}

inline void fill_bernoulli_x2(std::int64_t* __restrict__ out, std::size_t rows, double p) noexcept {
    constexpr double kScale53 = 9007199254740992.0;  // 2^53
    const auto threshold = static_cast<std::uint64_t>(p * kScale53);
    auto& rng2 = get_rng_x2();
    std::size_t i = 0;
    while (i + 2 <= rows) {
        const auto bits = rng2();
        out[i] = ((bits[0] >> 11) < threshold) ? 1 : 0;
        out[i + 1] = ((bits[1] >> 11) < threshold) ? 1 : 0;
        i += 2;
    }
    if (i < rows) {
        const auto bits = rng2();
        out[i] = ((bits[0] >> 11) < threshold) ? 1 : 0;
    }
}

// Marsaglia Polar method: 2-lane version of fill_normal_x4.
inline void fill_normal_x2(double* __restrict__ out, std::size_t rows, double mean,
                           double stddev) noexcept {
    auto& rng2 = get_rng_x2();
    std::size_t i = 0;
    while (i < rows) {
        const auto r1 = rng2();
        const auto r2 = rng2();
        for (std::size_t lane = 0; lane < 2 && i < rows; ++lane) {
            const double u1 = bits_to_pm1(r1[lane]);
            const double u2 = bits_to_pm1(r2[lane]);
            const double s = u1 * u1 + u2 * u2;
            if (s >= 1.0 || s == 0.0) [[unlikely]]
                continue;
            const double scale = std::sqrt(-2.0 * std::log(s) / s);
            out[i++] = mean + stddev * u1 * scale;
            if (i < rows)
                out[i++] = mean + stddev * u2 * scale;
        }
    }
}

}  // namespace ibex::runtime
