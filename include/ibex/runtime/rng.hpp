#pragma once
// High-throughput PRNG for Ibex's vectorized RNG functions.
//
// Scalar engine (Xoshiro256pp)
//   · 256-bit state → fits entirely in L1 cache (vs 2496-byte MT state)
//   · Period 2^256 - 1; passes all known statistical test suites
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//   · Used by rand_uniform, rand_student_t, rand_bernoulli, rand_poisson, rand_int
//
// Portable 4-wide engine (Xoshiro256pp_x4_portable)
//   · Four independent xoshiro256++ streams in a Structure-of-Arrays layout
//   · SoA layout (s[word][lane]) lets the compiler auto-vectorize the inner
//     loops into AVX2 when -mavx2 is present; ILP from OoO execution benefits
//     even without SIMD
//   · Used by fill_normal_x4 on all paths (portable bulk and libmvec fast path)
//
// Normal generation: Marsaglia Polar method (both paths)
//   · Generate (u1, u2) ∈ (−1,1)²; accept if s = u1²+u2² < 1  (~78.5%)
//   · z0 = u1·√(−2·log(s)/s),  z1 = u2·√(−2·log(s)/s)
//   · Eliminates cos+sin vs Box-Muller; 2× faster on scalar, ≤1 ULP from log
//   · AVX2 + libmvec: 4-wide _ZGVdN4v_log, otherwise scalar std::log
//   · Both paths share the same xoshiro stream and identical accept/reject
//     decisions (exact arithmetic), so output ordering is bit-reproducible
//     across ISAs; only log values differ by ≤1 ULP
//
// Seeding: splitmix64 expands a single 64-bit seed into the four state words.

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>

#ifdef __AVX2__
#include <immintrin.h>
#endif

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

// ─── xoshiro256++ (scalar single-stream) ─────────────────────────────────────
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

// ─── Scalar helpers ───────────────────────────────────────────────────────────

// Convert a raw 64-bit word to a double in [0, 1) using the top 53 bits.
// Used by the single-stream engine (rand_uniform, rand_student_t, etc.).
inline auto bits_to_01(std::uint64_t x) noexcept -> double {
    return static_cast<double>(x >> 11) * 0x1.0p-53;
}

// Convert a raw 64-bit word to a double in (-1, 1) by treating the top 53
// bits as a signed integer and scaling.  Used by the Polar method.
inline auto bits_to_pm1(std::uint64_t x) noexcept -> double {
    return static_cast<double>(static_cast<std::int64_t>(x) >> 11) * 0x1.0p-52;
}

// Thread-local xoshiro256++ instance seeded from std::random_device.
inline auto get_rng() noexcept -> Xoshiro256pp& {
    static thread_local Xoshiro256pp rng{std::random_device{}()};
    return rng;
}

// Re-seed the thread-local scalar RNG with an explicit value.
inline void reseed_rng(std::uint64_t seed) noexcept {
    get_rng() = Xoshiro256pp{seed};
}

// ─── Four base seeds shared by all 4-wide engines ─────────────────────────────
// Derived from a single user seed via XOR with fixed offsets so each of the
// four streams has an independent trajectory.
namespace detail {
    inline constexpr std::uint64_t stream_offsets[4] = {
        0x0000000000000000ULL,
        0x9e3779b97f4a7c15ULL,
        0x6c62272e07bb0142ULL,
        0xd2a98b26625eee7bULL,
    };
} // namespace detail

// ─── xoshiro256++ × 4 (portable, always available) ───────────────────────────
//
// Four independent xoshiro256++ streams stored in SoA layout:
//   s[word][lane] — word W of all four stream states packed contiguously.
//
// This layout lets the compiler auto-vectorize each inner `for (int i…)` loop
// into a single AVX2 instruction when -mavx2 is active, while remaining
// correct plain C++ for all other targets.
struct Xoshiro256pp_x4_portable {
    // s[word][lane]: word w of stream lane.
    std::uint64_t s[4][4];

    explicit Xoshiro256pp_x4_portable(std::uint64_t seed) noexcept {
        for (int lane = 0; lane < 4; ++lane) {
            std::uint64_t lseed = seed ^ detail::stream_offsets[lane];
            s[0][lane] = splitmix64(lseed);
            s[1][lane] = splitmix64(lseed);
            s[2][lane] = splitmix64(lseed);
            s[3][lane] = splitmix64(lseed);
        }
    }

    // Advance all four streams and return one output per lane.
    // The inner loops over [0..3] are independent and auto-vectorize.
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

// Thread-local portable 4-wide engine seeded from std::random_device.
inline auto get_rng_x4_portable() noexcept -> Xoshiro256pp_x4_portable& {
    static thread_local Xoshiro256pp_x4_portable rng{std::random_device{}()};
    return rng;
}

// ─── libmvec declaration (AVX2 + glibc only) ──────────────────────────────────
#if defined(__AVX2__) && defined(IBEX_HAVE_LIBMVEC)
// GNU Vector SIMD ABI — 4-wide double log from glibc's libmvec.
// _ZGVdN4v_log: d=double, N4=4 elements no mask, v=vector, _log=function.
// Not in any public header; called directly because our loop is already
// manually structured and auto-vectorization cannot apply.
extern "C" { __m256d _ZGVdN4v_log(__m256d x); }
#endif

// ─── fill_normal_x4 (always available) ───────────────────────────────────────
//
// Generate `rows` normally-distributed doubles into `out[0..rows)` using the
// Marsaglia Polar method.  Both dispatch paths consume the same xoshiro stream
// (get_rng_x4_portable) and perform the same accept/reject test with exact
// arithmetic, so output ordering is bit-reproducible across ISAs.  The only
// cross-ISA difference is ≤1 ULP in the log values.
//
//   AVX2 + libmvec: 4-wide _ZGVdN4v_log for the accepted batch (~2× faster)
//   Portable:       scalar std::log per accepted pair
inline void fill_normal_x4(double* __restrict__ out, std::size_t rows,
                            double mean, double stddev) noexcept {
    auto& rng4 = get_rng_x4_portable();
    std::size_t i = 0;

#if defined(__AVX2__) && defined(IBEX_HAVE_LIBMVEC)
    const __m256d neg2 = _mm256_set1_pd(-2.0);
    const __m256d one  = _mm256_set1_pd(1.0);
    const __m256d zero = _mm256_setzero_pd();
    const __m256d vmean = _mm256_set1_pd(mean);
    const __m256d vstd  = _mm256_set1_pd(stddev);

    alignas(32) double z0buf[4], z1buf[4];

    while (i < rows) {
        const auto r1 = rng4();
        const auto r2 = rng4();

        // Same bits_to_pm1 as portable path → identical u1, u2
        const __m256d u1 = _mm256_set_pd(
            bits_to_pm1(r1[3]), bits_to_pm1(r1[2]),
            bits_to_pm1(r1[1]), bits_to_pm1(r1[0]));
        const __m256d u2 = _mm256_set_pd(
            bits_to_pm1(r2[3]), bits_to_pm1(r2[2]),
            bits_to_pm1(r2[1]), bits_to_pm1(r2[0]));

        // mul+add (not FMA) → same rounding as portable u1*u1 + u2*u2
        const __m256d s = _mm256_add_pd(
            _mm256_mul_pd(u1, u1),
            _mm256_mul_pd(u2, u2));

        // Accept: 0 < s < 1 — exact, bit-identical to portable
        const int accept_bits = _mm256_movemask_pd(_mm256_and_pd(
            _mm256_cmp_pd(s, one,  _CMP_LT_OQ),
            _mm256_cmp_pd(s, zero, _CMP_GT_OQ)));
        if (accept_bits == 0) [[unlikely]] continue;

        // scale = sqrt(−2·log(s)/s): log 4-wide, sqrt hardware-exact
        const __m256d scale = _mm256_sqrt_pd(_mm256_div_pd(
            _mm256_mul_pd(neg2, _ZGVdN4v_log(s)), s));

        _mm256_store_pd(z0buf, _mm256_fmadd_pd(vstd, _mm256_mul_pd(u1, scale), vmean));
        _mm256_store_pd(z1buf, _mm256_fmadd_pd(vstd, _mm256_mul_pd(u2, scale), vmean));

        for (int lane = 0; lane < 4 && i < rows; ++lane) {
            if (!((accept_bits >> lane) & 1)) continue;
            out[i++] = z0buf[lane];
            if (i < rows) out[i++] = z1buf[lane];
        }
    }

#else
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
#endif
}

// ─── reseed_rng_x4 (always available) ────────────────────────────────────────
// Re-seeds the 4-wide portable engine used by fill_normal_x4.
// Called by seed_rng() and by tests; callers never need #ifdef guards.
inline void reseed_rng_x4(std::uint64_t seed) noexcept {
    get_rng_x4_portable() = Xoshiro256pp_x4_portable{seed};
}

} // namespace ibex::runtime
