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
//   · Each operator()() call advances all four streams and returns one value
//     per stream in a std::array<uint64_t,4>
//   · SoA layout (s[word][lane]) lets the compiler auto-vectorize the inner
//     loops into AVX2 when -mavx2 is present; ILP from OoO execution benefits
//     even without SIMD
//   · Used by fill_normal_x4's Polar path (portable bulk + libmvec tail)
//
// SIMD engine (Xoshiro256pp_x4, __AVX2__ + IBEX_HAVE_LIBMVEC only)
//   · Four independent streams held in four __m256i registers (one per state
//     word, four lanes per register)
//   · Combined with libmvec _ZGVdN4v_log/cos/sin for fully 4-wide Box-Muller
//   · ~5× throughput vs scalar; entire gain comes from vectorized transcendentals
//
// Algorithm selection in fill_normal_x4
//   · libmvec available: Box-Muller (x4 xoshiro + vectorized log/cos/sin) for
//     bulk, Polar for tail
//   · no libmvec: Marsaglia Polar method throughout (~2× faster than scalar
//     Box-Muller: eliminating cos+sin saves more than the ~27% rejection overhead)
//   · The two paths use different algorithms and produce different output
//     sequences for the same seed; both are statistically correct.
//
// Seeding: splitmix64 expands a single 64-bit seed into the four state words.

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numbers>
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

// Convert a raw 64-bit word to a double in [0, 1) using the IEEE-754 mantissa
// trick: OR the low 52 bits into a biased [1, 2) exponent then subtract 1.0.
// Produces the same bit pattern as bits_to_01_x4 on the same raw value, so
// the two paths agree bit-for-bit on the uniform doubles they feed to Box-Muller.
inline auto bits_to_01_portable(std::uint64_t x) noexcept -> double {
    constexpr std::uint64_t mantissa_mask = 0x000FFFFFFFFFFFFFULL;
    constexpr std::uint64_t exponent_one  = 0x3FF0000000000000ULL;
    return std::bit_cast<double>((x & mantissa_mask) | exponent_one) - 1.0;
}

// Convert a raw 64-bit word to a double in (-1, 1) by treating the top 53
// bits as a signed integer and scaling.  Used by the Polar method.
inline auto bits_to_pm1(std::uint64_t x) noexcept -> double {
    return static_cast<double>(static_cast<std::int64_t>(x) >> 11) * 0x1.0p-52;
}

// Box-Muller transform: produce two standard-normal variates from two
// independent uniforms u1 ∈ (0,1) and u2 ∈ [0,1).
// Used only by the libmvec fast path (the portable path uses Polar instead).
inline void box_muller(double u1, double u2, double& z0, double& z1) noexcept {
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

// Re-seed the thread-local scalar RNG with an explicit value.
inline void reseed_rng(std::uint64_t seed) noexcept {
    get_rng() = Xoshiro256pp{seed};
}

// ─── Four base seeds shared by both 4-wide engines ────────────────────────────
// Derived from a single user seed.  Both Xoshiro256pp_x4_portable and
// Xoshiro256pp_x4 use these constants so their streams are identical.
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
//
// Produces the same uniform stream as Xoshiro256pp_x4 for the same seed.
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

// ─── xoshiro256++ × 4 (AVX2 explicit-SIMD) ───────────────────────────────────
#ifdef __AVX2__

// GNU Vector SIMD ABI — 4-wide double transcendentals from glibc's libmvec.
//
// These look like C++ mangled names but are not: _ZGVdN4v_<func> is the GNU
// Vector SIMD ABI naming scheme (glibc ≥ 2.22, stable ABI):
//   _ZGV  — GNU Vector function prefix
//   d     — double precision
//   N4    — 4 elements, no mask
//   v     — vector (all lanes active)
//   _log  — the scalar math function being vectorized
//
// There is no public glibc header for them: they are intended to be called
// only by compilers via -fveclib=libmvec.  We call them directly because our
// loop is already manually vectorized and auto-vec can't apply.  On non-glibc
// systems (musl, macOS) IBEX_HAVE_LIBMVEC is not defined and fill_normal_x4
// falls back to scalar transcendentals even when __AVX2__ is set.
#if defined(IBEX_HAVE_LIBMVEC)
extern "C" {
    __m256d _ZGVdN4v_log(__m256d x);
    __m256d _ZGVdN4v_cos(__m256d x);
    __m256d _ZGVdN4v_sin(__m256d x);
}
#endif

struct Xoshiro256pp_x4 {
    // s[w] holds word w of all four streams packed in one __m256i.
    __m256i s[4];

    explicit Xoshiro256pp_x4(std::uint64_t seed) noexcept {
        std::uint64_t sw[4][4]; // sw[lane][word]
        for (int lane = 0; lane < 4; ++lane) {
            std::uint64_t lseed = seed ^ detail::stream_offsets[lane];
            sw[lane][0] = splitmix64(lseed);
            sw[lane][1] = splitmix64(lseed);
            sw[lane][2] = splitmix64(lseed);
            sw[lane][3] = splitmix64(lseed);
        }
        for (int w = 0; w < 4; ++w) {
            s[w] = _mm256_set_epi64x(
                static_cast<long long>(sw[3][w]),
                static_cast<long long>(sw[2][w]),
                static_cast<long long>(sw[1][w]),
                static_cast<long long>(sw[0][w]));
        }
    }

    [[nodiscard]] __m256i operator()() noexcept {
        const __m256i result = add(rotl(add(s[0], s[3]), 23), s[0]);
        const __m256i t      = _mm256_slli_epi64(s[1], 17);
        s[2] = xorv(s[2], s[0]);
        s[3] = xorv(s[3], s[1]);
        s[1] = xorv(s[1], s[2]);
        s[0] = xorv(s[0], s[3]);
        s[2] = xorv(s[2], t);
        s[3] = rotl(s[3], 45);
        return result;
    }

private:
    static __m256i add(__m256i a, __m256i b) noexcept { return _mm256_add_epi64(a, b); }
    static __m256i xorv(__m256i a, __m256i b) noexcept { return _mm256_xor_si256(a, b); }
    static __m256i rotl(__m256i v, int k) noexcept {
        return xorv(_mm256_slli_epi64(v, k), _mm256_srli_epi64(v, 64 - k));
    }
};

// Convert four raw uint64_t to four doubles in [0, 1).
// Uses the same IEEE-754 mantissa trick as bits_to_01_portable so that both
// paths produce bit-identical doubles for the same raw random word.
inline __m256d bits_to_01_x4(__m256i x) noexcept {
    const __m256i mantissa_mask = _mm256_set1_epi64x(0x000FFFFFFFFFFFFFLL);
    const __m256i exponent_one  = _mm256_set1_epi64x(0x3FF0000000000000LL);
    const __m256i bits = _mm256_or_si256(
        _mm256_and_si256(x, mantissa_mask),
        exponent_one);
    return _mm256_sub_pd(_mm256_castsi256_pd(bits), _mm256_set1_pd(1.0));
}

// Thread-local AVX2 4-wide engine.
inline auto get_rng_x4() noexcept -> Xoshiro256pp_x4& {
    static thread_local Xoshiro256pp_x4 rng{std::random_device{}()};
    return rng;
}

#endif // __AVX2__

// ─── fill_normal_x4 (always available) ───────────────────────────────────────
//
// Generate `rows` normally-distributed doubles into `out[0..rows)`.
//
// Two dispatch paths, selected at compile time:
//
//   AVX2 + libmvec  (both __AVX2__ and IBEX_HAVE_LIBMVEC defined)
//     Xoshiro256pp_x4 (__m256i) + _ZGVdN4v_{log,cos,sin} (Box-Muller) for
//     groups of 8 outputs — ~5× vs scalar.  The remaining tail (< 8 elements)
//     is handled by the Polar fallback below.
//
//   Portable (everything else, including the libmvec tail)
//     Marsaglia Polar method with Xoshiro256pp_x4_portable:
//       · Generate (u1, u2) ∈ (−1,1)²; accept if s = u1²+u2² < 1
//       · z0 = u1·√(−2·log(s)/s),  z1 = u2·√(−2·log(s)/s)
//       · Acceptance rate π/4 ≈ 78.5%; only log+sqrt, no trig
//     Benchmarked ~2× faster than scalar Box-Muller: eliminating cos+sin saves
//     more than the ~27% extra candidates from rejection.  Polar + libmvec
//     would be slower than Box-Muller + libmvec (rejection overhead > trig
//     savings when transcendentals are already vectorized), so Box-Muller is
//     kept for the libmvec bulk path.
//
// Note: the libmvec and portable paths use different algorithms (Box-Muller vs
// Polar) and therefore produce different output sequences for the same seed.
// Both are statistically correct and seeding is per-engine (reseed_rng_x4
// reseeds both engines so determinism is preserved within each path).
inline void fill_normal_x4(double* __restrict__ out, std::size_t rows,
                            double mean, double stddev) noexcept {
    std::size_t i = 0;

#if defined(__AVX2__) && defined(IBEX_HAVE_LIBMVEC)
    // ── fast path: 4-wide xoshiro + 4-wide libmvec transcendentals ───────────
    auto& rng4 = get_rng_x4();

    const __m256d two_pi = _mm256_set1_pd(2.0 * std::numbers::pi);
    const __m256d neg2   = _mm256_set1_pd(-2.0);
    const __m256d eps    = _mm256_set1_pd(1e-300);
    const __m256d vmean  = _mm256_set1_pd(mean);
    const __m256d vstd   = _mm256_set1_pd(stddev);

    for (; i + 7 < rows; i += 8) {
        const __m256d u1 = _mm256_add_pd(bits_to_01_x4(rng4()), eps);
        const __m256d u2 = bits_to_01_x4(rng4());
        const __m256d r  = _mm256_sqrt_pd(_mm256_mul_pd(neg2, _ZGVdN4v_log(u1)));
        const __m256d th = _mm256_mul_pd(two_pi, u2);
        const __m256d z0 = _mm256_add_pd(vmean,
            _mm256_mul_pd(vstd, _mm256_mul_pd(r, _ZGVdN4v_cos(th))));
        const __m256d z1 = _mm256_add_pd(vmean,
            _mm256_mul_pd(vstd, _mm256_mul_pd(r, _ZGVdN4v_sin(th))));

        // Interleave: out[i+0]=z0[0], out[i+1]=z1[0], out[i+2]=z0[1], …
        const __m256d lo = _mm256_unpacklo_pd(z0, z1);
        const __m256d hi = _mm256_unpackhi_pd(z0, z1);
        _mm256_storeu_pd(out + i,     _mm256_permute2f128_pd(lo, hi, 0x20));
        _mm256_storeu_pd(out + i + 4, _mm256_permute2f128_pd(lo, hi, 0x31));
    }
#endif

    // ── Polar method: portable bulk path + libmvec tail (i..rows) ────────────
    // Processes 4 candidate pairs per xoshiro call; accepted pairs are written
    // sequentially.  Naturally handles any remaining count including odd tails.
    auto& rng4p = get_rng_x4_portable();
    while (i < rows) {
        const auto r1 = rng4p();
        const auto r2 = rng4p();
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

// ─── reseed_rng_x4 (always available) ────────────────────────────────────────
// Re-seeds all 4-wide engines that are active for this build.
// Called by seed_rng() and by tests; callers never need #ifdef guards.
inline void reseed_rng_x4(std::uint64_t seed) noexcept {
    get_rng_x4_portable() = Xoshiro256pp_x4_portable{seed};
#ifdef __AVX2__
    // Always reseed get_rng_x4() when AVX2 is available, even if
    // IBEX_HAVE_LIBMVEC is not defined in the current TU.  Inline functions
    // with static-thread_local state obey ODR: get_rng_x4() is the same
    // object in every TU, so callers that lack IBEX_HAVE_LIBMVEC must still
    // reseed it for TUs that do have it (e.g. ibex_runtime.a compiled with
    // IBEX_HAVE_LIBMVEC=1 and using the libmvec fill_normal_x4 path).
    get_rng_x4() = Xoshiro256pp_x4{seed};
#endif
}

} // namespace ibex::runtime
