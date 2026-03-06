#pragma once
// High-throughput PRNG for Ibex's vectorized RNG functions.
//
// Scalar engine: xoshiro256++ by Blackman & Vigna (https://prng.di.unimi.it/)
//   · 256-bit state → fits entirely in L1 cache (vs 2496-byte MT state)
//   · ~3-4× faster than std::mt19937_64 in the scalar generation loop
//   · Period 2^256 - 1; passes all known statistical test suites
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//
// SIMD engine: Xoshiro256pp_x4 (AVX2, available when __AVX2__ is defined)
//   · Runs four independent xoshiro256++ streams in parallel using __m256i
//   · Each call to operator()() advances all four streams and returns four
//     uint64_t in a single __m256i — four times the raw throughput
//   · bits_to_01_x4() converts them to four doubles in [0, 1) via the
//     IEEE-754 mantissa trick (no division, no explicit integer→float path)
//
// Seeding: splitmix64 expands a single 64-bit seed into the state words.

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

// ─── xoshiro256++ (scalar) ────────────────────────────────────────────────────
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
inline auto bits_to_01(std::uint64_t x) noexcept -> double {
    return static_cast<double>(x >> 11) * 0x1.0p-53;
}

// Box-Muller transform: produce two standard-normal variates from two
// independent uniforms u1 ∈ (0,1) and u2 ∈ [0,1).
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

// ─── xoshiro256++ × 4 (AVX2) ─────────────────────────────────────────────────
//
// Four fully independent xoshiro256++ streams packed into four __m256i words.
// Each operator()() call advances all four streams simultaneously and returns
// a __m256i holding one output uint64_t from each lane.
//
// Independence: the four streams are seeded from four well-separated seeds
// produced by running splitmix64 in four different positions.  They share no
// state and produce statistically independent sequences.
//
// Only compiled when the compiler has AVX2 enabled (-mavx2 / -march=native).
#ifdef __AVX2__

// libmvec 4-wide double transcendentals (glibc ≥ 2.22).
// These are regular C functions exported from libmvec.so; the mangled names
// follow the GNU Vector ABI: _ZGVdN4v_<func>.
extern "C" {
    __m256d _ZGVdN4v_log(__m256d x);
    __m256d _ZGVdN4v_cos(__m256d x);
    __m256d _ZGVdN4v_sin(__m256d x);
}

struct Xoshiro256pp_x4 {
    // s[w] holds word w of all four streams:
    //   lane 0 = stream A, lane 1 = stream B, lane 2 = stream C, lane 3 = stream D
    __m256i s[4];

    explicit Xoshiro256pp_x4(std::uint64_t seed) noexcept {
        // Derive four well-separated seeds then expand each with splitmix64.
        // Multiplying by successive large primes gives state-space separation.
        std::uint64_t seeds[4] = {
            seed,
            seed ^ 0x9e3779b97f4a7c15ULL,
            seed ^ 0x6c62272e07bb0142ULL,
            seed ^ 0xd2a98b26625eee7bULL,
        };
        std::uint64_t sw[4][4];
        for (int lane = 0; lane < 4; ++lane) {
            sw[lane][0] = splitmix64(seeds[lane]);
            sw[lane][1] = splitmix64(seeds[lane]);
            sw[lane][2] = splitmix64(seeds[lane]);
            sw[lane][3] = splitmix64(seeds[lane]);
        }
        // Pack: s[word] = [ lane0.word, lane1.word, lane2.word, lane3.word ]
        for (int w = 0; w < 4; ++w) {
            s[w] = _mm256_set_epi64x(
                static_cast<long long>(sw[3][w]),
                static_cast<long long>(sw[2][w]),
                static_cast<long long>(sw[1][w]),
                static_cast<long long>(sw[0][w]));
        }
    }

    // Advance all four streams and return four random uint64_t.
    [[nodiscard]] __m256i operator()() noexcept {
        // xoshiro256++ output: rotl(s0+s3, 23) + s0
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
// Uses the IEEE-754 mantissa trick: OR the top 52 bits into the exponent+mantissa
// of a biased [1, 2) double, then subtract 1.0.  No integer→float conversion
// instruction needed; 52 bits of randomness (vs 53 scalar) is negligible.
inline __m256d bits_to_01_x4(__m256i x) noexcept {
    const __m256i mantissa_mask = _mm256_set1_epi64x(0x000FFFFFFFFFFFFFLL);
    const __m256i exponent_one  = _mm256_set1_epi64x(0x3FF0000000000000LL);
    // Keep top 52 bits of the random word as the mantissa of a [1,2) double.
    const __m256i bits = _mm256_or_si256(
        _mm256_and_si256(x, mantissa_mask),
        exponent_one);
    return _mm256_sub_pd(_mm256_castsi256_pd(bits), _mm256_set1_pd(1.0));
}

// Thread-local 4-wide engine seeded from std::random_device.
inline auto get_rng_x4() noexcept -> Xoshiro256pp_x4& {
    static thread_local Xoshiro256pp_x4 rng{std::random_device{}()};
    return rng;
}

// Fill `out[0..rows)` with uniform doubles in [0,1) using the 4-wide engine.
// Bulk-generates 4 values per step; the remainder is filled via scalar.
inline void fill_uniform_x4(double* __restrict__ out, std::size_t rows) noexcept {
    auto& rng4 = get_rng_x4();
    std::size_t i = 0;
    for (; i + 3 < rows; i += 4) {
        _mm256_storeu_pd(out + i, bits_to_01_x4(rng4()));
    }
    // Scalar tail
    auto& rng = get_rng();
    for (; i < rows; ++i) out[i] = bits_to_01(rng());
}

// Generate `rows` normally-distributed doubles into `out`.
// Uses the 4-wide engine to produce batches of 8 uniforms (4 pairs), then
// applies the Box-Muller transform 4 times in parallel via libmvec.
//
// Layout:  u1 = out[0..3], u2 = out[4..7]  → 4 (z0,z1) pairs → 8 normals.
inline void fill_normal_x4(double* __restrict__ out, std::size_t rows,
                            double mean, double stddev) noexcept {
    auto& rng4 = get_rng_x4();

    const __m256d two_pi = _mm256_set1_pd(2.0 * std::numbers::pi);
    const __m256d neg2   = _mm256_set1_pd(-2.0);
    const __m256d eps    = _mm256_set1_pd(1e-300);
    const __m256d vmean  = _mm256_set1_pd(mean);
    const __m256d vstd   = _mm256_set1_pd(stddev);

    std::size_t i = 0;
    for (; i + 7 < rows; i += 8) {
        // 4 × u1 and 4 × u2, each in (0, 1)
        const __m256d u1 = _mm256_add_pd(bits_to_01_x4(rng4()), eps);
        const __m256d u2 = bits_to_01_x4(rng4());

        // r = sqrt(-2 * log(u1))  — vectorized log then scalar sqrt
        const __m256d r = _mm256_sqrt_pd(
            _mm256_mul_pd(neg2, _ZGVdN4v_log(u1)));

        // theta = 2π * u2
        const __m256d theta = _mm256_mul_pd(two_pi, u2);

        // z0 = r * cos(theta),  z1 = r * sin(theta)
        const __m256d z0 = _mm256_add_pd(vmean,
            _mm256_mul_pd(vstd, _mm256_mul_pd(r, _ZGVdN4v_cos(theta))));
        const __m256d z1 = _mm256_add_pd(vmean,
            _mm256_mul_pd(vstd, _mm256_mul_pd(r, _ZGVdN4v_sin(theta))));

        // Interleave z0[0..3] and z1[0..3] into out[i..i+7]:
        //   out[i+0]=z0[0], out[i+1]=z1[0], out[i+2]=z0[1], out[i+3]=z1[1], …
        const __m256d lo = _mm256_unpacklo_pd(z0, z1); // [z0[0],z1[0],z0[2],z1[2]]
        const __m256d hi = _mm256_unpackhi_pd(z0, z1); // [z0[1],z1[1],z0[3],z1[3]]
        // Rearrange into output order: lo_lo, hi_lo, lo_hi, hi_hi
        _mm256_storeu_pd(out + i,     _mm256_permute2f128_pd(lo, hi, 0x20));
        _mm256_storeu_pd(out + i + 4, _mm256_permute2f128_pd(lo, hi, 0x31));
    }

    // Scalar tail (< 8 remaining elements)
    auto& rng = get_rng();
    for (; i + 1 < rows; i += 2) {
        const double u1 = bits_to_01(rng()) + 1e-300;
        const double u2 = bits_to_01(rng());
        double z0, z1;
        box_muller(u1, u2, z0, z1);
        out[i]     = mean + stddev * z0;
        out[i + 1] = mean + stddev * z1;
    }
    if (i < rows) {
        const double u1 = bits_to_01(rng()) + 1e-300;
        const double u2 = bits_to_01(rng());
        double z0, z1;
        box_muller(u1, u2, z0, z1);
        out[i] = mean + stddev * z0;
    }
}

#endif // __AVX2__

} // namespace ibex::runtime
