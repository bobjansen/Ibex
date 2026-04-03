#pragma once

// Zorro — high-performance xoshiro256++ with automatic SIMD dispatch.
//
// Single header. Copy into your project, compile with -mavx2 or -march=native.
//
//   #include "zorro/zorro.hpp"
//
//   zorro::Rng rng(42);
//   std::vector<double> u(1'000'000);
//   rng.fill_uniform(u.data(), u.size());                // [0, 1)
//   rng.fill_normal(u.data(), u.size());                 // N(0, 1)
//   rng.fill_exponential(u.data(), u.size());            // Exp(1)
//   rng.fill_bernoulli(u.data(), u.size(), 0.3);         // Bernoulli(0.3)
//   rng.fill_gamma(u.data(), u.size(), 2.0);             // Gamma(alpha, 1), alpha >= 1
//   rng.fill_student_t(u.data(), u.size(), 5.0);         // Student's t(nu)
//
// SIMD tier is selected at compile time:
//   -mavx512f -mavx512vl -mavx512dq  →  16-wide AVX-512 (2×8 interleaved)
//   -mavx2                            →   8-wide AVX2   (2×4 interleaved)
//   (neither)                         →   4-wide portable (compiler auto-vec)
//
// AMD Zen 4 runtime detection: integer-only kernels (uniform, bernoulli) use
// AVX-512 at full speed; FP-heavy kernels (normal) fall back to AVX2 because
// 512-bit sqrt/div serialize on the 256-bit datapath.
//
// Optional: #define ZORRO_USE_LIBMVEC before including, and link -lmvec -lm,
// for exact vectorized log in the libmvec-backed normal/gamma/Student-t paths
// and the AVX-512 exponential path (glibc only).
//
// Optional: #define ZORRO_EXACT_EXPONENTIAL_LOG before including to force the
// exact AVX2 exponential path as well. By default, AVX2 exponential uses the
// faster validated approximation.

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>

#if defined(__AVX2__) || defined(__AVX512F__) || defined(_MSC_VER)
#include <immintrin.h>
#endif
#if defined(__AVX512F__)
#if defined(_MSC_VER)
#include <intrin.h>
#elif defined(__GNUC__) || defined(__clang__)
#include <cpuid.h>
#endif
#endif

#ifdef ZORRO_USE_LIBMVEC
#ifdef __AVX2__
extern "C" __m256d _ZGVdN4v_log(__m256d) noexcept;
#endif
#ifdef __AVX512F__
extern "C" __m512d _ZGVeN8v_log(__m512d) noexcept;
#endif
#endif

namespace zorro {

// ─── Utilities
// ────────────────────────────────────────────────────────────────

inline auto splitmix64(std::uint64_t& state) noexcept -> std::uint64_t {
    state += 0x9e3779b97f4a7c15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

inline auto bits_to_01(std::uint64_t x) noexcept -> double {
    return static_cast<double>(x >> 11) * 0x1.0p-53;
}

inline auto bits_to_pm1(std::uint64_t x) noexcept -> double {
    return static_cast<double>(static_cast<std::int64_t>(x) >> 11) * 0x1.0p-52;
}

// ─── Scalar engine (UniformRandomBitGenerator)
// ────────────────────────────────

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

// ─── Portable multi-lane engines
// ──────────────────────────────────────────────

namespace detail {
inline constexpr std::uint64_t stream_offsets[4] = {
    0x0000000000000000ULL,
    0x9e3779b97f4a7c15ULL,
    0x6c62272e07bb0142ULL,
    0xd2a98b26625eee7bULL,
};
}  // namespace detail

struct Xoshiro256pp_x2 {
    std::uint64_t s[4][2];

    explicit Xoshiro256pp_x2(std::uint64_t seed) noexcept {
        for (int lane = 0; lane < 2; ++lane) {
            std::uint64_t lseed = seed ^ detail::stream_offsets[lane];
            s[0][lane] = splitmix64(lseed);
            s[1][lane] = splitmix64(lseed);
            s[2][lane] = splitmix64(lseed);
            s[3][lane] = splitmix64(lseed);
        }
    }

    [[nodiscard]] auto operator()() noexcept -> std::array<std::uint64_t, 2> {
        std::uint64_t result[2];
        for (int i = 0; i < 2; ++i)
            result[i] = rotl(s[0][i] + s[3][i], 23) + s[0][i];
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
            s[3][i] = rotl(s[3][i], 45);
        return {result[0], result[1]};
    }

   private:
    static constexpr auto rotl(std::uint64_t x, int k) noexcept -> std::uint64_t {
        return (x << k) | (x >> (64 - k));
    }
};

struct Xoshiro256pp_x4_portable {
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

    [[nodiscard]] auto operator()() noexcept -> std::array<std::uint64_t, 4> {
        std::uint64_t result[4];
        for (int i = 0; i < 4; ++i)
            result[i] = rotl(s[0][i] + s[3][i], 23) + s[0][i];
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
            s[3][i] = rotl(s[3][i], 45);
        return {result[0], result[1], result[2], result[3]};
    }

   private:
    static constexpr auto rotl(std::uint64_t x, int k) noexcept -> std::uint64_t {
        return (x << k) | (x >> (64 - k));
    }
};

// ─── Detail: seeding, state management
// ────────────────────────────────────────

namespace detail {

inline constexpr auto rotl64(std::uint64_t x, int k) noexcept -> std::uint64_t {
    return (x << k) | (x >> (64 - k));
}

inline void state_advance(std::uint64_t (&s)[4]) noexcept {
    const std::uint64_t t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotl64(s[3], 45);
}

// Advance state by 2^192 steps (for independent streams).
inline void long_jump(std::uint64_t (&s)[4]) noexcept {
    static constexpr std::uint64_t kCoeffs[4] = {
        0x76e15d3efefdcbbfULL,
        0xc5004e441c522fb3ULL,
        0x77710069854ee241ULL,
        0x39109bb02acbe635ULL,
    };
    std::uint64_t t[4] = {};
    for (auto coeff : kCoeffs) {
        for (int b = 0; b < 64; ++b) {
            if (coeff & (std::uint64_t{1} << b)) {
                t[0] ^= s[0];
                t[1] ^= s[1];
                t[2] ^= s[2];
                t[3] ^= s[3];
            }
            state_advance(s);
        }
    }
    s[0] = t[0];
    s[1] = t[1];
    s[2] = t[2];
    s[3] = t[3];
}

// Seed `n` independent lanes into SoA arrays. Lane 0 from splitmix64;
// each subsequent lane is 2^192 steps ahead via long_jump.
inline void seed_lanes(std::uint64_t seed, int n, std::uint64_t* s0, std::uint64_t* s1,
                       std::uint64_t* s2, std::uint64_t* s3) noexcept {
    std::uint64_t state[4] = {
        splitmix64(seed),
        splitmix64(seed),
        splitmix64(seed),
        splitmix64(seed),
    };
    for (int lane = 0; lane < n; ++lane) {
        s0[lane] = state[0];
        s1[lane] = state[1];
        s2[lane] = state[2];
        s3[lane] = state[3];
        if (lane + 1 < n)
            long_jump(state);
    }
}

inline auto next_pp(std::uint64_t (&s)[4]) noexcept -> std::uint64_t {
    const std::uint64_t r = rotl64(s[0] + s[3], 23) + s[0];
    state_advance(s);
    return r;
}

// ─── AVX2 detail
// ──────────────────────────────────────────────────────────────

#ifdef __AVX2__

template <int k>
inline auto rotl64_avx2(__m256i x) noexcept -> __m256i {
#ifdef __AVX512VL__
    return _mm256_rol_epi64(x, k);
#else
    return _mm256_or_si256(_mm256_slli_epi64(x, k), _mm256_srli_epi64(x, 64 - k));
#endif
}

inline auto next_x4_avx2(__m256i& s0, __m256i& s1, __m256i& s2, __m256i& s3) noexcept -> __m256i {
    const __m256i result = _mm256_add_epi64(rotl64_avx2<23>(_mm256_add_epi64(s0, s3)), s0);
    const __m256i t = _mm256_slli_epi64(s1, 17);
    s2 = _mm256_xor_si256(s2, s0);
    s3 = _mm256_xor_si256(s3, s1);
    s1 = _mm256_xor_si256(s1, s2);
    s0 = _mm256_xor_si256(s0, s3);
    s2 = _mm256_xor_si256(s2, t);
    s3 = rotl64_avx2<45>(s3);
    return result;
}

inline auto u64_to_uniform01_avx2(__m256i bits) noexcept -> __m256d {
    const __m256i exponent = _mm256_set1_epi64x(0x3ff0000000000000LL);
    const __m256d one = _mm256_set1_pd(1.0);
    const __m256i mantissa = _mm256_srli_epi64(bits, 12);
    return _mm256_sub_pd(_mm256_castsi256_pd(_mm256_or_si256(mantissa, exponent)), one);
}

inline auto u64_to_pm1_avx2(__m256i bits) noexcept -> __m256d {
    return _mm256_sub_pd(_mm256_mul_pd(u64_to_uniform01_avx2(bits), _mm256_set1_pd(2.0)),
                         _mm256_set1_pd(1.0));
}

inline auto floatbitmask64_avx2(__m256i bits) noexcept -> __m256d {
    const __m256i exponent = _mm256_set1_epi64x(0x3ff0000000000000ULL);
    const __m256i mantissa = _mm256_and_si256(
        bits, _mm256_set1_epi64x(static_cast<std::int64_t>(0x000fffffffffffffULL)));
    return _mm256_castsi256_pd(_mm256_or_si256(mantissa, exponent));
}

// Fast AVX2 approximation of -log(u) for u in (0, 1]. This is shared by the
// exponential path today and is shaped so other log-based transforms can reuse
// the same mantissa/exponent normalization later.
inline auto log2_3q_avx2(__m256d v, __m256d e) noexcept -> __m256d {
    const __m256d c0 = _mm256_set1_pd(0.22119417504560815);
    const __m256d c1 = _mm256_set1_pd(0.22007686931522777);
    const __m256d c2 = _mm256_set1_pd(0.26237080574885147);
    const __m256d c3 = _mm256_set1_pd(0.32059774779444955);
    const __m256d c4 = _mm256_set1_pd(0.41219859454853247);
    const __m256d c5 = _mm256_set1_pd(0.5770780162997059);
    const __m256d c6 = _mm256_set1_pd(0.9617966939260809);
    const __m256d scale = _mm256_set1_pd(2.8853900817779268);

    const __m256d m1 = _mm256_mul_pd(v, v);
#ifdef __FMA__
    const __m256d fma1 = _mm256_fmadd_pd(m1, c0, c1);
    const __m256d fma2 = _mm256_fmadd_pd(fma1, m1, c2);
    const __m256d fma3 = _mm256_fmadd_pd(fma2, m1, c3);
    const __m256d fma4 = _mm256_fmadd_pd(fma3, m1, c4);
    const __m256d fma5 = _mm256_fmadd_pd(fma4, m1, c5);
    const __m256d fma6 = _mm256_fmadd_pd(fma5, m1, c6);
#else
    const __m256d fma1 = _mm256_add_pd(_mm256_mul_pd(m1, c0), c1);
    const __m256d fma2 = _mm256_add_pd(_mm256_mul_pd(fma1, m1), c2);
    const __m256d fma3 = _mm256_add_pd(_mm256_mul_pd(fma2, m1), c3);
    const __m256d fma4 = _mm256_add_pd(_mm256_mul_pd(fma3, m1), c4);
    const __m256d fma5 = _mm256_add_pd(_mm256_mul_pd(fma4, m1), c5);
    const __m256d fma6 = _mm256_add_pd(_mm256_mul_pd(fma5, m1), c6);
#endif

    const __m256d m2 = _mm256_mul_pd(v, scale);
    const __m256d a1 = _mm256_add_pd(e, m2);
    const __m256d s1 = _mm256_sub_pd(e, a1);
    const __m256d a2 = _mm256_add_pd(m2, s1);
    const __m256d m3 = _mm256_mul_pd(v, m1);
#ifdef __FMA__
    return _mm256_fmadd_pd(fma6, m3, _mm256_add_pd(a1, a2));
#else
    return _mm256_add_pd(_mm256_mul_pd(fma6, m3), _mm256_add_pd(a1, a2));
#endif
}

inline auto exponent_words_to_log2_bias_avx2(__m256i exponent_words) noexcept -> __m256d {
    const __m128i lo = _mm256_castsi256_si128(exponent_words);
    const __m128i hi = _mm256_extracti128_si256(exponent_words, 1);
    const __m128i pair01 = _mm_unpacklo_epi32(lo, _mm_srli_si128(lo, 8));
    const __m128i pair23 = _mm_unpacklo_epi32(hi, _mm_srli_si128(hi, 8));
    const __m128i packed = _mm_unpacklo_epi64(pair01, pair23);
    const __m256d exponent = _mm256_cvtepi32_pd(packed);
    return _mm256_add_pd(exponent, _mm256_set1_pd(-1023.0 + 0.4150374992788438));
}

inline auto fast_neglog01_avx2(__m256d u) noexcept -> __m256d {
    const __m256i bits = _mm256_castpd_si256(u);
    const __m256i exponent_mask = _mm256_set1_epi64x(0x7ff0000000000000ULL);
    const __m256i mantissa_mask = _mm256_set1_epi64x(0x000fffffffffffffULL);
    const __m256i exponent_bits = _mm256_set1_epi64x(0x3ff0000000000000ULL);
    const __m256d four_thirds = _mm256_set1_pd(1.3333333333333333);
    const __m256d neg_ln2 = _mm256_set1_pd(-0.6931471805599453);
    const __m256i exponent_words = _mm256_srli_epi64(_mm256_and_si256(bits, exponent_mask), 52);

    const __m256d mantissa =
        _mm256_castsi256_pd(_mm256_or_si256(_mm256_and_si256(bits, mantissa_mask), exponent_bits));
    const __m256d v =
        _mm256_div_pd(_mm256_sub_pd(mantissa, four_thirds), _mm256_add_pd(mantissa, four_thirds));
    const __m256d log2_u = log2_3q_avx2(v, exponent_words_to_log2_bias_avx2(exponent_words));
    const __m256d neglog_u = _mm256_mul_pd(neg_ln2, log2_u);
    return _mm256_max_pd(neglog_u, _mm256_setzero_pd());
}

inline void seed_x8(std::uint64_t seed, std::uint64_t (&sa0)[4], std::uint64_t (&sa1)[4],
                    std::uint64_t (&sa2)[4], std::uint64_t (&sa3)[4], std::uint64_t (&sb0)[4],
                    std::uint64_t (&sb1)[4], std::uint64_t (&sb2)[4],
                    std::uint64_t (&sb3)[4]) noexcept {
    std::uint64_t all0[8], all1[8], all2[8], all3[8];
    seed_lanes(seed, 8, all0, all1, all2, all3);
    for (int i = 0; i < 4; ++i) {
        sa0[i] = all0[i];
        sa1[i] = all1[i];
        sa2[i] = all2[i];
        sa3[i] = all3[i];
        sb0[i] = all0[i + 4];
        sb1[i] = all1[i + 4];
        sb2[i] = all2[i + 4];
        sb3[i] = all3[i + 4];
    }
}

#endif  // __AVX2__

// ─── AVX-512 detail
// ───────────────────────────────────────────────────────────

#ifdef __AVX512F__

inline auto cpu_is_amd() noexcept -> bool {
    static const bool is_amd = [] {
        unsigned eax, ebx, ecx, edx;
        __cpuid(0, eax, ebx, ecx, edx);
        return ebx == 0x68747541u && edx == 0x69746e65u && ecx == 0x444d4163u;
    }();
    return is_amd;
}

template <int k>
inline auto rotl64_avx512(__m512i x) noexcept -> __m512i {
    return _mm512_rol_epi64(x, k);
}

inline auto next_x8_avx512(__m512i& s0, __m512i& s1, __m512i& s2, __m512i& s3) noexcept -> __m512i {
    const __m512i result = _mm512_add_epi64(rotl64_avx512<23>(_mm512_add_epi64(s0, s3)), s0);
    const __m512i t = _mm512_slli_epi64(s1, 17);
    s2 = _mm512_xor_si512(s2, s0);
    s3 = _mm512_xor_si512(s3, s1);
    s1 = _mm512_xor_si512(s1, s2);
    s0 = _mm512_xor_si512(s0, s3);
    s2 = _mm512_xor_si512(s2, t);
    s3 = rotl64_avx512<45>(s3);
    return result;
}

inline auto u64_to_uniform01_avx512(__m512i bits) noexcept -> __m512d {
    const __m512i exponent = _mm512_set1_epi64(0x3ff0000000000000LL);
    const __m512d one = _mm512_set1_pd(1.0);
    const __m512i mantissa = _mm512_srli_epi64(bits, 12);
    return _mm512_sub_pd(_mm512_castsi512_pd(_mm512_or_si512(mantissa, exponent)), one);
}

inline auto u64_to_pm1_avx512(__m512i bits) noexcept -> __m512d {
    return _mm512_sub_pd(_mm512_mul_pd(u64_to_uniform01_avx512(bits), _mm512_set1_pd(2.0)),
                         _mm512_set1_pd(1.0));
}

inline void seed_x16(std::uint64_t seed, std::uint64_t (&sa0)[8], std::uint64_t (&sa1)[8],
                     std::uint64_t (&sa2)[8], std::uint64_t (&sa3)[8], std::uint64_t (&sb0)[8],
                     std::uint64_t (&sb1)[8], std::uint64_t (&sb2)[8],
                     std::uint64_t (&sb3)[8]) noexcept {
    std::uint64_t all0[16], all1[16], all2[16], all3[16];
    seed_lanes(seed, 16, all0, all1, all2, all3);
    for (int i = 0; i < 8; ++i) {
        sa0[i] = all0[i];
        sa1[i] = all1[i];
        sa2[i] = all2[i];
        sa3[i] = all3[i];
        sb0[i] = all0[i + 8];
        sb1[i] = all1[i + 8];
        sb2[i] = all2[i + 8];
        sb3[i] = all3[i + 8];
    }
}

#endif  // __AVX512F__

}  // namespace detail

// ─── Rng: stateful batch generator with automatic SIMD dispatch
// ───────────────
//
// Internally manages 2 interleaved groups of SIMD-width lanes for maximum ILP.
// The SIMD tier is selected at compile time; the AMD Zen 4 workaround for
// FP-heavy kernels is applied at runtime.

class alignas(64) Rng {
   public:
    explicit Rng(std::uint64_t seed) noexcept {
#if defined(__AVX512F__)
        detail::seed_x16(seed, sa_[0], sa_[1], sa_[2], sa_[3], sb_[0], sb_[1], sb_[2], sb_[3]);
#elif defined(__AVX2__)
        detail::seed_x8(seed, sa_[0], sa_[1], sa_[2], sa_[3], sb_[0], sb_[1], sb_[2], sb_[3]);
#else
        detail::seed_lanes(seed, 4, s_[0], s_[1], s_[2], s_[3]);
#endif
    }

    // ── Uniform [low, high) ──────────────────────────────────────────────────

    void fill_uniform(double* __restrict out, std::size_t count, double low = 0.0,
                      double high = 1.0) noexcept {
#if defined(__AVX512F__)
        const double range = high - low;
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx512();
        const __m512d vlo = _mm512_set1_pd(low);
        const __m512d vrng = _mm512_set1_pd(range);
        std::size_t i = 0;
        while (i + 16 <= count) {
            _mm512_storeu_pd(
                out + i,
                _mm512_add_pd(vlo,
                              _mm512_mul_pd(vrng, detail::u64_to_uniform01_avx512(
                                                      detail::next_x8_avx512(a0, a1, a2, a3)))));
            _mm512_storeu_pd(
                out + i + 8,
                _mm512_add_pd(vlo,
                              _mm512_mul_pd(vrng, detail::u64_to_uniform01_avx512(
                                                      detail::next_x8_avx512(b0, b1, b2, b3)))));
            i += 16;
        }
        if (i + 8 <= count) {
            _mm512_storeu_pd(
                out + i,
                _mm512_add_pd(vlo,
                              _mm512_mul_pd(vrng, detail::u64_to_uniform01_avx512(
                                                      detail::next_x8_avx512(a0, a1, a2, a3)))));
            i += 8;
        }
        if (i < count) {
            alignas(64) double tail[8];
            _mm512_storeu_pd(
                tail, _mm512_add_pd(
                          vlo, _mm512_mul_pd(vrng, detail::u64_to_uniform01_avx512(
                                                       detail::next_x8_avx512(a0, a1, a2, a3)))));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
        store_avx512(a0, a1, a2, a3, b0, b1, b2, b3);

#elif defined(__AVX2__)
        const double range = high - low;
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx2();
        const __m256d vlo = _mm256_set1_pd(low);
        const __m256d vrng = _mm256_set1_pd(range);
        std::size_t i = 0;
        while (i + 8 <= count) {
            _mm256_storeu_pd(
                out + i,
                _mm256_add_pd(vlo, _mm256_mul_pd(vrng, detail::u64_to_uniform01_avx2(
                                                           detail::next_x4_avx2(a0, a1, a2, a3)))));
            _mm256_storeu_pd(
                out + i + 4,
                _mm256_add_pd(vlo, _mm256_mul_pd(vrng, detail::u64_to_uniform01_avx2(
                                                           detail::next_x4_avx2(b0, b1, b2, b3)))));
            i += 8;
        }
        if (i + 4 <= count) {
            _mm256_storeu_pd(
                out + i,
                _mm256_add_pd(vlo, _mm256_mul_pd(vrng, detail::u64_to_uniform01_avx2(
                                                           detail::next_x4_avx2(a0, a1, a2, a3)))));
            i += 4;
        }
        if (i < count) {
            alignas(32) double tail[4];
            _mm256_storeu_pd(
                tail,
                _mm256_add_pd(vlo, _mm256_mul_pd(vrng, detail::u64_to_uniform01_avx2(
                                                           detail::next_x4_avx2(a0, a1, a2, a3)))));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
        store_avx2(a0, a1, a2, a3, b0, b1, b2, b3);

#else
        fill_uniform_portable(out, count, low, high);
#endif
    }

    // ── Normal N(mean, stddev) — Marsaglia polar method ──────────────────────

    void fill_normal(double* __restrict out, std::size_t count, double mean = 0.0,
                     double stddev = 1.0) noexcept {
#if defined(__AVX512F__) && defined(ZORRO_USE_LIBMVEC)
        if (!detail::cpu_is_amd()) {
            fill_normal_avx512_vecpolar(out, count, mean, stddev);
            return;
        }
        // AMD: fall through to AVX2
        fill_normal_avx2(out, count, mean, stddev);
#elif defined(__AVX2__)
        fill_normal_avx2(out, count, mean, stddev);
#else
        fill_normal_portable(out, count, mean, stddev);
#endif
    }

    // ── Exponential Exp(lambda) ──────────────────────────────────────────────

    void fill_exponential(double* __restrict out, std::size_t count, double lambda = 1.0) noexcept {
        const double inv_lambda = 1.0 / lambda;
#if defined(__AVX512F__) && defined(ZORRO_USE_LIBMVEC)
        fill_exponential_avx512_veclog(out, count, inv_lambda);
#elif defined(__AVX2__)
        fill_exponential_avx2(out, count, inv_lambda);
#else
        fill_exponential_portable(out, count, inv_lambda);
#endif
    }

    // ── Bernoulli(p) → double (0.0 / 1.0) ───────────────────────────────────

    void fill_bernoulli(double* __restrict out, std::size_t count, double p) noexcept {
        const auto threshold = static_cast<std::uint64_t>(p * 0x1.0p64);
#if defined(__AVX512F__)
        fill_bernoulli_avx512(out, count, threshold);
#elif defined(__AVX2__)
        fill_bernoulli_avx2(out, count, threshold);
#else
        fill_bernoulli_portable(out, count, threshold);
#endif
    }

    // ── Gamma(alpha, 1) — Marsaglia & Tsang, alpha >= 1 ──────────────────────

    void fill_gamma(double* __restrict out, std::size_t count, double alpha) noexcept {
#ifdef __AVX2__
        fill_gamma_avx2(out, count, alpha);
#else
        fill_gamma_portable(out, count, alpha);
#endif
    }

    // ── Student's t(nu) = N(0,1) / sqrt(Gamma(nu/2, 1) / (nu/2)) ─────────────

    void fill_student_t(double* __restrict out, std::size_t count, double nu) noexcept {
#ifdef __AVX2__
        fill_student_t_avx2(out, count, nu);
#else
        fill_student_t_portable(out, count, nu);
#endif
    }

   private:
    // ── State storage ────────────────────────────────────────────────────────
    //
    // Two interleaved groups (a, b) of SIMD-width lanes.
    // sa_[word][lane], sb_[word][lane] — Structure-of-Arrays for direct
    // SIMD load/store.  For the portable path, s_[word][lane] with 4 lanes.

#if defined(__AVX512F__)
    static constexpr int kLanes = 8;
    alignas(64) std::uint64_t sa_[4][8];
    alignas(64) std::uint64_t sb_[4][8];
#elif defined(__AVX2__)
    static constexpr int kLanes = 4;
    alignas(32) std::uint64_t sa_[4][4];
    alignas(32) std::uint64_t sb_[4][4];
#else
    std::uint64_t s_[4][4];
#endif

    // ── Load / store helpers ─────────────────────────────────────────────────

#ifdef __AVX2__
    struct Avx2State {
        __m256i a0, a1, a2, a3, b0, b1, b2, b3;
    };

    [[nodiscard]] auto load_avx2() const noexcept -> Avx2State {
        return {
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sa_[0])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sa_[1])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sa_[2])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sa_[3])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sb_[0])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sb_[1])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sb_[2])),
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sb_[3])),
        };
    }

    void store_avx2(__m256i a0, __m256i a1, __m256i a2, __m256i a3, __m256i b0, __m256i b1,
                    __m256i b2, __m256i b3) noexcept {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sa_[0]), a0);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sa_[1]), a1);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sa_[2]), a2);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sa_[3]), a3);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sb_[0]), b0);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sb_[1]), b1);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sb_[2]), b2);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sb_[3]), b3);
    }
#endif

#ifdef __AVX512F__
    struct Avx512State {
        __m512i a0, a1, a2, a3, b0, b1, b2, b3;
    };

    [[nodiscard]] auto load_avx512() const noexcept -> Avx512State {
        return {
            _mm512_loadu_si512(sa_[0]), _mm512_loadu_si512(sa_[1]), _mm512_loadu_si512(sa_[2]),
            _mm512_loadu_si512(sa_[3]), _mm512_loadu_si512(sb_[0]), _mm512_loadu_si512(sb_[1]),
            _mm512_loadu_si512(sb_[2]), _mm512_loadu_si512(sb_[3]),
        };
    }

    void store_avx512(__m512i a0, __m512i a1, __m512i a2, __m512i a3, __m512i b0, __m512i b1,
                      __m512i b2, __m512i b3) noexcept {
        _mm512_storeu_si512(sa_[0], a0);
        _mm512_storeu_si512(sa_[1], a1);
        _mm512_storeu_si512(sa_[2], a2);
        _mm512_storeu_si512(sa_[3], a3);
        _mm512_storeu_si512(sb_[0], b0);
        _mm512_storeu_si512(sb_[1], b1);
        _mm512_storeu_si512(sb_[2], b2);
        _mm512_storeu_si512(sb_[3], b3);
    }
#endif

    // ── Portable (no SIMD intrinsics) ────────────────────────────────────────

#ifndef __AVX2__
    void fill_uniform_portable(double* __restrict out, std::size_t count, double low,
                               double high) noexcept {
        const double range = high - low;
        std::uint64_t result[4];
        std::size_t i = 0;
        while (i + 4 <= count) {
            next_x4_portable(result);
            out[i] = low + bits_to_01(result[0]) * range;
            out[i + 1] = low + bits_to_01(result[1]) * range;
            out[i + 2] = low + bits_to_01(result[2]) * range;
            out[i + 3] = low + bits_to_01(result[3]) * range;
            i += 4;
        }
        if (i < count) {
            next_x4_portable(result);
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = low + bits_to_01(result[lane]) * range;
        }
    }

    void fill_normal_portable(double* __restrict out, std::size_t count, double mean,
                              double stddev) noexcept {
        std::uint64_t result[4];
        std::size_t i = 0;
        while (i < count) {
            next_x4_portable(result);
            for (int lane = 0; lane < 4 && i < count; lane += 2) {
                const double u1 = bits_to_pm1(result[lane]);
                const double u2 = bits_to_pm1(result[lane + 1]);
                const double sq = u1 * u1 + u2 * u2;
                if (sq >= 1.0 || sq == 0.0) [[unlikely]]
                    continue;
                const double scale = std::sqrt(-2.0 * std::log(sq) / sq);
                out[i++] = mean + stddev * u1 * scale;
                if (i < count)
                    out[i++] = mean + stddev * u2 * scale;
            }
        }
    }

    void fill_exponential_portable(double* __restrict out, std::size_t count,
                                   double inv_lambda) noexcept {
        std::uint64_t result[4];
        std::size_t i = 0;
        while (i + 4 <= count) {
            next_x4_portable(result);
            out[i] = -std::log(bits_to_01(result[0]) + 1e-300) * inv_lambda;
            out[i + 1] = -std::log(bits_to_01(result[1]) + 1e-300) * inv_lambda;
            out[i + 2] = -std::log(bits_to_01(result[2]) + 1e-300) * inv_lambda;
            out[i + 3] = -std::log(bits_to_01(result[3]) + 1e-300) * inv_lambda;
            i += 4;
        }
        if (i < count) {
            next_x4_portable(result);
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = -std::log(bits_to_01(result[lane]) + 1e-300) * inv_lambda;
        }
    }

    void fill_bernoulli_portable(double* __restrict out, std::size_t count,
                                 std::uint64_t threshold) noexcept {
        std::uint64_t result[4];
        std::size_t i = 0;
        while (i + 4 <= count) {
            next_x4_portable(result);
            out[i] = result[0] < threshold ? 1.0 : 0.0;
            out[i + 1] = result[1] < threshold ? 1.0 : 0.0;
            out[i + 2] = result[2] < threshold ? 1.0 : 0.0;
            out[i + 3] = result[3] < threshold ? 1.0 : 0.0;
            i += 4;
        }
        if (i < count) {
            next_x4_portable(result);
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = result[lane] < threshold ? 1.0 : 0.0;
        }
    }

    void fill_gamma_portable(double* __restrict out, std::size_t count, double alpha) noexcept {
        const double d = alpha - 1.0 / 3.0;
        const double c = 1.0 / std::sqrt(9.0 * d);
        std::uint64_t result[4];
        std::size_t i = 0;
        while (i < count) {
            next_x4_portable(result);
            const double u1 = bits_to_pm1(result[0]);
            const double u2 = bits_to_pm1(result[1]);
            const double sq = u1 * u1 + u2 * u2;
            if (sq >= 1.0 || sq == 0.0)
                continue;
            const double x = u1 * std::sqrt(-2.0 * std::log(sq) / sq);
            const double vr = 1.0 + c * x;
            if (vr <= 0.0)
                continue;
            const double v = vr * vr * vr;
            const double u = bits_to_01(result[2]);
            const double x2 = x * x;
            if (u < 1.0 - 0.0331 * x2 * x2) {
                out[i++] = d * v;
                continue;
            }
            if (std::log(u) < 0.5 * x2 + d * (1.0 - v + std::log(v))) {
                out[i++] = d * v;
            }
        }
    }

    void fill_student_t_portable(double* __restrict out, std::size_t count, double nu) noexcept {
        static constexpr std::size_t kChunk = 128;
        double z_buf[kChunk];
        double g_buf[kChunk];
        const double inv_half_nu = 2.0 / nu;
        const double shape = nu / 2.0;
        std::size_t i = 0;
        while (i < count) {
            const std::size_t chunk = count - i < kChunk ? count - i : kChunk;
            fill_normal_portable(z_buf, chunk, 0.0, 1.0);
            fill_gamma_portable(g_buf, chunk, shape);
            for (std::size_t k = 0; k < chunk; ++k)
                out[i + k] = z_buf[k] / std::sqrt(g_buf[k] * inv_half_nu);
            i += chunk;
        }
    }

    void next_x4_portable(std::uint64_t* result) noexcept {
        for (int lane = 0; lane < 4; ++lane)
            result[lane] = detail::rotl64(s_[0][lane] + s_[3][lane], 23) + s_[0][lane];
        std::uint64_t t[4];
        for (int lane = 0; lane < 4; ++lane)
            t[lane] = s_[1][lane] << 17;
        for (int lane = 0; lane < 4; ++lane)
            s_[2][lane] ^= s_[0][lane];
        for (int lane = 0; lane < 4; ++lane)
            s_[3][lane] ^= s_[1][lane];
        for (int lane = 0; lane < 4; ++lane)
            s_[1][lane] ^= s_[2][lane];
        for (int lane = 0; lane < 4; ++lane)
            s_[0][lane] ^= s_[3][lane];
        for (int lane = 0; lane < 4; ++lane)
            s_[2][lane] ^= t[lane];
        for (int lane = 0; lane < 4; ++lane)
            s_[3][lane] = detail::rotl64(s_[3][lane], 45);
    }
#endif  // !__AVX2__

    // ── AVX2 distribution kernels ────────────────────────────────────────────

#ifdef __AVX2__
    void fill_normal_avx2(double* __restrict out, std::size_t count, double mean,
                          double stddev) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx2();
#if defined(ZORRO_USE_LIBMVEC) && \
    !(defined(__GNUC__) && !defined(__clang__) && defined(__SANITIZE_ADDRESS__))
        // Fully vectorized polar with libmvec log
        const __m256d one = _mm256_set1_pd(1.0);
        const __m256d zero = _mm256_setzero_pd();
        const __m256d neg2 = _mm256_set1_pd(-2.0);
        const __m256d safe_val = _mm256_set1_pd(0.5);
        const __m256d vmean = _mm256_set1_pd(mean);
        const __m256d vstd = _mm256_set1_pd(stddev);

        std::size_t i = 0;
        while (i < count) {
            // Group a
            {
                const __m256d u1 = detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3));
                const __m256d u2 = detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3));
                const __m256d s = _mm256_add_pd(_mm256_mul_pd(u1, u1), _mm256_mul_pd(u2, u2));
                const __m256d accept = _mm256_and_pd(_mm256_cmp_pd(s, one, _CMP_LT_OQ),
                                                     _mm256_cmp_pd(s, zero, _CMP_GT_OQ));
                const int mask_bits = _mm256_movemask_pd(accept);
                if (mask_bits) {
                    const __m256d safe_s = _mm256_blendv_pd(safe_val, s, accept);
                    const __m256d factor = _mm256_sqrt_pd(
                        _mm256_div_pd(_mm256_mul_pd(neg2, _ZGVdN4v_log(safe_s)), safe_s));
                    const __m256d n1 =
                        _mm256_add_pd(vmean, _mm256_mul_pd(vstd, _mm256_mul_pd(u1, factor)));
                    const __m256d n2 =
                        _mm256_add_pd(vmean, _mm256_mul_pd(vstd, _mm256_mul_pd(u2, factor)));
                    alignas(32) double n1v[4], n2v[4];
                    _mm256_storeu_pd(n1v, n1);
                    _mm256_storeu_pd(n2v, n2);
                    for (int lane = 0; lane < 4 && i < count; ++lane) {
                        if (mask_bits & (1 << lane)) {
                            out[i++] = n1v[lane];
                            if (i < count)
                                out[i++] = n2v[lane];
                        }
                    }
                }
            }
            if (i >= count)
                break;
            // Group b
            {
                const __m256d u1 = detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3));
                const __m256d u2 = detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3));
                const __m256d s = _mm256_add_pd(_mm256_mul_pd(u1, u1), _mm256_mul_pd(u2, u2));
                const __m256d accept = _mm256_and_pd(_mm256_cmp_pd(s, one, _CMP_LT_OQ),
                                                     _mm256_cmp_pd(s, zero, _CMP_GT_OQ));
                const int mask_bits = _mm256_movemask_pd(accept);
                if (mask_bits) {
                    const __m256d safe_s = _mm256_blendv_pd(safe_val, s, accept);
                    const __m256d factor = _mm256_sqrt_pd(
                        _mm256_div_pd(_mm256_mul_pd(neg2, _ZGVdN4v_log(safe_s)), safe_s));
                    const __m256d n1 =
                        _mm256_add_pd(vmean, _mm256_mul_pd(vstd, _mm256_mul_pd(u1, factor)));
                    const __m256d n2 =
                        _mm256_add_pd(vmean, _mm256_mul_pd(vstd, _mm256_mul_pd(u2, factor)));
                    alignas(32) double n1v[4], n2v[4];
                    _mm256_storeu_pd(n1v, n1);
                    _mm256_storeu_pd(n2v, n2);
                    for (int lane = 0; lane < 4 && i < count; ++lane) {
                        if (mask_bits & (1 << lane)) {
                            out[i++] = n1v[lane];
                            if (i < count)
                                out[i++] = n2v[lane];
                        }
                    }
                }
            }
        }
#else
        // SIMD RNG + scalar polar (no libmvec dependency)
        alignas(32) double tmp[4];
        std::size_t i = 0;
        while (i < count) {
            // Group a: generate 4 pairs
            _mm256_storeu_pd(tmp, detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
            double u1a[4];
            std::memcpy(u1a, tmp, 32);
            _mm256_storeu_pd(tmp, detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
            for (int lane = 0; lane < 4 && i < count; ++lane) {
                const double u1 = u1a[lane], u2 = tmp[lane];
                const double sq = u1 * u1 + u2 * u2;
                if (sq >= 1.0 || sq == 0.0) [[unlikely]]
                    continue;
                const double scale = std::sqrt(-2.0 * std::log(sq) / sq);
                out[i++] = mean + stddev * u1 * scale;
                if (i < count)
                    out[i++] = mean + stddev * u2 * scale;
            }
            if (i >= count)
                break;
            // Group b: generate 4 pairs
            _mm256_storeu_pd(tmp, detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3)));
            std::memcpy(u1a, tmp, 32);
            _mm256_storeu_pd(tmp, detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3)));
            for (int lane = 0; lane < 4 && i < count; ++lane) {
                const double u1 = u1a[lane], u2 = tmp[lane];
                const double sq = u1 * u1 + u2 * u2;
                if (sq >= 1.0 || sq == 0.0) [[unlikely]]
                    continue;
                const double scale = std::sqrt(-2.0 * std::log(sq) / sq);
                out[i++] = mean + stddev * u1 * scale;
                if (i < count)
                    out[i++] = mean + stddev * u2 * scale;
            }
        }
#endif  // ZORRO_USE_LIBMVEC && !(__GNUC__ && __SANITIZE_ADDRESS__)
        store_avx2(a0, a1, a2, a3, b0, b1, b2, b3);
    }

    void fill_exponential_avx2(double* __restrict out, std::size_t count,
                               double inv_lambda) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx2();
#if defined(ZORRO_USE_LIBMVEC) && defined(ZORRO_EXACT_EXPONENTIAL_LOG)
        const __m256d neg_inv = _mm256_set1_pd(-inv_lambda);
        std::size_t i = 0;
        while (i + 8 <= count) {
            _mm256_storeu_pd(out + i,
                             _mm256_mul_pd(neg_inv, _ZGVdN4v_log(detail::u64_to_uniform01_avx2(
                                                        detail::next_x4_avx2(a0, a1, a2, a3)))));
            _mm256_storeu_pd(out + i + 4,
                             _mm256_mul_pd(neg_inv, _ZGVdN4v_log(detail::u64_to_uniform01_avx2(
                                                        detail::next_x4_avx2(b0, b1, b2, b3)))));
            i += 8;
        }
        while (i + 4 <= count) {
            _mm256_storeu_pd(out + i,
                             _mm256_mul_pd(neg_inv, _ZGVdN4v_log(detail::u64_to_uniform01_avx2(
                                                        detail::next_x4_avx2(a0, a1, a2, a3)))));
            i += 4;
        }
        if (i < count) {
            alignas(32) double tail[4];
            _mm256_storeu_pd(tail,
                             _mm256_mul_pd(neg_inv, _ZGVdN4v_log(detail::u64_to_uniform01_avx2(
                                                        detail::next_x4_avx2(a0, a1, a2, a3)))));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
#else
        const __m256d one = _mm256_set1_pd(1.0);
        const __m256d inv = _mm256_set1_pd(inv_lambda);
        // The approximation is keyed to u in (0, 1], so we generate Exp(1) via
        // -log(1 - u) instead of -log(u). The distribution is identical.
        std::size_t i = 0;
        while (i + 8 <= count) {
            _mm256_storeu_pd(
                out + i, _mm256_mul_pd(inv, detail::fast_neglog01_avx2(_mm256_sub_pd(
                                                one, detail::u64_to_uniform01_avx2(
                                                         detail::next_x4_avx2(a0, a1, a2, a3))))));
            _mm256_storeu_pd(
                out + i + 4,
                _mm256_mul_pd(inv, detail::fast_neglog01_avx2(_mm256_sub_pd(
                                       one, detail::u64_to_uniform01_avx2(
                                                detail::next_x4_avx2(b0, b1, b2, b3))))));
            i += 8;
        }
        while (i + 4 <= count) {
            _mm256_storeu_pd(
                out + i, _mm256_mul_pd(inv, detail::fast_neglog01_avx2(_mm256_sub_pd(
                                                one, detail::u64_to_uniform01_avx2(
                                                         detail::next_x4_avx2(a0, a1, a2, a3))))));
            i += 4;
        }
        if (i < count) {
            alignas(32) double tail[4];
            _mm256_storeu_pd(
                tail, _mm256_mul_pd(inv, detail::fast_neglog01_avx2(_mm256_sub_pd(
                                             one, detail::u64_to_uniform01_avx2(
                                                      detail::next_x4_avx2(a0, a1, a2, a3))))));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
#endif
        store_avx2(a0, a1, a2, a3, b0, b1, b2, b3);
    }

    void fill_bernoulli_avx2(double* __restrict out, std::size_t count,
                             std::uint64_t threshold) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx2();
        const __m256i sign_flip =
            _mm256_set1_epi64x(static_cast<std::int64_t>(0x8000000000000000ULL));
        const __m256i thresh_vec =
            _mm256_set1_epi64x(static_cast<std::int64_t>(threshold ^ 0x8000000000000000ULL));
        const __m256d one_d = _mm256_set1_pd(1.0);

        std::size_t i = 0;
        while (i + 8 <= count) {
            const __m256i ra = detail::next_x4_avx2(a0, a1, a2, a3);
            const __m256i rb = detail::next_x4_avx2(b0, b1, b2, b3);
            _mm256_storeu_pd(out + i,
                             _mm256_and_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(
                                               thresh_vec, _mm256_xor_si256(ra, sign_flip))),
                                           one_d));
            _mm256_storeu_pd(out + i + 4,
                             _mm256_and_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(
                                               thresh_vec, _mm256_xor_si256(rb, sign_flip))),
                                           one_d));
            i += 8;
        }
        while (i + 4 <= count) {
            const __m256i ra = detail::next_x4_avx2(a0, a1, a2, a3);
            _mm256_storeu_pd(out + i,
                             _mm256_and_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(
                                               thresh_vec, _mm256_xor_si256(ra, sign_flip))),
                                           one_d));
            i += 4;
        }
        if (i < count) {
            alignas(32) double tail[4];
            const __m256i ra = detail::next_x4_avx2(a0, a1, a2, a3);
            _mm256_storeu_pd(tail, _mm256_and_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(
                                                     thresh_vec, _mm256_xor_si256(ra, sign_flip))),
                                                 one_d));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
        store_avx2(a0, a1, a2, a3, b0, b1, b2, b3);
    }

    // ── Gamma (two-phase: SIMD polar → N(0,1) buffer; MT acceptance) ──────────

    void fill_gamma_avx2(double* __restrict out, std::size_t count, double alpha) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx2();
        const double d = alpha - 1.0 / 3.0;
        const double c = 1.0 / std::sqrt(9.0 * d);

        static constexpr int kBuf = 64;
        alignas(32) double buf_x[kBuf];

#ifdef ZORRO_USE_LIBMVEC
        const __m256d one = _mm256_set1_pd(1.0);
        const __m256d zero = _mm256_setzero_pd();
        const __m256d neg2 = _mm256_set1_pd(-2.0);
        const __m256d half = _mm256_set1_pd(0.5);
        const __m256d safe_val = _mm256_set1_pd(0.5);
        const __m256d d_vec = _mm256_set1_pd(d);
        const __m256d c_vec = _mm256_set1_pd(c);
        const __m256d mt_coeff = _mm256_set1_pd(0.0331);

        std::size_t i = 0;
        while (i < count) {
            // Phase 1: vectorized polar → buf_x
            int n = 0;
            while (n < kBuf) {
                // Group a
                {
                    const __m256d u1 =
                        detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3));
                    const __m256d u2 =
                        detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3));
                    const __m256d s = _mm256_add_pd(_mm256_mul_pd(u1, u1), _mm256_mul_pd(u2, u2));
                    const __m256d acc = _mm256_and_pd(_mm256_cmp_pd(s, one, _CMP_LT_OQ),
                                                      _mm256_cmp_pd(s, zero, _CMP_GT_OQ));
                    const int bits = _mm256_movemask_pd(acc);
                    if (bits) {
                        const __m256d sf = _mm256_blendv_pd(safe_val, s, acc);
                        const __m256d fac = _mm256_sqrt_pd(
                            _mm256_div_pd(_mm256_mul_pd(neg2, _ZGVdN4v_log(sf)), sf));
                        alignas(32) double xa[4], xa2[4];
                        _mm256_storeu_pd(xa, _mm256_mul_pd(u1, fac));
                        _mm256_storeu_pd(xa2, _mm256_mul_pd(u2, fac));
                        for (int lane = 0; lane < 4 && n < kBuf; ++lane) {
                            if (bits & (1 << lane)) {
                                buf_x[n++] = xa[lane];
                                if (n < kBuf)
                                    buf_x[n++] = xa2[lane];
                            }
                        }
                    }
                }
                // Group b
                {
                    const __m256d u1 =
                        detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3));
                    const __m256d u2 =
                        detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3));
                    const __m256d s = _mm256_add_pd(_mm256_mul_pd(u1, u1), _mm256_mul_pd(u2, u2));
                    const __m256d acc = _mm256_and_pd(_mm256_cmp_pd(s, one, _CMP_LT_OQ),
                                                      _mm256_cmp_pd(s, zero, _CMP_GT_OQ));
                    const int bits = _mm256_movemask_pd(acc);
                    if (bits) {
                        const __m256d sf = _mm256_blendv_pd(safe_val, s, acc);
                        const __m256d fac = _mm256_sqrt_pd(
                            _mm256_div_pd(_mm256_mul_pd(neg2, _ZGVdN4v_log(sf)), sf));
                        alignas(32) double xb[4], xb2[4];
                        _mm256_storeu_pd(xb, _mm256_mul_pd(u1, fac));
                        _mm256_storeu_pd(xb2, _mm256_mul_pd(u2, fac));
                        for (int lane = 0; lane < 4 && n < kBuf; ++lane) {
                            if (bits & (1 << lane)) {
                                buf_x[n++] = xb[lane];
                                if (n < kBuf)
                                    buf_x[n++] = xb2[lane];
                            }
                        }
                    }
                }
            }
            // Phase 2: vectorized MT acceptance using group a for uniforms
            int k = 0;
            for (; k + 4 <= n && i < count; k += 4) {
                const __m256d x = _mm256_load_pd(buf_x + k);
                const __m256d u =
                    detail::u64_to_uniform01_avx2(detail::next_x4_avx2(a0, a1, a2, a3));
                const __m256d vr = _mm256_add_pd(one, _mm256_mul_pd(c_vec, x));
                const __m256d vr_pos = _mm256_cmp_pd(vr, zero, _CMP_GT_OQ);
                const __m256d sv = _mm256_blendv_pd(one, vr, vr_pos);
                const __m256d v = _mm256_mul_pd(sv, _mm256_mul_pd(sv, sv));
                const __m256d x2 = _mm256_mul_pd(x, x);
                const __m256d x4 = _mm256_mul_pd(x2, x2);
                const __m256d fast_thresh = _mm256_sub_pd(one, _mm256_mul_pd(mt_coeff, x4));
                const __m256d fast_acc = _mm256_cmp_pd(u, fast_thresh, _CMP_LT_OQ);
                const __m256d log_u = _ZGVdN4v_log(u);
                const __m256d log_v = _ZGVdN4v_log(v);
                const __m256d slow_rhs = _mm256_add_pd(
                    _mm256_mul_pd(half, x2),
                    _mm256_mul_pd(d_vec, _mm256_add_pd(_mm256_sub_pd(one, v), log_v)));
                const __m256d slow_acc = _mm256_cmp_pd(log_u, slow_rhs, _CMP_LT_OQ);
                const __m256d accept = _mm256_and_pd(vr_pos, _mm256_or_pd(fast_acc, slow_acc));
                const int accept_bits = _mm256_movemask_pd(accept);
                if (accept_bits) {
                    alignas(32) double gv[4];
                    _mm256_storeu_pd(gv, _mm256_mul_pd(d_vec, v));
                    for (int lane = 0; lane < 4 && i < count; ++lane) {
                        if (accept_bits & (1 << lane))
                            out[i++] = gv[lane];
                    }
                }
            }
            // Scalar tail
            for (; k < n && i < count; ++k) {
                const double x = buf_x[k];
                const double vr = 1.0 + c * x;
                if (vr <= 0.0)
                    continue;
                const double v = vr * vr * vr;
                alignas(32) double utail[4];
                _mm256_storeu_pd(
                    utail, detail::u64_to_uniform01_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
                const double u = utail[0];
                const double x2 = x * x;
                if (u < 1.0 - 0.0331 * x2 * x2) {
                    out[i++] = d * v;
                    continue;
                }
                if (std::log(u) < 0.5 * x2 + d * (1.0 - v + std::log(v))) {
                    out[i++] = d * v;
                }
            }
        }
#else
        // SIMD RNG + scalar polar + scalar MT
        alignas(32) double tmp_u1[4], tmp_u2[4];
        std::size_t i = 0;
        while (i < count) {
            // Phase 1: fill buf_x with N(0,1) values
            int n = 0;
            while (n < kBuf) {
                _mm256_storeu_pd(tmp_u1,
                                 detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
                _mm256_storeu_pd(tmp_u2,
                                 detail::u64_to_pm1_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
                for (int lane = 0; lane < 4 && n < kBuf; ++lane) {
                    const double u1 = tmp_u1[lane], u2 = tmp_u2[lane];
                    const double sq = u1 * u1 + u2 * u2;
                    if (sq >= 1.0 || sq == 0.0) [[unlikely]]
                        continue;
                    const double scale = std::sqrt(-2.0 * std::log(sq) / sq);
                    buf_x[n++] = u1 * scale;
                    if (n < kBuf)
                        buf_x[n++] = u2 * scale;
                }
                _mm256_storeu_pd(tmp_u1,
                                 detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3)));
                _mm256_storeu_pd(tmp_u2,
                                 detail::u64_to_pm1_avx2(detail::next_x4_avx2(b0, b1, b2, b3)));
                for (int lane = 0; lane < 4 && n < kBuf; ++lane) {
                    const double u1 = tmp_u1[lane], u2 = tmp_u2[lane];
                    const double sq = u1 * u1 + u2 * u2;
                    if (sq >= 1.0 || sq == 0.0) [[unlikely]]
                        continue;
                    const double scale = std::sqrt(-2.0 * std::log(sq) / sq);
                    buf_x[n++] = u1 * scale;
                    if (n < kBuf)
                        buf_x[n++] = u2 * scale;
                }
            }
            // Phase 2: scalar MT acceptance, uniforms drawn from a stream
            alignas(32) double u_batch[4];
            int k = 0;
            for (; k + 4 <= n && i < count; k += 4) {
                _mm256_storeu_pd(
                    u_batch, detail::u64_to_uniform01_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
                for (int lane = 0; lane < 4 && i < count; ++lane) {
                    const double x = buf_x[k + lane];
                    const double vr = 1.0 + c * x;
                    if (vr <= 0.0)
                        continue;
                    const double v = vr * vr * vr;
                    const double u = u_batch[lane];
                    const double x2 = x * x;
                    if (u < 1.0 - 0.0331 * x2 * x2) {
                        out[i++] = d * v;
                        continue;
                    }
                    if (std::log(u) < 0.5 * x2 + d * (1.0 - v + std::log(v))) {
                        out[i++] = d * v;
                    }
                }
            }
            for (; k < n && i < count; ++k) {
                const double x = buf_x[k];
                const double vr = 1.0 + c * x;
                if (vr <= 0.0)
                    continue;
                const double v = vr * vr * vr;
                _mm256_storeu_pd(
                    u_batch, detail::u64_to_uniform01_avx2(detail::next_x4_avx2(a0, a1, a2, a3)));
                const double u = u_batch[0];
                const double x2 = x * x;
                if (u < 1.0 - 0.0331 * x2 * x2) {
                    out[i++] = d * v;
                    continue;
                }
                if (std::log(u) < 0.5 * x2 + d * (1.0 - v + std::log(v))) {
                    out[i++] = d * v;
                }
            }
        }
#endif
        store_avx2(a0, a1, a2, a3, b0, b1, b2, b3);
    }

    void fill_student_t_avx2(double* __restrict out, std::size_t count, double nu) noexcept {
        static constexpr std::size_t kChunk = 128;
        alignas(32) double z_buf[kChunk];
        alignas(32) double g_buf[kChunk];
        const double inv_half_nu = 2.0 / nu;
        const double shape = nu / 2.0;
        std::size_t i = 0;
        while (i < count) {
            const std::size_t chunk = count - i < kChunk ? count - i : kChunk;
            fill_normal_avx2(z_buf, chunk, 0.0, 1.0);
            fill_gamma_avx2(g_buf, chunk, shape);
            for (std::size_t k = 0; k < chunk; ++k)
                out[i + k] = z_buf[k] / std::sqrt(g_buf[k] * inv_half_nu);
            i += chunk;
        }
    }
#endif  // __AVX2__

    // ── AVX-512 distribution kernels ─────────────────────────────────────────

#ifdef __AVX512F__
    void fill_bernoulli_avx512(double* __restrict out, std::size_t count,
                               std::uint64_t threshold) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx512();
        const __m512i thresh_vec = _mm512_set1_epi64(static_cast<std::int64_t>(threshold));
        const __m512d one_d = _mm512_set1_pd(1.0);

        std::size_t i = 0;
        while (i + 16 <= count) {
            const __mmask8 ma = _mm512_cmp_epu64_mask(detail::next_x8_avx512(a0, a1, a2, a3),
                                                      thresh_vec, _MM_CMPINT_LT);
            const __mmask8 mb = _mm512_cmp_epu64_mask(detail::next_x8_avx512(b0, b1, b2, b3),
                                                      thresh_vec, _MM_CMPINT_LT);
            _mm512_storeu_pd(out + i, _mm512_maskz_mov_pd(ma, one_d));
            _mm512_storeu_pd(out + i + 8, _mm512_maskz_mov_pd(mb, one_d));
            i += 16;
        }
        if (i + 8 <= count) {
            const __mmask8 ma = _mm512_cmp_epu64_mask(detail::next_x8_avx512(a0, a1, a2, a3),
                                                      thresh_vec, _MM_CMPINT_LT);
            _mm512_storeu_pd(out + i, _mm512_maskz_mov_pd(ma, one_d));
            i += 8;
        }
        if (i < count) {
            alignas(64) double tail[8];
            const __mmask8 ma = _mm512_cmp_epu64_mask(detail::next_x8_avx512(a0, a1, a2, a3),
                                                      thresh_vec, _MM_CMPINT_LT);
            _mm512_storeu_pd(tail, _mm512_maskz_mov_pd(ma, one_d));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
        store_avx512(a0, a1, a2, a3, b0, b1, b2, b3);
    }

#ifdef ZORRO_USE_LIBMVEC
    void fill_normal_avx512_vecpolar(double* __restrict out, std::size_t count, double mean,
                                     double stddev) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx512();
        const __m512d one = _mm512_set1_pd(1.0);
        const __m512d zero = _mm512_setzero_pd();
        const __m512d neg2 = _mm512_set1_pd(-2.0);
        const __m512d safe_val = _mm512_set1_pd(0.5);
        const __m512d vmean = _mm512_set1_pd(mean);
        const __m512d vstd = _mm512_set1_pd(stddev);

        std::size_t i = 0;
        while (i < count) {
            // Group a
            {
                const __m512d u1 =
                    detail::u64_to_pm1_avx512(detail::next_x8_avx512(a0, a1, a2, a3));
                const __m512d u2 =
                    detail::u64_to_pm1_avx512(detail::next_x8_avx512(a0, a1, a2, a3));
                const __m512d s = _mm512_add_pd(_mm512_mul_pd(u1, u1), _mm512_mul_pd(u2, u2));
                const __mmask8 accept = _mm512_cmp_pd_mask(s, one, _CMP_LT_OQ) &
                                        _mm512_cmp_pd_mask(s, zero, _CMP_GT_OQ);
                if (accept) {
                    const __m512d safe_s = _mm512_mask_blend_pd(accept, safe_val, s);
                    const __m512d factor = _mm512_sqrt_pd(
                        _mm512_div_pd(_mm512_mul_pd(neg2, _ZGVeN8v_log(safe_s)), safe_s));
                    const __m512d n1 =
                        _mm512_add_pd(vmean, _mm512_mul_pd(vstd, _mm512_mul_pd(u1, factor)));
                    const __m512d n2 =
                        _mm512_add_pd(vmean, _mm512_mul_pd(vstd, _mm512_mul_pd(u2, factor)));
                    alignas(64) double t1[8], t2[8];
                    const int n = __builtin_popcount(accept);
                    _mm512_mask_compressstoreu_pd(t1, accept, n1);
                    _mm512_mask_compressstoreu_pd(t2, accept, n2);
                    for (int k = 0; k < n && i < count; ++k) {
                        out[i++] = t1[k];
                        if (i < count)
                            out[i++] = t2[k];
                    }
                }
            }
            if (i >= count)
                break;
            // Group b
            {
                const __m512d u1 =
                    detail::u64_to_pm1_avx512(detail::next_x8_avx512(b0, b1, b2, b3));
                const __m512d u2 =
                    detail::u64_to_pm1_avx512(detail::next_x8_avx512(b0, b1, b2, b3));
                const __m512d s = _mm512_add_pd(_mm512_mul_pd(u1, u1), _mm512_mul_pd(u2, u2));
                const __mmask8 accept = _mm512_cmp_pd_mask(s, one, _CMP_LT_OQ) &
                                        _mm512_cmp_pd_mask(s, zero, _CMP_GT_OQ);
                if (accept) {
                    const __m512d safe_s = _mm512_mask_blend_pd(accept, safe_val, s);
                    const __m512d factor = _mm512_sqrt_pd(
                        _mm512_div_pd(_mm512_mul_pd(neg2, _ZGVeN8v_log(safe_s)), safe_s));
                    const __m512d n1 =
                        _mm512_add_pd(vmean, _mm512_mul_pd(vstd, _mm512_mul_pd(u1, factor)));
                    const __m512d n2 =
                        _mm512_add_pd(vmean, _mm512_mul_pd(vstd, _mm512_mul_pd(u2, factor)));
                    alignas(64) double t1[8], t2[8];
                    const int n = __builtin_popcount(accept);
                    _mm512_mask_compressstoreu_pd(t1, accept, n1);
                    _mm512_mask_compressstoreu_pd(t2, accept, n2);
                    for (int k = 0; k < n && i < count; ++k) {
                        out[i++] = t1[k];
                        if (i < count)
                            out[i++] = t2[k];
                    }
                }
            }
        }
        store_avx512(a0, a1, a2, a3, b0, b1, b2, b3);
    }

    void fill_exponential_avx512_veclog(double* __restrict out, std::size_t count,
                                        double inv_lambda) noexcept {
        auto [a0, a1, a2, a3, b0, b1, b2, b3] = load_avx512();
        const __m512d neg_inv = _mm512_set1_pd(-inv_lambda);
        std::size_t i = 0;
        while (i + 16 <= count) {
            _mm512_storeu_pd(out + i,
                             _mm512_mul_pd(neg_inv, _ZGVeN8v_log(detail::u64_to_uniform01_avx512(
                                                        detail::next_x8_avx512(a0, a1, a2, a3)))));
            _mm512_storeu_pd(out + i + 8,
                             _mm512_mul_pd(neg_inv, _ZGVeN8v_log(detail::u64_to_uniform01_avx512(
                                                        detail::next_x8_avx512(b0, b1, b2, b3)))));
            i += 16;
        }
        if (i + 8 <= count) {
            _mm512_storeu_pd(out + i,
                             _mm512_mul_pd(neg_inv, _ZGVeN8v_log(detail::u64_to_uniform01_avx512(
                                                        detail::next_x8_avx512(a0, a1, a2, a3)))));
            i += 8;
        }
        if (i < count) {
            alignas(64) double tail[8];
            _mm512_storeu_pd(tail,
                             _mm512_mul_pd(neg_inv, _ZGVeN8v_log(detail::u64_to_uniform01_avx512(
                                                        detail::next_x8_avx512(a0, a1, a2, a3)))));
            for (std::size_t lane = 0; i < count; ++lane, ++i)
                out[i] = tail[lane];
        }
        store_avx512(a0, a1, a2, a3, b0, b1, b2, b3);
    }
#endif  // ZORRO_USE_LIBMVEC
#endif  // __AVX512F__
};

// ─── Convenience: thread-local singleton ─────────────────────────────────────

inline auto get_rng() noexcept -> Rng& {
    alignas(64) static thread_local Rng rng{std::random_device{}()};
    return rng;
}

inline void reseed(std::uint64_t seed) noexcept {
    get_rng() = Rng{seed};
}

}  // namespace zorro
