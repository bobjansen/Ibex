#include <ibex/runtime/rng.hpp>

#ifdef _MSC_VER
#include <intrin.h>
#endif

namespace ibex::runtime {

namespace {
// Compute (a * b) >> 53 using 128-bit multiplication.
// On GCC/Clang we use __uint128_t; on MSVC we use _umul128.
inline auto mul_shift53(std::uint64_t a, std::uint64_t b) noexcept -> std::uint64_t {
#ifdef _MSC_VER
    std::uint64_t high = 0;
    std::uint64_t low = _umul128(a, b, &high);
    return __shiftright128(low, high, 53);
#else
    return static_cast<std::uint64_t>((static_cast<__uint128_t>(a) * b) >> 53);
#endif
}
}  // namespace

namespace {

alignas(64) thread_local Xoshiro256pp g_rng{std::random_device{}()};
alignas(64) thread_local zorro::Rng g_rng_simd{std::random_device{}()};
alignas(64) thread_local Xoshiro256pp_x4_portable g_rng_x4{std::random_device{}()};

}  // namespace

auto get_rng() noexcept -> Xoshiro256pp& {
    return g_rng;
}

auto get_rng_simd() noexcept -> zorro::Rng& {
    return g_rng_simd;
}

auto get_rng_x4() noexcept -> Xoshiro256pp_x4_portable& {
    return g_rng_x4;
}

void reseed(std::uint64_t seed) noexcept {
    g_rng = Xoshiro256pp{seed};
    g_rng_simd = zorro::Rng{seed};
    g_rng_x4 = Xoshiro256pp_x4_portable{seed};
}

void fill_uniform(double* __restrict out, std::size_t rows, double low, double high) noexcept {
    g_rng_simd.fill_uniform(out, rows, low, high);
}

void fill_normal(double* __restrict out, std::size_t rows, double mean, double stddev) noexcept {
    g_rng_simd.fill_normal(out, rows, mean, stddev);
}

void fill_exponential(double* __restrict out, std::size_t rows, double lambda) noexcept {
    g_rng_simd.fill_exponential(out, rows, lambda);
}

void fill_bernoulli(std::int64_t* __restrict out, std::size_t rows, double p) noexcept {
    constexpr double kScale53 = 9007199254740992.0;  // 2^53
    const auto threshold = static_cast<std::uint64_t>(p * kScale53);
    std::size_t i = 0;
    while (i + 4 <= rows) {
        const auto bits = g_rng_x4();
        out[i] = ((bits[0] >> 11) < threshold) ? 1 : 0;
        out[i + 1] = ((bits[1] >> 11) < threshold) ? 1 : 0;
        out[i + 2] = ((bits[2] >> 11) < threshold) ? 1 : 0;
        out[i + 3] = ((bits[3] >> 11) < threshold) ? 1 : 0;
        i += 4;
    }
    if (i < rows) {
        const auto bits = g_rng_x4();
        for (std::size_t lane = 0; i < rows; ++lane, ++i) {
            out[i] = ((bits[lane] >> 11) < threshold) ? 1 : 0;
        }
    }
}

void fill_int(std::int64_t* __restrict out, std::size_t rows, std::int64_t lo,
              std::uint64_t span) noexcept {
    std::size_t i = 0;
    while (i + 4 <= rows) {
        const auto bits = g_rng_x4();
        out[i] = lo + static_cast<std::int64_t>(mul_shift53(bits[0] >> 11, span));
        out[i + 1] = lo + static_cast<std::int64_t>(mul_shift53(bits[1] >> 11, span));
        out[i + 2] = lo + static_cast<std::int64_t>(mul_shift53(bits[2] >> 11, span));
        out[i + 3] = lo + static_cast<std::int64_t>(mul_shift53(bits[3] >> 11, span));
        i += 4;
    }
    if (i < rows) {
        const auto bits = g_rng_x4();
        for (std::size_t lane = 0; i < rows; ++lane, ++i) {
            out[i] = lo + static_cast<std::int64_t>(mul_shift53(bits[lane] >> 11, span));
        }
    }
}

}  // namespace ibex::runtime
