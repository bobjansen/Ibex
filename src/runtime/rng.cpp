#include <ibex/runtime/rng.hpp>

namespace ibex::runtime {

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

// Bulk int64 fills delegate to the SIMD-dispatched zorro engine (the same one
// that backs the double fills), so bernoulli/int are hand-vectorized rather
// than relying on the compiler to auto-vectorize a portable loop.
void fill_bernoulli(std::int64_t* __restrict out, std::size_t rows, double p) noexcept {
    g_rng_simd.fill_bernoulli(out, rows, p);
}

void fill_int(std::int64_t* __restrict out, std::size_t rows, std::int64_t lo,
              std::uint64_t span) noexcept {
    g_rng_simd.fill_int(out, rows, lo, span);
}

}  // namespace ibex::runtime
