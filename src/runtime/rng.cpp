#include <ibex/runtime/rng.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <random>
#include <thread>

#include "zorro.hpp"

namespace ibex::runtime {

namespace {

// Draw a nondeterministic seed without ever throwing. std::random_device can
// throw std::system_error when its entropy source is unavailable (chroot,
// minimal container, seccomp-blocked getrandom). The engines below are
// thread_locals, so a throw escaping their initialization would call
// std::terminate — abort the whole process on first RNG use. Fall back to a
// clock + thread-id seed instead.
std::uint64_t entropy_seed() noexcept {
    try {
        std::random_device rd;
        return (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
    } catch (...) {
        auto t =
            static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
        // Mix so two threads seeding within the same clock tick still diverge.
        return t ^ (static_cast<std::uint64_t>(tid) * 0x9E3779B97F4A7C15ULL);
    }
}

}  // namespace

// The engines live as function-local thread_locals rather than namespace-scope
// globals so each stays a per-thread singleton without tripping
// cppcoreguidelines-avoid-non-const-global-variables. Accessors are called once
// per bulk fill, so the first-use guard is off the hot path.

auto get_rng() noexcept -> Xoshiro256pp& {
    alignas(64) static thread_local Xoshiro256pp rng{entropy_seed()};
    return rng;
}

auto get_rng_simd() noexcept -> zorro::Rng& {
    alignas(64) static thread_local zorro::Rng rng{entropy_seed()};
    return rng;
}

auto get_rng_x4() noexcept -> Xoshiro256pp_x4_portable& {
    alignas(64) static thread_local Xoshiro256pp_x4_portable rng{entropy_seed()};
    return rng;
}

void reseed(std::uint64_t seed) noexcept {
    get_rng() = Xoshiro256pp{seed};
    get_rng_simd() = zorro::Rng{seed};
    get_rng_x4() = Xoshiro256pp_x4_portable{seed};
}

void fill_uniform(double* __restrict out, std::size_t rows, double low, double high) noexcept {
    get_rng_simd().fill_uniform(out, rows, low, high);
}

void fill_normal(double* __restrict out, std::size_t rows, double mean, double stddev) noexcept {
    get_rng_simd().fill_normal(out, rows, mean, stddev);
}

void fill_exponential(double* __restrict out, std::size_t rows, double lambda) noexcept {
    get_rng_simd().fill_exponential(out, rows, lambda);
}

// Bulk int64 fills delegate to the SIMD-dispatched zorro engine (the same one
// that backs the double fills), so bernoulli/int are hand-vectorized rather
// than relying on the compiler to auto-vectorize a portable loop.
void fill_bernoulli(std::int64_t* __restrict out, std::size_t rows, double p) noexcept {
    get_rng_simd().fill_bernoulli(out, rows, p);
}

void fill_int(std::int64_t* __restrict out, std::size_t rows, std::int64_t lo,
              std::uint64_t span) noexcept {
    get_rng_simd().fill_int(out, rows, lo, span);
}

}  // namespace ibex::runtime
