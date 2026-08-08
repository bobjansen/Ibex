// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

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

auto derive_rng_seed(std::uint64_t master_seed, std::uint64_t stream_id) noexcept -> std::uint64_t {
    // SplitMix64 finalizer. Mixing the stream ID separately avoids correlated
    // low-numbered worker seeds while keeping assignment fully deterministic.
    auto mix = [](std::uint64_t value) noexcept {
        value ^= value >> 30;
        value *= 0xBF58476D1CE4E5B9ULL;
        value ^= value >> 27;
        value *= 0x94D049BB133111EBULL;
        return value ^ (value >> 31);
    };
    return mix(master_seed ^ mix(stream_id + 0x9E3779B97F4A7C15ULL));
}

RngStream::RngStream(std::uint64_t seed) noexcept : scalar_{seed}, simd_{seed}, x4_{seed} {}

void RngStream::reseed(std::uint64_t seed) noexcept {
    scalar_ = Xoshiro256pp{seed};
    simd_ = zorro::Rng{seed};
    x4_ = Xoshiro256pp_x4_portable{seed};
}

// The default stream lives as a function-local thread_local rather than a
// namespace-scope global. It is safe for independent callers on different
// threads, but explicit streams make parallel scheduling reproducible.
auto get_rng_stream() noexcept -> RngStream& {
    alignas(64) static thread_local RngStream rng{entropy_seed()};
    return rng;
}

auto get_rng() noexcept -> Xoshiro256pp& {
    return get_rng_stream().scalar();
}

auto get_rng_simd() noexcept -> zorro::Rng& {
    return get_rng_stream().simd();
}

auto get_rng_x4() noexcept -> Xoshiro256pp_x4_portable& {
    return get_rng_stream().x4();
}

void reseed(std::uint64_t seed) noexcept {
    get_rng_stream().reseed(seed);
}

void fill_uniform(double* __restrict out, std::size_t rows, double low, double high) noexcept {
    fill_uniform(get_rng_stream(), out, rows, low, high);
}

void fill_uniform(RngStream& stream, double* __restrict out, std::size_t rows, double low,
                  double high) noexcept {
    stream.simd().fill_uniform(out, rows, low, high);
}

void fill_normal(double* __restrict out, std::size_t rows, double mean, double stddev) noexcept {
    fill_normal(get_rng_stream(), out, rows, mean, stddev);
}

void fill_normal(RngStream& stream, double* __restrict out, std::size_t rows, double mean,
                 double stddev) noexcept {
    stream.simd().fill_normal(out, rows, mean, stddev);
}

void fill_exponential(double* __restrict out, std::size_t rows, double lambda) noexcept {
    fill_exponential(get_rng_stream(), out, rows, lambda);
}

void fill_exponential(RngStream& stream, double* __restrict out, std::size_t rows,
                      double lambda) noexcept {
    stream.simd().fill_exponential(out, rows, lambda);
}

// Bulk int64 fills delegate to the SIMD-dispatched zorro engine (the same one
// that backs the double fills), so bernoulli/int are hand-vectorized rather
// than relying on the compiler to auto-vectorize a portable loop.
void fill_bernoulli(std::int64_t* __restrict out, std::size_t rows, double p) noexcept {
    fill_bernoulli(get_rng_stream(), out, rows, p);
}

void fill_bernoulli(RngStream& stream, std::int64_t* __restrict out, std::size_t rows,
                    double p) noexcept {
    stream.simd().fill_bernoulli(out, rows, p);
}

void fill_int(std::int64_t* __restrict out, std::size_t rows, std::int64_t lo,
              std::uint64_t span) noexcept {
    fill_int(get_rng_stream(), out, rows, lo, span);
}

void fill_int(RngStream& stream, std::int64_t* __restrict out, std::size_t rows, std::int64_t lo,
              std::uint64_t span) noexcept {
    stream.simd().fill_int(out, rows, lo, span);
}

}  // namespace ibex::runtime
