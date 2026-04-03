#pragma once
// Ibex RNG layer — delegates to zorro.hpp for SIMD-dispatched xoshiro256++.
//
// Scalar engine (zorro::Xoshiro256pp)
//   · Satisfies UniformRandomBitGenerator — drop-in for std::<dist>(rng)
//   · Used by std::<dist> paths (student_t, gamma, poisson)
//
// SIMD engine (zorro::Rng)
//   · Automatic compile-time dispatch: AVX-512 → AVX2 → portable
//   · AMD Zen 4: integer kernels use AVX-512, FP-heavy kernels fall back to AVX2
//   · Used for fill_uniform, fill_normal, fill_exponential
//
// Integer fills (bernoulli → int64, rand_int)
//   · Use zorro::Xoshiro256pp_x4_portable directly (integer-domain comparison)
//
// Seeding: reseed() resets all three thread-local engines to the given seed.

#include <cstdint>
#include <random>
#include <zorro.hpp>

namespace ibex::runtime {

// Re-export scalar primitives from zorro.
using zorro::bits_to_01;
using zorro::bits_to_pm1;
using zorro::Xoshiro256pp;

// ─── Thread-local scalar engine (for std::distribution) ──────────────────────

auto get_rng() noexcept -> Xoshiro256pp&;

// ─── Thread-local SIMD-dispatched engine (for bulk double fills) ─────────────

auto get_rng_simd() noexcept -> zorro::Rng&;

// ─── Thread-local x4 portable engine (for integer fills) ─────────────────────

using zorro::Xoshiro256pp_x4_portable;

auto get_rng_x4() noexcept -> Xoshiro256pp_x4_portable&;

// ─── Seeding ─────────────────────────────────────────────────────────────────

void reseed(std::uint64_t seed) noexcept;

// ─── Bulk fills: double output (delegate to zorro::Rng) ──────────────────────

void fill_uniform(double* __restrict__ out, std::size_t rows, double low, double high) noexcept;

void fill_normal(double* __restrict__ out, std::size_t rows, double mean, double stddev) noexcept;

void fill_exponential(double* __restrict__ out, std::size_t rows, double lambda) noexcept;

// ─── Bulk fills: int64 output (zorro has no int64 bernoulli/int) ─────────────

void fill_bernoulli(std::int64_t* __restrict__ out, std::size_t rows, double p) noexcept;

void fill_int(std::int64_t* __restrict__ out, std::size_t rows, std::int64_t lo,
              std::uint64_t span) noexcept;

}  // namespace ibex::runtime
