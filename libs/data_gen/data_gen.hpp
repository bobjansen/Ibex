// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Synthetic data generators for testing and demos.
//
// Build as a shared library alongside data_gen.hpp and place it in a
// directory on IBEX_LIBRARY_PATH so the Ibex REPL can load it via:
//
//   import data_gen;
//   let ticks = gen_ticks(1000);
//
// All generators draw through ExternRegistry::rng() (see RngBridge in
// extern_registry.hpp) rather than calling ibex::runtime::fill_* directly:
// this plugin is dlopen'd with its own statically-linked copy of the RNG
// engine, so a direct call would run against state `seed_rng` never touches.
// Routing through the bridge means `seed_rng(...)` in the REPL makes these
// reproducible.

#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <string>

namespace ibex::data_gen {

/// Synthetic tick data: timestamp, symbol, price, volume. `symbols` is a
/// comma-separated list, e.g. "AAPL,MSFT,GOOG". Each symbol has its own base
/// price (spread around `start_price`) and its own mean-reverting random walk,
/// so the series stay distinct even over millions of rows. `symbol` is a
/// Categorical column.
/// Inter-arrival times are drawn from an Exponential distribution with mean
/// `interval_ms` (a Poisson process), not evenly spaced. `start_ts_ms` is the
/// first timestamp in Unix milliseconds (0 means "use current wall-clock time").
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto gen_ticks(const runtime::RngBridge& rng, std::int64_t n, const std::string& symbols,
               double start_price, double volatility, double interval_ms, std::int64_t start_ts_ms)
    -> runtime::Table;

/// Gaussian random walk: value[0] = start, value[i] = value[i-1] + N(0, step_std).
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto gen_walk(const runtime::RngBridge& rng, std::int64_t n, double start, double step_std)
    -> runtime::Table;

/// n iid samples from N(mean, stddev) in a single "value" column.
auto gen_normal(const runtime::RngBridge& rng, std::int64_t n, double mean, double stddev)
    -> runtime::Table;

/// n iid samples from Uniform[low, high) in a single "value" column.
auto gen_uniform(const runtime::RngBridge& rng, std::int64_t n, double low, double high)
    -> runtime::Table;

/// n sequential string ids "<prefix>0" .. "<prefix>(n-1)".
auto gen_ids(std::int64_t n, const std::string& prefix) -> runtime::Table;

/// Static reference/master data, one row per distinct symbol in `symbols` (a
/// comma-separated list, e.g. "AAPL,MSFT,GOOG"). Columns: symbol, name,
/// sector, currency, lot_size, tick_size. Deterministic: a given symbol
/// always maps to the same row, so it joins cleanly against `gen_ticks`
/// output on `symbol` regardless of RNG seed.
auto gen_reference(const std::string& symbols) -> runtime::Table;

}  // namespace ibex::data_gen
