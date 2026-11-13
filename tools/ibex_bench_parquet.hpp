// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The `parquet` suite of ibex_bench: synthetic Parquet files written once per
// shape, and queries over them run the way IbexEngine runs a Polars plan (a
// fresh REPL session with the script planner's deferred and streaming scans).
// Compiled only when the bundled Parquet backend is built
// (IBEX_HAS_PARQUET_BACKEND); the declarations keep ibex_bench.cpp free of
// Arrow headers.

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <expected>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::bench_parquet {

struct BenchFiles {
    /// `fact`: k (uniform over 1..dim_rows), c (sorted, four rows per value),
    /// v (Float64), q (1..50), flag ("A" / "N" / "R").
    std::string fact;
    /// `dim`: k 1..dim_rows, size 1..50. Joined to fact on k.
    std::string dim;
    /// `orders`: c 1..fact_rows / 4, w 0..999. Joined to fact on c.
    std::string orders;
    std::size_t fact_rows = 0;
    std::size_t dim_rows = 0;
    std::size_t order_rows = 0;
};

/// The files for `fact_rows` rows under `directory`, written first if absent.
/// The data depends only on `fact_rows` and the generator version, so every
/// build that runs the suite reads byte-identical files.
[[nodiscard]] auto prepare_files(const std::filesystem::path& directory, std::size_t fact_rows)
    -> std::expected<BenchFiles, std::string>;

struct BenchQuery {
    std::string name;
    /// One table expression; `parquet::read` is declared before it.
    std::string expression;
};

[[nodiscard]] auto queries(const BenchFiles& files) -> std::vector<BenchQuery>;

/// Run one query in a fresh session, as IbexEngine's `execute_pipeline` does.
[[nodiscard]] auto run_query(const BenchQuery& query) -> std::expected<runtime::Table, std::string>;

}  // namespace ibex::bench_parquet
