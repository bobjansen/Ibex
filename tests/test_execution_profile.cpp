// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <memory>

#include "execution_profile_internal.hpp"

namespace {

using namespace ibex;

TEST_CASE("operator profiling records pulls, rows, and pooled work", "[runtime][profile]") {
    auto profile = std::make_shared<runtime::ExecutionProfileState>(false);
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    ir::ScanNode scan{ir::NodeId{42}, "trades"};
    auto op = runtime::profile_operator(std::make_unique<runtime::TableSourceOperator>(table),
                                        profile, scan);

    REQUIRE(op->next().value().has_value());
    REQUIRE_FALSE(op->next().value().has_value());

    auto* stage = profile->stage("test pool stage");
    {
        runtime::ExecutionProfileScope scope(stage, runtime::ProfilePhase::Next);
        runtime::WorkerPool pool{2};
        auto batch = pool.submit(2, [](std::size_t) {
            // Give the steady clock a non-zero interval without making the test
            // depend on a particular scheduler or duration.
            const auto until = std::chrono::steady_clock::now() + std::chrono::microseconds(50);
            while (std::chrono::steady_clock::now() < until) {
            }
        });
        batch.wait();
    }

    const auto rows = profile->snapshot();
    const auto scan_row = std::ranges::find_if(
        rows, [](const auto& row) { return row.node_id == 42 && row.label == "scan trades"; });
    REQUIRE(scan_row != rows.end());
    CHECK(scan_row->calls == 2);
    CHECK(scan_row->chunks == 1);
    CHECK(scan_row->rows == 3);
    CHECK(scan_row->span_ns > 0);

    const auto pool_row =
        std::ranges::find_if(rows, [](const auto& row) { return row.label == "test pool stage"; });
    REQUIRE(pool_row != rows.end());
    CHECK(pool_row->pool_tasks == 2);
    CHECK(pool_row->pool_work_ns > 0);
}

}  // namespace
