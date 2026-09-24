// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The filter-over-aggregate prefilter (src/runtime/aggregate_prefilter.hpp).
//
// The filter itself stays above the aggregate, so a prefilter that keeps too
// much is invisible in any query result. What these tests have to catch is the
// other failure, a group dropped that the filter would have kept, and whether
// the prefilter runs at all. So every aggregate here is run twice through the
// same physical build, with and without the terms, and compared both raw (the
// prefilter really removed groups) and after the real filter (it removed
// nothing the filter keeps), over every key representation the aggregate has
// and serially, in parallel and over chunks.

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/env.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "aggregate_prefilter.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "physical_executor_internal.hpp"
#include "physical_plan.hpp"

using namespace ibex;

namespace {

class ChunkGrainGuard {
   public:
    explicit ChunkGrainGuard(const std::string& grain) {
        if (const char* value = std::getenv("IBEX_CHUNK_ROWS"); value != nullptr) {
            saved_ = value;
        }
        runtime::set_env("IBEX_CHUNK_ROWS", grain);
    }
    ~ChunkGrainGuard() {
        if (saved_.has_value()) {
            runtime::set_env("IBEX_CHUNK_ROWS", *saved_);
        } else {
            runtime::unset_env("IBEX_CHUNK_ROWS");
        }
    }
    ChunkGrainGuard(const ChunkGrainGuard&) = delete;
    auto operator=(const ChunkGrainGuard&) -> ChunkGrainGuard& = delete;
    ChunkGrainGuard(ChunkGrainGuard&&) = delete;
    auto operator=(ChunkGrainGuard&&) -> ChunkGrainGuard& = delete;

   private:
    std::optional<std::string> saved_;
};

auto lower_query(const std::string& source) -> ir::NodePtr {
    auto program = parser::parse(source);
    REQUIRE(program.has_value());
    auto lowered = parser::lower(program.value());
    REQUIRE(lowered.has_value());
    return std::move(lowered.value());
}

auto find_filter_over_aggregate(const ir::Node& node) -> const ir::FilterNode* {
    if (node.kind() == ir::NodeKind::Filter && !node.children().empty() &&
        node.children().front()->kind() == ir::NodeKind::Aggregate) {
        return &ir::node_cast<ir::FilterNode>(node);
    }
    for (const auto& child : node.children()) {
        if (const auto* found = find_filter_over_aggregate(*child)) {
            return found;
        }
    }
    return nullptr;
}

auto run_aggregate(const ir::Node& aggregate, const runtime::TableRegistry& registry,
                   const runtime::ExecutionContext& exec,
                   const runtime::AggregatePrefilter& prefilter) -> runtime::Table {
    const auto plan = runtime::physical::plan_physical(aggregate, registry, nullptr);
    REQUIRE(plan.migrated);
    REQUIRE(plan.aggregate.describes);
    auto op = runtime::physical_executor_detail::build_physical_aggregate(
        plan, aggregate, registry, nullptr, nullptr, exec, nullptr, prefilter);
    REQUIRE(op.has_value());
    auto table = runtime::materialize_operator(std::move(op.value()));
    REQUIRE(table.has_value());
    return std::move(table.value());
}

/// The real filter, over a materialized table: a Filter over a Scan, which has
/// no aggregate source and so no prefilter of its own.
auto apply_filter(runtime::Table table, const std::string& predicate) -> runtime::Table {
    runtime::TableRegistry registry;
    registry.emplace("a", std::move(table));
    const auto ir = lower_query("a[filter " + predicate + "];");
    runtime::ExecutionContext exec;
    exec.parallel_threads = 1;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

void require_same(const runtime::Table& expected, const runtime::Table& actual) {
    if (auto mismatch = runtime::compare_tables(expected, actual); mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}

/// 300k rows, 50k groups of 6. `v` is constant within a group (its key mod
/// 1000, halved), so `s = sum(v)` is 3 * (k mod 1000) and a threshold picks an
/// exact share of the groups; `w` varies within a group.
auto make_registry(bool null_values) -> runtime::TableRegistry {
    constexpr std::int64_t kRows = 300'000;
    constexpr std::int64_t kGroups = 50'000;
    Column<std::int64_t> k;
    Column<std::int64_t> k2;
    Column<std::string> ks;
    Column<Categorical> kc;
    Column<double> v;
    Column<std::int64_t> w;
    runtime::ValidityBitmap v_valid;
    for (std::int64_t i = 0; i < kRows; ++i) {
        const std::int64_t key = i % kGroups;
        k.push_back(key);
        k2.push_back(key % 7);
        ks.push_back("g" + std::to_string(key));
        kc.push_back("c" + std::to_string(key));
        v.push_back(static_cast<double>(key % 1000) * 0.5);
        w.push_back(i % 997);
        // Every row of one group in ten is null, so its sum and mean are null.
        v_valid.push_back(!null_values || key % 10 != 0);
    }
    runtime::Table t;
    t.add_column("k", std::move(k));
    t.add_column("k2", std::move(k2));
    t.add_column("ks", std::move(ks));
    t.add_column("kc", std::move(kc));
    t.add_column("v", std::move(v));
    t.add_column("w", std::move(w));
    if (null_values) {
        t.columns[4].validity = std::move(v_valid);
    }
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));
    return registry;
}

auto contexts() -> std::vector<std::pair<std::string, runtime::ExecutionContext>> {
    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.parallel_min_rows = 0;
    return {{"serial", serial}, {"parallel", parallel}};
}

/// Run `aggregate[filter predicate]`'s aggregate with and without its terms,
/// and check both what the prefilter kept and that the filter's answer is
/// unchanged. `exact` when no aggregate is null, so the prefilter should keep
/// precisely the groups that pass.
void check_prefilter(const runtime::TableRegistry& registry, const std::string& select,
                     const std::string& by, const std::string& predicate, bool exact) {
    const std::string query = "t[select { " + select + " }, by { " + by + " }][filter " +
                              predicate + "];";
    INFO(query);
    const auto ir = lower_query(query);
    const auto* filter = find_filter_over_aggregate(*ir);
    REQUIRE(filter != nullptr);
    const auto& aggregate = ir::node_cast<ir::AggregateNode>(*filter->children().front());
    const auto terms = runtime::aggregate_prefilter_terms(filter->predicate(), aggregate);
    REQUIRE_FALSE(terms.empty());

    for (const char* grain : {"", "70001"}) {
        std::optional<ChunkGrainGuard> guard;
        if (*grain != '\0') {
            guard.emplace(grain);
        }
        for (const auto& [name, exec] : contexts()) {
            INFO(name << " chunk rows " << (*grain == '\0' ? "all" : grain));
            const auto full = run_aggregate(aggregate, registry, exec, {});
            const auto kept = run_aggregate(aggregate, registry, exec, terms);
            const auto expected = apply_filter(full, predicate);
            // It ran: groups the filter drops never reached emission.
            REQUIRE(kept.rows() < full.rows());
            REQUIRE(kept.rows() >= expected.rows());
            if (exact) {
                REQUIRE(kept.rows() == expected.rows());
            }
            // And it dropped nothing the filter keeps, in the same order.
            require_same(expected, apply_filter(kept, predicate));
        }
    }
}

}  // namespace

TEST_CASE("aggregate prefilter: terms are the filter's aggregate-vs-literal conjuncts",
          "[aggregate][prefilter]") {
    const auto terms_of = [](const std::string& predicate) {
        const auto ir = lower_query(
            "t[select { s = sum(v), n = count() }, by { k }][filter " + predicate + "];");
        const auto* filter = find_filter_over_aggregate(*ir);
        REQUIRE(filter != nullptr);
        return runtime::aggregate_prefilter_terms(
            filter->predicate(), ir::node_cast<ir::AggregateNode>(*filter->children().front()));
    };

    // A literal on the left is turned around; a group key is not an output.
    const auto terms = terms_of("s > 10.5 && 3 <= n && k > 1");
    REQUIRE(terms.size() == 2);
    CHECK(terms[0].output == "s");
    CHECK(terms[0].op == ir::CompareOp::Gt);
    CHECK(std::get<double>(terms[0].literal) == 10.5);
    CHECK(terms[1].output == "n");
    CHECK(terms[1].op == ir::CompareOp::Ge);
    CHECK(std::get<std::int64_t>(terms[1].literal) == 3);

    // Not conjunctions of output-vs-literal comparisons: nothing to use.
    CHECK(terms_of("s > 10.0 || n > 3").empty());
    CHECK(terms_of("s > n").empty());
}

TEST_CASE("aggregate prefilter: a group fails only when a term is definitely false",
          "[aggregate][prefilter]") {
    using ir::AggFunc;
    using runtime::ExprType;
    const auto spec = [](AggFunc func, std::string alias) {
        ir::AggSpec out;
        out.func = func;
        out.alias = std::move(alias);
        return out;
    };
    const std::vector<ir::AggSpec> aggs = {
        spec(AggFunc::Count, "n"), spec(AggFunc::Sum, "si"), spec(AggFunc::Sum, "sd"),
        spec(AggFunc::Mean, "m"),  spec(AggFunc::Min, "lo"), spec(AggFunc::Max, "txt"),
    };
    const std::vector<ExprType> kinds = {ExprType::Int,    ExprType::Int,    ExprType::Double,
                                         ExprType::Double, ExprType::Double, ExprType::String};
    const auto one = [&](std::string output, ir::CompareOp op,
                         std::variant<std::int64_t, double> literal) {
        auto resolved = runtime::GroupPrefilter::resolve(
            {{.output = std::move(output), .op = op, .literal = literal}}, aggs, kinds);
        REQUIRE(resolved.has_value());
        return *resolved;
    };
    std::vector<runtime::AggSlotCore> slots(aggs.size());
    const auto passes = [&](const runtime::GroupPrefilter& prefilter) {
        return prefilter.may_pass(slots.data());
    };

    // Count reads the row count.
    const auto n_gt_3 = one("n", ir::CompareOp::Gt, std::int64_t{3});
    slots[0].count = 3;
    CHECK_FALSE(passes(n_gt_3));
    slots[0].count = 4;
    CHECK(passes(n_gt_3));

    // An Int sum: exact integer comparison; no row at all is null, so kept.
    const auto si_eq_5 = one("si", ir::CompareOp::Eq, std::int64_t{5});
    slots[1].count = 1;
    slots[1].int_value = 5;
    CHECK(passes(si_eq_5));
    slots[1].int_value = 6;
    CHECK_FALSE(passes(si_eq_5));
    slots[1].count = 0;
    CHECK(passes(si_eq_5));
    slots[1].count = 1;

    // Against a fractional literal the integer compares exactly, and a value
    // too large to convert exactly is left to the filter.
    const auto si_gt_half = one("si", ir::CompareOp::Gt, 2.5);
    slots[1].int_value = 2;
    CHECK_FALSE(passes(si_gt_half));
    slots[1].int_value = 3;
    CHECK(passes(si_gt_half));
    slots[1].int_value = std::int64_t{1} << 60;
    CHECK(passes(one("si", ir::CompareOp::Lt, 2.5)));

    // A Double sum: NaN is the filter's to decide; an integer literal compares
    // as its double.
    const auto sd_gt_3 = one("sd", ir::CompareOp::Gt, std::int64_t{3});
    slots[2].count = 1;
    slots[2].double_value = 3.0;
    CHECK_FALSE(passes(sd_gt_3));
    slots[2].double_value = std::numeric_limits<double>::quiet_NaN();
    CHECK(passes(sd_gt_3));
    CHECK(passes(one("sd", ir::CompareOp::Ne, 1.0)));

    // Mean is sum / count, as emitted; an empty mean is null.
    const auto m_ge_2 = one("m", ir::CompareOp::Ge, 2.0);
    slots[3].double_value = 5.0;
    slots[3].count = 2;
    CHECK(passes(m_ge_2));
    slots[3].count = 3;
    CHECK_FALSE(passes(m_ge_2));
    slots[3].count = 0;
    CHECK(passes(m_ge_2));

    // Every comparison, on a Double min.
    slots[4].count = 1;
    slots[4].double_value = 1.0;
    CHECK(passes(one("lo", ir::CompareOp::Lt, 2.0)));
    CHECK_FALSE(passes(one("lo", ir::CompareOp::Gt, 2.0)));
    CHECK(passes(one("lo", ir::CompareOp::Le, 1.0)));
    CHECK_FALSE(passes(one("lo", ir::CompareOp::Ge, 1.5)));
    CHECK(passes(one("lo", ir::CompareOp::Eq, 1.0)));
    CHECK_FALSE(passes(one("lo", ir::CompareOp::Ne, 1.0)));

    // A text max, an unknown name and a non-finite literal are not terms.
    CHECK_FALSE(
        runtime::GroupPrefilter::resolve({{.output = "txt", .op = ir::CompareOp::Gt, .literal = 1.0}},
                                         aggs, kinds)
            .has_value());
    CHECK_FALSE(runtime::GroupPrefilter::resolve(
                    {{.output = "nope", .op = ir::CompareOp::Gt, .literal = 1.0}}, aggs, kinds)
                    .has_value());
    CHECK_FALSE(runtime::GroupPrefilter::resolve(
                    {{.output = "sd",
                      .op = ir::CompareOp::Gt,
                      .literal = std::numeric_limits<double>::infinity()}},
                    aggs, kinds)
                    .has_value());
}

TEST_CASE("aggregate prefilter: every key representation keeps exactly what the filter keeps",
          "[aggregate][prefilter]") {
    const auto registry = make_registry(/*null_values=*/false);
    const std::string select = "s = sum(v), n = count(), m = mean(v), lo = min(w), hi = max(w)";
    // Int64, String, Categorical, two Int64s, and Int64 + String (the generic
    // key), each with a threshold on a different aggregate.
    check_prefilter(registry, select, "k", "s > 2700.0", true);
    check_prefilter(registry, select, "ks", "m < 30.0 && n == 6", true);
    check_prefilter(registry, select, "kc", "2700.0 <= s", true);
    check_prefilter(registry, select, "k, k2", "lo >= 900", true);
    check_prefilter(registry, select, "k, ks", "hi <= 400 && s > 1500.0", true);
}

TEST_CASE("aggregate prefilter: the owned-partition paths prune before they merge",
          "[aggregate][prefilter]") {
    // One Double sum by one Int64 key is q18's shape: the async hot path at
    // several workers, which prunes inside the partition merge.
    const auto registry = make_registry(/*null_values=*/false);
    check_prefilter(registry, "s = sum(v)", "k", "s > 2700.0", true);
    check_prefilter(registry, "s = sum(w)", "k", "s < 1000", true);
    check_prefilter(registry, "s = sum(v)", "k, k2", "s > 2700.0", true);
}

TEST_CASE("aggregate prefilter: a null aggregate is left to the filter", "[aggregate][prefilter]") {
    // Groups whose values are all null have a null sum and mean. The filter
    // drops them; the prefilter must not decide them either way, so it keeps
    // them and the answers still agree.
    const auto registry = make_registry(/*null_values=*/true);
    check_prefilter(registry, "s = sum(v), m = mean(v)", "k", "s > 2700.0", false);
    check_prefilter(registry, "s = sum(v)", "k", "s > 2700.0", false);
    check_prefilter(registry, "s = sum(v), m = mean(v)", "ks", "m < 30.0", false);
}

TEST_CASE("aggregate prefilter: whole queries answer as the aggregate then the filter",
          "[aggregate][prefilter]") {
    // Through the ordinary executor: the pipeline source (serial) and the
    // parallel run's input both build the aggregate with its prefilter, and a
    // projection fused over the filter must not hide it or change the answer.
    // Also an ungrouped aggregate, which always emits one row and is never
    // prefiltered.
    const auto registry = make_registry(/*null_values=*/true);
    for (const auto& [aggregate, predicate, tail] : std::vector<std::tuple<std::string, std::string, std::string>>{
             {"t[select { s = sum(v), n = count() }, by { k }]", "s > 2700.0 && n > 5", ""},
             {"t[select { s = sum(v) }, by { k }]", "s > 2700.0", "[select { k }]"},
             {"t[select { s = sum(v) }, by { ks }]", "s < 60.0", "[select { ks }]"},
             {"t[select { s = sum(v) }]", "s > 1000000000000000000.0", ""},
             {"t[select { s = sum(v) }]", "s < 1000000000000000000.0", ""},
         }) {
        INFO(aggregate << "[filter " << predicate << "]" << tail);
        for (const char* grain : {"", "70001"}) {
            std::optional<ChunkGrainGuard> guard;
            if (*grain != '\0') {
                guard.emplace(grain);
            }
            for (const auto& [name, base] : contexts()) {
                INFO(name << " chunk rows " << (*grain == '\0' ? "all" : grain));
                runtime::ExecutionContext exec = base;
                auto profile = std::make_shared<runtime::ExecutionProfileState>(
                    /*worker_budget=*/8, /*report=*/false);
                exec.execution_profile = profile;
                const auto whole = runtime::interpret(
                    *lower_query(aggregate + "[filter " + predicate + "]" + tail + ";"), registry,
                    nullptr, nullptr, nullptr, exec);
                REQUIRE(whole.has_value());
                // A grouped aggregate emitted fewer than its 50,000 groups: the
                // executor built it with the prefilter, on this path too.
                if (aggregate.find("by {") != std::string::npos) {
                    const auto rows = profile->snapshot();
                    const auto row = std::ranges::find_if(rows, [](const auto& r) {
                        return r.label.starts_with("aggregate");
                    });
                    REQUIRE(row != rows.end());
                    CHECK(row->rows < 50'000);
                }
                exec.execution_profile = nullptr;
                // The reference: the aggregate materialized on its own, then
                // the filter and the tail over the table.
                auto grouped =
                    runtime::interpret(*lower_query(aggregate + ";"), registry, nullptr, nullptr,
                                       nullptr, exec);
                REQUIRE(grouped.has_value());
                runtime::TableRegistry staged;
                staged.emplace("a", std::move(grouped.value()));
                const auto expected = runtime::interpret(
                    *lower_query("a[filter " + predicate + "]" + tail + ";"), staged, nullptr,
                    nullptr, nullptr, exec);
                REQUIRE(expected.has_value());
                require_same(*expected, *whole);
            }
        }
    }
}
