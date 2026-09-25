// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// A filter over an aggregate's outputs, applied to the groups before they are
// ordered and emitted.
//
// `big[select { s = sum(v) }, by { k }][filter s > 300.0]` builds every group,
// puts all of them in first-occurrence order, writes all of them out, and only
// then keeps the few that pass. q18 is exactly this: 12M groups, 57 kept, and
// ordering plus emission of the other 11,999,943 was about 15% of the query.
//
// The filter stays where it is. This only lets the aggregate drop, early, the
// groups that filter would certainly drop, so it can never change an answer: a
// group is dropped only when one of the filter's own conjuncts, read from the
// value the aggregate is about to emit, is definitely false. Anything uncertain
// is kept -- a null aggregate (no rows reached it), a NaN, a comparison that
// would not be exact -- and the filter decides it as before.

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "interpreter_internal.hpp"
#include "physical_plan.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

class WorkerPool;

/// One comparison of an aggregate output against a numeric literal, as the
/// filter wrote it, normalized to `output <op> literal`.
struct AggregatePrefilterTerm {
    std::string output;
    ir::CompareOp op = ir::CompareOp::Eq;
    std::variant<std::int64_t, double> literal;
};
using AggregatePrefilter = std::vector<AggregatePrefilterTerm>;

/// The conjuncts of `predicate` that compare one of `aggregate`'s outputs
/// against a numeric literal. The rest of the predicate is simply not used:
/// every conjunct must hold for a row to pass, so any subset of them is a
/// sound early test.
[[nodiscard]] auto aggregate_prefilter_terms(const ir::Expr& predicate,
                                             const ir::AggregateNode& aggregate)
    -> AggregatePrefilter;

/// The terms for a map pipeline whose source is an aggregate and whose first
/// step over it is a filter (fused or not). Empty otherwise.
[[nodiscard]] auto aggregate_prefilter_for_source(const physical::Plan& plan) -> AggregatePrefilter;

/// The terms resolved against one aggregate's slot layout.
class GroupPrefilter {
   public:
    /// What each aggregate emits, per aggregate in order: its function and
    /// its value kind. Terms naming an output this cannot read exactly (a
    /// text min, a moment, a count distinct, a literal that does not compare
    /// exactly) are dropped; nullopt when none is left.
    [[nodiscard]] static auto resolve(const AggregatePrefilter& terms,
                                      const std::vector<ir::AggSpec>& aggregations,
                                      std::span<const ExprType> kinds)
        -> std::optional<GroupPrefilter>;

    /// Whether the group whose slots start at `slots` may pass. False only
    /// when a term is definitely false for the value it would emit.
    [[nodiscard]] auto may_pass(const AggSlotCore* slots) const noexcept -> bool;

    /// Indices of the groups in `[0, groups)` that may pass, ascending. Group
    /// g's slots start at `slots + g * stride`. Fans out over `pool` when it
    /// is non-null and `groups` is large.
    [[nodiscard]] auto survivors(const AggSlotCore* slots, std::size_t groups, std::size_t stride,
                                 WorkerPool* pool, std::size_t workers) const
        -> std::vector<std::uint32_t>;

   private:
    enum class Read : std::uint8_t { Count, Int, Double, Mean };
    struct Term {
        std::size_t agg = 0;
        Read read = Read::Int;
        ir::AggFunc func = ir::AggFunc::Count;
        ir::CompareOp op = ir::CompareOp::Eq;
        bool literal_is_int = true;
        std::int64_t int_literal = 0;
        double real_literal = 0.0;
    };
    std::vector<Term> terms_;
};

/// Keep the elements of `values` at the ascending indices `keep`, in order,
/// `stride` elements per index. In place: `keep[o] >= o`, so every read is at
/// or after the write it feeds. The caller shrinks the container.
template <typename T>
void compact_by_index(T* values, std::span<const std::uint32_t> keep, std::size_t stride = 1) {
    for (std::size_t o = 0; o < keep.size(); ++o) {
        const std::size_t from = static_cast<std::size_t>(keep[o]) * stride;
        const std::size_t to = o * stride;
        if (from == to) {
            continue;
        }
        for (std::size_t s = 0; s < stride; ++s) {
            values[to + s] = std::move(values[from + s]);
        }
    }
}

}  // namespace ibex::runtime
