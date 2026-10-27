// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "aggregate_prefilter.hpp"

#include <ibex/ir/node.hpp>
#include <ibex/runtime/pipeline.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "interpreter_internal.hpp"
#include "physical_plan.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

auto inverted(ir::CompareOp op) -> ir::CompareOp {
    switch (op) {
        case ir::CompareOp::Lt:
            return ir::CompareOp::Gt;
        case ir::CompareOp::Le:
            return ir::CompareOp::Ge;
        case ir::CompareOp::Gt:
            return ir::CompareOp::Lt;
        case ir::CompareOp::Ge:
            return ir::CompareOp::Le;
        default:
            return op;
    }
}

auto numeric_literal(const ir::Expr& expr, const ScalarRegistry* scalars)
    -> std::optional<std::variant<std::int64_t, double>> {
    const auto* literal = std::get_if<ir::Literal>(&expr.node);
    if (literal == nullptr) {
        // A bound scalar is as constant as a literal for this query.
        const auto* ref = std::get_if<ir::ColumnRef>(&expr.node);
        if (ref == nullptr || !ref->lexical || scalars == nullptr) {
            return std::nullopt;
        }
        const auto it = scalars->find(ref->name);
        if (it == scalars->end()) {
            return std::nullopt;
        }
        if (const auto* integer = std::get_if<std::int64_t>(&it->second)) {
            return *integer;
        }
        if (const auto* real = std::get_if<double>(&it->second)) {
            return *real;
        }
        return std::nullopt;
    }
    if (const auto* integer = std::get_if<std::int64_t>(&literal->value)) {
        return *integer;
    }
    if (const auto* real = std::get_if<double>(&literal->value)) {
        return *real;
    }
    return std::nullopt;
}

void collect_conjuncts(const ir::Expr& expr, std::vector<const ir::Expr*>& out) {
    if (const auto* logical = std::get_if<ir::LogicalExpr>(&expr.node);
        logical != nullptr && logical->op == ir::LogicalOp::And && logical->left != nullptr &&
        logical->right != nullptr) {
        collect_conjuncts(*logical->left, out);
        collect_conjuncts(*logical->right, out);
        return;
    }
    out.push_back(&expr);
}

template <typename T>
auto holds(ir::CompareOp op, T value, T literal) -> bool {
    switch (op) {
        case ir::CompareOp::Eq:
            return value == literal;
        case ir::CompareOp::Ne:
            return value != literal;
        case ir::CompareOp::Lt:
            return value < literal;
        case ir::CompareOp::Le:
            return value <= literal;
        case ir::CompareOp::Gt:
            return value > literal;
        case ir::CompareOp::Ge:
            return value >= literal;
    }
    return true;
}

// Integers of at most this magnitude convert to double exactly, so an integer
// compared with a double through a double comparison gets the exact answer.
constexpr double kExactDoubleLimit = 9007199254740992.0;  // 2^53

}  // namespace

auto aggregate_prefilter_terms(const ir::Expr& predicate, const ir::AggregateNode& aggregate,
                               const ScalarRegistry* scalars,
                               const robin_hood::unordered_map<std::string, std::string>* origins)
    -> AggregatePrefilter {
    std::vector<const ir::Expr*> conjuncts;
    collect_conjuncts(predicate, conjuncts);
    AggregatePrefilter terms;
    for (const ir::Expr* conjunct : conjuncts) {
        const auto* comparison = std::get_if<ir::CompareExpr>(&conjunct->node);
        if (comparison == nullptr || comparison->float_comparison != ir::FloatComparison::IEEE ||
            comparison->left == nullptr || comparison->right == nullptr) {
            continue;
        }
        const ir::ColumnRef* column = ir::as_column_ref(*comparison->left);
        auto literal = numeric_literal(*comparison->right, scalars);
        ir::CompareOp op = comparison->op;
        if (column == nullptr || !literal.has_value()) {
            column = ir::as_column_ref(*comparison->right);
            literal = numeric_literal(*comparison->left, scalars);
            op = inverted(op);
        }
        if (column == nullptr || !literal.has_value()) {
            continue;
        }
        std::string output = column->name;
        if (origins != nullptr) {
            const auto origin = origins->find(column->name);
            if (column->lexical || origin == origins->end()) {
                continue;
            }
            output = origin->second;
        }
        const bool is_output = std::ranges::any_of(
            aggregate.aggregations(), [&](const ir::AggSpec& spec) { return spec.alias == output; });
        // A group-by key with the same name as an aggregate cannot happen (the
        // output would have two columns of that name), but a key alone is not
        // an aggregate output, so it is not read from the slots.
        if (!is_output) {
            continue;
        }
        terms.push_back({.output = std::move(output), .op = op, .literal = *literal});
    }
    return terms;
}

auto aggregate_prefilter_for_source(const physical::Plan& plan, const ScalarRegistry* scalars)
    -> AggregatePrefilter {
    if (plan.steps.empty() || plan.source_node == nullptr ||
        plan.source_node->kind() != ir::NodeKind::Aggregate) {
        return {};
    }
    const auto& aggregate = ir::node_cast<ir::AggregateNode>(*plan.source_node);
    if (aggregate.group_by().empty()) {
        return {};
    }
    // Which visible name holds which aggregate output, walking up from the
    // source. Steps are ordered sink first, so the walk runs backwards. A
    // Project keeps the names it lists; an Update copies an output under a
    // new name, wraps it as `coalesce(output, c)`, or makes the name mean
    // something else. `coalesce` is sound here because the prefilter keeps
    // any group whose output is null: a non-null output IS the coalesced
    // value, and a null one is left for the filter above to judge against
    // `c`. Anything that can drop or duplicate rows ends the walk before the
    // filter, which must then not be used.
    robin_hood::unordered_map<std::string, std::string> origins;
    for (const ir::AggSpec& spec : aggregate.aggregations()) {
        origins.emplace(spec.alias, spec.alias);
    }
    const ir::Node* below = plan.source_node;
    for (std::size_t index = plan.steps.size(); index-- > 0;) {
        const ir::Node* node = plan.steps[index].node;
        if (node == nullptr || node->children().empty() ||
            node->children().front().get() != below) {
            return {};
        }
        if (node->kind() == ir::NodeKind::Filter) {
            return aggregate_prefilter_terms(ir::node_cast<ir::FilterNode>(*node).predicate(),
                                             aggregate, scalars, &origins);
        }
        if (node->kind() == ir::NodeKind::Project) {
            robin_hood::unordered_map<std::string, std::string> kept;
            for (const ir::ColumnRef& column : ir::node_cast<ir::ProjectNode>(*node).columns()) {
                if (const auto it = origins.find(column.name); it != origins.end()) {
                    kept.emplace(column.name, it->second);
                }
            }
            origins = std::move(kept);
        } else if (node->kind() == ir::NodeKind::Update) {
            const auto& update = ir::node_cast<ir::UpdateNode>(*node);
            if (update.guard() != nullptr || !update.group_by().empty() ||
                !update.tuple_fields().empty()) {
                return {};
            }
            // Fields read the input, so resolve every one before assigning.
            std::vector<std::pair<std::string, std::optional<std::string>>> assigned;
            for (const ir::FieldSpec& field : update.fields()) {
                const ir::Expr* source = &field.expr;
                if (const auto* call = std::get_if<ir::CallExpr>(&field.expr.node);
                    call != nullptr && call->callee == "coalesce" && call->args.size() == 2 &&
                    call->named_args.empty() && call->args[0] != nullptr &&
                    call->args[1] != nullptr &&
                    numeric_literal(*call->args[1], scalars).has_value()) {
                    source = call->args[0].get();
                }
                std::optional<std::string> origin;
                if (const auto* ref = std::get_if<ir::ColumnRef>(&source->node);
                    ref != nullptr && !ref->lexical) {
                    if (const auto it = origins.find(ref->name); it != origins.end()) {
                        origin = it->second;
                    }
                }
                assigned.emplace_back(field.alias, std::move(origin));
            }
            for (auto& [alias, origin] : assigned) {
                if (origin.has_value()) {
                    origins.insert_or_assign(alias, std::move(*origin));
                } else {
                    origins.erase(alias);
                }
            }
        } else {
            return {};
        }
        below = node;
    }
    return {};
}

auto GroupPrefilter::resolve(const AggregatePrefilter& terms,
                             const std::vector<ir::AggSpec>& aggregations,
                             std::span<const ExprType> kinds) -> std::optional<GroupPrefilter> {
    GroupPrefilter out;
    for (const auto& term : terms) {
        const auto it = std::ranges::find_if(
            aggregations, [&](const ir::AggSpec& spec) { return spec.alias == term.output; });
        if (it == aggregations.end()) {
            continue;
        }
        const auto agg = static_cast<std::size_t>(it - aggregations.begin());
        if (agg >= kinds.size()) {
            continue;
        }
        // What the aggregate emits for this output, read the way emission
        // reads it: Count as its count, Mean as sum / count in double, and
        // Sum, Min and Max as the Int64 or Double value their kind names.
        Term resolved{.agg = agg, .func = it->func, .op = term.op};
        switch (it->func) {
            case ir::AggFunc::Count:
                resolved.read = Read::Count;
                break;
            case ir::AggFunc::Mean:
                resolved.read = Read::Mean;
                break;
            case ir::AggFunc::Sum:
            case ir::AggFunc::Min:
            case ir::AggFunc::Max:
                if (kinds[agg] == ExprType::Int) {
                    resolved.read = Read::Int;
                } else if (kinds[agg] == ExprType::Double) {
                    resolved.read = Read::Double;
                } else {
                    continue;
                }
                break;
            default:
                continue;
        }
        const bool value_is_int = resolved.read == Read::Count || resolved.read == Read::Int;
        if (const auto* integer = std::get_if<std::int64_t>(&term.literal)) {
            resolved.literal_is_int = true;
            resolved.int_literal = *integer;
            // A double value against an integer literal compares in double;
            // keep that exact.
            if (!value_is_int && std::fabs(static_cast<double>(*integer)) >= kExactDoubleLimit) {
                continue;
            }
        } else {
            const double real = std::get<double>(term.literal);
            if (!std::isfinite(real)) {
                continue;
            }
            resolved.literal_is_int = false;
            resolved.real_literal = real;
        }
        out.terms_.push_back(resolved);
    }
    if (out.terms_.empty()) {
        return std::nullopt;
    }
    return out;
}

auto GroupPrefilter::may_pass(const AggSlotCore* slots) const noexcept -> bool {
    for (const Term& term : terms_) {
        const AggSlotCore& slot = slots[term.agg];
        switch (term.read) {
            case Read::Count: {
                const std::int64_t value = slot.count;
                if (term.literal_is_int
                        ? !holds(term.op, value, term.int_literal)
                        : (std::fabs(static_cast<double>(value)) < kExactDoubleLimit &&
                           !holds(term.op, static_cast<double>(value), term.real_literal))) {
                    return false;
                }
                break;
            }
            case Read::Int: {
                // No row reached it: the output is null, and the filter decides.
                if (!slot.present()) {
                    break;
                }
                const std::int64_t value = slot.int_value;
                if (term.literal_is_int
                        ? !holds(term.op, value, term.int_literal)
                        : (std::fabs(static_cast<double>(value)) < kExactDoubleLimit &&
                           !holds(term.op, static_cast<double>(value), term.real_literal))) {
                    return false;
                }
                break;
            }
            case Read::Double:
            case Read::Mean: {
                if (term.read == Read::Mean ? slot.count <= 0 : !slot.present()) {
                    break;
                }
                const double value = term.read == Read::Mean
                                         ? slot.double_value / static_cast<double>(slot.count)
                                         : slot.double_value;
                // NaN is left to the filter, whatever its ordering rule is.
                if (std::isnan(value)) {
                    break;
                }
                const double literal =
                    term.literal_is_int ? static_cast<double>(term.int_literal) : term.real_literal;
                if (!holds(term.op, value, literal)) {
                    return false;
                }
                break;
            }
        }
    }
    return true;
}

auto GroupPrefilter::survivors(const AggSlotCore* slots, std::size_t groups, std::size_t stride,
                               WorkerPool* pool, std::size_t workers, std::size_t max_keep) const
    -> std::optional<std::vector<std::uint32_t>> {
    constexpr std::size_t kSerialGroups = std::size_t{1} << 16U;
    const auto scan = [&](std::size_t lo, std::size_t hi, std::vector<std::uint32_t>& out) {
        for (std::size_t g = lo; g < hi; ++g) {
            if (may_pass(slots + (g * stride))) {
                out.push_back(static_cast<std::uint32_t>(g));
            }
        }
    };
    if (pool == nullptr || workers < 2 || groups < kSerialGroups) {
        std::vector<std::uint32_t> out;
        scan(0, groups, out);
        if (out.size() > max_keep) {
            return std::nullopt;
        }
        return out;
    }
    const std::size_t ranges = workers * 4;
    const std::size_t grain = (groups + ranges - 1) / ranges;
    std::vector<std::vector<std::uint32_t>> parts(ranges);
    std::atomic<std::size_t> cursor{0};
    auto batch = pool->submit(workers, [&](std::size_t) {
        while (true) {
            const std::size_t r = cursor.fetch_add(1, std::memory_order_relaxed);
            if (r >= ranges) {
                return;
            }
            const std::size_t lo = std::min(groups, r * grain);
            const std::size_t hi = std::min(groups, lo + grain);
            scan(lo, hi, parts[r]);
        }
    });
    batch.wait();
    std::size_t total = 0;
    for (const auto& part : parts) {
        total += part.size();
    }
    if (total > max_keep) {
        return std::nullopt;
    }
    std::vector<std::uint32_t> out;
    out.reserve(total);
    for (const auto& part : parts) {
        out.insert(out.end(), part.begin(), part.end());
    }
    return out;
}

}  // namespace ibex::runtime
