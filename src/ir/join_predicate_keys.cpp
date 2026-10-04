// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/join_predicate_keys.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {

namespace {

auto type_name(ColumnType type) -> std::string_view {
    switch (type) {
        case ColumnType::Int32:
            return "Int32";
        case ColumnType::Int64:
            return "Int64";
        case ColumnType::Float32:
            return "Float32";
        case ColumnType::Float64:
            return "Float64";
        case ColumnType::Bool:
            return "Bool";
        case ColumnType::String:
            return "String";
        case ColumnType::Date:
            return "Date";
        case ColumnType::Timestamp:
            return "Timestamp";
        case ColumnType::Categorical:
            return "String";  // a user only ever writes String for it
        case ColumnType::Decimal:
            return "Decimal";
    }
    return "?";
}

/// The conjuncts of `expr` if every one is `column == column`; empty otherwise.
auto equality_conjuncts(const Expr& expr, std::vector<const CompareExpr*>& out) -> bool {
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node)) {
        return logical->op == LogicalOp::And && logical->left && logical->right &&
               equality_conjuncts(*logical->left, out) && equality_conjuncts(*logical->right, out);
    }
    const auto* compare = std::get_if<CompareExpr>(&expr.node);
    if (compare == nullptr || compare->op != CompareOp::Eq || !compare->left || !compare->right) {
        return false;
    }
    const auto* lhs = std::get_if<ColumnRef>(&compare->left->node);
    const auto* rhs = std::get_if<ColumnRef>(&compare->right->node);
    if (lhs == nullptr || rhs == nullptr || lhs->lexical || rhs->lexical) {
        return false;
    }
    out.push_back(compare);
    return true;
}

enum class Side : std::uint8_t { Left, Right, Unknown, Neither };

/// Which input `ref` names, as the predicate join itself resolves it: an
/// explicit `left()`/`right()`, else the one input holding the name.
auto side_of(const ColumnRef& ref, const SchemaInfo& left, const SchemaInfo& right) -> Side {
    if (ref.side == JoinSide::Left) {
        return Side::Left;
    }
    if (ref.side == JoinSide::Right) {
        return Side::Right;
    }
    if (!left.is_known() || !right.is_known() || left.is_open() || right.is_open()) {
        return Side::Unknown;
    }
    const bool in_left = left.find(ref.name) != nullptr;
    const bool in_right = right.find(ref.name) != nullptr;
    if (in_left == in_right) {
        return Side::Neither;  // ambiguous or absent: the join reports it
    }
    return in_left ? Side::Left : Side::Right;
}

/// Why `l == r` cannot become a key, or nullopt when it can.
auto key_blocker(const SchemaField* l, const SchemaField* r) -> std::optional<std::string> {
    if (l == nullptr || r == nullptr || !l->type.has_value() || !r->type.has_value()) {
        return "the column types are not known when the plan is made";
    }
    const auto lt = *l->type;
    const auto rt = *r->type;
    const auto stringy = [](ColumnType t) {
        return t == ColumnType::String || t == ColumnType::Categorical;
    };
    if (lt != rt && !(stringy(lt) && stringy(rt))) {
        return "`" + l->name + "` is " + std::string(type_name(lt)) + " but `" + r->name + "` is " +
               std::string(type_name(rt)) + "; a key needs one type, so cast one side";
    }
    if (lt == ColumnType::Float32 || lt == ColumnType::Float64) {
        return "`" + l->name + "` and `" + r->name +
               "` are floating-point, where `==` and a hash match disagree on NaN";
    }
    if (lt == ColumnType::Decimal &&
        (!l->decimal.has_value() || !r->decimal.has_value() || *l->decimal != *r->decimal)) {
        return "`" + l->name + "` and `" + r->name + "` are decimals of different scales";
    }
    if (l->name == r->name) {
        return "both sides call the key `" + l->name +
               "`, and as a key the two columns would fold into one";
    }
    return std::nullopt;
}

void rewrite(Node& node, const SourceSchemas& sources, std::vector<std::string>& warnings) {
    for (auto& child : node.mutable_children()) {
        if (child != nullptr) {
            rewrite(*child, sources, warnings);
        }
    }
    if (node.kind() != NodeKind::Join || node.children().size() != 2 ||
        node.children()[0] == nullptr || node.children()[1] == nullptr) {
        return;
    }
    auto& join = node_cast<JoinNode>(node);
    if (!join.predicate().has_value() || join.null_match() != NullMatch::Never) {
        return;
    }
    switch (join.kind()) {
        case JoinKind::Inner:
        case JoinKind::Left:
        case JoinKind::Right:
        case JoinKind::Outer:
        case JoinKind::Semi:
        case JoinKind::Anti:
            break;
        default:
            return;
    }
    std::vector<const CompareExpr*> equalities;
    if (!equality_conjuncts(*join.predicate(), equalities)) {
        return;  // a genuine theta join
    }

    const SchemaInfo left = infer_schema(*join.children()[0], sources);
    const SchemaInfo right = infer_schema(*join.children()[1], sources);
    std::vector<JoinKey> keys = join.keys();
    std::string pairs;
    std::optional<std::string> blocker;
    for (const CompareExpr* eq : equalities) {
        const auto& a = std::get<ColumnRef>(eq->left->node);
        const auto& b = std::get<ColumnRef>(eq->right->node);
        const Side sa = side_of(a, left, right);
        const Side sb = side_of(b, left, right);
        if (sa == Side::Neither || sb == Side::Neither) {
            return;  // the join itself reports an ambiguous or missing name
        }
        if ((sa == sb && sa != Side::Unknown)) {
            return;  // a one-sided term, not a key: leave the predicate alone
        }
        const ColumnRef& l = (sa == Side::Right || sb == Side::Left) ? b : a;
        const ColumnRef& r = (sa == Side::Right || sb == Side::Left) ? a : b;
        if (!pairs.empty()) {
            pairs += ", ";
        }
        pairs += l.name + " = " + r.name;
        if (blocker.has_value()) {
            continue;
        }
        if (sa == Side::Unknown || sb == Side::Unknown) {
            blocker = "the input columns are not known when the plan is made";
            continue;
        }
        blocker = key_blocker(left.find(l.name), right.find(r.name));
        if (!blocker.has_value()) {
            keys.emplace_back(l.name, r.name);
        }
    }
    if (blocker.has_value()) {
        warnings.push_back("join on an equality predicate runs as a nested loop: " + *blocker +
                           ". Write `on { " + pairs + " }` for a hash join.");
        return;
    }
    join.set_keys(std::move(keys));
    join.set_predicate(std::nullopt);
}

}  // namespace

auto equality_predicates_to_join_keys(NodePtr root, const SourceSchemas& sources)
    -> JoinPredicateKeyRewrite {
    JoinPredicateKeyRewrite out;
    if (root != nullptr) {
        rewrite(*root, sources, out.warnings);
    }
    out.plan = std::move(root);
    return out;
}

}  // namespace ibex::ir
