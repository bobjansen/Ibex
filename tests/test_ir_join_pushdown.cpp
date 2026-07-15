#include <ibex/ir/join_pushdown.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto col(std::string name) -> ir::Expr {
    return ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}};
}

auto lit(std::int64_t v) -> ir::Expr {
    return ir::Expr{.node = ir::Literal{.value = v}};
}

auto eq(ir::Expr left, ir::Expr right) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{.op = ir::CompareOp::Eq,
                                            .left = ir::make_expr_ptr(std::move(left)),
                                            .right = ir::make_expr_ptr(std::move(right))}};
}

auto and_expr(ir::Expr left, ir::Expr right) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::And,
                                            .left = ir::make_expr_ptr(std::move(left)),
                                            .right = ir::make_expr_ptr(std::move(right))}};
}

auto make_scan(ir::NodeId id, std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(id, std::move(name));
}

auto make_join(ir::NodeId id, ir::JoinKind kind, std::vector<std::string> keys, ir::NodePtr left,
               ir::NodePtr right) -> ir::NodePtr {
    auto join = std::make_unique<ir::JoinNode>(id, kind, std::move(keys));
    join->add_child(std::move(left));
    join->add_child(std::move(right));
    return join;
}

auto make_filter(ir::NodeId id, ir::Expr pred, ir::NodePtr child) -> ir::NodePtr {
    auto filter = std::make_unique<ir::FilterNode>(id, std::move(pred));
    filter->add_child(std::move(child));
    return filter;
}

/// Schemas: `a` has {k, ax, shared}; `b` has {k, bx, shared}. `k` is the join
/// key; `shared` is a NON-key column both sides produce.
auto test_sources() -> ir::SourceSchemas {
    ir::SourceSchemas sources;
    sources.insert_or_assign(
        "a", ir::SchemaInfo::known({{.name = "k", .type = ir::ColumnType::Int64},
                                    {.name = "ax", .type = ir::ColumnType::Int64},
                                    {.name = "shared", .type = ir::ColumnType::Int64}}));
    sources.insert_or_assign(
        "b", ir::SchemaInfo::known({{.name = "k", .type = ir::ColumnType::Int64},
                                    {.name = "bx", .type = ir::ColumnType::Int64},
                                    {.name = "shared", .type = ir::ColumnType::Int64}}));
    // `c` is the semi/anti-join filter source: it carries whichever key is tested.
    sources.insert_or_assign(
        "c", ir::SchemaInfo::known({{.name = "ax", .type = ir::ColumnType::Int64},
                                    {.name = "bx", .type = ir::ColumnType::Int64}}));
    return sources;
}

/// Join(semi_kind, [semi_key], Join(inner_kind, [k], a, b), c).
auto semi_over_join_tree(ir::JoinKind semi_kind, std::string semi_key,
                         ir::JoinKind inner_kind = ir::JoinKind::Inner) -> ir::NodePtr {
    auto inner = make_join({2}, inner_kind, {"k"}, make_scan({3}, "a"), make_scan({4}, "b"));
    auto semi = std::make_unique<ir::JoinNode>(ir::NodeId{1}, semi_kind,
                                               std::vector<std::string>{std::move(semi_key)});
    semi->add_child(std::move(inner));
    semi->add_child(make_scan({5}, "c"));
    return semi;
}

auto filter_join_tree(ir::JoinKind kind, ir::Expr pred) -> ir::NodePtr {
    return make_filter({1}, std::move(pred),
                       make_join({2}, kind, {"k"}, make_scan({3}, "a"), make_scan({4}, "b")));
}

/// The scan under `side`, unwrapping at most one Filter.
auto scan_name(const ir::Node& side) -> std::string {
    const ir::Node* node = &side;
    if (node->kind() == ir::NodeKind::Filter) {
        node = node->children().front().get();
    }
    REQUIRE(node->kind() == ir::NodeKind::Scan);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    return static_cast<const ir::ScanNode&>(*node).source_name();
}

}  // namespace

TEST_CASE("join pushdown: pure-left conjunct moves below an inner join", "[ir][join_pushdown]") {
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("ax"), lit(1))), test_sources());
    // The Filter above the join is gone; the left child is now a Filter(Scan a).
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(scan_name(*out->children()[0]) == "a");
    REQUIRE(out->children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("join pushdown: pure-right conjunct moves below an inner join", "[ir][join_pushdown]") {
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("bx"), lit(1))), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Scan);
    REQUIRE(out->children()[1]->kind() == ir::NodeKind::Filter);
    REQUIRE(scan_name(*out->children()[1]) == "b");
}

TEST_CASE("join pushdown: conjuncts split, mixed conjunct stays above", "[ir][join_pushdown]") {
    // ax==1 AND bx==2 AND ax==bx: left, right, and mixed.
    auto pred =
        and_expr(and_expr(eq(col("ax"), lit(1)), eq(col("bx"), lit(2))), eq(col("ax"), col("bx")));
    auto out = ir::push_filters_into_joins(filter_join_tree(ir::JoinKind::Inner, std::move(pred)),
                                           test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Filter);  // the mixed conjunct
    const auto& join = *out->children().front();
    REQUIRE(join.kind() == ir::NodeKind::Join);
    REQUIRE(join.children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(join.children()[1]->kind() == ir::NodeKind::Filter);
}

TEST_CASE("join pushdown: join-key conjunct pre-filters both sides of an inner join",
          "[ir][join_pushdown]") {
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("k"), lit(5))), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children()[1]->kind() == ir::NodeKind::Filter);
}

TEST_CASE("join pushdown: unknown column stays above and does not crash", "[ir][join_pushdown]") {
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("mystery"), lit(1))), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Join);
    REQUIRE(out->children().front()->children()[0]->kind() == ir::NodeKind::Scan);
    REQUIRE(out->children().front()->children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("join pushdown: shared non-key column pushes LEFT only", "[ir][join_pushdown]") {
    // `shared` exists on both sides; above the join the name resolves to the
    // LEFT column, so the conjunct must go left and never right.
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("shared"), lit(1))), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("join pushdown: non-row-local conjunct stays above", "[ir][join_pushdown]") {
    // lag(ax) reads a neighbouring row; below the join it means something else.
    std::vector<ir::ExprPtr> args;
    args.push_back(ir::make_expr_ptr(col("ax")));
    auto lag =
        ir::Expr{.node = ir::CallExpr{.callee = "lag", .args = std::move(args), .named_args = {}}};
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(std::move(lag), lit(1))), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->children()[0]->kind() == ir::NodeKind::Scan);
    REQUIRE(out->children().front()->children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("join pushdown: unknown side schema keeps its conjuncts above", "[ir][join_pushdown]") {
    ir::SourceSchemas sources = test_sources();
    sources.erase("b");  // right side Unknown
    // bx is not provably right-side (right Unknown), so it must stay above.
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("bx"), lit(1))), sources);
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    // ...but a conjunct proven LEFT still moves.
    auto out2 = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Inner, eq(col("ax"), lit(1))), sources);
    REQUIRE(out2->kind() == ir::NodeKind::Join);
    REQUIRE(out2->children()[0]->kind() == ir::NodeKind::Filter);
}

TEST_CASE("join pushdown: left join pushes left conjuncts, never right ones",
          "[ir][join_pushdown]") {
    auto pred = and_expr(eq(col("ax"), lit(1)), eq(col("bx"), lit(2)));
    auto out = ir::push_filters_into_joins(filter_join_tree(ir::JoinKind::Left, std::move(pred)),
                                           test_sources());
    // bx==2 must stay above (pushing it would null-extend instead of filter).
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    const auto& join = *out->children().front();
    REQUIRE(join.children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(join.children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("join pushdown: left join degrades a key conjunct to a left-only push",
          "[ir][join_pushdown]") {
    auto out = ir::push_filters_into_joins(
        filter_join_tree(ir::JoinKind::Left, eq(col("k"), lit(5))), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("join pushdown: right join pushes right-only conjuncts, keys stay above",
          "[ir][join_pushdown]") {
    auto pred =
        and_expr(and_expr(eq(col("ax"), lit(1)), eq(col("bx"), lit(2))), eq(col("k"), lit(5)));
    auto out = ir::push_filters_into_joins(filter_join_tree(ir::JoinKind::Right, std::move(pred)),
                                           test_sources());
    // ax==1 (left side is null-supplying) and k==5 (output key column is
    // left-owned, null-extended for unmatched right rows) both stay above.
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    const auto& join = *out->children().front();
    REQUIRE(join.children()[0]->kind() == ir::NodeKind::Scan);
    REQUIRE(join.children()[1]->kind() == ir::NodeKind::Filter);
}

TEST_CASE("join pushdown: outer and asof joins are never rewritten", "[ir][join_pushdown]") {
    for (const auto kind : {ir::JoinKind::Outer, ir::JoinKind::Asof, ir::JoinKind::Semi,
                            ir::JoinKind::Anti, ir::JoinKind::Cross}) {
        auto out = ir::push_filters_into_joins(filter_join_tree(kind, eq(col("ax"), lit(1))),
                                               test_sources());
        REQUIRE(out->kind() == ir::NodeKind::Filter);
        REQUIRE(out->children().front()->children()[0]->kind() == ir::NodeKind::Scan);
        REQUIRE(out->children().front()->children()[1]->kind() == ir::NodeKind::Scan);
    }
}

TEST_CASE("join pushdown: filter over nested joins pushes through both levels",
          "[ir][join_pushdown]") {
    ir::SourceSchemas sources = test_sources();
    sources.insert_or_assign(
        "c", ir::SchemaInfo::known({{.name = "k", .type = ir::ColumnType::Int64},
                                    {.name = "cx", .type = ir::ColumnType::Int64}}));
    // Filter(ax==1, Join(Join(a, b), c)): ax belongs to the inner join's left.
    auto inner =
        make_join({2}, ir::JoinKind::Inner, {"k"}, make_scan({3}, "a"), make_scan({4}, "b"));
    auto outer = make_join({5}, ir::JoinKind::Inner, {"k"}, std::move(inner), make_scan({6}, "c"));
    auto tree = make_filter({1}, eq(col("ax"), lit(1)), std::move(outer));
    auto out = ir::push_filters_into_joins(std::move(tree), sources);
    REQUIRE(out->kind() == ir::NodeKind::Join);
    const auto& inner_join = *out->children()[0];
    REQUIRE(inner_join.kind() == ir::NodeKind::Join);
    REQUIRE(inner_join.children()[0]->kind() == ir::NodeKind::Filter);
    REQUIRE(scan_name(*inner_join.children()[0]) == "a");
}

TEST_CASE("join pushdown: schema flows through a Project side", "[ir][join_pushdown]") {
    // Left side is Project{k, ax}(Scan a) — its schema is Known even without a
    // source schema; refs to the projected-away `shared` cannot push left.
    std::vector<ir::ColumnRef> cols;
    cols.push_back(ir::ColumnRef{.name = "k"});
    cols.push_back(ir::ColumnRef{.name = "ax"});
    auto project = std::make_unique<ir::ProjectNode>(ir::NodeId{7}, std::move(cols));
    project->add_child(make_scan({3}, "a"));
    auto tree = make_filter(
        {1}, eq(col("shared"), lit(1)),
        make_join({2}, ir::JoinKind::Inner, {"k"}, std::move(project), make_scan({4}, "b")));
    auto out = ir::push_filters_into_joins(std::move(tree), test_sources());
    // `shared` is absent from the (closed) projected left schema and present on
    // the right, and it is not a key — so it pushes RIGHT.
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Project);
    REQUIRE(out->children()[1]->kind() == ir::NodeKind::Filter);
}

TEST_CASE("semi pushdown: key in the left side descends there", "[ir][join_pushdown][semi]") {
    // (a JOIN b on k) SEMI c on ax  ->  (a SEMI c on ax) JOIN b on k.
    auto out =
        ir::push_semi_joins_down(semi_over_join_tree(ir::JoinKind::Semi, "ax"), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(*out).kind() == ir::JoinKind::Inner);
    const auto& left = *out->children()[0];
    REQUIRE(left.kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(left).kind() == ir::JoinKind::Semi);
    REQUIRE(scan_name(*left.children()[0]) == "a");
    REQUIRE(scan_name(*left.children()[1]) == "c");
    REQUIRE(scan_name(*out->children()[1]) == "b");  // b untouched on the right
}

TEST_CASE("semi pushdown: key in the right side descends there", "[ir][join_pushdown][semi]") {
    // (a JOIN b on k) SEMI c on bx  ->  a JOIN (b SEMI c on bx) on k.
    auto out =
        ir::push_semi_joins_down(semi_over_join_tree(ir::JoinKind::Semi, "bx"), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    REQUIRE(scan_name(*out->children()[0]) == "a");  // a untouched on the left
    const auto& right = *out->children()[1];
    REQUIRE(right.kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(right).kind() == ir::JoinKind::Semi);
    REQUIRE(scan_name(*right.children()[0]) == "b");
    REQUIRE(scan_name(*right.children()[1]) == "c");
}

TEST_CASE("anti pushdown works the same as semi", "[ir][join_pushdown][semi]") {
    auto out =
        ir::push_semi_joins_down(semi_over_join_tree(ir::JoinKind::Anti, "ax"), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    const auto& left = *out->children()[0];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(left).kind() == ir::JoinKind::Anti);
    REQUIRE(scan_name(*left.children()[0]) == "a");
}

TEST_CASE("semi pushdown: key on both sides pushes left", "[ir][join_pushdown][semi]") {
    // The inner-join key k is in both a and b; a left push is the choice.
    auto out =
        ir::push_semi_joins_down(semi_over_join_tree(ir::JoinKind::Semi, "k"), test_sources());
    REQUIRE(out->kind() == ir::NodeKind::Join);
    const auto& left = *out->children()[0];
    REQUIRE(left.kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(left).kind() == ir::JoinKind::Semi);
    REQUIRE(scan_name(*left.children()[0]) == "a");
}

TEST_CASE("semi pushdown: does not descend a left join", "[ir][join_pushdown][semi]") {
    // A Left inner join changes which left rows survive; the semi filter must
    // stay above it.
    auto out = ir::push_semi_joins_down(
        semi_over_join_tree(ir::JoinKind::Semi, "ax", ir::JoinKind::Left), test_sources());
    // Unchanged: still Semi at the root.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(*out).kind() == ir::JoinKind::Semi);
    REQUIRE(out->children()[0]->kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    REQUIRE(static_cast<const ir::JoinNode&>(*out->children()[0]).kind() == ir::JoinKind::Left);
}
