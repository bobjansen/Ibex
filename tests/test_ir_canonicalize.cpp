#include <ibex/ir/canonicalize.hpp>
#include <ibex/ir/node.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <utility>

using namespace ibex;

namespace {

auto make_filter(ir::NodeId id) -> ir::NodePtr {
    // Use a non-trivial predicate (`x == 1`) so canonicalize R17 doesn't
    // simplify the Filter away — these tests want the structural Filter node.
    auto col =
        std::make_unique<ir::FilterExpr>(ir::FilterExpr{.node = ir::FilterColumn{.name = "x"}});
    auto lit = std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{.node = ir::FilterLiteral{.value = std::int64_t{1}}});
    auto pred = std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        .node = ir::FilterCmp{
            .op = ir::CompareOp::Eq, .left = std::move(col), .right = std::move(lit)}});
    return std::make_unique<ir::FilterNode>(id, std::move(pred));
}

auto make_order(ir::NodeId id, std::vector<ir::OrderKey> keys) -> ir::NodePtr {
    return std::make_unique<ir::OrderNode>(id, std::move(keys));
}

auto make_project(ir::NodeId id, std::vector<std::string> cols) -> ir::NodePtr {
    std::vector<ir::ColumnRef> refs;
    refs.reserve(cols.size());
    for (auto& c : cols) {
        refs.push_back(ir::ColumnRef{.name = std::move(c), .source = {0}});
    }
    return std::make_unique<ir::ProjectNode>(id, std::move(refs));
}

auto make_rename(ir::NodeId id, std::vector<ir::RenameSpec> renames) -> ir::NodePtr {
    return std::make_unique<ir::RenameNode>(id, std::move(renames));
}

auto make_scan(ir::NodeId id, std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(id, std::move(name));
}

auto make_head(ir::NodeId id, std::size_t n, std::vector<ir::ColumnRef> group_by = {})
    -> ir::NodePtr {
    return std::make_unique<ir::HeadNode>(id, n, std::move(group_by));
}

auto make_tail(ir::NodeId id, std::size_t n, std::vector<ir::ColumnRef> group_by = {})
    -> ir::NodePtr {
    return std::make_unique<ir::TailNode>(id, n, std::move(group_by));
}

auto with_child(ir::NodePtr parent, ir::NodePtr child) -> ir::NodePtr {
    parent->add_child(std::move(child));
    return parent;
}

}  // namespace

TEST_CASE("canonicalize R1: Filter(Order(x)) sinks to Order(Filter(x))", "[ir][canonicalize]") {
    auto tree = with_child(
        make_filter({1}),
        with_child(make_order({2}, {{.name = "k", .ascending = true}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Order);
    REQUIRE(out->children().size() == 1);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R2: Project(Order(x)) sinks when keys preserved", "[ir][canonicalize]") {
    auto tree = with_child(
        make_project({1}, {"a", "b"}),
        with_child(make_order({2}, {{.name = "a", .ascending = true}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Order);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Project);
}

TEST_CASE("canonicalize R2: Project(Order(x)) preserved when keys dropped", "[ir][canonicalize]") {
    // Order key "c" is not in the projection, so the rewrite must NOT fire
    // (sinking Order would lose the key).
    auto tree = with_child(
        make_project({1}, {"a", "b"}),
        with_child(make_order({2}, {{.name = "c", .ascending = true}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Project);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Order);
}

TEST_CASE("canonicalize R3: Order(Rename(x)) rises to Rename(Order(x)) with remapped keys",
          "[ir][canonicalize]") {
    auto tree = with_child(
        make_order({1}, {{.name = "key", .ascending = true}}),
        with_child(make_rename({2}, {{.new_name = "key", .old_name = "k"}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Rename);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Order);
    const auto& order = static_cast<const ir::OrderNode&>(*out->children().front());
    REQUIRE(order.keys().size() == 1);
    REQUIRE(order.keys().front().name == "k");
}

TEST_CASE("canonicalize R4: Head past Project and Rename chain", "[ir][canonicalize]") {
    auto tree = with_child(
        make_head({1}, 10),
        with_child(make_project({2}, {"a", "b"}),
                   with_child(make_rename({3}, {{.new_name = "b", .old_name = "orig_b"}}),
                              make_scan({4}, "t"))));
    auto out = ir::canonicalize(std::move(tree));
    // Expect: Project(Rename(Head(Scan)))
    REQUIRE(out->kind() == ir::NodeKind::Project);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Rename);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Head);
}

TEST_CASE("canonicalize R4: Tail past Rename remaps grouped columns", "[ir][canonicalize]") {
    std::vector<ir::ColumnRef> gb{{.name = "new_g", .source = {0}}};
    auto tree = with_child(make_tail({1}, 5, std::move(gb)),
                           with_child(make_rename({2}, {{.new_name = "new_g", .old_name = "g"}}),
                                      make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Rename);
    const auto& tail = static_cast<const ir::TailNode&>(*out->children().front());
    REQUIRE(tail.group_by().size() == 1);
    REQUIRE(tail.group_by().front().name == "g");
}

namespace {

auto make_update_row_local(ir::NodeId id, std::string alias, std::string col_ref) -> ir::NodePtr {
    std::vector<ir::FieldSpec> fields;
    ir::Expr e{.node = ir::ColumnRef{.name = std::move(col_ref), .source = {0}}};
    fields.push_back(ir::FieldSpec{.alias = std::move(alias), .expr = std::move(e)});
    return std::make_unique<ir::UpdateNode>(id, std::move(fields));
}

auto make_update_rolling(ir::NodeId id, std::string alias, std::string col_ref) -> ir::NodePtr {
    std::vector<ir::FieldSpec> fields;
    auto col_arg = std::make_shared<ir::Expr>(
        ir::Expr{.node = ir::ColumnRef{.name = std::move(col_ref), .source = {0}}});
    ir::CallExpr call{.callee = "rolling_sum", .args = {std::move(col_arg)}, .named_args = {}};
    fields.push_back(
        ir::FieldSpec{.alias = std::move(alias), .expr = ir::Expr{.node = std::move(call)}});
    return std::make_unique<ir::UpdateNode>(id, std::move(fields));
}

}  // namespace

TEST_CASE("canonicalize R5: Project(Filter(x)) fuses to FilterProject(x)", "[ir][canonicalize]") {
    auto tree = with_child(make_project({1}, {"a", "b"}),
                           with_child(make_filter({2}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::FilterProject);
    REQUIRE(out->children().size() == 1);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& fp = static_cast<const ir::FilterProjectNode&>(*out);
    REQUIRE(fp.columns().size() == 2);
    REQUIRE(fp.columns()[0].name == "a");
}

TEST_CASE("canonicalize R6: Project(Update(Filter(x))) fuses when row-local",
          "[ir][canonicalize]") {
    auto tree = with_child(make_project({1}, {"a", "b"}),
                           with_child(make_update_row_local({2}, "b", "raw_b"),
                                      with_child(make_filter({3}), make_scan({4}, "t"))));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::FilterUpdateProject);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(*out);
    REQUIRE(fup.fields().size() == 1);
    REQUIRE(fup.fields().front().alias == "b");
    REQUIRE(fup.project_columns().size() == 2);
}

TEST_CASE("canonicalize R6: Project(Update(Filter(x))) preserved when update is cross-row",
          "[ir][canonicalize]") {
    // rolling_sum reaches across rows, so R6 must not fire — the shape stays
    // Project(Update(Filter(Scan))).
    auto tree = with_child(make_project({1}, {"a", "b"}),
                           with_child(make_update_rolling({2}, "b", "raw_b"),
                                      with_child(make_filter({3}), make_scan({4}, "t"))));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Project);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Update);
}

TEST_CASE("canonicalize R7: Head(Filter(x)) fuses to FilterHead(x)", "[ir][canonicalize]") {
    auto tree = with_child(make_head({1}, 10), with_child(make_filter({2}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::FilterHead);
    REQUIRE(out->children().size() == 1);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& fh = static_cast<const ir::FilterHeadNode&>(*out);
    REQUIRE(fh.count() == 10);
}

TEST_CASE("canonicalize R7: Head(Filter(x)) preserved when Head has group_by",
          "[ir][canonicalize]") {
    std::vector<ir::ColumnRef> gb{{.name = "g", .source = {0}}};
    auto tree = with_child(make_head({1}, 10, std::move(gb)),
                           with_child(make_filter({2}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Head);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Filter);
}

TEST_CASE("canonicalize R8: Tail(Filter(x)) fuses to FilterTail(x)", "[ir][canonicalize]") {
    auto tree = with_child(make_tail({1}, 5), with_child(make_filter({2}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::FilterTail);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& ft = static_cast<const ir::FilterTailNode&>(*out);
    REQUIRE(ft.count() == 5);
}

namespace {

auto make_filter_cmp_col(ir::NodeId id, std::string col_name, std::int64_t threshold)
    -> ir::NodePtr {
    auto left = std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{.node = ir::FilterColumn{.name = std::move(col_name)}});
    auto right = std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{.node = ir::FilterLiteral{.value = threshold}});
    auto cmp = std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        .node = ir::FilterCmp{
            .op = ir::CompareOp::Lt, .left = std::move(left), .right = std::move(right)}});
    return std::make_unique<ir::FilterNode>(id, std::move(cmp));
}

}  // namespace

TEST_CASE("canonicalize R9: Rename(Rename(x)) composes to single Rename", "[ir][canonicalize]") {
    // Outer: a→final; inner: raw→a.  Composed: raw→final.
    auto tree = with_child(
        make_rename({1}, {{.new_name = "final", .old_name = "a"}}),
        with_child(make_rename({2}, {{.new_name = "a", .old_name = "raw"}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Rename);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& rn = static_cast<const ir::RenameNode&>(*out);
    REQUIRE(rn.renames().size() == 1);
    REQUIRE(rn.renames().front().new_name == "final");
    REQUIRE(rn.renames().front().old_name == "raw");
}

TEST_CASE("canonicalize R9: opposing renames cancel", "[ir][canonicalize]") {
    // k→key then key→k should cancel to nothing.
    auto tree = with_child(
        make_rename({1}, {{.new_name = "k", .old_name = "key"}}),
        with_child(make_rename({2}, {{.new_name = "key", .old_name = "k"}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R10: drops identity Rename", "[ir][canonicalize]") {
    auto tree =
        with_child(make_rename({1}, {{.new_name = "a", .old_name = "a"}}), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R11: Filter(Rename(x)) bubbles Rename up with remapped predicate",
          "[ir][canonicalize]") {
    auto tree = with_child(
        make_filter_cmp_col({1}, "key", 500),
        with_child(make_rename({2}, {{.new_name = "key", .old_name = "k"}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Rename);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Filter);
    // The predicate's column ref should now reference the pre-rename name "k".
    const auto& filter = static_cast<const ir::FilterNode&>(*out->children().front());
    const auto& cmp = std::get<ir::FilterCmp>(filter.predicate().node);
    const auto& col = std::get<ir::FilterColumn>(cmp.left->node);
    REQUIRE(col.name == "k");
}

TEST_CASE("canonicalize R12: Filter(Update(x)) sinks when predicate is independent",
          "[ir][canonicalize]") {
    // Predicate references "a" (not produced by update, which adds "b"),
    // so Filter can move under Update.
    auto tree =
        with_child(make_filter_cmp_col({1}, "a", 500),
                   with_child(make_update_row_local({2}, "b", "raw_b"), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Update);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R12: Filter(Update(x)) preserved when predicate reads update output",
          "[ir][canonicalize]") {
    // Predicate references "b", which the update produces — swapping would
    // change the predicate's value.
    auto tree =
        with_child(make_filter_cmp_col({1}, "b", 500),
                   with_child(make_update_row_local({2}, "b", "raw_b"), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Update);
}

TEST_CASE("canonicalize R12 then R6: Project(Filter(Update(x))) fuses end-to-end",
          "[ir][canonicalize]") {
    auto tree = with_child(
        make_project({1}, {"a", "b"}),
        with_child(make_filter_cmp_col({2}, "a", 500),
                   with_child(make_update_row_local({3}, "b", "raw_b"), make_scan({4}, "t"))));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::FilterUpdateProject);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R13: Head(Head(x)) collapses to tighter bound", "[ir][canonicalize]") {
    auto tree =
        with_child(make_head({1}, 10), with_child(make_head({2}, 100), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Head);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& h = static_cast<const ir::HeadNode&>(*out);
    REQUIRE(h.count() == 10);
}

TEST_CASE("canonicalize R14: Tail(Tail(x)) collapses to tighter bound", "[ir][canonicalize]") {
    auto tree =
        with_child(make_tail({1}, 1000), with_child(make_tail({2}, 50), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Tail);
    const auto& t = static_cast<const ir::TailNode&>(*out);
    REQUIRE(t.count() == 50);
}

namespace {

auto make_filter_eq_const(ir::NodeId id, std::string col_name, std::int64_t value) -> ir::NodePtr {
    auto left = std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{.node = ir::FilterColumn{.name = std::move(col_name)}});
    auto right =
        std::make_unique<ir::FilterExpr>(ir::FilterExpr{.node = ir::FilterLiteral{.value = value}});
    auto eq = std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        .node = ir::FilterCmp{
            .op = ir::CompareOp::Eq, .left = std::move(left), .right = std::move(right)}});
    return std::make_unique<ir::FilterNode>(id, std::move(eq));
}

}  // namespace

TEST_CASE("canonicalize R15: Order key pinned by equality Filter is dropped",
          "[ir][canonicalize]") {
    // filter a == 5, order a asc, b desc  — `a` is constant so Order on `a`
    // is redundant; `b` remains.
    auto tree = with_child(
        make_order({1}, {{.name = "a", .ascending = true}, {.name = "b", .ascending = false}}),
        with_child(make_filter_eq_const({2}, "a", 5), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Order);
    const auto& order = static_cast<const ir::OrderNode&>(*out);
    REQUIRE(order.keys().size() == 1);
    REQUIRE(order.keys().front().name == "b");
}

TEST_CASE("canonicalize R15: Order dissolves when all keys are pinned", "[ir][canonicalize]") {
    auto tree = with_child(make_order({1}, {{.name = "a", .ascending = true}}),
                           with_child(make_filter_eq_const({2}, "a", 5), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize composes R3, R11, R1: Filter(Order(Rename(x)))", "[ir][canonicalize]") {
    auto tree =
        with_child(make_filter({1}),
                   with_child(make_order({2}, {{.name = "key", .ascending = true}}),
                              with_child(make_rename({3}, {{.new_name = "key", .old_name = "k"}}),
                                         make_scan({4}, "t"))));
    auto out = ir::canonicalize(std::move(tree));
    // R3 rewrites Order(Rename) → Rename(Order); R11 then lifts Rename above
    // Filter; R1 sinks Filter below Order. Final shape: Rename(Order(Filter(Scan)))
    // — the sort runs on already-filtered rows with the pre-rename schema.
    REQUIRE(out->kind() == ir::NodeKind::Rename);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Order);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->children().front()->children().front()->kind() ==
            ir::NodeKind::Scan);
}

namespace {

auto fexpr(ir::FilterExpr e) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(std::move(e));
}

auto blit(bool b) -> ir::FilterExprPtr {
    return fexpr({.node = ir::FilterLiteral{.value = b}});
}

auto ilit(std::int64_t v) -> ir::FilterExprPtr {
    return fexpr({.node = ir::FilterLiteral{.value = v}});
}

auto col_ref(std::string name) -> ir::FilterExprPtr {
    return fexpr({.node = ir::FilterColumn{.name = std::move(name)}});
}

auto make_filter_with(ir::NodeId id, ir::FilterExprPtr pred) -> ir::NodePtr {
    return std::make_unique<ir::FilterNode>(id, std::move(pred));
}

}  // namespace

TEST_CASE("canonicalize R17: Filter(true) is dropped", "[ir][canonicalize]") {
    auto tree = with_child(make_filter_with({1}, blit(true)), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R17: Filter(false) becomes Head(0)", "[ir][canonicalize]") {
    auto tree = with_child(make_filter_with({1}, blit(false)), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Head);
    const auto& h = static_cast<const ir::HeadNode&>(*out);
    REQUIRE(h.count() == 0);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R17: x AND true -> x; NOT NOT x -> x", "[ir][canonicalize]") {
    // (col == 5) AND true  →  col == 5
    auto eq = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Eq, .left = col_ref("c"), .right = ilit(5)}});
    auto pred = fexpr({.node = ir::FilterAnd{.left = std::move(eq), .right = blit(true)}});
    auto tree = with_child(make_filter_with({1}, std::move(pred)), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    const auto& f = static_cast<const ir::FilterNode&>(*out);
    const auto* cmp = std::get_if<ir::FilterCmp>(&f.predicate().node);
    REQUIRE(cmp != nullptr);
    REQUIRE(cmp->op == ir::CompareOp::Eq);

    // NOT NOT (col == 5)  →  col == 5
    auto eq2 = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Eq, .left = col_ref("c"), .right = ilit(5)}});
    auto nn = fexpr({.node = ir::FilterNot{
                         .operand = fexpr({.node = ir::FilterNot{.operand = std::move(eq2)}})}});
    auto tree2 = with_child(make_filter_with({3}, std::move(nn)), make_scan({4}, "t"));
    auto out2 = ir::canonicalize(std::move(tree2));
    REQUIRE(out2->kind() == ir::NodeKind::Filter);
}

TEST_CASE("canonicalize R17: literal-only comparison folds to bool", "[ir][canonicalize]") {
    // 5 == 5 → true → Filter dropped
    auto pred =
        fexpr({.node = ir::FilterCmp{.op = ir::CompareOp::Eq, .left = ilit(5), .right = ilit(5)}});
    auto tree = with_child(make_filter_with({1}, std::move(pred)), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R17: arithmetic on literals folds", "[ir][canonicalize]") {
    // (col == 2 + 3) → (col == 5), still a non-trivial Filter.
    auto add = fexpr(
        {.node = ir::FilterArith{.op = ir::ArithmeticOp::Add, .left = ilit(2), .right = ilit(3)}});
    auto pred =
        fexpr({.node = ir::FilterCmp{
                   .op = ir::CompareOp::Eq, .left = col_ref("c"), .right = std::move(add)}});
    auto tree = with_child(make_filter_with({1}, std::move(pred)), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    const auto& f = static_cast<const ir::FilterNode&>(*out);
    const auto* cmp = std::get_if<ir::FilterCmp>(&f.predicate().node);
    REQUIRE(cmp != nullptr);
    const auto* lit = std::get_if<ir::FilterLiteral>(&cmp->right->node);
    REQUIRE(lit != nullptr);
    REQUIRE(std::get<std::int64_t>(lit->value) == 5);
}

TEST_CASE("canonicalize R16: Head(Order(x)) fuses to TopK(First)", "[ir][canonicalize]") {
    auto tree = with_child(
        make_head({1}, 10),
        with_child(make_order({2}, {{.name = "price", .ascending = false}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::TopK);
    const auto& topk = static_cast<const ir::TopKNode&>(*out);
    REQUIRE(topk.count() == 10);
    REQUIRE(topk.keep_mode() == ir::TopKNode::KeepMode::First);
    REQUIRE(topk.keys().size() == 1);
    REQUIRE(topk.keys().front().name == "price");
    REQUIRE(topk.keys().front().ascending == false);
    REQUIRE(topk.children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R16: Tail(Order(x)) fuses to TopK(Last) preserving group_by",
          "[ir][canonicalize]") {
    std::vector<ir::ColumnRef> gb{{.name = "symbol", .source = {0}}};
    auto tree = with_child(
        make_tail({1}, 3, std::move(gb)),
        with_child(make_order({2}, {{.name = "score", .ascending = true}}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::TopK);
    const auto& topk = static_cast<const ir::TopKNode&>(*out);
    REQUIRE(topk.count() == 3);
    REQUIRE(topk.keep_mode() == ir::TopKNode::KeepMode::Last);
    REQUIRE(topk.group_by().size() == 1);
    REQUIRE(topk.group_by().front().name == "symbol");
}

TEST_CASE("canonicalize R19: adjacent Filters merge into Filter(AND)", "[ir][canonicalize]") {
    auto p_outer = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Gt, .left = col_ref("a"), .right = ilit(0)}});
    auto p_inner = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Lt, .left = col_ref("b"), .right = ilit(10)}});
    auto tree =
        with_child(make_filter_with({1}, std::move(p_outer)),
                   with_child(make_filter_with({2}, std::move(p_inner)), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Scan);
    const auto& f = static_cast<const ir::FilterNode&>(*out);
    REQUIRE(std::holds_alternative<ir::FilterAnd>(f.predicate().node));
}

namespace {

auto make_aggregate(ir::NodeId id, std::vector<std::string> gb_cols, std::vector<ir::AggSpec> aggs)
    -> ir::NodePtr {
    std::vector<ir::ColumnRef> gb;
    gb.reserve(gb_cols.size());
    for (auto& c : gb_cols) {
        gb.push_back(ir::ColumnRef{.name = std::move(c), .source = {0}});
    }
    return std::make_unique<ir::AggregateNode>(id, std::move(gb), std::move(aggs));
}

}  // namespace

TEST_CASE("canonicalize R18: Filter on group_by column pushes below Aggregate",
          "[ir][canonicalize]") {
    // Filter(g == 1, Aggregate(group_by=[g], [sum(x) as s], scan))
    auto pred = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Eq, .left = col_ref("g"), .right = ilit(1)}});
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{.func = ir::AggFunc::Sum,
                               .column = ir::ColumnRef{.name = "x", .source = {0}},
                               .alias = "s"});
    auto tree =
        with_child(make_filter_with({1}, std::move(pred)),
                   with_child(make_aggregate({2}, {"g"}, std::move(aggs)), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Aggregate);
    REQUIRE(out->children().size() == 1);
    // R18 pushes Filter below Aggregate; R20 then inserts a column-pruning
    // Project between Aggregate and Filter, which fuses with the Filter via R5
    // into a FilterProject node.
    REQUIRE(out->children().front()->kind() == ir::NodeKind::FilterProject);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R18: Filter on agg alias stays above Aggregate (HAVING-style)",
          "[ir][canonicalize]") {
    // Filter(s > 5, Aggregate(group_by=[g], [sum(x) as s], scan))
    auto pred = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Gt, .left = col_ref("s"), .right = ilit(5)}});
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{.func = ir::AggFunc::Sum,
                               .column = ir::ColumnRef{.name = "x", .source = {0}},
                               .alias = "s"});
    auto tree =
        with_child(make_filter_with({1}, std::move(pred)),
                   with_child(make_aggregate({2}, {"g"}, std::move(aggs)), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Aggregate);
}

TEST_CASE("canonicalize R18: Filter does not push past Aggregate with empty group_by",
          "[ir][canonicalize]") {
    // Filter(c == 1, Aggregate(group_by=[], [sum(x) as s], scan))
    auto pred = fexpr(
        {.node = ir::FilterCmp{.op = ir::CompareOp::Eq, .left = col_ref("c"), .right = ilit(1)}});
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{.func = ir::AggFunc::Sum,
                               .column = ir::ColumnRef{.name = "x", .source = {0}},
                               .alias = "s"});
    auto tree =
        with_child(make_filter_with({1}, std::move(pred)),
                   with_child(make_aggregate({2}, {}, std::move(aggs)), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Aggregate);
}

TEST_CASE("canonicalize R20: pruning Project inserted below Aggregate over Scan",
          "[ir][canonicalize]") {
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{.func = ir::AggFunc::Sum,
                               .column = ir::ColumnRef{.name = "x", .source = {0}},
                               .alias = "s"});
    auto tree = with_child(make_aggregate({1}, {"g"}, std::move(aggs)), make_scan({2}, "t"));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Aggregate);
    REQUIRE(out->children().size() == 1);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Project);
    const auto& proj = static_cast<const ir::ProjectNode&>(*out->children().front());
    REQUIRE(proj.columns().size() == 2);
    REQUIRE(proj.columns()[0].name == "g");
    REQUIRE(proj.columns()[1].name == "x");
    REQUIRE(proj.children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R20: not inserted when child is already Project", "[ir][canonicalize]") {
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{.func = ir::AggFunc::Sum,
                               .column = ir::ColumnRef{.name = "x", .source = {0}},
                               .alias = "s"});
    auto tree = with_child(make_aggregate({1}, {"g"}, std::move(aggs)),
                           with_child(make_project({2}, {"g", "x"}), make_scan({3}, "t")));
    auto out = ir::canonicalize(std::move(tree));
    REQUIRE(out->kind() == ir::NodeKind::Aggregate);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Project);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("canonicalize R20: Aggregate(Order(x)) prunes without looping (regression)",
          "[ir][canonicalize]") {
    // Regression: R20 inserts a column-pruning Project below the Aggregate, and
    // R2 then pushes that Project beneath the Order. R20 used to re-fire because
    // the Aggregate's direct child was Order again (not the Project it had just
    // created), looping forever. The fix makes R20 peek past leading Order
    // nodes. With no count(*) agg, R20 is active, so this exercises the path.
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{.func = ir::AggFunc::Sum,
                               .column = ir::ColumnRef{.name = "v", .source = {0}},
                               .alias = "s"});
    auto tree = with_child(make_aggregate({1}, {"a", "b"}, std::move(aggs)),
                           with_child(make_order({2}, {{.name = "a", .ascending = true},
                                                       {.name = "b", .ascending = true}}),
                                      make_scan({3}, "t")));

    auto out = ir::canonicalize(std::move(tree));  // must terminate

    // Clean fixpoint: the pruning Project sits between the Order and the Scan
    // (Aggregate(Order(Project(Scan)))) — not a deeper nest, which is what a
    // capped-but-still-looping run would leave behind.
    REQUIRE(out->kind() == ir::NodeKind::Aggregate);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Order);
    const auto& order = *out->children().front();
    REQUIRE(order.children().front()->kind() == ir::NodeKind::Project);
    const auto& project = *order.children().front();
    REQUIRE(project.children().front()->kind() == ir::NodeKind::Scan);
}
