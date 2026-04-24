#include <ibex/ir/canonicalize.hpp>
#include <ibex/ir/node.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <utility>

using namespace ibex;

namespace {

auto make_filter(ir::NodeId id) -> ir::NodePtr {
    auto pred =
        std::make_unique<ir::FilterExpr>(ir::FilterExpr{.node = ir::FilterLiteral{.value = true}});
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

TEST_CASE("canonicalize composes R3 then R1: Filter(Order(Rename(x)))", "[ir][canonicalize]") {
    auto tree =
        with_child(make_filter({1}),
                   with_child(make_order({2}, {{.name = "key", .ascending = true}}),
                              with_child(make_rename({3}, {{.new_name = "key", .old_name = "k"}}),
                                         make_scan({4}, "t"))));
    auto out = ir::canonicalize(std::move(tree));
    // R3 first rewrites Order(Rename) to Rename(Order); then Filter sits above
    // Rename. No further rules apply to Filter(Rename), so the final shape is
    // Filter(Rename(Order(Scan))). The sort still runs on the pre-rename schema.
    REQUIRE(out->kind() == ir::NodeKind::Filter);
    REQUIRE(out->children().front()->kind() == ir::NodeKind::Rename);
    REQUIRE(out->children().front()->children().front()->kind() == ir::NodeKind::Order);
}
