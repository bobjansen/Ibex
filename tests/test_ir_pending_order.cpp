// `annotate_pending_orders` tells a join what a following `order` will ask
// for, so the executor can weigh preserving the left's row order against a
// larger hash index. The annotation is a cost hint: nothing here changes which
// rows a plan produces, only which side the join indexes.

#include <ibex/ir/node.hpp>
#include <ibex/ir/pending_order.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>
#include <vector>

using namespace ibex;

namespace {

// The first Join in the plan, or nullptr.
auto find_join(const ir::Node& node) -> const ir::JoinNode* {
    if (node.kind() == ir::NodeKind::Join) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<const ir::JoinNode*>(&node);
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            if (const auto* found = find_join(*child)) {
                return found;
            }
        }
    }
    return nullptr;
}

// Lowering runs the optimizer, which is where the annotation pass sits, so
// this exercises the pass in the position it actually runs in.
auto pending_of(std::string_view src) -> std::vector<std::pair<std::string, bool>> {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    const auto* join = find_join(*lowered.value());
    REQUIRE(join != nullptr);
    std::vector<std::pair<std::string, bool>> out;
    for (const auto& key : join->pending_order()) {
        out.emplace_back(key.name, key.ascending);
    }
    return out;
}

}  // namespace

TEST_CASE("pending order: an order above a join reaches the join", "[ir][pending_order]") {
    auto keys = pending_of("(lhs join rhs on id)[order { id asc }];");
    CHECK(keys == std::vector<std::pair<std::string, bool>>{{"id", true}});
}

TEST_CASE("pending order: direction and multiple keys carry through",
          "[ir][pending_order]") {
    auto keys = pending_of("(lhs join rhs on id)[order { id asc, v desc }];");
    CHECK(keys == std::vector<std::pair<std::string, bool>>{{"id", true}, {"v", false}});
}

TEST_CASE("pending order: a join with no order above it is left alone",
          "[ir][pending_order]") {
    CHECK(pending_of("lhs join rhs on id;").empty());
}

TEST_CASE("pending order: an order under the join is not the join's to use",
          "[ir][pending_order]") {
    // This `order` establishes the LEFT's ordering; it is not a request the
    // join's output has to satisfy.
    CHECK(pending_of("lhs[order { id asc }] join rhs on id;").empty());
}

TEST_CASE("pending order: a bare order annotates nothing", "[ir][pending_order]") {
    // `order` with no keys sorts by the whole schema, resolved at execution.
    // There is nothing to hand the join that it could compare a claim against.
    CHECK(pending_of("(lhs join rhs on id)[order];").empty());
}

TEST_CASE("pending order: canonicalize lifts an order above a filter first",
          "[ir][pending_order]") {
    // R1 rewrites Filter(Order(x)) to Order(Filter(x)), so by the time this
    // pass runs the `order` sits directly on the join and is seen. That the
    // pass only looks at an immediate parent is what makes running it last
    // matter.
    auto keys = pending_of("(lhs join rhs on id)[order { id asc }][filter id > 0];");
    CHECK(keys == std::vector<std::pair<std::string, bool>>{{"id", true}});
}
