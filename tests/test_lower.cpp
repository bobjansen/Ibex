#include <ibex/ir/schema.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <vector>

namespace {

using namespace ibex;

auto require_parse(const char* source) -> parser::Program {
    auto result = parser::parse(source);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

template <typename T>
const T* as_node(const ir::Node* node) {
    return dynamic_cast<const T*>(node);
}

}  // namespace

TEST_CASE("Lower filter and select to IR") {
    auto program = require_parse("df[filter price > 10, select { price }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    // Canonicalize R5 fuses Project(Filter(x)) into FilterProject(x).
    const auto* fp = as_node<ir::FilterProjectNode>(result->get());
    REQUIRE(fp != nullptr);
    REQUIRE(fp->columns().size() == 1);
    REQUIRE(fp->columns()[0].name == "price");

    // Predicate is a FilterCmp with a FilterColumn on the left referencing "price".
    const auto* cmp = std::get_if<ibex::ir::CompareExpr>(&fp->predicate().node);
    REQUIRE(cmp != nullptr);
    const auto* col = std::get_if<ibex::ir::ColumnRef>(&cmp->left->node);
    REQUIRE(col != nullptr);
    REQUIRE(col->name == "price");

    REQUIRE(fp->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(fp->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lowering optimizer elides dead pure preamble calls") {
    auto program = require_parse(R"(
extern fn warmup(x: Int) -> Int effects {} from "x.hpp";
warmup(1);
df;
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    // Preamble is dropped and ProgramNode is unwrapped to its main expression.
    const auto* scan = as_node<ir::ScanNode>(result->get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lowering optimizer keeps effectful preamble calls") {
    auto program = require_parse(R"(
extern fn init(x: Int) -> Int effects { io_write } from "x.hpp";
init(1);
df;
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* prog = as_node<ir::ProgramNode>(result->get());
    REQUIRE(prog != nullptr);
    REQUIRE(prog->preamble().size() == 1);
    const auto* pre = as_node<ir::ExternCallNode>(prog->preamble()[0].get());
    REQUIRE(pre != nullptr);
    REQUIRE(pre->callee() == "init");

    const auto* scan = as_node<ir::ScanNode>(&prog->main_node());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("lower_script separates table sinks from the relational result", "[parser][lower]") {
    auto program = require_parse(R"(
extern fn read(path: String) -> DataFrame from "reader.hpp";
extern fn write(df: DataFrame, path: String) -> Int from "writer.hpp";
let source = read("input");
let result = source[select { id }];
write(result, "output");
result;
)");

    auto lowered = parser::lower_script(program);
    REQUIRE(lowered.has_value());
    REQUIRE(lowered->preamble.empty());
    REQUIRE(lowered->sinks.size() == 1);
    CHECK(lowered->sinks[0].callee == "write");
    CHECK(lowered->sinks[0].input_binding == std::optional<std::string>{"result"});
    CHECK(lowered->result_binding == std::optional<std::string>{"result"});
    REQUIRE(lowered->sinks[0].args.size() == 1);
    REQUIRE(std::get_if<ir::Literal>(&lowered->sinks[0].args[0].node) != nullptr);

    const auto* result = as_node<ir::ProjectNode>(lowered->result.get());
    REQUIRE(result != nullptr);
    REQUIRE(result->children().size() == 1);
    CHECK(result->children()[0]->kind() == ir::NodeKind::ExternCall);
}

namespace {

void count_scans_of(const ir::Node& node, const std::string& name, std::size_t& count) {
    if (const auto* scan = as_node<ir::ScanNode>(&node); scan != nullptr) {
        if (scan->source_name() == name) {
            ++count;
        }
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            count_scans_of(*child, name, count);
        }
    }
}

}  // namespace

TEST_CASE("lower_script shares an expensive binding referenced twice", "[parser][lower]") {
    auto program = require_parse(R"(
let agg = t[select { b, s = sum(a) }, by { b }];
let lo = agg[filter b == 1];
let hi = agg[filter b == 2];
let result = lo join hi on b;
result;
)");

    auto lowered = parser::lower_script(program);
    REQUIRE(lowered.has_value());
    REQUIRE(lowered->shared_bindings.size() == 1);
    CHECK(lowered->shared_bindings[0].name == "agg");
    // The shared plan is the aggregate itself...
    std::size_t agg_nodes = 0;
    CHECK(as_node<ir::AggregateNode>(lowered->shared_bindings[0].plan.get()) != nullptr);
    count_scans_of(*lowered->shared_bindings[0].plan, "t", agg_nodes);
    CHECK(agg_nodes == 1);
    // ...and both references in the result plan are scans of its name, not
    // clones of the aggregate subtree.
    std::size_t scans = 0;
    count_scans_of(*lowered->result, "agg", scans);
    CHECK(scans == 2);
}

TEST_CASE("lower_script keeps a cheap repeated binding inlined", "[parser][lower]") {
    // A scan/filter chain is cheap to re-run and inlining preserves each
    // consumer's own selection pushdown, so it is not shared.
    auto program = require_parse(R"(
let flt = t[filter a > 0];
let lo = flt[filter b == 1];
let hi = flt[filter b == 2];
let result = lo join hi on b;
result;
)");

    auto lowered = parser::lower_script(program);
    REQUIRE(lowered.has_value());
    CHECK(lowered->shared_bindings.empty());
    std::size_t scans = 0;
    count_scans_of(*lowered->result, "t", scans);
    CHECK(scans == 2);
}

TEST_CASE("lower_script does not share a binding referenced once", "[parser][lower]") {
    auto program = require_parse(R"(
let agg = t[select { b, s = sum(a) }, by { b }];
let result = agg[filter b == 1];
result;
)");

    auto lowered = parser::lower_script(program);
    REQUIRE(lowered.has_value());
    CHECK(lowered->shared_bindings.empty());
}

TEST_CASE("lower_script does not share a rebound name", "[parser][lower]") {
    // `agg` is bound twice; name-keyed sharing cannot represent both versions,
    // so both stay inlined.
    auto program = require_parse(R"(
let agg = t[select { b, s = sum(a) }, by { b }];
let lo = agg[filter b == 1];
let agg = t[select { b, s = max(a) }, by { b }];
let hi = agg[filter b == 2];
let result = lo join hi on b;
result;
)");

    auto lowered = parser::lower_script(program);
    REQUIRE(lowered.has_value());
    CHECK(lowered->shared_bindings.empty());
    std::size_t scans = 0;
    count_scans_of(*lowered->result, "t", scans);
    CHECK(scans == 2);
}

TEST_CASE("Lower grouped aggregation to IR") {
    auto program = require_parse("df[select { symbol, total = sum(price) }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* agg = as_node<ir::AggregateNode>(result->get());
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    REQUIRE(agg->group_by()[0].name == "symbol");
    REQUIRE(agg->aggregations().size() == 1);
    REQUIRE(agg->aggregations()[0].alias == "total");
    REQUIRE(agg->aggregations()[0].column.name == "price");
    REQUIRE(agg->aggregations()[0].func == ir::AggFunc::Sum);
}

TEST_CASE("Lower grouped aggregation with null cleanup wrapper to IR") {
    auto program =
        require_parse("df[select { symbol, avg_price = mean(null_if_nan(price)) }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* agg = as_node<ir::AggregateNode>(result->get());
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    REQUIRE(agg->group_by()[0].name == "symbol");
    REQUIRE(agg->aggregations().size() == 1);
    REQUIRE(agg->aggregations()[0].alias == "avg_price");
    REQUIRE(agg->aggregations()[0].column.name == "_agg0");
    REQUIRE(agg->aggregations()[0].func == ir::AggFunc::Mean);

    REQUIRE(agg->children().size() == 1);
    // Canonicalize R20 inserts a column-pruning Project between the Aggregate
    // and the Update so the breaker scans only the columns it needs.
    const auto* proj = as_node<ir::ProjectNode>(agg->children()[0].get());
    REQUIRE(proj != nullptr);
    REQUIRE(proj->children().size() == 1);
    const auto* update = as_node<ir::UpdateNode>(proj->children()[0].get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "_agg0");
    const auto* call = std::get_if<ir::CallExpr>(&update->fields()[0].expr.node);
    REQUIRE(call != nullptr);
    REQUIRE(call->callee == "null_if_nan");
    REQUIRE(call->args.size() == 1);
    const auto* arg = std::get_if<ir::ColumnRef>(&call->args[0]->node);
    REQUIRE(arg != nullptr);
    REQUIRE(arg->name == "price");
}

TEST_CASE("Lower grouped aggregation with computed input to IR") {
    auto program =
        require_parse("df[select { symbol, avg_price = mean(price + fee) }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* agg = as_node<ir::AggregateNode>(result->get());
    REQUIRE(agg != nullptr);
    REQUIRE(agg->aggregations().size() == 1);
    REQUIRE(agg->aggregations()[0].alias == "avg_price");
    REQUIRE(agg->aggregations()[0].column.name == "_agg0");

    REQUIRE(agg->children().size() == 1);
    const auto* proj = as_node<ir::ProjectNode>(agg->children()[0].get());
    REQUIRE(proj != nullptr);
    REQUIRE(proj->children().size() == 1);
    const auto* update = as_node<ir::UpdateNode>(proj->children()[0].get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "_agg0");
    const auto* bin = std::get_if<ir::BinaryExpr>(&update->fields()[0].expr.node);
    REQUIRE(bin != nullptr);
    const auto* left = std::get_if<ir::ColumnRef>(&bin->left->node);
    const auto* right = std::get_if<ir::ColumnRef>(&bin->right->node);
    REQUIRE(left != nullptr);
    REQUIRE(right != nullptr);
    REQUIRE(left->name == "price");
    REQUIRE(right->name == "fee");
}

TEST_CASE("Lower grouped aggregation with compile-time map expansion to IR") {
    auto program = require_parse(R"(
let measures = ["price", "fee"];
df[select { symbol, map m in measures => `avg_${m}` = mean(get(m)) }, by symbol];
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* agg = as_node<ir::AggregateNode>(result->get());
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    REQUIRE(agg->group_by()[0].name == "symbol");
    REQUIRE(agg->aggregations().size() == 2);
    REQUIRE(agg->aggregations()[0].alias == "avg_price");
    REQUIRE(agg->aggregations()[0].column.name == "price");
    REQUIRE(agg->aggregations()[1].alias == "avg_fee");
    REQUIRE(agg->aggregations()[1].column.name == "fee");
}

TEST_CASE("Lower update with compile-time map expansion to IR") {
    auto program = require_parse(R"(
let cols = ["ask_price", "bid_price", "wap"];
df[update {
    map (i, a) in cols, (j, b) in cols where i > j => `${a}_${b}_imb` = (get(a) - get(b)) / (get(a) + get(b))
}];
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* update = as_node<ir::UpdateNode>(result->get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 3);
    REQUIRE(update->fields()[0].alias == "bid_price_ask_price_imb");
    REQUIRE(update->fields()[1].alias == "wap_ask_price_imb");
    REQUIRE(update->fields()[2].alias == "wap_bid_price_imb");
}

TEST_CASE("Lower columns table call to IR") {
    auto program = require_parse("columns(df);");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* columns = as_node<ir::ColumnsNode>(result->get());
    REQUIRE(columns != nullptr);
    REQUIRE(columns->children().size() == 1);

    const auto* scan = as_node<ir::ScanNode>(columns->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lower compile-time map from columns metadata table") {
    auto program = require_parse(R"(
let cols = columns(Table { ask_price = [101.0], bid_price = [99.0], wap = [100.0] });
df[update {
    map c in cols => `copy_${c}` = get(c)
}];
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* update = as_node<ir::UpdateNode>(result->get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 3);
    REQUIRE(update->fields()[0].alias == "copy_ask_price");
    REQUIRE(update->fields()[1].alias == "copy_bid_price");
    REQUIRE(update->fields()[2].alias == "copy_wap");
}

TEST_CASE("Lower head to IR") {
    auto program = require_parse("df[order price desc, head 10];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    // Canonicalize R16 fuses Head(Order(x)) into TopK(..., First).
    const auto* topk = as_node<ir::TopKNode>(result->get());
    REQUIRE(topk != nullptr);
    REQUIRE(topk->count() == 10);
    REQUIRE(topk->group_by().empty());
    REQUIRE(topk->keep_mode() == ir::TopKNode::KeepMode::First);
    REQUIRE(topk->keys().size() == 1);
    REQUIRE(topk->keys()[0].name == "price");
    REQUIRE(topk->keys()[0].ascending == false);
}

TEST_CASE("Lower grouped head to IR") {
    auto program = require_parse("df[order score desc, head 3, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* topk = as_node<ir::TopKNode>(result->get());
    REQUIRE(topk != nullptr);
    REQUIRE(topk->count() == 3);
    REQUIRE(topk->group_by().size() == 1);
    REQUIRE(topk->group_by()[0].name == "symbol");
    REQUIRE(topk->keep_mode() == ir::TopKNode::KeepMode::First);
}

TEST_CASE("Lower tail to IR") {
    auto program = require_parse("df[order price desc, tail 10];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* topk = as_node<ir::TopKNode>(result->get());
    REQUIRE(topk != nullptr);
    REQUIRE(topk->count() == 10);
    REQUIRE(topk->group_by().empty());
    REQUIRE(topk->keep_mode() == ir::TopKNode::KeepMode::Last);
    REQUIRE(topk->keys().size() == 1);
    REQUIRE(topk->keys()[0].name == "price");
    REQUIRE(topk->keys()[0].ascending == false);
}

TEST_CASE("Lower grouped tail to IR") {
    auto program = require_parse("df[order score desc, tail 3, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* topk = as_node<ir::TopKNode>(result->get());
    REQUIRE(topk != nullptr);
    REQUIRE(topk->count() == 3);
    REQUIRE(topk->group_by().size() == 1);
    REQUIRE(topk->group_by()[0].name == "symbol");
    REQUIRE(topk->keep_mode() == ir::TopKNode::KeepMode::Last);
}

TEST_CASE("Lowering rejects nested aggregate input") {
    auto program = require_parse("df[select { symbol, bad = sum(mean(price)) }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message == "nested aggregate function calls are not allowed");
}

TEST_CASE("Lower update with by to IR") {
    auto program = require_parse("df[update { avg = price }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* update = as_node<ir::UpdateNode>(result->get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "avg");
    const auto* expr = std::get_if<ir::ColumnRef>(&update->fields()[0].expr.node);
    REQUIRE(expr != nullptr);
    REQUIRE(expr->name == "price");
    REQUIRE(update->group_by().size() == 1);
    REQUIRE(update->group_by()[0].name == "symbol");
}

TEST_CASE("Lower computed select without aggregation") {
    auto program = require_parse("df[select { x = price * price }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 1);
    REQUIRE(project->columns()[0].name == "x");

    REQUIRE(project->children().size() == 1);
    const auto* update = as_node<ir::UpdateNode>(project->children()[0].get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "x");
}

TEST_CASE("Lower distinct to IR") {
    auto program = require_parse("df[distinct symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* distinct = as_node<ir::DistinctNode>(result->get());
    REQUIRE(distinct != nullptr);
    REQUIRE(distinct->children().size() == 1);
    const auto* project = as_node<ir::ProjectNode>(distinct->children()[0].get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 1);
    REQUIRE(project->columns()[0].name == "symbol");
}

TEST_CASE("Lower order to IR") {
    auto program = require_parse("df[order symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* order = as_node<ir::OrderNode>(result->get());
    REQUIRE(order != nullptr);
    REQUIRE(order->keys().size() == 1);
    REQUIRE(order->keys()[0].name == "symbol");
    REQUIRE(order->keys()[0].ascending);
    REQUIRE(order->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(order->children()[0].get());
    REQUIRE(scan != nullptr);
}

TEST_CASE("Lowering accepts computed group keys in select+by aggregate") {
    auto program = require_parse("df[select { n = count() }, by { yr = year(ts) }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());
}

TEST_CASE("Lowering rejects computed group keys in contexts without pre-update support") {
    // head/tail/update/resample don't yet inject the pre-update needed to
    // materialize a computed key. The error message hints at the workaround.
    auto program = require_parse("df[head 5, by { yr = year(ts) }];");
    auto result = parser::lower(program);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Lower rename to IR") {
    auto program = require_parse("df[rename { cost = price }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* rename = as_node<ir::RenameNode>(result->get());
    REQUIRE(rename != nullptr);
    REQUIRE(rename->renames().size() == 1);
    REQUIRE(rename->renames()[0].new_name == "cost");
    REQUIRE(rename->renames()[0].old_name == "price");
    REQUIRE(rename->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(rename->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lower schema ascription to AscribeNode") {
    auto program = require_parse("df as DataFrame<{ a: Int64, b: Float64 }>;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* ascribe = as_node<ir::AscribeNode>(result->get());
    REQUIRE(ascribe != nullptr);
    REQUIRE(ascribe->schema().size() == 2);
    REQUIRE(ascribe->schema()[0].name == "a");
    REQUIRE(ascribe->schema()[0].type == ir::ColumnType::Int64);
    REQUIRE(ascribe->schema()[1].name == "b");
    REQUIRE(ascribe->schema()[1].type == ir::ColumnType::Float64);
    REQUIRE(ascribe->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(ascribe->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lower rejects an impossible ascription over a known input") {
    // A Table literal has a statically known schema, so an ascription it cannot
    // satisfy is a lower-time error rather than a deferred runtime check.
    SECTION("missing required column") {
        auto program = require_parse("Table { a = [1] } as DataFrame<{ salary: Int64 }>;");
        auto result = parser::lower(program);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("salary") != std::string::npos);
    }
    SECTION("wrong column type") {
        auto program = require_parse("Table { a = [1] } as DataFrame<{ a: Float64 }>;");
        auto result = parser::lower(program);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("Float64") != std::string::npos);
    }
    SECTION("an exact ascription forbids unlisted extra columns") {
        auto program = require_parse("Table { a = [1], b = [2.5] } as DataFrame<{ a: Int64 }>;");
        auto result = parser::lower(program);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("extra column 'b'") != std::string::npos);
    }
    SECTION("a wildcard ascription allows extra columns") {
        auto program = require_parse("Table { a = [1], b = [2.5] } as DataFrame<{ a: Int64, * }>;");
        auto result = parser::lower(program);
        REQUIRE(result.has_value());
        const auto* ascribe = as_node<ir::AscribeNode>(result->get());
        REQUIRE(ascribe != nullptr);
        REQUIRE(ascribe->open());
    }
}

TEST_CASE("Lower `as { ... }` sugar desugars to a DataFrame ascription") {
    // `df` is an unknown source, so the static forbid-extras check is skipped;
    // this isolates the sugar's parse/lower behavior.
    SECTION("bare schema") {
        auto result = parser::lower(require_parse("df as { a: Int64, b: Float64 };"));
        REQUIRE(result.has_value());
        const auto* asc = as_node<ir::AscribeNode>(result->get());
        REQUIRE(asc != nullptr);
        REQUIRE(asc->schema().size() == 2);
        REQUIRE(asc->schema()[0].name == "a");
        REQUIRE(asc->schema()[0].type == ir::ColumnType::Int64);
        REQUIRE(asc->schema()[1].name == "b");
        REQUIRE_FALSE(asc->open());
    }
    SECTION("with wildcard") {
        auto result = parser::lower(require_parse("df as { a: Int64, * };"));
        REQUIRE(result.has_value());
        const auto* asc = as_node<ir::AscribeNode>(result->get());
        REQUIRE(asc != nullptr);
        REQUIRE(asc->open());
    }
}

TEST_CASE("Lower validates references against a declared reader return schema") {
    SECTION("missing column is rejected at lower time") {
        auto program = require_parse(
            "extern fn read_typed(p: String) -> DataFrame<{ a: Int64 }> from \"x.hpp\";\n"
            "read_typed(\"f\")[select { b }];");
        auto result = parser::lower(program);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("b") != std::string::npos);
    }
    SECTION("a declared column lowers cleanly") {
        auto program = require_parse(
            "extern fn read_typed(p: String) -> DataFrame<{ a: Int64 }> from \"x.hpp\";\n"
            "read_typed(\"f\")[select { a }];");
        REQUIRE(parser::lower(program).has_value());
    }
    SECTION("a wildcard reader allows unlisted columns") {
        auto program = require_parse(
            "extern fn read_open(p: String) -> DataFrame<{ a: Int64, * }> from \"x.hpp\";\n"
            "read_open(\"f\")[select { b }];");
        REQUIRE(parser::lower(program).has_value());
    }
}

TEST_CASE("Lower rejects missing column references over a known schema") {
    // Column-only clause positions are validated at lower time when the input
    // schema is statically known (here, a Table literal).
    auto rejects = [](const char* src, const char* needle) {
        auto program = require_parse(src);
        auto result = parser::lower(program);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find(needle) != std::string::npos);
    };
    rejects("Table { a = [1] }[select { b }];", "b");
    rejects("Table { a = [1] }[order { b }];", "b");
    rejects("Table { a = [1] }[rename { x = b }];", "b");
    rejects("Table { a = [1] }[select { a }, order { b }];", "b");

    SECTION("valid references lower cleanly") {
        auto program = require_parse("Table { a = [1], b = [2] }[select { a }, order { a }];");
        REQUIRE(parser::lower(program).has_value());
    }
}

TEST_CASE("Lower rename with multiple renames") {
    auto program = require_parse("df[rename { cost = price, amount = qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* rename = as_node<ir::RenameNode>(result->get());
    REQUIRE(rename != nullptr);
    REQUIRE(rename->renames().size() == 2);
    REQUIRE(rename->renames()[0].new_name == "cost");
    REQUIRE(rename->renames()[0].old_name == "price");
    REQUIRE(rename->renames()[1].new_name == "amount");
    REQUIRE(rename->renames()[1].old_name == "qty");
}

TEST_CASE("Lower filter + order pipeline") {
    auto program = require_parse("df[filter price > 10, order { price asc }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    // Order is on top of filter
    const auto* order = as_node<ir::OrderNode>(result->get());
    REQUIRE(order != nullptr);
    REQUIRE(order->keys().size() == 1);
    REQUIRE(order->keys()[0].name == "price");
    REQUIRE(order->keys()[0].ascending);

    REQUIRE(order->children().size() == 1);
    const auto* filter = as_node<ir::FilterNode>(order->children()[0].get());
    REQUIRE(filter != nullptr);
}

TEST_CASE("Lower update without group-by") {
    auto program = require_parse("df[update { total = price * qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* update = as_node<ir::UpdateNode>(result->get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "total");
    REQUIRE(update->group_by().empty());
}

TEST_CASE("Lower join to IR") {
    auto program = require_parse("a join b on key;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Inner);
    REQUIRE(join->keys().size() == 1);
    REQUIRE(join->keys()[0] == "key");
    REQUIRE(join->children().size() == 2);
}

TEST_CASE("Lower left join to IR") {
    auto program = require_parse("a left join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Left);
}

TEST_CASE("Lower right join to IR") {
    auto program = require_parse("a right join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Right);
}

TEST_CASE("Lower outer join to IR") {
    auto program = require_parse("a outer join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Outer);
}
TEST_CASE("Lower semi join to IR") {
    auto program = require_parse("a semi join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Semi);
}

TEST_CASE("Lower anti join to IR") {
    auto program = require_parse("a anti join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Anti);
}

TEST_CASE("Lower cross join to IR") {
    auto program = require_parse("a cross join b;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Cross);
    REQUIRE(join->keys().empty());
}

TEST_CASE("Lower asof join to IR") {
    auto program = require_parse("a asof join b on {ts, symbol};");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Asof);
    REQUIRE(join->keys().size() == 2);
}

TEST_CASE("Lower select with multiple plain columns") {
    auto program = require_parse("df[select { price, symbol, qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 3);
    REQUIRE(project->columns()[0].name == "price");
    REQUIRE(project->columns()[1].name == "symbol");
    REQUIRE(project->columns()[2].name == "qty");
}

TEST_CASE("Lower implicit select with plain columns") {
    auto program = require_parse("df[{ price, symbol, qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 3);
    REQUIRE(project->columns()[0].name == "price");
    REQUIRE(project->columns()[1].name == "symbol");
    REQUIRE(project->columns()[2].name == "qty");
}

TEST_CASE("Lower order with multiple keys") {
    auto program = require_parse("df[order { symbol asc, price desc }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* order = as_node<ir::OrderNode>(result->get());
    REQUIRE(order != nullptr);
    REQUIRE(order->keys().size() == 2);
    REQUIRE(order->keys()[0].name == "symbol");
    REQUIRE(order->keys()[0].ascending);
    REQUIRE(order->keys()[1].name == "price");
    REQUIRE_FALSE(order->keys()[1].ascending);
}

TEST_CASE("Lower distinct with braces") {
    auto program = require_parse("df[distinct { a, b, c }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* distinct = as_node<ir::DistinctNode>(result->get());
    REQUIRE(distinct != nullptr);
    REQUIRE(distinct->children().size() == 1);
    const auto* project = as_node<ir::ProjectNode>(distinct->children()[0].get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 3);
}

TEST_CASE("Lower let-bound table reuse preserves full child pipeline") {
    auto program = require_parse(R"(
let enriched = trades[update { x = price * 2 }];
enriched[filter x > 10, select { x }];
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    // Canonicalize R5 fuses Project(Filter(x)) into FilterProject(x); here x
    // is the Update subtree, so the shape is FilterProject(Update(Scan)).
    const auto* fp = as_node<ir::FilterProjectNode>(result->get());
    REQUIRE(fp != nullptr);
    REQUIRE(fp->children().size() == 1);

    const auto* update = as_node<ir::UpdateNode>(fp->children()[0].get());
    REQUIRE(update != nullptr);
    REQUIRE(update->children().size() == 1);
    REQUIRE(update->children()[0] != nullptr);

    const auto* scan = as_node<ir::ScanNode>(update->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "trades");
}

TEST_CASE("Lower stream expression with context-provided source and sink externs") {
    auto program = require_parse(R"(
Stream {
    source = udp_recv(9001),
    transform = [resample 1m, select { open = first(price) }],
    sink = ws_send(8080)
};
)");
    REQUIRE(program.statements.size() == 1);
    const auto* expr_stmt = std::get_if<parser::ExprStmt>(&program.statements[0]);
    REQUIRE(expr_stmt != nullptr);

    parser::LowerContext ctx;
    ctx.table_externs.insert("udp_recv");
    ctx.sink_externs.insert("ws_send");

    auto result = parser::lower_expr(*expr_stmt->expr, ctx);
    REQUIRE(result.has_value());
    const auto* stream = as_node<ir::StreamNode>(result->get());
    REQUIRE(stream != nullptr);
    REQUIRE(stream->source_callee() == "udp_recv");
    REQUIRE(stream->sink_callee() == "ws_send");
}

TEST_CASE("Lower stream source binds named arguments to declared positions") {
    // Named args are supplied out of order; they must bind to the declared
    // parameter order (brokers, topic, group, schema) in source_args.
    auto program = require_parse(R"(
extern fn feed(brokers: String, topic: String, group: String, schema: String)
    -> DataFrame from "x";
extern fn ws_send(df: DataFrame, port: Int) -> Int from "x";
Stream {
    source = feed(group = "g", brokers = "b", topic = "t", schema = "s"),
    transform = [select { x = price }],
    sink = ws_send(8080)
};
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* stream = as_node<ir::StreamNode>(result->get());
    REQUIRE(stream != nullptr);
    REQUIRE(stream->source_callee() == "feed");

    const auto& args = stream->source_args();
    REQUIRE(args.size() == 4);
    const std::array<std::string, 4> expected{"b", "t", "g", "s"};
    for (std::size_t i = 0; i < expected.size(); ++i) {
        const auto* lit = std::get_if<ir::Literal>(&args[i].node);
        REQUIRE(lit != nullptr);
        const auto* str = std::get_if<std::string>(&lit->value);
        REQUIRE(str != nullptr);
        REQUIRE(*str == expected[i]);
    }
}

namespace {

// Sources with a statically known schema: a correlated subquery has to name the
// columns it keeps, so it needs one (SPEC 5.7).
constexpr const char* kCorrelatedSources = R"(
let parts = Table { p_partkey = [1, 2], p_name = ["nut", "bolt"] };
let supply = Table { ps_partkey = [1, 1], ps_cost = [5.0, 3.0] };
)";

auto lower_source(const std::string& source) -> parser::LowerResult {
    auto program = require_parse(source.c_str());
    return parser::lower(program);
}

/// Every node of `root`, so a test can assert on what the plan does and does
/// not contain without depending on how canonicalize fused it.
void collect_kinds(const ir::Node& node, std::vector<ir::NodeKind>& out) {
    out.push_back(node.kind());
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_kinds(*child, out);
        }
    }
}

auto find_join(const ir::Node& node) -> const ir::JoinNode* {
    if (const auto* join = dynamic_cast<const ir::JoinNode*>(&node)) {
        return join;
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

}  // namespace

TEST_CASE("Lower decorrelates a scalar subquery into an aggregate plus a left join") {
    auto result = lower_source(std::string(kCorrelatedSources) +
                               R"(
parts[filter p_partkey == scalar(
    supply[filter ps_partkey == outer(p_partkey), select { m = min(ps_cost) }]
)];
)");
    REQUIRE(result.has_value());

    // The correlation is gone: what is left is ordinary relational algebra.
    std::vector<ir::NodeKind> kinds;
    collect_kinds(*result.value(), kinds);
    const auto has = [&](ir::NodeKind kind) {
        return std::ranges::find(kinds, kind) != kinds.end();
    };
    REQUIRE(has(ir::NodeKind::Join));
    REQUIRE(has(ir::NodeKind::Aggregate));

    // The subquery runs ONCE, as a grouped aggregate — never per outer row.
    const auto* join = find_join(*result.value());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Left);
    REQUIRE(join->keys() == std::vector<std::string>{"p_partkey"});
    REQUIRE(join->predicate() == std::nullopt);  // a theta join would be a nested loop

    // Right side: Rename(ps_partkey -> p_partkey) over the aggregate.
    const auto* rename = as_node<ir::RenameNode>(join->children()[1].get());
    REQUIRE(rename != nullptr);
    REQUIRE(rename->renames().size() == 1);
    REQUIRE(rename->renames()[0].old_name == "ps_partkey");
    REQUIRE(rename->renames()[0].new_name == "p_partkey");

    const auto* aggregate = as_node<ir::AggregateNode>(rename->children()[0].get());
    REQUIRE(aggregate != nullptr);
    REQUIRE(aggregate->group_by().size() == 1);
    REQUIRE(aggregate->group_by()[0].name == "ps_partkey");
    REQUIRE(aggregate->aggregations().size() == 1);
    REQUIRE(aggregate->aggregations()[0].func == ir::AggFunc::Min);
    REQUIRE(aggregate->aggregations()[0].column.name == "ps_cost");
    REQUIRE(aggregate->aggregations()[0].alias == "__ibex_scalar_0");
}

TEST_CASE("Lower projects the subquery's generated column back off") {
    auto result = lower_source(std::string(kCorrelatedSources) +
                               R"(
parts[filter p_partkey == scalar(
    supply[filter ps_partkey == outer(p_partkey), select { m = min(ps_cost) }]
)];
)");
    REQUIRE(result.has_value());

    // A filter returns the rows it kept, not a wider table: the plan ends in a
    // projection back to the outer query's columns, with no trace of the
    // generated one. (Canonicalize fuses the Project onto the Filter below it.)
    const auto* fused = as_node<ir::FilterProjectNode>(result->get());
    REQUIRE(fused != nullptr);
    std::vector<std::string> kept;
    for (const auto& column : fused->columns()) {
        kept.push_back(column.name);
    }
    REQUIRE(kept == std::vector<std::string>{"p_partkey", "p_name"});
}

TEST_CASE("Lower gives each scalar subquery in a filter its own generated column") {
    auto result = lower_source(std::string(kCorrelatedSources) +
                               R"(
parts[filter
    p_partkey == scalar(supply[filter ps_partkey == outer(p_partkey), select { a = min(ps_cost) }])
    && p_partkey != scalar(supply[filter ps_partkey == outer(p_partkey), select { b = max(ps_cost) }])
];
)");
    REQUIRE(result.has_value());

    std::vector<std::string> aliases;
    const auto walk = [&](auto&& self, const ir::Node& node) -> void {
        if (const auto* aggregate = dynamic_cast<const ir::AggregateNode*>(&node)) {
            for (const auto& spec : aggregate->aggregations()) {
                aliases.push_back(spec.alias);
            }
        }
        for (const auto& child : node.children()) {
            if (child != nullptr) {
                self(self, *child);
            }
        }
    };
    walk(walk, *result.value());
    std::ranges::sort(aliases);
    REQUIRE(aliases == std::vector<std::string>{"__ibex_scalar_0", "__ibex_scalar_1"});
}

TEST_CASE("Lower avoids a generated name the enclosing query already uses") {
    auto result = lower_source(R"(
let parts = Table { p_partkey = [1, 2], __ibex_scalar_0 = [7, 8] };
let supply = Table { ps_partkey = [1, 1], ps_cost = [5.0, 3.0] };
parts[filter p_partkey == scalar(
    supply[filter ps_partkey == outer(p_partkey), select { m = min(ps_cost) }]
)];
)");
    REQUIRE(result.has_value());

    // The user's own __ibex_scalar_0 is kept, so the subquery had to land in
    // __ibex_scalar_1 instead of silently colliding with it.
    const auto* fused = as_node<ir::FilterProjectNode>(result->get());
    REQUIRE(fused != nullptr);
    std::vector<std::string> kept;
    for (const auto& column : fused->columns()) {
        kept.push_back(column.name);
    }
    REQUIRE(kept == std::vector<std::string>{"p_partkey", "__ibex_scalar_0"});

    const auto* join = find_join(*result.value());
    REQUIRE(join != nullptr);
    const auto* rename = as_node<ir::RenameNode>(join->children()[1].get());
    REQUIRE(rename != nullptr);
    const auto* aggregate = as_node<ir::AggregateNode>(rename->children()[0].get());
    REQUIRE(aggregate != nullptr);
    REQUIRE(aggregate->aggregations()[0].alias == "__ibex_scalar_1");
}

TEST_CASE("Lower rejects unsupported correlated-subquery shapes") {
    const auto lower_filter = [](const std::string& predicate) {
        return lower_source(std::string(kCorrelatedSources) + "parts[filter " + predicate + "];");
    };
    const auto subquery = [](const char* capture, const char* projection) {
        return std::string("scalar(supply[filter ") + capture + ", select { " + projection + " }])";
    };

    SECTION("outer() outside a subquery") {
        auto result = lower_filter("p_partkey == outer(p_partkey)");
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("only valid inside a scalar") != std::string::npos);
    }
    SECTION("a capture must be an equality") {
        auto result = lower_filter("p_partkey == " +
                                   subquery("ps_partkey > outer(p_partkey)", "m = min(ps_cost)"));
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("must be an equality") != std::string::npos);
    }
    SECTION("the subquery must select exactly one column") {
        auto result =
            lower_filter("p_partkey == " + subquery("ps_partkey == outer(p_partkey)",
                                                    "m = min(ps_cost), n = max(ps_cost)"));
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("exactly one column") != std::string::npos);
    }
    SECTION("the selected column must be an aggregate") {
        auto result = lower_filter("p_partkey == " +
                                   subquery("ps_partkey == outer(p_partkey)", "m = ps_cost"));
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("must be an aggregate") != std::string::npos);
    }
    SECTION("the captured column must exist in the enclosing query") {
        auto result = lower_filter("p_partkey == " +
                                   subquery("ps_partkey == outer(nope)", "m = min(ps_cost)"));
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("outer(nope)") != std::string::npos);
    }
    SECTION("the subquery must be one whole side of the comparison") {
        auto result = lower_filter("p_partkey == 1 + " +
                                   subquery("ps_partkey == outer(p_partkey)", "m = min(ps_cost)"));
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("one whole side") != std::string::npos);
    }
    SECTION("a subquery is not allowed outside a filter") {
        auto result =
            lower_source(std::string(kCorrelatedSources) + "parts[update { x = " +
                         subquery("ps_partkey == outer(p_partkey)", "m = min(ps_cost)") + " }];");
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("comparison in a filter") != std::string::npos);
    }
    SECTION("the enclosing query's schema must be known") {
        // `unknown` is a bare scan: no schema, so the columns to keep cannot be named.
        auto result =
            lower_source(std::string(kCorrelatedSources) + "unknown[filter p_partkey == " +
                         subquery("ps_partkey == outer(p_partkey)", "m = min(ps_cost)") + "];");
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().message.find("not statically known") != std::string::npos);
    }
}

TEST_CASE("Lower broadcasts an uncorrelated subquery with a cross join") {
    auto result = lower_source(std::string(kCorrelatedSources) +
                               R"(
parts[filter p_partkey == scalar(supply[select { m = min(ps_cost) }])];
)");
    REQUIRE(result.has_value());

    // No capture, so the subquery is one value for every row: an UNGROUPED
    // aggregate, cross-joined. A left join would need keys it does not have,
    // and a grouped aggregate could return more than one row and silently
    // multiply the outer rows.
    const auto* join = find_join(*result.value());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Cross);
    REQUIRE(join->keys().empty());

    const auto* aggregate = as_node<ir::AggregateNode>(join->children()[1].get());
    REQUIRE(aggregate != nullptr);
    REQUIRE(aggregate->group_by().empty());
    REQUIRE(aggregate->aggregations().size() == 1);
    REQUIRE(aggregate->aggregations()[0].alias == "__ibex_scalar_0");

    // Still a filter, so still not a wider table.
    const auto* fused = as_node<ir::FilterProjectNode>(result->get());
    REQUIRE(fused != nullptr);
    std::vector<std::string> kept;
    for (const auto& column : fused->columns()) {
        kept.push_back(column.name);
    }
    REQUIRE(kept == std::vector<std::string>{"p_partkey", "p_name"});
}
