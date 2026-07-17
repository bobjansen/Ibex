#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using ibex::ir::ColumnType;
using ibex::ir::SchemaInfo;
using ibex::ir::SourceSchemas;

namespace {

// A table "t" with a known three-column schema, used as a leaf source.
auto base_sources() -> SourceSchemas {
    return {{"t", SchemaInfo::known({
                      {.name = "a", .type = ColumnType::Int64},
                      {.name = "b", .type = ColumnType::Float64},
                      {.name = "c", .type = ColumnType::String},
                  })}};
}

auto schema_of(std::string_view src, const SourceSchemas& sources = {}) -> SchemaInfo {
    auto parsed = ibex::parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = ibex::parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    return ibex::ir::infer_schema(*lowered.value(), sources);
}

// Names of the columns in declaration order.
auto names(const SchemaInfo& schema) -> std::vector<std::string> {
    std::vector<std::string> out;
    for (const auto& field : schema.fields()) {
        out.push_back(field.name);
    }
    return out;
}

auto type_of(const SchemaInfo& schema, std::string_view name) -> std::optional<ColumnType> {
    const auto* field = schema.find(name);
    REQUIRE(field != nullptr);
    return field->type;
}

}  // namespace

TEST_CASE("schema: filter passes the child schema through unchanged", "[ir][schema]") {
    auto s = schema_of("t[filter a > 0];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b", "c"});
    REQUIRE(type_of(s, "a") == ColumnType::Int64);
    REQUIRE(type_of(s, "b") == ColumnType::Float64);
    REQUIRE(type_of(s, "c") == ColumnType::String);
}

TEST_CASE("schema: select narrows to the listed columns, carrying types", "[ir][schema]") {
    auto s = schema_of("t[select { c, a }];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"c", "a"});
    REQUIRE(type_of(s, "c") == ColumnType::String);
    REQUIRE(type_of(s, "a") == ColumnType::Int64);
}

TEST_CASE("schema: update adds derived columns with inferred types", "[ir][schema]") {
    auto s = schema_of("t[update { d = a, e = 5 }];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b", "c", "d", "e"});
    REQUIRE(type_of(s, "d") == ColumnType::Int64);  // copy of a
    REQUIRE(type_of(s, "e") == ColumnType::Int64);  // integer literal
}

TEST_CASE("schema: rename relabels columns, keeping the rest", "[ir][schema]") {
    auto s = schema_of("t[rename { x = a }];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"x", "b", "c"});
    REQUIRE(type_of(s, "x") == ColumnType::Int64);
}

TEST_CASE("schema: aggregate yields group keys plus aggregate outputs", "[ir][schema]") {
    auto s = schema_of("t[select { a, total = sum(b), n = count() }, by a];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "total", "n"});
    REQUIRE(type_of(s, "a") == ColumnType::Int64);
    REQUIRE(type_of(s, "n") == ColumnType::Int64);        // count -> Int64
    REQUIRE(type_of(s, "total") == ColumnType::Float64);  // sum(b) preserves Float64
}

TEST_CASE("schema: aggregate resolves sum and mean result types", "[ir][schema]") {
    auto s = schema_of("t[select { a, si = sum(a), sf = sum(b), avg = mean(a) }, by a];",
                       base_sources());
    REQUIRE(s.is_known());
    REQUIRE(type_of(s, "si") == ColumnType::Int64);     // sum preserves Int64
    REQUIRE(type_of(s, "sf") == ColumnType::Float64);   // sum preserves Float64
    REQUIRE(type_of(s, "avg") == ColumnType::Float64);  // mean is always Float64
}

// --- Unique constraints -------------------------------------------------
//
// Each of these asserts a proof carried by construction, so what matters is
// that it survives the plan the LOWERER emits -- `schema_of` parses and lowers,
// which is the point. A constraint proved against a hand-built IR shape the
// lowerer never produces would be worth nothing (the same mistake that left
// `reorder_inner_joins_for_aggregates` dead behind a test that passed).

TEST_CASE("schema: a group-by proves its keys unique", "[ir][schema]") {
    auto s = schema_of("t[select { a, total = sum(b) }, by a];", base_sources());
    REQUIRE(s.is_unique_within({"a"}));
    // A wider set containing the key is unique too -- it can only split groups.
    REQUIRE(s.is_unique_within({"a", "total"}));
    // The aggregate value is not a key: two groups can sum to the same number.
    REQUIRE_FALSE(s.is_unique_within({"total"}));
}

TEST_CASE("schema: an ungrouped aggregate proves at most one row", "[ir][schema]") {
    auto s = schema_of("t[select { total = sum(b) }];", base_sources());
    // The empty key: no column is needed to tell its rows apart.
    REQUIRE(s.unique_keys().size() == 1);
    REQUIRE(s.unique_keys().front().empty());
    REQUIRE(s.is_unique_within({}));
    REQUIRE(s.is_unique_within({"anything"}));
}

TEST_CASE("schema: a group-by key stays unique through filter and select", "[ir][schema]") {
    // The row-wise operators a real plan puts between an aggregate and a join.
    // If the proof does not survive them it never reaches the estimator.
    auto s = schema_of("t[filter b > 0, select { a, total = sum(b) }, by a][filter total > 1];",
                       base_sources());
    REQUIRE(s.is_unique_within({"a"}));
}

TEST_CASE("schema: projecting the group key away drops the proof", "[ir][schema]") {
    auto s = schema_of("t[select { a, total = sum(b) }, by a][select { total }];", base_sources());
    REQUIRE(s.unique_keys().empty());
}

TEST_CASE("schema: overwriting a unique column drops the proof", "[ir][schema]") {
    // `update` keeps the column's name but replaces its values, and nothing
    // says the new ones are still distinct.
    auto s = schema_of("t[select { a, total = sum(b) }, by a][update { a = 1 }];", base_sources());
    REQUIRE_FALSE(s.is_unique_within({"a"}));
    auto kept =
        schema_of("t[select { a, total = sum(b) }, by a][update { z = 1 }];", base_sources());
    REQUIRE(kept.is_unique_within({"a"}));  // an unrelated column is no threat
}

TEST_CASE("schema: renaming a unique column renames the proof", "[ir][schema]") {
    auto s = schema_of("t[select { a, total = sum(b) }, by a][rename { k = a }];", base_sources());
    REQUIRE(s.is_unique_within({"k"}));
    REQUIRE_FALSE(s.is_unique_within({"a"}));
}

TEST_CASE("schema: distinct proves every column together unique", "[ir][schema]") {
    // `distinct a, b` lowers to Distinct(Project(a, b)), so the proof covers
    // exactly the projected columns.
    auto s = schema_of("t[distinct { a, b }];", base_sources());
    REQUIRE(s.is_unique_within({"a", "b"}));
    REQUIRE_FALSE(s.is_unique_within({"a"}));
}

TEST_CASE("schema: an inner join keeps a proof both sides can support", "[ir][schema]") {
    // Both sides unique on `a`, so the join matches at most one row against at
    // most one row: neither side's rows can duplicate, and `a` stays unique.
    auto s =
        schema_of("t[select { a, x = sum(b) }, by a] join t[select { a, y = sum(b) }, by a] on a;",
                  base_sources());
    REQUIRE(s.is_known());
    REQUIRE(s.is_unique_within({"a"}));
}

TEST_CASE("schema: a join with a non-unique left loses the right's proof", "[ir][schema]") {
    // This is the soundness edge. `revenue` is unique on `a`, but `t` is not:
    // several `t` rows can match one revenue row, duplicating it. So `a` is NOT
    // unique in the output, even though it is unique on the side it came from.
    // (The join's *size* is still bounded here -- that is what the estimator
    // uses -- but a bound on rows is not a proof about keys.)
    auto s = schema_of("t join t[select { a, total = sum(b) }, by a] on a;", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(s.unique_keys().empty());
}

TEST_CASE("schema: a join between two non-unique sides proves nothing", "[ir][schema]") {
    auto s = schema_of("t join t on a;", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(s.unique_keys().empty());
}

TEST_CASE("schema: update infers arithmetic result types", "[ir][schema]") {
    auto s = schema_of("t[update { i = a * 2, f = a * b, d = a / a }];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(type_of(s, "i") == ColumnType::Int64);    // Int * Int literal -> Int64
    REQUIRE(type_of(s, "f") == ColumnType::Float64);  // Int * Float -> Float64
    REQUIRE(type_of(s, "d") == ColumnType::Float64);  // division -> Float64
}

// --- Fused nodes --------------------------------------------------------
//
// canonicalize fuses `Project(Filter(x))` and `Project(Update(Filter(x)))` --
// the shape of an ordinary scan leaf -- so these arms are not a corner case:
// while they were Unknown, no real plan's leaf had a schema, and every pass
// gated on a Known input silently declined on every query.

TEST_CASE("schema: a fused filter+select reports the projection's columns", "[ir][schema]") {
    // R5 fuses this into FilterProject; the schema must not change with it.
    auto s = schema_of("t[filter a > 0, select { c, a }];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"c", "a"});
    REQUIRE(type_of(s, "c") == ColumnType::String);
    REQUIRE(type_of(s, "a") == ColumnType::Int64);
}

TEST_CASE("schema: a fused filter+update+select types its computed fields", "[ir][schema]") {
    // R6 fuses this into FilterUpdateProject. The update's fields are visible
    // only through the projection, so the update has to be typed first.
    // Chained blocks, since `select` and `update` are mutually exclusive within
    // one (SPEC C5) -- the fusion is what brings them back together.
    auto s = schema_of("t[filter a > 0][update { d = a * b }][select { a, d }];", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "d"});
    REQUIRE(type_of(s, "d") == ColumnType::Float64);  // Int * Float -> Float64
}

TEST_CASE("schema: a fused node carries a unique key through", "[ir][schema]") {
    auto s = schema_of("t[select { a, total = sum(b) }, by a][filter total > 0, select { a }];",
                       base_sources());
    REQUIRE(s.is_unique_within({"a"}));
}

TEST_CASE("schema: an unknown source produces an unknown schema", "[ir][schema]") {
    auto s = schema_of("t[filter a > 0];");  // no source schema injected
    REQUIRE_FALSE(s.is_known());
}

TEST_CASE("schema: select fixes the column set even over an unknown child", "[ir][schema]") {
    auto s = schema_of("t[select { a, b }];");  // unknown source
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b"});
    REQUIRE(type_of(s, "a") == std::nullopt);
    REQUIRE(type_of(s, "b") == std::nullopt);
}

TEST_CASE("schema: a Table literal is known from its column literals", "[ir][schema]") {
    auto s = schema_of(R"(Table { a = [1, 2], b = [1.5, 2.5], c = ["x", "y"] };)");
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b", "c"});
    REQUIRE(type_of(s, "a") == ColumnType::Int64);
    REQUIRE(type_of(s, "b") == ColumnType::Float64);
    REQUIRE(type_of(s, "c") == ColumnType::String);
}

TEST_CASE("schema: scan resolves from the source environment", "[ir][schema]") {
    ibex::ir::ScanNode scan(ibex::ir::NodeId{1}, "t");
    REQUIRE_FALSE(ibex::ir::infer_schema(scan).is_known());  // empty env -> Unknown
    auto s = ibex::ir::infer_schema(scan, base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b", "c"});
}

TEST_CASE("schema: join unions both sides, deduplicating shared keys", "[ir][schema]") {
    ibex::ir::JoinNode join(ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner, {"a"});
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "u"));

    SourceSchemas sources{
        {"t", SchemaInfo::known({{.name = "a", .type = ColumnType::Int64},
                                 {.name = "b", .type = ColumnType::Float64}})},
        {"u", SchemaInfo::known({{.name = "a", .type = ColumnType::Int64},
                                 {.name = "c", .type = ColumnType::String}})},
    };
    auto s = ibex::ir::infer_schema(join, sources);
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b", "c"});  // shared "a" appears once
    REQUIRE(type_of(s, "c") == ColumnType::String);
}

TEST_CASE("schema: ascription recovers a known schema over an unknown child", "[ir][schema]") {
    // The key unlock: `as` defeats an Unknown source so downstream is checkable.
    auto s = schema_of("t as DataFrame<{ x: Int64, y: Float64 }>;");  // unknown source
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"x", "y"});
    REQUIRE(type_of(s, "x") == ColumnType::Int64);
    REQUIRE(type_of(s, "y") == ColumnType::Float64);
}

TEST_CASE("schema: melt yields id columns plus variable/value", "[ir][schema]") {
    ibex::ir::MeltNode melt(ibex::ir::NodeId{2}, {"a"}, {"b"});  // id=a, measure=b
    melt.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    auto s = ibex::ir::infer_schema(melt, base_sources());  // t = {a:Int64, b:Float64, c:String}
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "variable", "value"});
    REQUIRE(type_of(s, "a") == ColumnType::Int64);
    REQUIRE(type_of(s, "variable") == ColumnType::String);
    REQUIRE(type_of(s, "value") == ColumnType::Float64);  // measure b is Float64
}

TEST_CASE("schema: cov yields 'column' plus one Float64 per numeric column", "[ir][schema]") {
    ibex::ir::CovNode cov(ibex::ir::NodeId{2});
    cov.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    auto s = ibex::ir::infer_schema(cov, base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"column", "a", "b"});  // c (String) excluded
    REQUIRE(type_of(s, "column") == ColumnType::String);
    REQUIRE(type_of(s, "a") == ColumnType::Float64);
    REQUIRE(type_of(s, "b") == ColumnType::Float64);
}

TEST_CASE("schema: resample is open when the input has no known time index", "[ir][schema]") {
    std::vector<ibex::ir::ColumnRef> group_by{{.name = "a"}};
    std::vector<ibex::ir::AggSpec> aggs{
        {.func = ibex::ir::AggFunc::Sum, .column = {.name = "b"}, .alias = "total", .param = 0.0}};
    ibex::ir::ResampleNode rs(ibex::ir::NodeId{2}, std::chrono::seconds{1}, group_by, aggs);
    rs.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    auto s = ibex::ir::infer_schema(rs, base_sources());  // t has no time index
    REQUIRE(s.is_known());
    REQUIRE(s.is_open());  // the time-bucket column cannot be named
    REQUIRE(s.find("a") != nullptr);
    REQUIRE(type_of(s, "total") == ColumnType::Float64);  // sum(b), b is Float64
}

TEST_CASE("schema: as_timeframe designates the time index (promoting it to Timestamp)",
          "[ir][schema]") {
    SourceSchemas sources{
        {"src", SchemaInfo::known({{.name = "ts", .type = ColumnType::Int64},
                                   {.name = "px", .type = ColumnType::Float64}})}};
    ibex::ir::AsTimeframeNode atf(ibex::ir::NodeId{2}, "ts");
    atf.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "src"));
    auto s = ibex::ir::infer_schema(atf, sources);
    REQUIRE(s.is_known());
    REQUIRE(s.time_index() == "ts");
    REQUIRE(type_of(s, "ts") == ColumnType::Timestamp);  // promoted from Int64
    REQUIRE(type_of(s, "px") == ColumnType::Float64);
}

TEST_CASE("schema: resample over a time-indexed input is closed", "[ir][schema]") {
    SourceSchemas sources{
        {"src", SchemaInfo::known({{.name = "ts", .type = ColumnType::Int64},
                                   {.name = "sym", .type = ColumnType::String},
                                   {.name = "px", .type = ColumnType::Float64}})}};
    auto atf = std::make_unique<ibex::ir::AsTimeframeNode>(ibex::ir::NodeId{2}, "ts");
    atf->add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "src"));

    std::vector<ibex::ir::ColumnRef> group_by{{.name = "sym"}};
    std::vector<ibex::ir::AggSpec> aggs{
        {.func = ibex::ir::AggFunc::Mean, .column = {.name = "px"}, .alias = "avg", .param = 0.0}};
    ibex::ir::ResampleNode rs(ibex::ir::NodeId{3}, std::chrono::seconds{1}, group_by, aggs);
    rs.add_child(std::move(atf));

    auto s = ibex::ir::infer_schema(rs, sources);
    REQUIRE(s.is_known());
    REQUIRE_FALSE(s.is_open());  // now closed — bucket column named after the index
    REQUIRE(names(s) == std::vector<std::string>{"ts", "sym", "avg"});
    REQUIRE(type_of(s, "ts") == ColumnType::Timestamp);
    REQUIRE(type_of(s, "sym") == ColumnType::String);
    REQUIRE(type_of(s, "avg") == ColumnType::Float64);  // mean(px)
    REQUIRE(s.time_index() == "ts");
}

TEST_CASE("schema: comparison/logical/null-test expressions type as Bool", "[ir][schema]") {
    auto col = [](std::string name) {
        return ibex::ir::make_expr_ptr(
            ibex::ir::Expr{.node = ibex::ir::ColumnRef{.name = std::move(name)}});
    };
    std::vector<ibex::ir::FieldSpec> fields;
    fields.push_back(
        {.alias = "gt",
         .expr = ibex::ir::Expr{.node = ibex::ir::CompareExpr{.op = ibex::ir::CompareOp::Gt,
                                                              .left = col("a"),
                                                              .right = col("b")}}});
    fields.push_back(
        {.alias = "both",
         .expr = ibex::ir::Expr{.node = ibex::ir::LogicalExpr{.op = ibex::ir::LogicalOp::And,
                                                              .left = col("a"),
                                                              .right = col("b")}}});
    fields.push_back({.alias = "present",
                      .expr = ibex::ir::Expr{
                          .node = ibex::ir::IsNullExpr{.operand = col("a"), .negated = true}}});
    ibex::ir::UpdateNode update(ibex::ir::NodeId{2}, std::move(fields));
    update.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    auto s = ibex::ir::infer_schema(update, base_sources());
    REQUIRE(s.is_known());
    REQUIRE(type_of(s, "gt") == ColumnType::Bool);
    REQUIRE(type_of(s, "both") == ColumnType::Bool);
    REQUIRE(type_of(s, "present") == ColumnType::Bool);
}

TEST_CASE("schema: an unmodelled operator falls back to unknown", "[ir][schema]") {
    ibex::ir::TransposeNode transpose(ibex::ir::NodeId{2});
    transpose.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    // Even with a known child, transpose's output columns are data-dependent.
    REQUIRE_FALSE(ibex::ir::infer_schema(transpose, base_sources()).is_known());
}

TEST_CASE("schema: like infers a Bool column", "[ir][schema]") {
    auto s = schema_of(R"(t[update { m = like(c, "%x%") }];)", base_sources());
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"a", "b", "c", "m"});
    REQUIRE(type_of(s, "m") == ColumnType::Bool);
}
