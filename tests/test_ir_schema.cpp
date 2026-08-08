// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
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

TEST_CASE("schema: a mapped join carries the right's proof under its own name", "[ir][schema]") {
    // The old rule refused every mapped join outright. The right key column is
    // retained natively, so the proof it carries is still about an output
    // column -- `right_id`.
    //
    // Both sides also carry `val`, which is a collision now that the planner
    // no longer renames one silently; the suffix clause is incidental to the
    // proof being tested, but without it there is no output schema at all.
    ibex::ir::JoinNode join(
        ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner,
        std::vector<ibex::ir::JoinKey>{{"left_id", "right_id"}}, std::nullopt,
        ibex::ir::JoinSuffixPolicy{.present = true, .left = "_l", .right = "_r"});
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));

    SchemaInfo left = SchemaInfo::known({{.name = "left_id", .type = ColumnType::Int64},
                                         {.name = "val", .type = ColumnType::Float64}});
    left.add_unique_key({"left_id"});
    SchemaInfo right = SchemaInfo::known({{.name = "right_id", .type = ColumnType::Int64},
                                          {.name = "val", .type = ColumnType::Float64}});
    right.add_unique_key({"right_id"});

    auto s = ibex::ir::infer_schema(join, SourceSchemas{{"left", left}, {"right", right}});
    REQUIRE(s.is_known());
    CHECK(s.is_unique_within({"left_id"}));
    CHECK(s.is_unique_within({"right_id"}));
}

TEST_CASE("schema: a proof on a renamed right column follows the rename", "[ir][schema]") {
    // `code` collides, and the suffix clause emits the right one as
    // `code_right`; the right's proof is about that output column, not the
    // left `code`. An empty left suffix keeps the left name untouched, which
    // is what makes the two spellings distinguishable here.
    ibex::ir::JoinNode join(
        ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner,
        std::vector<ibex::ir::JoinKey>{{"id", "id"}}, std::nullopt,
        ibex::ir::JoinSuffixPolicy{.present = true, .left = "", .right = "_right"});
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));

    SchemaInfo left = SchemaInfo::known(
        {{.name = "id", .type = ColumnType::Int64}, {.name = "code", .type = ColumnType::String}});
    left.add_unique_key({"id"});
    SchemaInfo right = SchemaInfo::known(
        {{.name = "id", .type = ColumnType::Int64}, {.name = "code", .type = ColumnType::String}});
    right.add_unique_key({"code"});

    auto s = ibex::ir::infer_schema(join, SourceSchemas{{"left", left}, {"right", right}});
    REQUIRE(s.is_known());
    CHECK(s.is_unique_within({"code_right"}));
    CHECK_FALSE(s.is_unique_within({"code"}));
}

TEST_CASE("schema: a join between two non-unique sides proves nothing", "[ir][schema]") {
    // A self-join collides on every non-key column, so it needs the clause.
    auto s = schema_of("t join t on a suffix { \"\", \"_right\" };", base_sources());
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
    ibex::ir::JoinNode join(ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner,
                            std::vector<ibex::ir::JoinKey>{{"a", "a"}});
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

TEST_CASE("schema: mapped join retains both differently named keys", "[ir][schema]") {
    ibex::ir::JoinNode join(
        ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner,
        std::vector<ibex::ir::JoinKey>{{"left_id", "right_id"}}, std::nullopt,
        ibex::ir::JoinSuffixPolicy{.present = true, .left = "", .right = "_right"});
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));

    SourceSchemas sources{
        {"left", SchemaInfo::known({{.name = "left_id", .type = ColumnType::Int64},
                                    {.name = "value", .type = ColumnType::Float64}})},
        {"right", SchemaInfo::known({{.name = "right_id", .type = ColumnType::Int64},
                                     {.name = "value", .type = ColumnType::String}})},
    };
    auto s = ibex::ir::infer_schema(join, sources);
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"left_id", "value", "right_id", "value_right"});
}

TEST_CASE("schema: ascription recovers a known schema over an unknown child", "[ir][schema]") {
    // The key unlock: `as` defeats an Unknown source so downstream is checkable.
    auto s = schema_of("t as DataFrame<{ x: Int64, y: Float64 }>;");  // unknown source
    REQUIRE(s.is_known());
    REQUIRE(names(s) == std::vector<std::string>{"x", "y"});
    REQUIRE(type_of(s, "x") == ColumnType::Int64);
    REQUIRE(type_of(s, "y") == ColumnType::Float64);
}

TEST_CASE("schema: ascription hides physical extras even with a wildcard", "[ir][schema]") {
    auto s = schema_of("t as DataFrame<{ a: Int64, * }>;", base_sources());
    REQUIRE(s.is_known());
    REQUIRE_FALSE(s.is_open());
    REQUIRE(names(s) == std::vector<std::string>{"a"});
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

namespace {

// A join of scans over "left" and "right", so the check can be driven by the
// two source schemas alone.
auto join_of(std::vector<ibex::ir::JoinKey> keys, ibex::ir::JoinKind kind = ibex::ir::JoinKind::Inner,
             ibex::ir::JoinSuffixPolicy suffix = {}) -> ibex::ir::JoinNode {
    ibex::ir::JoinNode join(ibex::ir::NodeId{3}, kind, std::move(keys), std::nullopt,
                            std::move(suffix));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));
    return join;
}

auto two_sources(SchemaInfo left, SchemaInfo right) -> SourceSchemas {
    return {{"left", std::move(left)}, {"right", std::move(right)}};
}

}  // namespace

TEST_CASE("check_joins: a key absent from one side is named with its side", "[ir][schema]") {
    auto join = join_of({{"id", "id"}});
    auto sources = two_sources(
        SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}),
        SchemaInfo::known({{.name = "other", .type = ColumnType::Int64},
                           {.name = "v", .type = ColumnType::Float64}}));
    auto err = ibex::ir::check_joins(join, sources);
    REQUIRE(err.has_value());
    REQUIRE(err->find("'id'") != std::string::npos);
    REQUIRE(err->find("right side") != std::string::npos);
    REQUIRE(err->find("other, v") != std::string::npos);
}

TEST_CASE("check_joins: a mapped key is checked against its own side", "[ir][schema]") {
    // left_id exists on the left and right_id on the right, so the join is
    // fine; swapping the mapping is what must fail.
    auto sources =
        two_sources(SchemaInfo::known({{.name = "left_id", .type = ColumnType::Int64}}),
                    SchemaInfo::known({{.name = "right_id", .type = ColumnType::Int64}}));
    auto ok = join_of({{"left_id", "right_id"}});
    REQUIRE_FALSE(ibex::ir::check_joins(ok, sources).has_value());

    auto swapped = join_of({{"right_id", "left_id"}});
    auto err = ibex::ir::check_joins(swapped, sources);
    REQUIRE(err.has_value());
    REQUIRE(err->find("left side") != std::string::npos);
}

TEST_CASE("check_joins: the two sides of a key must agree on type", "[ir][schema]") {
    auto join = join_of({{"id", "id"}});
    auto sources = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}),
                               SchemaInfo::known({{.name = "id", .type = ColumnType::String}}));
    auto err = ibex::ir::check_joins(join, sources);
    REQUIRE(err.has_value());
    REQUIRE(err->find("Int64") != std::string::npos);
    REQUIRE(err->find("String") != std::string::npos);
}

TEST_CASE("check_joins: widths that share a runtime kind are compatible", "[ir][schema]") {
    // The runtime carries one integer width, so Int32 and Int64 keys meet as
    // the same physical column. Rejecting this pair statically would reject a
    // join the executor runs.
    auto join = join_of({{"id", "id"}});
    auto sources = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int32}}),
                               SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}));
    REQUIRE_FALSE(ibex::ir::check_joins(join, sources).has_value());
}

TEST_CASE("check_joins: an untyped or open side defers to the runtime", "[ir][schema]") {
    auto join = join_of({{"id", "id"}});
    // No type on the right: the column is known to exist, nothing more.
    auto untyped = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}),
                               SchemaInfo::known({{.name = "id", .type = std::nullopt}}));
    REQUIRE_FALSE(ibex::ir::check_joins(join, untyped).has_value());

    // An open schema may carry the key among the columns it does not list.
    auto open = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}),
                            SchemaInfo::known({{.name = "v", .type = std::nullopt}}, /*open=*/true));
    REQUIRE_FALSE(ibex::ir::check_joins(join, open).has_value());

    // An unknown source proves nothing either way.
    SourceSchemas one{{"left", SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}})}};
    REQUIRE_FALSE(ibex::ir::check_joins(join, one).has_value());
}

TEST_CASE("check_joins: an unresolved output collision is reported here", "[ir][schema]") {
    // infer_schema falls to Unknown on a collision, so this is the only place
    // the diagnostic can come from before execution.
    auto join = join_of({{"id", "id"}});
    auto sources = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64},
                                                  {.name = "v", .type = ColumnType::Float64}}),
                               SchemaInfo::known({{.name = "id", .type = ColumnType::Int64},
                                                  {.name = "v", .type = ColumnType::Float64}}));
    REQUIRE_FALSE(ibex::ir::infer_schema(join, sources).is_known());
    auto err = ibex::ir::check_joins(join, sources);
    REQUIRE(err.has_value());
    REQUIRE(err->find("v") != std::string::npos);

    // With suffixes it resolves, and the check passes.
    auto suffixed = join_of({{"id", "id"}}, ibex::ir::JoinKind::Inner,
                            ibex::ir::JoinSuffixPolicy{.present = true, .left = "_l", .right = "_r"});
    REQUIRE_FALSE(ibex::ir::check_joins(suffixed, sources).has_value());
}

TEST_CASE("check_joins: a join nested below another operator is still checked", "[ir][schema]") {
    auto join = std::make_unique<ibex::ir::JoinNode>(
        ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner, std::vector<ibex::ir::JoinKey>{{"id", "id"}});
    join->add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    join->add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));
    ibex::ir::HeadNode head(ibex::ir::NodeId{4}, std::size_t{5});
    head.add_child(std::move(join));

    auto sources = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}),
                               SchemaInfo::known({{.name = "id", .type = ColumnType::String}}));
    REQUIRE(ibex::ir::check_joins(head, sources).has_value());
}

TEST_CASE("schema: a declared expect carries the proofs a join cannot prove",
          "[ir][schema]") {
    // Neither side has a proven unique key here, so inference alone carries
    // nothing. `expect n:1` says each left row matches at most one right row,
    // which is exactly what keeps a left-side unique key unique in the output.
    // The executor checks the declaration, so relying on it cannot describe a
    // result that was actually produced -- a run whose data disagrees fails.
    auto sources = two_sources(
        SchemaInfo::known({{.name = "id", .type = ColumnType::Int64},
                           {.name = "cust", .type = ColumnType::Int64}}),
        SchemaInfo::known({{.name = "cust", .type = ColumnType::Int64},
                           {.name = "tier", .type = ColumnType::Int64}}));
    sources["left"].add_unique_key({"id"});

    auto plain = join_of({{"cust", "cust"}});
    CHECK(ibex::ir::infer_schema(plain, sources).unique_keys().empty());

    ibex::ir::JoinNode declared(
        ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner,
        std::vector<ibex::ir::JoinKey>{{"cust", "cust"}}, std::nullopt, {},
        ibex::ir::NullMatch::Never,
        ibex::ir::JoinExpect{.left = ibex::ir::JoinMultiplicity::Many,
                             .right = ibex::ir::JoinMultiplicity::One});
    declared.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    declared.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));

    const auto out = ibex::ir::infer_schema(declared, sources);
    CHECK(out.is_unique_within({"id"}));
}

// ── Nullability ──────────────────────────────────────────────────────────
//
// `Nullability::Never` is a proof, so every test below is really two
// assertions: that the rule fires where it is argued, and that it does *not*
// fire where the argument runs out. The second is the one that keeps a
// downstream pass honest, so it is written out rather than left implied.

namespace {

using ibex::ir::Nullability;

auto nulls_of(const SchemaInfo& schema, std::string_view name) -> Nullability {
    const auto* field = schema.find(name);
    REQUIRE(field != nullptr);
    return field->nulls;
}

// `t` again, but with `a` declared null-free at the source -- the shape a
// reader footer or an adapter supplies. `b` and `c` stay unproven, so every
// test has both a proof to carry and a non-proof to preserve.
auto nullable_sources() -> SourceSchemas {
    return {{"t", SchemaInfo::known({
                      {.name = "a", .type = ColumnType::Int64, .nulls = Nullability::Never},
                      {.name = "b", .type = ColumnType::Float64},
                      {.name = "c", .type = ColumnType::String},
                  })}};
}

}  // namespace

TEST_CASE("schema: a source's nullability survives the row-shaping operators", "[ir][schema]") {
    for (const auto* query : {"t[order { a asc }];", "t[head 5];", "t[select { a, b }];",
                              "t[rename { z = a }][rename { a = z }];"}) {
        CAPTURE(query);
        auto s = schema_of(query, nullable_sources());
        REQUIRE(s.is_known());
        CHECK(nulls_of(s, "a") == Nullability::Never);
        CHECK(nulls_of(s, "b") == Nullability::Maybe);
    }
}

TEST_CASE("schema: a filter proves the columns its predicate had to read", "[ir][schema]") {
    // A null `b` makes `b > 0` null, and null is not true, so no row with a
    // null `b` survives. `c` is untouched by the predicate and stays unproven.
    auto s = schema_of("t[filter b > 0];", nullable_sources());
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "b") == Nullability::Never);
    CHECK(nulls_of(s, "c") == Nullability::Maybe);
}

TEST_CASE("schema: a filter's proof reaches through arithmetic and conjunction", "[ir][schema]") {
    auto s = schema_of("t[filter b * 2 > 0 && c == \"x\"];", nullable_sources());
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "b") == Nullability::Never);
    CHECK(nulls_of(s, "c") == Nullability::Never);
}

TEST_CASE("schema: a filter proves nothing under a disjunction", "[ir][schema]") {
    // A row surviving `b > 0 || c == "x"` may have satisfied either branch, so
    // neither column is proved. Under `&&` both branches are true at once and
    // both are proved -- the contrast is the whole rule.
    auto disjunction = schema_of("t[filter b > 0 || c == \"x\"];", nullable_sources());
    REQUIRE(disjunction.is_known());
    CHECK(nulls_of(disjunction, "b") == Nullability::Maybe);
    CHECK(nulls_of(disjunction, "c") == Nullability::Maybe);
}

TEST_CASE("schema: is_not_null proves its column and is_null does not", "[ir][schema]") {
    auto positive = schema_of("t[filter is_not_null(b)];", nullable_sources());
    CHECK(nulls_of(positive, "b") == Nullability::Never);

    // `is_null(b)` is true exactly for the rows this proof would exclude.
    auto negative = schema_of("t[filter is_null(b)];", nullable_sources());
    CHECK(nulls_of(negative, "b") == Nullability::Maybe);
}

TEST_CASE("schema: a negated null test proves its column", "[ir][schema]") {
    // `!is_null(x)` says exactly what `is_not_null(x)` says: a null test is
    // total, so negating one is still a statement about presence, not the
    // ordinary `!` that proves nothing.
    //
    // This is not an exotic spelling to accommodate. dplyr's `filter(!is.na(x))`
    // is *the* idiom for it, and `is.na()` lowers to `is_null()`, so a frontend
    // cannot reach `is_not_null` from what its users write.
    auto s = schema_of("t[filter !is_null(b)];", nullable_sources());
    CHECK(nulls_of(s, "b") == Nullability::Never);

    // The double negative is the `is_null` case again, and proves nothing.
    auto doubled = schema_of("t[filter !is_not_null(b)];", nullable_sources());
    CHECK(nulls_of(doubled, "b") == Nullability::Maybe);

    // And an ordinary negated comparison still proves nothing: it is true for
    // no null `b`, but false for one, and neither row is kept.
    auto compared = schema_of("t[filter !(b > 0)];", nullable_sources());
    CHECK(nulls_of(compared, "b") == Nullability::Maybe);
}

TEST_CASE("schema: a filter proves nothing about a column a null-consumer read", "[ir][schema]") {
    // `coalesce(b, 0) > 0` is true for a null `b`, so surviving says nothing
    // about `b`. Reading the argument list without minding the callee would
    // claim it does.
    auto s = schema_of("t[filter coalesce(b, 0.0) > 0];", nullable_sources());
    CHECK(nulls_of(s, "b") == Nullability::Maybe);
}

TEST_CASE("schema: a computed field is null-free when its operands are", "[ir][schema]") {
    auto s = schema_of("t[update { lit = 5, from_proved = a * 2, from_unproved = b * 2 }];",
                       nullable_sources());
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "lit") == Nullability::Never);
    CHECK(nulls_of(s, "from_proved") == Nullability::Never);
    CHECK(nulls_of(s, "from_unproved") == Nullability::Maybe);
}

TEST_CASE("schema: fill_null makes a column null-free whatever it replaced", "[ir][schema]") {
    auto s = schema_of("t[update { filled = fill_null(b, 0.0), coalesced = coalesce(b, 0.0) }];",
                       nullable_sources());
    CHECK(nulls_of(s, "filled") == Nullability::Never);
    CHECK(nulls_of(s, "coalesced") == Nullability::Never);
}

TEST_CASE("schema: an order-dependent call is not proved by its argument", "[ir][schema]") {
    // `a` is null-free, but `lag(a)` is null in the first row regardless: the
    // rule has to turn on the function's shape, not just its arguments.
    auto s = schema_of("t[update { prev = lag(a) }];", nullable_sources());
    CHECK(nulls_of(s, "prev") == Nullability::Maybe);
}

TEST_CASE("schema: assigning over a column replaces its proof", "[ir][schema]") {
    // `a` arrives proved; the update overwrites it with an unproven expression,
    // and the proof must not outlive the values it described.
    auto s = schema_of("t[update { a = b * 2 }];", nullable_sources());
    CHECK(nulls_of(s, "a") == Nullability::Maybe);
}

TEST_CASE("schema: a grouped count is null-free and a grouped sum inherits", "[ir][schema]") {
    auto s = schema_of("t[select { c, n = count(), sa = sum(a), sb = sum(b) }, by c];",
                       nullable_sources());
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "n") == Nullability::Never);
    CHECK(nulls_of(s, "sa") == Nullability::Never);
    CHECK(nulls_of(s, "sb") == Nullability::Maybe);
    // The key column holds one of the values it grouped, so it carries that
    // column's own proof -- here, none.
    CHECK(nulls_of(s, "c") == Nullability::Maybe);
}

TEST_CASE("schema: an ungrouped aggregate proves only its count", "[ir][schema]") {
    // The single row is emitted even over an empty input, where `min(a)` has
    // no value to return however null-free `a` is. `count()` is 0 there, which
    // is a value.
    auto s = schema_of("t[select { n = count(), m = min(a) }];", nullable_sources());
    CHECK(nulls_of(s, "n") == Nullability::Never);
    CHECK(nulls_of(s, "m") == Nullability::Maybe);
}

TEST_CASE("schema: an inner join proves its key columns null-free", "[ir][schema]") {
    // Under the default `nulls never` a null key matches nothing, so every row
    // that survived an inner join has a value in every key -- a proof neither
    // input carried.
    auto join = join_of({{"id", "id"}});
    auto sources = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64},
                                                  {.name = "lv", .type = ColumnType::Float64}}),
                               SchemaInfo::known({{.name = "id", .type = ColumnType::Int64},
                                                  {.name = "rv", .type = ColumnType::Float64}}));
    auto s = ibex::ir::infer_schema(join, sources);
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "id") == Nullability::Never);
    CHECK(nulls_of(s, "lv") == Nullability::Maybe);
}

TEST_CASE("schema: `nulls equal` withdraws the inner join's key proof", "[ir][schema]") {
    // The proof rests on a null key matching nothing. `nulls equal` is exactly
    // the option that makes it match.
    ibex::ir::JoinNode join(ibex::ir::NodeId{3}, ibex::ir::JoinKind::Inner,
                            std::vector<ibex::ir::JoinKey>{{"id", "id"}}, std::nullopt, {},
                            ibex::ir::NullMatch::Equal);
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "left"));
    join.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{2}, "right"));
    auto sources = two_sources(SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}),
                               SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}));
    CHECK(nulls_of(ibex::ir::infer_schema(join, sources), "id") == Nullability::Maybe);
}

TEST_CASE("schema: a left join withdraws every proof on its right side", "[ir][schema]") {
    // This is the case the plan opened with: the right columns are null for
    // every unmatched left row, whatever the right input's own schema said.
    auto join = join_of({{"id", "id"}}, ibex::ir::JoinKind::Left);
    auto sources = two_sources(
        SchemaInfo::known(
            {{.name = "id", .type = ColumnType::Int64, .nulls = Nullability::Never},
             {.name = "lv", .type = ColumnType::Float64, .nulls = Nullability::Never}}),
        SchemaInfo::known(
            {{.name = "id", .type = ColumnType::Int64, .nulls = Nullability::Never},
             {.name = "rv", .type = ColumnType::Float64, .nulls = Nullability::Never}}));
    auto s = ibex::ir::infer_schema(join, sources);
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "lv") == Nullability::Never);
    CHECK(nulls_of(s, "rv") == Nullability::Maybe);
    // The same-name key folds into one column taken from the left, which is
    // present in every row a left join emits.
    CHECK(nulls_of(s, "id") == Nullability::Never);
}

TEST_CASE("schema: a right join's folded key needs both sides' proofs", "[ir][schema]") {
    // A right join emits rows with no left row at all, and the executor fills
    // the folded key column from the right key there. So the output column is
    // null-free only if both sides' key columns are -- which is why it is not
    // simply "a left column, and the left may be missing".
    auto both = join_of({{"id", "id"}}, ibex::ir::JoinKind::Right);
    auto proved =
        two_sources(SchemaInfo::known(
                        {{.name = "id", .type = ColumnType::Int64, .nulls = Nullability::Never},
                         {.name = "lv", .type = ColumnType::Float64, .nulls = Nullability::Never}}),
                    SchemaInfo::known(
                        {{.name = "id", .type = ColumnType::Int64, .nulls = Nullability::Never}}));
    auto s = ibex::ir::infer_schema(both, proved);
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "id") == Nullability::Never);
    CHECK(nulls_of(s, "lv") == Nullability::Maybe);  // no left row for an unmatched right row

    // Withdraw the right key's proof and the folded column loses its own.
    auto one_sided = proved;
    one_sided["right"] = SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}});
    CHECK(nulls_of(ibex::ir::infer_schema(both, one_sided), "id") == Nullability::Maybe);
}

TEST_CASE("schema: a mapped key in a right join is an ordinary left column", "[ir][schema]") {
    // Differently-named keys are both emitted natively, so nothing folds and
    // nothing is filled from the other side: `left_id` is null for an unmatched
    // right row like any other left column.
    auto join = join_of({{"left_id", "right_id"}}, ibex::ir::JoinKind::Right);
    auto sources = two_sources(
        SchemaInfo::known(
            {{.name = "left_id", .type = ColumnType::Int64, .nulls = Nullability::Never}}),
        SchemaInfo::known(
            {{.name = "right_id", .type = ColumnType::Int64, .nulls = Nullability::Never}}));
    auto s = ibex::ir::infer_schema(join, sources);
    CHECK(nulls_of(s, "left_id") == Nullability::Maybe);
    CHECK(nulls_of(s, "right_id") == Nullability::Never);
}

TEST_CASE("schema: a semi join keeps the left's proofs and an anti join its key", "[ir][schema]") {
    auto sources =
        two_sources(SchemaInfo::known(
                        {{.name = "id", .type = ColumnType::Int64},
                         {.name = "lv", .type = ColumnType::Float64, .nulls = Nullability::Never}}),
                    SchemaInfo::known({{.name = "id", .type = ColumnType::Int64}}));

    // Semi emits whole left rows, and only matched ones -- so the key is proved
    // for the same reason an inner join's is.
    auto semi = join_of({{"id", "id"}}, ibex::ir::JoinKind::Semi);
    auto s = ibex::ir::infer_schema(semi, sources);
    CHECK(nulls_of(s, "id") == Nullability::Never);
    CHECK(nulls_of(s, "lv") == Nullability::Never);

    // Anti keeps the rows that matched *nothing*, which is what a null key
    // does under `nulls never`. The key proof must not carry over.
    auto anti = join_of({{"id", "id"}}, ibex::ir::JoinKind::Anti);
    auto a = ibex::ir::infer_schema(anti, sources);
    CHECK(nulls_of(a, "id") == Nullability::Maybe);
    CHECK(nulls_of(a, "lv") == Nullability::Never);
}

TEST_CASE("schema: an ascription asserts nothing and erases nothing", "[ir][schema]") {
    // There is no surface syntax to ascribe nullability with, so the ascription
    // adds no proof -- but it is an identity on the data, so it must not take
    // away the one its input arrived with either.
    auto s = schema_of("t as DataFrame<{ a: Int64, b: Float64 }>;", nullable_sources());
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "a") == Nullability::Never);
    CHECK(nulls_of(s, "b") == Nullability::Maybe);
}

TEST_CASE("schema: a literal column is null-free", "[ir][schema]") {
    auto s = schema_of("Table { x = [1, 2, 3] };");
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "x") == Nullability::Never);
}

TEST_CASE("schema: rbind keeps a proof only when every operand supports it", "[ir][schema]") {
    // The result takes child[0]'s column set, but not its rows: `x` is null-free
    // in the first operand and unproven in the second, and the concatenation
    // holds both operands' rows. Riding the proof along with the schema, as the
    // types do, would be wrong here.
    auto s = schema_of(
        "rbind(t, u);",
        SourceSchemas{
            {"t", SchemaInfo::known(
                      {{.name = "x", .type = ColumnType::Int64, .nulls = Nullability::Never},
                       {.name = "y", .type = ColumnType::Int64, .nulls = Nullability::Never}})},
            {"u", SchemaInfo::known(
                      {{.name = "x", .type = ColumnType::Int64},
                       {.name = "y", .type = ColumnType::Int64, .nulls = Nullability::Never}})},
        });
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "x") == Nullability::Maybe);
    CHECK(nulls_of(s, "y") == Nullability::Never);
}

TEST_CASE("schema: a negation has one operand, and the fold must not read two", "[ir][schema]") {
    // `LogicalExpr` carries a null `right` for `Not`. Handing both operands to
    // the fold unconditionally segfaulted the transpiler on the first `!expr`
    // it met -- caught by the parity suite, not by any unit test, because no
    // unit test had put a negation in a computed field.
    auto s =
        schema_of("t[update { neg = !(b > 0), both = !(a > 0) && a > 1 }];", nullable_sources());
    REQUIRE(s.is_known());
    CHECK(nulls_of(s, "neg") == Nullability::Maybe);
    CHECK(nulls_of(s, "both") == Nullability::Never);
    // And in predicate position, where the proof rules read the same node.
    CHECK(schema_of("t[filter !(b > 0)];", nullable_sources()).is_known());
}
