#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

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
    REQUIRE(type_of(s, "n") == ColumnType::Int64);  // count -> Int64
    REQUIRE(type_of(s, "total") == std::nullopt);   // sum type deferred
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

TEST_CASE("schema: an unmodelled operator falls back to unknown", "[ir][schema]") {
    ibex::ir::TransposeNode transpose(ibex::ir::NodeId{2});
    transpose.add_child(std::make_unique<ibex::ir::ScanNode>(ibex::ir::NodeId{1}, "t"));
    // Even with a known child, transpose's output columns are data-dependent.
    REQUIRE_FALSE(ibex::ir::infer_schema(transpose, base_sources()).is_known());
}
