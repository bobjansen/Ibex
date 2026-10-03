// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parser/ast.hpp>
#include <ibex/parser/names.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

#include <expected>
#include <robin_hood.h>
#include <string>
#include <utility>
#include <variant>

namespace {

using namespace ibex::parser;

auto callee_of(const Stmt& stmt) -> std::string {
    const Expr* expr = nullptr;
    if (const auto* let = std::get_if<LetStmt>(&stmt)) {
        expr = let->value.get();
    } else {
        expr = std::get<ExprStmt>(stmt).expr.get();
    }
    return std::get<CallExpr>(expr->node).callee;
}

/// Parse `source` and resolve it with `declared` as everything else in scope
/// (the declarations in `source` are added to it).
auto resolve(const std::string& source, robin_hood::unordered_set<std::string> declared = {})
    -> std::expected<Program, ParseError> {
    auto program = parse(source);
    REQUIRE(program.has_value());
    collect_declared_names(*program, declared);
    UsingScope usings;
    auto status = resolve_names(*program, usings, declared_names_of(declared));
    if (!status) {
        return std::unexpected(status.error());
    }
    return std::move(*program);
}

}  // namespace

TEST_CASE("Names: a qualified name resolves only to its declaration") {
    auto ok = resolve("adbc::query(db, \"q\");", {"adbc::query"});
    REQUIRE(ok.has_value());
    REQUIRE(callee_of(ok->statements[0]) == "adbc::query");

    auto missing = resolve("adbc::qurey(db, \"q\");", {"adbc::query", "adbc::close"});
    REQUIRE_FALSE(missing.has_value());
    REQUIRE(missing.error().message ==
            "unknown function 'adbc::qurey'; did you mean 'adbc::query'?");
    REQUIRE(missing.error().line == 1);

    auto no_namespace = resolve("nope::query(1);", {"adbc::query"});
    REQUIRE_FALSE(no_namespace.has_value());
    REQUIRE(no_namespace.error().message ==
            "unknown namespace 'nope' in 'nope::query' (is it imported?)");
}

TEST_CASE("Names: a column or let named like the namespace cannot shadow it") {
    // `adbc` is a binding and `fs` a column; neither is consulted for a
    // qualified name.
    auto result = resolve(
        "let adbc = 1;\n"
        "let fs = t[select { fs = adbc + 1 }];\n"
        "t[select { y = fs::list(fs) }];\n"
        "adbc::close(db);\n",
        {"fs::list", "adbc::close"});
    REQUIRE(result.has_value());
    const auto& block = std::get<BlockExpr>(std::get<ExprStmt>(result->statements[2]).expr->node);
    const auto& select = std::get<SelectClause>(block.clauses[0]);
    REQUIRE(std::get<CallExpr>(select.fields[0].expr->node).callee == "fs::list");
    REQUIRE(callee_of(result->statements[3]) == "adbc::close");
}

TEST_CASE("Names: using a namespace or one name") {
    const robin_hood::unordered_set<std::string> declared = {"adbc::query", "adbc::close",
                                                             "adbc::sub::deep"};
    auto whole = resolve("using adbc;\nquery(db, \"q\");\nclose(db);\nlen(x);", declared);
    REQUIRE(whole.has_value());
    REQUIRE(callee_of(whole->statements[1]) == "adbc::query");
    REQUIRE(callee_of(whole->statements[2]) == "adbc::close");
    // Not brought in: left for lowering's own lookup.
    REQUIRE(callee_of(whole->statements[3]) == "len");

    // `using adbc;` does not reach into nested namespaces.
    auto nested = resolve("using adbc;\ndeep(1);", declared);
    REQUIRE(nested.has_value());
    REQUIRE(callee_of(nested->statements[1]) == "deep");

    auto one = resolve("using adbc::query;\nquery(db, \"q\");\nclose(db);", declared);
    REQUIRE(one.has_value());
    REQUIRE(callee_of(one->statements[1]) == "adbc::query");
    REQUIRE(callee_of(one->statements[2]) == "close");

    // A using applies from its statement on, not before.
    auto before = resolve("query(db, \"q\");\nusing adbc;", declared);
    REQUIRE(before.has_value());
    REQUIRE(callee_of(before->statements[0]) == "query");
}

TEST_CASE("Names: using something undeclared is an error at the using") {
    auto result = resolve("using nothing;", {"adbc::query"});
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.contains("using 'nothing'"));
}

TEST_CASE("Names: two usings that disagree are an error where the name is used") {
    const robin_hood::unordered_set<std::string> declared = {"csv::read", "json::read",
                                                             "csv::write"};
    // Declaring both is fine...
    auto unused = resolve("using csv;\nusing json;\nwrite(t, \"x\");", declared);
    REQUIRE(unused.has_value());
    REQUIRE(callee_of(unused->statements[2]) == "csv::write");

    // ...using the shared name is not.
    auto used = resolve("using csv;\nusing json;\nread(\"x\");", declared);
    REQUIRE_FALSE(used.has_value());
    REQUIRE(used.error().message ==
            "'read' is ambiguous: it could be 'csv::read' or 'json::read'; write the qualified "
            "name");
    REQUIRE(used.error().line == 3);

    // The qualified name always works.
    REQUIRE(resolve("using csv;\nusing json;\ncsv::read(\"x\");", declared).has_value());

    // A global declaration of the same name is a candidate too.
    auto global = resolve("fn read(p: String) -> Int { 1; }\nusing csv;\nread(\"x\");", declared);
    REQUIRE_FALSE(global.has_value());
    REQUIRE(global.error().message.contains("'read' or 'csv::read'"));
}

TEST_CASE("Names: '::f' is the global f even with a using in effect") {
    auto result =
        resolve("fn read(p: String) -> Int { 1; }\nusing csv;\n::read(\"x\");", {"csv::read"});
    REQUIRE(result.has_value());
    REQUIRE(callee_of(result->statements[2]) == "read");
}

TEST_CASE("Names: a function in a namespace sees its namespace, then enclosing ones") {
    auto result = resolve(
        "namespace a { namespace b {\n"
        "  fn f(x: Int) -> Int { g(x) + h(x) + k(x); }\n"
        "} }\n",
        {"a::b::g", "a::h", "k"});
    REQUIRE(result.has_value());
    const auto& f = std::get<FunctionDecl>(result->statements[0]);
    const auto& body = std::get<BinaryExpr>(std::get<ExprStmt>(f.body[0]).expr->node);
    const auto& left = std::get<BinaryExpr>(body.left->node);
    REQUIRE(std::get<CallExpr>(left.left->node).callee == "a::b::g");
    REQUIRE(std::get<CallExpr>(left.right->node).callee == "a::h");
    REQUIRE(std::get<CallExpr>(body.right->node).callee == "k");
}

TEST_CASE("Names: a namespace block does not reach into other namespaces") {
    auto result = resolve("namespace a { fn f(x: Int) -> Int { g(x); } }", {"b::g"});
    REQUIRE(result.has_value());
    const auto& f = std::get<FunctionDecl>(result->statements[0]);
    REQUIRE(std::get<CallExpr>(std::get<ExprStmt>(f.body[0]).expr->node).callee == "g");
}

TEST_CASE("Names: resolution is idempotent") {
    auto program = parse("using adbc;\nquery(db, \"q\");");
    REQUIRE(program.has_value());
    const robin_hood::unordered_set<std::string> declared = {"adbc::query"};
    for (int pass = 0; pass < 2; ++pass) {
        UsingScope usings;
        REQUIRE(resolve_names(*program, usings, declared_names_of(declared)).has_value());
        REQUIRE(callee_of(program->statements[1]) == "adbc::query");
    }
}
