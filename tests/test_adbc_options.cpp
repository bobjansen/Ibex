// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The read_adbc options grammar. Header-only, so this runs in every build,
// including those without an ADBC driver manager.

#include <catch2/catch_test_macros.hpp>

#include <adbc_options.hpp>
#include <string>
#include <string_view>
#include <utility>

namespace {

using ibex::adbc::OptionList;
using ibex::adbc::parse_options;

auto kv(std::string key, std::string value) -> std::pair<std::string, std::string> {
    return {std::move(key), std::move(value)};
}

auto error_of(std::string_view spec) -> std::string {
    auto parsed = parse_options(spec);
    REQUIRE_FALSE(parsed.has_value());
    return parsed.error();
}

auto contains(const std::string& haystack, std::string_view needle) -> bool {
    return haystack.find(needle) != std::string::npos;
}

}  // namespace

TEST_CASE("read_adbc options route by prefix", "[adbc][options]") {
    auto parsed =
        parse_options("entrypoint=MyInit; db.a=1; conn.b=2; conn.post.c=3; stmt.d=4; bare=5");
    REQUIRE(parsed.has_value());
    CHECK(parsed->entrypoint == "MyInit");
    CHECK(parsed->database == OptionList{kv("a", "1"), kv("bare", "5")});
    CHECK(parsed->connection == OptionList{kv("b", "2")});
    CHECK(parsed->connection_post == OptionList{kv("c", "3")});
    CHECK(parsed->statement == OptionList{kv("d", "4")});
}

TEST_CASE("read_adbc options keep full ADBC keys after the prefix", "[adbc][options]") {
    auto parsed = parse_options(
        "conn.adbc.connection.autocommit=true\nstmt.adbc.sqlite.query.batch_rows=2\n");
    REQUIRE(parsed.has_value());
    CHECK(parsed->connection == OptionList{kv("adbc.connection.autocommit", "true")});
    CHECK(parsed->statement == OptionList{kv("adbc.sqlite.query.batch_rows", "2")});
}

TEST_CASE("read_adbc options accept blank specs and blank entries", "[adbc][options]") {
    for (const std::string_view spec : {"", "   ", ";", " ; \n ;"}) {
        auto parsed = parse_options(spec);
        REQUIRE(parsed.has_value());
        CHECK(parsed->database.empty());
        CHECK(parsed->entrypoint.empty());
    }
}

TEST_CASE("read_adbc options trim and allow empty values", "[adbc][options]") {
    auto parsed = parse_options("  db.k  =  v w  ; db.empty=");
    REQUIRE(parsed.has_value());
    CHECK(parsed->database == OptionList{kv("k", "v w"), kv("empty", "")});
}

TEST_CASE("read_adbc options split a value only at the first '='", "[adbc][options]") {
    auto parsed = parse_options("uri_params=a=b=c");
    REQUIRE(parsed.has_value());
    CHECK(parsed->database == OptionList{kv("uri_params", "a=b=c")});
}

TEST_CASE("read_adbc options support backslash escapes", "[adbc][options]") {
    auto parsed = parse_options(R"(db.pw=a\;b\\c\=d; db.lines=x\ny\tz; db.pad=\t)");
    REQUIRE(parsed.has_value());
    CHECK(parsed->database ==
          OptionList{kv("pw", "a;b\\c=d"), kv("lines", "x\ny\tz"), kv("pad", "\t")});
}

TEST_CASE("read_adbc options reject malformed entries", "[adbc][options]") {
    CHECK(contains(error_of("novalue"), "key=value"));
    CHECK(contains(error_of("db.a=1;novalue"), "'novalue'"));
    CHECK(contains(error_of("=v"), "invalid key"));
    CHECK(contains(error_of("a b=1"), "invalid key"));
    CHECK(contains(error_of("conn.=1"), "missing an option name"));
    CHECK(contains(error_of("conn.post.=1"), "missing an option name"));
    CHECK(contains(error_of("db.a=1\\"), "trailing backslash"));
    CHECK(contains(error_of("db.a=\\q"), "unknown escape"));
}

TEST_CASE("read_adbc options reject duplicates and positional keys", "[adbc][options]") {
    CHECK(contains(error_of("db.a=1;db.a=2"), "duplicate option 'db.a'"));
    CHECK(contains(error_of("a=1;db.a=2"), "duplicate"));
    CHECK(contains(error_of("entrypoint=x;entrypoint=y"), "duplicate option 'entrypoint'"));
    CHECK(contains(error_of("driver=x"), "positional"));
    CHECK(contains(error_of("db.uri=x"), "positional"));
    // The same key in different scopes is not a duplicate.
    CHECK(parse_options("conn.a=1;conn.post.a=2;stmt.a=3;db.a=4").has_value());
}
