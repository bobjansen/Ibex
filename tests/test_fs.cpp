// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <csv.hpp>
#include <exception>
#include <filesystem>
#include <fs.hpp>
#include <fstream>
#include <string>
#include <variant>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
#include <vector>

namespace {

namespace stdfs = std::filesystem;

auto current_process_id() -> int {
#ifdef _WIN32
    return _getpid();
#else
    return getpid();
#endif
}

auto make_temp_dir(const std::string& tag) -> stdfs::path {
    const auto dir = stdfs::temp_directory_path() /
                     ("ibex_fs_test_" + tag + "_" + std::to_string(current_process_id()));
    std::error_code ec;
    stdfs::remove_all(dir, ec);
    stdfs::create_directories(dir);
    return dir;
}

void write_file(const stdfs::path& p, std::string_view contents) {
    std::ofstream out(p);
    out << contents;
}

auto strings(const ibex::runtime::Table& t, const std::string& col) -> std::vector<std::string> {
    const auto* c = std::get_if<ibex::Column<std::string>>(t.find(col));
    REQUIRE(c != nullptr);
    std::vector<std::string> out;
    for (std::size_t i = 0; i < c->size(); ++i) {
        out.emplace_back((*c)[i]);
    }
    return out;
}

}  // namespace

TEST_CASE("wildcard_match: shell globs", "[fs]") {
    using ibex::fs::detail::wildcard_match;
    CHECK(wildcard_match("x.csv", "*.csv"));
    CHECK(wildcard_match("x.csv", "*"));
    CHECK(wildcard_match("x.csv", "x.???"));
    CHECK_FALSE(wildcard_match("x.csv", "*.parquet"));
    CHECK_FALSE(wildcard_match("notes.txt", "*.csv"));
    CHECK(wildcard_match("a1", "a[0-9]"));
    CHECK_FALSE(wildcard_match("ax", "a[0-9]"));
    CHECK(wildcard_match("ax", "a[!0-9]"));
    CHECK(wildcard_match("report_2026.csv", "report_*.csv"));
}

TEST_CASE("list_files: schema, splitting, glob, recursion", "[fs]") {
    const auto dir = make_temp_dir("list");
    write_file(dir / "x.csv", "a,b\n1,2\n");
    write_file(dir / "y.csv", "a,b\n3,4\n");
    write_file(dir / "notes.txt", "hello");
    stdfs::create_directories(dir / "sub");
    write_file(dir / "sub" / "z.csv", "a,b\n5,6\n");

    SECTION("non-recursive, all entries") {
        const auto t = ibex::fs::list_files(dir.string(), "*", false);
        CHECK(t.rows() == 4);
        for (const auto* name : {"path", "name", "stem", "ext", "size_bytes", "is_dir"}) {
            CHECK(t.find(name) != nullptr);
        }
    }
    SECTION("glob filters on the file name") {
        const auto t = ibex::fs::list_files(dir.string(), "*.csv", false);
        CHECK(strings(t, "name") == std::vector<std::string>{"x.csv", "y.csv"});
        CHECK(strings(t, "stem") == std::vector<std::string>{"x", "y"});
        CHECK(strings(t, "ext") == std::vector<std::string>{"csv", "csv"});
    }
    SECTION("recursive descends into subdirectories") {
        const auto t = ibex::fs::list_files(dir.string(), "*.csv", true);
        CHECK(t.rows() == 3);
        const auto names = strings(t, "name");
        CHECK(std::ranges::find(names, "z.csv") != names.end());
    }
    SECTION("is_dir flags directories") {
        const auto t = ibex::fs::list_files(dir.string(), "*", false);
        const auto* is_dir = std::get_if<ibex::Column<bool>>(t.find("is_dir"));
        REQUIRE(is_dir != nullptr);
        const auto names = strings(t, "name");
        for (std::size_t i = 0; i < names.size(); ++i) {
            CHECK((*is_dir)[i] == (names[i] == "sub"));
        }
    }
    SECTION("missing directory is an error") {
        CHECK_THROWS_AS(ibex::fs::list_files((dir / "nope").string(), "*", false),
                        std::runtime_error);
    }
}

namespace {

auto make_registry() -> ibex::runtime::ExternRegistry {
    ibex::runtime::ExternRegistry registry;
    registry.register_table(
        "list_files",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            const auto* dir = args.empty() ? nullptr : std::get_if<std::string>(&args[0]);
            const auto* pat = args.size() >= 2 ? std::get_if<std::string>(&args[1]) : nullptr;
            if (dir == nullptr) {
                return std::unexpected("list_files: dir must be a string");
            }
            try {
                return ibex::runtime::ExternValue{
                    ibex::fs::list_files(*dir, pat != nullptr ? *pat : std::string("*"), false)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
    return registry;
}

}  // namespace

TEST_CASE("map clause: row-wise scalar evaluation", "[fs][map]") {
    auto registry = make_registry();
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session.execute("let t = Table { x = [1, 2, 3], label = [\"a\", \"b\", \"c\"] };").ok);

    SECTION("columns are in scope as scalars") {
        const auto r = session.execute("t[map { doubled = x * 2, tag = label }];");
        REQUIRE(r.ok);
        REQUIRE(r.table.has_value());
        const auto* doubled = std::get_if<ibex::Column<std::int64_t>>(r.table->find("doubled"));
        REQUIRE(doubled != nullptr);
        CHECK((*doubled)[0] == 2);
        CHECK((*doubled)[2] == 6);
        CHECK(strings(*r.table, "tag") == std::vector<std::string>{"a", "b", "c"});
    }

    SECTION("a row column shadows an outer scalar let") {
        REQUIRE(session.execute("let x = 100;").ok);
        const auto r = session.execute("t[map { v = x }];");
        REQUIRE(r.ok);
        const auto* v = std::get_if<ibex::Column<std::int64_t>>(r.table->find("v"));
        REQUIRE(v != nullptr);
        CHECK((*v)[0] == 1);
        CHECK((*v)[2] == 3);
    }

    SECTION("a field that yields a table is an error") {
        const auto r = session.execute("t[map { bad = t }];");
        CHECK_FALSE(r.ok);
        CHECK(r.error.find("must evaluate to a scalar") != std::string::npos);
    }
}

TEST_CASE("map clause: must be the last clause", "[fs][map]") {
    auto registry = make_registry();
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session.execute("let t = Table { x = [1, 2] };").ok);
    const auto r = session.execute("t[map { y = x }, filter y > 0];");
    CHECK_FALSE(r.ok);
    CHECK(r.error.find("last clause") != std::string::npos);
}

TEST_CASE("map clause: effectful externs run per row (csv round-trip)", "[fs][map]") {
    const auto src = make_temp_dir("map_src");
    const auto dst = make_temp_dir("map_dst");
    write_file(src / "one.csv", "a,b\n1,2\n3,4\n");
    write_file(src / "two.csv", "a,b\n5,6\n");

    auto registry = make_registry();
    registry.register_table("read_csv",
                            [](const ibex::runtime::ExternArgs& args)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                const auto* p =
                                    args.empty() ? nullptr : std::get_if<std::string>(&args[0]);
                                if (p == nullptr) {
                                    return std::unexpected("read_csv: path must be a string");
                                }
                                try {
                                    return ibex::runtime::ExternValue{::read_csv(*p)};
                                } catch (const std::exception& e) {
                                    return std::unexpected(std::string(e.what()));
                                }
                            });
    registry.register_scalar_table_consumer(
        "write_csv", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            const auto* path = args.empty() ? nullptr : std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("write_csv: path must be a string");
            }
            try {
                return ibex::runtime::ExternValue{
                    ibex::runtime::ScalarValue{::write_csv(table, *path)}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session
                .execute(R"(
extern fn list_files(dir: String, pattern: String = "*") -> DataFrame from "fs.hpp";
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
extern fn write_csv(df: DataFrame, path: String) -> Int from "csv.hpp";
)")
                .ok);

    const std::string script = "list_files(\"" + src.string() + "\", \"*.csv\")[map { " +
                               "source = path, " + "target = `" + dst.string() +
                               "/${stem}.out.csv`, " + "rows = write_csv(read_csv(path), `" +
                               dst.string() + "/${stem}.out.csv`) }];";
    const auto r = session.execute(script);
    INFO("script: " << script);
    INFO("error: " << r.error);
    REQUIRE(r.ok);
    REQUIRE(r.table.has_value());
    CHECK(r.table->rows() == 2);
    const auto* rows = std::get_if<ibex::Column<std::int64_t>>(r.table->find("rows"));
    REQUIRE(rows != nullptr);
    CHECK((*rows)[0] == 2);
    CHECK((*rows)[1] == 1);
    CHECK(stdfs::exists(dst / "one.out.csv"));
    CHECK(stdfs::exists(dst / "two.out.csv"));
}
