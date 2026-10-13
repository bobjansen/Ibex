// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// `csv::read` with a fully declared schema binds as a lazy source: the schema
// and row count up front, then only the columns a query references. Every
// answer must equal the eager reader's; the only differences allowed are the
// two SPEC.md documents (a rejected value surfaces when its column is first
// read, and a never-read column is never checked).

#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdlib>
#include <csv.hpp>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <string>

namespace {

namespace detail = ibex::csv::detail;

auto temp_path(const std::string& name) -> std::filesystem::path {
    return std::filesystem::temp_directory_path() / name;
}

void write_file(const std::filesystem::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary);
    out << content;
}

auto read_file(const std::filesystem::path& path) -> std::string {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

auto options_for(std::string_view nulls, bool has_header, std::string_view schema)
    -> detail::CsvReadOptions {
    auto options = detail::csv_parse_null_spec(nulls);
    options.has_header = has_header;
    options.schema = detail::csv_parse_schema(schema);
    return options;
}

auto exec_with(std::size_t threads) -> ibex::runtime::ExecutionContext {
    ibex::runtime::ExecutionContext exec;
    exec.parallel_threads = threads;
    return exec;
}

void require_same(const ibex::runtime::Table& expected, const ibex::runtime::Table& actual) {
    const auto mismatch = ibex::runtime::compare_tables(expected, actual);
    INFO((mismatch ? mismatch->message() : std::string{}));
    REQUIRE_FALSE(mismatch.has_value());
}

void set_env(const char* name, const char* value) {
#ifdef _WIN32
    _putenv_s(name, value == nullptr ? "" : value);
#else
    if (value == nullptr) {
        ::unsetenv(name);
    } else {
        ::setenv(name, value, 1);
    }
#endif
}

/// Rows with the shapes that move row boundaries: CRLF and lone CR line ends,
/// blank lines, quoted delimiters and escapes, and (optionally) quoted fields
/// that span lines.
auto mixed_file(std::size_t rows, bool embedded_newlines, bool quotes = true) -> std::string {
    std::string out = "k,v,d,s,day,amount\n";
    for (std::size_t r = 0; r < rows; ++r) {
        if (r % 41 == 0) {
            out += '\n';
        }
        out += "k" + std::to_string(r % 5) + ",";
        out += r % 13 == 0 ? std::string{"NA"} : std::to_string(r % 97);
        out += "," + std::to_string(static_cast<double>(r) / 8.0) + ",";
        switch (quotes ? r % 4 : 3) {
            case 0:
                out += "\"a,b\"";
                break;
            case 1:
                out += R"("say ""hi""")";
                break;
            case 2:
                out += embedded_newlines ? "\"two\nlines\"" : "plain";
                break;
            default:
                out += "bare";
                break;
        }
        out += ",2024-02-" + std::to_string(10 + (r % 18));
        out += "," + std::to_string(r % 1000) + "." + std::to_string(r % 100);
        out += r % 3 == 0 ? "\r\n" : (r % 7 == 0 ? "\r" : "\n");
    }
    return out;
}

constexpr const char* kMixedSchema = "k:cat,v:int,d:double,s:str,day:date,amount:decimal(12,2)";

}  // namespace

TEST_CASE("lazy csv::read equals the eager read on every column", "[csv][lazy]") {
    for (const bool embedded : {false, true}) {
        INFO("embedded newlines: " << embedded);
        const auto path = temp_path("ibex_lazy_mixed.csv");
        write_file(path, mixed_file(3000, embedded));
        // Small ranges, so the row count and the decode both cut the file.
        set_env("IBEX_CSV_PARALLEL_RANGE_BYTES", "613");
        const auto options = options_for("NA", true, kMixedSchema);
        auto lazy = detail::csv_lazy_read(path.string(), options);
        REQUIRE(lazy.has_value());
        REQUIRE(lazy.value() != nullptr);
        const auto eager = detail::read_csv_with_options(path.string(), options);
        CHECK(lazy.value()->rows() == eager.rows());
        for (const std::size_t threads : {std::size_t{1}, std::size_t{4}}) {
            auto table = lazy.value()->materialize(exec_with(threads));
            REQUIRE(table.has_value());
            require_same(eager, *table);
        }
        set_env("IBEX_CSV_PARALLEL_RANGE_BYTES", nullptr);
        std::filesystem::remove(path);
    }
}

TEST_CASE("lazy csv::read counts rows exactly in a file with no quotes", "[csv][lazy]") {
    // No quote anywhere: the row count is the byte scan, not the tokenizer.
    // CRLF, lone CR, and blank lines must all count as the tokenizer counts.
    const auto path = temp_path("ibex_lazy_unquoted.csv");
    write_file(path, mixed_file(3000, false, false));
    set_env("IBEX_CSV_PARALLEL_RANGE_BYTES", "613");
    const auto options = options_for("NA", true, kMixedSchema);
    auto lazy = detail::csv_lazy_read(path.string(), options);
    REQUIRE(lazy.has_value());
    REQUIRE(lazy.value() != nullptr);
    const auto eager = detail::read_csv_with_options(path.string(), options);
    CHECK(lazy.value()->rows() == eager.rows());
    auto table = lazy.value()->materialize(exec_with(4));
    REQUIRE(table.has_value());
    require_same(eager, *table);
    set_env("IBEX_CSV_PARALLEL_RANGE_BYTES", nullptr);
    std::filesystem::remove(path);
}

TEST_CASE("lazy csv::read without a header uses the eager column names", "[csv][lazy]") {
    auto content = mixed_file(500, false);
    content.erase(0, content.find('\n') + 1);
    const auto path = temp_path("ibex_lazy_noheader.csv");
    write_file(path, content);
    const auto options = options_for("NA", false, "cat,int,double,str,date,decimal(12,2)");
    auto lazy = detail::csv_lazy_read(path.string(), options);
    REQUIRE(lazy.has_value());
    REQUIRE(lazy.value() != nullptr);
    auto table = lazy.value()->materialize(exec_with(2));
    REQUIRE(table.has_value());
    require_same(detail::read_csv_with_options(path.string(), options), *table);
    std::filesystem::remove(path);
}

TEST_CASE("lazy csv::read parses only the columns a query reads", "[csv][lazy]") {
    // `bad` is declared int but holds text in one row. Reading other columns
    // must not touch it; reading it must fail exactly as the eager read does.
    std::string content = "a,bad,c\n";
    for (int r = 0; r < 200; ++r) {
        content += std::to_string(r) + "," + (r == 150 ? "oops" : std::to_string(r)) + ",x" +
                   std::to_string(r % 3) + "\n";
    }
    const auto path = temp_path("ibex_lazy_projection.csv");
    write_file(path, content);
    const auto options = options_for("", true, "a:int,bad:int,c:cat");
    auto lazy = detail::csv_lazy_read(path.string(), options);
    REQUIRE(lazy.has_value());
    REQUIRE(lazy.value() != nullptr);
    CHECK(lazy.value()->rows() == 200);

    auto projected = lazy.value()->project({"a", "c"}, exec_with(2));
    REQUIRE(projected.has_value());
    const auto eager_ok =
        detail::read_csv_with_options(path.string(), options_for("", true, "a:int,bad:str,c:cat"));
    ibex::runtime::Table expected;
    expected.add_column("a", *eager_ok.find("a"));
    expected.add_column("c", *eager_ok.find("c"));
    require_same(expected, *projected);

    std::string eager_error;
    try {
        (void)detail::read_csv_with_options(path.string(), options);
    } catch (const std::exception& e) {
        eager_error = e.what();
    }
    REQUIRE_FALSE(eager_error.empty());
    auto bad = lazy.value()->project({"bad"}, exec_with(2));
    REQUIRE_FALSE(bad.has_value());
    CHECK(bad.error() == eager_error);
    std::filesystem::remove(path);
}

TEST_CASE("lazy csv::read decodes a row selection", "[csv][lazy]") {
    const auto path = temp_path("ibex_lazy_rows.csv");
    write_file(path, mixed_file(400, true));
    const auto options = options_for("NA", true, kMixedSchema);
    auto lazy = detail::csv_lazy_read(path.string(), options);
    REQUIRE(lazy.has_value());
    REQUIRE(lazy.value() != nullptr);
    const ibex::runtime::Selection rows{0, 3, 13, 26, 399};
    auto picked = lazy.value()->project_rows({"k", "v", "s", "amount"}, rows, exec_with(2));
    REQUIRE(picked.has_value());
    const auto eager = detail::read_csv_with_options(path.string(), options);
    ibex::runtime::Table whole;
    for (const char* name : {"k", "v", "s", "amount"}) {
        const auto& entry = *eager.find_entry(name);
        if (entry.validity.has_value()) {
            whole.add_column(name, *entry.column, *entry.validity);
        } else {
            whole.add_column(name, *entry.column);
        }
    }
    require_same(detail::csv_gather_rows(whole, rows), *picked);
    std::filesystem::remove(path);
}

TEST_CASE("lazy csv::read declines whenever the eager read must run", "[csv][lazy]") {
    const auto path = temp_path("ibex_lazy_decline.csv");
    write_file(path, "a,b\n1,x\n2,y\n");
    const auto declines = [](std::string_view p, const detail::CsvReadOptions& options) {
        auto lazy = detail::csv_lazy_read(p, options);
        return lazy.has_value() && lazy.value() == nullptr;
    };
    CHECK(declines(path.string(), options_for("", true, "")));       // no schema
    CHECK(declines(path.string(), options_for("", true, "a:int")));  // partial schema
    CHECK(declines("/nonexistent/ibex.csv", options_for("", true, "a:int,b:str")));
    CHECK(declines("https://example.com/x.csv", options_for("", true, "a:int,b:str")));
    write_file(path, "a,b\n");  // header only: no first row
    CHECK(declines(path.string(), options_for("", true, "a:int,b:str")));
    write_file(path, "a,b\n1,x\n2,y\n");
    CHECK_FALSE(declines(path.string(), options_for("", true, "a:int,b:str")));
    std::filesystem::remove(path);
}

TEST_CASE("lazy csv::read refuses a file that changed after binding", "[csv][lazy]") {
    const auto path = temp_path("ibex_lazy_changed.csv");
    write_file(path, "a,b\n1,x\n2,y\n");
    auto lazy = detail::csv_lazy_read(path.string(), options_for("", true, "a:int,b:str"));
    REQUIRE(lazy.has_value());
    REQUIRE(lazy.value() != nullptr);
    write_file(path, "a,b\n1,x\n2,y\n3,z\n");
    auto decoded = lazy.value()->project({"a"}, exec_with(1));
    REQUIRE_FALSE(decoded.has_value());
    CHECK(decoded.error().contains("changed after it was bound"));
    std::filesystem::remove(path);
}

TEST_CASE("REPL binds csv::read lazily with a declared schema and eagerly without",
          "[csv][lazy][repl]") {
    // The real readers, registered the way the plugin registers them, with a
    // count of how often the lazy entry bound and how often it declined.
    std::size_t bound = 0;
    std::size_t declined = 0;
    ibex::runtime::ExternRegistry registry;
    const auto options_from = [](const ibex::runtime::ExternArgs& args) {
        auto options = detail::csv_parse_null_spec(std::get<std::string>(args[1]));
        options.delimiter = detail::csv_parse_delimiter(std::get<std::string>(args[2]));
        options.has_header = std::get<bool>(args[3]);
        options.schema = detail::csv_parse_schema(std::get<std::string>(args[4]));
        return options;
    };
    registry.register_table("csv::read",
                            [&](const ibex::runtime::ExternArgs& args)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                return ibex::runtime::ExternValue{detail::read_csv_with_options(
                                    std::get<std::string>(args[0]), options_from(args))};
                            });
    registry.register_lazy_table("csv::read",
                                 [&](const ibex::runtime::ExternArgs& args)
                                     -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
                                     auto lazy = detail::csv_lazy_read(
                                         std::get<std::string>(args[0]), options_from(args));
                                     if (lazy.has_value()) {
                                         ++(lazy.value() == nullptr ? declined : bound);
                                     }
                                     return lazy;
                                 });
    registry.register_scalar_table_consumer(
        "csv::write", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{
                ibex::ext::csv::write(table, std::get<std::string>(args[0]))}};
        });

    const auto data = temp_path("ibex_lazy_repl.csv");
    write_file(data, mixed_file(2000, true));
    const auto out = [](int i) {
        return temp_path("ibex_lazy_repl_out" + std::to_string(i) + ".csv");
    };
    const std::string schema = kMixedSchema;
    const std::string read_lazy =
        "csv::read(\"" + data.generic_string() + "\", nulls = \"NA\", schema = \"" + schema + "\")";
    const std::string read_eager = "csv::read(\"" + data.generic_string() + "\", nulls = \"NA\")";
    const std::string query = "[filter v > 20, select { s = sum(v), n = count() }, by k, order k]";
    const std::string source =
        R"(extern fn csv::read(path: String, nulls: String = "", delimiter: String = ",",
                    has_header: Bool = true, schema: String = "") -> DataFrame
    from "csv.hpp";
)"
        "extern fn csv::write(df: DataFrame, path: String) -> Int from \"csv.hpp\";\n"
        "let lazy = " +
        read_lazy + ";\n" + "let eager = " + read_eager + ";\n" + "csv::write(lazy" + query +
        ", \"" + out(1).generic_string() + "\");\n" + "csv::write(eager" + query + ", \"" +
        out(2).generic_string() + "\");\n" + "csv::write(" + read_lazy + query + ", \"" +
        out(3).generic_string() + "\");\n" + "csv::write(" + read_eager + query + ", \"" +
        out(4).generic_string() + "\");\n";

    REQUIRE(ibex::repl::execute_script(source, registry));
    CHECK(bound >= 2);
    CHECK(declined >= 2);
    const auto expected = read_file(out(2));
    CHECK_FALSE(expected.empty());
    CHECK(read_file(out(1)) == expected);
    CHECK(read_file(out(3)) == expected);
    CHECK(read_file(out(4)) == expected);
    std::filesystem::remove(data);
    for (int i = 1; i <= 4; ++i) {
        std::filesystem::remove(out(i));
    }
}
