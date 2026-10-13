// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The parallel csv::read must produce exactly what the serial reader does:
// same types, values, nulls, and Categorical codes and dictionaries. Every
// case here reads one generated file twice -- serial (IBEX_CSV_SERIAL) and
// parallel with tiny ranges -- and compares the tables with the parity
// predicate.

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <csv.hpp>
#include <filesystem>
#include <fstream>
#include <optional>
#include <random>
#include <string>

namespace {

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

struct ReadResult {
    std::optional<ibex::runtime::Table> table;
    std::string error;
};

auto read_with(const std::filesystem::path& path, bool serial, std::string_view nulls,
               bool has_header, std::string_view schema) -> ReadResult {
    set_env("IBEX_CSV_SERIAL", serial ? "1" : nullptr);
    set_env("IBEX_CSV_PARALLEL_MIN_BYTES", "1");
    set_env("IBEX_CSV_PARALLEL_RANGE_BYTES", "997");
    ReadResult out;
    try {
        out.table = ibex::ext::csv::read(path.string(), nulls, ",", has_header, schema);
    } catch (const std::exception& e) {
        out.error = e.what();
    }
    set_env("IBEX_CSV_SERIAL", nullptr);
    set_env("IBEX_CSV_PARALLEL_MIN_BYTES", nullptr);
    set_env("IBEX_CSV_PARALLEL_RANGE_BYTES", nullptr);
    return out;
}

/// Read `content` both ways and require identical results. Returns whether the
/// parallel path actually ran (it may decline, e.g. on a cut inside quotes).
auto check_same(const std::string& name, const std::string& content, std::string_view nulls = "",
                bool has_header = true, std::string_view schema = "") -> bool {
    const auto path = std::filesystem::temp_directory_path() / name;
    {
        std::ofstream out(path, std::ios::binary);
        out << content;
    }
    const auto before = ibex::csv::detail::csv_parallel_reads.load();
    const auto serial = read_with(path, true, nulls, has_header, schema);
    const auto parallel = read_with(path, false, nulls, has_header, schema);
    const bool ran = ibex::csv::detail::csv_parallel_reads.load() > before;
    std::filesystem::remove(path);
    INFO(name);
    REQUIRE(serial.error == parallel.error);
    if (serial.table.has_value()) {
        REQUIRE(parallel.table.has_value());
        const auto mismatch = ibex::runtime::compare_tables(*serial.table, *parallel.table);
        INFO((mismatch ? mismatch->message() : std::string{}));
        CHECK_FALSE(mismatch.has_value());
    }
    return ran;
}

/// A mixed file: every inference outcome, quoting, escapes, ragged rows, blank
/// lines and CRLF, deterministic from `seed`.
auto generate(std::uint32_t seed, std::size_t rows, bool crlf, bool embedded_newlines)
    -> std::string {
    std::mt19937 rng(seed);
    const auto pick = [&](std::size_t n) { return static_cast<std::size_t>(rng() % n); };
    const char* eol = crlf ? "\r\n" : "\n";
    std::string out = "i,i_empty,d,mixed,late_text,cat,high,empty,quoted,na,date";
    out += eol;
    for (std::size_t r = 0; r < rows; ++r) {
        if (pick(97) == 0) {
            out += eol;  // blank line
        }
        out += std::to_string(static_cast<std::int64_t>(rng() % 100000) - 50000);
        out += ',';
        if (pick(10) != 0) {
            out += std::to_string(pick(1000));
        }
        out += ',';
        out += std::to_string(static_cast<double>(rng() % 1000000) / 1000.0);
        out += ',';
        // Int-looking almost everywhere, so a range-local probe would say Int.
        out += r == rows - 3 ? "1.5" : std::to_string(pick(50));
        out += ',';
        // Numeric through the sampled rows, text near the end: the parallel
        // reader's typed sink must hand this column back to the views path.
        out += r == rows - 5 ? "x" : std::to_string(pick(1000));
        out += ',';
        out += "sym" + std::to_string(pick(7));
        out += ',';
        out += "u" + std::to_string(r * 7919 % 100003);
        out += ',';
        out += ',';
        switch (pick(4)) {
            case 0:
                out += "\"a,b\"";
                break;
            case 1:
                out += R"("say ""hi""")";
                break;
            case 2:
                out += embedded_newlines ? "\"two\nlines\"" : "\"plain\"";
                break;
            default:
                out += "bare";
                break;
        }
        out += ',';
        out += pick(5) == 0 ? "NA" : std::to_string(pick(9));
        if (pick(53) != 0) {  // ragged: occasionally drop the last field
            out += ",2024-01-" + std::to_string(10 + pick(18));
        }
        out += eol;
    }
    return out;
}

}  // namespace

TEST_CASE("parallel csv read matches serial on mixed inferred columns", "[csv][parallel]") {
    CHECK(check_same("ibex_par_mixed.csv", generate(1, 4000, false, false)));
    CHECK(check_same("ibex_par_crlf.csv", generate(2, 4000, true, false)));
}

TEST_CASE("parallel csv read matches serial with a null spec", "[csv][parallel]") {
    CHECK(check_same("ibex_par_nulls.csv", generate(3, 4000, false, false), "NA,<empty>"));
}

TEST_CASE("parallel csv read matches serial with a declared schema", "[csv][parallel]") {
    const auto content = generate(4, 4000, false, false);
    CHECK(check_same("ibex_par_schema.csv", content, "", true,
                     "i:int,i_empty:str,d:double,mixed:double,late_text:str,cat:cat,"
                     "high:str,empty:str,quoted:cat,na:str,date:str"));
    CHECK(check_same("ibex_par_schema_nulls.csv", content, "NA,<empty>", true,
                     "i:int,i_empty:int,d:decimal(12,3),mixed:double,late_text:cat,cat:cat,"
                     "high:cat,empty:int,quoted:str,na:int,date:date"));
}

TEST_CASE("parallel csv read raises the serial error for a bad declared value", "[csv][parallel]") {
    // `mixed` holds one 1.5: as an int column, both paths must fail the same way.
    check_same("ibex_par_bad.csv", generate(5, 4000, false, false), "", true, "mixed:int");
}

TEST_CASE("parallel csv read matches serial without a header", "[csv][parallel]") {
    auto content = generate(6, 4000, false, false);
    content.erase(0, content.find('\n') + 1);
    CHECK(check_same("ibex_par_noheader.csv", content, "", false));
}

TEST_CASE("parallel csv read matches serial when quoted fields span lines", "[csv][parallel]") {
    // Cuts can land inside a quoted field; the check must catch every one of
    // them, so the result is the serial one whichever path produced it.
    check_same("ibex_par_newlines.csv", generate(7, 4000, false, true));
    check_same("ibex_par_newlines_crlf.csv", generate(8, 4000, true, true));
}

TEST_CASE("parallel csv read matches serial on high-cardinality strings", "[csv][parallel]") {
    // Every value distinct: past the Categorical promotion limit, so String.
    std::string content = "k,v\n";
    for (int r = 0; r < 20000; ++r) {
        content += "key" + std::to_string(r) + "," + std::to_string(r % 13) + "\n";
    }
    CHECK(check_same("ibex_par_highcard.csv", content));
}
