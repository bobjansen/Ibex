// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

/// Decimal end to end: source text -> parse -> lower -> interpret, plus the
/// CSV and Arrow C Data boundaries. Semantics: plans/decimal-plan.md.

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_format.hpp>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <csv.hpp>
#include <expected>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

using namespace ibex;

auto dec_type(int p, int s) -> DecimalType {
    return DecimalType{.precision = static_cast<std::uint8_t>(p),
                       .scale = static_cast<std::uint8_t>(s)};
}

auto dec_col(DecimalType t, std::initializer_list<const char*> values) -> Column<Decimal> {
    auto col = runtime::make_decimal_column(t);
    for (const char* v : values) {
        auto units = decimal::parse(v, t);
        REQUIRE(units.has_value());
        col.push_back(Decimal{*units});
    }
    return col;
}

auto run(std::string_view src, const runtime::TableRegistry& tables)
    -> std::expected<runtime::Table, std::string> {
    auto parsed = parser::parse(src);
    if (!parsed) {
        return std::unexpected("parse: " + parsed.error().format());
    }
    auto lowered = parser::lower(*parsed);
    if (!lowered) {
        return std::unexpected("lower: " + lowered.error().message);
    }
    return runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
}

auto run_ok(std::string_view src, const runtime::TableRegistry& tables) -> runtime::Table {
    auto result = run(src, tables);
    INFO(src);
    if (!result) {
        FAIL(result.error());
    }
    return std::move(*result);
}

auto run_err(std::string_view src, const runtime::TableRegistry& tables) -> std::string {
    auto result = run(src, tables);
    INFO(src);
    REQUIRE_FALSE(result.has_value());
    return result.error();
}

/// A Decimal column's values as exact text, "null" for a null row.
auto texts(const runtime::Table& t, const std::string& name) -> std::vector<std::string> {
    const auto* entry = t.find_entry(name);
    REQUIRE(entry != nullptr);
    const auto* col = std::get_if<Column<Decimal>>(entry->column.get());
    REQUIRE(col != nullptr);
    const int scale = runtime::decimal_type_of(*col).scale;
    std::vector<std::string> out;
    out.reserve(col->size());
    for (std::size_t i = 0; i < col->size(); ++i) {
        out.push_back(runtime::is_null(*entry, i) ? "null"
                                                  : decimal::to_string((*col)[i].units, scale));
    }
    return out;
}

auto type_of(const runtime::Table& t, const std::string& name) -> DecimalType {
    const auto* col = std::get_if<Column<Decimal>>(t.find(name));
    REQUIRE(col != nullptr);
    return runtime::decimal_type_of(*col);
}

auto contains(const std::string& haystack, std::string_view needle) -> bool {
    return haystack.contains(needle);
}

auto prices() -> runtime::TableRegistry {
    runtime::Table t;
    t.add_column("price", dec_col(dec_type(10, 2), {"10.50", "10.49", "-3.25", "100.00"}));
    t.add_column("qty", Column<std::int64_t>{3, 7, 2, 1});
    t.add_column("fee", dec_col(dec_type(6, 4), {"0.0125", "1.0000", "-0.5000", "0.0001"}));
    t.add_column("f", Column<double>{1.0, 2.0, 3.0, 4.0});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    return tables;
}

}  // namespace

TEST_CASE("Decimal types and literals parse; impossible types are rejected", "[decimal][e2e]") {
    CHECK(parser::parse("t[update { a = decimal\"12.30\" }];").has_value());
    CHECK(parser::parse("t as DataFrame<{price: Decimal(12, 2), qty: Int64}>;").has_value());
    CHECK(parser::parse("t[update { a = Decimal(qty, 12, 2) }];").has_value());
    CHECK_FALSE(parser::parse("t as DataFrame<{price: Decimal(39, 2)}>;").has_value());
    CHECK_FALSE(parser::parse("t as DataFrame<{price: Decimal(5, 6)}>;").has_value());
    CHECK_FALSE(parser::parse("t[update { a = Decimal(qty, 0, 0) }];").has_value());
    CHECK_FALSE(parser::parse("t[update { a = decimal\"1.2.3\" }];").has_value());
    CHECK_FALSE(parser::parse("t[update { x = decimal\"1e-39\" }];").has_value());
}

TEST_CASE("Decimal filters are exact across scales and literal kinds", "[decimal][e2e]") {
    const auto tables = prices();
    CHECK(texts(run_ok("t[filter price > 10.49];", tables), "price") ==
          std::vector<std::string>{"10.50", "100.00"});
    // A float literal is the decimal it was written as, not its binary value.
    CHECK(texts(run_ok("t[filter price == 10.5];", tables), "price") ==
          std::vector<std::string>{"10.50"});
    CHECK(texts(run_ok("t[filter price == decimal\"10.500\"];", tables), "price") ==
          std::vector<std::string>{"10.50"});
    CHECK(texts(run_ok("t[filter price >= 100];", tables), "price") ==
          std::vector<std::string>{"100.00"});
    CHECK(texts(run_ok("t[filter price < 0];", tables), "price") ==
          std::vector<std::string>{"-3.25"});
    // Column vs column across scales: 10.50 vs 0.0125, 10.49 vs 1.0000, ...
    CHECK(texts(run_ok("t[filter fee > price];", tables), "price") ==
          std::vector<std::string>{"-3.25"});
    // Row-local arithmetic inside a predicate takes the vectorized path.
    CHECK(texts(run_ok("t[filter price * qty > 70];", tables), "price") ==
          std::vector<std::string>{"10.49", "100.00"});
}

TEST_CASE("Decimal arithmetic result types and exact values", "[decimal][e2e]") {
    const auto tables = prices();
    auto out = run_ok(
        "t[update { total = price * qty, gross = price + fee, net = price - 1, half = price / 2 "
        "}];",
        tables);
    CHECK(texts(out, "total") == std::vector<std::string>{"31.50", "73.43", "-6.50", "100.00"});
    CHECK(type_of(out, "total") == dec_type(29, 2));  // Decimal(10,2) * Decimal(19,0)
    CHECK(texts(out, "gross") ==
          std::vector<std::string>{"10.5125", "11.4900", "-3.7500", "100.0001"});
    CHECK(type_of(out, "gross") == dec_type(13, 4));
    CHECK(texts(out, "net") == std::vector<std::string>{"9.50", "9.49", "-4.25", "99.00"});
    // `/` has one result type, Float64.
    const auto* half = std::get_if<Column<double>>(out.find("half"));
    REQUIRE(half != nullptr);
    CHECK((*half)[0] == 5.25);
    CHECK((*half)[2] == -1.625);

    auto product = run_ok("t[update { p = price * fee }];", tables);
    CHECK(type_of(product, "p") == dec_type(16, 6));
    CHECK(texts(product, "p") ==
          std::vector<std::string>{"0.131250", "10.490000", "1.625000", "0.010000"});
}

TEST_CASE("Decimal overflow is an error, never a wrap", "[decimal][e2e]") {
    runtime::Table t;
    t.add_column("big", dec_col(dec_type(38, 0), {"99999999999999999999999999999999999999"}));
    t.add_column("small", dec_col(dec_type(38, 20), {"1.5"}));
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    CHECK(contains(run_err("t[update { y = big + 1 }];", tables), "decimal overflow"));
    CHECK(contains(run_err("t[update { y = big * 10 }];", tables), "decimal overflow"));
    CHECK(contains(run_err("t[filter big * 10 > 0];", tables), "decimal overflow"));
    // A product whose scale would exceed 38 is refused statically.
    CHECK(contains(run_err("t[update { y = small * small }];", tables), "scale"));
    // The same magnitude with room to spare is fine.
    CHECK(texts(run_ok("t[update { y = big - 1 }];", tables), "y") ==
          std::vector<std::string>{"99999999999999999999999999999999999998"});
}

TEST_CASE("Decimal casts round half away from zero at the boundary", "[decimal][e2e]") {
    runtime::Table t;
    t.add_column("r", dec_col(dec_type(10, 3), {"1.005", "-1.005", "2.500", "-2.500", "1.004"}));
    t.add_column("s", Column<std::string>{"12.345", "-0.005", " 7 ", "1e2", "0.004"});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    auto out = run_ok(
        "t[update { r2 = Decimal(r, 10, 2), r0 = Decimal(r, 10, 0), p = Decimal(s, 10, 2) }];",
        tables);
    CHECK(texts(out, "r2") == std::vector<std::string>{"1.01", "-1.01", "2.50", "-2.50", "1.00"});
    CHECK(texts(out, "r0") == std::vector<std::string>{"1", "-1", "3", "-3", "1"});
    CHECK(texts(out, "p") == std::vector<std::string>{"12.35", "-0.01", "7.00", "100.00", "0.00"});
    CHECK(type_of(out, "p") == dec_type(10, 2));
    // Narrowing below the value's magnitude is an overflow, not a truncation.
    CHECK(contains(run_err("t[update { x = Decimal(r, 2, 2) }];", tables), "overflow"));
    CHECK(contains(run_err("t[update { x = Decimal(\"abc\", 10, 2) }];", tables), "abc"));
}

TEST_CASE("Decimal converts to Float64 and whole values to Int64", "[decimal][e2e]") {
    const auto tables = prices();
    auto out = run_ok("t[update { d = Float64(price) }];", tables);
    const auto* d = std::get_if<Column<double>>(out.find("d"));
    REQUIRE(d != nullptr);
    CHECK((*d)[0] == 10.5);
    CHECK((*d)[2] == -3.25);
    auto whole = run_ok("t[filter price == 100][update { i = Int64(price) }];", tables);
    const auto* i = std::get_if<Column<std::int64_t>>(whole.find("i"));
    REQUIRE(i != nullptr);
    CHECK((*i)[0] == 100);
    CHECK(contains(run_err("t[update { i = Int64(price) }];", tables), "not a whole number"));
}

TEST_CASE("Decimal refuses to mix with Float64 columns", "[decimal][e2e]") {
    const auto tables = prices();
    CHECK(contains(run_err("t[update { z = price + f }];", tables), "Float64"));
    CHECK(contains(run_err("t[update { z = price % 2 }];", tables), "%"));
}

TEST_CASE("Decimal nulls propagate through arithmetic and filters", "[decimal][e2e]") {
    runtime::Table t;
    auto col = dec_col(dec_type(10, 2), {"1.50", "0.00", "-2.25"});
    // Garbage in a null slot must not surface as an overflow.
    col[1] = Decimal{decimal::kMaxUnits};
    t.add_column("price", std::move(col), runtime::ValidityBitmap{true, false, true});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    auto out = run_ok("t[update { y = price * decimal\"1000000000000000000000000000\" }];", tables);
    CHECK(texts(out, "y")[1] == "null");
    auto vec = run_ok("t[filter price * 2 > -10];", tables);
    CHECK(texts(vec, "price") == std::vector<std::string>{"1.50", "-2.25"});
    auto sum = run_ok("t[select { s = sum(price), n = count(price) }];", tables);
    CHECK(texts(sum, "s") == std::vector<std::string>{"-0.75"});
}

TEST_CASE("Decimal sorts by value, including negatives and wide values", "[decimal][e2e]") {
    const auto tables = prices();
    CHECK(texts(run_ok("t[order price];", tables), "price") ==
          std::vector<std::string>{"-3.25", "10.49", "10.50", "100.00"});
    CHECK(texts(run_ok("t[order { price desc }];", tables), "price") ==
          std::vector<std::string>{"100.00", "10.50", "10.49", "-3.25"});

    // More than 18 digits: units no longer fit int64, so the ordinal path.
    runtime::Table wide;
    wide.add_column(
        "v", dec_col(dec_type(38, 0), {"12345678901234567890123", "-99999999999999999999", "5",
                                       "12345678901234567890122"}));
    runtime::TableRegistry wide_tables;
    wide_tables.emplace("t", std::move(wide));
    CHECK(texts(run_ok("t[order v];", wide_tables), "v") ==
          std::vector<std::string>{"-99999999999999999999", "5", "12345678901234567890122",
                                   "12345678901234567890123"});
}

TEST_CASE("Decimal grouping and aggregates", "[decimal][e2e]") {
    runtime::Table t;
    t.add_column("acct", Column<std::string>{"a", "b", "a", "b", "a"});
    t.add_column("amt", dec_col(dec_type(12, 2), {"0.10", "5.00", "0.20", "-5.00", "0.30"}));
    t.add_column("band", dec_col(dec_type(4, 1), {"1.0", "2.5", "1.0", "2.5", "1.0"}));
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run_ok(
        "t[select { s = sum(amt), lo = min(amt), hi = max(amt), n = count(), f = first(amt), "
        "l = last(amt) }, by acct][order acct];",
        tables);
    // 0.10 + 0.20 + 0.30 is exactly 0.60 -- the case a double gets wrong.
    CHECK(texts(out, "s") == std::vector<std::string>{"0.60", "0.00"});
    CHECK(type_of(out, "s") == dec_type(38, 2));
    CHECK(texts(out, "lo") == std::vector<std::string>{"0.10", "-5.00"});
    CHECK(texts(out, "hi") == std::vector<std::string>{"0.30", "5.00"});
    CHECK(type_of(out, "hi") == dec_type(12, 2));
    CHECK(texts(out, "f") == std::vector<std::string>{"0.10", "5.00"});
    CHECK(texts(out, "l") == std::vector<std::string>{"0.30", "-5.00"});

    auto mean = run_ok("t[select { m = mean(amt) }, by acct][order acct];", tables);
    const auto* m = std::get_if<Column<double>>(mean.find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[0] == 0.2);
    CHECK((*m)[1] == 0.0);

    auto total = run_ok("t[select { s = sum(amt) }];", tables);
    CHECK(texts(total, "s") == std::vector<std::string>{"0.60"});

    // A Decimal group key.
    auto by_band = run_ok("t[select { s = sum(amt) }, by band][order band];", tables);
    CHECK(texts(by_band, "band") == std::vector<std::string>{"1.0", "2.5"});
    CHECK(texts(by_band, "s") == std::vector<std::string>{"0.60", "0.00"});

    CHECK(contains(run_err("t[select { m = median(amt) }];", tables), "Decimal"));
}

TEST_CASE("Decimal join keys match by value; mismatched scales are refused", "[decimal][e2e]") {
    runtime::Table left;
    left.add_column("k", dec_col(dec_type(10, 2), {"1.50", "2.00", "3.25"}));
    left.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::Table right;
    right.add_column("k", dec_col(dec_type(10, 2), {"3.25", "1.50", "9.99"}));
    right.add_column("y", Column<std::int64_t>{30, 10, 99});
    runtime::Table other;
    other.add_column("k", dec_col(dec_type(10, 3), {"1.500"}));
    runtime::TableRegistry tables;
    tables.emplace("a", std::move(left));
    tables.emplace("b", std::move(right));
    tables.emplace("c", std::move(other));

    auto out = run_ok("(a join b on k)[order k];", tables);
    CHECK(texts(out, "k") == std::vector<std::string>{"1.50", "3.25"});
    const auto* y = std::get_if<Column<std::int64_t>>(out.find("y"));
    REQUIRE(y != nullptr);
    CHECK((*y)[0] == 10);
    CHECK((*y)[1] == 30);
    CHECK(contains(run_err("a join c on k;", tables), "scale"));
}

TEST_CASE("Decimal table literals keep their values and a unified type", "[decimal][e2e]") {
    // Regression: the Table-constructor lowering once copied only an allow-list
    // of literal types, so decimal elements silently became Int64 zeros.
    const runtime::TableRegistry none;
    auto out = run_ok(R"(Table { amt = [decimal"1.5", decimal"2.25", decimal"-100"] };)", none);
    CHECK(type_of(out, "amt") == dec_type(5, 2));
    CHECK(texts(out, "amt") == std::vector<std::string>{"1.50", "2.25", "-100.00"});
}

TEST_CASE("Decimal keeps its scale through operators that rebuild columns", "[decimal][e2e]") {
    // Each of these built a fresh output column: without carrying the source's
    // precision and scale, 1.50 read back as 150 (Decimal(38, 0)).
    runtime::Table t;
    t.add_column("k", dec_col(dec_type(6, 2), {"1.50", "2.00", "3.25", "4.00"}));
    t.add_column("v", Column<std::int64_t>{1, 2, 3, 4});
    runtime::Table holes;
    auto hk = dec_col(dec_type(6, 2), {"1.50", "0.00", "3.25"});
    holes.add_column("k", std::move(hk), runtime::ValidityBitmap{true, false, true});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    tables.emplace("h", std::move(holes));

    auto shifted = run_ok("t[update { p = lag(k, 1), n = lead(k, 1) }];", tables);
    CHECK(texts(shifted, "p") == std::vector<std::string>{"null", "1.50", "2.00", "3.25"});
    CHECK(texts(shifted, "n") == std::vector<std::string>{"2.00", "3.25", "4.00", "null"});
    CHECK(type_of(shifted, "p") == dec_type(6, 2));

    auto filled =
        run_ok(R"(h[update { a = fill_null(k, decimal"0.5"), b = fill_null(k, 7) }];)", tables);
    CHECK(texts(filled, "a") == std::vector<std::string>{"1.50", "0.50", "3.25"});
    CHECK(texts(filled, "b") == std::vector<std::string>{"1.50", "7.00", "3.25"});
    CHECK(type_of(filled, "a") == dec_type(6, 2));

    // coalesce across scales: the narrowest type holding both, each value
    // rescaled into it.
    auto coalesced = run_ok(R"(h[update { c = coalesce(k, decimal"0.125") }];)", tables);
    CHECK(type_of(coalesced, "c") == dec_type(7, 3));
    CHECK(texts(coalesced, "c") == std::vector<std::string>{"1.500", "0.125", "3.250"});
}

TEST_CASE("Decimal rbind, distinct, and semi/anti joins", "[decimal][e2e]") {
    runtime::Table a;
    a.add_column("k", dec_col(dec_type(6, 2), {"1.50", "2.00", "1.50"}));
    runtime::Table b;
    b.add_column("k", dec_col(dec_type(4, 1), {"1.5", "9.0"}));
    runtime::Table c;
    c.add_column("k", dec_col(dec_type(6, 2), {"1.50", "9.00"}));
    runtime::TableRegistry tables;
    tables.emplace("a", std::move(a));
    tables.emplace("b", std::move(b));
    tables.emplace("c", std::move(c));

    // rbind across scales: units rescaled, not concatenated raw.
    auto bound = run_ok("rbind(a, b);", tables);
    CHECK(type_of(bound, "k") == dec_type(6, 2));
    CHECK(texts(bound, "k") == std::vector<std::string>{"1.50", "2.00", "1.50", "1.50", "9.00"});

    // Regression: a single Decimal column once produced no rows at all.
    CHECK(texts(run_ok("a[distinct k];", tables), "k") == std::vector<std::string>{"1.50", "2.00"});

    CHECK(texts(run_ok("a semi join c on k;", tables), "k") ==
          std::vector<std::string>{"1.50", "1.50"});
    CHECK(texts(run_ok("a anti join c on k;", tables), "k") == std::vector<std::string>{"2.00"});
    // A scale mismatch is an error, never a silently empty result.
    CHECK(contains(run_err("a semi join b on k;", tables), "scale"));
}

TEST_CASE("Decimal renders with exactly its scale", "[decimal][e2e]") {
    const auto tables = prices();
    std::ostringstream text;
    runtime::format_table(run_ok("t[select { price }];", tables), text, 10);
    CHECK(contains(text.str(), "100.00"));
    CHECK(contains(text.str(), "-3.25"));
}

TEST_CASE("CSV decimal(p,s) reads and writes exactly", "[decimal][csv]") {
    const auto path = std::filesystem::temp_directory_path() / "ibex_decimal_roundtrip.csv";
    {
        std::ofstream out(path);
        out << "id,price\n1,12.30\n2,-0.05\n3,1.005\n4,0.1\n";
    }
    auto table = read_csv(path.string(), "", ",", true, "id:int,price:decimal(12,2)");
    runtime::TableRegistry tables;
    tables.emplace("t", table);
    CHECK(texts(table, "price") == std::vector<std::string>{"12.30", "-0.05", "1.01", "0.10"});
    CHECK(type_of(table, "price") == dec_type(12, 2));
    CHECK(texts(run_ok("t[select { s = sum(price) }];", tables), "s") ==
          std::vector<std::string>{"13.36"});

    const auto out_path = std::filesystem::temp_directory_path() / "ibex_decimal_out.csv";
    write_csv(table, out_path.string());
    std::ifstream in(out_path);
    const std::string written((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    CHECK(contains(written, "12.30"));
    CHECK(contains(written, "-0.05"));
    auto again = read_csv(out_path.string(), "", ",", true, "id:int,price:decimal(12,2)");
    CHECK(texts(again, "price") == texts(table, "price"));

    CHECK_THROWS(read_csv(path.string(), "", ",", true, "id:int,price:decimal(40,2)"));
    CHECK_THROWS(read_csv(path.string(), "", ",", true, "id:int,price:decimal"));
    {
        std::ofstream out(path);
        out << "id,price\n1,12345.67\n";
    }
    CHECK_THROWS(read_csv(path.string(), "", ",", true, "id:int,price:decimal(5,2)"));
}

TEST_CASE("Arrow C Data round-trips decimal128 exactly", "[decimal][interop][arrow]") {
    runtime::Table source;
    source.add_column("amt",
                      dec_col(dec_type(38, 10),
                              {"1234567890123456789012345678.0123456789", "-0.0000000001", "0"}),
                      runtime::ValidityBitmap{true, true, false});

    ArrowArray array{};
    ArrowSchema schema{};
    REQUIRE(interop::export_table_to_arrow(source, &array, &schema).has_value());
    REQUIRE(schema.n_children == 1);
    CHECK(std::string(schema.children[0]->format) == "d:38,10");

    auto imported = interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());
    CHECK(type_of(*imported, "amt") == dec_type(38, 10));
    CHECK(texts(*imported, "amt") ==
          std::vector<std::string>{"1234567890123456789012345678.0123456789", "-0.0000000001",
                                   "null"});

    // Unsupported decimals are refused by name, not read approximately.
    const char* original = schema.children[0]->format;
    for (const char* bad : {"d:40,2", "d:10,2,256", "d:10,-2", "d:5,6", "d:10"}) {
        schema.children[0]->format = bad;
        auto rejected = interop::import_table_from_arrow(array, schema);
        INFO(bad);
        REQUIRE_FALSE(rejected.has_value());
        CHECK(contains(rejected.error(), "decimal"));
    }

    // decimal32 widens.
    std::array<std::int32_t, 3> narrow{12345, -1, 0};
    const void* saved = array.children[0]->buffers[1];
    array.children[0]->buffers[1] = narrow.data();
    schema.children[0]->format = "d:9,2,32";
    auto widened = interop::import_table_from_arrow(array, schema);
    REQUIRE(widened.has_value());
    CHECK(texts(*widened, "amt") == std::vector<std::string>{"123.45", "-0.01", "null"});
    array.children[0]->buffers[1] = saved;
    schema.children[0]->format = original;

    schema.release(&schema);
    array.release(&array);
}
