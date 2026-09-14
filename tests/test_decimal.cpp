// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/decimal.hpp>

#include <catch2/catch_test_macros.hpp>

#include <compare>
#include <cstdint>
#include <string>

using ibex::DecimalType;
using ibex::Int128;
namespace dec = ibex::decimal;

namespace {

auto parse_units(std::string_view text, int p, int s) -> Int128 {
    auto r = dec::parse(text, DecimalType{.precision = static_cast<std::uint8_t>(p),
                                          .scale = static_cast<std::uint8_t>(s)});
    REQUIRE(r.has_value());
    return *r;
}

auto str(Int128 units, int scale) -> std::string {
    return dec::to_string(units, scale);
}

}  // namespace

TEST_CASE("decimal formatting prints exactly scale digits", "[decimal]") {
    CHECK(str(1230, 2) == "12.30");
    CHECK(str(-5, 2) == "-0.05");
    CHECK(str(0, 2) == "0.00");
    CHECK(str(7, 0) == "7");
    CHECK(str(-7, 0) == "-7");
    CHECK(str(dec::kMaxUnits, 0) == std::string(38, '9'));
    CHECK(str(-dec::kMaxUnits, 38) == "-0." + std::string(38, '9'));
}

TEST_CASE("decimal parsing is exact and round-trips through text", "[decimal]") {
    CHECK(parse_units("12.30", 10, 2) == 1230);
    CHECK(parse_units("  -0.05 ", 10, 2) == -5);
    CHECK(parse_units("+3", 10, 2) == 300);
    CHECK(parse_units("1.5e2", 10, 2) == 15000);
    CHECK(parse_units("150E-2", 10, 2) == 150);
    CHECK(parse_units(".5", 10, 1) == 5);
    const std::string max38(38, '9');
    CHECK(str(parse_units(max38, 38, 0), 0) == max38);
    CHECK(str(parse_units("-0." + max38, 38, 38), 38) == "-0." + max38);

    CHECK_FALSE(dec::parse("", DecimalType{}).has_value());
    CHECK_FALSE(dec::parse("abc", DecimalType{}).has_value());
    CHECK_FALSE(dec::parse("1.2.3", DecimalType{}).has_value());
    CHECK_FALSE(dec::parse("1x", DecimalType{}).has_value());
    CHECK_FALSE(dec::parse("-", DecimalType{}).has_value());
    CHECK_FALSE(dec::parse("1e", DecimalType{}).has_value());
    // 39 significant digits never fit.
    CHECK_FALSE(dec::parse(std::string(39, '9'), DecimalType{}).has_value());
}

TEST_CASE("decimal rounding is half away from zero at every boundary", "[decimal]") {
    CHECK(parse_units("1.005", 10, 2) == 101);
    CHECK(parse_units("-1.005", 10, 2) == -101);
    CHECK(parse_units("1.0049999", 10, 2) == 100);
    CHECK(parse_units("-1.0049999", 10, 2) == -100);
    CHECK(parse_units("0.5", 10, 0) == 1);
    CHECK(parse_units("-0.5", 10, 0) == -1);
    CHECK(parse_units("0.4999", 10, 0) == 0);
    CHECK(parse_units("2.5", 10, 0) == 3);  // not banker's
    // More than 38 dropped digits rounds to zero rather than tripping pow10.
    CHECK(parse_units("0." + std::string(39, '0') + "9", 10, 0) == 0);

    Int128 out = 0;
    REQUIRE(dec::rescale(12345, 3, 1, out));
    CHECK(out == 123);
    REQUIRE(dec::rescale(12355, 3, 1, out));
    CHECK(out == 124);
    REQUIRE(dec::rescale(-12355, 3, 1, out));
    CHECK(out == -124);
    REQUIRE(dec::rescale(5, 0, 3, out));
    CHECK(out == 5000);
    // Dropping all 38 digits of the largest value: 0.99..9 rounds to 1.
    REQUIRE(dec::rescale(dec::kMaxUnits, 38, 0, out));
    CHECK(out == 1);
}

TEST_CASE("decimal precision is enforced when fitting", "[decimal]") {
    CHECK(dec::parse("999.99", DecimalType{.precision = 5, .scale = 2}).has_value());
    CHECK_FALSE(dec::parse("1000.00", DecimalType{.precision = 5, .scale = 2}).has_value());
    // Rounding can carry into a new digit, which must also be checked.
    CHECK_FALSE(dec::parse("999.995", DecimalType{.precision = 5, .scale = 2}).has_value());
    CHECK_FALSE(dec::from_int64(100000, DecimalType{.precision = 5, .scale = 0}).has_value());
    CHECK(dec::from_int64(INT64_MIN, dec::kInt64Type).has_value());
    CHECK(*dec::from_int64(-42, DecimalType{.precision = 10, .scale = 2}) == -4200);
}

TEST_CASE("decimal checked arithmetic never exceeds 38 digits", "[decimal]") {
    Int128 out = 0;
    CHECK(dec::checked_add(dec::kMaxUnits - 1, 1, out));
    CHECK(out == dec::kMaxUnits);
    CHECK_FALSE(dec::checked_add(dec::kMaxUnits, 1, out));
    CHECK_FALSE(dec::checked_add(-dec::kMaxUnits, -1, out));
    // Two maximal operands would overflow int128 itself if added unchecked.
    CHECK_FALSE(dec::checked_add(dec::kMaxUnits, dec::kMaxUnits, out));
    CHECK(dec::checked_add(dec::kMaxUnits, -dec::kMaxUnits, out));
    CHECK(out == 0);
    CHECK_FALSE(dec::checked_sub(-dec::kMaxUnits, 1, out));

    CHECK(dec::checked_mul(dec::pow10(19), dec::pow10(18), out));
    CHECK(out == dec::pow10(37));
    CHECK_FALSE(dec::checked_mul(dec::pow10(19), dec::pow10(19), out));
    CHECK_FALSE(dec::checked_mul(-dec::pow10(20), dec::pow10(19), out));
    CHECK(dec::checked_mul(-3, 7, out));
    CHECK(out == -21);
    CHECK(dec::checked_mul(0, dec::kMaxUnits, out));
    CHECK(out == 0);
    CHECK_FALSE(dec::rescale(dec::pow10(37), 0, 2, out));
}

TEST_CASE("decimal comparison is exact across scales", "[decimal]") {
    using so = std::strong_ordering;
    CHECK(dec::compare(100, 2, 1, 0) == so::equal);    // 1.00 == 1
    CHECK(dec::compare(105, 2, 1, 0) == so::greater);  // 1.05 > 1
    CHECK(dec::compare(-105, 2, -1, 0) == so::less);   // -1.05 < -1
    CHECK(dec::compare(1, 0, 100, 2) == so::equal);
    CHECK(dec::compare(1, 38, 0, 0) == so::greater);
    // Upscaling the lower-scale side past 38 digits: its sign decides.
    CHECK(dec::compare(dec::pow10(30), 0, dec::kMaxUnits, 38) == so::greater);
    CHECK(dec::compare(-dec::pow10(30), 0, dec::kMaxUnits, 38) == so::less);
    CHECK(dec::compare(dec::kMaxUnits, 38, -dec::pow10(30), 0) == so::greater);
}

TEST_CASE("decimal result types follow the documented rules", "[decimal]") {
    const DecimalType a{.precision = 10, .scale = 2};
    const DecimalType b{.precision = 5, .scale = 4};
    CHECK(dec::add_result_type(a, b) == DecimalType{.precision = 13, .scale = 4});
    CHECK(dec::mul_result_type(a, b) == DecimalType{.precision = 15, .scale = 6});
    CHECK(dec::add_result_type(DecimalType{.precision = 38, .scale = 2}, a) ==
          DecimalType{.precision = 38, .scale = 2});
    CHECK(dec::sum_result_type(a) == DecimalType{.precision = 38, .scale = 2});
    CHECK(dec::mul_scale_ok(DecimalType{.precision = 38, .scale = 20},
                            DecimalType{.precision = 38, .scale = 18}));
    CHECK_FALSE(dec::mul_scale_ok(DecimalType{.precision = 38, .scale = 20},
                                  DecimalType{.precision = 38, .scale = 19}));
    CHECK(dec::add_result_type(dec::kInt64Type, a) == DecimalType{.precision = 22, .scale = 2});
}

TEST_CASE("decimal literals carry their own precision and scale", "[decimal]") {
    auto v = dec::parse_literal("12.30");
    REQUIRE(v.has_value());
    CHECK(v->units == 1230);
    CHECK(v->type == DecimalType{.precision = 4, .scale = 2});
    v = dec::parse_literal("0.05");
    REQUIRE(v.has_value());
    CHECK(v->type == DecimalType{.precision = 2, .scale = 2});
    v = dec::parse_literal("-007");
    REQUIRE(v.has_value());
    CHECK(v->units == -7);
    CHECK(v->type == DecimalType{.precision = 1, .scale = 0});
    v = dec::parse_literal("0");
    REQUIRE(v.has_value());
    CHECK(v->type == DecimalType{.precision = 1, .scale = 0});
    v = dec::parse_literal("1.5e3");
    REQUIRE(v.has_value());
    CHECK(v->units == 1500);
    CHECK(v->type == DecimalType{.precision = 4, .scale = 0});
    CHECK_FALSE(dec::parse_literal(std::string(39, '1')).has_value());
}

TEST_CASE("decimal numeric conversions", "[decimal]") {
    CHECK(dec::to_double(1230, 2) == 12.3);
    CHECK(dec::to_double(-5, 2) == -0.05);
    CHECK(dec::to_double(1, 1) == 0.1);  // correctly rounded, not 1 * 0.1
    // Slow path (beyond 2^53) must also round correctly.
    CHECK(dec::to_double(dec::kMaxUnits, 0) == 1e38);

    auto d = dec::from_double(0.1, DecimalType{.precision = 10, .scale = 2});
    REQUIRE(d.has_value());
    CHECK(*d == 10);  // shortest text "0.1", not the binary expansion
    d = dec::from_double(2.675, DecimalType{.precision = 10, .scale = 2});
    REQUIRE(d.has_value());
    CHECK(*d == 268);  // text "2.675" rounds half away, though the double is 2.67499...
    CHECK_FALSE(dec::from_double(0.0 / 0.0, DecimalType{}).has_value());
    CHECK_FALSE(dec::from_double(1e40, DecimalType{}).has_value());

    auto lit = dec::literal_from_double(10.5);
    REQUIRE(lit.has_value());
    CHECK(lit->units == 105);
    CHECK(lit->type == DecimalType{.precision = 3, .scale = 1});

    CHECK(*dec::to_int64(1200, 2) == 12);
    CHECK_FALSE(dec::to_int64(1201, 2).has_value());
    CHECK_FALSE(dec::to_int64(dec::pow10(20), 0).has_value());
    CHECK(*dec::to_int64(-dec::pow10(18) * 9, 0) == -9'000'000'000'000'000'000);
}

TEST_CASE("decimal division to double rounds once, not twice", "[decimal]") {
    // 0.60 / 3: dividing doubles gives 0.19999999999999998.
    CHECK(dec::divide_to_double(60, 2, 3) == 0.2);
    CHECK(dec::divide_to_double(-60, 2, 3) == -0.2);
    CHECK(dec::divide_to_double(100, 2, 3) == 1.0 / 3.0);
    CHECK(dec::divide_to_double(0, 5, 7) == 0.0);
    // A dividend already at 38 digits cannot widen; the quotient still rounds.
    CHECK(dec::divide_to_double(dec::kMaxUnits, 0, 1) == 1e38);
    CHECK(dec::divide_to_double(dec::pow10(37), 37, 4) == 0.25);
}

TEST_CASE("decimal hash distinguishes high and low halves", "[decimal]") {
    const std::hash<ibex::Decimal> h;
    CHECK(h(ibex::Decimal{.units = 1}) != h(ibex::Decimal{.units = Int128{1} << 64}));
    CHECK(h(ibex::Decimal{.units = -1}) != h(ibex::Decimal{.units = 1}));
    CHECK(h(ibex::Decimal{.units = 42}) == h(ibex::Decimal{.units = 42}));
}
