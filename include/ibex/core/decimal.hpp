// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <functional>
#include <locale>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#if defined(_MSC_VER) && !defined(__clang__)
#include <__msvc_int128.hpp>
#endif

namespace ibex {

/// Signed 128-bit integer. MSVC has no `__int128`; its STL's `_Signed128`
/// (the type behind `iota_view`'s difference type) supports the same
/// arithmetic. Nothing here relies on overflow builtins, so the two are
/// interchangeable: every checked operation compares against the decimal
/// bound instead, which keeps every intermediate far inside int128's range.
#if defined(_MSC_VER) && !defined(__clang__)
using Int128 = std::_Signed128;
#else
__extension__ using Int128 = __int128;
#endif

/// Precision and scale of a decimal: `precision` significant digits, `scale`
/// of them after the point. Invariant (enforced by `decimal::valid_type`):
/// `1 <= precision <= 38` and `scale <= precision`.
struct DecimalType {
    std::uint8_t precision = 38;
    std::uint8_t scale = 0;
    [[nodiscard]] auto operator==(const DecimalType&) const -> bool = default;
};

/// Column element of a `Decimal(p, s)` column: a count of `10^-s` units.
///
/// Deliberately carries no scale. Precision and scale are properties of the
/// column (`ColumnMeta::decimal`), which travels with it through every copy
/// and gather; a per-row scale would double the width and make every kernel
/// re-check what the type already guarantees.
struct Decimal {
    Int128 units = 0;

    // Spelled out rather than defaulted: MSVC's `_Signed128` is not
    // guaranteed to provide `<=>`, but it does provide `==` and `<`.
    friend constexpr auto operator==(const Decimal& a, const Decimal& b) noexcept -> bool {
        return a.units == b.units;
    }
    friend constexpr auto operator<=>(const Decimal& a, const Decimal& b) noexcept
        -> std::strong_ordering {
        if (a.units == b.units) {
            return std::strong_ordering::equal;
        }
        return a.units < b.units ? std::strong_ordering::less : std::strong_ordering::greater;
    }
};

/// A self-describing decimal scalar. A scalar has no column to hang its
/// precision and scale on, so it carries them. Equality is structural (same
/// units at the same type); numeric comparison across scales is
/// `decimal::compare`.
struct DecimalValue {
    Int128 units = 0;
    DecimalType type;

    friend constexpr auto operator==(const DecimalValue& a, const DecimalValue& b) noexcept
        -> bool {
        return a.units == b.units && a.type == b.type;
    }
};

namespace decimal {

inline constexpr int kMaxPrecision = 38;

/// The type an Int64 is read as when it meets a decimal: every int64 value
/// has at most 19 digits, so this is exact.
inline constexpr DecimalType kInt64Type{.precision = 19, .scale = 0};

[[nodiscard]] constexpr auto valid_type(DecimalType t) noexcept -> bool {
    return t.precision >= 1 && t.precision <= kMaxPrecision && t.scale <= t.precision;
}

namespace detail {
[[nodiscard]] constexpr auto make_pow10() noexcept -> std::array<Int128, kMaxPrecision + 1> {
    std::array<Int128, kMaxPrecision + 1> out{};
    out[0] = 1;
    // Stops at 10^38: one more step would overflow int128 (10^39 > 2^127),
    // which is not a constant expression.
    for (std::size_t i = 1; i < out.size(); ++i) {
        out[i] = out[i - 1] * 10;
    }
    return out;
}
inline constexpr std::array<Int128, kMaxPrecision + 1> kPow10 = make_pow10();
}  // namespace detail

/// `10^k` for `0 <= k <= 38`.
[[nodiscard]] constexpr auto pow10(int k) noexcept -> Int128 {
    return detail::kPow10[static_cast<std::size_t>(k)];
}

/// Largest magnitude a `Decimal(precision, _)` holds: `10^precision - 1`.
[[nodiscard]] constexpr auto max_units(int precision) noexcept -> Int128 {
    return pow10(precision) - 1;
}

/// The bound every stored value and every intermediate respects.
inline constexpr Int128 kMaxUnits = detail::kPow10[kMaxPrecision] - 1;

[[nodiscard]] constexpr auto magnitude(Int128 v) noexcept -> Int128 {
    return v < 0 ? -v : v;
}

[[nodiscard]] constexpr auto fits(Int128 units, int precision) noexcept -> bool {
    return magnitude(units) <= max_units(precision);
}

/// Checked `a + b` for operands inside ±kMaxUnits. False on overflow of the
/// 38-digit bound (the caller checks the result type's own precision).
[[nodiscard]] constexpr auto checked_add(Int128 a, Int128 b, Int128& out) noexcept -> bool {
    if ((b > 0 && a > kMaxUnits - b) || (b < 0 && a < -kMaxUnits - b)) {
        return false;
    }
    out = a + b;
    return true;
}

[[nodiscard]] constexpr auto checked_sub(Int128 a, Int128 b, Int128& out) noexcept -> bool {
    return checked_add(a, -b, out);
}

[[nodiscard]] constexpr auto checked_mul(Int128 a, Int128 b, Int128& out) noexcept -> bool {
    if (a == 0 || b == 0) {
        out = 0;
        return true;
    }
    if (magnitude(a) > kMaxUnits / magnitude(b)) {
        return false;
    }
    out = a * b;
    return true;
}

/// Change the scale of `units` from `from` to `to`. Growing the scale is exact
/// and fails only past the 38-digit bound; shrinking it rounds **half away
/// from zero** — the single rounding rule Ibex decimals use everywhere.
[[nodiscard]] constexpr auto rescale(Int128 units, int from, int to, Int128& out) noexcept -> bool {
    if (to == from) {
        out = units;
        return true;
    }
    if (to > from) {
        return checked_mul(units, pow10(to - from), out);
    }
    const Int128 divisor = pow10(from - to);
    Int128 q = units / divisor;
    const Int128 r = magnitude(units % divisor);
    // `r >= divisor - r` is `2r >= divisor` without forming 2r, which can
    // exceed int128 when divisor is 10^38.
    if (r >= divisor - r) {
        q = units < 0 ? q - 1 : q + 1;
    }
    out = q;
    return true;
}

/// Exact three-way comparison of `a·10^-sa` against `b·10^-sb`.
[[nodiscard]] constexpr auto compare(Int128 a, int sa, Int128 b, int sb) noexcept
    -> std::strong_ordering {
    auto cmp = [](Int128 x, Int128 y) {
        if (x == y) {
            return std::strong_ordering::equal;
        }
        return x < y ? std::strong_ordering::less : std::strong_ordering::greater;
    };
    if (sa == sb) {
        return cmp(a, b);
    }
    if (sa < sb) {
        Int128 up = 0;
        if (checked_mul(a, pow10(sb - sa), up)) {
            return cmp(up, b);
        }
        // |a·10^k| > kMaxUnits >= |b|: a's sign decides.
        return a < 0 ? std::strong_ordering::less : std::strong_ordering::greater;
    }
    Int128 up = 0;
    if (checked_mul(b, pow10(sa - sb), up)) {
        return cmp(a, up);
    }
    return b < 0 ? std::strong_ordering::greater : std::strong_ordering::less;
}

[[nodiscard]] constexpr auto compare(const DecimalValue& a, const DecimalValue& b) noexcept
    -> std::strong_ordering {
    return compare(a.units, a.type.scale, b.units, b.type.scale);
}

// ── Static result-type rules (plans/decimal-plan.md) ────────────────────────

[[nodiscard]] constexpr auto add_result_type(DecimalType a, DecimalType b) noexcept -> DecimalType {
    const int scale = std::max(a.scale, b.scale);
    const int integral = std::max(a.precision - a.scale, b.precision - b.scale);
    const int precision = std::min(kMaxPrecision, integral + scale + 1);
    return {.precision = static_cast<std::uint8_t>(precision),
            .scale = static_cast<std::uint8_t>(scale)};
}

/// `nullopt`-free by convention: callers must first check `mul_scale_ok`.
[[nodiscard]] constexpr auto mul_scale_ok(DecimalType a, DecimalType b) noexcept -> bool {
    return a.scale + b.scale <= kMaxPrecision;
}

[[nodiscard]] constexpr auto mul_result_type(DecimalType a, DecimalType b) noexcept -> DecimalType {
    const int scale = a.scale + b.scale;
    const int precision = std::max(scale, std::min(kMaxPrecision, a.precision + b.precision));
    return {.precision = static_cast<std::uint8_t>(std::max(precision, 1)),
            .scale = static_cast<std::uint8_t>(scale)};
}

/// The narrowest type holding every value of both `a` and `b` exactly (capped
/// at 38 digits): for CASE arms, coalesce, and list literals that mix scales.
[[nodiscard]] constexpr auto union_type(DecimalType a, DecimalType b) noexcept -> DecimalType {
    const int scale = std::max(a.scale, b.scale);
    const int integral = std::max(a.precision - a.scale, b.precision - b.scale);
    const int precision = std::max(1, std::min(kMaxPrecision, integral + scale));
    return {.precision = static_cast<std::uint8_t>(precision),
            .scale = static_cast<std::uint8_t>(std::min(scale, precision))};
}

[[nodiscard]] constexpr auto sum_result_type(DecimalType a) noexcept -> DecimalType {
    return {.precision = static_cast<std::uint8_t>(kMaxPrecision), .scale = a.scale};
}

// ── Text ────────────────────────────────────────────────────────────────────

/// Render `units·10^-scale` with exactly `scale` fractional digits: `12.30`,
/// `-0.05`, `7`. This is the text CSV writes and the REPL prints, so a
/// decimal round-trips through text unchanged.
[[nodiscard]] inline auto to_string(Int128 units, int scale) -> std::string {
    std::array<char, 48> digits{};
    const auto frac = static_cast<std::size_t>(std::clamp(scale, 0, kMaxPrecision));
    std::size_t n = 0;
    Int128 mag = magnitude(units);
    do {
        digits[n++] = static_cast<char>('0' + static_cast<int>(mag % 10));
        mag = mag / 10;
    } while (mag != 0);
    // Pad so there is at least one digit before the point.
    while (n <= frac) {
        digits[n++] = '0';
    }
    std::string out;
    out.reserve(n + 2);
    if (units < 0) {
        out.push_back('-');
    }
    for (std::size_t i = n; i-- > 0;) {
        out.push_back(digits[i]);
        if (i == frac && frac > 0) {
            out.push_back('.');
        }
    }
    return out;
}

[[nodiscard]] inline auto to_string(const DecimalValue& v) -> std::string {
    return to_string(v.units, v.type.scale);
}

/// A decimal number read from text, before it is fitted to a type.
struct ParsedText {
    Int128 units = 0;  ///< all significant digits, sign applied
    int scale = 0;     ///< digits after the point, less the exponent (may be < 0)
    int integral_digits = 0;
};

/// Parse `[+-]digits[.digits][(e|E)[+-]digits]`, surrounding ASCII whitespace
/// ignored. Leading zeros do not count toward precision.
[[nodiscard]] inline auto parse_text(std::string_view text)
    -> std::expected<ParsedText, std::string> {
    auto is_space = [](char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; };
    while (!text.empty() && is_space(text.front())) {
        text.remove_prefix(1);
    }
    while (!text.empty() && is_space(text.back())) {
        text.remove_suffix(1);
    }
    if (text.empty()) {
        return std::unexpected("empty decimal");
    }
    ParsedText out;
    bool negative = false;
    std::size_t i = 0;
    if (text[i] == '+' || text[i] == '-') {
        negative = text[i] == '-';
        ++i;
    }
    int significant = 0;  // digits accumulated into units (leading zeros skipped)
    int integral = 0;
    int fraction = 0;
    bool any_digit = false;
    bool seen_point = false;
    for (; i < text.size(); ++i) {
        const char c = text[i];
        if (c == '.') {
            if (seen_point) {
                return std::unexpected("invalid decimal '" + std::string(text) + "'");
            }
            seen_point = true;
            continue;
        }
        if (c < '0' || c > '9') {
            break;
        }
        any_digit = true;
        if (seen_point) {
            ++fraction;
        }
        if (out.units == 0 && c == '0') {
            // A leading zero contributes scale (after the point) but never
            // precision.
            continue;
        }
        if (significant >= kMaxPrecision) {
            return std::unexpected("decimal '" + std::string(text) + "' has more than 38 digits");
        }
        out.units = (out.units * 10) + (c - '0');
        ++significant;
        if (!seen_point) {
            ++integral;
        }
    }
    if (!any_digit) {
        return std::unexpected("invalid decimal '" + std::string(text) + "'");
    }
    int exponent = 0;
    if (i < text.size() && (text[i] == 'e' || text[i] == 'E')) {
        ++i;
        const char* first = text.data() + i;
        if (i < text.size() && text[i] == '+') {
            ++first;
        }
        const auto [ptr, ec] = std::from_chars(first, text.data() + text.size(), exponent);
        if (ec != std::errc{} || first == text.data() + text.size()) {
            return std::unexpected("invalid decimal exponent in '" + std::string(text) + "'");
        }
        i = static_cast<std::size_t>(ptr - text.data());
        if (exponent < -1000 || exponent > 1000) {
            return std::unexpected("decimal exponent out of range in '" + std::string(text) + "'");
        }
    }
    if (i != text.size()) {
        return std::unexpected("invalid decimal '" + std::string(text) + "'");
    }
    out.scale = fraction - exponent;
    out.integral_digits = std::max(0, integral + exponent);
    if (negative) {
        out.units = -out.units;
    }
    return out;
}

/// Fit parsed text to `target`, rounding extra fractional digits half away
/// from zero. Fails when the value needs more than `target.precision` digits.
[[nodiscard]] inline auto fit(const ParsedText& p, DecimalType target)
    -> std::expected<Int128, std::string> {
    Int128 units = p.units;
    int scale = p.scale;
    // Negative scale (a positive exponent): multiply up to scale 0 first.
    if (scale < 0) {
        if (-scale > kMaxPrecision || !checked_mul(units, pow10(-scale), units)) {
            return std::unexpected("decimal overflow");
        }
        scale = 0;
    }
    // Dropping more than 38 digits leaves |units| < 10^38 < 10^k / 2, which
    // rounds to zero; `pow10` cannot express the divisor, so say so directly.
    // Rounding digit by digit instead would round twice.
    if (scale - target.scale > kMaxPrecision) {
        return Int128{0};
    }
    Int128 out = 0;
    if (!rescale(units, scale, target.scale, out) || !fits(out, target.precision)) {
        return std::unexpected("decimal overflow: value does not fit Decimal(" +
                               std::to_string(target.precision) + ", " +
                               std::to_string(target.scale) + ")");
    }
    return out;
}

[[nodiscard]] inline auto parse(std::string_view text, DecimalType target)
    -> std::expected<Int128, std::string> {
    auto parsed = parse_text(text);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    return fit(*parsed, target);
}

/// A literal's own type: scale = its fractional digits, precision = its
/// significant integral digits plus the scale (at least 1). `decimal"12.30"`
/// is `Decimal(4, 2)`, `decimal"0.05"` is `Decimal(2, 2)`.
[[nodiscard]] inline auto parse_literal(std::string_view text)
    -> std::expected<DecimalValue, std::string> {
    auto parsed = parse_text(text);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    if (parsed->scale > kMaxPrecision) {
        return std::unexpected("decimal literal '" + std::string(text) +
                               "' needs a scale greater than 38");
    }
    const int scale = std::max(parsed->scale, 0);
    const int precision = std::max(1, parsed->integral_digits + scale);
    if (precision > kMaxPrecision) {
        return std::unexpected("decimal literal '" + std::string(text) +
                               "' needs more than 38 digits");
    }
    const DecimalType type{.precision = static_cast<std::uint8_t>(precision),
                           .scale = static_cast<std::uint8_t>(scale)};
    auto units = fit(*parsed, type);
    if (!units) {
        return std::unexpected(units.error());
    }
    return DecimalValue{.units = *units, .type = type};
}

// ── Numeric conversions ─────────────────────────────────────────────────────

/// Nearest double to `units·10^-scale`, correctly rounded. The common case
/// (|units| < 2^53, scale <= 22) is a quotient of two exactly representable
/// doubles, which IEEE division rounds once; the rest goes through text,
/// which `from_chars` also rounds once.
[[nodiscard]] inline auto to_double(Int128 units, int scale) -> double {
    constexpr auto kExact = Int128{9'007'199'254'740'992};  // 2^53
    if (magnitude(units) < kExact && scale <= 22) {
        constexpr std::array<double, 23> kPow10d{1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,
                                                 1e8,  1e9,  1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
                                                 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
        const auto mantissa = static_cast<double>(static_cast<std::int64_t>(units));
        return mantissa / kPow10d[static_cast<std::size_t>(scale)];
    }
    const std::string text = to_string(units, scale);
    double out = 0.0;
    // strtod, not from_chars: Apple's libc++ (Xcode 15) has no floating-point
    // from_chars. `text` is our own NUL-terminated, locale-neutral digits.
    out = std::strtod(text.c_str(), nullptr);
    return out;
}

/// Convert `units·10^-scale / divisor` (divisor > 0) through decimal long
/// division. The quotient is not a stored Decimal: its fractional digits may
/// extend beyond scale 38. Keep 128 fractional digits before applying the
/// scale, enough to distinguish double rounding boundaries throughout the
/// supported decimal/int64 range. Remainders stay below divisor, so multiplying
/// them by ten fits int128. Conversion happens once, in the classic locale.
[[nodiscard]] inline auto divide_to_double(Int128 units, int scale, std::int64_t divisor)
    -> double {
    const auto d = Int128{divisor};
    const Int128 n = magnitude(units);
    std::string text = units < 0 ? "-" : "";
    text += to_string(n / d, 0);
    text += '.';
    Int128 remainder = n % d;
    for (int i = 0; i < 128 && remainder != 0; ++i) {
        remainder *= 10;
        text += static_cast<char>('0' + static_cast<int>(remainder / d));
        remainder %= d;
    }
    text += "e-" + std::to_string(scale);
    std::istringstream input(text);
    input.imbue(std::locale::classic());
    double result = 0.0;
    input >> result;
    return result;
}

/// A double fitted to `target` through its shortest round-trip text, so
/// `Decimal(0.1, 10, 2)` is `0.10` rather than the binary expansion of 0.1.
[[nodiscard]] inline auto from_double(double value, DecimalType target)
    -> std::expected<Int128, std::string> {
    // std::isfinite, not `x == 1.0 / 0.0`: MSVC rejects a constant division by
    // zero outright (C2124).
    if (!std::isfinite(value)) {
        return std::unexpected("cannot convert NaN or infinity to Decimal");
    }
    std::array<char, 64> buf{};
    const auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    if (ec != std::errc{}) {
        return std::unexpected("cannot convert double to Decimal");
    }
    return parse(std::string_view(buf.data(), static_cast<std::size_t>(ptr - buf.data())), target);
}

/// The exact decimal a double *literal* denotes, typed by its own digits.
[[nodiscard]] inline auto literal_from_double(double value)
    -> std::expected<DecimalValue, std::string> {
    if (!std::isfinite(value)) {
        return std::unexpected("cannot convert NaN or infinity to Decimal");
    }
    std::array<char, 64> buf{};
    const auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    if (ec != std::errc{}) {
        return std::unexpected("cannot convert double to Decimal");
    }
    return parse_literal(std::string_view(buf.data(), static_cast<std::size_t>(ptr - buf.data())));
}

[[nodiscard]] inline auto from_int64(std::int64_t value, DecimalType target)
    -> std::expected<Int128, std::string> {
    Int128 out = 0;
    if (!rescale(Int128{value}, 0, target.scale, out) || !fits(out, target.precision)) {
        return std::unexpected("decimal overflow: " + std::to_string(value) +
                               " does not fit Decimal(" + std::to_string(target.precision) + ", " +
                               std::to_string(target.scale) + ")");
    }
    return out;
}

/// Whole-valued decimals only, matching the Float→Int cast rule.
[[nodiscard]] inline auto to_int64(Int128 units, int scale)
    -> std::expected<std::int64_t, std::string> {
    const Int128 divisor = pow10(scale);
    if (units % divisor != 0) {
        return std::unexpected("Int64: decimal " + to_string(units, scale) +
                               " is not a whole number");
    }
    const Int128 whole = units / divisor;
    if (whole > Int128{INT64_MAX} || whole < Int128{INT64_MIN}) {
        return std::unexpected("Int64: decimal " + to_string(units, scale) +
                               " is out of Int64 range");
    }
    return static_cast<std::int64_t>(whole);
}

/// A `DecimalValue` from text known to be valid for `Decimal(p, s)`: the form
/// generated C++ uses to spell a decimal constant exactly. Throws on text that
/// does not fit, which in generated code means the emitter was wrong.
[[nodiscard]] inline auto make_value(std::string_view text, int precision, int scale)
    -> DecimalValue {
    const DecimalType type{.precision = static_cast<std::uint8_t>(precision),
                           .scale = static_cast<std::uint8_t>(scale)};
    auto units = parse(text, type);
    if (!units) {
        throw std::invalid_argument(units.error());
    }
    return DecimalValue{.units = *units, .type = type};
}

[[nodiscard]] inline auto type_name(DecimalType t) -> std::string {
    return "Decimal(" + std::to_string(t.precision) + ", " + std::to_string(t.scale) + ")";
}

[[nodiscard]] constexpr auto hash_units(Int128 units) noexcept -> std::size_t {
    // The high half by division rather than a shift of a signed value.
    constexpr Int128 kTwo64 = Int128{4'294'967'296} * 4'294'967'296;
    const auto lo = static_cast<std::uint64_t>(units);
    const auto hi = static_cast<std::uint64_t>(units / kTwo64);
    // splitmix64-style finalizer over the two halves.
    std::uint64_t h = lo ^ (hi * 0x9E3779B97F4A7C15ULL);
    h ^= h >> 30U;
    h *= 0xBF58476D1CE4E5B9ULL;
    h ^= h >> 27U;
    h *= 0x94D049BB133111EBULL;
    h ^= h >> 31U;
    return static_cast<std::size_t>(h);
}

}  // namespace decimal
}  // namespace ibex

template <>
struct std::hash<ibex::Decimal> {
    auto operator()(const ibex::Decimal& d) const noexcept -> std::size_t {
        return ibex::decimal::hash_units(d.units);
    }
};

/// Consistent with `DecimalValue`'s structural `==`: the type participates.
template <>
struct std::hash<ibex::DecimalValue> {
    auto operator()(const ibex::DecimalValue& d) const noexcept -> std::size_t {
        return ibex::decimal::hash_units(d.units) ^
               ((static_cast<std::size_t>(d.type.precision) << 8U) | d.type.scale);
    }
};
