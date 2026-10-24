// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// expr.cpp — expression evaluation: the row-wise scalar-builtin registry
// (single source of truth for infer + eval), type inference, per-field column
// evaluation (calls over columns), lag/lead/cum/fill/float-clean transforms,
// and the RNG/rep column generators.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/enums.hpp>
#include <ibex/core/time.hpp>
#include <ibex/core/time_zone.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/like.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/rng.hpp>
#include <ibex/runtime/safe_arith.hpp>
#include <ibex/runtime/table_format.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "decimal_ops.hpp"
#include "interpreter_internal.hpp"
#include "numeric_math.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

constexpr auto rng_func_returns_int(std::string_view name) -> bool;

}  // namespace

// ── Row-wise scalar builtins: one source of truth ───────────────────────────
//
// A pure row-wise scalar function is a function of its already-evaluated
// arguments — one value in each row maps to one value out. Both the
// type-inference pass (`infer_expr_type`, which picks the output column type)
// and the column evaluator (`eval_call_columns`, which applies the function to
// each row of its argument columns) dispatch these functions through this
// single table. Previously each pass had
// its own hand-maintained switch, so functions could drift out of sync — casts,
// ceil/floor/trunc, and round were usable at the REPL top level but missing
// from `update`/`select`. Registering once here keeps both passes in lockstep.
//
// Functions that need column-level context (fill_*, cum*, rolling_*, lag/lead,
// RNG, rep, externs) or that are not pure functions of their evaluated
// arguments (round's mode is a syntactic identifier; is_null is null-aware) are
// handled outside this table, in the call sites below.

// Parse an entire string as an Int64 / Float64. Leading and trailing ASCII
// whitespace is tolerated; anything else left over is a failure. Used by the
// String -> numeric casts (SPEC 3.1.1).
auto parse_string_as_int(std::string_view s) -> std::optional<std::int64_t> {
    while (!s.empty() && (std::isspace(static_cast<unsigned char>(s.front())) != 0)) {
        s.remove_prefix(1);
    }
    while (!s.empty() && (std::isspace(static_cast<unsigned char>(s.back())) != 0)) {
        s.remove_suffix(1);
    }
    if (s.empty()) {
        return std::nullopt;
    }
    std::int64_t out{};
    const auto* begin = s.data();
    if (!s.empty() && s.front() == '+') {
        begin += 1;  // std::from_chars rejects a leading '+'
    }
    const auto [ptr, ec] = std::from_chars(begin, s.data() + s.size(), out);
    if (ec != std::errc{} || ptr != s.data() + s.size()) {
        return std::nullopt;
    }
    return out;
}

auto parse_string_as_double(std::string_view s) -> std::optional<double> {
    const std::string owned(s);
    const char* begin = owned.c_str();
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(begin, &end);
    if (end == begin || errno == ERANGE) {
        return std::nullopt;
    }
    while (*end != '\0' && (std::isspace(static_cast<unsigned char>(*end)) != 0)) {
        ++end;
    }
    if (*end != '\0') {
        return std::nullopt;
    }
    return value;
}

auto expr_value_to_double(const ScalarValue& v) -> std::optional<double> {
    if (const auto* i = std::get_if<std::int64_t>(&v)) {
        return static_cast<double>(*i);
    }
    if (const auto* d = std::get_if<double>(&v)) {
        return *d;
    }
    if (const auto* dv = std::get_if<DecimalValue>(&v)) {
        return decimal::to_double(dv->units, dv->type.scale);
    }
    if (const auto* s = std::get_if<std::string>(&v)) {
        return parse_string_as_double(*s);
    }
    return std::nullopt;
}

// Render a scalar value as text for string interpolation (`__interp`). Mirrors
// the table/REPL display formatting (floats via format_float_mixed, dates/
// timestamps via their formatters) so an interpolated value reads the same as a
// printed one.
auto expr_value_to_string(const ScalarValue& v) -> std::string {
    if (const auto* s = std::get_if<std::string>(&v)) {
        return *s;
    }
    if (const auto* i = std::get_if<std::int64_t>(&v)) {
        return std::to_string(*i);
    }
    if (const auto* d = std::get_if<double>(&v)) {
        return format_float_mixed(*d);
    }
    if (const auto* b = std::get_if<bool>(&v)) {
        return *b ? "true" : "false";
    }
    if (const auto* dt = std::get_if<Date>(&v)) {
        return format_date(*dt);
    }
    if (const auto* ts = std::get_if<Timestamp>(&v)) {
        return format_timestamp(*ts);
    }
    if (const auto* dv = std::get_if<DecimalValue>(&v)) {
        return decimal::to_string(*dv);
    }
    return "null";  // Null — matches the table display of a missing cell
}

namespace {

// Extract a calendar/clock field from a Date or Timestamp value.
auto eval_date_part(std::string_view name, const ScalarValue& v)
    -> std::expected<ScalarValue, std::string> {
    using namespace std::chrono;
    const bool date_ok = (name == "year" || name == "month" || name == "day");
    if (const auto* dv = std::get_if<Date>(&v)) {
        if (!date_ok) {
            return std::unexpected(std::string(name) + ": argument must be Timestamp");
        }
        const year_month_day ymd{sys_days{days{dv->days}}};
        if (name == "year") {
            return ScalarValue{static_cast<std::int64_t>(static_cast<int>(ymd.year()))};
        }
        if (name == "month") {
            return ScalarValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.month()))};
        }
        return ScalarValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.day()))};
    }
    if (const auto* tv = std::get_if<Timestamp>(&v)) {
        const sys_time<nanoseconds> tp{nanoseconds{tv->nanos}};
        auto day_pt = floor<days>(tp);
        const year_month_day ymd{day_pt};
        const hh_mm_ss<nanoseconds> hms{tp - day_pt};
        if (name == "year") {
            return ScalarValue{static_cast<std::int64_t>(static_cast<int>(ymd.year()))};
        }
        if (name == "month") {
            return ScalarValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.month()))};
        }
        if (name == "day") {
            return ScalarValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.day()))};
        }
        if (name == "hour") {
            return ScalarValue{static_cast<std::int64_t>(hms.hours().count())};
        }
        if (name == "minute") {
            return ScalarValue{static_cast<std::int64_t>(hms.minutes().count())};
        }
        return ScalarValue{static_cast<std::int64_t>(hms.seconds().count())};
    }
    return std::unexpected(std::string(name) + ": argument must be Date or Timestamp");
}

// ── Scalar whole-column kernels ──────────────────────────────────────────────
// Fast paths for the three null-handling scalars, resolved from the one-byte
// ScalarKernel id on their registry entries (see scalar_kernel_fn). Each must
// agree with its entry's per-row eval — the eval is the semantic reference.

auto fill_null_kernel(const ir::CallExpr& call, const Table& input, std::size_t /*rows*/,
                      const ColumnEvalCtx& /*ctx*/) -> std::expected<ComputedColumn, std::string> {
    auto res = eval_fill_null(call, input);
    if (!res) {
        return std::unexpected(res.error());
    }
    return ComputedColumn{.column = std::move(res->column), .validity = std::move(res->validity)};
}

auto float_clean_kernel(const ir::CallExpr& call, const Table& input, std::size_t /*rows*/,
                        const ColumnEvalCtx& /*ctx*/)
    -> std::expected<ComputedColumn, std::string> {
    auto res = eval_float_clean(
        call, input,
        call.callee == "null_if_nan" ? FloatCleanMode::NullIfNan : FloatCleanMode::NullIfNotFinite);
    if (!res) {
        return std::unexpected(res.error());
    }
    return ComputedColumn{.column = std::move(res->column), .validity = std::move(res->validity)};
}

auto coalesce_kernel(const ir::CallExpr& call, const Table& input, std::size_t rows,
                     const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string> {
    return eval_coalesce_column(call, input, ctx.scalars, RowRange::whole(rows));
}

// One `like` argument in kernel shape: a String literal / lexical String scalar,
// a dense String column, or a Categorical column. The string_view borrows the
// literal or the registry entry, both of which outlive the call.
struct LikeArg {
    const Column<std::string>* dense = nullptr;
    const Column<Categorical>* categorical = nullptr;
    const ValidityBitmap* validity = nullptr;
    std::string_view scalar;  // meaningful only when neither column is set
};

/// The arguments common to the two zone casts, already validated and resolved.
struct ZoneCallArgs {
#ifdef IBEX_HAS_STD_CHRONO_TIME_ZONES
    const std::chrono::time_zone* zone = nullptr;
#endif
    const std::string* zone_text = nullptr;  // borrowed from the literal in `call`
    const ColumnEntry* entry = nullptr;
    const Column<Timestamp>* stamps = nullptr;
};

/// Both zone casts take the same shape -- `f(timestamp_column, "Zone")` -- and
/// differ only in what they do once the pieces are in hand, so the checking and
/// the error wording live here once, parameterised by the caller's name.
auto resolve_zone_call(std::string_view fn, const ir::CallExpr& call, const Table& input)
    -> std::expected<ZoneCallArgs, std::string> {
    const std::string prefix(fn);
    if (call.args.size() != 2) {
        return std::unexpected(prefix + ": expected 2 arguments (column, zone)");
    }
    const auto* lit = std::get_if<ir::Literal>(&call.args[1]->node);
    const auto* zone_text = lit != nullptr ? std::get_if<std::string>(&lit->value) : nullptr;
    if (zone_text == nullptr) {
        return std::unexpected(prefix + ": the zone must be a String literal");
    }

    if (!is_known_zone(*zone_text)) {
        return std::unexpected(prefix + ": unknown time zone '" + *zone_text + "'");
    }

    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (col_ref == nullptr) {
        return std::unexpected(prefix + ": first argument must be a column name");
    }
    const auto* entry = input.find_entry(col_ref->name);
    if (entry == nullptr) {
        return std::unexpected(prefix + ": unknown column '" + col_ref->name + "'");
    }
    const auto* stamps = std::get_if<Column<Timestamp>>(entry->column.get());
    if (stamps == nullptr) {
        return std::unexpected(prefix + ": '" + col_ref->name + "' is not a Timestamp column");
    }
    ZoneCallArgs result{.zone_text = zone_text, .entry = entry, .stamps = stamps};
#ifdef IBEX_HAS_STD_CHRONO_TIME_ZONES
    result.zone = std::chrono::locate_zone(*zone_text);
#endif
    return result;
}

/// `in_timezone(ts, "Zone")` -- tag a Timestamp column with `Zone`, leaving every
/// instant exactly as it was.
///
/// The sibling of `with_timezone`, and the two are worth keeping straight because
/// they are opposites:
///
///   * `with_timezone` says the VALUES are wrong -- they are wall clocks that
///     were mistaken for instants -- and shifts them.
///   * `in_timezone` says the values are already right and only the RENDERING
///     was missing, so it changes nothing but the label.
///
/// The label is not merely cosmetic: `resample` cuts on local boundaries once a
/// zone is present, so relabelling a correct UTC column moves its day boundaries
/// onto the zone's calendar without moving any of its data.
auto eval_in_timezone(const ir::CallExpr& call, const Table& input)
    -> std::expected<ComputedColumn, std::string> {
    auto args = resolve_zone_call("in_timezone", call, input);
    if (!args) {
        return std::unexpected(args.error());
    }
    Column<Timestamp> out = *args->stamps;
    out.set_meta(ColumnMeta{.zone = intern_zone(*args->zone_text), .decimal = std::nullopt});
    return ComputedColumn{.column = ColumnValue{std::move(out)}, .validity = args->entry->validity};
}

/// `with_timezone(ts, "Zone")` -- reinterpret a Timestamp column's values as
/// wall-clock readings in `Zone`, convert them to the instants they denote, and
/// tag the column with the zone.
///
/// This is the cast for "these timestamps were never UTC". Ibex stores an
/// instant (SPEC 2.4), so a column loaded from a source that recorded local
/// wall clocks holds the right NUMBERS interpreted the wrong way; this shifts
/// them by the zone's offset AT THAT LOCAL TIME, which is what makes it correct
/// across a DST boundary where a fixed offset is not.
///
/// The two irregular local times are handled rather than rejected wholesale:
///   * Ambiguous (the hour repeated when clocks go back) resolves to the
///     earlier instant, which is the conventional reading of a bare wall clock.
///   * Nonexistent (the hour skipped when clocks go forward) yields NULL. That
///     wall clock never happened, so there is no instant to name -- and failing
///     the whole query over one bad row would be worse than saying so per row.
auto eval_with_timezone(const ir::CallExpr& call, const Table& input)
    -> std::expected<ComputedColumn, std::string> {
    auto args = resolve_zone_call("with_timezone", call, input);
    if (!args) {
        return std::unexpected(args.error());
    }
    const std::string* zone_text = args->zone_text;
    const auto* entry = args->entry;
    const auto* stamps = args->stamps;

    Column<Timestamp> out;
    out.reserve(stamps->size());
    std::optional<ValidityBitmap> validity = entry->validity;

    // `to_sys` with a `choose` resolves a nonexistent local time instead of
    // reporting it, so the classification comes from `get_info` directly. It
    // also avoids exception machinery on the hot path.
    //
    // The offset only changes at a transition -- twice a year for most zones --
    // so the last answer is cached and reused while the value still falls inside
    // the local interval it covers. Timestamp columns are usually sorted, which
    // makes that nearly every row.
#ifdef IBEX_HAS_STD_CHRONO_TIME_ZONES
    const std::chrono::time_zone* zone = args->zone;
    std::chrono::nanoseconds cached_offset{};
    std::chrono::local_time<std::chrono::nanoseconds> cache_begin{};
    std::chrono::local_time<std::chrono::nanoseconds> cache_end{};
    bool cache_valid = false;
#endif

    for (std::size_t row = 0; row < stamps->size(); ++row) {
#ifdef IBEX_HAS_STD_CHRONO_TIME_ZONES

        const std::chrono::local_time<std::chrono::nanoseconds> wall{
            std::chrono::nanoseconds{(*stamps)[row].nanos}};

        if (cache_valid && wall >= cache_begin && wall < cache_end) {
            out.push_back(Timestamp{(wall.time_since_epoch() - cached_offset).count()});
            continue;
        }

        const std::chrono::local_info info = zone->get_info(wall);
        if (info.result == std::chrono::local_info::nonexistent) {
            // That wall clock never happened, so there is no instant it names.
            if (!validity.has_value()) {
                validity = ValidityBitmap(stamps->size(), true);
            }
            validity->set(row, false);
            out.push_back(Timestamp{0});
            cache_valid = false;
            continue;
        }

        // `first` is the interval in effect before any transition at this local
        // time, so for an ambiguous reading it is the earlier of the two
        // instants -- the conventional reading of a bare wall clock.
        const auto offset = std::chrono::duration_cast<std::chrono::nanoseconds>(info.first.offset);
        out.push_back(Timestamp{(wall.time_since_epoch() - offset).count()});

        if (info.result == std::chrono::local_info::unique) {
            cached_offset = offset;
            cache_begin = std::chrono::local_time<std::chrono::nanoseconds>{
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    info.first.begin.time_since_epoch()) +
                offset};
            cache_end = std::chrono::local_time<std::chrono::nanoseconds>{
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    info.first.end.time_since_epoch()) +
                offset};
            cache_valid = true;
        } else {
            cache_valid = false;
        }
#else
        const auto instant = local_time_to_sys(*zone_text, (*stamps)[row].nanos);
        if (!instant.has_value()) {
            if (!validity.has_value()) {
                validity = ValidityBitmap(stamps->size(), true);
            }
            validity->set(row, false);
            out.push_back(Timestamp{0});
        } else {
            out.push_back(Timestamp{*instant});
        }
#endif
    }
    out.set_meta(ColumnMeta{.zone = intern_zone(*zone_text), .decimal = std::nullopt});

    return ComputedColumn{.column = ColumnValue{std::move(out)}, .validity = std::move(validity)};
}

auto resolve_like_arg(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::expected<LikeArg, std::string> {
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        const auto* text = std::get_if<std::string>(&lit->value);
        if (text == nullptr) {
            return std::unexpected("like: arguments must be String");
        }
        return LikeArg{.scalar = *text};
    }
    const auto* ref = std::get_if<ir::ColumnRef>(&expr.node);
    if (ref == nullptr) {
        return std::unexpected("like: arguments must be columns, string literals, or scalars");
    }
    if (const auto* entry = ref->lexical ? nullptr : input.find_entry(ref->name);
        entry != nullptr) {
        LikeArg arg;
        arg.validity = entry->validity.has_value() ? &*entry->validity : nullptr;
        if (const auto* dense = std::get_if<Column<std::string>>(entry->column.get())) {
            arg.dense = dense;
            return arg;
        }
        if (const auto* cat = std::get_if<Column<Categorical>>(entry->column.get())) {
            arg.categorical = cat;
            return arg;
        }
        return std::unexpected("like: column '" + ref->name + "' is not a String column");
    }
    if (scalars != nullptr) {
        if (auto it = scalars->find(ref->name); it != scalars->end()) {
            const auto* text = std::get_if<std::string>(&it->second);
            if (text == nullptr) {
                return std::unexpected("like: arguments must be String");
            }
            return LikeArg{.scalar = *text};
        }
    }
    if (ref->lexical) {
        return std::unexpected("like: '^" + ref->name + "' does not resolve to a lexical binding");
    }
    return std::unexpected("like: unknown column '" + ref->name + "'");
}

// like(value, pattern) over whole columns: compile the pattern once, then scan.
// The per-row registry eval is the semantic reference; this only avoids
// re-compiling the pattern and copying every value into an ScalarValue.
auto like_kernel(const ir::CallExpr& call, const Table& input, std::size_t rows,
                 const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string> {
    if (call.args.size() != 2) {
        return std::unexpected("like: expected 2 arguments (value, pattern)");
    }
    auto value = resolve_like_arg(*call.args[0], input, ctx.scalars);
    if (!value) {
        return std::unexpected(value.error());
    }
    auto pattern = resolve_like_arg(*call.args[1], input, ctx.scalars);
    if (!pattern) {
        return std::unexpected(pattern.error());
    }

    Column<bool> out;
    out.resize(rows);
    const bool scalar_pattern = pattern->dense == nullptr && pattern->categorical == nullptr;

    if (scalar_pattern) {
        auto compiled = compile_like_pattern(pattern->scalar);
        if (!compiled) {
            return std::unexpected(compiled.error());
        }
        if (value->categorical != nullptr) {
            // The dictionary is the work: match each distinct value once, then
            // map the answer through the codes.
            const auto& dict = value->categorical->dictionary();
            std::vector<bool> dict_match(dict.size(), false);
            for (std::size_t d = 0; d < dict.size(); ++d) {
                dict_match[d] = like_match(*compiled, dict[d]);
            }
            const auto* codes = value->categorical->codes_data();
            for (std::size_t i = 0; i < rows; ++i) {
                out.set(i, dict_match[static_cast<std::size_t>(codes[i])]);
            }
        } else if (value->dense != nullptr) {
            for (std::size_t i = 0; i < rows; ++i) {
                out.set(i, like_match(*compiled, (*value->dense)[i]));
            }
        } else {
            const bool matched = like_match(*compiled, value->scalar);
            out.resize(rows, matched);
        }
    } else {
        // Pattern column: compile per row, caching the last one — dynamic
        // patterns in practice repeat (a joined-in pattern column, a grouped
        // key), and a cache miss only costs the compile it would have done.
        const auto value_at = [&value](std::size_t i) -> std::string_view {
            if (value->dense != nullptr) {
                return (*value->dense)[i];
            }
            if (value->categorical != nullptr) {
                return (*value->categorical)[i];
            }
            return value->scalar;
        };
        std::string_view cached_text;
        LikePattern cached;
        bool have_cached = false;
        for (std::size_t i = 0; i < rows; ++i) {
            const std::string_view pattern_text =
                pattern->dense != nullptr ? (*pattern->dense)[i] : (*pattern->categorical)[i];
            if (!have_cached || pattern_text != cached_text) {
                auto compiled = compile_like_pattern(pattern_text);
                if (!compiled) {
                    return std::unexpected(compiled.error());
                }
                cached = std::move(*compiled);
                cached_text = pattern_text;
                have_cached = true;
            }
            out.set(i, like_match(cached, value_at(i)));
        }
    }

    return ComputedColumn{
        .column = ColumnValue{std::move(out)},
        .validity = merge_validity(value->validity, 0, pattern->validity, 0, rows)};
}

// Int64/Int32/Int and Float64/Float32 over a bare column, literal, or lexical
// scalar. The per-row eval boxes every cell into an ScalarValue and re-runs the
// registry dispatch per row; the cast is an identity or widening loop. This is
// the path `count(col)` takes — it lowers to `sum(Int64(col is not null))`,
// whose bool column lands here after the boolean arm is materialized.
// Semantics mirror the per-row eval exactly: Bool -> 0/1 for Int, non-integer
// Float -> Int errors, null cells propagate untouched (their payload is never
// read, so a garbage NaN in a null slot cannot fail the integrality check).
// Broadcast a scalar cast result into a full column. A null (NullScalar) scalar
// becomes an all-null column of the cast's target type -- `Int64(null)` is null,
// not an error.
auto broadcast_cast_result(const ScalarValue& value, bool to_int, std::size_t rows)
    -> ComputedColumn {
    if (is_null_scalar(value)) {
        ValidityBitmap none(rows, false);
        if (to_int) {
            Column<std::int64_t> col;
            col.resize(rows, 0);
            return ComputedColumn{.column = ColumnValue{std::move(col)},
                                  .validity = std::move(none)};
        }
        Column<double> col;
        col.resize(rows, 0.0);
        return ComputedColumn{.column = ColumnValue{std::move(col)}, .validity = std::move(none)};
    }
    return ComputedColumn{.column = broadcast_scalar_column(value, rows), .validity = std::nullopt};
}

auto numeric_cast_kernel(const ir::CallExpr& call, const Table& input, std::size_t rows,
                         const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected(call.callee + "(): expected one argument");
    }
    const bool to_int = call.callee.front() == 'I';  // Int64/Int32/Int vs Float64/Float32

    const ir::Expr& arg = *call.args[0];
    const ColumnEntry* entry = nullptr;
    if (const auto* ref = std::get_if<ir::ColumnRef>(&arg.node)) {
        entry = ref->lexical ? nullptr : input.find_entry(ref->name);
        if (entry == nullptr) {
            // A lexical scalar: convert once through the per-row eval (the
            // semantic reference), then broadcast.
            if (ctx.scalars != nullptr) {
                if (auto it = ctx.scalars->find(ref->name); it != ctx.scalars->end()) {
                    // The raw eval skips the registry's null propagation, so a
                    // null binding is handled here: a cast of null is null.
                    if (is_null_scalar(it->second)) {
                        return broadcast_cast_result(it->second, to_int, rows);
                    }
                    const auto& eval = std::get<ScalarExec>(find_builtin(call.callee)->exec).eval;
                    auto value = eval(call.callee, {it->second});
                    if (!value) {
                        return std::unexpected(value.error());
                    }
                    // A null result is a null of the cast's type.
                    return broadcast_cast_result(
                        is_null_scalar(*value)
                            ? ScalarValue{NullScalar{to_int ? ScalarKind::Int : ScalarKind::Double}}
                            : *value,
                        to_int, rows);
                }
            }
            return std::unexpected(call.callee + "(): unknown column '" + ref->name + "'");
        }
    } else {
        // Literal argument: convert once, broadcast.
        const auto& lit = std::get<ir::Literal>(arg.node);
        const auto& eval = std::get<ScalarExec>(find_builtin(call.callee)->exec).eval;
        auto value = eval(call.callee, {scalar_from_literal(lit)});
        if (!value) {
            return std::unexpected(value.error());
        }
        return broadcast_cast_result(
            is_null_scalar(*value)
                ? ScalarValue{NullScalar{to_int ? ScalarKind::Int : ScalarKind::Double}}
                : *value,
            to_int, rows);
    }

    const ValidityBitmap* validity = entry->validity.has_value() ? &*entry->validity : nullptr;
    std::optional<ValidityBitmap> out_validity;
    if (validity != nullptr) {
        out_validity = *validity;
    }
    const ColumnValue& col = *entry->column;

    if (to_int) {
        if (const auto* ints = std::get_if<Column<std::int64_t>>(&col)) {
            return ComputedColumn{.column = *ints, .validity = std::move(out_validity)};
        }
        if (const auto* bools = std::get_if<Column<bool>>(&col)) {
            Column<std::int64_t> out;
            out.resize(rows);
            auto* op = out.data();
            for (std::size_t i = 0; i < rows; ++i) {
                op[i] = (*bools)[i] ? 1 : 0;
            }
            return ComputedColumn{.column = ColumnValue{std::move(out)},
                                  .validity = std::move(out_validity)};
        }
        if (const auto* dbls = std::get_if<Column<double>>(&col)) {
            Column<std::int64_t> out;
            out.resize(rows);
            auto* op = out.data();
            const auto* dp = dbls->data();
            for (std::size_t i = 0; i < rows; ++i) {
                if (validity != nullptr && !(*validity)[i]) {
                    op[i] = 0;  // null propagates via the bitmap; payload unread
                    continue;
                }
                if (dp[i] != std::trunc(dp[i])) {
                    return std::unexpected(call.callee +
                                           "(): cannot cast non-integer Float to Int (use "
                                           "floor(), ceil(), or round())");
                }
                op[i] = static_cast<std::int64_t>(dp[i]);
            }
            return ComputedColumn{.column = ColumnValue{std::move(out)},
                                  .validity = std::move(out_validity)};
        }
        if (const auto* strs = std::get_if<Column<std::string>>(&col)) {
            Column<std::int64_t> out;
            out.resize(rows);
            auto* op = out.data();
            for (std::size_t i = 0; i < rows; ++i) {
                if (validity != nullptr && !(*validity)[i]) {
                    op[i] = 0;
                    continue;
                }
                auto parsed = parse_string_as_int((*strs)[i]);
                if (!parsed) {
                    return std::unexpected(call.callee + "(): cannot parse '" +
                                           std::string((*strs)[i]) + "' as an integer");
                }
                op[i] = *parsed;
            }
            return ComputedColumn{.column = ColumnValue{std::move(out)},
                                  .validity = std::move(out_validity)};
        }
        if (const auto* decs = std::get_if<Column<Decimal>>(&col)) {
            const int scale = decimal_type_of(*decs).scale;
            Column<std::int64_t> out;
            out.resize(rows);
            auto* op = out.data();
            for (std::size_t i = 0; i < rows; ++i) {
                if (validity != nullptr && !(*validity)[i]) {
                    op[i] = 0;
                    continue;
                }
                auto whole = decimal::to_int64((*decs)[i].units, scale);
                if (!whole) {
                    return std::unexpected(call.callee + "(): " + whole.error() +
                                           " (use Decimal(x, p, 0) to round first)");
                }
                op[i] = *whole;
            }
            return ComputedColumn{.column = ColumnValue{std::move(out)},
                                  .validity = std::move(out_validity)};
        }
        return std::unexpected(call.callee + "(): cannot cast non-numeric to Int");
    }

    if (const auto* decs = std::get_if<Column<Decimal>>(&col)) {
        const int scale = decimal_type_of(*decs).scale;
        Column<double> out;
        out.resize(rows);
        auto* op = out.data();
        for (std::size_t i = 0; i < rows; ++i) {
            op[i] = decimal::to_double((*decs)[i].units, scale);
        }
        return ComputedColumn{.column = ColumnValue{std::move(out)},
                              .validity = std::move(out_validity)};
    }
    if (const auto* dbls = std::get_if<Column<double>>(&col)) {
        return ComputedColumn{.column = *dbls, .validity = std::move(out_validity)};
    }
    if (const auto* ints = std::get_if<Column<std::int64_t>>(&col)) {
        Column<double> out;
        out.resize(rows);
        auto* op = out.data();
        const auto* ip = ints->data();
        for (std::size_t i = 0; i < rows; ++i) {
            op[i] = static_cast<double>(ip[i]);
        }
        return ComputedColumn{.column = ColumnValue{std::move(out)},
                              .validity = std::move(out_validity)};
    }
    if (const auto* strs = std::get_if<Column<std::string>>(&col)) {
        Column<double> out;
        out.resize(rows);
        auto* op = out.data();
        for (std::size_t i = 0; i < rows; ++i) {
            if (validity != nullptr && !(*validity)[i]) {
                op[i] = 0.0;
                continue;
            }
            auto parsed = parse_string_as_double((*strs)[i]);
            if (!parsed) {
                return std::unexpected(call.callee + "(): cannot parse '" +
                                       std::string((*strs)[i]) + "' as a number");
            }
            op[i] = *parsed;
        }
        return ComputedColumn{.column = ColumnValue{std::move(out)},
                              .validity = std::move(out_validity)};
    }
    return std::unexpected(call.callee + "(): cannot cast non-numeric to Float");
}

auto string_length_kernel(const ir::CallExpr& call, const Table& input, std::size_t rows,
                          const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string>;

}  // namespace

auto scalar_kernel_fn(ScalarKernel kernel) -> ColumnEvalFn {
    switch (kernel) {
        case ScalarKernel::None:
            return nullptr;
        case ScalarKernel::FillNull:
            return &fill_null_kernel;
        case ScalarKernel::FloatClean:
            return &float_clean_kernel;
        case ScalarKernel::Coalesce:
            return &coalesce_kernel;
        case ScalarKernel::Like:
            return &like_kernel;
        case ScalarKernel::StringLength:
            return &string_length_kernel;
        case ScalarKernel::NumericCast:
            return &numeric_cast_kernel;
    }
    return nullptr;  // unreachable; MSVC C4715
}

namespace {

[[noreturn]] auto builtin_invariant(std::string_view name, std::string_view detail) -> void {
    invariant_violation("builtins(): invalid entry for '" + std::string(name) +
                        "': " + std::string(detail));
}

auto validate_builtin(std::string_view name, const BuiltinFn& fn) -> void {
    if (fn.min_args < 0 || fn.max_args < -1 || (fn.max_args >= 0 && fn.max_args < fn.min_args)) {
        builtin_invariant(name, "invalid arity range");
    }
    if (fn.infer == nullptr) {
        builtin_invariant(name, "missing type-inference function");
    }

    if (const auto* scalar = std::get_if<ScalarExec>(&fn.exec)) {
        if (scalar->eval == nullptr) {
            builtin_invariant(name, "ScalarExec is missing its row evaluator");
        }
        if (fn.scalar_kernel != ScalarKernel::None &&
            scalar_kernel_fn(fn.scalar_kernel) == nullptr) {
            builtin_invariant(name, "scalar kernel id has no evaluator");
        }
        return;
    }

    if (fn.null_policy != NullPolicy::Propagate) {
        builtin_invariant(name, "non-scalar entry has a scalar null policy");
    }
    if (fn.scalar_kernel != ScalarKernel::None) {
        builtin_invariant(name, "non-scalar entry has a scalar kernel id");
    }
    if (const auto* transform = std::get_if<TransformExec>(&fn.exec);
        transform != nullptr && transform->column_eval == nullptr) {
        builtin_invariant(name, "TransformExec is missing its column evaluator");
    }
    if (const auto* generator = std::get_if<GeneratorExec>(&fn.exec);
        generator != nullptr && generator->column_eval == nullptr) {
        builtin_invariant(name, "GeneratorExec is missing its column evaluator");
    }
    if (const auto* aggregate = std::get_if<AggregateExec>(&fn.exec);
        aggregate != nullptr &&
        static_cast<std::uint8_t>(aggregate->func) > static_cast<std::uint8_t>(ir::AggFunc::LastNull)) {
        builtin_invariant(name, "AggregateExec has an invalid aggregate function id");
    }
}

/// Byte offset where each UTF-8 codepoint starts, with a trailing sentinel
/// equal to the string length; so the codepoint count is `size() - 1` and
/// codepoint `i` spans `[out[i], out[i + 1])`. A malformed lead or truncated
/// sequence is treated as a single-byte codepoint, which keeps the walk in
/// bounds rather than reading past the end.
auto utf8_codepoint_advance(std::string_view s, std::size_t i) -> std::size_t {
    const auto lead = static_cast<unsigned char>(s[i]);
    std::size_t advance = 1;
    if (lead >= 0xF0) {
        advance = 4;
    } else if (lead >= 0xE0) {
        advance = 3;
    } else if (lead >= 0xC0) {
        advance = 2;
    }
    return std::min(advance, s.size() - i);
}

auto utf8_codepoint_offsets(std::string_view s) -> std::vector<std::size_t> {
    std::vector<std::size_t> offsets;
    offsets.reserve(s.size() + 1);
    std::size_t i = 0;
    while (i < s.size()) {
        offsets.push_back(i);
        i += utf8_codepoint_advance(s, i);
    }
    offsets.push_back(s.size());
    return offsets;
}

[[nodiscard]] auto utf8_codepoint_count(std::string_view s) -> std::int64_t {
    std::int64_t count = 0;
    for (std::size_t i = 0; i < s.size(); i += utf8_codepoint_advance(s, i)) {
        ++count;
    }
    return count;
}

/// `substring(s, start[, length])` on Unicode codepoints, Polars `str.slice`
/// semantics: `start` is 0-based and may be negative (counted from the end);
/// `length` is optional (to the end when omitted). Indices clamp into range
/// rather than erroring, so an out-of-range slice is empty, not a failure.
auto utf8_substring(const std::string& s, std::int64_t start, std::optional<std::int64_t> length)
    -> std::string {
    const std::vector<std::size_t> offsets = utf8_codepoint_offsets(s);
    const auto count = static_cast<std::int64_t>(offsets.size() - 1);

    std::int64_t begin = start < 0 ? start + count : start;
    begin = std::clamp<std::int64_t>(begin, 0, count);

    std::int64_t end = count;
    if (length.has_value()) {
        if (*length <= 0) {
            end = begin;
        } else if (*length < count - begin) {
            end = begin + *length;
        }
    }
    return s.substr(
        offsets[static_cast<std::size_t>(begin)],
        offsets[static_cast<std::size_t>(end)] - offsets[static_cast<std::size_t>(begin)]);
}

auto string_length_kernel(const ir::CallExpr& call, const Table& input, std::size_t rows,
                          const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected(call.callee + ": expected 1 String argument");
    }
    auto value = resolve_like_arg(*call.args[0], input, ctx.scalars);
    if (!value) {
        return std::unexpected(call.callee + ": " + value.error());
    }

    const bool bytes = call.callee == "byte_length";
    const auto measure = [bytes](std::string_view text) -> std::int64_t {
        return bytes ? static_cast<std::int64_t>(text.size()) : utf8_codepoint_count(text);
    };

    Column<std::int64_t> out;
    if (value->categorical != nullptr) {
        out.resize(rows);
        auto* values = out.data();
        const auto& dict = value->categorical->dictionary();
        std::vector<std::int64_t> dictionary_lengths(dict.size());
        for (std::size_t i = 0; i < dict.size(); ++i) {
            dictionary_lengths[i] = measure(dict[i]);
        }
        const auto* codes = value->categorical->codes_data();
        for (std::size_t i = 0; i < rows; ++i) {
            values[i] = dictionary_lengths[static_cast<std::size_t>(codes[i])];
        }
    } else if (value->dense != nullptr) {
        out.resize(rows);
        auto* values = out.data();
        for (std::size_t i = 0; i < rows; ++i) {
            values[i] = measure((*value->dense)[i]);
        }
    } else {
        out.resize(rows, measure(value->scalar));
    }

    std::optional<ValidityBitmap> validity;
    if (value->validity != nullptr) {
        validity = *value->validity;
    }
    return ComputedColumn{.column = ColumnValue{std::move(out)}, .validity = std::move(validity)};
}

}  // namespace

const robin_hood::unordered_map<std::string_view, BuiltinFn>& builtins() {
    using IT = std::expected<ExprType, std::string>;
    using IV = std::expected<ScalarValue, std::string>;
    static const robin_hood::unordered_map<std::string_view, BuiltinFn> table = [] {
        robin_hood::unordered_map<std::string_view, BuiltinFn> m;

        // abs: numeric -> same numeric type.
        m.emplace("abs",
                  BuiltinFn{
                      .min_args = 1,
                      .max_args = 1,
                      .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                          if (a[0] == ExprType::Int || a[0] == ExprType::Double ||
                              a[0] == ExprType::Decimal) {
                              return a[0];
                          }
                          return std::unexpected("abs: argument must be numeric");
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          if (const auto* i = std::get_if<std::int64_t>(a.data())) {
                              return ScalarValue{safe_iabs(*i)};
                          }
                          if (const auto* d = std::get_if<double>(a.data())) {
                              return ScalarValue{std::abs(*d)};
                          }
                          if (const auto* d = std::get_if<DecimalValue>(a.data())) {
                              return ScalarValue{DecimalValue{
                                  .units = d->units < 0 ? -d->units : d->units, .type = d->type}};
                          }
                          return std::unexpected("abs: argument must be numeric");
                      }},
                  });

        // __interp: string interpolation, generated by the parser for backtick
        // template strings (`a=${a} b=${b}`). Variadic; every argument is
        // stringified and concatenated, so literal segments (string args) carry
        // through verbatim and embedded values are formatted in place. Always
        // yields String.
        m.emplace("__interp",
                  BuiltinFn{
                      .min_args = 0,
                      .max_args = -1,
                      .infer = [](std::string_view, const std::vector<ExprType>&) -> IT {
                          return ExprType::String;
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          std::string out;
                          for (const auto& v : a) {
                              out += expr_value_to_string(v);
                          }
                          return ScalarValue{std::move(out)};
                      }},
                  });

        // sqrt / log / exp: numeric -> Float64.
        const BuiltinFn transcendental{
            .min_args = 1,
            .max_args = 1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return ExprType::Double;
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            },
            .exec = ScalarExec{.eval = [](std::string_view name,
                                          const std::vector<ScalarValue>& a) -> IV {
                auto x = expr_value_to_double(a[0]);
                if (!x) {
                    return std::unexpected(std::string(name) + ": argument must be numeric");
                }
                const double v = *x;
                if (name == "sqrt")
                    return ScalarValue{std::sqrt(v)};
                if (name == "log")
                    return ScalarValue{std::log(v)};
                if (name == "log2")
                    return ScalarValue{std::log2(v)};
                if (name == "log10")
                    return ScalarValue{std::log10(v)};
                if (name == "exp")
                    return ScalarValue{std::exp(v)};
                if (name == "sin")
                    return ScalarValue{std::sin(v)};
                if (name == "cos")
                    return ScalarValue{std::cos(v)};
                if (name == "tan")
                    return ScalarValue{std::tan(v)};
                if (name == "asin")
                    return ScalarValue{std::asin(v)};
                if (name == "acos")
                    return ScalarValue{std::acos(v)};
                if (name == "atan")
                    return ScalarValue{std::atan(v)};
                if (name == "sinh")
                    return ScalarValue{std::sinh(v)};
                if (name == "cosh")
                    return ScalarValue{std::cosh(v)};
                return ScalarValue{std::tanh(v)};  // tanh
            }},
        };
        for (const auto* fn : {
                 "sqrt",
                 "log",
                 "log2",
                 "log10",
                 "exp",
                 "sin",
                 "cos",
                 "tan",
                 "asin",
                 "acos",
                 "atan",
                 "sinh",
                 "cosh",
                 "tanh",
             }) {
            m.emplace(fn, transcendental);
        }

        // ceil / floor / trunc: round to an integral value, preserving the
        // numeric type (Int is already integral, so it passes through). Use
        // round(x, ceil|floor|trunc) for a Float -> Int64 conversion.
        const BuiltinFn integral{
            .min_args = 1,
            .max_args = 1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return a[0];
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            },
            .exec = ScalarExec{.eval = [](std::string_view name,
                                          const std::vector<ScalarValue>& a) -> IV {
                if (std::holds_alternative<std::int64_t>(a[0])) {
                    return a[0];
                }
                if (const auto* d = std::get_if<double>(a.data())) {
                    if (name == "ceil") {
                        return ScalarValue{std::ceil(*d)};
                    }
                    if (name == "floor") {
                        return ScalarValue{std::floor(*d)};
                    }
                    return ScalarValue{std::trunc(*d)};
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            }},
        };
        m.emplace("ceil", integral);
        m.emplace("floor", integral);
        m.emplace("trunc", integral);

        // Float64 / Float32: cast Int or Float to Float64. The column kernel
        // handles the bare column/literal/scalar shape; this eval is the
        // semantic reference and handles computed arguments.
        const BuiltinFn to_float{
            .min_args = 1,
            .max_args = 1,
            .scalar_kernel = ScalarKernel::NumericCast,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double || a[0] == ExprType::String ||
                    a[0] == ExprType::Decimal) {
                    return ExprType::Double;
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Float");
            },
            .exec =
                ScalarExec{.eval = [](std::string_view, const std::vector<ScalarValue>& a) -> IV {
                    if (const auto* dv = std::get_if<DecimalValue>(a.data())) {
                        return ScalarValue{decimal::to_double(dv->units, dv->type.scale)};
                    }
                    if (const auto* s = std::get_if<std::string>(a.data())) {
                        if (auto d = parse_string_as_double(*s)) {
                            return ScalarValue{*d};
                        }
                        return std::unexpected("Float64(): cannot parse '" + *s + "' as a number");
                    }
                    if (auto d = expr_value_to_double(a[0])) {
                        return ScalarValue{*d};
                    }
                    return std::unexpected("cast: cannot cast non-numeric to Float");
                }},
        };
        m.emplace("Float64", to_float);
        m.emplace("Float32", to_float);

        // Int64 / Int32 / Int: cast Int or whole-valued Float to Int64. Column
        // kernel as for Float64 — notably it vectorizes the 0/1 column that
        // `count(col)` lowers to.
        const BuiltinFn to_int{
            .min_args = 1,
            .max_args = 1,
            .scalar_kernel = ScalarKernel::NumericCast,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                // Bool casts to 0/1 — the standard SQL spelling for counting a
                // predicate (`sum(Int64(is_not_null(x)))`, which is what
                // `count(x)` lowers to).
                if (a[0] == ExprType::Int || a[0] == ExprType::Double || a[0] == ExprType::Bool ||
                    a[0] == ExprType::String || a[0] == ExprType::Decimal) {
                    return ExprType::Int;
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Int");
            },
            .exec = ScalarExec{.eval = [](std::string_view name,
                                          const std::vector<ScalarValue>& a) -> IV {
                if (const auto* i = std::get_if<std::int64_t>(a.data())) {
                    return ScalarValue{*i};
                }
                if (const auto* dv = std::get_if<DecimalValue>(a.data())) {
                    auto whole = decimal::to_int64(dv->units, dv->type.scale);
                    if (!whole) {
                        return std::unexpected(std::string(name) + "(): " + whole.error() +
                                               " (use Decimal(x, p, 0) to round first)");
                    }
                    return ScalarValue{*whole};
                }
                if (const auto* b = std::get_if<bool>(a.data())) {
                    return ScalarValue{static_cast<std::int64_t>(*b ? 1 : 0)};
                }
                if (const auto* d = std::get_if<double>(a.data())) {
                    if (*d != std::trunc(*d)) {
                        return std::unexpected(std::string(name) +
                                               "(): cannot cast non-integer Float to Int (use "
                                               "floor(), ceil(), or round())");
                    }
                    return ScalarValue{static_cast<std::int64_t>(*d)};
                }
                if (const auto* s = std::get_if<std::string>(a.data())) {
                    if (auto parsed = parse_string_as_int(*s)) {
                        return ScalarValue{*parsed};
                    }
                    return std::unexpected(std::string(name) + "(): cannot parse '" + *s +
                                           "' as an integer");
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Int");
            }},
        };
        m.emplace("Int64", to_int);
        m.emplace("Int32", to_int);
        m.emplace("Int", to_int);

        // Decimal(x, precision, scale): Int (exact), Decimal (rescaled, half
        // away from zero), String (exact parse), or Float64 (its shortest
        // round-trip text) -> Decimal(p, s). The parser guarantees p and s
        // are valid integer literals; infer_decimal_type reads them.
        m.emplace(
            "Decimal",
            BuiltinFn{
                .min_args = 3,
                .max_args = 3,
                .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                    if (a[1] != ExprType::Int || a[2] != ExprType::Int) {
                        return std::unexpected(
                            "Decimal(x, precision, scale): precision and scale must be integers");
                    }
                    if (a[0] == ExprType::Int || a[0] == ExprType::Double ||
                        a[0] == ExprType::String || a[0] == ExprType::Decimal) {
                        return ExprType::Decimal;
                    }
                    return std::unexpected(
                        "Decimal(): cannot cast to Decimal (expected Int, Float, String, or "
                        "Decimal)");
                },
                .exec = ScalarExec{.eval = [](std::string_view,
                                              const std::vector<ScalarValue>& a) -> IV {
                    const auto* p = std::get_if<std::int64_t>(&a[1]);
                    const auto* s = std::get_if<std::int64_t>(&a[2]);
                    if (p == nullptr || s == nullptr || *p < 1 || *p > decimal::kMaxPrecision ||
                        *s < 0 || *s > *p) {
                        return std::unexpected("Decimal(x, precision, scale): invalid type");
                    }
                    const DecimalType t{.precision = static_cast<std::uint8_t>(*p),
                                        .scale = static_cast<std::uint8_t>(*s)};
                    std::expected<Int128, std::string> units =
                        std::unexpected("Decimal(): cannot cast to Decimal");
                    if (const auto* str = std::get_if<std::string>(a.data())) {
                        units = decimal::parse(*str, t);
                        if (!units) {
                            return std::unexpected("Decimal(): cannot parse '" + *str +
                                                   "': " + units.error());
                        }
                    } else if (const auto* f = std::get_if<double>(a.data())) {
                        units = decimal::from_double(*f, t);
                    } else {
                        units = fit_decimal(a[0], t);
                    }
                    if (!units) {
                        return std::unexpected("Decimal(): " + units.error());
                    }
                    return ScalarValue{DecimalValue{.units = *units, .type = t}};
                }},
            });

        // year / month / day / hour / minute / second: Date|Timestamp -> Int.
        const BuiltinFn date_part{
            .min_args = 1,
            .max_args = 1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                const bool date_ok = (name == "year" || name == "month" || name == "day");
                if (a[0] == ExprType::Timestamp || (date_ok && a[0] == ExprType::Date)) {
                    return ExprType::Int;
                }
                return std::unexpected(std::string(name) +
                                       (date_ok ? ": argument must be Date or Timestamp"
                                                : ": argument must be Timestamp"));
            },
            .exec = ScalarExec{.eval = [](std::string_view name, const std::vector<ScalarValue>& a)
                                   -> IV { return eval_date_part(name, a[0]); }},
        };
        m.emplace("year", date_part);
        m.emplace("month", date_part);
        m.emplace("day", date_part);
        m.emplace("hour", date_part);
        m.emplace("minute", date_part);
        m.emplace("second", date_part);

        // Date: Timestamp|Date|Int -> Date. Truncating an instant to its
        // calendar day cuts on UTC, like every other calendar-boundary
        // operation over a zone-less Timestamp (SPEC section 2). An Int is
        // read as days since the epoch, which is Date's storage.
        m.emplace(
            "Date",
            BuiltinFn{
                .min_args = 1,
                .max_args = 1,
                .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                    if (a[0] == ExprType::Timestamp || a[0] == ExprType::Date ||
                        a[0] == ExprType::Int) {
                        return ExprType::Date;
                    }
                    return std::unexpected(std::string(name) +
                                           "(): cannot cast to Date (expected Timestamp, Date, or "
                                           "Int days since the epoch)");
                },
                .exec = ScalarExec{.eval = [](std::string_view name,
                                              const std::vector<ScalarValue>& a) -> IV {
                    if (const auto* ts = std::get_if<Timestamp>(a.data())) {
                        return ScalarValue{timestamp_to_date(*ts)};
                    }
                    if (const auto* d = std::get_if<Date>(a.data())) {
                        return ScalarValue{*d};
                    }
                    if (const auto* i = std::get_if<std::int64_t>(a.data())) {
                        if (*i < std::numeric_limits<std::int32_t>::min() ||
                            *i > std::numeric_limits<std::int32_t>::max()) {
                            return std::unexpected(std::string(name) + "(): date out of range");
                        }
                        return ScalarValue{Date{static_cast<std::int32_t>(*i)}};
                    }
                    return std::unexpected(std::string(name) + "(): cannot cast to Date");
                }},
            });

        // is_nan: Float64 -> Bool.
        m.emplace("is_nan",
                  BuiltinFn{
                      .min_args = 1,
                      .max_args = 1,
                      .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                          if (a[0] == ExprType::Double) {
                              return ExprType::Bool;
                          }
                          return std::unexpected("is_nan: argument must be Float64");
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          if (const auto* d = std::get_if<double>(a.data())) {
                              return ScalarValue{std::isnan(*d)};
                          }
                          return std::unexpected("is_nan: argument must be Float64");
                      }},
                  });

        // like(value, pattern): SQL-LIKE matching, String x String -> Bool. The
        // column kernel (compile the pattern once, scan) is the fast path for
        // the bare column-and-literal form; this eval is the semantic reference
        // and handles computed arguments.
        m.emplace("like",
                  BuiltinFn{
                      .min_args = 2,
                      .max_args = 2,
                      .scalar_kernel = ScalarKernel::Like,
                      .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                          if (a[0] != ExprType::String || a[1] != ExprType::String) {
                              return std::unexpected("like: both arguments must be String");
                          }
                          return ExprType::Bool;
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          const auto* value = std::get_if<std::string>(a.data());
                          const auto* pattern = std::get_if<std::string>(&a[1]);
                          if (value == nullptr || pattern == nullptr) {
                              return std::unexpected("like: both arguments must be String");
                          }
                          auto compiled = compile_like_pattern(*pattern);
                          if (!compiled) {
                              return std::unexpected(compiled.error());
                          }
                          return ScalarValue{like_match(*compiled, *value)};
                      }},
                  });

        // substring(s, start[, length]): a Unicode-codepoint slice with Polars
        // `str.slice` semantics — start is 0-based and may be negative (from the
        // end), length is optional (to the end when omitted). String -> String.
        // No column kernel yet; the per-row eval is both semantics and impl.
        m.emplace("substring",
                  BuiltinFn{
                      .min_args = 2,
                      .max_args = 3,
                      .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                          if (a[0] != ExprType::String) {
                              return std::unexpected("substring: first argument must be String");
                          }
                          for (std::size_t i = 1; i < a.size(); ++i) {
                              if (a[i] != ExprType::Int) {
                                  return std::unexpected("substring: start and length must be Int");
                              }
                          }
                          return ExprType::String;
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          const auto* value = std::get_if<std::string>(a.data());
                          const auto* start = std::get_if<std::int64_t>(&a[1]);
                          if (value == nullptr || start == nullptr) {
                              return std::unexpected("substring: expects (String, Int[, Int])");
                          }
                          std::optional<std::int64_t> length;
                          if (a.size() == 3) {
                              const auto* len = std::get_if<std::int64_t>(&a[2]);
                              if (len == nullptr) {
                                  return std::unexpected("substring: length must be Int");
                              }
                              length = *len;
                          }
                          return ScalarValue{utf8_substring(*value, *start, length)};
                      }},
                  });

        for (const std::string_view name : {"length", "byte_length"}) {
            m.emplace(name,
                      BuiltinFn{
                          .min_args = 1,
                          .max_args = 1,
                          .scalar_kernel = ScalarKernel::StringLength,
                          .infer = [](std::string_view function_name,
                                      const std::vector<ExprType>& a) -> IT {
                              if (a[0] != ExprType::String) {
                                  return std::unexpected(std::string(function_name) +
                                                         ": argument must be String");
                              }
                              return ExprType::Int;
                          },
                          .exec = ScalarExec{.eval = [](std::string_view function_name,
                                                        const std::vector<ScalarValue>& a) -> IV {
                              const auto* value = std::get_if<std::string>(a.data());
                              if (value == nullptr) {
                                  return std::unexpected(std::string(function_name) +
                                                         ": argument must be String");
                              }
                              if (function_name == "byte_length") {
                                  return ScalarValue{static_cast<std::int64_t>(value->size())};
                              }
                              return ScalarValue{utf8_codepoint_count(*value)};
                          }},
                      });
        }

        // pmin / pmax: 2+ comparable args of one type (Int/Float widen to Float).
        const BuiltinFn pminmax{
            .min_args = 2,
            .max_args = -1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                std::optional<ExprType> result;
                for (ExprType t : a) {
                    if (!result) {
                        result = t;
                        continue;
                    }
                    if ((*result == ExprType::Int && t == ExprType::Double) ||
                        (*result == ExprType::Double && t == ExprType::Int)) {
                        result = ExprType::Double;
                        continue;
                    }
                    if (*result != t) {
                        return std::unexpected(
                            std::string(name) +
                            ": arguments must all be comparable and of one type");
                    }
                }
                if (*result == ExprType::Bool) {
                    return std::unexpected(std::string(name) +
                                           ": Bool arguments are not supported");
                }
                return *result;
            },
            .exec = ScalarExec{.eval = [](std::string_view name,
                                          const std::vector<ScalarValue>& a) -> IV {
                const bool want_min = (name == "pmin");
                auto better = [&](const ScalarValue& cand,
                                  const ScalarValue& best) -> std::expected<bool, std::string> {
                    auto num = [](const ScalarValue& v) -> std::optional<double> {
                        return expr_value_to_double(v);
                    };
                    if (auto c = num(cand)) {
                        auto b = num(best);
                        if (!b) {
                            return std::unexpected(std::string(name) +
                                                   ": arguments must all be comparable");
                        }
                        return want_min ? *c < *b : *c > *b;
                    }
                    if (std::holds_alternative<std::string>(cand) &&
                        std::holds_alternative<std::string>(best)) {
                        return want_min ? std::get<std::string>(cand) < std::get<std::string>(best)
                                        : std::get<std::string>(cand) > std::get<std::string>(best);
                    }
                    if (std::holds_alternative<Date>(cand) && std::holds_alternative<Date>(best)) {
                        return want_min ? std::get<Date>(cand) < std::get<Date>(best)
                                        : std::get<Date>(cand) > std::get<Date>(best);
                    }
                    if (std::holds_alternative<Timestamp>(cand) &&
                        std::holds_alternative<Timestamp>(best)) {
                        return want_min ? std::get<Timestamp>(cand) < std::get<Timestamp>(best)
                                        : std::get<Timestamp>(cand) > std::get<Timestamp>(best);
                    }
                    return std::unexpected(std::string(name) +
                                           ": arguments must all be comparable and of one type");
                };
                ScalarValue best = a[0];
                if (std::holds_alternative<bool>(best)) {
                    return std::unexpected(std::string(name) +
                                           ": Bool arguments are not supported");
                }
                for (std::size_t i = 1; i < a.size(); ++i) {
                    auto take = better(a[i], best);
                    if (!take) {
                        return std::unexpected(take.error());
                    }
                    if (*take) {
                        best = a[i];
                    }
                }
                return best;
            }},
        };
        m.emplace("pmin", pminmax);
        m.emplace("pmax", pminmax);

        // ── Generators (∅→N): produce a column from parameters/pattern; no
        // input rows are read. Their GeneratorExec payload is whole-column by
        // construction (no per-row form exists); every evaluator dispatches
        // them through the registry
        // instead of a bespoke `is_rng_func`/`callee == "rep"` branch. Arity
        // and argument values are validated by the kernels (which have the
        // precise messages), so min/max stay lenient here.
        const BuiltinFn rng_generator{
            .min_args = 0,
            .max_args = -1,
            .infer = [](std::string_view name, const std::vector<ExprType>&) -> IT {
                return rng_func_returns_int(name) ? ExprType::Int : ExprType::Double;
            },
            .exec = GeneratorExec{.column_eval = [](const ir::CallExpr& call, const Table&,
                                                    std::size_t rows, const ColumnEvalCtx&)
                                      -> std::expected<ComputedColumn, std::string> {
                auto col = apply_rng_func(call, rows);
                if (!col) {
                    return std::unexpected(col.error());
                }
                return ComputedColumn{.column = std::move(*col), .validity = std::nullopt};
            }},
        };
        for (const auto* fn : {
                 "rand_uniform",
                 "rand_normal",
                 "rand_student_t",
                 "rand_gamma",
                 "rand_exponential",
                 "rand_bernoulli",
                 "rand_poisson",
                 "rand_int",
             }) {
            m.emplace(fn, rng_generator);
        }

        // rep(x, ...) / rep([e0, e1, ...], ...): yields the type of its first
        // positional argument (the array form lowers its elements to positional
        // args, so args[0] is the first element either way).
        m.emplace(
            "rep",
            BuiltinFn{
                .min_args = 1,
                .max_args = -1,
                .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                    return a[0];
                },
                .exec =
                    GeneratorExec{.column_eval = [](const ir::CallExpr& call, const Table& input,
                                                    std::size_t rows, const ColumnEvalCtx&)
                                      -> std::expected<ComputedColumn, std::string> {
                        auto col = apply_rep_func(call, input, rows);
                        if (!col) {
                            return std::unexpected(col.error());
                        }
                        return ComputedColumn{.column = std::move(*col), .validity = std::nullopt};
                    }},
            });

        // seq(from=0, by=1): an arithmetic ramp the length of the frame. Both
        // parameters are positional because `infer` is handed positional types
        // only — Int when both are Int, Float as soon as either one is.
        m.emplace(
            "seq",
            BuiltinFn{
                .min_args = 0,
                .max_args = 2,
                .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                    for (const auto t : a) {
                        if (t == ExprType::Double) {
                            return ExprType::Double;
                        }
                        if (t != ExprType::Int) {
                            return std::unexpected(
                                std::string("seq: from and by must be Int or Float"));
                        }
                    }
                    return ExprType::Int;
                },
                .exec = GeneratorExec{.column_eval = [](const ir::CallExpr& call, const Table&,
                                                        std::size_t rows, const ColumnEvalCtx&)
                                          -> std::expected<ComputedColumn, std::string> {
                    auto col = apply_seq_func(call, rows);
                    if (!col) {
                        return std::unexpected(col.error());
                    }
                    return ComputedColumn{.column = std::move(*col), .validity = std::nullopt};
                }},
            });

        // ── Transforms (N→N, ordered/validity-aware): the output row i depends
        // on other rows (rolling/cum/lag/lead/fill_forward/fill_backward) or on
        // column validity (fill_null, null_if_*), so they evaluate at column
        // level via `column_eval`. As with Generators, arity and argument shape
        // (e.g. "first argument must be a column name") are validated by the
        // kernels, which carry the precise messages; `infer` only reports the
        // result type so nested transforms type-check.
        m.emplace("lag",
                  BuiltinFn{
                      .min_args = 0,
                      .max_args = -1,
                      .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                          if (a.empty()) {
                              return std::unexpected(std::string(name) + ": expected 2 arguments");
                          }
                          return a[0];
                      },
                      .exec = TransformExec{.column_eval = [](const ir::CallExpr& call,
                                                              const Table& input, std::size_t,
                                                              const ColumnEvalCtx& ctx)
                                                -> std::expected<ComputedColumn, std::string> {
                          auto res = eval_lag_lead_column(call, input, call.callee == "lag",
                                                          ctx.scalars, ctx.externs);
                          if (!res) {
                              return std::unexpected(res.error());
                          }
                          return ComputedColumn{.column = std::move(res->column),
                                                .validity = std::move(res->validity)};
                      }},
                  });
        m.emplace("lead", m.at("lag"));

        m.emplace("with_timezone",
                  BuiltinFn{
                      .min_args = 2,
                      .max_args = 2,
                      .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                          if (a.size() != 2) {
                              return std::unexpected(std::string(name) +
                                                     ": expected 2 arguments (column, zone)");
                          }
                          return ExprType::Timestamp;
                      },
                      .exec = TransformExec{.column_eval = [](const ir::CallExpr& call,
                                                              const Table& input, std::size_t,
                                                              const ColumnEvalCtx&)
                                                -> std::expected<ComputedColumn, std::string> {
                          return eval_with_timezone(call, input);
                      }},
                  });

        m.emplace("in_timezone",
                  BuiltinFn{
                      .min_args = 2,
                      .max_args = 2,
                      .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                          if (a.size() != 2) {
                              return std::unexpected(std::string(name) +
                                                     ": expected 2 arguments (column, zone)");
                          }
                          return ExprType::Timestamp;
                      },
                      .exec = TransformExec{.column_eval = [](const ir::CallExpr& call,
                                                              const Table& input, std::size_t,
                                                              const ColumnEvalCtx&)
                                                -> std::expected<ComputedColumn, std::string> {
                          return eval_in_timezone(call, input);
                      }},
                  });

        m.emplace("cumsum",
                  BuiltinFn{
                      .min_args = 0,
                      .max_args = -1,
                      .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                          if (a.empty()) {
                              return std::unexpected(std::string(name) + ": expected 1 argument");
                          }
                          return a[0];
                      },
                      .exec = TransformExec{.column_eval = [](const ir::CallExpr& call,
                                                              const Table& input, std::size_t,
                                                              const ColumnEvalCtx&)
                                                -> std::expected<ComputedColumn, std::string> {
                          auto col =
                              eval_cumsum_cumprod_column(call, input, call.callee == "cumprod");
                          if (!col) {
                              return std::unexpected(col.error());
                          }
                          return std::move(*col);
                      }},
                  });
        m.emplace("cumprod", m.at("cumsum"));

        // fill_forward / fill_backward: genuinely ordered (LOCF / NOCB read
        // neighbouring rows), so they stay Transform-kind, column-only.
        m.emplace("fill_forward",
                  BuiltinFn{
                      .min_args = 0,
                      .max_args = -1,
                      .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                          if (a.empty()) {
                              return std::unexpected(std::string(name) +
                                                     ": expected at least 1 argument");
                          }
                          return a[0];
                      },
                      .exec = TransformExec{.column_eval = [](const ir::CallExpr& call,
                                                              const Table& input, std::size_t,
                                                              const ColumnEvalCtx& ctx)
                                                -> std::expected<ComputedColumn, std::string> {
                          auto res = call.callee == "fill_forward"
                                         ? eval_fill_forward(call, input, ctx.exec)
                                         : eval_fill_backward(call, input, ctx.exec);
                          if (!res) {
                              return std::unexpected(res.error());
                          }
                          return ComputedColumn{.column = std::move(res->column),
                                                .validity = std::move(res->validity)};
                      }},
                  });
        m.emplace("fill_backward", m.at("fill_forward"));

        // ── Null-handling scalars: row-local functions whose *purpose* is
        // null, so their per-row eval opts into receiving a null
        // (NullPolicy::Handles) instead of the
        // central propagation. Each keeps its column kernel as a whole-column
        // fast path for the top-level bare form; the per-row eval is the
        // general form and the semantic reference — the two must agree.
        m.emplace("fill_null",
                  BuiltinFn{
                      .min_args = 0,
                      .max_args = -1,
                      .null_policy = NullPolicy::Handles,
                      .scalar_kernel = ScalarKernel::FillNull,
                      .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                          if (a.size() != 2) {
                              return std::unexpected(std::string(name) +
                                                     ": expected 2 arguments (col, value)");
                          }
                          const bool numeric_widen =
                              a[0] == ExprType::Double && a[1] == ExprType::Int;
                          if (a[1] != a[0] && !numeric_widen) {
                              return std::unexpected(std::string(name) +
                                                     ": fill value must match the column type");
                          }
                          return a[0];
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          if (std::holds_alternative<NullScalar>(a[0])) {
                              return a[1];
                          }
                          return a[0];
                      }},
                  });

        m.emplace(
            "null_if_nan",
            BuiltinFn{
                .min_args = 0,
                .max_args = -1,
                .null_policy = NullPolicy::Handles,
                .scalar_kernel = ScalarKernel::FloatClean,
                .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                    if (a.size() != 1) {
                        return std::unexpected(std::string(name) + ": expected 1 argument");
                    }
                    if (a[0] != ExprType::Double) {
                        return std::unexpected(std::string(name) + ": column must be Float64");
                    }
                    return ExprType::Double;
                },
                .exec = ScalarExec{.eval = [](std::string_view name,
                                              const std::vector<ScalarValue>& a) -> IV {
                    if (std::holds_alternative<NullScalar>(a[0])) {
                        return a[0];  // already null: stays null
                    }
                    const auto* d = std::get_if<double>(a.data());
                    if (d == nullptr) {
                        return std::unexpected(std::string(name) + ": column must be Float64");
                    }
                    const bool to_null =
                        name == "null_if_nan" ? std::isnan(*d) : !std::isfinite(*d);
                    return to_null ? ScalarValue{NullScalar{ScalarKind::Double}} : a[0];
                }},
            });
        m.emplace("null_if_not_finite", m.at("null_if_nan"));

        // coalesce(a, b, ...): first non-null argument, per row. The column
        // kernel lives in filter.cpp because its arguments are arbitrary
        // expressions evaluated through eval_value_vec.
        m.emplace("coalesce",
                  BuiltinFn{
                      .min_args = 0,
                      .max_args = -1,
                      .null_policy = NullPolicy::Handles,
                      .scalar_kernel = ScalarKernel::Coalesce,
                      .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                          if (a.size() < 2) {
                              return std::unexpected("coalesce: expected at least 2 arguments");
                          }
                          const bool categorical = std::ranges::any_of(
                              a, [](ExprType type) { return type == ExprType::Categorical; });
                          for (const auto& t : a) {
                              if (t != a[0] && (t != ExprType::String || !categorical) &&
                                  (t != ExprType::Categorical || a[0] != ExprType::String)) {
                                  return std::unexpected("coalesce: arguments must share one type");
                              }
                          }
                          return categorical ? ExprType::Categorical : a[0];
                      },
                      .exec = ScalarExec{.eval = [](std::string_view,
                                                    const std::vector<ScalarValue>& a) -> IV {
                          for (const auto& v : a) {
                              if (!std::holds_alternative<NullScalar>(v)) {
                                  return v;
                              }
                          }
                          return a.back();  // every argument null
                      }},
                  });

        // rolling_*: the per-call window arg was peeled into the __window_n /
        // __window_ns sentinel named args by lowering; the enclosing `window`
        // clause (if any) arrives via ctx.window as the fallback.
        const BuiltinFn rolling_transform{
            .min_args = 0,
            .max_args = -1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (name == "rolling_count") {
                    return ExprType::Int;
                }
                if (name == "rolling_sum" || name == "rolling_min" || name == "rolling_max" ||
                    name == "rolling_first" || name == "rolling_last") {
                    if (a.empty()) {
                        return std::unexpected(std::string(name) + ": expected 1 argument");
                    }
                    return a[0];
                }
                return ExprType::Double;
            },
            .exec = TransformExec{.column_eval = [](const ir::CallExpr& call, const Table& input,
                                                    std::size_t, const ColumnEvalCtx& ctx)
                                      -> std::expected<ComputedColumn, std::string> {
                auto spec = rolling_window_spec(call, ctx.window);
                if (!spec) {
                    return std::unexpected(spec.error());
                }
                return apply_rolling_func(call, input, *spec, ctx.window_aligned);
            }},
        };
        for (const auto* fn : {
                 "rolling_sum",
                 "rolling_mean",
                 "rolling_min",
                 "rolling_max",
                 "rolling_count",
                 "rolling_median",
                 "rolling_std",
                 "rolling_ewma",
                 "rolling_quantile",
                 "rolling_skew",
                 "rolling_kurtosis",
                 "rolling_first",
                 "rolling_last",
             }) {
            m.emplace(fn, rolling_transform);
        }

        // window_start() / window_end(): the nominal bounds of the enclosing
        // `window` clause's window for each row (grid boundaries when aligned,
        // else [t-dur, t]). Zero-arg; the duration/alignment arrive via ctx.
        const BuiltinFn window_bound{
            .min_args = 0,
            .max_args = 0,
            .infer = [](std::string_view, const std::vector<ExprType>&) -> IT {
                return ExprType::Timestamp;
            },
            .exec = TransformExec{.column_eval = [](const ir::CallExpr& call, const Table& input,
                                                    std::size_t, const ColumnEvalCtx& ctx)
                                      -> std::expected<ComputedColumn, std::string> {
                if (!ctx.window.has_value()) {
                    return std::unexpected(call.callee + ": requires an enclosing `window` clause");
                }
                return window_bound_column(input, *ctx.window, ctx.window_aligned,
                                           /*want_end=*/call.callee == "window_end");
            }},
        };
        m.emplace("window_start", window_bound);
        m.emplace("window_end", window_bound);

        // ── Aggregates (N→1, per group): reduce a column to one scalar. The
        // registry is the single name-to-ir::AggFunc mapping — execution
        // (aggregate.cpp) resolves the enum through parse_aggregate_func,
        // which reads the AggregateExec payload here.
        const auto agg_infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
            if (name == "count" || name == "count_distinct") {
                return ExprType::Int;
            }
            if (name == "sum" || name == "min" || name == "max" || name == "first" ||
                name == "last" || name == "first_null" || name == "last_null") {
                // Type-preserving reductions.
                if (a.empty()) {
                    return std::unexpected(std::string(name) + "(): expected column argument");
                }
                return a[0];
            }
            return ExprType::Double;  // mean/median/std/ewma/quantile/skew/kurtosis/cov/corr
        };
        constexpr std::array<std::pair<std::string_view, ir::AggFunc>, 18> agg_funcs{{
            {"sum", ir::AggFunc::Sum},
            {"mean", ir::AggFunc::Mean},
            {"min", ir::AggFunc::Min},
            {"max", ir::AggFunc::Max},
            {"count", ir::AggFunc::Count},
            {"count_distinct", ir::AggFunc::CountDistinct},
            {"first", ir::AggFunc::First},
            {"last", ir::AggFunc::Last},
            {"first_null", ir::AggFunc::FirstNull},
            {"last_null", ir::AggFunc::LastNull},
            {"median", ir::AggFunc::Median},
            {"std", ir::AggFunc::Stddev},
            {"ewma", ir::AggFunc::Ewma},
            {"quantile", ir::AggFunc::Quantile},
            {"skew", ir::AggFunc::Skew},
            {"kurtosis", ir::AggFunc::Kurtosis},
            {"cov", ir::AggFunc::Cov},
            {"corr", ir::AggFunc::Corr},
        }};
        for (const auto& [name, func] : agg_funcs) {
            m.emplace(name, BuiltinFn{
                                .min_args = 0,
                                .max_args = -1,
                                .infer = agg_infer,
                                .exec = AggregateExec{.func = func},
                            });
        }

        // One source of truth for kind: the IR-side classifier (ir::fn_kind,
        // used by lowering/planning, which cannot see this registry) must
        // agree with the exec alternative each entry holds. Checked once, at
        // registry construction.
        for (const auto& [name, fn] : m) {
            validate_builtin(name, fn);
            const auto kind = ir::fn_kind(name);
            if (!kind.has_value() || *kind != fn_kind_of(fn)) {
                invariant_violation(
                    "builtins(): ir::fn_kind disagrees with the registry's exec "
                    "alternative for '" +
                    std::string(name) + "'");
            }
            // Same idea for null behaviour, which the IR side also cannot see.
            // A scalar opts into receiving Null exactly when its result does
            // not simply follow its arguments' presence, so the two tables have
            // one answer between them and disagreeing is a bug in whichever was
            // edited alone. Without this, adding the next `coalesce` and
            // forgetting the IR entry would leave the schema pass quietly
            // proving a column null-free that is not.
            const auto behavior = ir::scalar_null_behavior(name);
            if (behavior.has_value() && (fn.null_policy == NullPolicy::Handles) ==
                                            (*behavior == ir::NullBehavior::Propagates)) {
                invariant_violation(
                    "builtins(): ir::NullBehavior disagrees with the registry's null policy "
                    "for '" +
                    std::string(name) + "'");
            }
        }

        return m;
    }();
    return table;
}

auto find_builtin(std::string_view name) -> const BuiltinFn* {
    const auto& m = builtins();
    auto it = m.find(name);
    return it == m.end() ? nullptr : &it->second;
}

namespace {

// round(x, mode): lowering resolves the mode to its canonical `RoundMode::Member`
// string literal (SPEC.md Section 3.7), so round is dispatched separately from
// the value-based scalar registry above.
auto extract_ir_round_mode(const ir::Expr& arg) -> std::expected<RoundMode, std::string> {
    if (const auto* literal = std::get_if<ir::Literal>(&arg.node)) {
        if (const auto* text = std::get_if<std::string>(&literal->value)) {
            auto mode = resolve_round_mode(*text);
            if (!mode.has_value()) {
                return std::unexpected("round(): " + mode.error());
            }
            return *mode;
        }
    }
    return std::unexpected("round(): second argument must be a RoundMode such as Nearest");
}

auto apply_round(double v, RoundMode mode) -> std::int64_t {
    switch (mode) {
        case RoundMode::Nearest:
            return static_cast<std::int64_t>(std::llround(v));
        case RoundMode::Bankers:
            return static_cast<std::int64_t>(std::llrint(v));  // FE_TONEAREST: ties to even
        case RoundMode::Floor:
            return static_cast<std::int64_t>(std::floor(v));
        case RoundMode::Ceil:
            return static_cast<std::int64_t>(std::ceil(v));
        case RoundMode::Trunc:
            break;
    }
    return static_cast<std::int64_t>(std::trunc(v));
}

auto apply_decimal_round(const DecimalValue& value, RoundMode mode)
    -> std::expected<DecimalValue, std::string> {
    const int scale = value.type.scale;
    const Int128 divisor = decimal::pow10(scale);
    Int128 quotient = value.units / divisor;
    const Int128 remainder = value.units % divisor;
    const bool has_remainder = remainder != 0;
    bool increment = false;
    if (mode == RoundMode::Floor) {
        increment = value.units < 0 && has_remainder;
    } else if (mode == RoundMode::Ceil) {
        increment = value.units > 0 && has_remainder;
    } else if (mode == RoundMode::Nearest || mode == RoundMode::Bankers) {
        const Int128 magnitude = remainder < 0 ? -remainder : remainder;
        const Int128 half = divisor / 2;
        increment = magnitude > half ||
                    (magnitude == half && (mode == RoundMode::Nearest || (quotient % 2) != 0));
    }
    if (increment) {
        quotient += value.units < 0 ? -1 : 1;
    }
    const auto precision = static_cast<std::uint8_t>(
        std::min<int>(decimal::kMaxPrecision, std::max<int>(1, value.type.precision - scale + 1)));
    const DecimalType type{.precision = precision, .scale = 0};
    if (!decimal::fits(quotient, precision)) {
        return std::unexpected(decimal_overflow(type));
    }
    return DecimalValue{.units = quotient, .type = type};
}

}  // namespace

auto infer_expr_type(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars,
                     const ExternRegistry* externs) -> std::expected<ExprType, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        // `^name` resolves in lexical scope only, so the input's columns are
        // skipped even when one shares the name.
        const auto* source = col->lexical ? nullptr : input.find(col->name);
        if (source == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    // A binding's type, null or not (a null records its kind).
                    return expr_type_of_kind(scalar_kind(it->second));
                }
            }
            if (col->lexical) {
                return std::unexpected("'^" + col->name +
                                       "' does not resolve to a lexical binding");
            }
            return std::unexpected("unknown column in expression: " + col->name +
                                   " (available: " + format_columns(input) + ")");
        }
        // Most physical consumers intentionally see a categorical as String.
        // Expression inference retains the representation so CASE/coalesce can
        // plan a categorical result without changing aggregate signatures.
        return std::holds_alternative<Column<Categorical>>(*source) ? ExprType::Categorical
                                                                    : expr_type_for_column(*source);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        // Visited rather than tested alternative by alternative: a literal
        // arm that falls through to String allocates a String column for a
        // `date"..."`/`ts"..."` value that evaluation then hands back as a
        // Date/Timestamp, which the column append rejects. An
        // exhaustive visit makes a new Literal alternative a compile error
        // instead.
        return std::visit(
            [](const auto& value) -> ExprType {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::int64_t>) {
                    return ExprType::Int;
                } else if constexpr (std::is_same_v<T, double>) {
                    return ExprType::Double;
                } else if constexpr (std::is_same_v<T, bool>) {
                    return ExprType::Bool;
                } else if constexpr (std::is_same_v<T, Date>) {
                    return ExprType::Date;
                } else if constexpr (std::is_same_v<T, Timestamp>) {
                    return ExprType::Timestamp;
                } else if constexpr (std::is_same_v<T, DecimalValue>) {
                    return ExprType::Decimal;
                } else if constexpr (std::is_same_v<T, NullScalar>) {
                    return expr_type_of_kind(value.kind);
                } else {
                    static_assert(std::is_same_v<T, std::string>);
                    return ExprType::String;
                }
            },
            lit->value);
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = infer_expr_type(*bin->left, input, scalars, externs);
        if (!left) {
            return left;
        }
        auto right = infer_expr_type(*bin->right, input, scalars, externs);
        if (!right) {
            return right;
        }
        if (left.value() == ExprType::Decimal || right.value() == ExprType::Decimal) {
            // A decimal meets only exact operands: another decimal, an Int64
            // (exactly Decimal(19, 0)), or a float *literal*, read back from
            // its text. A Float64 column is rejected rather than silently
            // rounding every exact value it touches.
            const bool left_dec = left.value() == ExprType::Decimal;
            const ExprType other = left_dec ? right.value() : left.value();
            const ir::Expr& other_expr = left_dec ? *bin->right : *bin->left;
            const bool exact_other = other == ExprType::Decimal || other == ExprType::Int ||
                                     (other == ExprType::Double && is_float_literal(other_expr));
            if (!exact_other) {
                return std::unexpected(
                    other == ExprType::Double
                        ? "cannot mix Decimal and Float64 in arithmetic; convert one side "
                          "explicitly with Decimal(x, precision, scale) or Float64(x)"
                        : "Decimal arithmetic needs Decimal or Int64 operands");
            }
            if (bin->op == ir::ArithmeticOp::Div) {
                return ExprType::Double;
            }
            auto type = infer_decimal_type(expr, input, scalars);
            if (!type) {
                return std::unexpected(type.error());
            }
            return ExprType::Decimal;
        }
        if (left.value() == ExprType::String || right.value() == ExprType::String ||
            left.value() == ExprType::Categorical || right.value() == ExprType::Categorical) {
            return std::unexpected("string arithmetic not supported");
        }
        if (left.value() == ExprType::Date || right.value() == ExprType::Date ||
            left.value() == ExprType::Timestamp || right.value() == ExprType::Timestamp) {
            return std::unexpected("date/time arithmetic not supported");
        }
        if (bin->op == ir::ArithmeticOp::Div) {
            return ExprType::Double;
        }
        if (left.value() == ExprType::Double || right.value() == ExprType::Double) {
            return ExprType::Double;
        }
        return ExprType::Int;
    }
    // Boolean-producing nodes are valid in value position (a `Column<Bool>`):
    // comparisons, logical connectives, and `is_null` / `is_not_null`.
    if (std::holds_alternative<ir::CompareExpr>(expr.node) ||
        std::holds_alternative<ir::LogicalExpr>(expr.node) ||
        std::holds_alternative<ir::IsNullExpr>(expr.node)) {
        return ExprType::Bool;
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if (call->callee == "__null") {
            if (!call->args.empty()) {
                return std::unexpected("NULL takes no arguments");
            }
            return std::unexpected(std::string(kUntypedNullError));
        }
        if (call->callee == "__case") {
            if (call->args.empty() || call->args.size() % 2 == 0) {
                return std::unexpected("case: expected condition/value arm pairs followed by else");
            }
            for (std::size_t i = 0; i + 1 < call->args.size(); i += 2) {
                auto condition = infer_expr_type(*call->args[i], input, scalars, externs);
                if (!condition) {
                    return condition;
                }
                if (*condition != ExprType::Bool) {
                    return std::unexpected("case: each condition must have type Bool");
                }
            }
            std::optional<ExprType> result;
            bool categorical_result = false;
            // The value arms are the odd arguments, then `else` last: every one
            // is checked, whichever a row would take.
            std::vector<std::size_t> values;
            for (std::size_t i = 1; i + 1 < call->args.size(); i += 2) {
                values.push_back(i);
            }
            values.push_back(call->args.size() - 1);
            for (const std::size_t i : values) {
                const auto* null_arm = std::get_if<ir::CallExpr>(&call->args[i]->node);
                if (null_arm != nullptr && null_arm->callee == "__null") {
                    continue;
                }
                auto type = infer_expr_type(*call->args[i], input, scalars, externs);
                if (!type) {
                    return type;
                }
                categorical_result = categorical_result || *type == ExprType::Categorical;
                if (!result.has_value()) {
                    result = *type;
                } else if (*result == ExprType::Int && *type == ExprType::Double) {
                    result = ExprType::Double;
                } else if ((*result != ExprType::Double || *type != ExprType::Int) &&
                           (*result != ExprType::Categorical || *type != ExprType::String) &&
                           (*result != ExprType::String || *type != ExprType::Categorical) &&
                           *result != *type) {
                    return std::unexpected(
                        "case: all value arms must have the same type (or Int/Float)");
                }
            }
            if (!result.has_value()) {
                return std::unexpected("case: at least one arm must provide a non-NULL value");
            }
            return categorical_result ? ExprType::Categorical : *result;
        }
        // Pure row-wise scalar builtins: single source of truth (see
        // builtins()). Both this pass and eval_call_columns dispatch here.
        if (auto it = builtins().find(call->callee); it != builtins().end()) {
            const auto& fn = it->second;
            const auto argc = static_cast<int>(call->args.size());
            if (argc < fn.min_args || (fn.max_args >= 0 && argc > fn.max_args)) {
                return std::unexpected(std::string(call->callee) + ": wrong number of arguments");
            }
            std::vector<ExprType> arg_types;
            arg_types.reserve(call->args.size());
            for (const auto& arg : call->args) {
                auto t = infer_expr_type(*arg, input, scalars, externs);
                if (!t) {
                    return t;
                }
                // Categorical is a physical String representation. Only
                // CASE/coalesce preserve it; every other builtin keeps its
                // established String-facing signature (length, like, etc.).
                arg_types.push_back(call->callee == "coalesce" || *t != ExprType::Categorical
                                        ? *t
                                        : ExprType::String);
            }
            return fn.infer(call->callee, arg_types);
        }
        // round(x, mode): mode is a RoundMode literal, so it is dispatched apart
        // from the value-based registry. Always yields Int64.
        if (call->callee == "round") {
            if (call->args.size() != 2) {
                return std::unexpected("round: expected 2 arguments (value, mode)");
            }
            auto mode = extract_ir_round_mode(*call->args[1]);
            if (!mode) {
                return std::unexpected(mode.error());
            }
            auto arg_type = infer_expr_type(*call->args[0], input, scalars, externs);
            if (!arg_type) {
                return arg_type;
            }
            if (*arg_type != ExprType::Int && *arg_type != ExprType::Double &&
                *arg_type != ExprType::Decimal) {
                return std::unexpected("round: first argument must be numeric");
            }
            return *arg_type == ExprType::Decimal ? ExprType::Decimal : ExprType::Int;
        }
        // Transforms (rolling/cum/lag/lead/fill/null_if_*) infer through the
        // registry entries above, same as Scalars and Generators.
        // Extern scalar function lookup
        if (externs == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        const auto* fn = externs->find(call->callee);
        if (fn == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        if (fn->kind != ExternReturnKind::Scalar || !fn->scalar_kind.has_value()) {
            return std::unexpected("function not usable in expression: " + call->callee);
        }
        std::size_t scalar_begin = 0;
        if (fn->first_arg_is_table) {
            if (call->args.empty()) {
                return std::unexpected(call->callee + " requires a table first argument");
            }
            const auto* table_call = std::get_if<ir::CallExpr>(&call->args.front()->node);
            const auto* table_fn =
                table_call == nullptr ? nullptr : externs->find(table_call->callee);
            if (table_fn == nullptr || table_fn->kind != ExternReturnKind::Table ||
                table_fn->first_arg_is_table) {
                return std::unexpected(call->callee +
                                       ": first argument must be a table-returning extern call");
            }
            scalar_begin = 1;
        }
        for (std::size_t i = scalar_begin; i < call->args.size(); ++i) {
            const auto& arg = call->args[i];
            auto arg_type = infer_expr_type(*arg, input, scalars, externs);
            if (!arg_type) {
                return arg_type;
            }
        }
        return expr_type_of_kind(fn->scalar_kind.value());
    }
    return std::unexpected("unsupported expression");
}

auto infer_decimal_type(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::expected<DecimalType, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (const auto* source = col->lexical ? nullptr : input.find(col->name)) {
            if (const auto* d = std::get_if<Column<Decimal>>(source)) {
                return decimal_type_of(*d);
            }
            if (std::holds_alternative<Column<std::int64_t>>(*source)) {
                return decimal::kInt64Type;
            }
            return std::unexpected("column '" + col->name + "' is not Decimal");
        }
        if (scalars != nullptr) {
            if (auto it = scalars->find(col->name); it != scalars->end()) {
                if (const auto* d = std::get_if<DecimalValue>(&it->second)) {
                    return d->type;
                }
                if (const auto* null = std::get_if<NullScalar>(&it->second);
                    null != nullptr && null->kind == ScalarKind::Decimal) {
                    return null->decimal;
                }
                if (std::holds_alternative<std::int64_t>(it->second)) {
                    return decimal::kInt64Type;
                }
            }
        }
        return std::unexpected("'" + col->name + "' is not Decimal");
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        if (const auto* d = std::get_if<DecimalValue>(&lit->value)) {
            return d->type;
        }
        if (std::holds_alternative<std::int64_t>(lit->value)) {
            return decimal::kInt64Type;
        }
        if (const auto* f = std::get_if<double>(&lit->value)) {
            auto v = decimal::literal_from_double(*f);
            if (!v) {
                return std::unexpected(v.error());
            }
            return v->type;
        }
        if (const auto* null = std::get_if<NullScalar>(&lit->value);
            null != nullptr && null->kind == ScalarKind::Decimal) {
            return null->decimal;
        }
        return std::unexpected("literal is not Decimal");
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = infer_decimal_type(*bin->left, input, scalars);
        if (!left) {
            return left;
        }
        auto right = infer_decimal_type(*bin->right, input, scalars);
        if (!right) {
            return right;
        }
        return decimal_arith_type(bin->op, *left, *right);
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if ((call->callee == "abs" || call->callee == "rolling_min" ||
             call->callee == "rolling_max" || call->callee == "rolling_first" ||
             call->callee == "rolling_last") &&
            !call->args.empty()) {
            return infer_decimal_type(*call->args[0], input, scalars);
        }
        if ((call->callee == "cumsum" || call->callee == "rolling_sum") && !call->args.empty()) {
            auto input_type = infer_decimal_type(*call->args[0], input, scalars);
            if (!input_type) {
                return input_type;
            }
            return decimal::sum_result_type(*input_type);
        }
        if ((call->callee == "abs" || call->callee == "round") && !call->args.empty()) {
            auto input_type = infer_decimal_type(*call->args[0], input, scalars);
            if (!input_type) {
                return input_type;
            }
            if (call->callee == "abs") {
                return *input_type;
            }
            const auto precision = static_cast<std::uint8_t>(
                std::min<int>(decimal::kMaxPrecision,
                              std::max<int>(1, input_type->precision - input_type->scale + 1)));
            return DecimalType{.precision = precision, .scale = 0};
        }
        if (call->callee == "Decimal" && call->args.size() == 3) {
            const auto* p = std::get_if<ir::Literal>(&call->args[1]->node);
            const auto* s = std::get_if<ir::Literal>(&call->args[2]->node);
            const auto* pi = p != nullptr ? std::get_if<std::int64_t>(&p->value) : nullptr;
            const auto* si = s != nullptr ? std::get_if<std::int64_t>(&s->value) : nullptr;
            if (pi == nullptr || si == nullptr) {
                return std::unexpected(
                    "Decimal(x, precision, scale): precision and scale must "
                    "be integer literals");
            }
            const DecimalType t{.precision = static_cast<std::uint8_t>(*pi),
                                .scale = static_cast<std::uint8_t>(*si)};
            if (*pi < 1 || *pi > decimal::kMaxPrecision || *si < 0 || *si > *pi) {
                return std::unexpected("unsupported " + decimal::type_name(t));
            }
            return t;
        }
        // CASE arms and coalesce arguments: the narrowest type holding every
        // non-NULL alternative exactly.
        if (call->callee == "__case" || call->callee == "coalesce") {
            std::optional<DecimalType> result;
            for (std::size_t i = 0; i < call->args.size(); ++i) {
                if (call->callee == "__case" && i + 1 < call->args.size() && i % 2 == 0) {
                    continue;  // a condition, not a value arm
                }
                const auto* null_arm = std::get_if<ir::CallExpr>(&call->args[i]->node);
                if (null_arm != nullptr && null_arm->callee == "__null") {
                    continue;
                }
                auto t = infer_decimal_type(*call->args[i], input, scalars);
                if (!t) {
                    return t;
                }
                result = result.has_value() ? decimal::union_type(*result, *t) : *t;
            }
            if (result.has_value()) {
                return *result;
            }
        }
    }
    return std::unexpected("cannot determine the Decimal precision and scale of this expression");
}

auto evaluate_row_count_expr_impl(const ir::Expr& expr, const ScalarRegistry* scalars,
                                  const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string> {
    auto value = evaluate_scalar_expr(expr, scalars, externs);
    if (!value) {
        return std::unexpected("row count expression: " + value.error());
    }
    if (is_null_scalar(value.value())) {
        return std::unexpected("row count expression is null; a count must be a present value");
    }
    if (const auto* i = std::get_if<std::int64_t>(&value.value())) {
        if (*i < 0) {
            return std::unexpected("row count expression must be non-negative");
        }
        return static_cast<std::size_t>(*i);
    }
    return std::unexpected("row count expression must evaluate to Int64");
}

// ── Column evaluation of calls ────────────────────────────────────────────────
//
// Every field is typed before any row runs (`infer_expr_type`), and a call is
// evaluated over columns: its arguments as columns over the field's rows, then
// the function applied element by element into a column of the call's type.
// Nulls are validity bits throughout.

auto cell_value(const ColResult& arg, std::size_t i) -> ScalarValue {
    const std::size_t at = arg.offset + i;
    if (const auto* validity = arg.get_validity(); validity != nullptr && !(*validity)[at]) {
        return ScalarValue{null_of_column(deref_col(arg))};
    }
    return std::visit(
        [&](const auto& column) -> ScalarValue {
            using ColType = std::decay_t<decltype(column)>;
            if constexpr (std::is_same_v<ColType, Column<Categorical>> ||
                          std::is_same_v<ColType, Column<std::string>>) {
                return std::string(column[at]);
            } else if constexpr (std::is_same_v<ColType, Column<Decimal>>) {
                return DecimalValue{.units = column[at].units, .type = decimal_type_of(column)};
            } else {
                return column[at];
            }
        },
        deref_col(arg));
}

auto is_row_local(const ir::Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef> || std::is_same_v<T, ir::Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                 std::is_same_v<T, ir::CompareExpr>) {
                return is_row_local(*node.left) && is_row_local(*node.right);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                return is_row_local(*node.left) &&
                       (node.right == nullptr || is_row_local(*node.right));
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return is_row_local(*node.operand);
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (const auto* fn = find_builtin(node.callee);
                    fn != nullptr && is_column_only(*fn)) {
                    return false;
                }
                return std::ranges::all_of(node.args,
                                           [](const auto& arg) { return is_row_local(*arg); });
            } else {
                return false;
            }
        },
        expr.node);
}

auto expr_calls_extern(const ir::Expr& expr, const ExternRegistry* externs) -> bool {
    if (externs == nullptr) {
        return false;
    }
    return std::visit(
        [&](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::BinaryExpr> || std::is_same_v<T, ir::CompareExpr>) {
                return expr_calls_extern(*node.left, externs) ||
                       expr_calls_extern(*node.right, externs);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                return expr_calls_extern(*node.left, externs) ||
                       (node.right != nullptr && expr_calls_extern(*node.right, externs));
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return expr_calls_extern(*node.operand, externs);
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (find_builtin(node.callee) == nullptr && externs->find(node.callee) != nullptr) {
                    return true;
                }
                return std::ranges::any_of(
                    node.args, [&](const auto& arg) { return expr_calls_extern(*arg, externs); });
            } else {
                return false;
            }
        },
        expr.node);
}

namespace {

// An empty column of the field's static type.
auto typed_column_for(ExprType type, const ir::Expr& expr, const Table& input,
                      const ScalarRegistry* scalars) -> std::expected<ColumnValue, std::string> {
    switch (type) {
        case ExprType::Int:
            return Column<std::int64_t>{};
        case ExprType::Double:
            return Column<double>{};
        case ExprType::Bool:
            return Column<bool>{};
        case ExprType::String:
            return Column<std::string>{};
        case ExprType::Categorical:
            return Column<Categorical>{};
        case ExprType::Date:
            return Column<Date>{};
        case ExprType::Timestamp:
            return Column<Timestamp>{};
        case ExprType::Decimal: {
            auto decimal = infer_decimal_type(expr, input, scalars);
            if (!decimal) {
                return std::unexpected(decimal.error());
            }
            return make_decimal_column(*decimal);
        }
    }
    return std::unexpected("evaluate_field: unknown expression type");
}

// Append a null row: a placeholder payload the validity bit masks.
void append_null(ColumnValue& column) {
    std::visit(
        [](auto& col) {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<typename ColType::value_type, std::string_view>) {
                col.push_back(std::string_view{});
            } else {
                col.push_back(typename ColType::value_type{});
            }
        },
        column);
}

// Append a present value to a column of the field's type (Int widens to
// Float, a Decimal is fitted to the column's precision and scale).
auto append_value(ColumnValue& column, const ScalarValue& value)
    -> std::expected<void, std::string> {
    // Fitting a value to the column's Decimal(p, s) can overflow, which is a
    // query error -- so it is handled here, where one can be returned.
    if (auto* dec_col = std::get_if<Column<Decimal>>(&column)) {
        auto units = fit_decimal(value, decimal_type_of(*dec_col));
        if (!units) {
            return std::unexpected(units.error());
        }
        dec_col->push_back(Decimal{*units});
        return {};
    }
    std::visit(
        [&](auto& col) {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value);
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(static_cast<std::int64_t>(*double_value));
                } else {
                    invariant_violation(
                        "evaluate_field: expected Int64-compatible expression value");
                }
            } else if constexpr (std::is_same_v<ValueType, double>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(static_cast<double>(*int_value));
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(*double_value);
                } else {
                    invariant_violation(
                        "evaluate_field: expected Float64-compatible expression value");
                }
            } else if constexpr (std::is_same_v<ValueType, bool>) {
                if (const auto* bool_value = std::get_if<bool>(&value)) {
                    col.push_back(*bool_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value != 0);
                } else {
                    invariant_violation(
                        "evaluate_field: expected Bool-compatible expression value");
                }
            } else if constexpr (std::is_same_v<ValueType, std::string_view>) {
                // Column<std::string>::value_type is std::string_view; the
                // ScalarValue holds an owned std::string, which push_back copies
                // into the column's arena.
                if (const auto* v = std::get_if<std::string>(&value)) {
                    col.push_back(*v);
                } else {
                    invariant_violation("evaluate_field: expected String expression value");
                }
            } else if constexpr (std::is_same_v<ValueType, Date>) {
                if (const auto* date_value = std::get_if<Date>(&value)) {
                    col.push_back(*date_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(int64_to_date_checked(*int_value));
                } else {
                    invariant_violation(
                        "evaluate_field: expected Date-compatible expression value");
                }
            } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                if (const auto* timestamp_value = std::get_if<Timestamp>(&value)) {
                    col.push_back(*timestamp_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(Timestamp{*int_value});
                } else {
                    invariant_violation(
                        "evaluate_field: expected Timestamp-compatible expression value");
                }
            }
        },
        column);
    return {};
}

auto is_null_call(const ir::Expr& expr) -> bool {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    return call != nullptr && call->callee == "__null" && call->args.empty();
}

// An argument column over some rows, read back by position. `by_row` when it
// was evaluated over every row of the field (indexed by row), otherwise over
// the selected rows only (indexed by position among them).
struct RowsValue {
    ColResult column;
    bool by_row;
};

// `expr` over the `rows` (relative to `range`) of the field. All of them: the
// field's own range. Some: a gather of just those rows of the columns `expr`
// reads, so an arm is evaluated only where it is taken -- unless it is not
// row-local, which evaluates over every row and is read back by row.
auto eval_over_rows(const ir::Expr& expr, const Table& input, RowRange range,
                    const std::vector<std::size_t>& rows, const ColumnEvalCtx& ctx)
    -> std::expected<RowsValue, std::string> {
    if (rows.size() == range.count || !is_row_local(expr)) {
        auto all = eval_value_vec(expr, input, ctx.scalars, range, ctx.window, ctx.window_aligned,
                                  ctx.externs);
        if (!all) {
            return std::unexpected(all.error());
        }
        return RowsValue{.column = std::move(*all), .by_row = true};
    }
    std::vector<ir::ColumnRef> refs;
    ir::collect_expr_columns(expr, refs);
    Table narrow;
    for (const auto& ref : refs) {
        if (ref.lexical || narrow.find(ref.name) != nullptr) {
            continue;
        }
        if (const auto* entry = input.find_entry(ref.name)) {
            narrow.add_column_from(ref.name, *entry);
        }
    }
    std::vector<std::uint64_t> absolute;
    absolute.reserve(rows.size());
    for (const std::size_t row : rows) {
        absolute.push_back(range.begin + row);
    }
    Table subset = gather_rows(narrow, absolute);
    subset.logical_rows = rows.size();
    auto some = eval_value_vec(expr, subset, ctx.scalars, RowRange::whole(rows.size()), ctx.window,
                               ctx.window_aligned, ctx.externs);
    if (!some) {
        return std::unexpected(some.error());
    }
    // A bare column comes back borrowed from `subset`, which ends here: own it.
    if (const auto* const* borrowed = std::get_if<const ColumnValue*>(&some->data)) {
        std::optional<ValidityBitmap> validity;
        if (const auto* v = some->get_validity()) {
            validity = *v;
        }
        ColResult owned{ColumnValue{**borrowed}, std::move(validity)};
        return RowsValue{.column = std::move(owned), .by_row = false};
    }
    return RowsValue{.column = std::move(*some), .by_row = false};
}

// `case` over columns. Arms are tried in order, and a row's later conditions
// and untaken values are never evaluated -- each condition runs over the rows
// no earlier arm took, each value over the rows that take it -- so an arm a
// row does not take cannot fail it or call an extern for it.
auto eval_case_columns(const ir::CallExpr& call, const ir::Expr& expr, const Table& input,
                       RowRange range, const ColumnEvalCtx& ctx, ExprType type)
    -> std::expected<ComputedColumn, std::string> {
    if (call.args.empty() || call.args.size() % 2 == 0) {
        return std::unexpected("case: expected condition/value arm pairs followed by else");
    }
    const std::size_t n = range.count;
    const std::size_t arms = call.args.size() / 2;  // the else arm is index `arms`
    std::vector<std::size_t> arm_of(n, arms);
    std::vector<std::size_t> open(n);
    // NOLINTNEXTLINE(modernize-use-ranges): Apple libc++ does not provide ranges::iota.
    std::iota(open.begin(), open.end(), std::size_t{0});
    for (std::size_t arm = 0; arm < arms && !open.empty(); ++arm) {
        auto condition = eval_over_rows(*call.args[2 * arm], input, range, open, ctx);
        if (!condition) {
            return std::unexpected(condition.error());
        }
        std::vector<std::size_t> still_open;
        for (std::size_t p = 0; p < open.size(); ++p) {
            const auto value = cell_value(condition->column, condition->by_row ? open[p] : p);
            // A null condition does not match (three-valued logic).
            if (const auto* matched = std::get_if<bool>(&value); matched != nullptr && *matched) {
                arm_of[open[p]] = arm;
            } else if (std::holds_alternative<bool>(value) ||
                       std::holds_alternative<NullScalar>(value)) {
                still_open.push_back(open[p]);
            } else {
                return std::unexpected("case: each condition must evaluate to Bool");
            }
        }
        open = std::move(still_open);
    }

    // Each arm's rows, and each row's position among them.
    std::vector<std::vector<std::size_t>> rows_of(arms + 1);
    std::vector<std::size_t> position(n);
    for (std::size_t row = 0; row < n; ++row) {
        position[row] = rows_of[arm_of[row]].size();
        rows_of[arm_of[row]].push_back(row);
    }
    std::vector<std::optional<RowsValue>> values(arms + 1);
    for (std::size_t arm = 0; arm <= arms; ++arm) {
        const ir::Expr& value = arm < arms ? *call.args[(2 * arm) + 1] : *call.args.back();
        if (rows_of[arm].empty() || is_null_call(value)) {
            continue;  // untaken, or a NULL arm: null of the case's type
        }
        auto evaluated = eval_over_rows(value, input, range, rows_of[arm], ctx);
        if (!evaluated) {
            return std::unexpected(evaluated.error());
        }
        values[arm] = std::move(*evaluated);
    }

    auto column = typed_column_for(type, expr, input, ctx.scalars);
    if (!column) {
        return std::unexpected(column.error());
    }
    std::visit([&](auto& col) { col.reserve(n); }, *column);
    ValidityBitmap validity(n, true);
    bool any_null = false;
    // A NULL arm, or an arm no row took: a null of the case's type.
    const ScalarValue case_null{null_of_column(*column)};
    for (std::size_t row = 0; row < n; ++row) {
        const auto& taken = values[arm_of[row]];
        const ScalarValue value =
            taken.has_value() ? cell_value(taken->column, taken->by_row ? row : position[row])
                              : case_null;
        if (std::holds_alternative<NullScalar>(value)) {
            append_null(*column);
            validity.set(row, false);
            any_null = true;
        } else if (auto appended = append_value(*column, value); !appended) {
            return std::unexpected(appended.error());
        }
    }
    return ComputedColumn{
        .column = std::move(*column),
        .validity = any_null ? std::optional<ValidityBitmap>{std::move(validity)} : std::nullopt};
}

// Unary math (sqrt, log, abs, floor, ...) over an evaluated argument column:
// one vectorized kernel rather than a call per row, whatever produced the
// argument. floor/ceil/trunc of an Int are the Int itself. nullopt for a
// shape it does not cover (Decimal `abs`), which the per-row path handles.
auto unary_math_over_column(UnaryMath fn, const ColResult& arg, std::size_t n, ExprType type)
    -> std::optional<ComputedColumn> {
    const auto& source = deref_col(arg);
    std::optional<ValidityBitmap> validity;
    if (const auto* v = arg.get_validity()) {
        validity = gather_validity(std::optional<ValidityBitmap>{*v}, arg.offset, arg.offset + n);
    }
    if (const auto* ints = std::get_if<Column<std::int64_t>>(&source)) {
        const std::int64_t* src = ints->data() + arg.offset;
        if (unary_math_is_type_preserving(fn)) {
            if (type != ExprType::Int) {
                return std::nullopt;
            }
            Column<std::int64_t> out;
            out.resize_for_overwrite(n);
            if (fn == UnaryMath::Abs) {
                for (std::size_t i = 0; i < n; ++i) {
                    out.data()[i] = safe_iabs(src[i]);
                }
            } else {
                std::copy_n(src, n, out.data());
            }
            return ComputedColumn{.column = std::move(out), .validity = std::move(validity)};
        }
        if (type != ExprType::Double) {
            return std::nullopt;
        }
        Column<double> out;
        out.resize_for_overwrite(n);
        for (std::size_t i = 0; i < n; ++i) {
            out.data()[i] = static_cast<double>(src[i]);
        }
        apply_unary_math(fn, out.data(), out.data(), n);
        return ComputedColumn{.column = std::move(out), .validity = std::move(validity)};
    }
    if (const auto* doubles = std::get_if<Column<double>>(&source);
        doubles != nullptr && type == ExprType::Double) {
        Column<double> out;
        out.resize_for_overwrite(n);
        apply_unary_math(fn, doubles->data() + arg.offset, out.data(), n);
        return ComputedColumn{.column = std::move(out), .validity = std::move(validity)};
    }
    return std::nullopt;
}

// Any other call over columns: a scalar builtin, `round`, or an extern.
auto eval_call_columns(const ir::CallExpr& call, const ir::Expr& expr, const Table& input,
                       RowRange range, const ColumnEvalCtx& ctx, ExprType type)
    -> std::expected<ComputedColumn, std::string> {
    if (call.callee == "__case") {
        return eval_case_columns(call, expr, input, range, ctx, type);
    }
    if (call.callee == "__null") {
        return std::unexpected(std::string(kUntypedNullError));
    }
    const auto* entry = find_builtin(call.callee);
    const auto* scalar_exec = entry != nullptr ? std::get_if<ScalarExec>(&entry->exec) : nullptr;
    if (scalar_exec != nullptr && scalar_exec->eval == nullptr) {
        scalar_exec = nullptr;
    }
    std::optional<RoundMode> mode;
    // An extern is called per row, in order, over its argument columns.
    std::optional<std::vector<ScalarValue>> extern_values;
    if (scalar_exec != nullptr) {
        const auto argc = static_cast<int>(call.args.size());
        if (argc < entry->min_args || (entry->max_args >= 0 && argc > entry->max_args)) {
            return std::unexpected(call.callee + ": wrong number of arguments");
        }
    } else if (call.callee == "round") {
        // round(x, mode): the mode is a RoundMode member, fixed for the call.
        if (call.args.size() != 2) {
            return std::unexpected("round: expected 2 arguments (value, mode)");
        }
        auto parsed = extract_ir_round_mode(*call.args[1]);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        mode = *parsed;
    } else {
        if (ctx.externs == nullptr || ctx.externs->find(call.callee) == nullptr) {
            return std::unexpected("unknown function in expression: " + call.callee);
        }
        auto values = eval_extern_values(call, input, range, ctx);
        if (!values) {
            return std::unexpected(values.error());
        }
        extern_values = std::move(*values);
    }

    const std::size_t n = range.count;
    // The arguments, as columns over the field's rows. A bare NULL argument is
    // null in every row; round's mode is not a value.
    std::vector<std::optional<ColResult>> args;
    args.reserve(call.args.size());
    for (std::size_t a = 0; a < call.args.size(); ++a) {
        if ((mode.has_value() && a == 1) || is_null_call(*call.args[a]) ||
            extern_values.has_value()) {
            args.emplace_back();
            continue;
        }
        auto column = eval_value_vec(*call.args[a], input, ctx.scalars, range, ctx.window,
                                     ctx.window_aligned, ctx.externs);
        if (!column) {
            return std::unexpected(column.error());
        }
        args.emplace_back(std::move(*column));
    }
    if (const auto unary = call.args.size() == 1 ? lookup_unary_math(call.callee) : std::nullopt) {
        if (const auto& first = args.front(); first.has_value()) {
            if (auto applied = unary_math_over_column(*unary, *first, n, type)) {
                return std::move(*applied);
            }
        }
    }

    auto column = typed_column_for(type, expr, input, ctx.scalars);
    if (!column) {
        return std::unexpected(column.error());
    }
    std::visit([&](auto& col) { col.reserve(n); }, *column);
    // A null of the call's type: what a bare NULL argument is, and the result
    // until the function says otherwise.
    const ScalarValue call_null{null_of_column(*column)};
    const auto arg_at = [&](std::size_t a, std::size_t row) -> ScalarValue {
        return args[a].has_value() ? cell_value(*args[a], row) : call_null;
    };
    ValidityBitmap validity(n, true);
    bool any_null = false;
    std::vector<ScalarValue> row_args;
    row_args.reserve(call.args.size());
    for (std::size_t row = 0; row < n; ++row) {
        std::expected<ScalarValue, std::string> value = call_null;
        if (scalar_exec != nullptr) {
            // NullPolicy::Propagate, applied centrally: a null argument nulls
            // the call, so `eval` never sees one. Handles entries (coalesce,
            // fill_null, null_if_*) receive the null and decide.
            bool null_argument = false;
            row_args.clear();
            for (std::size_t a = 0; a < call.args.size(); ++a) {
                row_args.push_back(arg_at(a, row));
                null_argument = null_argument || is_null_scalar(row_args.back());
            }
            if (!null_argument || entry->null_policy != NullPolicy::Propagate) {
                value = scalar_exec->eval(call.callee, row_args);
            }
        } else if (mode.has_value()) {
            const auto x = arg_at(0, row);
            if (const auto* decimal_value = std::get_if<DecimalValue>(&x)) {
                auto rounded = apply_decimal_round(*decimal_value, *mode);
                if (!rounded) {
                    return std::unexpected(rounded.error());
                }
                value = ScalarValue{*rounded};
            } else if (const auto* integer = std::get_if<std::int64_t>(&x)) {
                value = ScalarValue{*integer};
            } else if (!std::holds_alternative<NullScalar>(x)) {
                auto d = expr_value_to_double(x);
                if (!d) {
                    return std::unexpected("round: first argument must be numeric");
                }
                value = ScalarValue{apply_round(*d, *mode)};
            }
        } else if (extern_values.has_value()) {
            value = std::move((*extern_values)[row]);
        }
        if (!value) {
            return std::unexpected(value.error());
        }
        if (std::holds_alternative<NullScalar>(*value)) {
            append_null(*column);
            validity.set(row, false);
            any_null = true;
        } else if (auto appended = append_value(*column, *value); !appended) {
            return std::unexpected(appended.error());
        }
    }
    return ComputedColumn{
        .column = std::move(*column),
        .validity = any_null ? std::optional<ValidityBitmap>{std::move(validity)} : std::nullopt};
}

}  // namespace

// The single field-expression evaluator: every field/value dispatch runs
// through here exactly once, typed before any row --
//   1. a top-level whole-column builtin (Transform/Generator) via the
//      registry's `column_eval`, with a row-count guard so a short generator
//      (rep's length_out) errors instead of reading out of bounds;
//   2. the fused numeric tree;
//   3. a call over its argument columns (eval_call_columns);
//   4. the vectorized, validity-aware evaluator for everything else.
// update_table and windowed_update_table (and through them the grouped,
// guarded, and chunked variants), scalars, `map`, and evaluate_field_column all
// dispatch here.
auto evaluate_field(const ir::Expr& expr, const Table& input, RowRange range,
                    const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string> {
    const std::size_t table_rows = input.rows();
    const bool whole = range.is_whole(table_rows);
    // Row count of the *result*. The paths that honour a partial range produce
    // exactly this many rows; the ones that do not are guarded below.
    const std::size_t rows = range.count;
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        // A row-local builtin with a column kernel (a cast, `like`, coalesce,
        // ...) over a partial range: its arguments are columns and literals, so
        // the kernel runs over just the columns it reads, sliced to the range.
        if (const auto* fn = find_builtin(call->callee);
            fn != nullptr && !whole && use_column_kernel(*fn, *call) && !is_column_only(*fn)) {
            std::vector<ir::ColumnRef> refs;
            ir::collect_expr_columns(expr, refs);
            Table slice;
            for (const auto& ref : refs) {
                if (ref.lexical || slice.find(ref.name) != nullptr) {
                    continue;
                }
                if (const auto* entry = input.find_entry(ref.name)) {
                    auto column = gather_range(*entry->column, range.begin, range.end());
                    if (auto validity =
                            gather_validity(entry->validity, range.begin, range.end())) {
                        slice.add_column(ref.name, std::move(column), std::move(*validity));
                    } else {
                        slice.add_column(ref.name, std::move(column));
                    }
                }
            }
            slice.logical_rows = rows;
            return column_eval_of(*fn)(*call, slice, rows, ctx);
        }
        if (const auto* fn = find_builtin(call->callee);
            fn != nullptr && use_column_kernel(*fn, *call)) {
            // A whole-column builtin (rolling/cum/lag, generators) reads
            // neighbouring rows or the whole frame, so a range would change its
            // answer rather than just its cost. These are permanently
            // non-range-native, not merely un-threaded yet.
            if (!whole) {
                invariant_violation(
                    "evaluate_field: whole-column builtin under a partial row range — "
                    "is_range_native_expr must never admit a column-kernel call");
            }
            auto col = column_eval_of(*fn)(*call, input, rows, ctx);
            if (!col) {
                return col;
            }
            // A generator told to produce fewer rows than the frame (rep's
            // length_out) must error, not leave a short column whose missing
            // tail reads out of bounds. rows == 0 is the data-gen bootstrap:
            // a generator field on an empty frame grows it.
            const std::size_t got = std::visit([](const auto& c) { return c.size(); }, col->column);
            if (rows != 0 && got != rows) {
                return std::unexpected(call->callee + ": generates " + std::to_string(got) +
                                       " rows but the frame has " + std::to_string(rows));
            }
            return col;
        }
    }
    auto inferred = infer_expr_type(expr, input, ctx.scalars, ctx.externs);
    if (!inferred) {
        return std::unexpected(inferred.error());
    }
    // The fused numeric tree is range-aware: its leaves advance their base
    // pointers by `range.begin`. It declines anything it cannot compile.
    if (auto fast = try_fast_update_numeric_expr(expr, input, range, inferred.value(), ctx.scalars);
        fast.has_value()) {
        return ComputedColumn{.column = std::move(fast.value()),
                              .validity = collect_expr_validity(expr, input, range)};
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        return eval_call_columns(*call, expr, input, range, ctx, inferred.value());
    }
    // Everything else -- a column, a literal, arithmetic, a comparison, logic,
    // `is null` -- is the vectorized, validity-aware evaluator's.
    auto res = eval_value_vec(expr, input, ctx.scalars, range, ctx.window, ctx.window_aligned,
                              ctx.externs);
    if (!res) {
        return std::unexpected(res.error());
    }
    // A bare column comes back borrowed, at the range's offset: copy its rows.
    if (const auto* const* borrowed = std::get_if<const ColumnValue*>(&res->data)) {
        std::optional<ValidityBitmap> validity;
        if (const auto* v = res->get_validity()) {
            validity =
                gather_validity(std::optional<ValidityBitmap>{*v}, res->offset, res->offset + rows);
        }
        return ComputedColumn{.column = gather_range(**borrowed, res->offset, res->offset + rows),
                              .validity = std::move(validity)};
    }
    std::optional<ValidityBitmap> validity;
    if (const auto* v = res->get_validity()) {
        validity = *v;
    }
    return ComputedColumn{.column = std::move(std::get<ColumnValue>(res->data)),
                          .validity = std::move(validity)};
}

// Produce a shifted copy of a column: lag(col, n)[i] = col[i-n], lead(col, n)[i] = col[i+n].
// Out-of-bounds entries are filled with type-appropriate zero/default values.
// LagLeadResult is declared up top (before eval_value_vec, which also calls this).
auto eval_lag_lead_column(const ir::CallExpr& call, const Table& input, bool is_lag,
                          const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<LagLeadResult, std::string> {
    const std::string fname = is_lag ? "lag" : "lead";
    if (call.args.size() != 2) {
        return std::unexpected(fname + ": expected 2 arguments");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected(fname + ": first argument must be a column name");
    }
    auto offset_value = evaluate_scalar_expr(*call.args[1], scalars, externs);
    if (!offset_value) {
        return std::unexpected(fname + ": " + offset_value.error());
    }
    const auto* offset_val = std::get_if<std::int64_t>(&offset_value.value());
    if (offset_val == nullptr || *offset_val < 0) {
        return std::unexpected(fname + ": second argument must evaluate to a non-negative Int64");
    }
    const auto* src = input.find(col_ref->name);
    if (!src) {
        return std::unexpected(fname + ": unknown column '" + col_ref->name + "'");
    }
    auto n = static_cast<std::size_t>(*offset_val);
    std::size_t rows = input.rows();
    LagLeadResult result;
    result.column = std::visit(
        [&](const auto& col) -> ColumnValue {
            using ColT = std::decay_t<decltype(col)>;
            ColT out;
            // A shifted column means what its source means: a Decimal keeps
            // its scale, a Timestamp its zone.
            out.set_meta(col.meta());
            if constexpr (std::is_same_v<ColT, Column<Categorical>> ||
                          std::is_same_v<ColT, Column<std::string>>) {
                // Categorical/string: element-wise fallback (no plain memcpy).
                out.reserve(rows);
                auto push_default = [&] {
                    if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                        out.push_back(std::string_view{});
                    } else {
                        using T = ColT::value_type;
                        out.push_back(T{});
                    }
                };
                if (is_lag) {
                    for (std::size_t i = 0; i < rows; ++i) {
                        if (i < n) {
                            push_default();
                        } else {
                            out.push_back(col[i - n]);
                        }
                    }
                } else {
                    for (std::size_t i = 0; i < rows; ++i) {
                        if (i + n >= rows) {
                            push_default();
                        } else {
                            out.push_back(col[i + n]);
                        }
                    }
                }
            } else {
                // POD column: bulk-copy the shifted region and default only
                // the `n` boundary rows (null below), rather than zero-filling
                // the whole column first.
                using T = ColT::value_type;
                if constexpr (!std::is_same_v<ColT, Column<bool>> &&
                              ::ibex::detail::overwrite_safe_v<T>) {
                    out.resize_for_overwrite(rows);
                    const std::size_t edge = std::min(n, rows);
                    std::fill_n(is_lag ? out.data() : out.data() + (rows - edge), edge, T{});
                } else {
                    out.resize(rows);  // zero-initialises
                }
                if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    if (is_lag) {
                        for (std::size_t i = n; i < rows; ++i) {
                            out.set(i, col[i - n]);
                        }
                    } else {
                        for (std::size_t i = 0; i + n < rows; ++i) {
                            out.set(i, col[i + n]);
                        }
                    }
                } else {
                    if (is_lag) {
                        if (n < rows)
                            std::memcpy(out.data() + n, col.data(), (rows - n) * sizeof(T));
                    } else {
                        if (n < rows)
                            std::memcpy(out.data(), col.data() + n, (rows - n) * sizeof(T));
                    }
                }
            }
            return out;
        },
        *src);

    // Mark the out-of-bounds rows null. For lag, that's the first `n` rows;
    // for lead, the last `n`. Without this every consumer would have to know
    // that `lag(x, k)` returns 0/empty for the boundary rather than null —
    // and `(close - lag(close, 1)) / lag(close, 1)` would silently produce a
    // meaningful-looking number for the first row of each group.
    if (n > 0 && rows > 0) {
        ValidityBitmap bm(rows, true);
        const std::size_t bad = std::min(n, rows);
        if (is_lag) {
            for (std::size_t i = 0; i < bad; ++i) {
                bm.set(i, false);
            }
        } else {
            for (std::size_t i = rows - bad; i < rows; ++i) {
                bm.set(i, false);
            }
        }
        result.validity = std::move(bm);
    }
    return result;
}

// Compute a cumulative sum or product column.
// cumsum(col)[i] = col[0] + col[1] + ... + col[i]  (identity: 0 for sum, 1 for product)
// cumprod(col)[i] = col[0] * col[1] * ... * col[i]
// Only valid for numeric (Int / Float) columns.
auto eval_cumsum_cumprod_column(const ir::CallExpr& call, const Table& input, bool is_prod)
    -> std::expected<ComputedColumn, std::string> {
    const std::string fname = is_prod ? "cumprod" : "cumsum";
    if (call.args.size() != 1) {
        return std::unexpected(fname + ": expected 1 argument");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected(fname + ": argument must be a column name");
    }
    const auto* src = input.find(col_ref->name);
    if (!src) {
        return std::unexpected(fname + ": unknown column '" + col_ref->name + "'");
    }
    std::size_t rows = input.rows();
    if (const auto* decimal_col = std::get_if<Column<Decimal>>(src)) {
        if (is_prod) {
            return std::unexpected("cumprod: Decimal result precision and scale are not fixed");
        }
        const DecimalType input_type = decimal_type_of(*decimal_col);
        const DecimalType result_type = decimal::sum_result_type(input_type);
        Column<Decimal> result = make_decimal_column(result_type);
        result.resize(rows);
        const auto* entry = input.find_entry(col_ref->name);
        const ValidityBitmap* source_validity =
            entry != nullptr && entry->validity.has_value() ? &*entry->validity : nullptr;
        std::optional<ValidityBitmap> result_validity;
        if (source_validity != nullptr) {
            result_validity.emplace(rows, true);
        }
        Int128 sum = 0;
        const Decimal* values = decimal_col->data();
        Decimal* output = result.data();
        for (std::size_t i = 0; i < rows; ++i) {
            if (source_validity != nullptr && !(*source_validity)[i]) {
                output[i] = Decimal{};
                if (result_validity.has_value()) {
                    result_validity->set(i, false);
                }
                continue;
            }
            Int128 next = 0;
            if (!decimal::checked_add(sum, values[i].units, next) ||
                !decimal::fits(next, result_type.precision)) {
                return std::unexpected("cumsum(" + col_ref->name +
                                       "): decimal overflow: "
                                       "result does not fit " +
                                       decimal::type_name(result_type));
            }
            sum = next;
            output[i] = Decimal{sum};
        }
        return ComputedColumn{.column = ColumnValue{std::move(result)},
                              .validity = std::move(result_validity)};
    }
    return std::visit(
        [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = ColT::value_type;
            if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>) {
                // Both ends are hoisted out of the loop on purpose. A Column
                // may hold adopted (Arrow) storage, so every `col[i]` and every
                // `push_back` would otherwise re-test which storage is in use —
                // two branches per row, against ~1 cycle of real work. Resolving
                // the pointers once costs the same branch twice in total.
                ColT result;
                // Every slot is written below, so skip the value-initialising
                // fill `resize` would do — that fill is a second full pass over
                // the output, as expensive as the computation itself.
                result.resize_for_overwrite(rows);
                const T* in = col.data();
                T* out = result.data();
                T acc = is_prod ? T{1} : T{0};
                for (std::size_t i = 0; i < rows; ++i) {
                    if (is_prod) {
                        acc *= in[i];
                    } else {
                        acc += in[i];
                    }
                    out[i] = acc;
                }
                return ComputedColumn{.column = ColumnValue{std::move(result)},
                                      .validity = std::nullopt};
            } else {
                return std::unexpected(fname + ": column must be numeric (Int or Float)");
            }
        },
        *src);
}

// ─── Null-fill functions ──────────────────────────────────────────────────────
//
// fill_null(col, value)  — constant fill: replace every null cell with `value`
// fill_forward(col)      — LOCF: carry the last valid value forward
// fill_backward(col)     — NOCB: carry the next valid value backward
//
// fill_forward/fill_backward leave unfillable leading/trailing nulls as null.
// fill_null produces a column with no validity bitmap (all rows are valid).

/// LOCF (`forward`) or NOCB over a dense column in parallel row ranges: each
/// range finds its last (first) valid row, a scan over the ranges gives each
/// one the carry from the nearest earlier (later) range that has a value, and
/// every range then fills on its own. Rows with nothing to carry -- before the
/// first valid row (after the last) -- get T{}; their nulls are the caller's,
/// from the returned edge: the first valid row for LOCF, one past the last for
/// NOCB (`rows` / 0 when there is none).
template <typename T, typename Valid>
auto fill_carry_ranges(const T* in, T* dst, std::size_t rows, const Valid& is_valid,
                       std::size_t ranges, bool forward) -> std::size_t {
    constexpr std::size_t kNone = std::numeric_limits<std::size_t>::max();
    const std::size_t grain = (rows + ranges - 1) / ranges;
    std::vector<std::size_t> edge(ranges, kNone);   // last (LOCF) / first (NOCB) valid
    std::vector<std::size_t> first(ranges, kNone);  // first valid (LOCF edge)
    std::vector<std::size_t> last(ranges, kNone);   // last valid (NOCB edge)
    const auto run = [&](const auto& body) {
        auto batch = process_worker_pool().submit(ranges, [&](std::size_t r) {
            const std::size_t b = std::min(rows, r * grain);
            body(r, b, std::min(rows, b + grain));
        });
        batch.wait();
    };
    run([&](std::size_t r, std::size_t b, std::size_t e) {
        for (std::size_t i = b; i < e; ++i) {
            if (is_valid(i)) {
                first[r] = i;
                break;
            }
        }
        for (std::size_t i = e; i-- > b;) {
            if (is_valid(i)) {
                last[r] = i;
                break;
            }
        }
        edge[r] = forward ? last[r] : first[r];
    });
    // carry[r]: the row whose value range r starts from, or kNone.
    std::vector<std::size_t> carry(ranges, kNone);
    if (forward) {
        for (std::size_t r = 1; r < ranges; ++r) {
            carry[r] = edge[r - 1] != kNone ? edge[r - 1] : carry[r - 1];
        }
    } else {
        for (std::size_t r = ranges - 1; r-- > 0;) {
            carry[r] = edge[r + 1] != kNone ? edge[r + 1] : carry[r + 1];
        }
    }
    run([&](std::size_t r, std::size_t b, std::size_t e) {
        bool have = carry[r] != kNone;
        T value = have ? in[carry[r]] : T{};
        if (forward) {
            for (std::size_t i = b; i < e; ++i) {
                if (is_valid(i)) {
                    value = in[i];
                    have = true;
                }
                dst[i] = have ? value : T{};
            }
        } else {
            for (std::size_t i = e; i-- > b;) {
                if (is_valid(i)) {
                    value = in[i];
                    have = true;
                }
                dst[i] = have ? value : T{};
            }
        }
    });
    if (forward) {
        for (std::size_t r = 0; r < ranges; ++r) {
            if (first[r] != kNone) {
                return first[r];
            }
        }
        return rows;
    }
    for (std::size_t r = ranges; r-- > 0;) {
        if (last[r] != kNone) {
            return last[r] + 1;
        }
    }
    return 0;
}

// fill_null(col, value): replace every null cell with the scalar `value`.
// Accepts any column type; `value` must be a literal matching the column type.
// Returns a column with no validity bitmap.
auto eval_fill_null(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string> {
    if (call.args.size() != 2) {
        return std::unexpected("fill_null: expected 2 arguments (col, value)");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected("fill_null: first argument must be a column name");
    }
    const auto* entry = input.find_entry(col_ref->name);
    if (!entry) {
        return std::unexpected("fill_null: unknown column '" + col_ref->name + "'");
    }
    const auto* fill_lit = std::get_if<ir::Literal>(&call.args[1]->node);
    if (!fill_lit) {
        return std::unexpected("fill_null: second argument must be a literal value");
    }

    std::size_t rows = input.rows();
    const bool has_validity = entry->validity.has_value();

    return std::visit(
        [&](const auto& col) -> std::expected<FillResult, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = ColT::value_type;

            // Extract the fill value of type T from the literal using std::get_if.
            // Each branch is a constexpr-guarded check on a specific alternative.
            std::optional<T> maybe_fill;
            if constexpr (std::is_same_v<T, std::int64_t>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&fill_lit->value))
                    maybe_fill = *int_value;
                else if (const auto* double_value = std::get_if<double>(&fill_lit->value))
                    maybe_fill = static_cast<std::int64_t>(*double_value);
            } else if constexpr (std::is_same_v<T, double>) {
                if (const auto* double_value = std::get_if<double>(&fill_lit->value))
                    maybe_fill = *double_value;
                else if (const auto* int_value = std::get_if<std::int64_t>(&fill_lit->value))
                    maybe_fill = static_cast<double>(*int_value);
            } else if constexpr (std::is_same_v<T, bool>) {
                if (const auto* v = std::get_if<bool>(&fill_lit->value))
                    maybe_fill = *v;
            } else if constexpr (std::is_same_v<T, std::string_view>) {
                // Covers both Column<std::string> and Column<Categorical>.
                // The literal's std::string lives as long as the IR tree.
                if (const auto* v = std::get_if<std::string>(&fill_lit->value))
                    maybe_fill = std::string_view(*v);
            } else if constexpr (std::is_same_v<T, Date>) {
                if (const auto* v = std::get_if<Date>(&fill_lit->value))
                    maybe_fill = *v;
            } else if constexpr (std::is_same_v<T, Timestamp>) {
                if (const auto* v = std::get_if<Timestamp>(&fill_lit->value))
                    maybe_fill = *v;
            } else if constexpr (std::is_same_v<T, Decimal>) {
                // A decimal or integer literal, fitted to the column's own
                // Decimal(p, s): the fill must not change the column's type.
                if (std::holds_alternative<DecimalValue>(fill_lit->value) ||
                    std::holds_alternative<std::int64_t>(fill_lit->value)) {
                    auto units = fit_decimal(scalar_from_literal(*fill_lit), decimal_type_of(col));
                    if (!units) {
                        return std::unexpected("fill_null: " + units.error());
                    }
                    maybe_fill = Decimal{*units};
                }
            }

            if (!maybe_fill) {
                return std::unexpected(
                    "fill_null: fill value type does not match column type for '" + col_ref->name +
                    "'");
            }
            const T fill_val = *maybe_fill;

            ColT result;
            // Filling nulls does not change what the values mean: a Decimal
            // keeps its scale, a Timestamp its zone.
            result.set_meta(col.meta());
            ColumnAppender<ColT> out(result, rows);
            for (std::size_t i = 0; i < rows; ++i) {
                const bool is_null_row = has_validity && !(*entry->validity)[i];
                out.push(is_null_row ? fill_val : col[i]);
            }
            // All rows are now valid (nulls replaced).
            return FillResult{std::move(result), std::nullopt};
        },
        *entry->column);
}

// fill_forward(col): LOCF — carry the last valid (non-null) value forward.
// Unfillable leading nulls (no prior valid value) remain null.
auto eval_fill_forward(const ir::CallExpr& call, const Table& input, const ExecutionContext* exec)
    -> std::expected<FillResult, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected("fill_forward: expected 1 argument (col)");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected("fill_forward: argument must be a column name");
    }
    const auto* entry = input.find_entry(col_ref->name);
    if (!entry) {
        return std::unexpected("fill_forward: unknown column '" + col_ref->name + "'");
    }

    // If there's no validity bitmap, no nulls exist — return the column unchanged.
    if (!entry->validity.has_value()) {
        return FillResult{.column = *entry->column, .validity = std::nullopt};
    }

    std::size_t rows = input.rows();
    const auto& validity = *entry->validity;

    return std::visit(
        [&](const auto& col) -> std::expected<FillResult, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = ColT::value_type;
            ColT result;
            ColumnAppender<ColT> out(result, rows);
            std::optional<ValidityBitmap> out_validity;

            if constexpr (is_dense_column_v<ColT>) {
                // The input storage is resolved once, here, and the carry lives in
                // a register. Reading through `col[i]` and `validity[i]` re-tests
                // "is this adopted Arrow storage?" and reloads both buffer
                // pointers on every row, because the store to the output may
                // alias the members they read -- about twenty instructions and a
                // spilled carry per row for one load, one bit test and one store.
                const T* const in = col.data();
                T* const dst = out.data();
                const std::uint8_t* const bits = validity.buffer_data();
                const std::size_t bit_base = validity.buffer_offset();
                const auto is_valid = [bits, bit_base](std::size_t row) {
                    const std::size_t bit = bit_base + row;
                    return ((bits[bit >> 3] >> (bit & 7U)) & 1U) != 0U;
                };

                const std::size_t workers =
                    exec != nullptr ? group_barrier_worker_count(*exec, rows) : 0;
                if (workers >= 2) {
                    const std::size_t lead = fill_carry_ranges(in, dst, rows, is_valid, workers,
                                                               /*forward=*/true);
                    if (lead > 0) {
                        out_validity.emplace(rows, true);
                        for (std::size_t row = 0; row < lead; ++row) {
                            out_validity->set(row, false);
                        }
                    }
                    return FillResult{std::move(result), std::move(out_validity)};
                }
                // Leading nulls have no value to carry; they stay null.
                std::size_t i = 0;
                while (i < rows && !is_valid(i)) {
                    dst[i] = T{};
                    ++i;
                }
                if (i > 0) {
                    out_validity.emplace(rows, true);
                    for (std::size_t row = 0; row < i; ++row) {
                        out_validity->set(row, false);
                    }
                }
                if (i < rows) {
                    T carry = in[i];
                    for (; i < rows; ++i) {
                        carry = is_valid(i) ? in[i] : carry;
                        dst[i] = carry;
                    }
                }
            } else {
                // carry: last seen valid value (safe for string_view: points into source col).
                T carry{};
                bool have_carry = false;

                for (std::size_t i = 0; i < rows; ++i) {
                    if (validity[i]) {
                        out.push(col[i]);
                        carry = col[i];
                        have_carry = true;
                    } else if (have_carry) {
                        out.push(carry);
                    } else {
                        // Leading null — no value to carry; stays null.
                        out.push(T{});
                        if (!out_validity) {
                            out_validity.emplace(rows, true);
                        }
                        out_validity->set(i, false);
                    }
                }
            }
            return FillResult{std::move(result), std::move(out_validity)};
        },
        *entry->column);
}

// fill_backward(col): NOCB — carry the next valid (non-null) value backward.
// Unfillable trailing nulls (no subsequent valid value) remain null.
auto eval_fill_backward(const ir::CallExpr& call, const Table& input, const ExecutionContext* exec)
    -> std::expected<FillResult, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected("fill_backward: expected 1 argument (col)");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected("fill_backward: argument must be a column name");
    }
    const auto* entry = input.find_entry(col_ref->name);
    if (!entry) {
        return std::unexpected("fill_backward: unknown column '" + col_ref->name + "'");
    }

    // If there's no validity bitmap, no nulls exist — return the column unchanged.
    if (!entry->validity.has_value()) {
        return FillResult{.column = *entry->column, .validity = std::nullopt};
    }

    std::size_t rows = input.rows();
    const auto& validity = *entry->validity;

    return std::visit(
        [&](const auto& col) -> std::expected<FillResult, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = ColT::value_type;

            std::optional<ValidityBitmap> out_validity;

            if constexpr (is_dense_column_v<ColT>) {
                // One right-to-left pass straight into the output, with the
                // storage resolved once; see eval_fill_forward.
                ColT result;
                ColumnAppender<ColT> out(result, rows);
                const T* const in = col.data();
                T* const dst = out.data();
                const std::uint8_t* const bits = validity.buffer_data();
                const std::size_t bit_base = validity.buffer_offset();
                const auto is_valid = [bits, bit_base](std::size_t row) {
                    const std::size_t bit = bit_base + row;
                    return ((bits[bit >> 3] >> (bit & 7U)) & 1U) != 0U;
                };

                const std::size_t workers =
                    exec != nullptr ? group_barrier_worker_count(*exec, rows) : 0;
                if (workers >= 2) {
                    const std::size_t trail = fill_carry_ranges(in, dst, rows, is_valid, workers,
                                                                /*forward=*/false);
                    if (trail < rows) {
                        out_validity.emplace(rows, true);
                        for (std::size_t row = trail; row < rows; ++row) {
                            out_validity->set(row, false);
                        }
                    }
                    return FillResult{std::move(result), std::move(out_validity)};
                }
                // Trailing nulls have no following value; they stay null.
                std::size_t end = rows;
                while (end > 0 && !is_valid(end - 1)) {
                    dst[--end] = T{};
                }
                if (end < rows) {
                    out_validity.emplace(rows, true);
                    for (std::size_t row = end; row < rows; ++row) {
                        out_validity->set(row, false);
                    }
                }
                if (end > 0) {
                    T next = in[end - 1];
                    for (std::size_t i = end; i-- > 0;) {
                        next = is_valid(i) ? in[i] : next;
                        dst[i] = next;
                    }
                }
                return FillResult{std::move(result), std::move(out_validity)};
            } else {
                // Scan right-to-left to compute (value, valid) for each row,
                // storing in plain vectors so we can then push_back into ColT.
                std::vector<T> vals(rows);

                bool have_val = false;
                T next_val{};
                for (std::size_t ri = 0; ri < rows; ++ri) {
                    const std::size_t i = rows - 1 - ri;
                    if (validity[i]) {
                        vals[i] = col[i];
                        next_val = col[i];
                        have_val = true;
                    } else if (have_val) {
                        vals[i] = next_val;
                    } else {
                        // Trailing null — no following value; stays null.
                        vals[i] = T{};
                        if (!out_validity) {
                            out_validity.emplace(rows, true);
                        }
                        out_validity->set(i, false);
                    }
                }
                // Build the output column, resolving its storage once.
                ColT result;
                ColumnAppender<ColT> out(result, rows);
                for (std::size_t i = 0; i < rows; ++i) {
                    out.push(vals[i]);
                }
                return FillResult{std::move(result), std::move(out_validity)};
            }
        },
        *entry->column);
}

auto eval_float_clean(const ir::CallExpr& call, const Table& input, FloatCleanMode mode)
    -> std::expected<FillResult, std::string> {
    const std::string_view fname =
        mode == FloatCleanMode::NullIfNan ? "null_if_nan" : "null_if_not_finite";
    if (call.args.size() != 1) {
        return std::unexpected(std::string(fname) + ": expected 1 argument (col)");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected(std::string(fname) + ": argument must be a column name");
    }
    const auto* entry = input.find_entry(col_ref->name);
    if (!entry) {
        return std::unexpected(std::string(fname) + ": unknown column '" + col_ref->name + "'");
    }
    const auto* src = std::get_if<Column<double>>(entry->column.get());
    if (src == nullptr) {
        return std::unexpected(std::string(fname) + ": column must be Float64");
    }

    Column<double> result = *src;
    std::optional<ValidityBitmap> validity;
    if (entry->validity.has_value()) {
        validity = *entry->validity;
    }

    bool changed = false;
    for (std::size_t i = 0; i < result.size(); ++i) {
        const bool is_valid = !validity.has_value() || (*validity)[i];
        if (!is_valid) {
            continue;
        }
        const double value = result[i];
        const bool should_null =
            mode == FloatCleanMode::NullIfNan ? std::isnan(value) : !std::isfinite(value);
        if (!should_null) {
            continue;
        }
        if (!validity.has_value()) {
            validity.emplace(result.size(), true);
        }
        validity->set(i, false);
        changed = true;
    }

    if (!changed && !entry->validity.has_value()) {
        validity.reset();
    }
    return FillResult{.column = ColumnValue{std::move(result)}, .validity = std::move(validity)};
}

// ─── Vectorized RNG ───────────────────────────────────────────────────────────

namespace {
constexpr auto rng_func_returns_int(std::string_view name) -> bool {
    return name == "rand_bernoulli" || name == "rand_poisson" || name == "rand_int";
}
}  // namespace

// get_rng() is defined in rng.hpp (returns thread-local Xoshiro256pp).

namespace {

// Extract a numeric parameter from a CallExpr argument at `pos`.
// Returns the value as double; the caller is responsible for range validation.
auto extract_rng_param(const ir::CallExpr& call, std::size_t pos, std::string_view func_name)
    -> std::expected<double, std::string> {
    if (pos >= call.args.size()) {
        return std::unexpected(std::string(func_name) + ": missing argument at position " +
                               std::to_string(pos));
    }
    const auto* lit = std::get_if<ir::Literal>(&call.args[pos]->node);
    if (!lit) {
        return std::unexpected(std::string(func_name) + ": argument " + std::to_string(pos) +
                               " must be a numeric literal");
    }
    return std::visit(
        [&](const auto& v) -> std::expected<double, std::string> {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return static_cast<double>(v);
            } else if constexpr (std::is_same_v<T, double>) {
                return v;
            } else {
                return std::unexpected(std::string(func_name) + ": argument " +
                                       std::to_string(pos) + " must be numeric");
            }
        },
        lit->value);
}

}  // namespace

// Generate a full column of `rows` independent draws from the named distribution.
//
//   rand_uniform(low, high)          – Uniform[low, high)            → Float
//   rand_normal(mean, stddev)        – Normal(mean, stddev²)          → Float
//   rand_student_t(df)               – Student-t(df)                  → Float
//   rand_gamma(shape, scale)         – Gamma(shape, scale)            → Float
//   rand_exponential(lambda)         – Exponential(1/lambda)          → Float
//   rand_bernoulli(p)                – Bernoulli(p)  → 0 or 1         → Int
//   rand_poisson(lambda)             – Poisson(lambda)                → Int
//   rand_int(lo, hi)                 – Uniform integer [lo, hi]       → Int
//
auto apply_rng_func(const ir::CallExpr& call, std::size_t rows)
    -> std::expected<ColumnValue, std::string> {
    auto& rng = get_rng();
    const auto& name = call.callee;

    if (name == "rand_uniform") {
        if (call.args.size() != 2) {
            return std::unexpected("rand_uniform: expected 2 arguments (low, high)");
        }
        auto low = extract_rng_param(call, 0, name);
        if (!low)
            return std::unexpected(low.error());
        auto high = extract_rng_param(call, 1, name);
        if (!high)
            return std::unexpected(high.error());
        if (*low >= *high) {
            return std::unexpected("rand_uniform: low must be less than high");
        }
        // x4 fill over independent xoshiro streams.
        Column<double> col;
        col.resize(rows);
        fill_uniform(col.data(), rows, *low, *high);
        return col;
    }

    if (name == "rand_normal") {
        if (call.args.size() != 2) {
            return std::unexpected("rand_normal: expected 2 arguments (mean, stddev)");
        }
        auto mean = extract_rng_param(call, 0, name);
        if (!mean)
            return std::unexpected(mean.error());
        auto stddev = extract_rng_param(call, 1, name);
        if (!stddev)
            return std::unexpected(stddev.error());
        if (*stddev <= 0.0) {
            return std::unexpected("rand_normal: stddev must be positive");
        }
        // Generate all normals into the column buffer via the portable x4 path.
        Column<double> col;
        col.resize(rows);
        fill_normal(col.data(), rows, *mean, *stddev);
        return col;
    }

    if (name == "rand_student_t") {
        if (call.args.size() != 1) {
            return std::unexpected("rand_student_t: expected 1 argument (degrees_of_freedom)");
        }
        auto df = extract_rng_param(call, 0, name);
        if (!df)
            return std::unexpected(df.error());
        if (*df <= 0.0) {
            return std::unexpected("rand_student_t: degrees_of_freedom must be positive");
        }
        std::student_t_distribution<double> dist(*df);
        Column<double> col;
        col.reserve(rows);
        for (std::size_t i = 0; i < rows; ++i)
            col.push_back(dist(rng));
        return col;
    }

    if (name == "rand_gamma") {
        if (call.args.size() != 2) {
            return std::unexpected("rand_gamma: expected 2 arguments (shape, scale)");
        }
        auto shape = extract_rng_param(call, 0, name);
        if (!shape)
            return std::unexpected(shape.error());
        auto scale = extract_rng_param(call, 1, name);
        if (!scale)
            return std::unexpected(scale.error());
        if (*shape <= 0.0) {
            return std::unexpected("rand_gamma: shape must be positive");
        }
        if (*scale <= 0.0) {
            return std::unexpected("rand_gamma: scale must be positive");
        }
        std::gamma_distribution<double> dist(*shape, *scale);
        Column<double> col;
        col.reserve(rows);
        for (std::size_t i = 0; i < rows; ++i)
            col.push_back(dist(rng));
        return col;
    }

    if (name == "rand_exponential") {
        if (call.args.size() != 1) {
            return std::unexpected("rand_exponential: expected 1 argument (lambda)");
        }
        auto lambda = extract_rng_param(call, 0, name);
        if (!lambda)
            return std::unexpected(lambda.error());
        if (*lambda <= 0.0) {
            return std::unexpected("rand_exponential: lambda must be positive");
        }
        // Direct inverse-CDF via the x4 engine.
        Column<double> col;
        col.resize(rows);
        fill_exponential(col.data(), rows, *lambda);
        return col;
    }

    if (name == "rand_bernoulli") {
        if (call.args.size() != 1) {
            return std::unexpected("rand_bernoulli: expected 1 argument (p)");
        }
        auto p = extract_rng_param(call, 0, name);
        if (!p)
            return std::unexpected(p.error());
        if (*p < 0.0 || *p > 1.0) {
            return std::unexpected("rand_bernoulli: p must be in [0, 1]");
        }
        Column<std::int64_t> col;
        col.resize(rows);
        fill_bernoulli(col.data(), rows, *p);
        return col;
    }

    if (name == "rand_poisson") {
        if (call.args.size() != 1) {
            return std::unexpected("rand_poisson: expected 1 argument (lambda)");
        }
        auto lambda = extract_rng_param(call, 0, name);
        if (!lambda)
            return std::unexpected(lambda.error());
        if (*lambda <= 0.0) {
            return std::unexpected("rand_poisson: lambda must be positive");
        }
        std::poisson_distribution<std::int64_t> dist(static_cast<double>(*lambda));
        Column<std::int64_t> col;
        col.reserve(rows);
        for (std::size_t i = 0; i < rows; ++i)
            col.push_back(dist(rng));
        return col;
    }

    if (name == "rand_int") {
        if (call.args.size() != 2) {
            return std::unexpected("rand_int: expected 2 arguments (lo, hi)");
        }
        auto lo_d = extract_rng_param(call, 0, name);
        if (!lo_d)
            return std::unexpected(lo_d.error());
        auto hi_d = extract_rng_param(call, 1, name);
        if (!hi_d)
            return std::unexpected(hi_d.error());
        auto lo = static_cast<std::int64_t>(*lo_d);
        auto hi = static_cast<std::int64_t>(*hi_d);
        if (lo > hi) {
            return std::unexpected("rand_int: lo must be <= hi");
        }
        // span as uint64: hi - lo + 1. When lo == INT64_MIN and hi == INT64_MAX,
        // this wraps to 0 (the full 2^64 range). Handle that edge case separately.
        const auto span = static_cast<std::uint64_t>(hi) - static_cast<std::uint64_t>(lo) + 1;
        Column<std::int64_t> col;
        col.resize(rows);
        if (span == 0) {
            // Full int64 range: every 64-bit word is a valid sample.
            auto& rng4 = get_rng_x4();
            std::size_t i = 0;
            while (i + 4 <= rows) {
                const auto bits = rng4();
                col[i] = static_cast<std::int64_t>(bits[0]);
                col[i + 1] = static_cast<std::int64_t>(bits[1]);
                col[i + 2] = static_cast<std::int64_t>(bits[2]);
                col[i + 3] = static_cast<std::int64_t>(bits[3]);
                i += 4;
            }
            if (i < rows) {
                const auto bits = rng4();
                for (std::size_t lane = 0; i < rows; ++lane, ++i)
                    col[i] = static_cast<std::int64_t>(bits[lane]);
            }
        } else {
            fill_int(col.data(), rows, lo, span);
        }
        return col;
    }

    return std::unexpected("unknown rng function: " + name);
}

// ─── rep ─────────────────────────────────────────────────────────────────────
//
// rep(x, times=1, each=1, length_out=-1)
//
//   x          – scalar literal (Int, Float, Bool, String) or column reference.
//   times      – repeat the whole sequence this many times (default 1).
//   each       – repeat each element this many times before advancing (default 1).
//   length_out – desired output length; default is the current table row count.
//                Shorter sequences are cycled; longer ones are truncated.
//
// Mirrors R's rep() semantics within the columnar context.

auto apply_rep_func(const ir::CallExpr& call, const Table& input, std::size_t rows)
    -> std::expected<ColumnValue, std::string> {
    if (call.args.empty()) {
        return std::unexpected("rep: expected one positional argument (x or [array])");
    }

    // ── detect array-literal form (lowered by lower_expr_to_ir) ────────────
    // When `rep([e0,e1,...])` is lowered, each element becomes a positional arg
    // and a sentinel named arg `__array_len = N` is added. In that form all
    // positional args are the elements to cycle over (not a single scalar x).
    std::int64_t array_len_sentinel = 0;
    for (const auto& narg : call.named_args) {
        if (narg.name == "__array_len") {
            const auto* lit = std::get_if<ir::Literal>(&narg.value->node);
            if (lit != nullptr) {
                if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                    array_len_sentinel = *iv;
                }
            }
        }
    }

    // ── parse named args ────────────────────────────────────────────────────
    std::int64_t times = 1;
    std::int64_t each = 1;
    auto length_out = static_cast<std::int64_t>(rows);  // default = table rows

    for (const auto& narg : call.named_args) {
        if (narg.name == "__array_len") {
            continue;  // already consumed above
        }
        const auto* lit = std::get_if<ir::Literal>(&narg.value->node);
        auto as_int = [&] -> std::expected<std::int64_t, std::string> {
            if (lit == nullptr) {
                return std::unexpected("rep: named argument '" + narg.name + "' must be a literal");
            }
            if (const auto* i = std::get_if<std::int64_t>(&lit->value)) {
                return *i;
            }
            if (const auto* d = std::get_if<double>(&lit->value)) {
                return static_cast<std::int64_t>(*d);
            }
            return std::unexpected("rep: named argument '" + narg.name + "' must be an integer");
        };
        if (narg.name == "times") {
            auto v = as_int();
            if (!v)
                return std::unexpected(v.error());
            times = *v;
            if (times <= 0)
                return std::unexpected("rep: times must be positive");
        } else if (narg.name == "each") {
            auto v = as_int();
            if (!v)
                return std::unexpected(v.error());
            each = *v;
            if (each <= 0)
                return std::unexpected("rep: each must be positive");
        } else if (narg.name == "length_out") {
            auto v = as_int();
            if (!v)
                return std::unexpected(v.error());
            length_out = *v;
            if (length_out < 0)
                return std::unexpected("rep: length_out must be non-negative");
        } else {
            return std::unexpected("rep: unknown named argument '" + narg.name + "'");
        }
    }

    const auto out_len = static_cast<std::size_t>(length_out);
    const auto n_times = static_cast<std::size_t>(times);
    const auto n_each = static_cast<std::size_t>(each);

    // ── scalar literal x ────────────────────────────────────────────────────
    if (array_len_sentinel == 0) {
        if (const auto* lit = std::get_if<ir::Literal>(&call.args[0]->node)) {
            if (std::holds_alternative<NullScalar>(lit->value)) {
                return std::unexpected("rep: a NULL has no value to repeat");
            }
            return std::visit(
                [&](const auto& v) -> ColumnValue {
                    using T = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<T, bool>) {
                        Column<bool> col;
                        col.reserve(out_len);
                        for (std::size_t i = 0; i < out_len; ++i)
                            col.push_back(v);
                        return col;
                    } else if constexpr (std::is_same_v<T, std::int64_t>) {
                        Column<std::int64_t> col;
                        col.reserve(out_len);
                        for (std::size_t i = 0; i < out_len; ++i)
                            col.push_back(v);
                        return col;
                    } else if constexpr (std::is_same_v<T, double>) {
                        Column<double> col;
                        col.reserve(out_len);
                        for (std::size_t i = 0; i < out_len; ++i)
                            col.push_back(v);
                        return col;
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        Column<std::string> col;
                        col.reserve(out_len);
                        for (std::size_t i = 0; i < out_len; ++i)
                            col.push_back(std::string_view{v});
                        return col;
                    } else if constexpr (std::is_same_v<T, Date>) {
                        Column<Date> col;
                        col.reserve(out_len);
                        for (std::size_t i = 0; i < out_len; ++i)
                            col.push_back(v);
                        return col;
                    } else if constexpr (std::is_same_v<T, DecimalValue>) {
                        Column<Decimal> col = make_decimal_column(v.type);
                        col.resize(out_len, Decimal{v.units});
                        return col;
                    } else if constexpr (std::is_same_v<T, NullScalar>) {
                        return Column<std::int64_t>{};  // rejected above
                    } else {
                        static_assert(std::is_same_v<T, Timestamp>);
                        Column<Timestamp> col;
                        col.reserve(out_len);
                        for (std::size_t i = 0; i < out_len; ++i)
                            col.push_back(v);
                        return col;
                    }
                },
                lit->value);
        }
    }  // end: array_len_sentinel == 0 guard around scalar arm

    // ── array-literal form: rep([e0,e1,...]) cycles the elements ────────────
    if (array_len_sentinel > 0) {
        // All positional args are literal elements of the array pattern.
        const auto arr_len = static_cast<std::size_t>(array_len_sentinel);
        if (call.args.size() != arr_len) {
            return std::unexpected("rep: internal error: arg count mismatch for array form");
        }
        if (arr_len == 0) {
            return std::unexpected("rep: array literal must not be empty");
        }
        const auto* first_lit = std::get_if<ir::Literal>(&call.args[0]->node);
        if (first_lit == nullptr) {
            return std::unexpected("rep: array literal elements must be literals");
        }
        // Build the output column cycling over the elements. The `each` and
        // `times` named args apply to the array as a whole pattern (R semantics:
        // each repeats each element before the next, times repeats the whole
        // pattern, length_out trims/pads).
        std::size_t pattern_len = arr_len * n_each * n_times;
        return std::visit(
            [&](const auto& proto) -> std::expected<ColumnValue, std::string> {
                using T = std::decay_t<decltype(proto)>;
                if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double> ||
                              std::is_same_v<T, bool> || std::is_same_v<T, std::string> ||
                              std::is_same_v<T, Date> || std::is_same_v<T, Timestamp>) {
                    using ColT = Column<T>;
                    // Collect all elements.
                    ColT elements;
                    elements.reserve(arr_len);
                    for (std::size_t j = 0; j < arr_len; ++j) {
                        const auto* lit = std::get_if<ir::Literal>(&call.args[j]->node);
                        if (lit == nullptr) {
                            return std::unexpected("rep: array element " + std::to_string(j) +
                                                   " is not a literal");
                        }
                        const auto* v = std::get_if<T>(&lit->value);
                        if (v == nullptr) {
                            return std::unexpected("rep: array literal has mixed element types");
                        }
                        if constexpr (std::is_same_v<T, std::string>) {
                            elements.push_back(std::string_view{*v});
                        } else {
                            elements.push_back(*v);
                        }
                    }
                    // Fill output by cycling the elements.
                    ColT result;
                    result.reserve(out_len);
                    for (std::size_t i = 0; i < out_len; ++i) {
                        const std::size_t pos_in_pattern = pattern_len > 0 ? (i % pattern_len) : 0;
                        const std::size_t pos_in_each_seq = pos_in_pattern % (arr_len * n_each);
                        const std::size_t src_idx = pos_in_each_seq / n_each;
                        if constexpr (std::is_same_v<T, std::string>) {
                            result.push_back(std::string_view{elements[src_idx]});
                        } else {
                            result.push_back(elements[src_idx]);
                        }
                    }
                    return ColumnValue{std::move(result)};
                } else {
                    return std::unexpected("rep: unsupported array element type");
                }
            },
            first_lit->value);
    }

    // ── column reference x ───────────────────────────────────────────────────
    if (const auto* col_ref = ir::as_column_ref(*call.args[0])) {
        const auto* src_col = input.find(col_ref->name);
        if (src_col == nullptr) {
            return std::unexpected("rep: unknown column '" + col_ref->name + "'");
        }
        return std::visit(
            [&](const auto& col) -> ColumnValue {
                using ColT = std::decay_t<decltype(col)>;
                const std::size_t src_len = col.size();
                if (src_len == 0) {
                    return ColT{};
                }
                // pattern_len = src_len * each * times (the logical full sequence)
                const std::size_t pattern_len = src_len * n_each * n_times;
                ColT result;
                result.reserve(out_len);
                for (std::size_t i = 0; i < out_len; ++i) {
                    // Position within the repeating pattern (cycled via %)
                    const std::size_t pos_in_pattern = pattern_len > 0 ? (i % pattern_len) : 0;
                    // Within one "times" cycle: position in (each-repeated sequence)
                    const std::size_t pos_in_each_seq = pos_in_pattern % (src_len * n_each);
                    // Index into original column
                    const std::size_t src_idx = pos_in_each_seq / n_each;
                    result.push_back(col[src_idx]);
                }
                return result;
            },
            *src_col);
    }

    return std::unexpected("rep: first argument must be a scalar literal or column reference");
}

// seq(from=0, by=1) — an arithmetic ramp over the frame's rows.
//
//   seq()          – 0, 1, 2, ...
//   seq(10)        – 10, 11, 12, ...
//   seq(10, 5)     – 10, 15, 20, ...
//   seq(0.0, 0.5)  – 0.0, 0.5, 1.0, ...
//
// `from` and `by` are positional, not named: `infer` receives only positional
// argument types (see infer_expr_type), so a named `from=0.0` would type the
// column Int and then be filled with Doubles. Named args are refused rather
// than ignored, so that mismatch cannot be written.
//
// There is no `length_out`. The output is always the frame's row count, which
// is the only length `rep`'s `length_out` accepts either — evaluate_field's
// generator guard rejects a generated length that differs from the frame.
auto apply_seq_func(const ir::CallExpr& call, std::size_t rows)
    -> std::expected<ColumnValue, std::string> {
    if (call.args.size() > 2) {
        return std::unexpected("seq: expected at most 2 arguments (from, by)");
    }
    if (!call.named_args.empty()) {
        return std::unexpected("seq: unknown named argument '" + call.named_args.front().name +
                               "'; from and by are positional — seq(from, by)");
    }

    // Whether either literal was written as a Float decides the column type:
    // Int only when both are Int, which is what `infer` concludes from these
    // same two literals.
    std::int64_t from_i = 0;
    std::int64_t by_i = 1;
    double from_d = 0.0;
    double by_d = 1.0;
    bool is_double = false;
    std::string err;

    const auto read = [&](std::size_t pos, std::int64_t& out_i, double& out_d) {
        if (pos >= call.args.size() || !err.empty()) {
            return;
        }
        const auto* lit = std::get_if<ir::Literal>(&call.args[pos]->node);
        if (lit == nullptr) {
            err = "seq: argument " + std::to_string(pos + 1) + " must be a numeric literal";
            return;
        }
        if (const auto* i = std::get_if<std::int64_t>(&lit->value)) {
            out_i = *i;
            out_d = static_cast<double>(*i);
            return;
        }
        if (const auto* d = std::get_if<double>(&lit->value)) {
            out_d = *d;
            out_i = static_cast<std::int64_t>(*d);
            is_double = true;
            return;
        }
        err = "seq: argument " + std::to_string(pos + 1) + " must be numeric";
    };
    read(0, from_i, from_d);
    read(1, by_i, by_d);
    if (!err.empty()) {
        return std::unexpected(err);
    }

    // Row i is computed from i rather than accumulated: a running `+= by` would
    // drift on the Float path.
    if (is_double) {
        Column<double> col;
        col.reserve(rows);
        for (std::size_t i = 0; i < rows; ++i) {
            col.push_back(from_d + (by_d * static_cast<double>(i)));
        }
        return col;
    }
    Column<std::int64_t> col;
    col.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        col.push_back(from_i + (by_i * static_cast<std::int64_t>(i)));
    }
    return col;
}

}  // namespace ibex::runtime
