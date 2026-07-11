// expr.cpp — expression evaluation: the row-wise scalar-builtin registry
// (single source of truth for infer + eval), type inference, per-row eval,
// per-field column evaluation, lag/lead/cum/fill/float-clean transforms,
// and the RNG/rep column generators.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/rng.hpp>
#include <ibex/runtime/safe_arith.hpp>
#include <ibex/runtime/table_format.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <optional>
#include <random>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "interpreter_internal.hpp"
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
// and the per-row evaluation pass (`eval_expr`, which computes each cell)
// dispatch these functions through this single table. Previously each pass had
// its own hand-maintained switch, so functions could drift out of sync — casts,
// ceil/floor/trunc, and round were usable at the REPL top level but missing
// from `update`/`select`. Registering once here keeps both passes in lockstep.
//
// Functions that need column-level context (fill_*, cum*, rolling_*, lag/lead,
// RNG, rep, externs) or that are not pure functions of their evaluated
// arguments (round's mode is a syntactic identifier; is_null is null-aware) are
// handled outside this table, in the call sites below.

auto expr_value_to_double(const ExprValue& v) -> std::optional<double> {
    if (const auto* i = std::get_if<std::int64_t>(&v)) {
        return static_cast<double>(*i);
    }
    if (const auto* d = std::get_if<double>(&v)) {
        return *d;
    }
    return std::nullopt;
}

// Render a scalar value as text for string interpolation (`__interp`). Mirrors
// the table/REPL display formatting (floats via format_float_mixed, dates/
// timestamps via their formatters) so an interpolated value reads the same as a
// printed one.
auto expr_value_to_string(const ExprValue& v) -> std::string {
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
    return "null";  // Null — matches the table display of a missing cell
}

namespace {

// Extract a calendar/clock field from a Date or Timestamp value.
auto eval_date_part(std::string_view name, const ExprValue& v)
    -> std::expected<ExprValue, std::string> {
    using namespace std::chrono;
    const bool date_ok = (name == "year" || name == "month" || name == "day");
    if (const auto* dv = std::get_if<Date>(&v)) {
        if (!date_ok) {
            return std::unexpected(std::string(name) + ": argument must be Timestamp");
        }
        const year_month_day ymd{sys_days{days{dv->days}}};
        if (name == "year") {
            return ExprValue{static_cast<std::int64_t>(static_cast<int>(ymd.year()))};
        }
        if (name == "month") {
            return ExprValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.month()))};
        }
        return ExprValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.day()))};
    }
    if (const auto* tv = std::get_if<Timestamp>(&v)) {
        const sys_time<nanoseconds> tp{nanoseconds{tv->nanos}};
        auto day_pt = floor<days>(tp);
        const year_month_day ymd{day_pt};
        const hh_mm_ss<nanoseconds> hms{tp - day_pt};
        if (name == "year") {
            return ExprValue{static_cast<std::int64_t>(static_cast<int>(ymd.year()))};
        }
        if (name == "month") {
            return ExprValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.month()))};
        }
        if (name == "day") {
            return ExprValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.day()))};
        }
        if (name == "hour") {
            return ExprValue{static_cast<std::int64_t>(hms.hours().count())};
        }
        if (name == "minute") {
            return ExprValue{static_cast<std::int64_t>(hms.minutes().count())};
        }
        return ExprValue{static_cast<std::int64_t>(hms.seconds().count())};
    }
    return std::unexpected(std::string(name) + ": argument must be Date or Timestamp");
}

}  // namespace

const robin_hood::unordered_map<std::string_view, BuiltinFn>& builtins() {
    using IT = std::expected<ExprType, std::string>;
    using IV = std::expected<ExprValue, std::string>;
    static const robin_hood::unordered_map<std::string_view, BuiltinFn> table = [] {
        robin_hood::unordered_map<std::string_view, BuiltinFn> m;

        // abs: numeric -> same numeric type.
        m.emplace("abs", BuiltinFn{
                             .min_args = 1,
                             .max_args = 1,
                             .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                                 if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                                     return a[0];
                                 }
                                 return std::unexpected("abs: argument must be numeric");
                             },
                             .exec = ScalarExec{.eval = [](std::string_view,
                                                           const std::vector<ExprValue>& a) -> IV {
                                 if (const auto* i = std::get_if<std::int64_t>(a.data())) {
                                     return ExprValue{std::int64_t{std::abs(*i)}};
                                 }
                                 if (const auto* d = std::get_if<double>(a.data())) {
                                     return ExprValue{std::abs(*d)};
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
                                                    const std::vector<ExprValue>& a) -> IV {
                          std::string out;
                          for (const auto& v : a) {
                              out += expr_value_to_string(v);
                          }
                          return ExprValue{std::move(out)};
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
                                          const std::vector<ExprValue>& a) -> IV {
                auto x = expr_value_to_double(a[0]);
                if (!x) {
                    return std::unexpected(std::string(name) + ": argument must be numeric");
                }
                const double v = *x;
                if (name == "sqrt")
                    return ExprValue{std::sqrt(v)};
                if (name == "log")
                    return ExprValue{std::log(v)};
                if (name == "log2")
                    return ExprValue{std::log2(v)};
                if (name == "log10")
                    return ExprValue{std::log10(v)};
                if (name == "exp")
                    return ExprValue{std::exp(v)};
                if (name == "sin")
                    return ExprValue{std::sin(v)};
                if (name == "cos")
                    return ExprValue{std::cos(v)};
                if (name == "tan")
                    return ExprValue{std::tan(v)};
                if (name == "asin")
                    return ExprValue{std::asin(v)};
                if (name == "acos")
                    return ExprValue{std::acos(v)};
                if (name == "atan")
                    return ExprValue{std::atan(v)};
                if (name == "sinh")
                    return ExprValue{std::sinh(v)};
                if (name == "cosh")
                    return ExprValue{std::cosh(v)};
                return ExprValue{std::tanh(v)};  // tanh
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
                                          const std::vector<ExprValue>& a) -> IV {
                if (std::holds_alternative<std::int64_t>(a[0])) {
                    return a[0];
                }
                if (const auto* d = std::get_if<double>(a.data())) {
                    if (name == "ceil") {
                        return ExprValue{std::ceil(*d)};
                    }
                    if (name == "floor") {
                        return ExprValue{std::floor(*d)};
                    }
                    return ExprValue{std::trunc(*d)};
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            }},
        };
        m.emplace("ceil", integral);
        m.emplace("floor", integral);
        m.emplace("trunc", integral);

        // Float64 / Float32: cast Int or Float to Float64.
        const BuiltinFn to_float{
            .min_args = 1,
            .max_args = 1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return ExprType::Double;
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Float");
            },
            .exec = ScalarExec{.eval = [](std::string_view, const std::vector<ExprValue>& a) -> IV {
                if (auto d = expr_value_to_double(a[0])) {
                    return ExprValue{*d};
                }
                return std::unexpected("cast: cannot cast non-numeric to Float");
            }},
        };
        m.emplace("Float64", to_float);
        m.emplace("Float32", to_float);

        // Int64 / Int32 / Int: cast Int or whole-valued Float to Int64.
        const BuiltinFn to_int{
            .min_args = 1,
            .max_args = 1,
            .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return ExprType::Int;
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Int");
            },
            .exec = ScalarExec{.eval = [](std::string_view name,
                                          const std::vector<ExprValue>& a) -> IV {
                if (const auto* i = std::get_if<std::int64_t>(a.data())) {
                    return ExprValue{*i};
                }
                if (const auto* d = std::get_if<double>(a.data())) {
                    if (*d != std::trunc(*d)) {
                        return std::unexpected(std::string(name) +
                                               "(): cannot cast non-integer Float to Int (use "
                                               "floor(), ceil(), or round())");
                    }
                    return ExprValue{static_cast<std::int64_t>(*d)};
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Int");
            }},
        };
        m.emplace("Int64", to_int);
        m.emplace("Int32", to_int);
        m.emplace("Int", to_int);

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
            .exec = ScalarExec{.eval = [](std::string_view name, const std::vector<ExprValue>& a)
                                   -> IV { return eval_date_part(name, a[0]); }},
        };
        m.emplace("year", date_part);
        m.emplace("month", date_part);
        m.emplace("day", date_part);
        m.emplace("hour", date_part);
        m.emplace("minute", date_part);
        m.emplace("second", date_part);

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
                                                    const std::vector<ExprValue>& a) -> IV {
                          if (const auto* d = std::get_if<double>(a.data())) {
                              return ExprValue{std::isnan(*d)};
                          }
                          return std::unexpected("is_nan: argument must be Float64");
                      }},
                  });

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
                                          const std::vector<ExprValue>& a) -> IV {
                const bool want_min = (name == "pmin");
                auto better = [&](const ExprValue& cand,
                                  const ExprValue& best) -> std::expected<bool, std::string> {
                    auto num = [](const ExprValue& v) -> std::optional<double> {
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
                ExprValue best = a[0];
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

        m.emplace(
            "cumsum",
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
                    auto col = eval_cumsum_cumprod_column(call, input, call.callee == "cumprod");
                    if (!col) {
                        return std::unexpected(col.error());
                    }
                    return ComputedColumn{.column = std::move(*col), .validity = std::nullopt};
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
                                                              const ColumnEvalCtx&)
                                                -> std::expected<ComputedColumn, std::string> {
                          auto res = call.callee == "fill_forward"
                                         ? eval_fill_forward(call, input)
                                         : eval_fill_backward(call, input);
                          if (!res) {
                              return std::unexpected(res.error());
                          }
                          return ComputedColumn{.column = std::move(res->column),
                                                .validity = std::move(res->validity)};
                      }},
                  });
        m.emplace("fill_backward", m.at("fill_forward"));

        // ── Null-handling scalars (plans/exprvalue-null-arm-plan.md, stage
        // 3): row-local functions whose *purpose* is null, so their per-row
        // eval opts into receiving Null (NullPolicy::Handles) instead of the
        // central propagation. Each keeps its column kernel as a whole-column
        // fast path for the top-level bare form; the per-row eval is the
        // general form and the semantic reference — the two must agree.
        m.emplace(
            "fill_null",
            BuiltinFn{
                .min_args = 0,
                .max_args = -1,
                .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                    if (a.size() != 2) {
                        return std::unexpected(std::string(name) +
                                               ": expected 2 arguments (col, value)");
                    }
                    const bool numeric_widen = a[0] == ExprType::Double && a[1] == ExprType::Int;
                    if (a[1] != a[0] && !numeric_widen) {
                        return std::unexpected(std::string(name) +
                                               ": fill value must match the column type");
                    }
                    return a[0];
                },
                .exec = ScalarExec{
                    .null_policy = NullPolicy::Handles,
                    .eval = [](std::string_view, const std::vector<ExprValue>& a) -> IV {
                        if (std::holds_alternative<Null>(a[0])) {
                            return a[1];
                        }
                        return a[0];
                    },
                    .kernel =
                        [](const ir::CallExpr& call, const Table& input, std::size_t,
                           const ColumnEvalCtx&) -> std::expected<ComputedColumn, std::string> {
                        auto res = eval_fill_null(call, input);
                        if (!res) {
                            return std::unexpected(res.error());
                        }
                        return ComputedColumn{.column = std::move(res->column),
                                              .validity = std::move(res->validity)};
                    }},
            });

        m.emplace(
            "null_if_nan",
            BuiltinFn{
                .min_args = 0,
                .max_args = -1,
                .infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                    if (a.size() != 1) {
                        return std::unexpected(std::string(name) + ": expected 1 argument");
                    }
                    if (a[0] != ExprType::Double) {
                        return std::unexpected(std::string(name) + ": column must be Float64");
                    }
                    return ExprType::Double;
                },
                .exec = ScalarExec{
                    .null_policy = NullPolicy::Handles,
                    .eval = [](std::string_view name, const std::vector<ExprValue>& a) -> IV {
                        if (std::holds_alternative<Null>(a[0])) {
                            return ExprValue{Null{}};  // already null: stays null
                        }
                        const auto* d = std::get_if<double>(a.data());
                        if (d == nullptr) {
                            return std::unexpected(std::string(name) + ": column must be Float64");
                        }
                        const bool to_null =
                            name == "null_if_nan" ? std::isnan(*d) : !std::isfinite(*d);
                        return to_null ? ExprValue{Null{}} : a[0];
                    },
                    .kernel =
                        [](const ir::CallExpr& call, const Table& input, std::size_t,
                           const ColumnEvalCtx&) -> std::expected<ComputedColumn, std::string> {
                        auto res = eval_float_clean(call, input,
                                                    call.callee == "null_if_nan"
                                                        ? FloatCleanMode::NullIfNan
                                                        : FloatCleanMode::NullIfNotFinite);
                        if (!res) {
                            return std::unexpected(res.error());
                        }
                        return ComputedColumn{.column = std::move(res->column),
                                              .validity = std::move(res->validity)};
                    }},
            });
        m.emplace("null_if_not_finite", m.at("null_if_nan"));

        // coalesce(a, b, ...): first non-null argument, per row. The column
        // kernel lives in filter.cpp because its arguments are arbitrary
        // expressions evaluated through eval_value_vec.
        m.emplace(
            "coalesce",
            BuiltinFn{
                .min_args = 0,
                .max_args = -1,
                .infer = [](std::string_view, const std::vector<ExprType>& a) -> IT {
                    if (a.size() < 2) {
                        return std::unexpected("coalesce: expected at least 2 arguments");
                    }
                    for (const auto& t : a) {
                        if (t != a[0]) {
                            return std::unexpected("coalesce: arguments must share one type");
                        }
                    }
                    return a[0];
                },
                .exec = ScalarExec{
                    .null_policy = NullPolicy::Handles,
                    .eval = [](std::string_view, const std::vector<ExprValue>& a) -> IV {
                        for (const auto& v : a) {
                            if (!std::holds_alternative<Null>(v)) {
                                return v;
                            }
                        }
                        return ExprValue{Null{}};  // every argument null
                    },
                    .kernel =
                        [](const ir::CallExpr& call, const Table& input, std::size_t rows,
                           const ColumnEvalCtx& ctx) -> std::expected<ComputedColumn, std::string> {
                        return eval_coalesce_column(call, input, ctx.scalars, rows);
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
                if (name == "rolling_sum" || name == "rolling_min" || name == "rolling_max") {
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
                return apply_rolling_func(call, input, *spec);
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
             }) {
            m.emplace(fn, rolling_transform);
        }

        // ── Aggregates (N→1, per group): reduce a column to one scalar. The
        // registry is the single name-to-ir::AggFunc mapping — execution
        // (aggregate.cpp) resolves the enum through parse_aggregate_func,
        // which reads the AggregateExec payload here.
        const auto agg_infer = [](std::string_view name, const std::vector<ExprType>& a) -> IT {
            if (name == "count") {
                return ExprType::Int;
            }
            if (name == "sum" || name == "min" || name == "max" || name == "first" ||
                name == "last") {
                // Type-preserving reductions.
                if (a.empty()) {
                    return std::unexpected(std::string(name) + "(): expected column argument");
                }
                return a[0];
            }
            return ExprType::Double;  // mean/median/std/ewma/quantile/skew/kurtosis
        };
        constexpr std::array<std::pair<std::string_view, ir::AggFunc>, 13> agg_funcs{{
            {"sum", ir::AggFunc::Sum},
            {"mean", ir::AggFunc::Mean},
            {"min", ir::AggFunc::Min},
            {"max", ir::AggFunc::Max},
            {"count", ir::AggFunc::Count},
            {"first", ir::AggFunc::First},
            {"last", ir::AggFunc::Last},
            {"median", ir::AggFunc::Median},
            {"std", ir::AggFunc::Stddev},
            {"ewma", ir::AggFunc::Ewma},
            {"quantile", ir::AggFunc::Quantile},
            {"skew", ir::AggFunc::Skew},
            {"kurtosis", ir::AggFunc::Kurtosis},
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
            if (ir::fn_kind(name) != fn_kind_of(fn)) {
                invariant_violation(
                    "builtins(): ir::fn_kind disagrees with the registry's exec "
                    "alternative for '" +
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

// round(x, mode): mode is a bare identifier (lowered to a ColumnRef), so round
// is dispatched separately from the value-based scalar registry above.
auto valid_round_mode(std::string_view m) -> bool {
    return m == "nearest" || m == "bankers" || m == "floor" || m == "ceil" || m == "trunc";
}

auto extract_ir_round_mode(const ir::Expr& arg) -> std::expected<std::string_view, std::string> {
    if (const auto* ref = std::get_if<ir::ColumnRef>(&arg.node)) {
        if (valid_round_mode(ref->name)) {
            return std::string_view{ref->name};
        }
        return std::unexpected("round(): unknown mode '" + ref->name +
                               "' (expected: nearest, bankers, floor, ceil, trunc)");
    }
    return std::unexpected(
        "round(): second argument must be a bare mode identifier "
        "(nearest, bankers, floor, ceil, trunc)");
}

auto apply_round(double v, std::string_view mode) -> std::int64_t {
    if (mode == "nearest") {
        return static_cast<std::int64_t>(std::llround(v));
    }
    if (mode == "bankers") {
        return static_cast<std::int64_t>(std::llrint(v));  // FE_TONEAREST: ties to even
    }
    if (mode == "floor") {
        return static_cast<std::int64_t>(std::floor(v));
    }
    if (mode == "ceil") {
        return static_cast<std::int64_t>(std::ceil(v));
    }
    return static_cast<std::int64_t>(std::trunc(v));  // trunc
}

}  // namespace

auto infer_expr_type(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars,
                     const ExternRegistry* externs) -> std::expected<ExprType, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* source = input.find(col->name);
        if (source == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    if (std::holds_alternative<std::int64_t>(it->second)) {
                        return ExprType::Int;
                    }
                    if (std::holds_alternative<double>(it->second)) {
                        return ExprType::Double;
                    }
                    if (std::holds_alternative<bool>(it->second)) {
                        return ExprType::Bool;
                    }
                    if (std::holds_alternative<Date>(it->second)) {
                        return ExprType::Date;
                    }
                    if (std::holds_alternative<Timestamp>(it->second)) {
                        return ExprType::Timestamp;
                    }
                    return ExprType::String;
                }
            }
            return std::unexpected("unknown column in expression: " + col->name +
                                   " (available: " + format_columns(input) + ")");
        }
        return expr_type_for_column(*source);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        if (std::holds_alternative<std::int64_t>(lit->value)) {
            return ExprType::Int;
        }
        if (std::holds_alternative<double>(lit->value)) {
            return ExprType::Double;
        }
        if (std::holds_alternative<bool>(lit->value)) {
            return ExprType::Bool;
        }
        return ExprType::String;
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
        if (left.value() == ExprType::String || right.value() == ExprType::String) {
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
        // Pure row-wise scalar builtins: single source of truth (see
        // builtins()). Both this pass and eval_expr dispatch here.
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
                arg_types.push_back(*t);
            }
            return fn.infer(call->callee, arg_types);
        }
        // round(x, mode): mode is a bare identifier, so it is dispatched apart
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
            if (*arg_type != ExprType::Int && *arg_type != ExprType::Double) {
                return std::unexpected("round: first argument must be numeric");
            }
            return ExprType::Int;
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
        for (const auto& arg : call->args) {
            auto arg_type = infer_expr_type(*arg, input, scalars, externs);
            if (!arg_type) {
                return arg_type;
            }
        }
        switch (fn->scalar_kind.value()) {
            case ScalarKind::Int:
                return ExprType::Int;
            case ScalarKind::Double:
                return ExprType::Double;
            case ScalarKind::Bool:
                return ExprType::Bool;
            case ScalarKind::String:
                return ExprType::String;
            case ScalarKind::Date:
                return ExprType::Date;
            case ScalarKind::Timestamp:
                return ExprType::Timestamp;
        }
    }
    return std::unexpected("unsupported expression");
}

auto eval_expr(const ir::Expr& expr, const Table& input, std::size_t row,
               const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<ExprValue, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* entry = input.find_entry(col->name);
        if (entry == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    return expr_from_scalar(it->second);
                }
            }
            return std::unexpected("unknown column in expression: " + col->name +
                                   " (available: " + format_columns(input) + ")");
        }
        // A null cell is Null, read from the validity bitmap — its payload is
        // undefined and never touched (plans/exprvalue-null-arm-plan.md).
        if (entry->validity.has_value() && !(*entry->validity)[row]) {
            return ExprValue{Null{}};
        }
        return std::visit(
            [&](const auto& column) -> ExprValue {
                using ColType = std::decay_t<decltype(column)>;
                if constexpr (std::is_same_v<ColType, Column<Categorical>> ||
                              std::is_same_v<ColType, Column<std::string>>) {
                    return std::string(column[row]);
                } else {
                    return column[row];
                }
            },
            *entry->column);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        return std::visit([](const auto& v) -> ExprValue { return v; }, lit->value);
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = eval_expr(*bin->left, input, row, scalars, externs);
        if (!left) {
            return left;
        }
        auto right = eval_expr(*bin->right, input, row, scalars, externs);
        if (!right) {
            return right;
        }
        // Null propagates through every operator: null in -> null out.
        if (std::holds_alternative<Null>(left.value()) ||
            std::holds_alternative<Null>(right.value())) {
            return ExprValue{Null{}};
        }
        if (std::holds_alternative<std::string>(left.value()) ||
            std::holds_alternative<std::string>(right.value())) {
            return std::unexpected("string arithmetic not supported");
        }
        if (std::holds_alternative<Date>(left.value()) ||
            std::holds_alternative<Date>(right.value()) ||
            std::holds_alternative<Timestamp>(left.value()) ||
            std::holds_alternative<Timestamp>(right.value())) {
            return std::unexpected("date/time arithmetic not supported");
        }
        if (std::holds_alternative<bool>(left.value()) ||
            std::holds_alternative<bool>(right.value())) {
            return std::unexpected("boolean arithmetic not supported");
        }
        auto to_double = [](const ExprValue& v) -> double {
            if (const auto* i = std::get_if<std::int64_t>(&v)) {
                return static_cast<double>(*i);
            }
            return std::get<double>(v);
        };
        const bool want_double = bin->op == ir::ArithmeticOp::Div ||
                                 std::holds_alternative<double>(left.value()) ||
                                 std::holds_alternative<double>(right.value());
        if (want_double) {
            const double lhs = to_double(left.value());
            const double rhs = to_double(right.value());
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return lhs + rhs;
                case ir::ArithmeticOp::Sub:
                    return lhs - rhs;
                case ir::ArithmeticOp::Mul:
                    return lhs * rhs;
                case ir::ArithmeticOp::Div:
                    return lhs / rhs;
                case ir::ArithmeticOp::Mod:
                    return std::fmod(lhs, rhs);
            }
        } else {
            std::int64_t lhs = std::get<std::int64_t>(left.value());
            std::int64_t rhs = std::get<std::int64_t>(right.value());
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return lhs + rhs;
                case ir::ArithmeticOp::Sub:
                    return lhs - rhs;
                case ir::ArithmeticOp::Mul:
                    return lhs * rhs;
                case ir::ArithmeticOp::Div:
                    return safe_idiv(lhs, rhs);
                case ir::ArithmeticOp::Mod:
                    return safe_imod(lhs, rhs);
            }
        }
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        // Pure row-wise scalar builtins: single source of truth (see
        // builtins()). Both this pass and infer_expr_type dispatch here.
        // Non-Scalar builtins hold no ScalarExec — they fall through to the
        // same "unknown function" error as before they joined the registry
        // (routing sends them to the vectorized path instead).
        const auto* entry = find_builtin(call->callee);
        const auto* scalar_exec =
            entry != nullptr ? std::get_if<ScalarExec>(&entry->exec) : nullptr;
        if (scalar_exec != nullptr && scalar_exec->eval != nullptr) {
            const auto argc = static_cast<int>(call->args.size());
            if (argc < entry->min_args || (entry->max_args >= 0 && argc > entry->max_args)) {
                return std::unexpected(std::string(call->callee) + ": wrong number of arguments");
            }
            std::vector<ExprValue> arg_values;
            arg_values.reserve(call->args.size());
            for (const auto& arg : call->args) {
                auto v = eval_expr(*arg, input, row, scalars, externs);
                if (!v) {
                    return v;
                }
                // NullPolicy::Propagate, applied centrally: a Null argument
                // short-circuits the call to Null, so `eval` never sees one.
                // Handles entries (coalesce/fill_null/null_if_*) receive the
                // Null and decide.
                if (scalar_exec->null_policy == NullPolicy::Propagate &&
                    std::holds_alternative<Null>(*v)) {
                    return ExprValue{Null{}};
                }
                arg_values.push_back(std::move(*v));
            }
            return scalar_exec->eval(call->callee, arg_values);
        }
        // round(x, mode): mode is a bare identifier; dispatched apart from the
        // value-based registry. Always yields Int64.
        if (call->callee == "round") {
            if (call->args.size() != 2) {
                return std::unexpected("round: expected 2 arguments (value, mode)");
            }
            auto mode = extract_ir_round_mode(*call->args[1]);
            if (!mode) {
                return std::unexpected(mode.error());
            }
            auto arg = eval_expr(*call->args[0], input, row, scalars, externs);
            if (!arg) {
                return arg;
            }
            if (std::holds_alternative<Null>(arg.value())) {
                return ExprValue{Null{}};
            }
            auto d = expr_value_to_double(arg.value());
            if (!d) {
                return std::unexpected("round: first argument must be numeric");
            }
            return ExprValue{apply_round(*d, *mode)};
        }
        if (externs == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        const auto* fn = externs->find(call->callee);
        if (fn == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        if (fn->kind != ExternReturnKind::Scalar) {
            return std::unexpected("function not usable in expression: " + call->callee);
        }
        // Extern scalar functions take null-free ScalarValue arguments; a
        // Null argument has no image there (externs never see null).
        std::vector<ScalarValue> arg_values;
        arg_values.reserve(call->args.size());
        for (const auto& arg : call->args) {
            auto value = eval_expr(*arg, input, row, scalars, externs);
            if (!value) {
                return value;
            }
            auto scalar = scalar_from_expr(value.value());
            if (!scalar.has_value()) {
                return std::unexpected(call->callee + ": null argument in extern function call");
            }
            arg_values.push_back(std::move(*scalar));
        }
        auto result = fn->func(arg_values);
        if (!result) {
            return std::unexpected(result.error());
        }
        if (auto* scalar = std::get_if<ScalarValue>(&result.value())) {
            return expr_from_scalar(*scalar);
        }
        return std::unexpected("function returned table in expression: " + call->callee);
    }
    return std::unexpected("unsupported expression");
}

auto evaluate_row_count_expr_impl(const ir::Expr& expr, const ScalarRegistry* scalars,
                                  const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string> {
    const Table empty;
    auto value = eval_expr(expr, empty, 0, scalars, externs);
    if (!value) {
        return std::unexpected("row count expression: " + value.error());
    }
    if (const auto* i = std::get_if<std::int64_t>(&value.value())) {
        if (*i < 0) {
            return std::unexpected("row count expression must be non-negative");
        }
        return static_cast<std::size_t>(*i);
    }
    return std::unexpected("row count expression must evaluate to Int64");
}

// True if a field expression must be evaluated through the vectorized, validity-
// aware path (eval_value_vec) instead of the per-row eval_expr, because somewhere
// in the tree it has a node the per-row evaluator can't produce:
//   - a boolean node (comparison / logical / is_null), or
//   - a whole-column builtin: any registry entry with a `column_eval`
//     (Transforms — rolling/cum/lag/lead/fill/null_if_*/coalesce — and
//     Generators — `rand_*` / `rep`).
// Detecting these anywhere (not just at the top or under arithmetic) is what
// lets a non-row-local call nest inside arithmetic OR inside a scalar call —
// e.g. `(close - lag(close,1)) / lag(close,1)` and `abs(rand_normal(0,1))`.
// A plain scalar field (`abs(x)`, `x + 1`) contains none of these and stays on
// the fast per-row path.
auto field_uses_vectorized_eval(const ir::Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::CompareExpr> ||
                          std::is_same_v<T, ir::LogicalExpr> || std::is_same_v<T, ir::IsNullExpr>) {
                return true;
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr>) {
                return field_uses_vectorized_eval(*node.left) ||
                       field_uses_vectorized_eval(*node.right);
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                // Column-ONLY builtins (Transform/Generator) cannot be built
                // per row. Hybrid null-handling scalars (fill_null/null_if_*/
                // coalesce) evaluate per-row via their NullPolicy::Handles
                // eval, so they don't force this path.
                if (const auto* fn = find_builtin(node.callee);
                    fn != nullptr && is_column_only(*fn)) {
                    return true;
                }
                return std::ranges::any_of(
                    node.args, [](const auto& a) { return field_uses_vectorized_eval(*a); });
            } else {
                return false;
            }
        },
        expr.node);
}

// The single field-expression evaluator (stage 6 of
// plans/function-kind-registry-plan.md): every field/value dispatch runs
// through here exactly once —
//   1. a top-level whole-column builtin (Transform/Generator) via the
//      registry's `column_eval`, with a row-count guard so a short generator
//      (rep's length_out) errors instead of reading out of bounds;
//   2. the vectorized, validity-aware path for anything containing a boolean
//      node or a nested whole-column call (eval_expr is pure and has no null);
//   3. the fused numeric fast path;
//   4. the per-row eval_expr loop.
// update_table and windowed_update_table (and through them the grouped,
// guarded, and chunked variants) and evaluate_field_column all dispatch here.
auto evaluate_field(const ir::Expr& expr, const Table& input, const ColumnEvalCtx& ctx)
    -> std::expected<ComputedColumn, std::string> {
    std::size_t rows = input.rows();
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if (const auto* fn = find_builtin(call->callee);
            fn != nullptr && use_column_kernel(*fn, *call)) {
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
    // Validity-/boolean-aware fields (comparisons, logical, is_null) and
    // nested whole-column calls cannot be built per row. Evaluate them
    // through the vectorized, validity-aware path.
    if (field_uses_vectorized_eval(expr)) {
        auto res = eval_value_vec(expr, input, ctx.scalars, rows);
        if (!res) {
            return std::unexpected(res.error());
        }
        ColumnValue col;
        if (auto* owned = std::get_if<ColumnValue>(&res->data)) {
            col = std::move(*owned);
        } else {
            col = *std::get<const ColumnValue*>(res->data);
        }
        std::optional<ValidityBitmap> validity;
        if (const auto* v = res->get_validity()) {
            validity = *v;
        }
        return ComputedColumn{.column = std::move(col), .validity = std::move(validity)};
    }
    if (auto fast = try_fast_update_numeric_expr(expr, input, rows, inferred.value(), ctx.scalars);
        fast.has_value()) {
        return ComputedColumn{.column = std::move(fast.value()),
                              .validity = collect_expr_validity(expr, input, rows)};
    }
    ColumnValue new_column;
    switch (inferred.value()) {
        case ExprType::Int:
            new_column = Column<std::int64_t>{};
            break;
        case ExprType::Double:
            new_column = Column<double>{};
            break;
        case ExprType::Bool:
            new_column = Column<bool>{};
            break;
        case ExprType::String:
            new_column = Column<std::string>{};
            break;
        case ExprType::Date:
            new_column = Column<Date>{};
            break;
        case ExprType::Timestamp:
            new_column = Column<Timestamp>{};
            break;
    }
    std::visit([&](auto& col) { col.reserve(rows); }, new_column);
    // Validity is produced inline: eval_expr returns Null for a null cell and
    // propagation carries it to the row's result, so the bitmap is exact —
    // no post-hoc collect_expr_validity mask, no computing on undefined
    // payloads (plans/exprvalue-null-arm-plan.md, stage 2).
    ValidityBitmap validity(rows, true);
    bool any_null = false;
    for (std::size_t row = 0; row < rows; ++row) {
        auto value = eval_expr(expr, input, row, ctx.scalars, ctx.externs);
        if (!value) {
            return std::unexpected(value.error());
        }
        if (std::holds_alternative<Null>(value.value())) {
            validity.set(row, false);
            any_null = true;
            std::visit(
                [](auto& col) {
                    using ColType = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<typename ColType::value_type, std::string_view>) {
                        col.push_back(std::string_view{});
                    } else {
                        col.push_back(typename ColType::value_type{});
                    }
                },
                new_column);
            continue;
        }
        std::visit(
            [&](auto& col) {
                using ColType = std::decay_t<decltype(col)>;
                using ValueType = ColType::value_type;
                if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                    if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(*int_value);
                    } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                        col.push_back(static_cast<std::int64_t>(*double_value));
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Int64-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, double>) {
                    if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(static_cast<double>(*int_value));
                    } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                        col.push_back(*double_value);
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Float64-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, bool>) {
                    if (const auto* bool_value = std::get_if<bool>(&value.value())) {
                        col.push_back(*bool_value);
                    } else if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(*int_value != 0);
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Bool-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, std::string_view>) {
                    // Column<std::string>::value_type is std::string_view; the
                    // ExprValue holds an owned std::string, which push_back copies
                    // into the column's arena.
                    if (const auto* v = std::get_if<std::string>(&value.value())) {
                        col.push_back(*v);
                    } else {
                        invariant_violation("eval_expr_column: expected String expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, Date>) {
                    if (const auto* date_value = std::get_if<Date>(&value.value())) {
                        col.push_back(*date_value);
                    } else if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(int64_to_date_checked(*int_value));
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Date-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                    if (const auto* timestamp_value = std::get_if<Timestamp>(&value.value())) {
                        col.push_back(*timestamp_value);
                    } else if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(Timestamp{*int_value});
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Timestamp-compatible expression value");
                    }
                }
            },
            new_column);
    }
    return ComputedColumn{
        .column = std::move(new_column),
        .validity = any_null ? std::optional<ValidityBitmap>{std::move(validity)} : std::nullopt};
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
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (!col_ref) {
        return std::unexpected(fname + ": first argument must be a column name");
    }
    const Table empty;
    auto offset_value = eval_expr(*call.args[1], empty, 0, scalars, externs);
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
                // POD column: zero-fill then bulk-copy the shifted region.
                using T = ColT::value_type;
                out.resize(rows);  // zero-initialises
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
    -> std::expected<ColumnValue, std::string> {
    const std::string fname = is_prod ? "cumprod" : "cumsum";
    if (call.args.size() != 1) {
        return std::unexpected(fname + ": expected 1 argument");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (!col_ref) {
        return std::unexpected(fname + ": argument must be a column name");
    }
    const auto* src = input.find(col_ref->name);
    if (!src) {
        return std::unexpected(fname + ": unknown column '" + col_ref->name + "'");
    }
    std::size_t rows = input.rows();
    return std::visit(
        [&](const auto& col) -> std::expected<ColumnValue, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = ColT::value_type;
            if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>) {
                ColT result;
                result.reserve(rows);
                T acc = is_prod ? T{1} : T{0};
                for (std::size_t i = 0; i < rows; ++i) {
                    if (is_prod)
                        acc *= col[i];
                    else
                        acc += col[i];
                    result.push_back(acc);
                }
                return result;
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

// fill_null(col, value): replace every null cell with the scalar `value`.
// Accepts any column type; `value` must be a literal matching the column type.
// Returns a column with no validity bitmap.
auto eval_fill_null(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string> {
    if (call.args.size() != 2) {
        return std::unexpected("fill_null: expected 2 arguments (col, value)");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
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
            }

            if (!maybe_fill) {
                return std::unexpected(
                    "fill_null: fill value type does not match column type for '" + col_ref->name +
                    "'");
            }
            const T fill_val = *maybe_fill;

            ColT result;
            result.reserve(rows);
            for (std::size_t i = 0; i < rows; ++i) {
                const bool is_null_row = has_validity && !(*entry->validity)[i];
                result.push_back(is_null_row ? fill_val : col[i]);
            }
            // All rows are now valid (nulls replaced).
            return FillResult{std::move(result), std::nullopt};
        },
        *entry->column);
}

// fill_forward(col): LOCF — carry the last valid (non-null) value forward.
// Unfillable leading nulls (no prior valid value) remain null.
auto eval_fill_forward(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected("fill_forward: expected 1 argument (col)");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
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
            result.reserve(rows);
            std::optional<ValidityBitmap> out_validity;

            // carry: last seen valid value (safe for string_view: points into source col).
            T carry{};
            bool have_carry = false;

            for (std::size_t i = 0; i < rows; ++i) {
                if (validity[i]) {
                    result.push_back(col[i]);
                    carry = col[i];
                    have_carry = true;
                } else if (have_carry) {
                    result.push_back(carry);
                } else {
                    // Leading null — no value to carry; stays null.
                    result.push_back(T{});
                    if (!out_validity) {
                        out_validity.emplace(rows, true);
                    }
                    out_validity->set(i, false);
                }
            }
            return FillResult{std::move(result), std::move(out_validity)};
        },
        *entry->column);
}

// fill_backward(col): NOCB — carry the next valid (non-null) value backward.
// Unfillable trailing nulls (no subsequent valid value) remain null.
auto eval_fill_backward(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected("fill_backward: expected 1 argument (col)");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
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

            // Scan right-to-left to compute (value, valid) for each row,
            // storing in plain vectors so we can then push_back into ColT.
            std::vector<T> vals(rows);
            std::optional<ValidityBitmap> out_validity;

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
            // Build the output column using push_back (safe for all column types).
            ColT result;
            result.reserve(rows);
            for (std::size_t i = 0; i < rows; ++i) {
                result.push_back(vals[i]);
            }
            return FillResult{std::move(result), std::move(out_validity)};
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
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
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
    if (const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node)) {
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

}  // namespace ibex::runtime
