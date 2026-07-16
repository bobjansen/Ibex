// filter.cpp — vectorized predicate evaluation and filtering: typed
// arithmetic/comparison kernels, fused arith+compare and pair-compare
// dispatch, eval_value_vec / compute_mask (3VL nulls), and the
// filter_table / filter+project / filter+limit entry points.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/safe_arith.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <functional>
#include <limits>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
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

// ─── Vectorized filter ────────────────────────────────────────────────────────
//
// Instead of evaluating the predicate tree once per row (N × tree-depth
// variant dispatches), we:
//   1. compute_mask()  — walk the tree once, producing a uint8_t[N] mask via
//                        tight typed loops the compiler can auto-vectorize.
//   2. gather()        — a single pass over each column, copying only the rows
//                        where mask[i] != 0.
//
// For the common column-vs-literal case (e.g. price > 500.0) the literal is
// held as a scalar — no broadcast allocation, just a hoisted constant in the
// comparison loop.

// Column result: either a pointer into the table (zero-copy) or an owned computed column,
// plus optional validity tracking for null propagation.
auto merge_validity(const ValidityBitmap* a, const ValidityBitmap* b, std::size_t n)
    -> std::optional<ValidityBitmap> {
    if (!a && !b)
        return std::nullopt;
    if (!a)
        return ValidityBitmap(*b);
    if (!b)
        return ValidityBitmap(*a);
    if (a == b)
        return ValidityBitmap(*a);
    ValidityBitmap out(*a);
    constexpr std::size_t kBitsPerWord = sizeof(ValidityBitmap::word_type) * 8;
    const auto full_words = n / kBitsPerWord;
    const auto tail_bits = n % kBitsPerWord;
    auto* __restrict out_words = out.words_data();
    const auto* __restrict b_words = b->words_data();

    for (std::size_t w = 0; w < full_words; ++w) {
        out_words[w] &= b_words[w];
    }
    if (tail_bits != 0) {
        const auto mask = (ValidityBitmap::word_type{1} << tail_bits) - 1;
        out_words[full_words] = (out_words[full_words] & ~mask) |
                                ((out_words[full_words] & b_words[full_words]) & mask);
    }
    return out;
}

namespace {

auto pack_selected_bool_bits(std::uint64_t values, std::uint64_t mask) noexcept -> std::uint64_t {
#ifdef __BMI2__
    return _pext_u64(values, mask);
#else
    std::uint64_t packed = 0;
    unsigned out_bit = 0;
    while (mask != 0) {
        const auto bit = static_cast<unsigned>(std::countr_zero(mask));
        packed |= ((values >> bit) & std::uint64_t{1}) << out_bit;
        ++out_bit;
        mask &= (mask - 1);
    }
    return packed;
#endif
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto append_packed_bool_bits(std::uint64_t packed, std::size_t count,
                             Column<bool>::word_type* dst_words, std::size_t& out_bit) noexcept
    -> void {
    if (count == 0) {
        return;
    }
    constexpr std::size_t kBitsPerWord = sizeof(Column<bool>::word_type) * 8;
    const std::size_t dst_word = out_bit / kBitsPerWord;
    const auto shift = static_cast<unsigned>(out_bit % kBitsPerWord);
    dst_words[dst_word] |= packed << shift;
    if (shift != 0 && count > kBitsPerWord - shift) {
        dst_words[dst_word + 1] |= packed >> (kBitsPerWord - shift);
    }
    out_bit += count;
}

auto pack_mask_word_scalar(const std::uint8_t* mp, const std::uint8_t* vp, std::size_t lim) noexcept
    -> std::uint64_t {
    std::uint64_t bits = 0;
    for (std::size_t i = 0; i < lim; ++i) {
        const bool keep = vp ? ((mp[i] & vp[i]) != 0) : (mp[i] != 0);
        bits |= static_cast<std::uint64_t>(keep) << i;
    }
    return bits;
}

}  // namespace

#ifdef __AVX2__
namespace {

auto pack_mask_word_avx2(const std::uint8_t* mp) noexcept -> std::uint64_t {
    const __m256i zero = _mm256_setzero_si256();
    const __m256i lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(mp));
    const __m256i hi = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(mp + 32));
    const auto lo_bits =
        static_cast<std::uint32_t>(_mm256_movemask_epi8(_mm256_cmpgt_epi8(lo, zero)));
    const auto hi_bits =
        static_cast<std::uint32_t>(_mm256_movemask_epi8(_mm256_cmpgt_epi8(hi, zero)));
    return static_cast<std::uint64_t>(lo_bits) | (static_cast<std::uint64_t>(hi_bits) << 32);
}

}  // namespace
#endif

// Collect the merged validity bitmap for all column refs in an ir::Expr.
// Returns nullopt if no referenced column has a validity bitmap.
auto collect_expr_validity(const ir::Expr& expr, const Table& table, std::size_t n)
    -> std::optional<ValidityBitmap> {
    std::optional<ValidityBitmap> result;
    auto merge_in = [&](const ValidityBitmap* v) {
        if (!v)
            return;
        result = merge_validity(result ? &*result : nullptr, v, n);
    };
    std::function<void(const ir::Expr&)> walk = [&](const ir::Expr& e) {
        std::visit(
            [&](const auto& node) {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                    auto it = table.index.find(node.name);
                    if (it != table.index.end()) {
                        const auto& entry = table.columns[it->second];
                        if (entry.validity.has_value())
                            merge_in(&*entry.validity);
                    }
                } else if constexpr (std::is_same_v<T, ir::BinaryExpr>) {
                    walk(*node.left);
                    walk(*node.right);
                } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                    for (const auto& arg : node.args)
                        walk(*arg);
                }
                // ir::Literal — no column refs
            },
            e.node);
    };
    walk(expr);
    return result;
}

namespace {

// Flip a comparison operator (swap lhs and rhs).
auto flip_cmp(ir::CompareOp op) -> ir::CompareOp {
    switch (op) {
        case ir::CompareOp::Lt:
            return ir::CompareOp::Gt;
        case ir::CompareOp::Le:
            return ir::CompareOp::Ge;
        case ir::CompareOp::Gt:
            return ir::CompareOp::Lt;
        case ir::CompareOp::Ge:
            return ir::CompareOp::Le;
        default:
            return op;  // Eq, Ne are symmetric
    }
}

}  // namespace

// Element-wise arithmetic: result type = common_type<L, R>.
namespace {

template <typename L, typename R>

auto arith_into(ir::ArithmeticOp op, const L* __restrict lp, const R* __restrict rp,
                std::common_type_t<L, R>* __restrict dp, std::size_t n) -> void {
    using Out = std::common_type_t<L, R>;
    switch (op) {
        case ir::ArithmeticOp::Add:
            for (std::size_t i = 0; i < n; ++i)
                dp[i] = static_cast<Out>(lp[i]) + static_cast<Out>(rp[i]);
            break;
        case ir::ArithmeticOp::Sub:
            for (std::size_t i = 0; i < n; ++i)
                dp[i] = static_cast<Out>(lp[i]) - static_cast<Out>(rp[i]);
            break;
        case ir::ArithmeticOp::Mul:
            for (std::size_t i = 0; i < n; ++i)
                dp[i] = static_cast<Out>(lp[i]) * static_cast<Out>(rp[i]);
            break;
        case ir::ArithmeticOp::Div:
            if constexpr (std::is_integral_v<Out>)
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = safe_idiv<Out>(static_cast<Out>(lp[i]), static_cast<Out>(rp[i]));
            else
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = static_cast<Out>(lp[i]) / static_cast<Out>(rp[i]);
            break;
        case ir::ArithmeticOp::Mod:
            if constexpr (std::is_integral_v<Out>)
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = safe_imod<Out>(static_cast<Out>(lp[i]), static_cast<Out>(rp[i]));
            else
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = std::fmod(static_cast<Out>(lp[i]), static_cast<Out>(rp[i]));
            break;
    }
}

}  // namespace

// Dispatch arith_into over all numeric column-type combinations.
auto arith_vec(ir::ArithmeticOp op, const ColumnValue& lhs, const ColumnValue& rhs, std::size_t n)
    -> std::expected<ColumnValue, std::string> {
    // int64 × int64 → int64, except `/`: division yields Float64 regardless
    // of operand types — matching type inference and the fused numeric and
    // per-row paths (`20/0` → inf, not safe_idiv's 0). Only `%` stays
    // integral. Previously this truncated, so `sum(x*x)/sum(x)` in an
    // aggregate-broadcast field and `coalesce(a,b)/b` in a vectorized field
    // disagreed with plain `a/b`.
    if (const auto* l = std::get_if<Column<std::int64_t>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {
            if (op == ir::ArithmeticOp::Div) {
                Column<double> out;
                out.resize(n);
                double* __restrict dp = out.data();
                const std::int64_t* __restrict lp = l->data();
                const std::int64_t* __restrict rp = r->data();
                for (std::size_t i = 0; i < n; ++i) {
                    dp[i] = static_cast<double>(lp[i]) / static_cast<double>(rp[i]);
                }
                return ColumnValue{std::move(out)};
            }
            Column<std::int64_t> out;
            out.resize(n);
            arith_into(op, l->data(), r->data(), out.data(), n);
            return ColumnValue{std::move(out)};
        }
        if (const auto* r = std::get_if<Column<double>>(&rhs)) {  // int64 × double → double
            Column<double> out;
            out.resize(n);
            arith_into(op, l->data(), r->data(), out.data(), n);
            return ColumnValue{std::move(out)};
        }
    }
    if (const auto* l = std::get_if<Column<double>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {  // double × int64 → double
            Column<double> out;
            out.resize(n);
            arith_into(op, l->data(), r->data(), out.data(), n);
            return ColumnValue{std::move(out)};
        }
        if (const auto* r = std::get_if<Column<double>>(&rhs)) {  // double × double → double
            Column<double> out;
            out.resize(n);
            arith_into(op, l->data(), r->data(), out.data(), n);
            return ColumnValue{std::move(out)};
        }
    }
    if (op == ir::ArithmeticOp::Sub) {
        if (const auto* l = std::get_if<Column<Date>>(&lhs)) {
            if (const auto* r = std::get_if<Column<Date>>(&rhs)) {
                Column<std::int64_t> out;
                out.resize(n);
                for (std::size_t i = 0; i < n; ++i) {
                    out[i] = static_cast<std::int64_t>((*l)[i].days) -
                             static_cast<std::int64_t>((*r)[i].days);
                }
                return ColumnValue{std::move(out)};
            }
        }
    }
    return std::unexpected("filter: arithmetic on string column");
}

// Element-wise comparison between a column and a scalar literal.
// The scalar is hoisted out of the loop — no broadcast allocation.
namespace {

template <typename ColT, typename LitT>

auto cmp_col_scalar_into(ir::CompareOp op, const ColT* __restrict cp, LitT rv,
                         uint8_t* __restrict mp, std::size_t n) -> void {
    using Common = std::common_type_t<ColT, LitT>;
    const auto crv = static_cast<Common>(rv);
    switch (op) {
        case ir::CompareOp::Eq:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(cp[i]) == crv;
            break;
        case ir::CompareOp::Ne:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(cp[i]) != crv;
            break;
        case ir::CompareOp::Lt:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(cp[i]) < crv;
            break;
        case ir::CompareOp::Le:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(cp[i]) <= crv;
            break;
        case ir::CompareOp::Gt:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(cp[i]) > crv;
            break;
        case ir::CompareOp::Ge:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(cp[i]) >= crv;
            break;
    }
}

}  // namespace

namespace {

template <ir::CompareOp Op>

auto cmp_col_scalar_into_double_op(const double* __restrict cp, double rv, uint8_t* __restrict mp,
                                   std::size_t n) -> void {
    if constexpr (Op == ir::CompareOp::Eq) {
        for (std::size_t i = 0; i < n; ++i)
            mp[i] = cp[i] == rv;
    } else if constexpr (Op == ir::CompareOp::Ne) {
        for (std::size_t i = 0; i < n; ++i)
            mp[i] = cp[i] != rv;
    } else if constexpr (Op == ir::CompareOp::Lt) {
        for (std::size_t i = 0; i < n; ++i)
            mp[i] = cp[i] < rv;
    } else if constexpr (Op == ir::CompareOp::Le) {
        for (std::size_t i = 0; i < n; ++i)
            mp[i] = cp[i] <= rv;
    } else if constexpr (Op == ir::CompareOp::Gt) {
        for (std::size_t i = 0; i < n; ++i)
            mp[i] = cp[i] > rv;
    } else if constexpr (Op == ir::CompareOp::Ge) {
        for (std::size_t i = 0; i < n; ++i)
            mp[i] = cp[i] >= rv;
    }
}

auto cmp_col_scalar_into_double(ir::CompareOp op, const double* __restrict cp, double rv,
                                uint8_t* __restrict mp, std::size_t n) -> void {
    switch (op) {
        case ir::CompareOp::Eq:
            cmp_col_scalar_into_double_op<ir::CompareOp::Eq>(cp, rv, mp, n);
            break;
        case ir::CompareOp::Ne:
            cmp_col_scalar_into_double_op<ir::CompareOp::Ne>(cp, rv, mp, n);
            break;
        case ir::CompareOp::Lt:
            cmp_col_scalar_into_double_op<ir::CompareOp::Lt>(cp, rv, mp, n);
            break;
        case ir::CompareOp::Le:
            cmp_col_scalar_into_double_op<ir::CompareOp::Le>(cp, rv, mp, n);
            break;
        case ir::CompareOp::Gt:
            cmp_col_scalar_into_double_op<ir::CompareOp::Gt>(cp, rv, mp, n);
            break;
        case ir::CompareOp::Ge:
            cmp_col_scalar_into_double_op<ir::CompareOp::Ge>(cp, rv, mp, n);
            break;
    }
}

}  // namespace

// Dispatch column-vs-scalar comparison over all type combinations.
using LitVal = std::variant<std::int64_t, double, bool, std::string, Date, Timestamp>;
namespace {

auto compare_col_scalar(ir::CompareOp op, const ColumnValue& col, const LitVal& lit, std::size_t n,
                        const ValidityBitmap* validity = nullptr)
    -> std::expected<Mask, std::string> {
    Mask result;
    result.value.resize(n);
    uint8_t* mp = result.value.data();
    if (const auto* s = std::get_if<std::string>(&lit)) {
        if (const auto* str_col = std::get_if<Column<std::string>>(&col)) {
            switch (op) {
                case ir::CompareOp::Eq:
                    for (std::size_t i = 0; i < n; ++i)
                        mp[i] = std::string_view((*str_col)[i]) == *s;
                    break;
                case ir::CompareOp::Ne:
                    for (std::size_t i = 0; i < n; ++i)
                        mp[i] = std::string_view((*str_col)[i]) != *s;
                    break;
                case ir::CompareOp::Lt:
                    for (std::size_t i = 0; i < n; ++i)
                        mp[i] = std::string_view((*str_col)[i]) < *s;
                    break;
                case ir::CompareOp::Le:
                    for (std::size_t i = 0; i < n; ++i)
                        mp[i] = std::string_view((*str_col)[i]) <= *s;
                    break;
                case ir::CompareOp::Gt:
                    for (std::size_t i = 0; i < n; ++i)
                        mp[i] = std::string_view((*str_col)[i]) > *s;
                    break;
                case ir::CompareOp::Ge:
                    for (std::size_t i = 0; i < n; ++i)
                        mp[i] = std::string_view((*str_col)[i]) >= *s;
                    break;
            }
            result.apply_validity(validity, n);
            return result;
        }
        if (const auto* cat_col = std::get_if<Column<Categorical>>(&col)) {
            if (auto code = cat_col->find_code(*s)) {
                const auto* codes = cat_col->codes().data();
                switch (op) {
                    case ir::CompareOp::Eq:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = codes[i] == *code;
                        break;
                    case ir::CompareOp::Ne:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = codes[i] != *code;
                        break;
                    case ir::CompareOp::Lt:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = std::string_view((*cat_col)[i]) < *s;
                        break;
                    case ir::CompareOp::Le:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = std::string_view((*cat_col)[i]) <= *s;
                        break;
                    case ir::CompareOp::Gt:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = std::string_view((*cat_col)[i]) > *s;
                        break;
                    case ir::CompareOp::Ge:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = std::string_view((*cat_col)[i]) >= *s;
                        break;
                }
                result.apply_validity(validity, n);
                return result;
            }
            if (op == ir::CompareOp::Eq || op == ir::CompareOp::Ne) {
                const uint8_t v = (op == ir::CompareOp::Ne) ? 1 : 0;
                std::fill(mp, mp + n, v);
                result.apply_validity(validity, n);
                return result;
            }
            for (std::size_t i = 0; i < n; ++i) {
                const std::string_view cv = (*cat_col)[i];
                switch (op) {
                    case ir::CompareOp::Lt:
                        mp[i] = cv < *s;
                        break;
                    case ir::CompareOp::Le:
                        mp[i] = cv <= *s;
                        break;
                    case ir::CompareOp::Gt:
                        mp[i] = cv > *s;
                        break;
                    case ir::CompareOp::Ge:
                        mp[i] = cv >= *s;
                        break;
                    default:
                        mp[i] = 0;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
        return std::unexpected("filter: cannot compare string and numeric");
    }

    if (const auto* date_value = std::get_if<Date>(&lit)) {
        if (const auto* date_col = std::get_if<Column<Date>>(&col)) {
            const auto rhs = date_value->days;
            for (std::size_t idx = 0; idx < n; ++idx) {
                const auto lhs = date_col->data()[idx].days;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[idx] = lhs == rhs;
                        break;
                    case ir::CompareOp::Ne:
                        mp[idx] = lhs != rhs;
                        break;
                    case ir::CompareOp::Lt:
                        mp[idx] = lhs < rhs;
                        break;
                    case ir::CompareOp::Le:
                        mp[idx] = lhs <= rhs;
                        break;
                    case ir::CompareOp::Gt:
                        mp[idx] = lhs > rhs;
                        break;
                    case ir::CompareOp::Ge:
                        mp[idx] = lhs >= rhs;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
        return std::unexpected("filter: cannot compare date and non-date");
    }

    if (const auto* ts_value = std::get_if<Timestamp>(&lit)) {
        if (const auto* ts_col = std::get_if<Column<Timestamp>>(&col)) {
            const auto rhs = ts_value->nanos;
            for (std::size_t idx = 0; idx < n; ++idx) {
                const auto lhs = ts_col->data()[idx].nanos;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[idx] = lhs == rhs;
                        break;
                    case ir::CompareOp::Ne:
                        mp[idx] = lhs != rhs;
                        break;
                    case ir::CompareOp::Lt:
                        mp[idx] = lhs < rhs;
                        break;
                    case ir::CompareOp::Le:
                        mp[idx] = lhs <= rhs;
                        break;
                    case ir::CompareOp::Gt:
                        mp[idx] = lhs > rhs;
                        break;
                    case ir::CompareOp::Ge:
                        mp[idx] = lhs >= rhs;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
        return std::unexpected("filter: cannot compare timestamp and non-timestamp");
    }

    if (const auto* int_col = std::get_if<Column<std::int64_t>>(&col)) {
        const std::int64_t* cp = int_col->data();
        if (const auto* i = std::get_if<std::int64_t>(&lit)) {
            cmp_col_scalar_into(op, cp, *i, mp, n);
            result.apply_validity(validity, n);
            return result;
        }
        if (const auto* d = std::get_if<double>(&lit)) {
            cmp_col_scalar_into(op, cp, *d, mp, n);
            result.apply_validity(validity, n);
            return result;
        }
    }
    if (const auto* dbl_col = std::get_if<Column<double>>(&col)) {
        const double* cp = dbl_col->data();
        if (const auto* i = std::get_if<std::int64_t>(&lit)) {
            cmp_col_scalar_into_double(op, cp, static_cast<double>(*i), mp, n);
            result.apply_validity(validity, n);
            return result;
        }
        if (const auto* d = std::get_if<double>(&lit)) {
            cmp_col_scalar_into_double(op, cp, *d, mp, n);
            result.apply_validity(validity, n);
            return result;
        }
    }
    if (const auto* date_col = std::get_if<Column<Date>>(&col)) {
        if (const auto* i = std::get_if<std::int64_t>(&lit)) {
            const std::int64_t rhs = *i;
            for (std::size_t idx = 0; idx < n; ++idx) {
                const std::int64_t lhs = date_col->data()[idx].days;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[idx] = lhs == rhs;
                        break;
                    case ir::CompareOp::Ne:
                        mp[idx] = lhs != rhs;
                        break;
                    case ir::CompareOp::Lt:
                        mp[idx] = lhs < rhs;
                        break;
                    case ir::CompareOp::Le:
                        mp[idx] = lhs <= rhs;
                        break;
                    case ir::CompareOp::Gt:
                        mp[idx] = lhs > rhs;
                        break;
                    case ir::CompareOp::Ge:
                        mp[idx] = lhs >= rhs;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
        if (const auto* d = std::get_if<double>(&lit)) {
            const double rhs = *d;
            for (std::size_t idx = 0; idx < n; ++idx) {
                const auto lhs = static_cast<double>(date_col->data()[idx].days);
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[idx] = lhs == rhs;
                        break;
                    case ir::CompareOp::Ne:
                        mp[idx] = lhs != rhs;
                        break;
                    case ir::CompareOp::Lt:
                        mp[idx] = lhs < rhs;
                        break;
                    case ir::CompareOp::Le:
                        mp[idx] = lhs <= rhs;
                        break;
                    case ir::CompareOp::Gt:
                        mp[idx] = lhs > rhs;
                        break;
                    case ir::CompareOp::Ge:
                        mp[idx] = lhs >= rhs;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
    }
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&col)) {
        if (const auto* i = std::get_if<std::int64_t>(&lit)) {
            const std::int64_t rhs = *i;
            for (std::size_t idx = 0; idx < n; ++idx) {
                const std::int64_t lhs = ts_col->data()[idx].nanos;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[idx] = lhs == rhs;
                        break;
                    case ir::CompareOp::Ne:
                        mp[idx] = lhs != rhs;
                        break;
                    case ir::CompareOp::Lt:
                        mp[idx] = lhs < rhs;
                        break;
                    case ir::CompareOp::Le:
                        mp[idx] = lhs <= rhs;
                        break;
                    case ir::CompareOp::Gt:
                        mp[idx] = lhs > rhs;
                        break;
                    case ir::CompareOp::Ge:
                        mp[idx] = lhs >= rhs;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
        if (const auto* d = std::get_if<double>(&lit)) {
            const double rhs = *d;
            for (std::size_t idx = 0; idx < n; ++idx) {
                const auto lhs = static_cast<double>(ts_col->data()[idx].nanos);
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[idx] = lhs == rhs;
                        break;
                    case ir::CompareOp::Ne:
                        mp[idx] = lhs != rhs;
                        break;
                    case ir::CompareOp::Lt:
                        mp[idx] = lhs < rhs;
                        break;
                    case ir::CompareOp::Le:
                        mp[idx] = lhs <= rhs;
                        break;
                    case ir::CompareOp::Gt:
                        mp[idx] = lhs > rhs;
                        break;
                    case ir::CompareOp::Ge:
                        mp[idx] = lhs >= rhs;
                        break;
                }
            }
            result.apply_validity(validity, n);
            return result;
        }
    }

    return std::unexpected("filter: cannot compare string and numeric");
}

}  // namespace

// Element-wise comparison between two full columns.
namespace {

template <typename L, typename R>

auto cmp_into(ir::CompareOp op, const L* __restrict lp, const R* __restrict rp,
              uint8_t* __restrict mp, std::size_t n) -> void {
    using Common = std::common_type_t<L, R>;
    switch (op) {
        case ir::CompareOp::Eq:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(lp[i]) == static_cast<Common>(rp[i]);
            break;
        case ir::CompareOp::Ne:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(lp[i]) != static_cast<Common>(rp[i]);
            break;
        case ir::CompareOp::Lt:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(lp[i]) < static_cast<Common>(rp[i]);
            break;
        case ir::CompareOp::Le:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(lp[i]) <= static_cast<Common>(rp[i]);
            break;
        case ir::CompareOp::Gt:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(lp[i]) > static_cast<Common>(rp[i]);
            break;
        case ir::CompareOp::Ge:
            for (std::size_t i = 0; i < n; ++i)
                mp[i] = static_cast<Common>(lp[i]) >= static_cast<Common>(rp[i]);
            break;
    }
}

// Dispatch column-vs-column comparison over all type combinations.
auto compare_vec(ir::CompareOp op, const ColumnValue& lhs, const ColumnValue& rhs, std::size_t n,
                 const ValidityBitmap* lv = nullptr, const ValidityBitmap* rv = nullptr)
    -> std::expected<Mask, std::string> {
    Mask result;
    result.value.resize(n);
    uint8_t* mp = result.value.data();
    if (const auto* l = std::get_if<Column<std::int64_t>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            {
                auto merged_v = merge_validity(lv, rv, n);
                result.apply_validity(merged_v ? &*merged_v : nullptr, n);
                return result;
            }
        }
        if (const auto* r = std::get_if<Column<double>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            {
                auto merged_v = merge_validity(lv, rv, n);
                result.apply_validity(merged_v ? &*merged_v : nullptr, n);
                return result;
            }
        }
    }
    if (const auto* l = std::get_if<Column<double>>(&lhs)) {
        if (const auto* r = std::get_if<Column<double>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            {
                auto merged_v = merge_validity(lv, rv, n);
                result.apply_validity(merged_v ? &*merged_v : nullptr, n);
                return result;
            }
        }
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            {
                auto merged_v = merge_validity(lv, rv, n);
                result.apply_validity(merged_v ? &*merged_v : nullptr, n);
                return result;
            }
        }
    }
    if (const auto* l = std::get_if<Column<Date>>(&lhs)) {
        if (const auto* r = std::get_if<Column<Date>>(&rhs)) {
            for (std::size_t i = 0; i < n; ++i) {
                const auto left_value = l->data()[i].days;
                const auto right_value = r->data()[i].days;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[i] = left_value == right_value;
                        break;
                    case ir::CompareOp::Ne:
                        mp[i] = left_value != right_value;
                        break;
                    case ir::CompareOp::Lt:
                        mp[i] = left_value < right_value;
                        break;
                    case ir::CompareOp::Le:
                        mp[i] = left_value <= right_value;
                        break;
                    case ir::CompareOp::Gt:
                        mp[i] = left_value > right_value;
                        break;
                    case ir::CompareOp::Ge:
                        mp[i] = left_value >= right_value;
                        break;
                }
            }
            {
                auto merged_v = merge_validity(lv, rv, n);
                result.apply_validity(merged_v ? &*merged_v : nullptr, n);
                return result;
            }
        }
    }
    if (const auto* l = std::get_if<Column<Timestamp>>(&lhs)) {
        if (const auto* r = std::get_if<Column<Timestamp>>(&rhs)) {
            for (std::size_t i = 0; i < n; ++i) {
                const auto left_value = l->data()[i].nanos;
                const auto right_value = r->data()[i].nanos;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[i] = left_value == right_value;
                        break;
                    case ir::CompareOp::Ne:
                        mp[i] = left_value != right_value;
                        break;
                    case ir::CompareOp::Lt:
                        mp[i] = left_value < right_value;
                        break;
                    case ir::CompareOp::Le:
                        mp[i] = left_value <= right_value;
                        break;
                    case ir::CompareOp::Gt:
                        mp[i] = left_value > right_value;
                        break;
                    case ir::CompareOp::Ge:
                        mp[i] = left_value >= right_value;
                        break;
                }
            }
            {
                auto merged_v = merge_validity(lv, rv, n);
                result.apply_validity(merged_v ? &*merged_v : nullptr, n);
                return result;
            }
        }
    }
    auto cmp_string_views = [&](auto&& lcol, auto&& rcol) {
        switch (op) {
            case ir::CompareOp::Eq:
                for (std::size_t i = 0; i < n; ++i)
                    mp[i] = std::string_view(lcol[i]) == std::string_view(rcol[i]);
                break;
            case ir::CompareOp::Ne:
                for (std::size_t i = 0; i < n; ++i)
                    mp[i] = std::string_view(lcol[i]) != std::string_view(rcol[i]);
                break;
            case ir::CompareOp::Lt:
                for (std::size_t i = 0; i < n; ++i)
                    mp[i] = std::string_view(lcol[i]) < std::string_view(rcol[i]);
                break;
            case ir::CompareOp::Le:
                for (std::size_t i = 0; i < n; ++i)
                    mp[i] = std::string_view(lcol[i]) <= std::string_view(rcol[i]);
                break;
            case ir::CompareOp::Gt:
                for (std::size_t i = 0; i < n; ++i)
                    mp[i] = std::string_view(lcol[i]) > std::string_view(rcol[i]);
                break;
            case ir::CompareOp::Ge:
                for (std::size_t i = 0; i < n; ++i)
                    mp[i] = std::string_view(lcol[i]) >= std::string_view(rcol[i]);
                break;
        }
    };
    auto return_with_validity = [&] -> Mask {
        auto merged_v = merge_validity(lv, rv, n);
        result.apply_validity(merged_v ? &*merged_v : nullptr, n);
        return std::move(result);
    };

    if (const auto* l = std::get_if<Column<std::string>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::string>>(&rhs)) {
            cmp_string_views(*l, *r);
            return return_with_validity();
        }
        if (const auto* r = std::get_if<Column<Categorical>>(&rhs)) {
            cmp_string_views(*l, *r);
            return return_with_validity();
        }
    }
    if (const auto* l = std::get_if<Column<Categorical>>(&lhs)) {
        if (const auto* r = std::get_if<Column<Categorical>>(&rhs)) {
            if (l->dictionary_ptr() == r->dictionary_ptr()) {
                const auto* lc = l->codes().data();
                const auto* rc = r->codes().data();
                switch (op) {
                    case ir::CompareOp::Eq:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = lc[i] == rc[i];
                        break;
                    case ir::CompareOp::Ne:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = lc[i] != rc[i];
                        break;
                    case ir::CompareOp::Lt:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = lc[i] < rc[i];
                        break;
                    case ir::CompareOp::Le:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = lc[i] <= rc[i];
                        break;
                    case ir::CompareOp::Gt:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = lc[i] > rc[i];
                        break;
                    case ir::CompareOp::Ge:
                        for (std::size_t i = 0; i < n; ++i)
                            mp[i] = lc[i] >= rc[i];
                        break;
                }
                return return_with_validity();
            }
            cmp_string_views(*l, *r);
            return return_with_validity();
        }
        if (const auto* r = std::get_if<Column<std::string>>(&rhs)) {
            cmp_string_views(*l, *r);
            return return_with_validity();
        }
    }
    return std::unexpected("filter: incompatible column types in comparison");
}

enum class NumericSpecKind : std::uint8_t {
    Int64,
    Double,
};

struct NumericCmpSpec {
    NumericSpecKind kind{};
    ir::CompareOp op{};
    const std::int64_t* i64 = nullptr;
    const double* dbl = nullptr;
    bool lit_is_int = false;
    std::int64_t lit_i64 = 0;
    double lit_dbl = 0.0;
};

auto try_extract_numeric_cmp_spec(const ir::Expr& expr, const Table& table)
    -> std::optional<NumericCmpSpec> {
    const auto* cmp = std::get_if<ir::CompareExpr>(&expr.node);
    if (cmp == nullptr) {
        return std::nullopt;
    }

    const ir::ColumnRef* col_node = nullptr;
    const ir::Literal* lit_node = nullptr;
    ir::CompareOp op = cmp->op;
    if (const auto* lcol = std::get_if<ir::ColumnRef>(&cmp->left->node)) {
        if (const auto* rlit = std::get_if<ir::Literal>(&cmp->right->node)) {
            col_node = lcol;
            lit_node = rlit;
        }
    }
    if (col_node == nullptr) {
        if (const auto* llit = std::get_if<ir::Literal>(&cmp->left->node)) {
            if (const auto* rcol = std::get_if<ir::ColumnRef>(&cmp->right->node)) {
                col_node = rcol;
                lit_node = llit;
                op = flip_cmp(op);
            }
        }
    }
    if (col_node == nullptr || lit_node == nullptr) {
        return std::nullopt;
    }

    auto it = table.index.find(col_node->name);
    if (it == table.index.end()) {
        return std::nullopt;
    }
    const auto& entry = table.columns[it->second];
    if (entry.validity.has_value()) {
        return std::nullopt;  // 3VL path handles null semantics
    }

    NumericCmpSpec spec{};
    spec.op = op;
    if (const auto* int_column = std::get_if<Column<std::int64_t>>(entry.column.get())) {
        spec.kind = NumericSpecKind::Int64;
        spec.i64 = int_column->data();
    } else if (const auto* double_column = std::get_if<Column<double>>(entry.column.get())) {
        spec.kind = NumericSpecKind::Double;
        spec.dbl = double_column->data();
    } else {
        return std::nullopt;
    }

    if (const auto* i = std::get_if<std::int64_t>(&lit_node->value)) {
        spec.lit_is_int = true;
        spec.lit_i64 = *i;
        spec.lit_dbl = static_cast<double>(*i);
        return spec;
    }
    if (const auto* d = std::get_if<double>(&lit_node->value)) {
        spec.lit_is_int = false;
        spec.lit_dbl = *d;
        return spec;
    }
    return std::nullopt;
}

struct NumericOperandSpec {
    NumericSpecKind kind{};
    bool is_lit = false;
    const std::int64_t* i64 = nullptr;
    const double* dbl = nullptr;
    std::int64_t lit_i64 = 0;
    double lit_dbl = 0.0;
};

struct NumericArithCmpSpec {
    ir::ArithmeticOp arith_op{};
    ir::CompareOp cmp_op{};
    NumericOperandSpec lhs;
    NumericOperandSpec rhs;
    bool lit_is_int = false;
    std::int64_t lit_i64 = 0;
    double lit_dbl = 0.0;
};

auto try_extract_numeric_operand_spec(const ir::Expr& expr, const Table& table)
    -> std::optional<NumericOperandSpec> {
    NumericOperandSpec spec{};
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        spec.is_lit = true;
        if (const auto* i = std::get_if<std::int64_t>(&lit->value)) {
            spec.kind = NumericSpecKind::Int64;
            spec.lit_i64 = *i;
            spec.lit_dbl = static_cast<double>(*i);
            return spec;
        }
        if (const auto* d = std::get_if<double>(&lit->value)) {
            spec.kind = NumericSpecKind::Double;
            spec.lit_dbl = *d;
            return spec;
        }
        return std::nullopt;
    }

    const auto* col_node = std::get_if<ir::ColumnRef>(&expr.node);
    if (col_node == nullptr) {
        return std::nullopt;
    }
    auto it = table.index.find(col_node->name);
    if (it == table.index.end()) {
        return std::nullopt;
    }
    const auto& entry = table.columns[it->second];
    if (entry.validity.has_value()) {
        return std::nullopt;
    }
    if (const auto* int_column = std::get_if<Column<std::int64_t>>(entry.column.get())) {
        spec.kind = NumericSpecKind::Int64;
        spec.i64 = int_column->data();
        return spec;
    }
    if (const auto* double_column = std::get_if<Column<double>>(entry.column.get())) {
        spec.kind = NumericSpecKind::Double;
        spec.dbl = double_column->data();
        return spec;
    }
    return std::nullopt;
}

auto try_extract_numeric_arith_cmp_spec(const ir::CompareExpr& cmp, const Table& table)
    -> std::optional<NumericArithCmpSpec> {
    const ir::BinaryExpr* bin = nullptr;
    const ir::Literal* lit = nullptr;
    ir::CompareOp op = cmp.op;

    if (const auto* lbin = std::get_if<ir::BinaryExpr>(&cmp.left->node)) {
        if (const auto* rlit = std::get_if<ir::Literal>(&cmp.right->node)) {
            bin = lbin;
            lit = rlit;
        }
    }
    if (bin == nullptr) {
        if (const auto* llit = std::get_if<ir::Literal>(&cmp.left->node)) {
            if (const auto* rbin = std::get_if<ir::BinaryExpr>(&cmp.right->node)) {
                bin = rbin;
                lit = llit;
                op = flip_cmp(op);
            }
        }
    }
    if (bin == nullptr || lit == nullptr) {
        return std::nullopt;
    }

    auto lhs = try_extract_numeric_operand_spec(*bin->left, table);
    auto rhs = try_extract_numeric_operand_spec(*bin->right, table);
    if (!lhs || !rhs) {
        return std::nullopt;
    }

    NumericArithCmpSpec spec{};
    spec.arith_op = bin->op;
    spec.cmp_op = op;
    spec.lhs = *lhs;
    spec.rhs = *rhs;
    if (const auto* i = std::get_if<std::int64_t>(&lit->value)) {
        spec.lit_is_int = true;
        spec.lit_i64 = *i;
        spec.lit_dbl = static_cast<double>(*i);
        return spec;
    }
    if (const auto* d = std::get_if<double>(&lit->value)) {
        spec.lit_is_int = false;
        spec.lit_dbl = *d;
        return spec;
    }
    return std::nullopt;
}

}  // namespace

namespace {

template <ir::CompareOp Op, typename L, typename R>

auto cmp_eval(L lhs, R rhs) -> uint8_t {
    using C = std::common_type_t<L, R>;
    const C l = static_cast<C>(lhs);
    const C r = static_cast<C>(rhs);
    if constexpr (Op == ir::CompareOp::Eq) {
        return l == r;
    } else if constexpr (Op == ir::CompareOp::Ne) {
        return l != r;
    } else if constexpr (Op == ir::CompareOp::Lt) {
        return l < r;
    } else if constexpr (Op == ir::CompareOp::Le) {
        return l <= r;
    } else if constexpr (Op == ir::CompareOp::Gt) {
        return l > r;
    } else {
        return l >= r;
    }
}

}  // namespace

namespace {

template <typename T>

struct NumericColumnOperand {
    const T* data = nullptr;
    [[nodiscard]] auto value(std::size_t i) const noexcept -> T { return data[i]; }
};

}  // namespace

namespace {

template <typename T>

struct NumericLiteralOperand {
    T literal{};
    [[nodiscard]] auto value(std::size_t /*unused*/) const noexcept -> T { return literal; }
};

template <typename Fn>

auto visit_numeric_operand(const NumericOperandSpec& spec, Fn&& fn) -> void {
    if (spec.kind == NumericSpecKind::Int64) {
        if (spec.is_lit) {
            fn(NumericLiteralOperand<std::int64_t>{spec.lit_i64});
        } else {
            fn(NumericColumnOperand<std::int64_t>{spec.i64});
        }
    } else {
        if (spec.is_lit) {
            fn(NumericLiteralOperand<double>{spec.lit_dbl});
        } else {
            fn(NumericColumnOperand<double>{spec.dbl});
        }
    }
}

}  // namespace

namespace {

template <ir::ArithmeticOp Op, typename L, typename R>

auto arith_eval(L lhs, R rhs) -> std::common_type_t<L, R> {
    using Out = std::common_type_t<L, R>;
    const Out l = static_cast<Out>(lhs);
    const Out r = static_cast<Out>(rhs);
    if constexpr (Op == ir::ArithmeticOp::Add) {
        return l + r;
    } else if constexpr (Op == ir::ArithmeticOp::Sub) {
        return l - r;
    } else if constexpr (Op == ir::ArithmeticOp::Mul) {
        return l * r;
    } else if constexpr (Op == ir::ArithmeticOp::Div) {
        if constexpr (std::is_integral_v<Out>) {
            return safe_idiv<Out>(l, r);
        } else {
            return l / r;
        }
    } else {
        if constexpr (std::is_integral_v<Out>) {
            return safe_imod<Out>(l, r);
        } else {
            return std::fmod(l, r);
        }
    }
}

}  // namespace

namespace {

template <ir::ArithmeticOp ArithOp, ir::CompareOp CmpOp, typename Lhs, typename Rhs, typename Lit>

auto numeric_arith_cmp_mask(Lhs lhs, Rhs rhs, Lit lit, uint8_t* __restrict out, std::size_t n)
    -> void {
    for (std::size_t i = 0; i < n; ++i) {
        out[i] = cmp_eval<CmpOp>(arith_eval<ArithOp>(lhs.value(i), rhs.value(i)), lit);
    }
}

}  // namespace

namespace {

template <ir::ArithmeticOp ArithOp, typename Lhs, typename Rhs, typename Lit>

auto dispatch_numeric_arith_cmp_op(ir::CompareOp cmp_op, Lhs lhs, Rhs rhs, Lit lit, uint8_t* out,
                                   std::size_t n) -> void {
    switch (cmp_op) {
        case ir::CompareOp::Eq:
            numeric_arith_cmp_mask<ArithOp, ir::CompareOp::Eq>(lhs, rhs, lit, out, n);
            break;
        case ir::CompareOp::Ne:
            numeric_arith_cmp_mask<ArithOp, ir::CompareOp::Ne>(lhs, rhs, lit, out, n);
            break;
        case ir::CompareOp::Lt:
            numeric_arith_cmp_mask<ArithOp, ir::CompareOp::Lt>(lhs, rhs, lit, out, n);
            break;
        case ir::CompareOp::Le:
            numeric_arith_cmp_mask<ArithOp, ir::CompareOp::Le>(lhs, rhs, lit, out, n);
            break;
        case ir::CompareOp::Gt:
            numeric_arith_cmp_mask<ArithOp, ir::CompareOp::Gt>(lhs, rhs, lit, out, n);
            break;
        case ir::CompareOp::Ge:
            numeric_arith_cmp_mask<ArithOp, ir::CompareOp::Ge>(lhs, rhs, lit, out, n);
            break;
    }
}

}  // namespace

namespace {

template <typename Lhs, typename Rhs, typename Lit>

auto dispatch_numeric_arith_op(ir::ArithmeticOp arith_op, ir::CompareOp cmp_op, Lhs lhs, Rhs rhs,
                               Lit lit, uint8_t* out, std::size_t n) -> void {
    switch (arith_op) {
        case ir::ArithmeticOp::Add:
            dispatch_numeric_arith_cmp_op<ir::ArithmeticOp::Add>(cmp_op, lhs, rhs, lit, out, n);
            break;
        case ir::ArithmeticOp::Sub:
            dispatch_numeric_arith_cmp_op<ir::ArithmeticOp::Sub>(cmp_op, lhs, rhs, lit, out, n);
            break;
        case ir::ArithmeticOp::Mul:
            dispatch_numeric_arith_cmp_op<ir::ArithmeticOp::Mul>(cmp_op, lhs, rhs, lit, out, n);
            break;
        case ir::ArithmeticOp::Div:
            dispatch_numeric_arith_cmp_op<ir::ArithmeticOp::Div>(cmp_op, lhs, rhs, lit, out, n);
            break;
        case ir::ArithmeticOp::Mod:
            dispatch_numeric_arith_cmp_op<ir::ArithmeticOp::Mod>(cmp_op, lhs, rhs, lit, out, n);
            break;
    }
}

auto dispatch_numeric_arith_cmp_kernel(const NumericArithCmpSpec& spec, uint8_t* out, std::size_t n)
    -> void {
    visit_numeric_operand(spec.lhs, [&](auto lhs) {
        visit_numeric_operand(spec.rhs, [&](auto rhs) {
            if (spec.lit_is_int) {
                dispatch_numeric_arith_op(spec.arith_op, spec.cmp_op, lhs, rhs, spec.lit_i64, out,
                                          n);
            } else {
                dispatch_numeric_arith_op(spec.arith_op, spec.cmp_op, lhs, rhs, spec.lit_dbl, out,
                                          n);
            }
        });
    });
}

}  // namespace

namespace {

template <ir::CompareOp LOp, ir::CompareOp ROp, bool UseAnd, typename L, typename LLit, typename R,
          typename RLit>

auto cmp_pair_mask(const L* __restrict lhs_data, LLit lhs_lit, const R* __restrict rhs_data,
                   RLit rhs_lit, uint8_t* __restrict out, std::size_t n) -> void {
    for (std::size_t i = 0; i < n; ++i) {
        const uint8_t l = cmp_eval<LOp>(lhs_data[i], lhs_lit);
        const uint8_t r = cmp_eval<ROp>(rhs_data[i], rhs_lit);
        out[i] = UseAnd ? (l & r) : (l | r);
    }
}

}  // namespace

namespace {

template <bool UseAnd, typename L, typename LLit, typename R, typename RLit>

auto dispatch_cmp_pair_ops(ir::CompareOp lop, ir::CompareOp rop, const L* lhs_data, LLit lhs_lit,
                           const R* rhs_data, RLit rhs_lit, uint8_t* out, std::size_t n) -> void {
    auto dispatch_right = [&](auto left_op_tag) {
        constexpr ir::CompareOp LOp = decltype(left_op_tag)::value;
        auto apply_right = [&](auto right_op_tag) {
            constexpr ir::CompareOp ROp = decltype(right_op_tag)::value;
            cmp_pair_mask<LOp, ROp, UseAnd>(lhs_data, lhs_lit, rhs_data, rhs_lit, out, n);
        };
        switch (rop) {
            case ir::CompareOp::Eq:
                apply_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Eq>{});
                break;
            case ir::CompareOp::Ne:
                apply_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Ne>{});
                break;
            case ir::CompareOp::Lt:
                apply_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Lt>{});
                break;
            case ir::CompareOp::Le:
                apply_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Le>{});
                break;
            case ir::CompareOp::Gt:
                apply_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Gt>{});
                break;
            case ir::CompareOp::Ge:
                apply_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Ge>{});
                break;
        }
    };
    switch (lop) {
        case ir::CompareOp::Eq:
            dispatch_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Eq>{});
            break;
        case ir::CompareOp::Ne:
            dispatch_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Ne>{});
            break;
        case ir::CompareOp::Lt:
            dispatch_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Lt>{});
            break;
        case ir::CompareOp::Le:
            dispatch_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Le>{});
            break;
        case ir::CompareOp::Gt:
            dispatch_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Gt>{});
            break;
        case ir::CompareOp::Ge:
            dispatch_right(std::integral_constant<ir::CompareOp, ir::CompareOp::Ge>{});
            break;
    }
}

}  // namespace

namespace {

template <bool UseAnd>

auto dispatch_numeric_cmp_pair_kernel(const NumericCmpSpec& lhs_spec,
                                      const NumericCmpSpec& rhs_spec, uint8_t* out, std::size_t n)
    -> void {
    auto run_rhs = [&](auto* lhs_data, auto lhs_lit) {
        auto apply_rhs = [&](auto* rhs_data, auto rhs_lit) {
            dispatch_cmp_pair_ops<UseAnd>(lhs_spec.op, rhs_spec.op, lhs_data, lhs_lit, rhs_data,
                                          rhs_lit, out, n);
        };
        if (rhs_spec.kind == NumericSpecKind::Int64) {
            if (rhs_spec.lit_is_int) {
                apply_rhs(rhs_spec.i64, rhs_spec.lit_i64);
            } else {
                apply_rhs(rhs_spec.i64, rhs_spec.lit_dbl);
            }
        } else {
            apply_rhs(rhs_spec.dbl, rhs_spec.lit_dbl);
        }
    };
    if (lhs_spec.kind == NumericSpecKind::Int64) {
        if (lhs_spec.lit_is_int) {
            run_rhs(lhs_spec.i64, lhs_spec.lit_i64);
        } else {
            run_rhs(lhs_spec.i64, lhs_spec.lit_dbl);
        }
    } else {
        run_rhs(lhs_spec.dbl, lhs_spec.lit_dbl);
    }
}

}  // namespace

// The vectorized predicate evaluator below dispatches the same way the
// select/update field evaluator does, rather than reimplementing functions.
// Row-wise calls go to evaluate_field_column (which consults the builtin
// registry's Scalar entries: casts, ceil/floor/trunc, round, math, date parts,
// pmin/pmax, is_nan); whole-column builtins (Transform/Generator — rolling,
// cum, lag/lead, fill, rand_*, rep) dispatch through the same registry's
// column_eval, so eval_value_vec treats them as column leaves that can nest
// inside arithmetic. Nothing is duplicated here.

// Boolean predicate evaluator (comparisons, logical, is_null). eval_value_vec
// routes boolean-producing nodes here so a Bool result can be used in value
// position (e.g. `update { flag = x > 5 }`, `update { miss = is_null(x) }`).

// True if a field expression must go through the vectorized, validity-aware
// path (a boolean node or a whole-column builtin call anywhere in the tree).
// Defined far below; eval_value_vec needs it to decide which arguments of a
// scalar call to materialize.

namespace {

// Evaluate a scalar call (abs, sqrt, casts, …) whose arguments nest a
// non-row-local sub-expression (a Transform/Generator): materialize those arguments
// into columns, then apply the scalar function over them. Defined after
// eval_value_vec (it recurses into it).
auto eval_scalar_over_columns(const ir::CallExpr& call, const Table& table,
                              const ScalarRegistry* scalars, std::size_t n)
    -> std::expected<ColResult, std::string>;

// Turn a boolean Mask into a `Column<Bool>` ColResult (3VL nulls -> validity).
inline auto mask_to_bool_result(Mask m, std::size_t n) -> ColResult {
    Column<bool> col;
    col.resize(n);
    for (std::size_t i = 0; i < n; ++i) {
        col.set(i, m.value[i] != 0);
    }
    ColResult r{ColumnValue{std::move(col)}};
    if (m.valid.has_value()) {
        ValidityBitmap vb(n, false);
        for (std::size_t i = 0; i < n; ++i) {
            vb.set(i, (*m.valid)[i] != 0);
        }
        r.owned_validity = std::move(vb);
    }
    return r;
}

}  // namespace

// Evaluate a value sub-expression over all n rows, returning a column.
// Returns a pointer into the table for simple column references (zero-copy),
// or an owned ColumnValue for computed intermediates.
auto eval_value_vec(const ir::Expr& expr, const Table& table, const ScalarRegistry* scalars,
                    std::size_t n) -> std::expected<ColResult, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<ColResult, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                if (const auto* col = table.find(node.name)) {
                    ColResult r{col};
                    auto idx_it = table.index.find(node.name);
                    if (idx_it != table.index.end()) {
                        const auto& entry = table.columns[idx_it->second];
                        if (entry.validity.has_value())
                            r.validity = &*entry.validity;
                    }
                    return r;
                }
                if (scalars != nullptr) {
                    auto it = scalars->find(node.name);
                    if (it != scalars->end()) {
                        // Broadcast scalar into a full column.
                        ColumnValue cv = std::visit(
                            [n](const auto& v) -> ColumnValue {
                                using U = std::decay_t<decltype(v)>;
                                Column<U> col;
                                col.resize(n, v);
                                return ColumnValue{std::move(col)};
                            },
                            it->second);
                        return ColResult{std::move(cv)};
                    }
                }
                return std::unexpected("filter: unknown column '" + node.name + "'");
            } else if constexpr (std::is_same_v<T, ir::Literal>) {
                // Broadcast literal into a full column (fallback; common path avoids this).
                ColumnValue cv = std::visit(
                    [n](const auto& v) -> ColumnValue {
                        using U = std::decay_t<decltype(v)>;
                        Column<U> col;
                        col.resize(n, v);
                        return ColumnValue{std::move(col)};
                    },
                    node.value);
                return ColResult{std::move(cv)};
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr>) {
                auto lhs = eval_value_vec(*node.left, table, scalars, n);
                if (!lhs)
                    return std::unexpected(lhs.error());
                auto rhs = eval_value_vec(*node.right, table, scalars, n);
                if (!rhs)
                    return std::unexpected(rhs.error());
                auto result = arith_vec(node.op, deref_col(*lhs), deref_col(*rhs), n);
                if (!result)
                    return std::unexpected(result.error());
                ColResult res{std::move(*result)};
                res.owned_validity = merge_validity(lhs->get_validity(), rhs->get_validity(), n);
                return res;
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (const auto* fn = find_builtin(node.callee);
                    fn != nullptr && use_column_kernel(*fn, node)) {
                    // Whole-column builtin (Transform/Generator, or a hybrid
                    // null-handling scalar in kernel shape) as a column leaf —
                    // lets rolling/cum/lag/fill and rand_*/rep nest inside
                    // arithmetic (e.g. `px - rolling_mean(px, 20)` or
                    // `t + rand_normal(0, 1)`), using the same kernels as a
                    // bare field. Externs are not threaded into this evaluator
                    // (same as scalar calls below); no enclosing `window`
                    // clause reaches here, so rolling_* needs a per-call
                    // window argument.
                    const ColumnEvalCtx ctx{
                        .scalars = scalars, .externs = nullptr, .window = std::nullopt};
                    auto col = column_eval_of(*fn)(node, table, n, ctx);
                    if (!col) {
                        return std::unexpected(col.error());
                    }
                    // A generator may be told to produce fewer rows than the
                    // frame (rep's length_out); inside an expression that
                    // cannot line up row-for-row.
                    const std::size_t got =
                        std::visit([](const auto& c) { return c.size(); }, col->column);
                    if (got != n) {
                        return std::unexpected(node.callee + ": generates " + std::to_string(got) +
                                               " rows but the expression needs " +
                                               std::to_string(n));
                    }
                    return ColResult{std::move(col->column), std::move(col->validity)};
                }
                // A scalar call (abs, sqrt, casts, round, pmin/pmax, date
                // parts, …). If any argument nests a non-row-local
                // sub-expression (a Transform/Generator call or a boolean
                // node), the per-row evaluator can't produce it — materialize
                // those arguments into columns first, then apply the scalar
                // function over them (e.g. `abs(rand_normal(0, 1))`).
                const bool args_need_vec = std::ranges::any_of(
                    node.args, [](const auto& a) { return field_uses_vectorized_eval(*a); });
                if (args_need_vec) {
                    return eval_scalar_over_columns(node, table, scalars, n);
                }
                // Otherwise delegate to the shared field evaluator, which
                // dispatches through the registry (per-row, incl. the
                // null-handling scalars) and carries validity. Extern scalar
                // functions are not available in predicate position (externs
                // are not threaded into this vectorized evaluator).
                auto col = evaluate_field(
                    expr, table,
                    ColumnEvalCtx{.scalars = scalars, .externs = nullptr, .window = std::nullopt});
                if (!col) {
                    return std::unexpected(col.error());
                }
                return ColResult{std::move(col->column), std::move(col->validity)};
            } else if constexpr (std::is_same_v<T, ir::CompareExpr> ||
                                 std::is_same_v<T, ir::LogicalExpr> ||
                                 std::is_same_v<T, ir::IsNullExpr>) {
                // Boolean-producing nodes used in value position: evaluate the
                // predicate to a Mask and return it as a Column<Bool> (3VL nulls
                // become column validity).
                auto m = compute_mask(expr, table, scalars, n);
                if (!m) {
                    return std::unexpected(m.error());
                }
                return mask_to_bool_result(std::move(*m), n);
            } else {
                return std::unexpected("filter: not a value expression");
            }
        },
        expr.node);
}

// coalesce(a, b, ...): first non-null argument, per row. Validity-aware — the
// null wrinkle: row-local by shape, but the per-row evaluator has no null, so
// it evaluates at column level. Lives here (not expr.cpp) because arguments
// are arbitrary expressions evaluated via eval_value_vec. Registered as a
// Transform in the builtin registry; arguments must share one column type
// (checked at inference; re-checked here since kernels are also called with
// unvalidated calls from the update ladders).
auto eval_coalesce_column(const ir::CallExpr& call, const Table& input,
                          const ScalarRegistry* scalars, std::size_t rows)
    -> std::expected<ComputedColumn, std::string> {
    if (call.args.size() < 2) {
        return std::unexpected("coalesce: expected at least 2 arguments");
    }
    std::vector<ColResult> cols;
    cols.reserve(call.args.size());
    for (const auto& a : call.args) {
        auto c = eval_value_vec(*a, input, scalars, rows);
        if (!c) {
            return std::unexpected(c.error());
        }
        cols.push_back(std::move(*c));
    }
    // Eager type check, mirroring registry inference (which the update ladders
    // bypass): without it a mismatched later argument would only error when a
    // null row actually reaches it.
    const auto t0 = expr_type_for_column(deref_col(cols[0]));
    for (std::size_t k = 1; k < cols.size(); ++k) {
        if (expr_type_for_column(deref_col(cols[k])) != t0) {
            return std::unexpected("coalesce: arguments must share one type");
        }
    }
    return std::visit(
        [&](const auto& c0) -> std::expected<ComputedColumn, std::string> {
            using Col = std::decay_t<decltype(c0)>;
            Col out;
            out.reserve(rows);
            ValidityBitmap valid(rows, true);
            bool any_invalid = false;
            for (std::size_t i = 0; i < rows; ++i) {
                bool filled = false;
                for (const auto& cr : cols) {
                    const auto* vk = cr.get_validity();
                    if (vk == nullptr || (*vk)[i]) {
                        const auto* tc = std::get_if<Col>(&deref_col(cr));
                        if (tc == nullptr) {
                            return std::unexpected("coalesce: arguments must share one type");
                        }
                        out.push_back((*tc)[i]);
                        filled = true;
                        break;
                    }
                }
                if (!filled) {
                    if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                        out.push_back(std::string_view{});
                    } else {
                        out.push_back(typename Col::value_type{});
                    }
                    valid.set(i, false);
                    any_invalid = true;
                }
            }
            ComputedColumn r{.column = ColumnValue{std::move(out)}, .validity = std::nullopt};
            if (any_invalid) {
                r.validity = std::move(valid);
            }
            return r;
        },
        deref_col(cols[0]));
}

// Evaluate a scalar call whose arguments nest non-row-local sub-expressions
// (RNG / lag / lead / boolean nodes). Each such argument is materialized into a
// synthetic column of a copy of the input table; the call is then rewritten to
// reference those columns and run through the ordinary row-wise field evaluator
// (e.g. `abs(rand_normal(0, 1))` -> materialize the draw, then apply `abs`).
// Row-local arguments (columns, literals, arithmetic, round's mode identifier)
// are kept verbatim. The result is null wherever any materialized argument was
// null — standard scalar null propagation.
namespace {
auto eval_scalar_over_columns(const ir::CallExpr& call, const Table& table,
                              const ScalarRegistry* scalars, std::size_t n)
    -> std::expected<ColResult, std::string> {
    Table tmp = table;
    ir::CallExpr rewritten;
    rewritten.callee = call.callee;
    rewritten.args.reserve(call.args.size());
    int synth = 0;
    for (const auto& arg : call.args) {
        if (!field_uses_vectorized_eval(*arg)) {
            rewritten.args.push_back(arg);  // row-local: keep verbatim (deep-copied)
            continue;
        }
        auto c = eval_value_vec(*arg, table, scalars, n);
        if (!c) {
            return std::unexpected(c.error());
        }
        ColumnValue owned;
        if (auto* v = std::get_if<ColumnValue>(&c->data)) {
            owned = std::move(*v);
        } else {
            owned = *std::get<const ColumnValue*>(c->data);
        }
        const std::string name = "__ibex_nl_" + std::to_string(synth++) + "__";
        if (const auto* v = c->get_validity()) {
            tmp.add_column(name, std::move(owned), *v);
        } else {
            tmp.add_column(name, std::move(owned));
        }
        ir::Expr ref;
        ref.node = ir::ColumnRef{.name = name};
        rewritten.args.push_back(ir::make_expr_ptr(std::move(ref)));
    }
    // The materialized columns carry their validity, and the shared field
    // evaluator produces the exact result validity from it (propagation or a
    // Handles eval) — no post-hoc AND of argument masks, which would wrongly
    // re-null rows a null-handling function just filled.
    ir::Expr rewritten_expr;
    rewritten_expr.node = std::move(rewritten);
    auto col = evaluate_field(
        rewritten_expr, tmp,
        ColumnEvalCtx{.scalars = scalars, .externs = nullptr, .window = std::nullopt});
    if (!col) {
        return std::unexpected(col.error());
    }
    return ColResult{std::move(col->column), std::move(col->validity)};
}
}  // namespace

namespace {

// Split a conjunct into (column, op, literal), normalizing `literal op column`
// into `column flip(op) literal`.
auto split_col_cmp_lit(const ir::Expr& expr)
    -> std::optional<std::tuple<const ir::ColumnRef*, ir::CompareOp, const ir::Literal*>> {
    const auto* cmp = std::get_if<ir::CompareExpr>(&expr.node);
    if (cmp == nullptr) {
        return std::nullopt;
    }
    if (const auto* col = std::get_if<ir::ColumnRef>(&cmp->left->node)) {
        if (const auto* lit = std::get_if<ir::Literal>(&cmp->right->node)) {
            return std::tuple{col, cmp->op, lit};
        }
    }
    if (const auto* lit = std::get_if<ir::Literal>(&cmp->left->node)) {
        if (const auto* col = std::get_if<ir::ColumnRef>(&cmp->right->node)) {
            return std::tuple{col, flip_cmp(cmp->op), lit};
        }
    }
    return std::nullopt;
}

// Flatten an OR tree into its leaves. `a || b || c` parses left-deep, and an
// IN-list is exactly the case where every leaf is `<same column> == <literal>`.
void flatten_or(const ir::Expr& expr, std::vector<const ir::Expr*>& out) {
    if (const auto* logical = std::get_if<ir::LogicalExpr>(&expr.node)) {
        if (logical->op == ir::LogicalOp::Or) {
            flatten_or(*logical->left, out);
            flatten_or(*logical->right, out);
            return;
        }
    }
    out.push_back(&expr);
}

/// `col == "a" || col == "b" || …` → (col, {"a","b",…}). One column, equality
/// only, string literals only; anything else is not an IN-list.
auto try_extract_in_list(const ir::Expr& expr)
    -> std::optional<std::pair<const ir::ColumnRef*, std::vector<std::string_view>>> {
    const auto* logical = std::get_if<ir::LogicalExpr>(&expr.node);
    if (logical == nullptr || logical->op != ir::LogicalOp::Or) {
        return std::nullopt;
    }
    std::vector<const ir::Expr*> leaves;
    flatten_or(expr, leaves);

    const ir::ColumnRef* column = nullptr;
    std::vector<std::string_view> values;
    values.reserve(leaves.size());
    for (const auto* leaf : leaves) {
        auto split = split_col_cmp_lit(*leaf);
        if (!split.has_value()) {
            return std::nullopt;
        }
        const auto& [col, op, lit] = *split;
        if (op != ir::CompareOp::Eq) {
            return std::nullopt;
        }
        const auto* text = std::get_if<std::string>(&lit->value);
        if (text == nullptr) {
            return std::nullopt;
        }
        if (column == nullptr) {
            column = col;
        } else if (column->name != col->name) {
            return std::nullopt;  // an OR across two columns is not a membership test
        }
        values.emplace_back(*text);
    }
    if (column == nullptr) {
        return std::nullopt;
    }
    return std::pair{column, std::move(values)};
}

/// Evaluate an IN-list in one pass: `col == "a" || col == "b" || …` over a
/// Categorical column is a lookup in a byte-per-dictionary-code table, so the
/// per-row cost is one indexed load regardless of how long the list is. The
/// generic OR path instead builds a full-width mask per arm and combines them,
/// which is what a SQL `IN` degrades into once it is spelled as an OR-chain.
auto try_in_list_mask(const ir::Expr& expr, const Table& table, std::size_t n)
    -> std::optional<Mask> {
    auto in_list = try_extract_in_list(expr);
    if (!in_list.has_value()) {
        return std::nullopt;
    }
    const auto& [column, values] = *in_list;
    auto index = table.index.find(column->name);
    if (index == table.index.end()) {
        return std::nullopt;
    }
    const auto& entry = table.columns[index->second];
    const auto* cat = std::get_if<Column<Categorical>>(entry.column.get());
    if (cat == nullptr) {
        return std::nullopt;  // plain string columns keep the generic path
    }

    const auto& dict = cat->dictionary();
    std::vector<std::uint8_t> code_ok(dict.size(), 0U);
    for (std::size_t code = 0; code < dict.size(); ++code) {
        code_ok[code] = static_cast<std::uint8_t>(
            std::find(values.begin(), values.end(), std::string_view{dict[code]}) != values.end());
    }

    Mask mask;
    mask.value.resize(n);
    const auto* codes = cat->codes_data();
    for (std::size_t row = 0; row < n; ++row) {
        mask.value[row] = code_ok[static_cast<std::size_t>(codes[row])];
    }
    mask.apply_validity(entry.validity.has_value() ? &*entry.validity : nullptr, n);
    return mask;
}

}  // namespace

// Compute a boolean Mask for all n rows, with 3-valued logic (3VL) for nulls.
// valid==nullopt means all rows are valid (common non-null path, zero overhead).
auto compute_mask(const ir::Expr& expr, const Table& table, const ScalarRegistry* scalars,
                  std::size_t n) -> std::expected<Mask, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<Mask, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::CompareExpr>) {
                // Fast path: row-local numeric arithmetic compared to a literal.
                // This avoids materializing expressions such as `price * qty`
                // when the filter only needs the final keep/drop mask.
                if (auto spec = try_extract_numeric_arith_cmp_spec(node, table); spec.has_value()) {
                    Mask fused;
                    fused.value.resize(n);
                    dispatch_numeric_arith_cmp_kernel(*spec, fused.value.data(), n);
                    return fused;
                }
                // Fast path: column/expr op literal (no broadcast needed).
                if (const auto* lit = std::get_if<ir::Literal>(&node.right->node)) {
                    auto lhs = eval_value_vec(*node.left, table, scalars, n);
                    if (!lhs)
                        return std::unexpected(lhs.error());
                    return compare_col_scalar(node.op, deref_col(*lhs), lit->value, n,
                                              lhs->get_validity());
                }
                // Fast path: literal op column/expr (flip the operator).
                if (const auto* lit = std::get_if<ir::Literal>(&node.left->node)) {
                    auto rhs = eval_value_vec(*node.right, table, scalars, n);
                    if (!rhs)
                        return std::unexpected(rhs.error());
                    return compare_col_scalar(flip_cmp(node.op), deref_col(*rhs), lit->value, n,
                                              rhs->get_validity());
                }
                // General: both sides are column expressions.
                auto lhs = eval_value_vec(*node.left, table, scalars, n);
                if (!lhs)
                    return std::unexpected(lhs.error());
                auto rhs = eval_value_vec(*node.right, table, scalars, n);
                if (!rhs)
                    return std::unexpected(rhs.error());
                auto res = compare_vec(node.op, deref_col(*lhs), deref_col(*rhs), n,
                                       lhs->get_validity(), rhs->get_validity());
                return res;
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                if (node.op == ir::LogicalOp::Not) {
                    // NOT null = null; NOT true = false; NOT false = true
                    auto mask = compute_mask(*node.left, table, scalars, n);
                    if (!mask)
                        return std::unexpected(mask.error());
                    for (auto& v : mask->value)
                        v ^= 1U;
                    // valid stays as-is (null propagates)
                    return std::move(*mask);
                }
                const bool is_and = node.op == ir::LogicalOp::And;
                // Fast path: an IN-list (`col == a || col == b || …` on one
                // categorical column) is a membership test, not a chain of
                // independent ORs — one pass, no mask per arm.
                if (!is_and) {
                    if (auto in_mask = try_in_list_mask(expr, table, n); in_mask.has_value()) {
                        return std::move(*in_mask);
                    }
                }
                // Fast path: two numeric (column cmp literal) terms without nulls.
                // Evaluate both comparisons and combine in a single pass.
                if (auto lspec = try_extract_numeric_cmp_spec(*node.left, table);
                    lspec.has_value()) {
                    if (auto rspec = try_extract_numeric_cmp_spec(*node.right, table);
                        rspec.has_value()) {
                        Mask fused;
                        fused.value.resize(n);
                        uint8_t* out = fused.value.data();
                        if (is_and) {
                            dispatch_numeric_cmp_pair_kernel<true>(*lspec, *rspec, out, n);
                        } else {
                            dispatch_numeric_cmp_pair_kernel<false>(*lspec, *rspec, out, n);
                        }
                        return fused;
                    }
                }

                // 3VL AND/OR (see truth tables): combine values, then recompute
                // validity so a known-false (AND) / known-true (OR) on either
                // side makes the row definitively valid.
                auto left = compute_mask(*node.left, table, scalars, n);
                if (!left)
                    return std::unexpected(left.error());
                auto right = compute_mask(*node.right, table, scalars, n);
                if (!right)
                    return std::unexpected(right.error());
                const uint8_t* lp = left->value.data();
                const uint8_t* rp = right->value.data();
                for (std::size_t i = 0; i < n; ++i)
                    left->value[i] = is_and ? (lp[i] & rp[i]) : (lp[i] | rp[i]);
                if (left->valid || right->valid) {
                    if (!left->valid)
                        left->valid.emplace(n, uint8_t{1});
                    if (!right->valid)
                        right->valid.emplace(n, uint8_t{1});
                    const uint8_t* lval = left->valid->data();
                    const uint8_t* rval = right->valid->data();
                    for (std::size_t i = 0; i < n; ++i) {
                        if (is_and) {
                            // definitively false if either side is a known false
                            const uint8_t a_false = lval[i] & (1U - lp[i]);
                            const uint8_t b_false = rval[i] & (1U - rp[i]);
                            (*left->valid)[i] = (lval[i] & rval[i]) | a_false | b_false;
                        } else {
                            // definitively true if either side is a known true
                            const uint8_t a_true = lval[i] & lp[i];
                            const uint8_t b_true = rval[i] & rp[i];
                            (*left->valid)[i] = (lval[i] & rval[i]) | a_true | b_true;
                        }
                    }
                }
                return std::move(*left);
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                // IS NULL / IS NOT NULL: test the operand column's validity.
                // Always produces a valid Bool (never null itself).
                const bool want_null = !node.negated;
                Mask m;
                m.value.resize(n, want_null ? uint8_t{0} : uint8_t{1});
                if (const auto* col_node = std::get_if<ir::ColumnRef>(&node.operand->node)) {
                    auto it = table.index.find(col_node->name);
                    if (it != table.index.end()) {
                        const auto& entry = table.columns[it->second];
                        if (entry.validity.has_value()) {
                            const auto& bm = *entry.validity;
                            for (std::size_t i = 0; i < n; ++i)
                                m.value[i] = static_cast<uint8_t>(want_null ? !bm[i] : bm[i]);
                        }
                        // no validity bitmap → all rows valid → fill stays correct
                    }
                    return m;
                }
                return std::unexpected("filter: 'is null' operand must be a column reference");
            } else {
                // ColumnRef, Literal, BinaryExpr, CallExpr, RankExpr are not
                // structurally boolean (compare/logical/is-null), but may still
                // evaluate to a Bool-typed column — e.g. a bare boolean column
                // reference (`filter is_manual`) or a Bool-returning scalar
                // builtin (`filter is_nan(x)`). Evaluate and check the result
                // type before giving up.
                auto val = eval_value_vec(expr, table, scalars, n);
                if (!val)
                    return std::unexpected(val.error());
                const auto* bcol = std::get_if<Column<bool>>(&deref_col(*val));
                if (bcol == nullptr)
                    return std::unexpected("filter: not a boolean expression");
                Mask m;
                m.value.resize(n);
                for (std::size_t i = 0; i < n; ++i)
                    m.value[i] = static_cast<uint8_t>((*bcol)[i]);
                m.apply_validity(val->get_validity(), n);
                return m;
            }
        },
        expr.node);
}

namespace {

auto filter_table_impl(const Table& input, const ir::Expr& predicate,
                       const std::vector<ir::ColumnRef>* project, std::size_t row_limit,
                       const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    const std::size_t n = input.rows();

    auto mask_result = compute_mask(predicate, input, scalars, n);
    if (!mask_result)
        return std::unexpected(mask_result.error());

    // 3VL: keep row iff value[i]==1 AND (no valid vector OR valid[i]==1)
    const uint8_t* mp = mask_result->value.data();
    const uint8_t* vp = mask_result->valid ? mask_result->valid->data() : nullptr;

    // Block-wise compaction: keep bits per 64-row chunk + popcount for out size.
    // When `row_limit` is non-zero, stop scanning after we've accumulated that
    // many kept rows — any suffix words stay zero and gather skips them.
    const std::size_t n_words = (n + 63) / 64;
    std::vector<std::uint64_t> keep_words(n_words, 0);
    std::size_t out_n = 0;
    for (std::size_t w = 0; w < n_words; ++w) {
        const std::size_t base = w * 64;
        const std::size_t lim = std::min<std::size_t>(64, n - base);
#ifdef __AVX2__
        const std::uint64_t bits =
            (vp == nullptr && lim == 64)
                ? pack_mask_word_avx2(mp + base)
                : pack_mask_word_scalar(mp + base, vp ? vp + base : nullptr, lim);
#else
        const std::uint64_t bits = pack_mask_word_scalar(mp + base, vp ? vp + base : nullptr, lim);
#endif
        const auto block_kept = static_cast<std::size_t>(std::popcount(bits));
        if (row_limit != 0 && out_n + block_kept >= row_limit) {
            // Trim this block's high-order keep bits so we emit exactly
            // `row_limit - out_n` more rows and then stop.
            std::size_t remaining = row_limit - out_n;
            std::uint64_t trimmed = 0;
            std::uint64_t b = bits;
            while (remaining > 0 && b != 0) {
                trimmed |= b & (~b + 1);  // lowest set bit
                b &= b - 1;
                --remaining;
            }
            keep_words[w] = trimmed;
            out_n = row_limit;
            break;
        }
        keep_words[w] = bits;
        out_n += block_kept;
    }

    // Build output skeleton: either all input columns (no projection) or
    // just the projected subset. `src_of_dst[d]` maps an output column index
    // to its source index in `input.columns`.
    Table output;
    std::vector<std::size_t> src_of_dst;
    if (project == nullptr) {
        output.columns.reserve(input.columns.size());
        src_of_dst.reserve(input.columns.size());
        for (std::size_t i = 0; i < input.columns.size(); ++i) {
            const auto& entry = input.columns[i];
            output.add_column(entry.name, make_empty_like(*entry.column));
            src_of_dst.push_back(i);
        }
    } else {
        output.columns.reserve(project->size());
        src_of_dst.reserve(project->size());
        for (const auto& col : *project) {
            auto it = input.index.find(col.name);
            if (it == input.index.end()) {
                return std::unexpected("select column not found: " + col.name);
            }
            const auto& entry = input.columns[it->second];
            output.add_column(entry.name, make_empty_like(*entry.column));
            src_of_dst.push_back(it->second);
        }
    }

    auto for_each_selected = [&](auto&& fn) {
        for (std::size_t w = 0; w < n_words; ++w) {
            std::uint64_t bits = keep_words[w];
            const std::size_t base = w * 64;
            while (bits != 0) {
                const int bit = std::countr_zero(bits);
                fn(base + static_cast<std::size_t>(bit));
                bits &= (bits - 1);
            }
        }
    };

    auto copy_column = [&](std::size_t dst_idx) {
        const std::size_t src_idx = src_of_dst[dst_idx];
        const auto& src_entry = input.columns[src_idx];
        auto& dst_entry = output.columns[dst_idx];
        std::visit(
            [&](const auto& src) {
                using ColT = std::decay_t<decltype(src)>;
                auto* dst = std::get_if<ColT>(dst_entry.column.get());
                if (dst == nullptr) {
                    invariant_violation(
                        "filter_table: source/destination gather column type mismatch");
                }
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    dst->resize(out_n);
                    const auto* sp = src.codes_data();
                    auto* dp = dst->codes_data();
                    std::size_t j = 0;
                    for_each_selected([&](std::size_t si) { dp[j++] = sp[si]; });
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    // Two-pass flat-buffer gather: compute total bytes, then bulk-memcpy
                    // slabs.
                    const uint32_t* src_off = src.offsets_data();
                    std::size_t total_chars = 0;
                    for_each_selected(
                        [&](std::size_t si) { total_chars += src_off[si + 1] - src_off[si]; });
                    dst->resize_for_gather(out_n, total_chars);
                    uint32_t* dst_off = dst->offsets_data();
                    char* dst_char = dst->chars_data();
                    const char* src_char = src.chars_data();
                    dst_off[0] = 0;
                    uint32_t cur = 0;
                    std::size_t j = 0;
                    for_each_selected([&](std::size_t si) {
                        const uint32_t len = src_off[si + 1] - src_off[si];
                        std::memcpy(dst_char + cur, src_char + src_off[si], len);
                        cur += len;
                        dst_off[++j] = cur;
                    });
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    dst->resize(out_n);
                    auto* __restrict dst_words = dst->words_data();
                    const auto* __restrict src_words = src.words_data();
                    std::size_t out_bit = 0;
                    for (std::size_t w = 0; w < n_words; ++w) {
                        const std::uint64_t select = keep_words[w];
                        if (select == 0) {
                            continue;
                        }
                        const std::uint64_t packed = pack_selected_bool_bits(src_words[w], select);
                        append_packed_bool_bits(packed,
                                                static_cast<std::size_t>(std::popcount(select)),
                                                dst_words, out_bit);
                    }
                } else {
                    using T = ColT::value_type;
                    dst->resize(out_n);
                    const T* sp = src.data();
                    T* dp = dst->data();
                    std::size_t j = 0;
                    for_each_selected([&](std::size_t si) { dp[j++] = sp[si]; });
                }
            },
            *src_entry.column);
    };

    for (std::size_t c = 0; c < output.columns.size(); ++c) {
        copy_column(c);
    }

    // Propagate validity bitmaps using the same selected row set.
    for (std::size_t c = 0; c < output.columns.size(); ++c) {
        const std::size_t src_idx = src_of_dst[c];
        if (input.columns[src_idx].validity.has_value()) {
            const auto& src_bm = *input.columns[src_idx].validity;
            ValidityBitmap dst_bm(out_n, false);
            std::size_t j = 0;
            for_each_selected([&](std::size_t si) { dst_bm.set(j++, src_bm[si]); });
            output.columns[c].validity = std::move(dst_bm);
        }
    }

    if (input.ordering.has_value() &&
        (project == nullptr || ordering_keys_present(*input.ordering, output.index))) {
        output.ordering = input.ordering;
    }
    if (input.time_index.has_value() &&
        (project == nullptr || output.index.contains(*input.time_index))) {
        output.time_index = input.time_index;
    }
    normalize_time_index(output);
    return output;
}

}  // namespace

auto filter_table(const Table& input, const ir::Expr& predicate, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string> {
    return filter_table_impl(input, predicate, nullptr, 0, scalars);
}

namespace {

// ─── Fused bounds ─────────────────────────────────────────────────────────────
//
// A filter pushed into a scan arrives as a list of conjuncts, and the generic
// path below evaluates them one at a time: a full-width mask per conjunct, then
// a remove_if over the surviving row ids. A multi-column range filter pays that
// per bound — q06 tests two bounds on l_shipdate, two on l_discount and one on
// l_quantity, so five mask passes over six million rows and five passes over a
// 48MB index vector, to keep ~1.8% of them.
//
// Fusing rewrites that into one pass. The conjuncts on each column collapse
// into a single lo/hi range (`l_discount >= 0.05 && l_discount <= 0.07` is one
// range test, not two comparisons), and every column's range is evaluated
// blockwise against the same L1-resident mask, appending survivors straight to
// the selection. Each predicate column is read exactly once and no full-width
// mask or full-width index vector is ever built.
//
// This is a pure optimization: AND is commutative and every conjunct here is
// row-local and side-effect free, so fusing and reordering cannot change which
// rows survive. Conjuncts that aren't `column <cmp> literal` over a numeric or
// time column stay in `leftover` and run on the (now much smaller) selection.

enum class BoundsKind : std::uint8_t { Int64, Double, Date, Timestamp };

struct BoundsSpec {
    BoundsKind kind{};
    const void* data = nullptr;
    const ValidityBitmap* validity = nullptr;  // null → no nulls to exclude
    std::int64_t lo_i = std::numeric_limits<std::int64_t>::min();
    std::int64_t hi_i = std::numeric_limits<std::int64_t>::max();
    double lo_d = -std::numeric_limits<double>::infinity();
    double hi_d = std::numeric_limits<double>::infinity();
    bool lo_incl = true;
    bool hi_incl = true;
    bool unsatisfiable = false;
};

template <typename T>
void tighten_lo(T& cur, bool& cur_incl, T v, bool incl) {
    if (v > cur) {
        cur = v;
        cur_incl = incl;
    } else if (v == cur && !incl) {
        cur_incl = false;
    }
}

template <typename T>
void tighten_hi(T& cur, bool& cur_incl, T v, bool incl) {
    if (v < cur) {
        cur = v;
        cur_incl = incl;
    } else if (v == cur && !incl) {
        cur_incl = false;
    }
}

// Fold one `column <op> literal` conjunct into the column's running range.
// Returns false when the literal's type doesn't match the column's, leaving the
// conjunct for the generic path rather than guessing at a conversion.
auto merge_bound(BoundsSpec& spec, ir::CompareOp op, const ir::Literal& lit) -> bool {
    if (op == ir::CompareOp::Ne) {
        return false;
    }

    const bool integral = spec.kind != BoundsKind::Double;
    std::int64_t vi = 0;
    double vd = 0.0;
    if (spec.kind == BoundsKind::Int64) {
        const auto* i = std::get_if<std::int64_t>(&lit.value);
        if (i == nullptr) {
            return false;  // e.g. `int_col < 24.5` — let the generic path round it
        }
        vi = *i;
    } else if (spec.kind == BoundsKind::Date) {
        const auto* d = std::get_if<Date>(&lit.value);
        if (d == nullptr) {
            return false;
        }
        vi = d->days;
    } else if (spec.kind == BoundsKind::Timestamp) {
        const auto* t = std::get_if<Timestamp>(&lit.value);
        if (t == nullptr) {
            return false;
        }
        vi = t->nanos;
    } else {
        if (const auto* d = std::get_if<double>(&lit.value)) {
            vd = *d;
        } else if (const auto* i = std::get_if<std::int64_t>(&lit.value)) {
            vd = static_cast<double>(*i);
        } else {
            return false;
        }
        if (std::isnan(vd)) {
            return false;  // every comparison against NaN is false; not a range
        }
    }

    const bool lower = op == ir::CompareOp::Gt || op == ir::CompareOp::Ge;
    const bool upper = op == ir::CompareOp::Lt || op == ir::CompareOp::Le;
    const bool incl = op != ir::CompareOp::Gt && op != ir::CompareOp::Lt;
    if (lower || op == ir::CompareOp::Eq) {
        if (integral) {
            tighten_lo(spec.lo_i, spec.lo_incl, vi, incl);
        } else {
            tighten_lo(spec.lo_d, spec.lo_incl, vd, incl);
        }
    }
    if (upper || op == ir::CompareOp::Eq) {
        if (integral) {
            tighten_hi(spec.hi_i, spec.hi_incl, vi, incl);
        } else {
            tighten_hi(spec.hi_d, spec.hi_incl, vd, incl);
        }
    }

    // Contradictory bounds (`x > 5 && x < 3`, or `x >= 5 && x < 5`) select
    // nothing; recording that lets the whole scan short-circuit.
    if (integral) {
        spec.unsatisfiable =
            spec.lo_i > spec.hi_i || (spec.lo_i == spec.hi_i && !(spec.lo_incl && spec.hi_incl));
    } else {
        spec.unsatisfiable =
            spec.lo_d > spec.hi_d || (spec.lo_d == spec.hi_d && !(spec.lo_incl && spec.hi_incl));
    }
    return true;
}

// A membership test on a text column: `col == "x"`, `col != "x"`, or an
// OR-chain of equalities on one column — an IN-list, which is how SQL's
// `col IN (...)` reaches Ibex since there is no `in` operator.
//
// These are the other half of a pushed-down filter, and until now only the
// numeric half fused: q19 pushes `l_shipinstruct == 'DELIVER IN PERSON'` and
// `l_shipmode IN ('AIR','AIR REG')`, which cost a full-width mask for the
// first, a mask *per arm* plus a combine for the OR, and a remove_if over the
// survivors. Folding them into the same blocked pass as the ranges reads each
// column once and allocates no full-width mask at all.
//
// A Categorical column is tested against a byte per dictionary code, so the
// per-row work is one indexed load however long the IN-list is. Note a literal
// that is not in the dictionary simply matches no code — an impossible
// predicate then selects nothing, for free.
struct SetSpec {
    const Column<Categorical>* cat = nullptr;
    const Column<std::string>* str = nullptr;
    const ValidityBitmap* validity = nullptr;

    // Accumulated across conjuncts on this column before being compiled below.
    // `allowed` is only meaningful once has_allowed is set (no Eq/IN seen yet
    // means "any value"); `denied` collects the `!=` literals.
    std::vector<std::string_view> allowed;
    std::vector<std::string_view> denied;
    bool has_allowed = false;
    bool unsatisfiable = false;

    // Compiled forms.
    std::vector<std::uint8_t> code_ok;  // Categorical: indexed by dictionary code
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> allow_set;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> deny_set;

    [[nodiscard]] auto matches(std::string_view value) const -> bool {
        if (has_allowed && !allow_set.contains(value)) {
            return false;
        }
        return deny_set.empty() || !deny_set.contains(value);
    }
};

struct BoundsPlan {
    std::vector<BoundsSpec> specs;
    std::vector<SetSpec> sets;
    std::vector<ir::Expr> leftover;
};

/// Bind a text column, or report that it is not one.
auto bind_set_column(const ColumnEntry& entry, SetSpec& spec) -> bool {
    if (const auto* c_cat = std::get_if<Column<Categorical>>(entry.column.get())) {
        spec.cat = c_cat;
    } else if (const auto* c_str = std::get_if<Column<std::string>>(entry.column.get())) {
        spec.str = c_str;
    } else {
        return false;
    }
    spec.validity = entry.validity.has_value() ? &*entry.validity : nullptr;
    return true;
}

/// Intersect this column's allowed values with `values` (an Eq or an IN-list).
/// Conjuncts AND together, so a second Eq narrows rather than widens.
void restrict_allowed(SetSpec& spec, const std::vector<std::string_view>& values) {
    if (!spec.has_allowed) {
        spec.allowed = values;
        spec.has_allowed = true;
        return;
    }
    std::vector<std::string_view> both;
    for (const auto& value : spec.allowed) {
        if (std::find(values.begin(), values.end(), value) != values.end()) {
            both.push_back(value);
        }
    }
    spec.allowed = std::move(both);
    if (spec.allowed.empty()) {
        spec.unsatisfiable = true;
    }
}

/// Turn the accumulated allow/deny literals into the form the kernel reads.
void compile_set_spec(SetSpec& spec) {
    for (const auto& value : spec.allowed) {
        spec.allow_set.insert(value);
    }
    for (const auto& value : spec.denied) {
        spec.deny_set.insert(value);
    }
    if (spec.cat == nullptr) {
        return;
    }
    const auto& dict = spec.cat->dictionary();
    spec.code_ok.assign(dict.size(), 0U);
    bool any = false;
    for (std::size_t code = 0; code < dict.size(); ++code) {
        const bool ok = spec.matches(dict[code]);
        spec.code_ok[code] = static_cast<std::uint8_t>(ok);
        any = any || ok;
    }
    if (!any) {
        spec.unsatisfiable = true;
    }
}

auto build_bounds_plan(const Table& input, const std::vector<ir::Expr>& conjuncts) -> BoundsPlan {
    BoundsPlan plan;
    robin_hood::unordered_map<std::string, std::size_t> spec_of_column;
    robin_hood::unordered_map<std::string, std::size_t> set_of_column;
    plan.leftover.reserve(conjuncts.size());

    // A text column and the values it is allowed to take: an Eq, a `!=`, or an
    // OR-chain of equalities on that one column.
    const auto add_set_conjunct = [&](const ir::ColumnRef& col,
                                      const std::vector<std::string_view>& values,
                                      bool negated) -> bool {
        auto index = input.index.find(col.name);
        if (index == input.index.end()) {
            return false;
        }
        auto existing = set_of_column.find(col.name);
        std::size_t at = 0;
        if (existing != set_of_column.end()) {
            at = existing->second;
        } else {
            SetSpec spec;
            if (!bind_set_column(input.columns[index->second], spec)) {
                return false;  // not a text column
            }
            at = plan.sets.size();
            plan.sets.push_back(std::move(spec));
            set_of_column.emplace(col.name, at);
        }
        SetSpec& spec = plan.sets[at];
        if (negated) {
            spec.denied.insert(spec.denied.end(), values.begin(), values.end());
        } else {
            restrict_allowed(spec, values);
        }
        return true;
    };

    for (const auto& conjunct : conjuncts) {
        if (auto in_list = try_extract_in_list(conjunct); in_list.has_value()) {
            if (add_set_conjunct(*in_list->first, in_list->second, /*negated=*/false)) {
                continue;
            }
            plan.leftover.push_back(conjunct);
            continue;
        }

        auto split = split_col_cmp_lit(conjunct);
        if (!split.has_value()) {
            plan.leftover.push_back(conjunct);
            continue;
        }
        const auto& [col, op, lit] = *split;

        // A text equality (or inequality) is a membership test, not a range.
        if (const auto* text = std::get_if<std::string>(&lit->value)) {
            if (op == ir::CompareOp::Eq || op == ir::CompareOp::Ne) {
                const std::vector<std::string_view> values{std::string_view{*text}};
                if (add_set_conjunct(*col, values, op == ir::CompareOp::Ne)) {
                    continue;
                }
            }
            plan.leftover.push_back(conjunct);
            continue;
        }

        auto index = input.index.find(col->name);
        if (index == input.index.end()) {
            plan.leftover.push_back(conjunct);
            continue;
        }
        const auto& entry = input.columns[index->second];

        auto existing = spec_of_column.find(col->name);
        BoundsSpec candidate;
        if (existing != spec_of_column.end()) {
            candidate = plan.specs[existing->second];
        } else {
            const auto& column = *entry.column;
            if (const auto* c_int = std::get_if<Column<std::int64_t>>(&column)) {
                candidate.kind = BoundsKind::Int64;
                candidate.data = c_int->data();
            } else if (const auto* c_dbl = std::get_if<Column<double>>(&column)) {
                candidate.kind = BoundsKind::Double;
                candidate.data = c_dbl->data();
            } else if (const auto* c_date = std::get_if<Column<Date>>(&column)) {
                // Date is a single int32 field and Timestamp a single int64, so
                // their storage is the scalar array the kernels want.
                candidate.kind = BoundsKind::Date;
                candidate.data = c_date->data();
            } else if (const auto* c_ts = std::get_if<Column<Timestamp>>(&column)) {
                candidate.kind = BoundsKind::Timestamp;
                candidate.data = c_ts->data();
            } else {
                plan.leftover.push_back(conjunct);  // string/bool/categorical
                continue;
            }
            candidate.validity = entry.validity.has_value() ? &*entry.validity : nullptr;
        }

        if (!merge_bound(candidate, op, *lit)) {
            plan.leftover.push_back(conjunct);
            continue;
        }
        if (existing != spec_of_column.end()) {
            plan.specs[existing->second] = candidate;
        } else {
            spec_of_column.emplace(col->name, plan.specs.size());
            plan.specs.push_back(candidate);
        }
    }

    for (auto& spec : plan.sets) {
        compile_set_spec(spec);
    }
    return plan;
}

template <typename T, bool LoIncl, bool HiIncl>
void bounds_kernel(const T* __restrict values, T lo, T hi, std::uint8_t* __restrict mask,
                   std::size_t len, bool first) {
    if (first) {
        for (std::size_t i = 0; i < len; ++i) {
            const T v = values[i];
            const bool ok = (LoIncl ? v >= lo : v > lo) && (HiIncl ? v <= hi : v < hi);
            mask[i] = static_cast<std::uint8_t>(ok);
        }
        return;
    }
    for (std::size_t i = 0; i < len; ++i) {
        const T v = values[i];
        const bool ok = (LoIncl ? v >= lo : v > lo) && (HiIncl ? v <= hi : v < hi);
        mask[i] = static_cast<std::uint8_t>(mask[i] & static_cast<std::uint8_t>(ok));
    }
}

template <typename T>
void bounds_range(const T* values, T lo, T hi, bool lo_incl, bool hi_incl, std::uint8_t* mask,
                  std::size_t len, bool first) {
    if (lo_incl && hi_incl) {
        bounds_kernel<T, true, true>(values, lo, hi, mask, len, first);
    } else if (lo_incl) {
        bounds_kernel<T, true, false>(values, lo, hi, mask, len, first);
    } else if (hi_incl) {
        bounds_kernel<T, false, true>(values, lo, hi, mask, len, first);
    } else {
        bounds_kernel<T, false, false>(values, lo, hi, mask, len, first);
    }
}

template <typename T>
auto clamp_bound(std::int64_t v) -> T {
    if (v < static_cast<std::int64_t>(std::numeric_limits<T>::min())) {
        return std::numeric_limits<T>::min();
    }
    if (v > static_cast<std::int64_t>(std::numeric_limits<T>::max())) {
        return std::numeric_limits<T>::max();
    }
    return static_cast<T>(v);
}

void apply_bounds_spec(const BoundsSpec& spec, std::size_t base, std::size_t len,
                       std::uint8_t* mask, bool first) {
    switch (spec.kind) {
        case BoundsKind::Int64: {
            const auto* values = static_cast<const std::int64_t*>(spec.data) + base;
            bounds_range<std::int64_t>(values, spec.lo_i, spec.hi_i, spec.lo_incl, spec.hi_incl,
                                       mask, len, first);
            break;
        }
        case BoundsKind::Timestamp: {
            const auto* values =
                reinterpret_cast<const std::int64_t*>(  // NOLINT(*-reinterpret-cast)
                    static_cast<const Timestamp*>(spec.data)) +
                base;
            bounds_range<std::int64_t>(values, spec.lo_i, spec.hi_i, spec.lo_incl, spec.hi_incl,
                                       mask, len, first);
            break;
        }
        case BoundsKind::Date: {
            const auto* values =
                reinterpret_cast<const std::int32_t*>(  // NOLINT(*-reinterpret-cast)
                    static_cast<const Date*>(spec.data)) +
                base;
            bounds_range<std::int32_t>(values, clamp_bound<std::int32_t>(spec.lo_i),
                                       clamp_bound<std::int32_t>(spec.hi_i), spec.lo_incl,
                                       spec.hi_incl, mask, len, first);
            break;
        }
        case BoundsKind::Double: {
            const auto* values = static_cast<const double*>(spec.data) + base;
            bounds_range<double>(values, spec.lo_d, spec.hi_d, spec.lo_incl, spec.hi_incl, mask,
                                 len, first);
            break;
        }
    }
    // A null compares false against any bound, so it never survives a conjunct.
    if (spec.validity != nullptr) {
        for (std::size_t i = 0; i < len; ++i) {
            mask[i] = static_cast<std::uint8_t>(
                mask[i] & static_cast<std::uint8_t>((*spec.validity)[base + i]));
        }
    }
}

void apply_set_spec(const SetSpec& spec, std::size_t base, std::size_t len, std::uint8_t* mask,
                    bool first) {
    if (spec.cat != nullptr) {
        // One indexed load per row, whatever the IN-list's length.
        const auto* codes = spec.cat->codes_data() + base;
        const std::uint8_t* code_ok = spec.code_ok.data();
        if (first) {
            for (std::size_t i = 0; i < len; ++i) {
                mask[i] = code_ok[static_cast<std::size_t>(codes[i])];
            }
        } else {
            for (std::size_t i = 0; i < len; ++i) {
                mask[i] = static_cast<std::uint8_t>(mask[i] &
                                                    code_ok[static_cast<std::size_t>(codes[i])]);
            }
        }
    } else {
        const auto& column = *spec.str;
        for (std::size_t i = 0; i < len; ++i) {
            if (!first && mask[i] == 0) {
                continue;  // already rejected — skip the lookup
            }
            const bool ok = spec.matches(std::string_view{column[base + i]});
            mask[i] = static_cast<std::uint8_t>(ok);
        }
    }
    // A null matches no literal, so it survives no membership test either.
    if (spec.validity != nullptr) {
        for (std::size_t i = 0; i < len; ++i) {
            mask[i] = static_cast<std::uint8_t>(
                mask[i] & static_cast<std::uint8_t>((*spec.validity)[base + i]));
        }
    }
}

auto select_bounds(const std::vector<BoundsSpec>& specs, const std::vector<SetSpec>& sets,
                   std::size_t rows) -> std::vector<std::size_t> {
    constexpr std::size_t kBlock = 4096;
    std::vector<std::size_t> selected;
    for (const auto& spec : specs) {
        if (spec.unsatisfiable) {
            return selected;
        }
    }
    for (const auto& spec : sets) {
        if (spec.unsatisfiable) {
            return selected;  // e.g. `x == 'nope'` where 'nope' is not a value x takes
        }
    }
    selected.reserve(std::min<std::size_t>(rows, kBlock));

    std::array<std::uint8_t, kBlock> mask{};
    // Left-pack each block's survivors here, then bulk-copy them out. Appending
    // straight to `selected` under an `if (mask[i])` branches once per row on a
    // predicate the predictor cannot learn — at l_returnflag's ~25% hit rate
    // that alone cost more than the whole comparison (the same trap that made
    // the mask path's push_back slower than the remove_if it replaced). The
    // store below is unconditional and only the cursor moves, so there is no
    // branch to miss; one slot of slack keeps the final rejected row in bounds.
    std::array<std::size_t, kBlock + 1> hits{};

    for (std::size_t base = 0; base < rows; base += kBlock) {
        const std::size_t len = std::min(kBlock, rows - base);
        bool first = true;
        for (const auto& spec : specs) {
            apply_bounds_spec(spec, base, len, mask.data(), first);
            first = false;
        }
        for (const auto& spec : sets) {
            apply_set_spec(spec, base, len, mask.data(), first);
            first = false;
        }

        std::size_t kept = 0;
        for (std::size_t i = 0; i < len; ++i) {
            hits[kept] = base + i;
            kept += static_cast<std::size_t>(mask[i] != 0);
        }
        selected.insert(selected.end(), hits.begin(),
                        hits.begin() + static_cast<std::ptrdiff_t>(kept));
        if (base == 0 && rows > kBlock) {
            // One up-front capacity estimate from the first block's hit rate.
            // Growing by doubling instead re-copies the vector ~10 times on a
            // q03-shaped scan (3.2M survivors, up to 26MB per re-copy). The
            // 12.5% headroom plus one block of slack absorbs drift; a
            // clustered predicate that defeats the estimate just falls back
            // to doubling from a larger base.
            const std::size_t blocks = (rows + kBlock - 1) / kBlock;
            const std::size_t estimated = selected.size() * blocks;
            selected.reserve(std::min(rows, estimated + (estimated / 8) + kBlock));
        }
    }
    return selected;
}

auto filter_selection_impl(const Table& input, const std::vector<ir::Expr>& conjuncts,
                           const ScalarRegistry* scalars, std::vector<std::size_t> selected)
    -> std::expected<std::vector<std::size_t>, std::string> {
    constexpr std::size_t kCompactionFactor = 4;
    const std::size_t rows = input.rows();
    if (selected.empty()) {
        return selected;
    }
    auto referenced_names = [&](const ir::Expr& conjunct) {
        robin_hood::unordered_set<std::string> referenced;
        ir::collect_expr_column_refs(conjunct, referenced);
        std::vector<std::string> names;
        names.reserve(referenced.size());
        for (const auto& entry : input.columns) {
            if (referenced.contains(entry.name)) {
                names.push_back(entry.name);
            }
        }
        return names;
    };

    for (std::size_t conjunct_index = 0; conjunct_index < conjuncts.size();) {
        const auto names = referenced_names(conjuncts[conjunct_index]);
        if (!names.empty() && selected.size() <= rows / kCompactionFactor) {
            std::size_t group_end = conjunct_index + 1;
            while (group_end < conjuncts.size() &&
                   referenced_names(conjuncts[group_end]) == names) {
                ++group_end;
            }

            // Once earlier conjuncts have rejected most rows, evaluating a
            // dense mask wastes work. Gather just this stage's columns into a
            // compact table, evaluate consecutive bounds together, then map
            // the local survivors back to input row indices.
            Table stage;
            for (const auto& name : names) {
                const auto* entry = input.find_entry(name);
                if (entry == nullptr) {
                    continue;
                }
                auto column = gather_column(*entry->column, selected.data(), selected.size());
                if (entry->validity.has_value()) {
                    ValidityBitmap validity(selected.size(), true);
                    for (std::size_t row = 0; row < selected.size(); ++row) {
                        validity.set(row, (*entry->validity)[selected[row]]);
                    }
                    stage.add_column(name, std::move(column), std::move(validity));
                } else {
                    stage.add_column(name, std::move(column));
                }
            }
            stage.logical_rows = selected.size();

            using ConjunctDiff = std::vector<ir::Expr>::difference_type;
            std::vector<ir::Expr> stage_conjuncts(
                conjuncts.begin() + static_cast<ConjunctDiff>(conjunct_index),
                conjuncts.begin() + static_cast<ConjunctDiff>(group_end));
            std::vector<std::size_t> local(selected.size());
            std::iota(local.begin(), local.end(), std::size_t{0});
            auto local_selected =
                filter_selection_impl(stage, stage_conjuncts, scalars, std::move(local));
            if (!local_selected) {
                return std::unexpected(local_selected.error());
            }
            std::vector<std::size_t> next;
            next.reserve(local_selected->size());
            for (std::size_t row : *local_selected) {
                next.push_back(selected[row]);
            }
            selected = std::move(next);
            conjunct_index = group_end;
            if (selected.empty()) {
                break;
            }
            continue;
        }

        auto mask = compute_mask(conjuncts[conjunct_index], input, scalars, rows);
        if (!mask) {
            return std::unexpected(mask.error());
        }
        const auto* valid = mask->valid ? mask->valid->data() : nullptr;
        auto end = std::remove_if(selected.begin(), selected.end(), [&](std::size_t row) {
            return mask->value[row] == 0 || (valid != nullptr && valid[row] == 0);
        });
        selected.erase(end, selected.end());
        if (selected.empty()) {
            break;
        }
        ++conjunct_index;
    }
    return selected;
}

}  // namespace

auto filter_selection(const Table& input, const std::vector<ir::Expr>& conjuncts,
                      const ScalarRegistry* scalars)
    -> std::expected<std::vector<std::size_t>, std::string> {
    // Range conjuncts fuse into one blocked pass that yields the selection
    // directly; whatever doesn't fuse runs against that selection instead of
    // against every row.
    auto plan = build_bounds_plan(input, conjuncts);
    if (!plan.specs.empty() || !plan.sets.empty()) {
        auto selected = select_bounds(plan.specs, plan.sets, input.rows());
        return filter_selection_impl(input, plan.leftover, scalars, std::move(selected));
    }

    // Nothing fused — a string equality (`l_returnflag == "R"`) or an OR block.
    // The first conjunct still yields the selection directly from its mask:
    // numbering every row only to delete three quarters of them again costs
    // more than the comparison does (a 48MB index vector over lineitem, then a
    // remove_if pass across it).
    //
    // Count first, then left-pack. `push_back` loses to the remove_if it
    // replaces at moderate selectivity: it re-checks capacity every row and
    // branches on a predicate that, at q10's ~25% hit rate, the predictor
    // cannot learn. Sizing the result up front makes the fill branchless.
    if (!conjuncts.empty()) {
        auto mask = compute_mask(conjuncts.front(), input, scalars, input.rows());
        if (!mask) {
            return std::unexpected(mask.error());
        }
        const std::size_t rows = input.rows();
        const auto* valid = mask->valid ? mask->valid->data() : nullptr;
        const auto* values = mask->value.data();
        auto keeps = [&](std::size_t row) {
            return values[row] != 0 && (valid == nullptr || valid[row] != 0);
        };

        std::size_t kept = 0;
        for (std::size_t row = 0; row < rows; ++row) {
            kept += static_cast<std::size_t>(keeps(row));
        }
        // One slot of slack so the unconditional store below always lands
        // in-bounds, including on the final rejected row.
        std::vector<std::size_t> selected(kept + 1);
        std::size_t out = 0;
        for (std::size_t row = 0; row < rows; ++row) {
            selected[out] = row;
            out += static_cast<std::size_t>(keeps(row));
        }
        selected.resize(kept);

        using ConjunctDiff = std::vector<ir::Expr>::difference_type;
        const std::vector<ir::Expr> rest(conjuncts.begin() + ConjunctDiff{1}, conjuncts.end());
        return filter_selection_impl(input, rest, scalars, std::move(selected));
    }

    std::vector<std::size_t> selected(input.rows());
    std::iota(selected.begin(), selected.end(), std::size_t{0});
    return filter_selection_impl(input, conjuncts, scalars, std::move(selected));
}

auto filter_project_table(const Table& input, const ir::Expr& predicate,
                          const std::vector<ir::ColumnRef>& columns, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string> {
    return filter_table_impl(input, predicate, &columns, 0, scalars);
}

auto filter_table_limit(const Table& input, const ir::Expr& predicate, std::size_t row_limit,
                        const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    return filter_table_impl(input, predicate, nullptr, row_limit, scalars);
}

}  // namespace ibex::runtime
