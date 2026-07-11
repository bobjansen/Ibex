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
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <functional>
#include <optional>
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
    // int64 × int64 → int64
    if (const auto* l = std::get_if<Column<std::int64_t>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {
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

// Forward declarations: the vectorized predicate evaluator below dispatches the
// same way the select/update field evaluator does, rather than reimplementing
// functions. Row-wise calls go to evaluate_field_column (which consults the one
// scalar-function registry: casts, ceil/floor/trunc, round, math, date parts,
// pmin/pmax, is_nan); the column-level lag/lead go to eval_lag_lead_column (the
// same helper select/update uses). Neither is duplicated here.

// Generator builtins (rand_*/rep) come from the function registry; eval_value_vec
// treats a Generator call as a column leaf so it can be nested inside arithmetic.

// Boolean predicate evaluator (comparisons, logical, is_null). eval_value_vec
// routes boolean-producing nodes here so a Bool result can be used in value
// position (e.g. `update { flag = x > 5 }`, `update { miss = is_null(x) }`).

// True if a field expression must go through the vectorized, validity-aware
// path (a boolean node, coalesce, or a non-row-local call — RNG / lag / lead —
// anywhere in the tree). Defined far below; eval_value_vec needs it to decide
// which arguments of a scalar call to materialize.

namespace {

// Evaluate a scalar call (abs, sqrt, casts, …) whose arguments nest a
// non-row-local sub-expression (RNG / lag / lead): materialize those arguments
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
                    fn != nullptr && fn->kind == ir::FnKind::Generator) {
                    // Generator (rand_* / rep) as a column leaf — lets a
                    // generated column be nested inside arithmetic (e.g.
                    // `t + rand_normal(0, 1)`), using the same vectorized
                    // kernel as a bare generator field.
                    auto col = fn->column_eval(node, table, n);
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
                if (node.callee == "coalesce") {
                    // First non-null argument per row (validity-aware). Arguments
                    // must share one column type (checked at inference).
                    if (node.args.size() < 2) {
                        return std::unexpected("coalesce: expected at least 2 arguments");
                    }
                    std::vector<ColResult> cols;
                    cols.reserve(node.args.size());
                    for (const auto& a : node.args) {
                        auto c = eval_value_vec(*a, table, scalars, n);
                        if (!c) {
                            return std::unexpected(c.error());
                        }
                        cols.push_back(std::move(*c));
                    }
                    return std::visit(
                        [&](const auto& c0) -> std::expected<ColResult, std::string> {
                            using Col = std::decay_t<decltype(c0)>;
                            Col out;
                            out.reserve(n);
                            ValidityBitmap valid(n, true);
                            bool any_invalid = false;
                            for (std::size_t i = 0; i < n; ++i) {
                                bool filled = false;
                                for (const auto& cr : cols) {
                                    const auto* vk = cr.get_validity();
                                    if (vk == nullptr || (*vk)[i]) {
                                        const auto* tc = std::get_if<Col>(&deref_col(cr));
                                        if (tc == nullptr) {
                                            return std::unexpected(
                                                "coalesce: arguments must share one type");
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
                            ColResult r{ColumnValue{std::move(out)}};
                            if (any_invalid) {
                                r.owned_validity = std::move(valid);
                            }
                            return r;
                        },
                        deref_col(cols[0]));
                }
                if (node.callee != "lag" && node.callee != "lead") {
                    // A scalar call (abs, sqrt, casts, round, pmin/pmax, date
                    // parts, …). If any argument nests a non-row-local
                    // sub-expression (RNG / lag / lead / a boolean node), the
                    // per-row evaluator can't produce it — materialize those
                    // arguments into columns first, then apply the scalar
                    // function over them (e.g. `abs(rand_normal(0, 1))`).
                    const bool args_need_vec = std::ranges::any_of(
                        node.args, [](const auto& a) { return field_uses_vectorized_eval(*a); });
                    if (args_need_vec) {
                        return eval_scalar_over_columns(node, table, scalars, n);
                    }
                    // Otherwise delegate to the row-wise field evaluator, which
                    // dispatches through the shared scalar registry. Extern
                    // scalar functions are not available in predicate position
                    // (externs are not threaded into this vectorized evaluator).
                    auto col = evaluate_field_column(expr, table, scalars, nullptr);
                    if (!col) {
                        return std::unexpected(col.error());
                    }
                    return ColResult{std::move(*col)};
                }
                // lag/lead are column-level shifts (each output row reads a
                // different input row), so they cannot go through the row-wise
                // registry. Delegate to the same helper select/update uses rather
                // than reimplementing the shift here.
                auto shifted =
                    eval_lag_lead_column(node, table, node.callee == "lag", scalars, nullptr);
                if (!shifted) {
                    return std::unexpected(shifted.error());
                }
                return ColResult{std::move(shifted->column), std::move(shifted->validity)};
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
    ValidityBitmap merged(n, true);
    bool any_invalid = false;
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
            for (std::size_t i = 0; i < n; ++i) {
                if (!(*v)[i]) {
                    merged.set(i, false);
                    any_invalid = true;
                }
            }
            tmp.add_column(name, std::move(owned), *v);
        } else {
            tmp.add_column(name, std::move(owned));
        }
        ir::Expr ref;
        ref.node = ir::ColumnRef{.name = name};
        rewritten.args.push_back(ir::make_expr_ptr(std::move(ref)));
    }
    ir::Expr rewritten_expr;
    rewritten_expr.node = std::move(rewritten);
    auto col = evaluate_field_column(rewritten_expr, tmp, scalars, nullptr);
    if (!col) {
        return std::unexpected(col.error());
    }
    ColResult res{std::move(*col)};
    if (any_invalid) {
        res.owned_validity = std::move(merged);
    }
    return res;
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
