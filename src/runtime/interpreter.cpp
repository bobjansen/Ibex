#include <ibex/ir/expr_predicates.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/pipeline.hpp>
#include <ibex/runtime/rng.hpp>
#include <ibex/runtime/safe_arith.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <robin_hood.h>
#include <set>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#ifdef __GLIBC__
#include <malloc.h>  // mallopt
#endif

#include "join_internal.hpp"
#include "model_internal.hpp"
#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

// Process-wide allocator tuning to flatten the large-buffer page-fault cliff.
//
// Every result column is backed by std::vector<T>, so any column above glibc's
// dynamic mmap threshold (grows up to 32 MB = 4M float64 rows) is served by a
// fresh mmap and munmapped on free. The next same-size allocation re-mmaps and
// re-faults every 4 KB page on first touch — a ~5x throughput cliff once columns
// cross ~32 MB (see plans/benchmark-perf-priorities.md, P0). Serving large
// allocations from the main arena and never trimming the heap top lets freed
// buffers recycle already-faulted pages across the warmup/timed iterations.
// glibc-only; a no-op elsewhere. Opt out via IBEX_NO_MALLOC_TUNING.
void tune_allocator_once() {
#ifdef __GLIBC__
    static std::once_flag flag;
    std::call_once(flag, [] {
        if (const char* off = std::getenv("IBEX_NO_MALLOC_TUNING");
            off != nullptr && off[0] != '\0' && off[0] != '0') {
            return;
        }
        mallopt(M_MMAP_MAX, 0);         // large allocs from sbrk arena, not mmap
        mallopt(M_TRIM_THRESHOLD, -1);  // keep freed buffers resident for reuse
    });
#endif
}

auto invoke_extern_call(const ir::ExternCallNode& ec, const ScalarRegistry* scalars,
                        const ExternRegistry* externs) -> std::expected<ExternValue, std::string>;
auto execute_program_preamble(const std::vector<ir::NodePtr>& preamble,
                              const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<void, std::string>;

auto ordering_keys_present(const std::vector<ir::OrderKey>& keys,
                           const std::unordered_map<std::string, std::size_t>& index) -> bool {
    return std::ranges::all_of(keys, [index](const auto& key) { return index.contains(key.name); });
}

auto ordering_keys_for_table(const Table& input, const std::vector<ir::OrderKey>& keys)
    -> std::vector<ir::OrderKey> {
    if (!keys.empty()) {
        return keys;
    }
    std::vector<ir::OrderKey> resolved;
    resolved.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        resolved.push_back(ir::OrderKey{.name = entry.name, .ascending = true});
    }
    return resolved;
}

struct ComputedColumn {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};

auto evaluate_rank_column(const Table& input, const ir::RankExpr& rank,
                          const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<ComputedColumn, std::string>;

auto format_tables(const TableRegistry& registry) -> std::string {
    if (registry.empty()) {
        return "<none>";
    }
    std::vector<std::string_view> names;
    names.reserve(registry.size());
    for (const auto& entry : registry) {
        names.emplace_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

[[noreturn]] void invariant_violation(std::string_view detail) {
    // This is triggered by a severe bug, everything in here is on a best effort basis
    (void)std::fputs("ibex internal invariant violated (runtime/interpreter): ", stderr);
    (void)std::fwrite(detail.data(), sizeof(char), detail.size(), stderr);
    (void)std::fputc('\n', stderr);
    std::abort();
}

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
struct ColResult {
    std::variant<const ColumnValue*, ColumnValue> data;
    const ValidityBitmap* validity = nullptr;      // source column validity (no-copy)
    std::optional<ValidityBitmap> owned_validity;  // for computed expressions

    explicit ColResult(const ColumnValue* p) : data(p) {}
    explicit ColResult(ColumnValue v) : data(std::move(v)) {}

    [[nodiscard]] const ValidityBitmap* get_validity() const noexcept {
        return owned_validity ? &*owned_validity : validity;
    }
};

auto deref_col(const ColResult& r) -> const ColumnValue& {
    return std::visit(
        [](const auto& v) -> const ColumnValue& {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, const ColumnValue*>)
                return *v;
            else
                // Safe: `deref_col` only accepts lvalue `ColResult`; rvalue overloads
                // are deleted, so this reference cannot bind to a temporary.
                // NOLINTNEXTLINE(bugprone-return-const-ref-from-parameter)
                return v;
        },
        r.data);
}

auto deref_col(ColResult&&) -> const ColumnValue& = delete;
auto deref_col(const ColResult&&) -> const ColumnValue& = delete;

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

auto pack_selected_bool_bits(std::uint64_t values, std::uint64_t mask) noexcept -> std::uint64_t {
#if defined(__BMI2__)
    return _pext_u64(values, mask);
#else
    std::uint64_t packed = 0;
    unsigned out_bit = 0;
    while (mask != 0) {
        const unsigned bit = static_cast<unsigned>(std::countr_zero(mask));
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
    const unsigned shift = static_cast<unsigned>(out_bit % kBitsPerWord);
    dst_words[dst_word] |= packed << shift;
    if (shift != 0 && count > kBitsPerWord - shift) {
        dst_words[dst_word + 1] |= packed >> (kBitsPerWord - shift);
    }
    out_bit += count;
}

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

// Element-wise arithmetic: result type = common_type<L, R>.
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
template <typename ColT, typename LitT>
auto cmp_col_scalar_into(ir::CompareOp op, const ColT* __restrict cp, LitT rv,
                         uint8_t* __restrict mp, std::size_t n) -> void {
    using Common = std::common_type_t<ColT, LitT>;
    const Common crv = static_cast<Common>(rv);
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

// Dispatch column-vs-scalar comparison over all type combinations.
using LitVal = std::variant<std::int64_t, double, bool, std::string, Date, Timestamp>;
template <typename T>
constexpr bool is_string_like_v =
    std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>;
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
                uint8_t v = (op == ir::CompareOp::Ne) ? 1 : 0;
                std::fill(mp, mp + n, v);
                result.apply_validity(validity, n);
                return result;
            }
            for (std::size_t i = 0; i < n; ++i) {
                std::string_view cv = (*cat_col)[i];
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
                const double lhs = static_cast<double>(date_col->data()[idx].days);
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
                const double lhs = static_cast<double>(ts_col->data()[idx].nanos);
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

// Element-wise comparison between two full columns.
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
    auto return_with_validity = [&]() -> Mask {
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

// Forward declarations: the vectorized predicate evaluator below dispatches the
// same way the select/update field evaluator does, rather than reimplementing
// functions. Row-wise calls go to evaluate_field_column (which consults the one
// scalar-function registry: casts, ceil/floor/trunc, round, math, date parts,
// pmin/pmax, is_nan); the column-level lag/lead go to eval_lag_lead_column (the
// same helper select/update uses). Neither is duplicated here.
struct LagLeadResult {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};
auto evaluate_field_column(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars,
                           const ExternRegistry* externs)
    -> std::expected<ColumnValue, std::string>;
auto eval_lag_lead_column(const ir::CallExpr& call, const Table& input, bool is_lag,
                          const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<LagLeadResult, std::string>;
// Vectorized RNG column generator (rand_normal/rand_uniform/...); eval_value_vec
// treats an RNG call as a column leaf so RNG can be nested inside arithmetic.
auto apply_rng_func(const ir::CallExpr& call, std::size_t rows)
    -> std::expected<ColumnValue, std::string>;

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
                if (ir::is_rng_func(node.callee)) {
                    // RNG generator as a column leaf — lets RNG be nested inside
                    // arithmetic (e.g. `t + rand_normal(0, 1)`), using the same
                    // vectorized draw as a bare RNG field.
                    auto col = apply_rng_func(node, n);
                    if (!col) {
                        return std::unexpected(col.error());
                    }
                    return ColResult{std::move(*col)};
                }
                if (node.callee != "lag" && node.callee != "lead") {
                    // Any other call: delegate to the row-wise field evaluator,
                    // which dispatches through the shared scalar registry. Extern
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
                ColResult res{std::move(shifted->column)};
                res.owned_validity = std::move(shifted->validity);
                return res;
            } else {
                return std::unexpected("filter: not a value expression");
            }
        },
        expr.node);
}

// Compute a boolean Mask for all n rows, with 3-valued logic (3VL) for nulls.
// valid==nullopt means all rows are valid (common non-null path, zero overhead).
auto compute_mask(const ir::Expr& expr, const Table& table, const ScalarRegistry* scalars,
                  std::size_t n) -> std::expected<Mask, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<Mask, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::CompareExpr>) {
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
                            uint8_t a_false = lval[i] & (1U - lp[i]);
                            uint8_t b_false = rval[i] & (1U - rp[i]);
                            (*left->valid)[i] = (lval[i] & rval[i]) | a_false | b_false;
                        } else {
                            // definitively true if either side is a known true
                            uint8_t a_true = lval[i] & lp[i];
                            uint8_t b_true = rval[i] & rp[i];
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
                // ColumnRef, Literal, BinaryExpr, CallExpr, RankExpr — not boolean
                return std::unexpected("filter: not a boolean expression");
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
        std::uint64_t bits = 0;
        for (std::size_t i = 0; i < lim; ++i) {
            const std::size_t row = base + i;
            const bool keep = vp ? ((mp[row] & vp[row]) != 0) : (mp[row] != 0);
            bits |= static_cast<std::uint64_t>(keep) << i;
        }
        const std::size_t block_kept = static_cast<std::size_t>(std::popcount(bits));
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
                        uint32_t len = src_off[si + 1] - src_off[si];
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
                    using T = typename ColT::value_type;
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

auto project_table(const Table& input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Table, std::string> {
    Table output;
    for (const auto& col : columns) {
        const auto* entry = input.find_entry(col.name);
        if (entry == nullptr) {
            return std::unexpected("select column not found: " + col.name +
                                   " (available: " + format_columns(input) + ")");
        }
        // Share the column's shared_ptr instead of deep-copying its data. The
        // projected table is a read-only selection; under copy-on-write any
        // later mutation reseats a fresh column, so sharing is safe.
        output.add_column_shared(col.name, entry->column, entry->validity);
    }
    if (input.ordering.has_value() && ordering_keys_present(*input.ordering, output.index)) {
        output.ordering = input.ordering;
    }
    if (input.time_index.has_value()) {
        if (output.index.contains(*input.time_index)) {
            output.time_index = input.time_index;
        } else {
            output.time_index.reset();
            output.ordering.reset();
        }
    }
    normalize_time_index(output);
    return output;
}

auto rename_table(const Table& input, const std::vector<ir::RenameSpec>& renames)
    -> std::expected<Table, std::string> {
    std::unordered_map<std::string, std::string> rename_map;
    rename_map.reserve(renames.size());
    for (const auto& spec : renames) {
        const auto* entry = input.find_entry(spec.old_name);
        if (entry == nullptr) {
            return std::unexpected("rename: column not found: " + spec.old_name +
                                   " (available: " + format_columns(input) + ")");
        }
        rename_map[spec.old_name] = spec.new_name;
    }

    Table output;
    for (const auto& entry : input.columns) {
        auto it = rename_map.find(entry.name);
        const std::string& out_name = (it != rename_map.end()) ? it->second : entry.name;
        // Rename only relabels columns; share the data rather than copying it.
        output.add_column_shared(out_name, entry.column, entry.validity);
    }

    if (input.ordering.has_value()) {
        std::vector<ir::OrderKey> new_ordering;
        for (const auto& key : *input.ordering) {
            auto it = rename_map.find(key.name);
            new_ordering.push_back({.name = (it != rename_map.end()) ? it->second : key.name,
                                    .ascending = key.ascending});
        }
        output.ordering = std::move(new_ordering);
    }

    if (input.time_index.has_value()) {
        auto it = rename_map.find(*input.time_index);
        output.time_index = (it != rename_map.end()) ? it->second : *input.time_index;
    }

    normalize_time_index(output);
    return output;
}

auto columns_table(const Table& input) -> std::expected<Table, std::string> {
    Table output;
    Column<std::string> names;
    names.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        names.push_back(entry.name);
    }
    output.add_column("name", std::move(names));
    return output;
}

struct Key {
    std::vector<ScalarValue> values;
};

struct KeyHash {
    auto operator()(const Key& key) const -> std::size_t {
        std::size_t seed = 0;
        auto hash_combine = [&](std::size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        for (const auto& value : key.values) {
            std::size_t h = std::visit(
                [](const auto& v) { return std::hash<std::decay_t<decltype(v)>>{}(v); }, value);
            hash_combine(h);
        }
        return seed;
    }
};

struct KeyEq {
    auto operator()(const Key& a, const Key& b) const -> bool { return a.values == b.values; }
};

using ExprValue = std::variant<std::int64_t, double, bool, std::string, Date, Timestamp>;

struct AggSlot {
    ir::AggFunc func = ir::AggFunc::Sum;
    ExprType kind = ExprType::Int;
    bool has_value = false;
    std::int64_t count = 0;
    std::int64_t int_value = 0;
    double double_value = 0.0;
    double sum = 0.0;
    double m2 = 0.0;     ///< Welford M2 accumulator: Σ(x-mean)². `double_value`
                         ///< doubles as the running mean for the moment aggs.
    double m3 = 0.0;     ///< Σ(x-mean)³ (online), for skewness.
    double m4 = 0.0;     ///< Σ(x-mean)⁴ (online), for kurtosis.
    double param = 0.0;  ///< Function-specific parameter (e.g. EWMA alpha).
    ScalarValue first_value;
    ScalarValue last_value;
    std::vector<double> values;  ///< Collected values for median.
};

// Online central-moment accumulators (Welford / Pébay), shared by the chunked
// aggregate operators. `double_value` holds the running mean; m2/m3/m4 hold
// Σ(x-mean)^k. These match the two-pass central moments the materializing
// aggregate computes to within floating-point rounding — and for stddev the
// M2 update is bit-identical (Pébay's term1 reduces to the simple Welford
// step), so `stddev` results agree exactly across paths.
inline void agg_update_stddev(AggSlot& slot, double x) {
    slot.count += 1;
    const double delta = x - slot.double_value;
    slot.double_value += delta / static_cast<double>(slot.count);
    slot.m2 += delta * (x - slot.double_value);
}

// Full m2/m3/m4 update for skewness/kurtosis (Pébay single-value recurrence).
// Updates m4 and m3 before m2 because they read the pre-update accumulators.
inline void agg_update_moments(AggSlot& slot, double x) {
    const double n1 = static_cast<double>(slot.count);
    slot.count += 1;
    const double n = static_cast<double>(slot.count);
    const double delta = x - slot.double_value;
    const double delta_n = delta / n;
    const double delta_n2 = delta_n * delta_n;
    const double term1 = delta * delta_n * n1;
    slot.double_value += delta_n;
    slot.m4 += (term1 * delta_n2 * ((n * n) - (3.0 * n) + 3.0)) + (6.0 * delta_n2 * slot.m2) -
               (4.0 * delta_n * slot.m3);
    slot.m3 += (term1 * delta_n * (n - 2.0)) - (3.0 * delta_n * slot.m2);
    slot.m2 += term1;
}

inline auto agg_finalize_stddev(const AggSlot& slot) -> double {
    return slot.count < 2 ? 0.0 : std::sqrt(slot.m2 / static_cast<double>(slot.count - 1));
}

inline auto agg_finalize_skew(const AggSlot& slot) -> double {
    if (slot.count < 3 || slot.m2 == 0.0) {
        return 0.0;
    }
    const auto n = static_cast<double>(slot.count);
    // Fisher–Pearson sample skewness (matches pandas/scipy default).
    return (n * std::sqrt(n - 1.0) / (n - 2.0)) * (slot.m3 / std::pow(slot.m2, 1.5));
}

inline auto agg_finalize_kurtosis(const AggSlot& slot) -> double {
    if (slot.count < 4 || slot.m2 == 0.0) {
        return 0.0;
    }
    const auto n = static_cast<double>(slot.count);
    // Unbiased Fisher excess kurtosis (matches pandas/scipy default).
    return (n - 1.0) / ((n - 2.0) * (n - 3.0)) *
           (((n + 1.0) * n * slot.m4 / (slot.m2 * slot.m2)) - (3.0 * (n - 1.0)));
}

// Whether a streamed aggregate slot has enough observations to be non-null.
// Mirrors the materializing aggregate's `agg_result_is_valid`.
inline auto chunked_agg_valid(ir::AggFunc func, const AggSlot& slot) -> bool {
    switch (func) {
        case ir::AggFunc::Mean:
            return slot.count > 0;
        case ir::AggFunc::Sum:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
            return slot.has_value;
        case ir::AggFunc::Stddev:
            return slot.count >= 2;
        case ir::AggFunc::Skew:
            return slot.count >= 3;
        case ir::AggFunc::Kurtosis:
            return slot.count >= 4;
        default:  // Count
            return true;
    }
}

// Whether a streamed aggregate carries a validity bitmap at all (Count never
// produces nulls; the value-bearing aggs may).
inline auto chunked_agg_tracks_validity(ir::AggFunc func) -> bool {
    switch (func) {
        case ir::AggFunc::Sum:
        case ir::AggFunc::Mean:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::Stddev:
        case ir::AggFunc::Skew:
        case ir::AggFunc::Kurtosis:
            return true;
        default:
            return false;
    }
}

struct AggState {
    std::vector<AggSlot> slots;
};

auto expr_type_for_column(const ColumnValue& column) -> ExprType {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return ExprType::Double;
    }
    if (std::holds_alternative<Column<bool>>(column)) {
        return ExprType::Bool;
    }
    if (std::holds_alternative<Column<Date>>(column)) {
        return ExprType::Date;
    }
    if (std::holds_alternative<Column<Timestamp>>(column)) {
        return ExprType::Timestamp;
    }
    return ExprType::String;
}

auto distinct_table(const Table& input) -> std::expected<Table, std::string> {
    if (input.columns.empty()) {
        Table output = input;
        output.ordering.reset();
        output.time_index.reset();
        return output;
    }
    std::size_t rows = input.rows();
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(*entry.column));
    }
    for (auto& entry : output.columns) {
        std::visit([&](auto& col) { col.reserve(rows); }, *entry.column);
    }

    robin_hood::unordered_flat_set<Key, KeyHash, KeyEq> seen;
    seen.reserve(rows);

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(input.columns.size());
        for (const auto& entry : input.columns) {
            key.values.push_back(scalar_from_column(*entry.column, row));
        }
        if (!seen.insert(std::move(key)).second) {
            continue;
        }
        for (std::size_t col = 0; col < input.columns.size(); ++col) {
            append_value(output.mutable_column(col), *input.columns[col].column, row);
        }
    }
    output.ordering.reset();
    output.time_index.reset();
    return output;
}

template <typename Idx>
auto gather_rows(const Table& input, const std::vector<Idx>& idx,
                 const std::vector<ir::OrderKey>* ordering = nullptr) -> Table {
    const std::size_t rows = idx.size();
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        ColumnValue gathered = std::visit(
            [&](const auto& src) -> ColumnValue {
                using ColT = std::decay_t<decltype(src)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    std::vector<Column<Categorical>::code_type> codes(rows);
                    const auto* sp = src.codes_data();
                    for (std::size_t pos = 0; pos < rows; ++pos)
                        codes[pos] = sp[static_cast<std::size_t>(idx[pos])];
                    return Column<Categorical>(src.dictionary_ptr(), src.index_ptr(),
                                               std::move(codes));
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    std::size_t total_chars = 0;
                    const auto* src_off = src.offsets_data();
                    const auto* src_char = src.chars_data();
                    for (std::size_t pos = 0; pos < rows; ++pos) {
                        auto si = static_cast<std::size_t>(idx[pos]);
                        total_chars += src_off[si + 1] - src_off[si];
                    }
                    ColT dst;
                    dst.resize_for_gather(rows, total_chars);
                    auto* dst_off = dst.offsets_data();
                    auto* dst_char = dst.chars_data();
                    dst_off[0] = 0;
                    std::uint32_t cur = 0;
                    for (std::size_t pos = 0; pos < rows; ++pos) {
                        auto si = static_cast<std::size_t>(idx[pos]);
                        std::uint32_t len = src_off[si + 1] - src_off[si];
                        std::memcpy(dst_char + cur, src_char + src_off[si], len);
                        cur += len;
                        dst_off[pos + 1] = cur;
                    }
                    return dst;
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    ColT dst;
                    dst.resize(rows);
                    for (std::size_t pos = 0; pos < rows; ++pos)
                        dst.set(pos, src[static_cast<std::size_t>(idx[pos])]);
                    return dst;
                } else {
                    ColT dst;
                    dst.resize(rows);
                    for (std::size_t pos = 0; pos < rows; ++pos)
                        dst[pos] = src[static_cast<std::size_t>(idx[pos])];
                    return dst;
                }
            },
            *entry.column);
        output.add_column(entry.name, std::move(gathered));
        if (entry.validity.has_value()) {
            const auto& src_bm = *entry.validity;
            ValidityBitmap dst_bm(rows, false);
            for (std::size_t pos = 0; pos < rows; ++pos)
                dst_bm.set(pos, src_bm[static_cast<std::size_t>(idx[pos])]);
            output.columns.back().validity = std::move(dst_bm);
        }
    }

    if (ordering != nullptr) {
        output.ordering = *ordering;
    } else {
        output.ordering = input.ordering;
    }
    output.time_index = input.time_index;
    normalize_time_index(output);
    return output;
}

// LSD radix sort over pre-sign-flipped uint64 keys.
// Idx is the index type: uint32_t for tables ≤ UINT32_MAX rows, uint64_t otherwise.
// Keys must already be sign-flipped (int64 XOR 1<<63) so unsigned order == signed order.
// All 8 byte histograms are built in a single pass; passes where every element
// shares the same byte value are skipped (common for clustered timestamps).
template <typename Idx>
auto radix_sort_impl(std::vector<std::uint64_t> src_keys, std::size_t rows) -> std::vector<Idx> {
    // Build all 8 byte-histograms in one sequential scan.
    std::array<std::array<std::size_t, 256>, 8> hists{};
    for (std::size_t i = 0; i < rows; ++i) {
        auto k = src_keys[i];
        for (std::size_t p = 0; p < 8; ++p)
            ++hists[p][(k >> (p * 8U)) & 0xFFU];
    }

    std::vector<std::uint64_t> dst_keys(rows);
    std::vector<Idx> src_idx(rows);
    std::vector<Idx> dst_idx(rows);
    std::iota(src_idx.begin(), src_idx.end(), Idx{0});

    std::array<std::size_t, 256> cnt;  //  NOLINT(cppcoreguidelines-pro-type-member-init)
    for (std::size_t pass = 0; pass < 8; ++pass) {
        const auto& h = hists[pass];
        // Skip pass if all elements have the same byte value.
        std::size_t non_zero = 0;
        for (auto c : h)
            if (c)
                ++non_zero;
        if (non_zero <= 1)
            continue;

        auto shift = pass * 8U;
        // Convert histogram to exclusive prefix-sum write positions.
        std::size_t total = 0;
        for (std::size_t b = 0; b < 256; ++b) {
            cnt[b] = total;
            total += h[b];
        }
        // Stable scatter: sequential reads, random writes.
        // Prefetch the destination cache line a few elements ahead.
        for (std::size_t i = 0; i < rows; ++i) {
#if defined(__GNUC__) || defined(__clang__)
            constexpr std::size_t kPrefetchDist = 8;
            if (i + kPrefetchDist < rows) {
                std::size_t pb = (src_keys[i + kPrefetchDist] >> shift) & 0xFFU;
                __builtin_prefetch(&dst_keys[cnt[pb]], 1, 1);
                __builtin_prefetch(&dst_idx[cnt[pb]], 1, 1);
            }
#endif
            std::size_t bucket = (src_keys[i] >> shift) & 0xFFU;
            dst_keys[cnt[bucket]] = src_keys[i];
            dst_idx[cnt[bucket]] = src_idx[i];
            ++cnt[bucket];
        }
        std::swap(src_keys, dst_keys);
        std::swap(src_idx, dst_idx);
    }
    return src_idx;
}

// Dispatch to 32-bit indices for tables that fit, 64-bit otherwise.
// Keys are taken by move — caller's FlatKey::u64 is consumed, no copy needed.
using SortIdx = std::variant<std::vector<std::uint32_t>, std::vector<std::uint64_t>>;
auto radix_sort_u64_asc(std::vector<std::uint64_t> keys, std::size_t rows) -> SortIdx {
    if (rows <= std::numeric_limits<std::uint32_t>::max())
        return radix_sort_impl<std::uint32_t>(std::move(keys), rows);
    return radix_sort_impl<std::uint64_t>(std::move(keys), rows);
}

auto order_table(const Table& input, const std::vector<ir::OrderKey>& keys)
    -> std::expected<Table, std::string> {
    std::size_t rows = input.rows();
    if (input.time_index.has_value()) {
        if (keys.size() != 1 || keys[0].name != *input.time_index || !keys[0].ascending) {
            return std::unexpected("order on TimeFrame must be by time index ascending");
        }
    }
    auto resolved_keys = ordering_keys_for_table(input, keys);
    if (rows <= 1 || input.columns.empty()) {
        Table output = input;
        output.ordering = resolved_keys;
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    }

    // Fast pre-sorted check for single ascending Timestamp/Date/Int key — avoids building
    // the 8 MB flat_keys[0].u64 vector when the input is already sorted (common TimeFrame case).
    if (resolved_keys.size() == 1 && resolved_keys[0].ascending) {
        const auto* column = input.find(resolved_keys[0].name);
        if (column != nullptr) {
            bool already_sorted = false;
            std::visit(
                [&](const auto& col) {
                    using ColT = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i].nanos < col[i - 1].nanos) {
                                already_sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i] < col[i - 1]) {
                                already_sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i].days < col[i - 1].days) {
                                already_sorted = false;
                                break;
                            }
                        }
                    }
                },
                *column);
            if (already_sorted) {
                Table output = input;
                output.ordering = std::move(resolved_keys);
                output.time_index = input.time_index;
                normalize_time_index(output);
                return output;
            }
        }
    }

    // Pre-extract each sort key into a flat typed array so the hot comparator
    // loop does plain vector indexing rather than per-comparison variant dispatch.
    // I64 keys are sign-flipped to uint64 at extraction time so that unsigned
    // comparison is equivalent to signed comparison — this lets radix_sort_u64_asc
    // consume the vector directly without an extra copy.
    constexpr std::uint64_t kSignFlip = std::uint64_t{1} << 63;
    enum class FlatKind : std::uint8_t { I64, F64, Str };
    struct FlatKey {
        FlatKind kind = FlatKind::I64;
        std::vector<std::uint64_t> u64;  // Int / Date.days / Timestamp.nanos, sign-flipped
        std::vector<double> f64;
        std::vector<std::string_view> str;  // views into original column storage
        bool ascending = true;
    };

    std::vector<FlatKey> flat_keys;
    flat_keys.reserve(resolved_keys.size());
    for (const auto& key : resolved_keys) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("order column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        FlatKey fk;
        fk.ascending = key.ascending;
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (auto v : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(v) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                    fk.kind = FlatKind::F64;
                    fk.f64.assign(col.begin(), col.end());
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (const auto& d : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(d.days) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (const auto& ts : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(ts.nanos) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.u64.push_back(static_cast<std::uint64_t>(col[i] ? 1 : 0) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    fk.kind = FlatKind::Str;
                    fk.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.str.push_back(col[i]);
                } else {
                    // Categorical: sort by dictionary value (string_view into shared dict)
                    fk.kind = FlatKind::Str;
                    fk.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.str.push_back(col[i]);
                }
            },
            *column);
        flat_keys.push_back(std::move(fk));
    }

    // Fast path: single ascending I64 key — radix sort (pre-sorted case already handled above).
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::I64 && flat_keys[0].ascending) {
        auto sort_result = radix_sort_u64_asc(std::move(flat_keys[0].u64), rows);
        return std::visit(
            [&]<typename Idx>(const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                return gather_rows(input, idx, &resolved_keys);
            },
            sort_result);
    }

    // General path: multi-key or non-I64 or descending — comparison-based stable sort.
    // u64 keys compare correctly with unsigned < because sign-flip preserves order.
    auto compare_row = [&](std::size_t lhs, std::size_t rhs) -> bool {
        for (const auto& fk : flat_keys) {
            switch (fk.kind) {
                case FlatKind::I64: {
                    auto l = fk.u64[lhs];
                    auto r = fk.u64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::F64: {
                    auto l = fk.f64[lhs];
                    auto r = fk.f64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::Str: {
                    auto l = fk.str[lhs];
                    auto r = fk.str[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
            }
        }
        return lhs < rhs;
    };
    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), 0);
    std::stable_sort(idx.begin(), idx.end(), compare_row);
    return gather_rows(input, idx, &resolved_keys);
}

auto head_table(const Table& input, std::size_t count, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string> {
    if (count == 0) {
        Table output;
        for (const auto& entry : input.columns) {
            output.add_column(entry.name, make_empty_like(*entry.column));
        }
        output.ordering = input.ordering;
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    }

    const std::size_t rows = input.rows();
    if (rows <= count && group_by.empty()) {
        Table output = input;
        normalize_time_index(output);
        return output;
    }

    if (group_by.empty()) {
        std::vector<std::size_t> idx(std::min(rows, count));
        std::iota(idx.begin(), idx.end(), 0);
        return gather_rows(input, idx);
    }

    robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> seen_counts;
    seen_counts.reserve(rows);
    std::vector<std::size_t> idx;
    idx.reserve(
        std::min(rows, count * std::max<std::size_t>(1, rows / std::max<std::size_t>(1, count))));

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_by.size());
        for (const auto& ref : group_by) {
            const auto* column = input.find(ref.name);
            if (column == nullptr) {
                return std::unexpected("head group-by column not found: " + ref.name +
                                       " (available: " + format_columns(input) + ")");
            }
            key.values.push_back(scalar_from_column(*column, row));
        }
        auto& seen = seen_counts[key];
        if (seen >= count) {
            continue;
        }
        ++seen;
        idx.push_back(row);
    }

    return gather_rows(input, idx);
}

auto tail_table(const Table& input, std::size_t count, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string> {
    if (count == 0) {
        Table output;
        for (const auto& entry : input.columns) {
            output.add_column(entry.name, make_empty_like(*entry.column));
        }
        output.ordering = input.ordering;
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    }

    const std::size_t rows = input.rows();
    if (rows <= count && group_by.empty()) {
        Table output = input;
        normalize_time_index(output);
        return output;
    }

    if (group_by.empty()) {
        const std::size_t keep = std::min(rows, count);
        std::vector<std::size_t> idx(keep);
        const std::size_t start = rows - keep;
        std::iota(idx.begin(), idx.end(), start);
        return gather_rows(input, idx);
    }

    robin_hood::unordered_flat_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> groups;
    groups.reserve(rows);
    std::vector<Key> order;
    order.reserve(rows);

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_by.size());
        for (const auto& ref : group_by) {
            const auto* column = input.find(ref.name);
            if (column == nullptr) {
                return std::unexpected("tail group-by column not found: " + ref.name +
                                       " (available: " + format_columns(input) + ")");
            }
            key.values.push_back(scalar_from_column(*column, row));
        }
        auto [it, inserted] = groups.try_emplace(key);
        if (inserted) {
            order.push_back(key);
        }
        it->second.push_back(row);
    }

    std::vector<std::size_t> idx;
    idx.reserve(rows);
    for (const auto& key : order) {
        const auto& group_rows = groups.find(key)->second;
        const std::size_t keep = std::min(group_rows.size(), count);
        const std::size_t start = group_rows.size() - keep;
        idx.insert(idx.end(), group_rows.begin() + static_cast<std::ptrdiff_t>(start),
                   group_rows.end());
    }

    return gather_rows(input, idx);
}

auto append_scalar(ColumnValue& column, const ScalarValue& value) -> void {
    std::visit(
        [&](auto& col) {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value);
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(static_cast<std::int64_t>(*double_value));
                } else {
                    invariant_violation("append_scalar: expected Int64-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, double>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(static_cast<double>(*int_value));
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(*double_value);
                } else {
                    invariant_violation("append_scalar: expected Float64-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, bool>) {
                if (const auto* bool_value = std::get_if<bool>(&value)) {
                    col.push_back(*bool_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value != 0);
                } else {
                    invariant_violation("append_scalar: expected Bool-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, std::string_view>) {
                // Column<std::string> flat-buffer specialization uses value_type=string_view.
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    invariant_violation("append_scalar: expected String scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, Date>) {
                if (const auto* date_value = std::get_if<Date>(&value)) {
                    col.push_back(*date_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(int64_to_date_checked(*int_value));
                } else {
                    invariant_violation("append_scalar: expected Date-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                if (const auto* ts_value = std::get_if<Timestamp>(&value)) {
                    col.push_back(*ts_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(Timestamp{*int_value});
                } else {
                    invariant_violation("append_scalar: expected Timestamp-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    invariant_violation("append_scalar: expected String scalar for Categorical");
                }
            }
        },
        column);
}

auto broadcast_scalar_column(const ScalarValue& value, std::size_t rows) -> ColumnValue {
    return std::visit(
        [rows](const auto& v) -> ColumnValue {
            using V = std::decay_t<decltype(v)>;
            Column<V> col;
            col.resize(rows, v);
            return ColumnValue{std::move(col)};
        },
        value);
}

struct FastOperand {
    bool is_column = false;
    const ColumnValue* column = nullptr;
    ScalarValue literal;
    ExprType kind = ExprType::Int;
};

auto scalar_kind_from_value(const ScalarValue& value) -> ExprType {
    if (std::holds_alternative<std::int64_t>(value)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<double>(value)) {
        return ExprType::Double;
    }
    if (std::holds_alternative<bool>(value)) {
        return ExprType::Bool;
    }
    if (std::holds_alternative<Date>(value)) {
        return ExprType::Date;
    }
    if (std::holds_alternative<Timestamp>(value)) {
        return ExprType::Timestamp;
    }
    return ExprType::String;
}

auto scalar_from_literal(const ir::Literal& literal) -> ScalarValue {
    return std::visit([](const auto& v) -> ScalarValue { return v; }, literal.value);
}

auto resolve_fast_operand(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::optional<FastOperand> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (const auto* source = input.find(col->name); source != nullptr) {
            return FastOperand{.is_column = true,
                               .column = source,
                               .literal = ScalarValue{},
                               .kind = expr_type_for_column(*source)};
        }
        if (scalars != nullptr) {
            if (auto it = scalars->find(col->name); it != scalars->end()) {
                return FastOperand{.is_column = false,
                                   .column = nullptr,
                                   .literal = it->second,
                                   .kind = scalar_kind_from_value(it->second)};
            }
        }
        return std::nullopt;
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        ScalarValue value = scalar_from_literal(*lit);
        return FastOperand{.is_column = false,
                           .column = nullptr,
                           .literal = value,
                           .kind = scalar_kind_from_value(value)};
    }
    return std::nullopt;
}

auto apply_int_op(ir::ArithmeticOp op, std::int64_t lhs, std::int64_t rhs) -> std::int64_t {
    switch (op) {
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
    return 0;
}

auto apply_double_op(ir::ArithmeticOp op, double lhs, double rhs) -> double {
    switch (op) {
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
    return 0.0;
}

auto get_int_value(const FastOperand& op, std::size_t row) -> std::int64_t {
    if (!op.is_column) {
        if (const auto* int_value = std::get_if<std::int64_t>(&op.literal)) {
            return *int_value;
        }
        if (const auto* date_value = std::get_if<Date>(&op.literal)) {
            return date_value->days;
        }
        if (const auto* ts_value = std::get_if<Timestamp>(&op.literal)) {
            return ts_value->nanos;
        }
        return static_cast<std::int64_t>(std::get<double>(op.literal));
    }
    if (const auto* int_col = std::get_if<Column<std::int64_t>>(op.column)) {
        return (*int_col)[row];
    }
    if (const auto* double_col = std::get_if<Column<double>>(op.column)) {
        return static_cast<std::int64_t>((*double_col)[row]);
    }
    if (const auto* date_col = std::get_if<Column<Date>>(op.column)) {
        return date_col->operator[](row).days;
    }
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(op.column)) {
        return ts_col->operator[](row).nanos;
    }
    invariant_violation("get_int_value: unexpected operand column type");
}

auto get_double_value(const FastOperand& op, std::size_t row) -> double {
    if (!op.is_column) {
        if (const auto* int_value = std::get_if<std::int64_t>(&op.literal)) {
            return static_cast<double>(*int_value);
        }
        if (const auto* date_value = std::get_if<Date>(&op.literal)) {
            return static_cast<double>(date_value->days);
        }
        if (const auto* ts_value = std::get_if<Timestamp>(&op.literal)) {
            return static_cast<double>(ts_value->nanos);
        }
        return std::get<double>(op.literal);
    }
    if (const auto* int_col = std::get_if<Column<std::int64_t>>(op.column)) {
        return static_cast<double>((*int_col)[row]);
    }
    if (const auto* double_col = std::get_if<Column<double>>(op.column)) {
        return (*double_col)[row];
    }
    if (const auto* date_col = std::get_if<Column<Date>>(op.column)) {
        return static_cast<double>(date_col->operator[](row).days);
    }
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(op.column)) {
        return static_cast<double>(ts_col->operator[](row).nanos);
    }
    invariant_violation("get_double_value: unexpected operand column type");
}

auto try_fast_update_binary(const ir::Expr& expr, const Table& input, std::size_t rows,
                            ExprType output_kind, const ScalarRegistry* scalars)
    -> std::optional<ColumnValue> {
    const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node);
    if (bin == nullptr) {
        return std::nullopt;
    }
    auto left = resolve_fast_operand(*bin->left, input, scalars);
    if (!left) {
        return std::nullopt;
    }
    auto right = resolve_fast_operand(*bin->right, input, scalars);
    if (!right) {
        return std::nullopt;
    }
    if (left->kind == ExprType::String || right->kind == ExprType::String ||
        left->kind == ExprType::Date || right->kind == ExprType::Date ||
        left->kind == ExprType::Timestamp || right->kind == ExprType::Timestamp) {
        return std::nullopt;
    }
    // Helper: dispatch on (op × layout) OUTSIDE the inner loop so each resulting
    // loop body is a branch-free array kernel that the compiler can auto-vectorize.
    // `run` receives a stateless lambda (unique type per op) and executes the
    // appropriate col/col, col/scalar, or scalar/col loop.
    auto make_double_result = [&](auto op_fn, const double* lp, double ls, const double* rp,
                                  double rs) -> ColumnValue {
        Column<double> out;
        out.resize(rows);
        double* dst = out.data();
        if (lp && rp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rp[i]);
        } else if (lp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rs);
        } else {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(ls, rp[i]);
        }
        return ColumnValue{std::move(out)};
    };
    auto make_int_result = [&](auto op_fn, const std::int64_t* lp, std::int64_t ls,
                               const std::int64_t* rp, std::int64_t rs) -> ColumnValue {
        Column<std::int64_t> out;
        out.resize(rows);
        std::int64_t* dst = out.data();
        if (lp && rp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rp[i]);
        } else if (lp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rs);
        } else {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(ls, rp[i]);
        }
        return ColumnValue{std::move(out)};
    };

    if (output_kind == ExprType::Double) {
        if (!left->is_column && !right->is_column) {
            double value =
                apply_double_op(bin->op, get_double_value(*left, 0), get_double_value(*right, 0));
            Column<double> out;
            out.assign(rows, value);
            return ColumnValue{std::move(out)};
        }
        // Hoist all variant/type dispatch outside the inner loop.
        // Falls back to nullptr + scalar=0 for int-typed columns (uncommon path
        // handled by the fallback reserve+push_back loop below).
        const double* lp = (left->is_column && left->kind == ExprType::Double)
                               ? std::get<Column<double>>(*left->column).data()
                               : nullptr;
        const double* rp = (right->is_column && right->kind == ExprType::Double)
                               ? std::get<Column<double>>(*right->column).data()
                               : nullptr;
        double ls = left->is_column ? 0.0 : get_double_value(*left, 0);
        double rs = right->is_column ? 0.0 : get_double_value(*right, 0);
        // Only take the SIMD path when every used operand resolved to a raw pointer.
        bool left_ok = !left->is_column || lp != nullptr;
        bool right_ok = !right->is_column || rp != nullptr;
        if (left_ok && right_ok) {
            // Dispatch on op once, outside the loop, so each kernel is branch-free.
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return make_double_result([](double a, double b) { return a + b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Sub:
                    return make_double_result([](double a, double b) { return a - b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Mul:
                    return make_double_result([](double a, double b) { return a * b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Div:
                    return make_double_result([](double a, double b) { return a / b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Mod:
                    return make_double_result([](double a, double b) { return std::fmod(a, b); },
                                              lp, ls, rp, rs);
            }
        }
        // Fallback: handles int-column inputs that need cast-to-double.
        Column<double> out;
        out.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row)
            out.push_back(apply_double_op(bin->op, get_double_value(*left, row),
                                          get_double_value(*right, row)));
        return ColumnValue{std::move(out)};
    }
    if (output_kind == ExprType::Int) {
        if (!left->is_column && !right->is_column) {
            std::int64_t value =
                apply_int_op(bin->op, get_int_value(*left, 0), get_int_value(*right, 0));
            Column<std::int64_t> out;
            out.assign(rows, value);
            return ColumnValue{std::move(out)};
        }
        const std::int64_t* lp = (left->is_column && left->kind == ExprType::Int)
                                     ? std::get<Column<std::int64_t>>(*left->column).data()
                                     : nullptr;
        const std::int64_t* rp = (right->is_column && right->kind == ExprType::Int)
                                     ? std::get<Column<std::int64_t>>(*right->column).data()
                                     : nullptr;
        std::int64_t ls = left->is_column ? 0 : get_int_value(*left, 0);
        std::int64_t rs = right->is_column ? 0 : get_int_value(*right, 0);
        bool left_ok = !left->is_column || lp != nullptr;
        bool right_ok = !right->is_column || rp != nullptr;
        if (left_ok && right_ok) {
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a + b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Sub:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a - b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Mul:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a * b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Div:
                    return make_int_result(
                        [](std::int64_t a, std::int64_t b) { return safe_idiv(a, b); }, lp, ls, rp,
                        rs);
                case ir::ArithmeticOp::Mod:
                    return make_int_result(
                        [](std::int64_t a, std::int64_t b) { return safe_imod(a, b); }, lp, ls, rp,
                        rs);
            }
        }
        Column<std::int64_t> out;
        out.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row)
            out.push_back(
                apply_int_op(bin->op, get_int_value(*left, row), get_int_value(*right, row)));
        return ColumnValue{std::move(out)};
    }
    return std::nullopt;
}

// NOLINTNEXTLINE(readability-function-size)
auto aggregate_table(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                     const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("group-by column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(column);
    }

    std::vector<const Column<Categorical>*> group_cats;
    group_cats.reserve(group_columns.size());
    for (const auto* col : group_columns) {
        if (const auto* cat = std::get_if<Column<Categorical>>(col)) {
            group_cats.push_back(cat);
        } else {
            group_cats.push_back(nullptr);
        }
    }

    std::vector<const ColumnValue*> agg_columns;
    agg_columns.reserve(aggregations.size());
    std::vector<const ColumnEntry*> agg_entries;
    agg_entries.reserve(aggregations.size());
    for (const auto& agg : aggregations) {
        if (agg.func == ir::AggFunc::Count) {
            agg_columns.push_back(nullptr);
            agg_entries.push_back(nullptr);
            continue;
        }
        const auto* entry = input.find_entry(agg.column.name);
        if (entry == nullptr) {
            return std::unexpected("aggregate column not found: " + agg.column.name +
                                   " (available: " + format_columns(input) + ")");
        }
        agg_entries.push_back(entry);
        agg_columns.push_back(entry->column.get());
    }

    std::vector<const ValidityBitmap*> agg_validity;
    agg_validity.reserve(aggregations.size());
    bool has_nullable_agg_inputs = false;
    for (const auto* entry : agg_entries) {
        if (entry != nullptr && entry->validity.has_value()) {
            agg_validity.push_back(&*entry->validity);
            has_nullable_agg_inputs = true;
        } else {
            agg_validity.push_back(nullptr);
        }
    }

    struct AggPlanItem {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
        double param = 0.0;  ///< Function-specific parameter (e.g. EWMA alpha).
        const Column<std::int64_t>* int_col = nullptr;
        const Column<double>* dbl_col = nullptr;
        const Column<std::string>* str_col = nullptr;
        const Column<Categorical>* cat_col = nullptr;
    };

    std::vector<AggPlanItem> plan;
    plan.reserve(aggregations.size());
    bool numeric_only = true;
    bool has_complex_agg = false;  // true when Median/Stddev/Ewma require the row-wise path
    for (std::size_t i = 0; i < aggregations.size(); ++i) {
        const auto& agg = aggregations[i];
        AggPlanItem item;
        item.func = agg.func;
        item.param = agg.param;
        if (agg.func == ir::AggFunc::Count) {
            item.kind = ExprType::Int;
        } else {
            item.kind = expr_type_for_column(*agg_columns[i]);
            if (const auto* int_col = std::get_if<Column<std::int64_t>>(agg_columns[i])) {
                item.int_col = int_col;
            } else if (const auto* dbl_col = std::get_if<Column<double>>(agg_columns[i])) {
                item.dbl_col = dbl_col;
            } else if (const auto* str_col = std::get_if<Column<std::string>>(agg_columns[i])) {
                item.str_col = str_col;
            } else if (const auto* cat_col = std::get_if<Column<Categorical>>(agg_columns[i])) {
                item.cat_col = cat_col;
            }
        }

        if (item.kind == ExprType::Date || item.kind == ExprType::Timestamp) {
            return std::unexpected("date/time aggregation not supported");
        }

        if (item.kind == ExprType::String &&
            (agg.func == ir::AggFunc::Sum || agg.func == ir::AggFunc::Mean ||
             agg.func == ir::AggFunc::Min || agg.func == ir::AggFunc::Max ||
             agg.func == ir::AggFunc::Median || agg.func == ir::AggFunc::Stddev ||
             agg.func == ir::AggFunc::Ewma || agg.func == ir::AggFunc::Quantile ||
             agg.func == ir::AggFunc::Skew || agg.func == ir::AggFunc::Kurtosis)) {
            return std::unexpected("string aggregation not supported");
        }

        if (agg.func == ir::AggFunc::Median || agg.func == ir::AggFunc::Stddev ||
            agg.func == ir::AggFunc::Ewma || agg.func == ir::AggFunc::Quantile ||
            agg.func == ir::AggFunc::Skew || agg.func == ir::AggFunc::Kurtosis) {
            has_complex_agg = true;
            numeric_only = false;
        } else if (agg.func == ir::AggFunc::First || agg.func == ir::AggFunc::Last) {
            // numeric First/Last are handled in the fast path; only fall back for strings
            if (item.kind == ExprType::String) {
                numeric_only = false;
            }
        } else if (agg.func != ir::AggFunc::Count && agg.func != ir::AggFunc::Sum &&
                   agg.func != ir::AggFunc::Mean && agg.func != ir::AggFunc::Min &&
                   agg.func != ir::AggFunc::Max) {
            numeric_only = false;
        }
        if (item.kind == ExprType::String) {
            numeric_only = false;
        }

        plan.push_back(item);
    }

    auto make_state = [&]() -> AggState {
        AggState state;
        state.slots.reserve(aggregations.size());
        for (std::size_t i = 0; i < plan.size(); ++i) {
            AggSlot slot;
            slot.func = plan[i].func;
            slot.kind = plan[i].kind;
            slot.param = plan[i].param;
            state.slots.push_back(slot);
        }
        return state;
    };

    auto update_state = [&](AggSlot* slots, std::size_t row) -> std::optional<std::string> {
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto& agg = aggregations[i];
            AggSlot& slot = slots[i];
            if (agg.func == ir::AggFunc::Count) {
                slot.count += 1;
                continue;
            }
            if ((agg.func == ir::AggFunc::Sum || agg.func == ir::AggFunc::Mean ||
                 agg.func == ir::AggFunc::Min || agg.func == ir::AggFunc::Max ||
                 agg.func == ir::AggFunc::Median || agg.func == ir::AggFunc::Stddev ||
                 agg.func == ir::AggFunc::Ewma || agg.func == ir::AggFunc::Quantile ||
                 agg.func == ir::AggFunc::Skew || agg.func == ir::AggFunc::Kurtosis) &&
                agg_validity[i] != nullptr && !(*agg_validity[i])[row]) {
                continue;
            }
            const ColumnValue& column = *agg_columns[i];
            if (agg.func == ir::AggFunc::First) {
                if (!slot.has_value) {
                    slot.first_value = scalar_from_column(column, row);
                }
                slot.has_value = true;
                continue;
            }
            if (agg.func == ir::AggFunc::Last) {
                slot.last_value = scalar_from_column(column, row);
                slot.has_value = true;
                continue;
            }
            if (agg.func == ir::AggFunc::Median || agg.func == ir::AggFunc::Quantile ||
                agg.func == ir::AggFunc::Skew || agg.func == ir::AggFunc::Kurtosis) {
                double x{};
                if (std::holds_alternative<Column<std::int64_t>>(column)) {
                    x = static_cast<double>(
                        std::get<std::int64_t>(scalar_from_column(column, row)));
                } else {
                    x = std::get<double>(scalar_from_column(column, row));
                }
                slot.values.push_back(x);
                continue;
            }
            if (agg.func == ir::AggFunc::Stddev) {
                double x{};
                if (std::holds_alternative<Column<std::int64_t>>(column)) {
                    x = static_cast<double>(
                        std::get<std::int64_t>(scalar_from_column(column, row)));
                } else {
                    x = std::get<double>(scalar_from_column(column, row));
                }
                slot.count += 1;
                double delta = x - slot.double_value;
                slot.double_value += delta / static_cast<double>(slot.count);
                double delta2 = x - slot.double_value;
                slot.m2 += delta * delta2;
                continue;
            }
            if (agg.func == ir::AggFunc::Ewma) {
                double x{};
                if (std::holds_alternative<Column<std::int64_t>>(column)) {
                    x = static_cast<double>(
                        std::get<std::int64_t>(scalar_from_column(column, row)));
                } else {
                    x = std::get<double>(scalar_from_column(column, row));
                }
                if (!slot.has_value) {
                    slot.double_value = x;
                    slot.has_value = true;
                } else {
                    slot.double_value = (slot.param * x) + ((1.0 - slot.param) * slot.double_value);
                }
                continue;
            }

            if (slot.kind == ExprType::Int) {
                std::int64_t value = 0;
                if (std::holds_alternative<Column<std::int64_t>>(column)) {
                    value = std::get<std::int64_t>(scalar_from_column(column, row));
                } else {
                    value = static_cast<std::int64_t>(
                        std::get<double>(scalar_from_column(column, row)));
                }
                switch (agg.func) {
                    case ir::AggFunc::Sum:
                        slot.int_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += static_cast<double>(value);
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                    case ir::AggFunc::Median:
                    case ir::AggFunc::Stddev:
                    case ir::AggFunc::Ewma:
                    case ir::AggFunc::Quantile:
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        break;
                }
                slot.has_value = true;
            } else {
                double value = 0.0;
                if (std::holds_alternative<Column<double>>(column)) {
                    value = std::get<double>(scalar_from_column(column, row));
                } else {
                    value = static_cast<double>(
                        std::get<std::int64_t>(scalar_from_column(column, row)));
                }
                switch (agg.func) {
                    case ir::AggFunc::Sum:
                        slot.double_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += value;
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                    case ir::AggFunc::Median:
                    case ir::AggFunc::Stddev:
                    case ir::AggFunc::Ewma:
                    case ir::AggFunc::Quantile:
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        break;
                }
                slot.has_value = true;
            }
        }
        return std::nullopt;
    };

    auto update_state_numeric = [&](AggSlot* slots, std::size_t row) -> std::optional<std::string> {
        for (std::size_t i = 0; i < plan.size(); ++i) {
            const auto& item = plan[i];
            AggSlot& slot = slots[i];
            if (item.func == ir::AggFunc::Count) {
                slot.count += 1;
                continue;
            }
            if (item.func == ir::AggFunc::First) {
                if (!slot.has_value) {
                    if (item.int_col != nullptr) {
                        slot.int_value = (*item.int_col)[row];
                    } else if (item.dbl_col != nullptr) {
                        slot.double_value = (*item.dbl_col)[row];
                    }
                    slot.has_value = true;
                }
                continue;
            }
            if (item.func == ir::AggFunc::Last) {
                if (item.int_col != nullptr) {
                    slot.int_value = (*item.int_col)[row];
                } else if (item.dbl_col != nullptr) {
                    slot.double_value = (*item.dbl_col)[row];
                }
                slot.has_value = true;
                continue;
            }

            if (slot.kind == ExprType::Int) {
                std::int64_t value = 0;
                if (item.int_col != nullptr) {
                    value = (*item.int_col)[row];
                } else if (item.dbl_col != nullptr) {
                    value = static_cast<std::int64_t>((*item.dbl_col)[row]);
                }

                switch (item.func) {
                    case ir::AggFunc::Sum:
                        slot.int_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += static_cast<double>(value);
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                    case ir::AggFunc::Median:
                    case ir::AggFunc::Stddev:
                    case ir::AggFunc::Ewma:
                    case ir::AggFunc::Quantile:
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        break;
                }
                slot.has_value = true;
            } else {
                double value = 0.0;
                if (item.dbl_col != nullptr) {
                    value = (*item.dbl_col)[row];
                } else if (item.int_col != nullptr) {
                    value = static_cast<double>((*item.int_col)[row]);
                }
                switch (item.func) {
                    case ir::AggFunc::Sum:
                        slot.double_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += value;
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                    case ir::AggFunc::Median:
                    case ir::AggFunc::Stddev:
                    case ir::AggFunc::Ewma:
                    case ir::AggFunc::Quantile:
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        break;
                }
                slot.has_value = true;
            }
        }
        return std::nullopt;
    };

    auto build_output = [&]() -> std::expected<Table, std::string> {
        Table output;
        for (std::size_t i = 0; i < group_by.size(); ++i) {
            const auto* column = input.find(group_by[i].name);
            if (column == nullptr) {
                return std::unexpected("group-by column not found: " + group_by[i].name);
            }
            if (group_cats[i] != nullptr) {
                output.add_column(group_by[i].name,
                                  Column<Categorical>(group_cats[i]->dictionary_ptr(),
                                                      group_cats[i]->index_ptr(), {}));
            } else {
                output.add_column(group_by[i].name, make_empty_like(*column));
            }
        }
        for (const auto& agg : aggregations) {
            ColumnValue column;
            switch (agg.func) {
                case ir::AggFunc::Count:
                    column = Column<std::int64_t>{};
                    break;
                case ir::AggFunc::Mean:
                case ir::AggFunc::Median:
                case ir::AggFunc::Stddev:
                case ir::AggFunc::Ewma:
                case ir::AggFunc::Quantile:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    column = Column<double>{};
                    break;
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max: {
                    const auto* input_col = input.find(agg.column.name);
                    if (input_col == nullptr) {
                        return std::unexpected("aggregate column not found: " + agg.column.name);
                    }
                    if (std::holds_alternative<Column<double>>(*input_col)) {
                        column = Column<double>{};
                    } else {
                        column = Column<std::int64_t>{};
                    }
                    break;
                }
                case ir::AggFunc::First:
                case ir::AggFunc::Last: {
                    const auto* input_col = input.find(agg.column.name);
                    if (input_col == nullptr) {
                        return std::unexpected("aggregate column not found: " + agg.column.name);
                    }
                    column = make_empty_like(*input_col);
                    break;
                }
            }
            output.add_column(agg.alias, std::move(column));
        }
        return output;
    };

    auto append_agg_values_flat = [&](Table& output,
                                      const AggSlot* slots) -> std::optional<std::string> {
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto& agg = aggregations[i];
            auto* column = output.find(agg.alias);
            if (column == nullptr) {
                return "missing aggregate column in output";
            }
            const AggSlot& slot = slots[i];
            switch (agg.func) {
                case ir::AggFunc::Count:
                    append_scalar(*column, slot.count);
                    break;
                case ir::AggFunc::Mean:
                    if (slot.count == 0) {
                        append_scalar(*column, 0.0);
                    } else {
                        append_scalar(*column, slot.sum / static_cast<double>(slot.count));
                    }
                    break;
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                    if (slot.kind == ExprType::Double) {
                        append_scalar(*column, slot.double_value);
                    } else {
                        append_scalar(*column, slot.int_value);
                    }
                    break;
                case ir::AggFunc::First:
                    if (slot.kind == ExprType::Int) {
                        append_scalar(*column, slot.int_value);
                    } else if (slot.kind == ExprType::Double) {
                        append_scalar(*column, slot.double_value);
                    } else {
                        append_scalar(*column, slot.first_value);
                    }
                    break;
                case ir::AggFunc::Last:
                    if (slot.kind == ExprType::Int) {
                        append_scalar(*column, slot.int_value);
                    } else if (slot.kind == ExprType::Double) {
                        append_scalar(*column, slot.double_value);
                    } else {
                        append_scalar(*column, slot.last_value);
                    }
                    break;
                case ir::AggFunc::Median: {
                    if (slot.values.empty()) {
                        append_scalar(*column, 0.0);
                    } else {
                        std::vector<double> sorted = slot.values;
                        std::ranges::sort(sorted);
                        std::size_t n = sorted.size();
                        double med = (n % 2 == 1) ? sorted[n / 2]
                                                  : (sorted[(n / 2) - 1] + sorted[n / 2]) / 2.0;
                        append_scalar(*column, med);
                    }
                    break;
                }
                case ir::AggFunc::Stddev:
                    if (slot.count < 2) {
                        append_scalar(*column, 0.0);
                    } else {
                        append_scalar(*column,
                                      std::sqrt(slot.m2 / static_cast<double>(slot.count - 1)));
                    }
                    break;
                case ir::AggFunc::Ewma:
                    append_scalar(*column, slot.double_value);
                    break;
                case ir::AggFunc::Quantile: {
                    if (slot.values.empty()) {
                        append_scalar(*column, 0.0);
                    } else {
                        std::vector<double> sorted = slot.values;
                        std::sort(sorted.begin(), sorted.end());
                        double idx = slot.param * static_cast<double>(sorted.size() - 1);
                        std::size_t lo = static_cast<std::size_t>(idx);
                        std::size_t hi = lo + 1 < sorted.size() ? lo + 1 : lo;
                        double frac = idx - static_cast<double>(lo);
                        append_scalar(*column, sorted[lo] + (frac * (sorted[hi] - sorted[lo])));
                    }
                    break;
                }
                case ir::AggFunc::Skew: {
                    std::size_t n = slot.values.size();
                    if (n < 3) {
                        append_scalar(*column, 0.0);
                    } else {
                        double mean = 0.0;
                        for (double x : slot.values)
                            mean += x;
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m3 = 0.0;
                        for (double x : slot.values) {
                            double d = x - mean;
                            m2 += d * d;
                            m3 += d * d * d;
                        }
                        if (m2 == 0.0) {
                            append_scalar(*column, 0.0);
                        } else {
                            double dn = static_cast<double>(n);
                            // Fisher–Pearson sample skewness (same as pandas default)
                            double skew =
                                (dn * std::sqrt(dn - 1.0) / (dn - 2.0)) * (m3 / std::pow(m2, 1.5));
                            append_scalar(*column, skew);
                        }
                    }
                    break;
                }
                case ir::AggFunc::Kurtosis: {
                    std::size_t n = slot.values.size();
                    if (n < 4) {
                        append_scalar(*column, 0.0);
                    } else {
                        double mean = 0.0;
                        for (double x : slot.values)
                            mean += x;
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m4 = 0.0;
                        for (double x : slot.values) {
                            double d = x - mean;
                            double d2 = d * d;
                            m2 += d2;
                            m4 += d2 * d2;
                        }
                        if (m2 == 0.0) {
                            append_scalar(*column, 0.0);
                        } else {
                            auto dn = static_cast<double>(n);
                            // Fisher excess kurtosis (unbiased, matches scipy/pandas default):
                            // G2 = (n-1)/((n-2)*(n-3)) * [(n+1)*n*m4/m2^2 - 3*(n-1)]
                            double kurt = (dn - 1.0) / ((dn - 2.0) * (dn - 3.0)) *
                                          ((dn + 1.0) * dn * m4 / (m2 * m2) - 3.0 * (dn - 1.0));
                            append_scalar(*column, kurt);
                        }
                    }
                    break;
                }
            }
        }
        return std::nullopt;
    };

    auto agg_result_is_valid = [&](std::size_t agg_index, const AggSlot& slot) -> bool {
        const auto func = aggregations[agg_index].func;
        switch (func) {
            case ir::AggFunc::Mean:
                return slot.count > 0;
            case ir::AggFunc::Sum:
            case ir::AggFunc::Min:
            case ir::AggFunc::Max:
                return slot.has_value;
            case ir::AggFunc::Median:
            case ir::AggFunc::Quantile:
                return !slot.values.empty();
            case ir::AggFunc::Stddev:
                return slot.count >= 2;
            case ir::AggFunc::Ewma:
                return slot.has_value;
            case ir::AggFunc::Skew:
                return slot.values.size() >= 3;
            case ir::AggFunc::Kurtosis:
                return slot.values.size() >= 4;
            case ir::AggFunc::Count:
            case ir::AggFunc::First:
            case ir::AggFunc::Last:
                return true;
        }
        return true;
    };

    std::size_t rows = input.rows();

    auto run_flat_pass2 = [&](const std::uint32_t* gids, std::uint32_t n_groups, AggSlot* fs,
                              std::size_t n_aggs_flat) -> void {
        for (std::size_t agg_i = 0; agg_i < plan.size(); ++agg_i) {
            const auto& item = plan[agg_i];
            const auto slot_for = [&](std::uint32_t g) -> AggSlot& {
                return fs[(static_cast<std::size_t>(g) * n_aggs_flat) + agg_i];
            };

            if (item.func == ir::AggFunc::Count) {
                std::vector<std::int64_t> acc(n_groups, 0);
                for (std::size_t row = 0; row < rows; ++row) {
                    acc[gids[row]]++;
                }
                for (std::uint32_t g = 0; g < n_groups; ++g) {
                    slot_for(g).count = acc[g];
                }
                continue;
            }

            if (item.func == ir::AggFunc::First) {
                std::vector<std::uint8_t> found(n_groups, 0U);
                if (item.int_col != nullptr) {
                    std::vector<std::int64_t> acc(n_groups, 0);
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (found[g] == 0U) {
                            acc[g] = (*item.int_col)[row];
                            found[g] = 1U;
                        }
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        auto& slot = slot_for(g);
                        slot.int_value = acc[g];
                        slot.has_value = true;
                    }
                } else if (item.dbl_col != nullptr) {
                    std::vector<double> acc(n_groups, 0.0);
                    const double* data = item.dbl_col->data();
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (found[g] == 0U) {
                            acc[g] = data[row];
                            found[g] = 1U;
                        }
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        auto& slot = slot_for(g);
                        slot.double_value = acc[g];
                        slot.has_value = true;
                    }
                } else if (item.str_col != nullptr) {
                    std::vector<std::string> acc(n_groups);
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (found[g] == 0U) {
                            acc[g] = (*item.str_col)[row];
                            found[g] = 1U;
                        }
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        auto& slot = slot_for(g);
                        slot.first_value = std::move(acc[g]);
                        slot.has_value = true;
                    }
                } else if (item.cat_col != nullptr) {
                    std::vector<std::string> acc(n_groups);
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (found[g] == 0U) {
                            acc[g] = std::string((*item.cat_col)[row]);
                            found[g] = 1U;
                        }
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        auto& slot = slot_for(g);
                        slot.first_value = std::move(acc[g]);
                        slot.has_value = true;
                    }
                }
                continue;
            }

            if (item.func == ir::AggFunc::Last) {
                if (item.int_col != nullptr) {
                    std::vector<std::int64_t> acc(n_groups, 0);
                    for (std::size_t row = 0; row < rows; ++row) {
                        acc[gids[row]] = (*item.int_col)[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        auto& slot = slot_for(g);
                        slot.int_value = acc[g];
                        slot.has_value = true;
                    }
                } else if (item.dbl_col != nullptr) {
                    std::vector<double> acc(n_groups, 0.0);
                    const double* data = item.dbl_col->data();
                    for (std::size_t row = 0; row < rows; ++row) {
                        acc[gids[row]] = data[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        auto& slot = slot_for(g);
                        slot.double_value = acc[g];
                        slot.has_value = true;
                    }
                } else if (item.str_col != nullptr) {
                    std::vector<std::string> acc(n_groups);
                    for (std::size_t row = 0; row < rows; ++row) {
                        acc[gids[row]] = (*item.str_col)[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        slot_for(g).last_value = std::move(acc[g]);
                        slot_for(g).has_value = true;
                    }
                } else if (item.cat_col != nullptr) {
                    std::vector<std::string> acc(n_groups);
                    for (std::size_t row = 0; row < rows; ++row) {
                        acc[gids[row]] = std::string((*item.cat_col)[row]);
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        slot_for(g).last_value = std::move(acc[g]);
                        slot_for(g).has_value = true;
                    }
                }
                continue;
            }

            if (item.dbl_col != nullptr) {
                const double* data = item.dbl_col->data();
                switch (item.func) {
                    case ir::AggFunc::Sum: {
                        std::vector<double> acc(n_groups, 0.0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] += data[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).double_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                        break;
                    }
                    case ir::AggFunc::Mean: {
                        std::vector<double> acc(n_groups, 0.0);
                        std::vector<std::int64_t> counts(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            acc[g] += data[row];
                            counts[g]++;
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).sum = acc[g];
                            slot_for(g).count = counts[g];
                        }
                        break;
                    }
                    case ir::AggFunc::Min: {
                        std::vector<double> acc(n_groups, std::numeric_limits<double>::infinity());
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            acc[g] = std::min(data[row], acc[g]);
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).double_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                        break;
                    }
                    case ir::AggFunc::Max: {
                        std::vector<double> acc(n_groups, -std::numeric_limits<double>::infinity());
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            acc[g] = std::max(data[row], acc[g]);
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).double_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                        break;
                    }
                    default:
                        break;
                }
            } else if (item.int_col != nullptr) {
                const std::int64_t* data = item.int_col->data();
                switch (item.func) {
                    case ir::AggFunc::Sum: {
                        std::vector<std::int64_t> acc(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] += data[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).int_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                        break;
                    }
                    case ir::AggFunc::Mean: {
                        std::vector<double> acc(n_groups, 0.0);
                        std::vector<std::int64_t> counts(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            acc[g] += static_cast<double>(data[row]);
                            counts[g]++;
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).sum = acc[g];
                            slot_for(g).count = counts[g];
                        }
                        break;
                    }
                    case ir::AggFunc::Min: {
                        std::vector<std::int64_t> acc(n_groups,
                                                      std::numeric_limits<std::int64_t>::max());
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            acc[g] = std::min(data[row], acc[g]);
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).int_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                        break;
                    }
                    case ir::AggFunc::Max: {
                        std::vector<std::int64_t> acc(n_groups,
                                                      std::numeric_limits<std::int64_t>::min());
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            acc[g] = std::max(data[row], acc[g]);
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).int_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    };

    // Null-aware / complex-agg fallback path:
    // use row-wise state updates so SUM/MEAN/MIN/MAX can ignore null rows and
    // emit null when every input row is null for a group.
    // Also required for Median/Stddev/Ewma which need per-row sequential processing.
    if (has_nullable_agg_inputs || has_complex_agg) {
        robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> index;
        index.reserve(rows == 0 ? 1 : rows);
        std::vector<Key> group_order;
        group_order.reserve(rows == 0 ? 1 : rows);
        std::vector<AggState> states;
        states.reserve(rows == 0 ? 1 : rows);

        for (std::size_t row = 0; row < rows; ++row) {
            Key key;
            key.values.reserve(group_columns.size());
            for (const auto* col : group_columns) {
                key.values.push_back(scalar_from_column(*col, row));
            }

            auto [it, inserted] = index.emplace(std::move(key), states.size());
            if (inserted) {
                group_order.push_back(it->first);
                states.push_back(make_state());
            }

            if (auto err = update_state(states[it->second].slots.data(), row)) {
                return std::unexpected(*err);
            }
        }

        auto output = build_output();
        if (!output.has_value()) {
            return std::unexpected(output.error());
        }

        std::vector<ValidityBitmap> agg_validity_out(aggregations.size());
        std::vector<std::uint8_t> track_agg_validity(aggregations.size(), 0U);
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto func = aggregations[i].func;
            if (func == ir::AggFunc::Sum || func == ir::AggFunc::Mean || func == ir::AggFunc::Min ||
                func == ir::AggFunc::Max || func == ir::AggFunc::Median ||
                func == ir::AggFunc::Stddev || func == ir::AggFunc::Ewma ||
                func == ir::AggFunc::Quantile || func == ir::AggFunc::Skew ||
                func == ir::AggFunc::Kurtosis) {
                track_agg_validity[i] = 1U;
                agg_validity_out[i].reserve(group_order.size());
            }
        }

        for (std::size_t g = 0; g < group_order.size(); ++g) {
            const Key& key = group_order[g];
            for (std::size_t ci = 0; ci < key.values.size(); ++ci) {
                auto* column = output->find(group_by[ci].name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                append_scalar(*column, key.values[ci]);
            }

            const AggSlot* slots = states[g].slots.data();
            for (std::size_t i = 0; i < aggregations.size(); ++i) {
                if (track_agg_validity[i] != 0U) {
                    agg_validity_out[i].push_back(agg_result_is_valid(i, slots[i]));
                }
            }

            if (auto err = append_agg_values_flat(*output, slots)) {
                return std::unexpected(*err);
            }
        }

        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            if (track_agg_validity[i] == 0U || agg_validity_out[i].empty()) {
                continue;
            }
            bool has_null = false;
            for (std::size_t row = 0; row < agg_validity_out[i].size(); ++row) {
                if (!agg_validity_out[i][row]) {
                    has_null = true;
                    break;
                }
            }
            if (!has_null) {
                continue;
            }
            auto out_it = output->index.find(aggregations[i].alias);
            if (out_it == output->index.end()) {
                return std::unexpected("missing aggregate column in output");
            }
            output->columns[out_it->second].validity = std::move(agg_validity_out[i]);
        }

        return output;
    }

    if (group_by.size() == 1) {
        const ColumnValue& key_column = *group_columns.front();
        if (group_cats.front() != nullptr) {
            const auto& col = *group_cats.front();
            const auto* codes = col.codes_data();

            // Pass 1: Assign group IDs. One hash lookup per row, with a sorted-run shortcut
            // that skips the lookup whenever the current key equals the previous one.
            robin_hood::unordered_flat_map<Column<Categorical>::code_type, std::uint32_t> key_to_id;
            key_to_id.reserve(64);
            std::vector<Column<Categorical>::code_type> order;
            order.reserve(64);
            // Flat AggSlot array: one contiguous allocation replaces n_groups heap allocations.
            // Layout: flat_slots[g * n_aggs + agg_i] is the slot for group g, aggregation agg_i.
            const std::size_t n_aggs = plan.size();
            AggState tmpl = make_state();
            std::vector<AggSlot> flat_slots;
            flat_slots.reserve(64 * (n_aggs == 0 ? 1 : n_aggs));
            std::vector<std::uint32_t> group_ids(rows);
            {
                Column<Categorical>::code_type prev_key = -1;
                std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
                for (std::size_t row = 0; row < rows; ++row) {
                    auto key = codes[row];
                    std::uint32_t gid{};
                    if (key == prev_key) {
                        gid = prev_gid;
                    } else {
                        auto it = key_to_id.find(key);
                        if (it == key_to_id.end()) {
                            gid = static_cast<std::uint32_t>(order.size());
                            key_to_id.emplace(key, gid);
                            order.push_back(key);
                            flat_slots.insert(flat_slots.end(), tmpl.slots.begin(),
                                              tmpl.slots.end());
                        } else {
                            gid = it->second;
                        }
                        prev_key = key;
                        prev_gid = gid;
                    }
                    group_ids[row] = gid;
                }
            }

            auto n_groups = static_cast<std::uint32_t>(order.size());
            const std::uint32_t* gids = group_ids.data();
            AggSlot* fs = flat_slots.data();
            run_flat_pass2(gids, n_groups, fs, n_aggs);

            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            if (auto* out_col = output->find(group_by.front().name);
                out_col != nullptr && std::holds_alternative<Column<Categorical>>(*out_col)) {
                auto& out_cat = std::get<Column<Categorical>>(*out_col);
                out_cat.reserve(order.size());
                for (auto code : order) {
                    out_cat.push_code(code);
                }
            } else {
                for (std::size_t i = 0; i < order.size(); ++i) {
                    auto* column = output->find(group_by.front().name);
                    if (column == nullptr) {
                        return std::unexpected("missing group-by column in output");
                    }
                    append_scalar(
                        *column, std::string(col.dictionary()[static_cast<std::size_t>(order[i])]));
                }
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                if (auto err = append_agg_values_flat(*output, &fs[i * n_aggs])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<Categorical>>(key_column)) {
            const auto& col = std::get<Column<Categorical>>(key_column);
            robin_hood::unordered_flat_map<Column<Categorical>::code_type, std::size_t> index;
            index.reserve(rows);
            std::vector<Column<Categorical>::code_type> order;
            order.reserve(rows);
            const std::size_t n_aggs_cat = plan.size();
            AggState tmpl_cat = make_state();
            std::vector<AggSlot> flat_slots_cat;
            flat_slots_cat.reserve(rows * (n_aggs_cat == 0 ? 1 : n_aggs_cat));
            for (std::size_t row = 0; row < rows; ++row) {
                auto key = col.code_at(row);
                auto it = index.find(key);
                std::size_t slot_index = 0;
                if (it == index.end()) {
                    slot_index = order.size();
                    index.emplace(key, slot_index);
                    order.push_back(key);
                    flat_slots_cat.insert(flat_slots_cat.end(), tmpl_cat.slots.begin(),
                                          tmpl_cat.slots.end());
                } else {
                    slot_index = it->second;
                }
                if (auto err =
                        (numeric_only
                             ? update_state_numeric(&flat_slots_cat[slot_index * n_aggs_cat], row)
                             : update_state(&flat_slots_cat[slot_index * n_aggs_cat], row))) {
                    return std::unexpected(*err);
                }
            }
            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                const auto& dict = col.dictionary();
                append_scalar(*column, dict[static_cast<std::size_t>(order[i])]);
                if (auto err = append_agg_values_flat(*output, &flat_slots_cat[i * n_aggs_cat])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<std::int64_t>>(key_column)) {
            const auto& col = std::get<Column<std::int64_t>>(key_column);
            robin_hood::unordered_flat_map<std::int64_t, std::size_t> index;
            index.reserve(rows);
            std::vector<std::int64_t> order;
            order.reserve(rows);
            const std::size_t n_aggs_i64 = plan.size();
            AggState tmpl_i64 = make_state();
            std::vector<AggSlot> flat_slots_i64;
            flat_slots_i64.reserve(rows * (n_aggs_i64 == 0 ? 1 : n_aggs_i64));
            for (std::size_t row = 0; row < rows; ++row) {
                std::int64_t key = col[row];
                auto it = index.find(key);
                std::size_t slot_index = 0;
                if (it == index.end()) {
                    slot_index = order.size();
                    index.emplace(key, slot_index);
                    order.push_back(key);
                    flat_slots_i64.insert(flat_slots_i64.end(), tmpl_i64.slots.begin(),
                                          tmpl_i64.slots.end());
                } else {
                    slot_index = it->second;
                }
                if (auto err =
                        (numeric_only
                             ? update_state_numeric(&flat_slots_i64[slot_index * n_aggs_i64], row)
                             : update_state(&flat_slots_i64[slot_index * n_aggs_i64], row))) {
                    return std::unexpected(*err);
                }
            }
            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                append_scalar(*column, order[i]);
                if (auto err = append_agg_values_flat(*output, &flat_slots_i64[i * n_aggs_i64])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<double>>(key_column)) {
            const auto& col = std::get<Column<double>>(key_column);
            robin_hood::unordered_flat_map<double, std::size_t> index;
            index.reserve(rows);
            std::vector<double> order;
            order.reserve(rows);
            const std::size_t n_aggs_dbl = plan.size();
            AggState tmpl_dbl = make_state();
            std::vector<AggSlot> flat_slots_dbl;
            flat_slots_dbl.reserve(rows * (n_aggs_dbl == 0 ? 1 : n_aggs_dbl));
            for (std::size_t row = 0; row < rows; ++row) {
                double key = col[row];
                auto it = index.find(key);
                std::size_t slot_index = 0;
                if (it == index.end()) {
                    slot_index = order.size();
                    index.emplace(key, slot_index);
                    order.push_back(key);
                    flat_slots_dbl.insert(flat_slots_dbl.end(), tmpl_dbl.slots.begin(),
                                          tmpl_dbl.slots.end());
                } else {
                    slot_index = it->second;
                }
                if (auto err =
                        (numeric_only
                             ? update_state_numeric(&flat_slots_dbl[slot_index * n_aggs_dbl], row)
                             : update_state(&flat_slots_dbl[slot_index * n_aggs_dbl], row))) {
                    return std::unexpected(*err);
                }
            }
            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                append_scalar(*column, order[i]);
                if (auto err = append_agg_values_flat(*output, &flat_slots_dbl[i * n_aggs_dbl])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<std::string>>(key_column)) {
            const auto& col = std::get<Column<std::string>>(key_column);
            const char* src_chars = col.chars_data();
            const std::uint32_t* src_off = col.offsets_data();

            // Pass 1: code assignment + flat output dictionary (no heap-allocated std::string
            // per distinct key; string_views in the map point into the original column buffer).
            //
            // Reserve based on rows (capped at 64K). High-cardinality string group-bys
            // like `sum by user_id` (~100K distinct in 2M rows) previously started at 1024
            // and paid ~7 rehashes of growing cost; starting larger cuts that to ~1.
            robin_hood::unordered_flat_map<std::string_view, std::uint32_t> key_to_gid;
            const std::size_t reserve_hint =
                std::min<std::size_t>(rows, static_cast<std::size_t>(65536));
            key_to_gid.reserve(reserve_hint);
            std::vector<std::uint32_t> dict_offsets;
            dict_offsets.reserve(reserve_hint + 1);
            dict_offsets.push_back(0);
            std::vector<char> dict_chars;
            dict_chars.reserve(reserve_hint * 16);
            // Flat AggSlot array: one contiguous allocation replaces n_groups heap allocations.
            // Layout: flat_slots[g * n_aggs + agg_i] is the slot for group g, aggregation agg_i.
            const std::size_t n_aggs = plan.size();
            AggState tmpl = make_state();
            std::vector<AggSlot> flat_slots;
            flat_slots.reserve(reserve_hint * (n_aggs == 0 ? 1 : n_aggs));
            std::uint32_t n_groups = 0;
            std::vector<std::uint32_t> group_ids(rows);
            {
                // Single-probe emplace: `find`+`emplace` re-hashes the key on
                // insertion. At 100K distinct keys that's ~100K extra probes.
                // robin_hood::emplace returns (iterator, inserted) so we can
                // do the full find-or-insert in one call.
                std::string_view prev_key;
                std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
                for (std::size_t row = 0; row < rows; ++row) {
                    std::string_view key{src_chars + src_off[row], src_off[row + 1] - src_off[row]};
                    std::uint32_t gid{};
                    if (key == prev_key) {
                        gid = prev_gid;
                    } else {
                        auto result = key_to_gid.emplace(key, n_groups);
                        if (result.second) {
                            gid = n_groups++;
                            dict_chars.insert(dict_chars.end(), key.begin(), key.end());
                            dict_offsets.push_back(static_cast<std::uint32_t>(dict_chars.size()));
                            flat_slots.insert(flat_slots.end(), tmpl.slots.begin(),
                                              tmpl.slots.end());
                        } else {
                            gid = result.first->second;
                        }
                        prev_key = key;
                        prev_gid = gid;
                    }
                    group_ids[row] = gid;
                }
            }

            const std::uint32_t* gids = group_ids.data();
            AggSlot* fs = flat_slots.data();
            run_flat_pass2(gids, n_groups, fs, n_aggs);

            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::uint32_t g = 0; g < n_groups; ++g) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                std::string_view key_sv{dict_chars.data() + dict_offsets[g],
                                        dict_offsets[g + 1] - dict_offsets[g]};
                if (auto* str_col = std::get_if<Column<std::string>>(column)) {
                    str_col->push_back(key_sv);
                } else {
                    append_scalar(*column, std::string(key_sv));
                }
                if (auto err = append_agg_values_flat(
                        *output, fs + (static_cast<std::size_t>(g) * n_aggs))) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
    }

    // Multi-column GROUP BY: 2-pass integer-coding (mirrors the single-key string path).
    //
    // Pass 1a: per-column code assignment — one hash-map lookup per row per column,
    //          each on a small per-column map. No heap allocation per row.
    // Pass 1b: compound group ID assignment. Cartesian encoding (code0*n1 + code1 + …)
    //          produces a unique uint64_t per combination; flat array lookup when the
    //          total Cartesian cells ≤ 4M, otherwise a robin_hood<uint64_t> map.
    // Pass 2:  Per-aggregation scatter-accumulate into flat AggState[] indexed by gid
    //          — same pattern as the single-key string path.
    const std::size_t n_keys = group_columns.size();

    // ── Pass 1a: per-column uint32_t code arrays ─────────────────────────────
    // Categorical key columns are handled zero-copy: cat_raw points directly into
    // the column's existing codes array; cc.codes is left empty. code_at() picks
    // the right source so pass 1b and output reconstruction need no special-casing.
    struct ColCodes {
        std::vector<std::uint32_t> codes;  // codes[row]; empty for categorical
        std::uint32_t n_distinct{0};
        std::vector<ScalarValue> vals;  // distinct values; empty for string columns
        const Column<Categorical>::code_type* cat_raw{nullptr};  // non-null → categorical
        // Flat dict for string key columns (avoids heap-allocating n_distinct std::strings).
        std::vector<std::uint32_t> str_offsets;
        std::vector<char> str_chars;
        bool is_str{false};

        [[nodiscard]] std::uint32_t code_at(std::size_t row) const noexcept {
            return cat_raw ? static_cast<std::uint32_t>(cat_raw[row]) : codes[row];
        }

        [[nodiscard]] std::string_view str_val_at(std::uint32_t code) const noexcept {
            return {str_chars.data() + str_offsets[code],
                    str_offsets[code + 1] - str_offsets[code]};
        }
    };
    std::vector<ColCodes> per_col(n_keys);

    for (std::size_t ci = 0; ci < n_keys; ++ci) {
        ColCodes& cc = per_col[ci];
        std::visit(
            [&](const auto& col) {
                using ColType = std::decay_t<decltype(col)>;
                using T = typename ColType::value_type;
                if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                    const auto& dict = col.dictionary();
                    cc.n_distinct = static_cast<std::uint32_t>(dict.size());
                    cc.vals.reserve(dict.size());
                    for (const auto& entry : dict) {
                        cc.vals.emplace_back(entry);
                    }
                    // Zero-copy: borrow the column's own codes array.
                    cc.cat_raw = col.codes_data();
                } else if constexpr (is_string_like_v<T>) {
                    cc.codes.resize(rows);
                    cc.is_str = true;
                    cc.str_offsets.reserve(65);
                    cc.str_offsets.push_back(0);
                    cc.str_chars.reserve(512);
                    robin_hood::unordered_flat_map<std::string_view, std::uint32_t> map;
                    map.reserve(64);
                    std::string_view prev_key;
                    std::uint32_t prev_code = std::numeric_limits<std::uint32_t>::max();
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::string_view key{col[row]};
                        std::uint32_t code{};
                        if (key == prev_key) {
                            code = prev_code;
                        } else {
                            auto it = map.find(key);
                            if (it == map.end()) {
                                code = cc.n_distinct++;
                                map.emplace(key, code);
                                // Flat dict: no heap-allocated std::string per distinct key.
                                cc.str_chars.insert(cc.str_chars.end(), key.begin(), key.end());
                                cc.str_offsets.push_back(
                                    static_cast<std::uint32_t>(cc.str_chars.size()));
                            } else {
                                code = it->second;
                            }
                            prev_key = key;
                            prev_code = code;
                        }
                        cc.codes[row] = code;
                    }
                } else {
                    cc.codes.resize(rows);
                    robin_hood::unordered_flat_map<T, std::uint32_t> map;
                    map.reserve(64);
                    for (std::size_t row = 0; row < rows; ++row) {
                        T key = col[row];
                        auto it = map.find(key);
                        std::uint32_t code{};
                        if (it == map.end()) {
                            code = cc.n_distinct++;
                            map.emplace(key, code);
                            cc.vals.emplace_back(key);
                        } else {
                            code = it->second;
                        }
                        cc.codes[row] = code;
                    }
                }
            },
            *group_columns[ci]);
    }

    // ── Pass 1b: compound group ID assignment ────────────────────────────────
    // Cartesian strides: cell = code[0]*strides[0] + code[1]*strides[1] + …
    std::vector<std::uint64_t> strides(n_keys);
    std::uint64_t total_cells = 1;
    {
        std::uint64_t s = 1;
        for (int ci = static_cast<int>(n_keys) - 1; ci >= 0; --ci) {
            strides[static_cast<std::size_t>(ci)] = s;
            s *= per_col[static_cast<std::size_t>(ci)]
                     .n_distinct;  // uint64_t; wraps only at 2^64 distinct groups
        }
        total_cells = s;
    }

    std::vector<std::uint32_t> compound_gids(rows);
    // Flat AggSlot array: one contiguous allocation replaces n_groups heap allocations.
    // Layout: flat_slots[g * n_aggs + agg_i] is the slot for group g, aggregation agg_i.
    const std::size_t n_aggs = plan.size();
    AggState tmpl = make_state();
    std::vector<AggSlot> flat_slots;
    // group_col_codes_flat[g*n_keys + ci] = per-column code for group g
    std::vector<std::uint32_t> group_col_codes_flat;
    std::uint32_t n_groups_m = 0;

    if (total_cells <= 4'000'000ULL) {
        // Fast path: plain array lookup — no hashing at all in the hot loop.
        std::vector<std::uint32_t> cell_to_gid(static_cast<std::size_t>(total_cells),
                                               std::numeric_limits<std::uint32_t>::max());
        flat_slots.reserve(256 * (n_aggs == 0 ? 1 : n_aggs));
        group_col_codes_flat.reserve(256 * n_keys);

        // Hoist per-key data out of the row loop so the compiler can see plain
        // pointer/scalar arithmetic instead of struct access through per_col[ci].
        // For all-categorical multi-key (the common case after CSV/Categorical
        // inference) this collapses the inner key loop into a couple of array
        // loads and a multiply.
        struct KeyAccess {
            const Column<Categorical>::code_type* cat_raw;
            const std::uint32_t* nonsparse_codes;
            std::uint32_t stride;
        };
        std::vector<KeyAccess> kacc(n_keys);
        bool all_cat = true;
        for (std::size_t ci = 0; ci < n_keys; ++ci) {
            kacc[ci].cat_raw = per_col[ci].cat_raw;
            kacc[ci].nonsparse_codes =
                per_col[ci].cat_raw == nullptr ? per_col[ci].codes.data() : nullptr;
            kacc[ci].stride = static_cast<std::uint32_t>(strides[ci]);
            if (per_col[ci].cat_raw == nullptr)
                all_cat = false;
        }

        auto code_at_fast = [&](std::size_t ci, std::size_t row) -> std::uint32_t {
            return kacc[ci].cat_raw != nullptr ? static_cast<std::uint32_t>(kacc[ci].cat_raw[row])
                                               : kacc[ci].nonsparse_codes[row];
        };

        if (all_cat && n_keys == 2) {
            // Two-key all-categorical specialization: by far the dominant
            // shape (e.g. `by {symbol, day}` over CSV-inferred columns).
            const auto* k0 = kacc[0].cat_raw;
            const auto* k1 = kacc[1].cat_raw;
            const std::uint32_t s0 = kacc[0].stride;
            const std::uint32_t s1 = kacc[1].stride;
            std::uint32_t* const cell_to_gid_data = cell_to_gid.data();
            for (std::size_t row = 0; row < rows; ++row) {
                const std::uint32_t cell = (static_cast<std::uint32_t>(k0[row]) * s0) +
                                           (static_cast<std::uint32_t>(k1[row]) * s1);
                std::uint32_t gid = cell_to_gid_data[cell];
                if (gid == std::numeric_limits<std::uint32_t>::max()) {
                    gid = n_groups_m++;
                    cell_to_gid_data[cell] = gid;
                    flat_slots.insert(flat_slots.end(), tmpl.slots.begin(), tmpl.slots.end());
                    group_col_codes_flat.push_back(static_cast<std::uint32_t>(k0[row]));
                    group_col_codes_flat.push_back(static_cast<std::uint32_t>(k1[row]));
                }
                compound_gids[row] = gid;
            }
        } else {
            for (std::size_t row = 0; row < rows; ++row) {
                std::uint32_t cell = 0;
                for (std::size_t ci = 0; ci < n_keys; ++ci)
                    cell += code_at_fast(ci, row) * kacc[ci].stride;
                std::uint32_t gid = cell_to_gid[cell];
                if (gid == std::numeric_limits<std::uint32_t>::max()) {
                    gid = n_groups_m++;
                    cell_to_gid[cell] = gid;
                    flat_slots.insert(flat_slots.end(), tmpl.slots.begin(), tmpl.slots.end());
                    for (std::size_t ci = 0; ci < n_keys; ++ci)
                        group_col_codes_flat.push_back(code_at_fast(ci, row));
                }
                compound_gids[row] = gid;
            }
        }
    } else {
        // Fallback: hash map on the uint64_t Cartesian cell key.
        robin_hood::unordered_flat_map<std::uint64_t, std::uint32_t> cell_to_gid;
        cell_to_gid.reserve(1024);
        flat_slots.reserve(1024 * (n_aggs == 0 ? 1 : n_aggs));
        group_col_codes_flat.reserve(1024 * n_keys);
        for (std::size_t row = 0; row < rows; ++row) {
            std::uint64_t cell = 0;
            for (std::size_t ci = 0; ci < n_keys; ++ci)
                cell += static_cast<std::uint64_t>(per_col[ci].code_at(row)) * strides[ci];
            auto [it, inserted] = cell_to_gid.emplace(cell, n_groups_m);
            if (inserted) {
                ++n_groups_m;
                flat_slots.insert(flat_slots.end(), tmpl.slots.begin(), tmpl.slots.end());
                for (std::size_t ci = 0; ci < n_keys; ++ci)
                    group_col_codes_flat.push_back(per_col[ci].code_at(row));
            }
            compound_gids[row] = it->second;
        }
    }

    const std::uint32_t* gids = compound_gids.data();
    AggSlot* fs = flat_slots.data();
    run_flat_pass2(gids, n_groups_m, fs, n_aggs);

    // ── Output reconstruction ─────────────────────────────────────────────────
    auto output = build_output();
    if (!output.has_value()) {
        return std::unexpected(output.error());
    }
    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
        const std::uint32_t* gc =
            group_col_codes_flat.data() + (static_cast<std::size_t>(g) * n_keys);
        for (std::size_t ci = 0; ci < n_keys; ++ci) {
            auto* column = output->find(group_by[ci].name);
            if (column == nullptr) {
                return std::unexpected("missing group-by column in output");
            }
            if (group_cats[ci] != nullptr && std::holds_alternative<Column<Categorical>>(*column)) {
                auto& out_cat = std::get<Column<Categorical>>(*column);
                out_cat.push_code(static_cast<Column<Categorical>::code_type>(gc[ci]));
            } else if (per_col[ci].is_str) {
                auto sv = per_col[ci].str_val_at(gc[ci]);
                if (auto* str_col = std::get_if<Column<std::string>>(column)) {
                    str_col->push_back(sv);
                } else {
                    append_scalar(*column, std::string(sv));
                }
            } else {
                append_scalar(*column, per_col[ci].vals[gc[ci]]);
            }
        }
        if (auto err = append_agg_values_flat(*output, &fs[g * n_aggs])) {
            return std::unexpected(*err);
        }
    }

    return output;
}

auto parse_aggregate_func(std::string_view name) -> std::optional<ir::AggFunc> {
    if (name == "sum")
        return ir::AggFunc::Sum;
    if (name == "mean")
        return ir::AggFunc::Mean;
    if (name == "min")
        return ir::AggFunc::Min;
    if (name == "max")
        return ir::AggFunc::Max;
    if (name == "count")
        return ir::AggFunc::Count;
    if (name == "first")
        return ir::AggFunc::First;
    if (name == "last")
        return ir::AggFunc::Last;
    if (name == "median")
        return ir::AggFunc::Median;
    if (name == "std")
        return ir::AggFunc::Stddev;
    if (name == "ewma")
        return ir::AggFunc::Ewma;
    if (name == "quantile")
        return ir::AggFunc::Quantile;
    if (name == "skew")
        return ir::AggFunc::Skew;
    if (name == "kurtosis")
        return ir::AggFunc::Kurtosis;
    return std::nullopt;
}

auto aggregate_call_to_spec(const ir::CallExpr& call, std::string alias)
    -> std::expected<std::optional<ir::AggSpec>, std::string> {
    auto func = parse_aggregate_func(call.callee);
    if (!func.has_value()) {
        return std::optional<ir::AggSpec>{};
    }
    if (call.callee == "count") {
        if (!call.args.empty()) {
            return std::unexpected("count() takes no arguments");
        }
        return std::optional<ir::AggSpec>{
            ir::AggSpec{.func = *func, .column = ir::ColumnRef{.name = ""}, .alias = alias}};
    }
    if (call.args.empty()) {
        return std::unexpected(call.callee + "(): expected column argument");
    }
    if (call.callee == "ewma" || call.callee == "quantile") {
        if (call.args.size() != 2) {
            return std::unexpected(call.callee + "(): expected two arguments");
        }
    } else if (call.args.size() != 1) {
        return std::unexpected("aggregate functions take one argument");
    }

    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (col_ref == nullptr) {
        return std::unexpected(call.callee +
                               "(): grouped update aggregate argument must be a column name");
    }

    double param = 0.0;
    if (call.callee == "ewma" || call.callee == "quantile") {
        const auto* lit = std::get_if<ir::Literal>(&call.args[1]->node);
        if (lit == nullptr) {
            return std::unexpected(call.callee + "(): second argument must be a numeric literal");
        }
        if (const auto* dv = std::get_if<double>(&lit->value)) {
            param = *dv;
        } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
            param = static_cast<double>(*iv);
        } else {
            return std::unexpected(call.callee + "(): second argument must be a numeric literal");
        }
    }

    return std::optional<ir::AggSpec>{ir::AggSpec{
        .func = *func,
        .column = ir::ColumnRef{.name = col_ref->name},
        .alias = std::move(alias),
        .param = param,
    }};
}

struct BroadcastAggregateColumn {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};

/// True iff `expr` mentions at least one built-in aggregate function call.
/// Used by `update + by` to decide whether to broadcast the field as a
/// per-group scalar (compound aggregate expression) or fall through to the
/// per-row value-expression evaluator.
auto expr_contains_aggregate_call(const ir::Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (parse_aggregate_func(node.callee).has_value()) {
                    return true;
                }
                return std::ranges::any_of(
                    node.args, [](const auto& arg) { return expr_contains_aggregate_call(*arg); });
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                 std::is_same_v<T, ir::CompareExpr>) {
                return expr_contains_aggregate_call(*node.left) ||
                       expr_contains_aggregate_call(*node.right);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                if (expr_contains_aggregate_call(*node.left)) {
                    return true;
                }
                return node.right && expr_contains_aggregate_call(*node.right);
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return expr_contains_aggregate_call(*node.operand);
            } else {
                return false;
            }
        },
        expr.node);
}

/// Evaluate a compound aggregate expression to a single scalar. Each
/// aggregate sub-call is computed against the (per-group) `input`; other
/// nodes compose via the existing column-arithmetic helpers on 1-row
/// columns so int/double promotion matches the column path. Bare
/// ColumnRefs resolve only against `scalars` — a column reference outside
/// an aggregate would not collapse to a per-group scalar.
auto eval_aggregate_scalar(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::expected<ScalarValue, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<ScalarValue, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::Literal>) {
                return std::visit([](const auto& v) -> ScalarValue { return v; }, node.value);
            } else if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                if (scalars != nullptr) {
                    auto it = scalars->find(node.name);
                    if (it != scalars->end()) {
                        return it->second;
                    }
                }
                return std::unexpected("update + by: non-aggregate column '" + node.name +
                                       "' in aggregate-broadcast field expression");
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr>) {
                auto lhs = eval_aggregate_scalar(*node.left, input, scalars);
                if (!lhs) {
                    return std::unexpected(lhs.error());
                }
                auto rhs = eval_aggregate_scalar(*node.right, input, scalars);
                if (!rhs) {
                    return std::unexpected(rhs.error());
                }
                ColumnValue lhs_col = broadcast_scalar_column(*lhs, 1);
                ColumnValue rhs_col = broadcast_scalar_column(*rhs, 1);
                auto result = arith_vec(node.op, lhs_col, rhs_col, 1);
                if (!result) {
                    return std::unexpected(result.error());
                }
                return scalar_from_column(*result, 0);
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                auto func = parse_aggregate_func(node.callee);
                if (!func.has_value()) {
                    return std::unexpected("update + by: non-aggregate function '" + node.callee +
                                           "' in aggregate-broadcast field expression");
                }
                // count() takes no arguments.
                if (node.callee == "count") {
                    ir::AggSpec spec{.func = *func,
                                     .column = ir::ColumnRef{.name = ""},
                                     .alias = "__agg_broadcast"};
                    auto agg =
                        aggregate_table(input, {}, std::vector<ir::AggSpec>{std::move(spec)});
                    if (!agg) {
                        return std::unexpected(agg.error());
                    }
                    const auto* entry = agg->find_entry("__agg_broadcast");
                    return scalar_from_column(*entry->column, 0);
                }
                if (node.args.empty()) {
                    return std::unexpected(node.callee + "(): expected column argument");
                }
                // ewma(col, alpha) / quantile(col, p) carry a literal numeric param.
                double param = 0.0;
                const bool has_param = node.callee == "ewma" || node.callee == "quantile";
                if (has_param) {
                    if (node.args.size() != 2) {
                        return std::unexpected(node.callee + "(): expected two arguments");
                    }
                    const auto* lit = std::get_if<ir::Literal>(&node.args[1]->node);
                    if (lit == nullptr) {
                        return std::unexpected(node.callee +
                                               "(): second argument must be a numeric literal");
                    }
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        param = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        param = static_cast<double>(*iv);
                    } else {
                        return std::unexpected(node.callee +
                                               "(): second argument must be a numeric literal");
                    }
                } else if (node.args.size() != 1) {
                    return std::unexpected("aggregate functions take one argument");
                }
                // If the aggregate's first arg is a bare column we aggregate it
                // directly; otherwise materialise the computed arg as a temp
                // column appended to a shallow copy of the input (columns are
                // shared_ptr-backed, so the copy is cheap).
                const auto* col_ref = std::get_if<ir::ColumnRef>(&node.args[0]->node);
                Table working;
                const Table* effective_input = &input;
                std::string agg_col_name;
                if (col_ref != nullptr) {
                    agg_col_name = col_ref->name;
                } else {
                    auto col_result = eval_value_vec(*node.args[0], input, scalars, input.rows());
                    if (!col_result) {
                        return std::unexpected(col_result.error());
                    }
                    working = input;
                    agg_col_name = "__agg_broadcast_arg";
                    while (working.find(agg_col_name) != nullptr) {
                        agg_col_name += "_";
                    }
                    ColumnValue materialised = std::visit(
                        [](auto& d) -> ColumnValue {
                            using D = std::decay_t<decltype(d)>;
                            if constexpr (std::is_same_v<D, const ColumnValue*>) {
                                return *d;
                            } else {
                                return std::move(d);
                            }
                        },
                        col_result->data);
                    working.add_column(agg_col_name, std::move(materialised));
                    effective_input = &working;
                }
                ir::AggSpec spec{
                    .func = *func,
                    .column = ir::ColumnRef{.name = agg_col_name},
                    .alias = "__agg_broadcast",
                    .param = param,
                };
                auto agg = aggregate_table(*effective_input, {},
                                           std::vector<ir::AggSpec>{std::move(spec)});
                if (!agg) {
                    return std::unexpected(agg.error());
                }
                const auto* entry = agg->find_entry("__agg_broadcast");
                if (entry == nullptr || entry->column == nullptr ||
                    column_size(*entry->column) != 1) {
                    return std::unexpected(
                        "update + by: internal error computing aggregate broadcast");
                }
                return scalar_from_column(*entry->column, 0);
            } else {
                return std::unexpected(
                    "update + by: unsupported expression shape in aggregate-broadcast field");
            }
        },
        expr.node);
}

auto broadcast_aggregate_column(const Table& input, const ir::FieldSpec& field,
                                const ScalarRegistry* scalars)
    -> std::expected<std::optional<BroadcastAggregateColumn>, std::string> {
    // Fast path: bare aggregate call (e.g. `mean(p)`). Preserves null
    // propagation from the aggregate-table machinery.
    if (const auto* call = std::get_if<ir::CallExpr>(&field.expr.node)) {
        auto spec = aggregate_call_to_spec(*call, field.alias);
        if (!spec) {
            return std::unexpected(spec.error());
        }
        if (spec->has_value()) {
            auto aggregated =
                aggregate_table(input, {}, std::vector<ir::AggSpec>{std::move(**spec)});
            if (!aggregated) {
                return std::unexpected(aggregated.error());
            }
            const auto* entry = aggregated->find_entry(field.alias);
            if (entry == nullptr || entry->column == nullptr || column_size(*entry->column) != 1) {
                return std::unexpected("grouped update aggregate produced invalid result: " +
                                       field.alias);
            }
            auto scalar = scalar_from_column(*entry->column, 0);
            BroadcastAggregateColumn result{
                .column = broadcast_scalar_column(scalar, input.rows()),
                .validity = std::nullopt,
            };
            if (entry->validity.has_value() && !(*entry->validity)[0]) {
                result.validity = ValidityBitmap(input.rows(), false);
            }
            return std::optional<BroadcastAggregateColumn>{std::move(result)};
        }
    }
    // Compound path: expression contains aggregate calls but isn't itself a
    // bare aggregate call (e.g. `sum(p*w) / sum(w)`, or any aggregate-UDF
    // body that has inlined to such a shape). Evaluate to a single scalar
    // then broadcast over the group's rows.
    if (!expr_contains_aggregate_call(field.expr)) {
        return std::optional<BroadcastAggregateColumn>{};
    }
    auto scalar = eval_aggregate_scalar(field.expr, input, scalars);
    if (!scalar) {
        return std::unexpected(scalar.error());
    }
    BroadcastAggregateColumn result{
        .column = broadcast_scalar_column(*scalar, input.rows()),
        .validity = std::nullopt,
    };
    return std::optional<BroadcastAggregateColumn>{std::move(result)};
}

using ir::is_cum_func;
using ir::is_rng_func;
using ir::is_rolling_func;
constexpr auto rng_func_returns_int(std::string_view name) -> bool;
constexpr auto is_fill_func(std::string_view name) -> bool;
constexpr auto is_float_clean_func(std::string_view name) -> bool;

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

// Extract a calendar/clock field from a Date or Timestamp value.
auto eval_date_part(std::string_view name, const ExprValue& v)
    -> std::expected<ExprValue, std::string> {
    using namespace std::chrono;
    const bool date_ok = (name == "year" || name == "month" || name == "day");
    if (const auto* dv = std::get_if<Date>(&v)) {
        if (!date_ok) {
            return std::unexpected(std::string(name) + ": argument must be Timestamp");
        }
        year_month_day ymd{sys_days{days{dv->days}}};
        if (name == "year") {
            return ExprValue{static_cast<std::int64_t>(static_cast<int>(ymd.year()))};
        }
        if (name == "month") {
            return ExprValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.month()))};
        }
        return ExprValue{static_cast<std::int64_t>(static_cast<unsigned>(ymd.day()))};
    }
    if (const auto* tv = std::get_if<Timestamp>(&v)) {
        sys_time<nanoseconds> tp{nanoseconds{tv->nanos}};
        auto day_pt = floor<days>(tp);
        year_month_day ymd{day_pt};
        hh_mm_ss<nanoseconds> hms{tp - day_pt};
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

struct ScalarBuiltin {
    int min_args = 1;
    int max_args = 1;  // -1 == variadic
    std::expected<ExprType, std::string> (*infer)(std::string_view, const std::vector<ExprType>&);
    std::expected<ExprValue, std::string> (*eval)(std::string_view, const std::vector<ExprValue>&);
};

const std::unordered_map<std::string_view, ScalarBuiltin>& scalar_builtins() {
    using IT = std::expected<ExprType, std::string>;
    using IV = std::expected<ExprValue, std::string>;
    static const std::unordered_map<std::string_view, ScalarBuiltin> table = [] {
        std::unordered_map<std::string_view, ScalarBuiltin> m;

        // abs: numeric -> same numeric type.
        m.emplace("abs", ScalarBuiltin{1, 1,
                                       [](std::string_view, const std::vector<ExprType>& a) -> IT {
                                           if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                                               return a[0];
                                           }
                                           return std::unexpected("abs: argument must be numeric");
                                       },
                                       [](std::string_view, const std::vector<ExprValue>& a) -> IV {
                                           if (const auto* i = std::get_if<std::int64_t>(&a[0])) {
                                               return ExprValue{std::int64_t{std::abs(*i)}};
                                           }
                                           if (const auto* d = std::get_if<double>(&a[0])) {
                                               return ExprValue{std::abs(*d)};
                                           }
                                           return std::unexpected("abs: argument must be numeric");
                                       }});

        // sqrt / log / exp: numeric -> Float64.
        const ScalarBuiltin transcendental{
            1, 1,
            [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return ExprType::Double;
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            },
            [](std::string_view name, const std::vector<ExprValue>& a) -> IV {
                auto x = expr_value_to_double(a[0]);
                if (!x) {
                    return std::unexpected(std::string(name) + ": argument must be numeric");
                }
                if (name == "sqrt") {
                    return ExprValue{std::sqrt(*x)};
                }
                if (name == "log") {
                    return ExprValue{std::log(*x)};
                }
                return ExprValue{std::exp(*x)};
            }};
        m.emplace("sqrt", transcendental);
        m.emplace("log", transcendental);
        m.emplace("exp", transcendental);

        // ceil / floor / trunc: round to an integral value, preserving the
        // numeric type (Int is already integral, so it passes through). Use
        // round(x, ceil|floor|trunc) for a Float -> Int64 conversion.
        const ScalarBuiltin integral{
            1, 1,
            [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return a[0];
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            },
            [](std::string_view name, const std::vector<ExprValue>& a) -> IV {
                if (std::holds_alternative<std::int64_t>(a[0])) {
                    return a[0];
                }
                if (const auto* d = std::get_if<double>(&a[0])) {
                    if (name == "ceil") {
                        return ExprValue{std::ceil(*d)};
                    }
                    if (name == "floor") {
                        return ExprValue{std::floor(*d)};
                    }
                    return ExprValue{std::trunc(*d)};
                }
                return std::unexpected(std::string(name) + ": argument must be numeric");
            }};
        m.emplace("ceil", integral);
        m.emplace("floor", integral);
        m.emplace("trunc", integral);

        // Float64 / Float32: cast Int or Float to Float64.
        const ScalarBuiltin to_float{
            1, 1,
            [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return ExprType::Double;
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Float");
            },
            [](std::string_view, const std::vector<ExprValue>& a) -> IV {
                if (auto d = expr_value_to_double(a[0])) {
                    return ExprValue{*d};
                }
                return std::unexpected("cast: cannot cast non-numeric to Float");
            }};
        m.emplace("Float64", to_float);
        m.emplace("Float32", to_float);

        // Int64 / Int32 / Int: cast Int or whole-valued Float to Int64.
        const ScalarBuiltin to_int{
            1, 1,
            [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                if (a[0] == ExprType::Int || a[0] == ExprType::Double) {
                    return ExprType::Int;
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Int");
            },
            [](std::string_view name, const std::vector<ExprValue>& a) -> IV {
                if (const auto* i = std::get_if<std::int64_t>(&a[0])) {
                    return ExprValue{*i};
                }
                if (const auto* d = std::get_if<double>(&a[0])) {
                    if (*d != std::trunc(*d)) {
                        return std::unexpected(std::string(name) +
                                               "(): cannot cast non-integer Float to Int (use "
                                               "floor(), ceil(), or round())");
                    }
                    return ExprValue{static_cast<std::int64_t>(*d)};
                }
                return std::unexpected(std::string(name) + "(): cannot cast non-numeric to Int");
            }};
        m.emplace("Int64", to_int);
        m.emplace("Int32", to_int);
        m.emplace("Int", to_int);

        // year / month / day / hour / minute / second: Date|Timestamp -> Int.
        const ScalarBuiltin date_part{
            1, 1,
            [](std::string_view name, const std::vector<ExprType>& a) -> IT {
                const bool date_ok = (name == "year" || name == "month" || name == "day");
                if (a[0] == ExprType::Timestamp || (date_ok && a[0] == ExprType::Date)) {
                    return ExprType::Int;
                }
                return std::unexpected(std::string(name) +
                                       (date_ok ? ": argument must be Date or Timestamp"
                                                : ": argument must be Timestamp"));
            },
            [](std::string_view name, const std::vector<ExprValue>& a) -> IV {
                return eval_date_part(name, a[0]);
            }};
        m.emplace("year", date_part);
        m.emplace("month", date_part);
        m.emplace("day", date_part);
        m.emplace("hour", date_part);
        m.emplace("minute", date_part);
        m.emplace("second", date_part);

        // is_nan: Float64 -> Bool.
        m.emplace("is_nan",
                  ScalarBuiltin{1, 1,
                                [](std::string_view, const std::vector<ExprType>& a) -> IT {
                                    if (a[0] == ExprType::Double) {
                                        return ExprType::Bool;
                                    }
                                    return std::unexpected("is_nan: argument must be Float64");
                                },
                                [](std::string_view, const std::vector<ExprValue>& a) -> IV {
                                    if (const auto* d = std::get_if<double>(&a[0])) {
                                        return ExprValue{std::isnan(*d)};
                                    }
                                    return std::unexpected("is_nan: argument must be Float64");
                                }});

        // pmin / pmax: 2+ comparable args of one type (Int/Float widen to Float).
        const ScalarBuiltin pminmax{
            2, -1,
            [](std::string_view name, const std::vector<ExprType>& a) -> IT {
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
            [](std::string_view name, const std::vector<ExprValue>& a) -> IV {
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
            }};
        m.emplace("pmin", pminmax);
        m.emplace("pmax", pminmax);

        return m;
    }();
    return table;
}

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
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        // Pure row-wise scalar builtins: single source of truth (see
        // scalar_builtins()). Both this pass and eval_expr dispatch here.
        if (auto it = scalar_builtins().find(call->callee); it != scalar_builtins().end()) {
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
        // Null-fill functions (fill_null / fill_forward / fill_backward)
        if (is_fill_func(call->callee)) {
            if (call->args.empty()) {
                return std::unexpected(std::string(call->callee) +
                                       ": expected at least 1 argument");
            }
            const auto* col_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
            if (!col_ref) {
                return std::unexpected(std::string(call->callee) +
                                       ": first argument must be a column name");
            }
            const auto* source = input.find(col_ref->name);
            if (!source) {
                return std::unexpected(std::string(call->callee) + ": unknown column '" +
                                       col_ref->name + "'");
            }
            return expr_type_for_column(*source);
        }
        if (is_float_clean_func(call->callee)) {
            if (call->args.size() != 1) {
                return std::unexpected(std::string(call->callee) + ": expected 1 argument");
            }
            const auto* col_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
            if (!col_ref) {
                return std::unexpected(std::string(call->callee) +
                                       ": argument must be a column name");
            }
            const auto* source = input.find(col_ref->name);
            if (!source) {
                return std::unexpected(std::string(call->callee) + ": unknown column '" +
                                       col_ref->name + "'");
            }
            auto kind = expr_type_for_column(*source);
            if (kind != ExprType::Double) {
                return std::unexpected(std::string(call->callee) + ": column must be Float64");
            }
            return ExprType::Double;
        }
        // Cumulative functions (cumsum / cumprod)
        if (is_cum_func(call->callee)) {
            if (call->args.size() != 1) {
                return std::unexpected(std::string(call->callee) + ": expected 1 argument");
            }
            const auto* col_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
            if (!col_ref) {
                return std::unexpected(std::string(call->callee) +
                                       ": argument must be a column name");
            }
            const auto* source = input.find(col_ref->name);
            if (!source) {
                return std::unexpected(std::string(call->callee) + ": unknown column '" +
                                       col_ref->name + "'");
            }
            return expr_type_for_column(*source);
        }
        // Built-in temporal shift functions
        if (call->callee == "lag" || call->callee == "lead") {
            if (call->args.size() != 2) {
                return std::unexpected(call->callee + ": expected 2 arguments");
            }
            const auto* col_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
            if (!col_ref) {
                return std::unexpected(call->callee + ": first argument must be a column name");
            }
            const auto* source = input.find(col_ref->name);
            if (!source) {
                return std::unexpected(call->callee + ": unknown column '" + col_ref->name + "'");
            }
            return expr_type_for_column(*source);
        }
        // Built-in rolling aggregate functions
        if (call->callee == "rolling_mean" || call->callee == "rolling_median" ||
            call->callee == "rolling_std" || call->callee == "rolling_ewma" ||
            call->callee == "rolling_quantile" || call->callee == "rolling_skew" ||
            call->callee == "rolling_kurtosis") {
            return ExprType::Double;
        }
        if (call->callee == "rolling_count") {
            return ExprType::Int;
        }
        if (call->callee == "rolling_sum" || call->callee == "rolling_min" ||
            call->callee == "rolling_max") {
            if (call->args.size() != 1) {
                return std::unexpected(call->callee + ": expected 1 argument");
            }
            const auto* col_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
            if (!col_ref) {
                return std::unexpected(call->callee + ": argument must be a column name");
            }
            const auto* source = input.find(col_ref->name);
            if (!source) {
                return std::unexpected(call->callee + ": unknown column '" + col_ref->name + "'");
            }
            return expr_type_for_column(*source);
        }
        // rep() — returns the same type as its first positional argument
        if (call->callee == "rep") {
            if (call->args.empty()) {
                return std::unexpected("rep: expected one positional argument (x)");
            }
            const auto& x = *call->args[0];
            if (const auto* lit = std::get_if<ir::Literal>(&x.node)) {
                if (std::holds_alternative<bool>(lit->value))
                    return ExprType::Bool;
                if (std::holds_alternative<std::int64_t>(lit->value))
                    return ExprType::Int;
                if (std::holds_alternative<double>(lit->value))
                    return ExprType::Double;
                if (std::holds_alternative<Date>(lit->value))
                    return ExprType::Date;
                if (std::holds_alternative<Timestamp>(lit->value))
                    return ExprType::Timestamp;
                return ExprType::String;
            }
            return infer_expr_type(x, input, scalars, externs);
        }
        // Vectorized RNG functions
        if (is_rng_func(call->callee)) {
            return rng_func_returns_int(call->callee) ? ExprType::Int : ExprType::Double;
        }
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
        const auto* source = input.find(col->name);
        if (source == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    return it->second;
                }
            }
            return std::unexpected("unknown column in expression: " + col->name +
                                   " (available: " + format_columns(input) + ")");
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
            *source);
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
        bool want_double = bin->op == ir::ArithmeticOp::Div ||
                           std::holds_alternative<double>(left.value()) ||
                           std::holds_alternative<double>(right.value());
        if (want_double) {
            double lhs = to_double(left.value());
            double rhs = to_double(right.value());
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
        // scalar_builtins()). Both this pass and infer_expr_type dispatch here.
        if (auto it = scalar_builtins().find(call->callee); it != scalar_builtins().end()) {
            const auto& fn = it->second;
            const auto argc = static_cast<int>(call->args.size());
            if (argc < fn.min_args || (fn.max_args >= 0 && argc > fn.max_args)) {
                return std::unexpected(std::string(call->callee) + ": wrong number of arguments");
            }
            std::vector<ExprValue> arg_values;
            arg_values.reserve(call->args.size());
            for (const auto& arg : call->args) {
                auto v = eval_expr(*arg, input, row, scalars, externs);
                if (!v) {
                    return v;
                }
                arg_values.push_back(std::move(*v));
            }
            return fn.eval(call->callee, arg_values);
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
        std::vector<ExprValue> arg_values;
        arg_values.reserve(call->args.size());
        for (const auto& arg : call->args) {
            auto value = eval_expr(*arg, input, row, scalars, externs);
            if (!value) {
                return value;
            }
            arg_values.push_back(std::move(value.value()));
        }
        auto result = fn->func(arg_values);
        if (!result) {
            return std::unexpected(result.error());
        }
        if (auto* scalar = std::get_if<ScalarValue>(&result.value())) {
            return *scalar;
        }
        return std::unexpected("function returned table in expression: " + call->callee);
    }
    return std::unexpected("unsupported expression");
}

auto evaluate_row_count_expr_impl(const ir::Expr& expr, const ScalarRegistry* scalars,
                                  const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string> {
    Table empty;
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

// True if `expr` contains an RNG generator call (rand_normal/rand_uniform/...)
// anywhere in its tree.
auto expr_contains_rng(const ir::Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (ir::is_rng_func(node.callee)) {
                    return true;
                }
                return std::ranges::any_of(node.args,
                                           [](const auto& a) { return expr_contains_rng(*a); });
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                 std::is_same_v<T, ir::CompareExpr>) {
                return expr_contains_rng(*node.left) || expr_contains_rng(*node.right);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                return expr_contains_rng(*node.left) ||
                       (node.right != nullptr && expr_contains_rng(*node.right));
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return expr_contains_rng(*node.operand);
            } else {
                return false;
            }
        },
        expr.node);
}

// Evaluate a single field expression against a (potentially growing) table,
// returning the resulting column. Handles fast-path binary ops and row-by-row eval.
auto evaluate_field_column(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars,
                           const ExternRegistry* externs)
    -> std::expected<ColumnValue, std::string> {
    std::size_t rows = input.rows();
    auto inferred = infer_expr_type(expr, input, scalars, externs);
    if (!inferred) {
        return std::unexpected(inferred.error());
    }
    // RNG nested inside arithmetic cannot be built per row (eval_expr is pure):
    // evaluate such expressions vectorized via eval_value_vec, which materializes
    // each RNG sub-expression as a column (the same draw as a bare RNG field) and
    // applies column arithmetic. Scoped to arithmetic-topped expressions so a
    // plain RNG call still takes the bare-field fast path and a registry call
    // wrapping RNG does not bounce back here.
    if (std::holds_alternative<ir::BinaryExpr>(expr.node) && expr_contains_rng(expr)) {
        auto res = eval_value_vec(expr, input, scalars, rows);
        if (!res) {
            return std::unexpected(res.error());
        }
        if (auto* owned = std::get_if<ColumnValue>(&res->data)) {
            return std::move(*owned);
        }
        return *std::get<const ColumnValue*>(res->data);
    }
    if (auto fast = try_fast_update_binary(expr, input, rows, inferred.value(), scalars);
        fast.has_value()) {
        return std::move(fast.value());
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
    for (std::size_t row = 0; row < rows; ++row) {
        auto value = eval_expr(expr, input, row, scalars, externs);
        if (!value) {
            return std::unexpected(value.error());
        }
        std::visit(
            [&](auto& col) {
                using ColType = std::decay_t<decltype(col)>;
                using ValueType = typename ColType::value_type;
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
                } else if constexpr (std::is_same_v<ValueType, std::string>) {
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
    return new_column;
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
    Table empty;
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
                        using T = typename ColT::value_type;
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
                using T = typename ColT::value_type;
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
            using T = typename ColT::value_type;
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

struct FillResult {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;  // nullopt = all rows valid
};

enum class FloatCleanMode : std::uint8_t {
    NullIfNan,
    NullIfNotFinite,
};

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
            using T = typename ColT::value_type;

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
            T fill_val = *maybe_fill;

            ColT result;
            result.reserve(rows);
            for (std::size_t i = 0; i < rows; ++i) {
                bool is_null_row = has_validity && !(*entry->validity)[i];
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
        return FillResult{*entry->column, std::nullopt};
    }

    std::size_t rows = input.rows();
    const auto& validity = *entry->validity;

    return std::visit(
        [&](const auto& col) -> std::expected<FillResult, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = typename ColT::value_type;
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
        return FillResult{*entry->column, std::nullopt};
    }

    std::size_t rows = input.rows();
    const auto& validity = *entry->validity;

    return std::visit(
        [&](const auto& col) -> std::expected<FillResult, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            using T = typename ColT::value_type;

            // Scan right-to-left to compute (value, valid) for each row,
            // storing in plain vectors so we can then push_back into ColT.
            std::vector<T> vals(rows);
            std::optional<ValidityBitmap> out_validity;

            bool have_val = false;
            T next_val{};
            for (std::size_t ri = 0; ri < rows; ++ri) {
                std::size_t i = rows - 1 - ri;
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
    return FillResult{ColumnValue{std::move(result)}, std::move(validity)};
}

auto eval_is_nan(const ir::CallExpr& call, const Table& input)
    -> std::expected<ColumnValue, std::string> {
    if (call.args.size() != 1) {
        return std::unexpected("is_nan: expected 1 argument (col)");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (!col_ref) {
        return std::unexpected("is_nan: argument must be a column name");
    }
    const auto* entry = input.find_entry(col_ref->name);
    if (!entry) {
        return std::unexpected("is_nan: unknown column '" + col_ref->name + "'");
    }
    const auto* src = std::get_if<Column<double>>(entry->column.get());
    if (src == nullptr) {
        return std::unexpected("is_nan: column must be Float64");
    }

    Column<bool> result;
    result.resize(src->size());
    const bool has_validity = entry->validity.has_value();
    for (std::size_t i = 0; i < src->size(); ++i) {
        const bool is_valid = !has_validity || (*entry->validity)[i];
        result.set(i, is_valid && std::isnan((*src)[i]));
    }
    return ColumnValue{std::move(result)};
}

constexpr auto is_fill_func(std::string_view name) -> bool {
    return name == "fill_null" || name == "fill_forward" || name == "fill_backward";
}

constexpr auto is_float_clean_func(std::string_view name) -> bool {
    return name == "null_if_nan" || name == "null_if_not_finite";
}

// Find the first row index lo in [0, row] where time[lo] >= time[row] - duration.
// The time index column must be Timestamp or Date and sorted ascending.
auto window_lo(const ColumnValue& time_col, std::size_t row, ir::Duration duration) -> std::size_t {
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&time_col)) {
        std::int64_t threshold = (*ts_col)[row].nanos - duration.count();
        std::size_t lo = 0;
        std::size_t hi = row;
        while (lo < hi) {
            std::size_t mid = lo + ((hi - lo) / 2);
            if ((*ts_col)[mid].nanos < threshold) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
    // Date column: convert duration (nanoseconds) to days
    const auto& date_col = std::get<Column<Date>>(time_col);
    static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
    auto duration_days = static_cast<std::int32_t>(duration.count() / kNsPerDay);
    std::int32_t threshold = date_col[row].days - duration_days;
    std::size_t lo = 0;
    std::size_t hi = row;
    while (lo < hi) {
        std::size_t mid = lo + ((hi - lo) / 2);
        if (date_col[mid].days < threshold) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

// Compute a rolling aggregate column over a time-indexed window.
// The table must be a TimeFrame (time_index set, sorted ascending).
auto apply_rolling_func(const ir::CallExpr& call, const Table& table, ir::Duration duration)
    -> std::expected<ColumnValue, std::string> {
    const auto& time_col = *table.find(*table.time_index);
    std::size_t rows = table.rows();

    // Map the time column to a contiguous int64 array and express duration in the same unit.
    // Timestamp: nanoseconds (layout-compatible with int64, no copy needed).
    // Date: days (int32, must be widened into a temporary buffer).
    const std::int64_t* time_vals = nullptr;
    std::vector<std::int64_t> time_vals_buf;  // only allocated for the Date path
    std::int64_t dur_val = 0;
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&time_col)) {
        // Timestamp is {int64_t nanos} — pointer-cast avoids an 8 MB copy.
        static_assert(sizeof(Timestamp) == sizeof(std::int64_t) &&
                      alignof(Timestamp) == alignof(std::int64_t));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        time_vals = reinterpret_cast<const std::int64_t*>(ts_col->data());
        dur_val = duration.count();
    } else {
        const auto& date_col = std::get<Column<Date>>(time_col);
        time_vals_buf.resize(rows);
        for (std::size_t i = 0; i < rows; ++i)
            time_vals_buf[i] = date_col[i].days;
        time_vals = time_vals_buf.data();
        static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
        dur_val = duration.count() / kNsPerDay;
    }

    // Two-pointer helper: advances lo to the first row still inside the window.
    // Because the TimeFrame is sorted ascending, lo never decreases across rows.
    auto advance_lo = [&](std::size_t& lo, std::size_t i) {
        std::int64_t threshold = time_vals[i] - dur_val;
        while (lo < i && time_vals[lo] < threshold)
            ++lo;
    };

    if (call.callee == "rolling_count") {
        Column<std::int64_t> result;
        result.resize(rows);
        std::size_t lo = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            advance_lo(lo, i);
            result[i] = static_cast<std::int64_t>(i - lo + 1);
        }
        return result;
    }

    if (call.args.empty()) {
        return std::unexpected(call.callee + ": expected column argument");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (!col_ref) {
        return std::unexpected(call.callee + ": argument must be a column name");
    }
    const auto* src = table.find(col_ref->name);
    if (!src) {
        return std::unexpected(call.callee + ": unknown column '" + col_ref->name + "'");
    }

    if (call.callee == "rolling_mean") {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_mean: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize(rows);
                    double sum = 0.0;
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        sum += static_cast<double>(col[i]);
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            sum -= static_cast<double>(col[lo]);
                            ++lo;
                        }
                        result[i] = sum / static_cast<double>(i - lo + 1);
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_sum") {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                using T = typename ColT::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_sum: column must be numeric (Int or Float)");
                } else {
                    ColT result;
                    result.resize(rows);
                    T sum{};
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        sum += col[i];
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            sum -= col[lo];
                            ++lo;
                        }
                        result[i] = sum;
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_median") {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_median: column must be numeric (Int or Float)");
                } else {
                    // Sliding-window median via two multisets — O(n log w).
                    //
                    // lo holds the lower half, hi the upper half.
                    // Invariants:
                    //   (1) lo.size() == hi.size()     (even total)
                    //    OR lo.size() == hi.size() + 1 (odd total)
                    //   (2) max(lo) <= min(hi)
                    //
                    // Median = max(lo) when sizes differ, else avg of both tops.
                    std::multiset<double> lo;  // lower half  — max is rbegin()
                    std::multiset<double> hi;  // upper half  — min is begin()

                    // Restore invariant (1) after a single insert or erase.
                    auto rebalance = [&]() {
                        if (lo.size() > hi.size() + 1) {
                            hi.insert(*lo.rbegin());
                            lo.erase(std::prev(lo.end()));
                        } else if (hi.size() > lo.size()) {
                            lo.insert(*hi.begin());
                            hi.erase(hi.begin());
                        }
                    };

                    auto insert_val = [&](double x) {
                        // Preserves invariant (2): x goes to lo if it belongs
                        // in the lower half, hi otherwise.
                        if (lo.empty() || x <= *lo.rbegin())
                            lo.insert(x);
                        else
                            hi.insert(x);
                        rebalance();
                    };

                    auto remove_val = [&](double x) {
                        // Remove one copy from whichever half contains it.
                        auto it = lo.find(x);
                        if (it != lo.end())
                            lo.erase(it);
                        else
                            hi.erase(hi.find(x));
                        rebalance();
                    };

                    Column<double> result;
                    result.resize(rows);
                    std::size_t lo_ptr = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        insert_val(static_cast<double>(col[i]));
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo_ptr < i && time_vals[lo_ptr] < threshold) {
                            remove_val(static_cast<double>(col[lo_ptr]));
                            ++lo_ptr;
                        }
                        result[i] = (lo.size() > hi.size()) ? static_cast<double>(*lo.rbegin())
                                                            : (*lo.rbegin() + *hi.begin()) / 2.0;
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_std") {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_std: column must be numeric (Int or Float)");
                } else {
                    // O(n) sliding window. The TimeFrame is sorted ascending, so
                    // `lo` is monotonic: each row is added once on the right and
                    // dropped once on the left. We maintain running (mean, m2)
                    // with Welford add and its exact inverse for removal, which
                    // matches the from-scratch Welford result the old O(n*w) loop
                    // produced — but in a single pass instead of one per row.
                    Column<double> result;
                    result.resize(rows);
                    double mean = 0.0;
                    double m2 = 0.0;
                    std::size_t cnt = 0;
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        // Add col[i] on the right (standard Welford).
                        double x = static_cast<double>(col[i]);
                        ++cnt;
                        double delta = x - mean;
                        mean += delta / static_cast<double>(cnt);
                        m2 += delta * (x - mean);
                        // Drop rows that have aged out on the left (inverse Welford).
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            double y = static_cast<double>(col[lo]);
                            double mean_old = mean;
                            --cnt;
                            mean = ((static_cast<double>(cnt) + 1.0) * mean_old - y) /
                                   static_cast<double>(cnt);
                            m2 -= (y - mean_old) * (y - mean);
                            ++lo;
                        }
                        std::size_t n = i - lo + 1;  // == cnt
                        // Clamp away tiny negative m2 from floating-point drift.
                        result[i] =
                            n < 2 ? 0.0 : std::sqrt(std::max(0.0, m2) / static_cast<double>(n - 1));
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_ewma") {
        // Parse alpha from the second argument (a numeric literal).
        double alpha = 0.0;
        if (call.args.size() < 2) {
            return std::unexpected(
                "rolling_ewma: expected two arguments: rolling_ewma(col, alpha)");
        }
        if (const auto* lit = std::get_if<ir::Literal>(&call.args[1]->node)) {
            if (const auto* dv = std::get_if<double>(&lit->value)) {
                alpha = *dv;
            } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                alpha = static_cast<double>(*iv);
            } else {
                return std::unexpected("rolling_ewma: alpha must be a numeric literal");
            }
        } else {
            return std::unexpected("rolling_ewma: alpha must be a numeric literal");
        }
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_ewma: column must be numeric (Int or Float)");
                } else {
                    // O(n) sliding window. The TimeFrame is sorted ascending, so
                    // `lo` is monotonic: each row enters once on the right and is
                    // dropped once on the left. The windowed EWMA restarts at each
                    // window's first element (the seed), which expands to
                    //   result[i] = alpha*R_i + (1-alpha)*beta^(i-lo)*col[lo]
                    // with R_i = sum_{j=lo..i} beta^(i-j)*col[j], maintained as
                    //   add right:  R = beta*R + col[i]
                    //   drop left:  R -= beta^(i-lo)*col[lo]
                    // reproducing the from-scratch O(n*w) recurrence in one pass.
                    // beta_pow caches beta^k (k bounded by the window width).
                    const double beta = 1.0 - alpha;
                    Column<double> result;
                    result.resize(rows);
                    std::vector<double> beta_pow{1.0};  // beta_pow[k] == beta^k
                    beta_pow.reserve(64);
                    auto bpow = [&](std::size_t k) -> double {
                        while (beta_pow.size() <= k)
                            beta_pow.push_back(beta_pow.back() * beta);
                        return beta_pow[k];
                    };
                    double r = 0.0;
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        r = (beta * r) + static_cast<double>(col[i]);  // add col[i] at weight 1
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            r -= bpow(i - lo) * static_cast<double>(col[lo]);
                            ++lo;
                        }
                        result[i] = (alpha * r) +
                                    ((1.0 - alpha) * bpow(i - lo) * static_cast<double>(col[lo]));
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_quantile") {
        // Parse p from the second argument (a numeric literal).
        double p = 0.5;
        if (call.args.size() < 2) {
            return std::unexpected(
                "rolling_quantile: expected two arguments: rolling_quantile(col, p)");
        }
        if (const auto* lit = std::get_if<ir::Literal>(&call.args[1]->node)) {
            if (const auto* dv = std::get_if<double>(&lit->value)) {
                p = *dv;
            } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                p = static_cast<double>(*iv);
            } else {
                return std::unexpected("rolling_quantile: p must be a numeric literal");
            }
        } else {
            return std::unexpected("rolling_quantile: p must be a numeric literal");
        }
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected(
                        "rolling_quantile: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        std::size_t n = i - lo + 1;
                        std::vector<double> window;
                        window.reserve(n);
                        for (std::size_t j = lo; j <= i; ++j)
                            window.push_back(static_cast<double>(col[j]));
                        std::sort(window.begin(), window.end());
                        double idx = p * static_cast<double>(n - 1);
                        std::size_t idx_lo = static_cast<std::size_t>(idx);
                        std::size_t idx_hi = idx_lo + 1 < n ? idx_lo + 1 : idx_lo;
                        double frac = idx - static_cast<double>(idx_lo);
                        result[i] = window[idx_lo] + (frac * (window[idx_hi] - window[idx_lo]));
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_skew") {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_skew: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        std::size_t n = i - lo + 1;
                        if (n < 3) {
                            result[i] = 0.0;
                            continue;
                        }
                        double mean = 0.0;
                        for (std::size_t j = lo; j <= i; ++j)
                            mean += static_cast<double>(col[j]);
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m3 = 0.0;
                        for (std::size_t j = lo; j <= i; ++j) {
                            double d = static_cast<double>(col[j]) - mean;
                            m2 += d * d;
                            m3 += d * d * d;
                        }
                        if (m2 == 0.0) {
                            result[i] = 0.0;
                        } else {
                            auto dn = static_cast<double>(n);
                            result[i] =
                                (dn * std::sqrt(dn - 1.0) / (dn - 2.0)) * (m3 / std::pow(m2, 1.5));
                        }
                    }
                    return result;
                }
            },
            *src);
    }

    if (call.callee == "rolling_kurtosis") {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected(
                        "rolling_kurtosis: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        std::size_t n = i - lo + 1;
                        if (n < 4) {
                            result[i] = 0.0;
                            continue;
                        }
                        double mean = 0.0;
                        for (std::size_t j = lo; j <= i; ++j)
                            mean += static_cast<double>(col[j]);
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m4 = 0.0;
                        for (std::size_t j = lo; j <= i; ++j) {
                            double d = static_cast<double>(col[j]) - mean;
                            double d2 = d * d;
                            m2 += d2;
                            m4 += d2 * d2;
                        }
                        if (m2 == 0.0) {
                            result[i] = 0.0;
                        } else {
                            double dn = static_cast<double>(n);
                            // Fisher excess kurtosis (unbiased, matches scipy/pandas):
                            result[i] = (dn - 1.0) / ((dn - 2.0) * (dn - 3.0)) *
                                        ((dn + 1.0) * dn * m4 / (m2 * m2) - 3.0 * (dn - 1.0));
                        }
                    }
                    return result;
                }
            },
            *src);
    }

    // rolling_min / rolling_max — O(n·w), monotonic deque not yet implemented.
    bool is_min = call.callee == "rolling_min";
    return std::visit(
        [&](const auto& col) -> std::expected<ColumnValue, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, Column<Categorical>> ||
                          std::is_same_v<ColT, Column<std::string>>) {
                return std::unexpected(call.callee + ": string columns not supported");
            } else {
                using T = typename ColT::value_type;
                ColT result;
                result.reserve(rows);
                for (std::size_t i = 0; i < rows; ++i) {
                    std::size_t win_lo = window_lo(time_col, i, duration);
                    T best = col[win_lo];
                    for (std::size_t j = win_lo + 1; j <= i; ++j) {
                        if (is_min ? (col[j] < best) : (col[j] > best))
                            best = col[j];
                    }
                    result.push_back(best);
                }
                return result;
            }
        },
        *src);
}

auto resample_table(const Table& input, ir::Duration bucket_dur,
                    const std::vector<ir::ColumnRef>& extra_group_by,
                    const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    if (!input.time_index.has_value())
        return std::unexpected("resample requires a TimeFrame — use as_timeframe() first");

    const std::string& ts_name = *input.time_index;
    const auto* ts_cv = input.find(ts_name);
    if (ts_cv == nullptr)
        return std::unexpected("resample: time index column '" + ts_name + "' not found");
    const auto* ts_col = std::get_if<Column<Timestamp>>(ts_cv);
    if (ts_col == nullptr)
        return std::unexpected("resample: time index must be a Timestamp column");

    const std::int64_t dur_ns = bucket_dur.count();
    if (dur_ns <= 0)
        return std::unexpected("resample: duration must be positive");

    const auto rows = input.rows();
    const auto bucket_of = [&](std::size_t i) -> std::int64_t {
        const std::int64_t nanos = (*ts_col)[i].nanos;
        std::int64_t q = nanos / dur_ns;
        if (nanos < 0 && nanos % dur_ns != 0)
            --q;  // floor for negative timestamps
        return q * dur_ns;
    };

    // Fast vectorised path: bucket-only grouping with simple numeric reducers
    // over non-null Int/Float columns. The time index is sorted, so each bucket
    // is a contiguous slice and per-bucket first/last/min/max/sum/mean/count
    // reduce with tight (auto-vectorising) loops — far cheaper than the generic
    // row-wise aggregate. Falls through for extra group-by, complex aggregates
    // (median/stddev/...), nullable inputs, or non-numeric columns.
    auto simple_resample = [&]() -> std::optional<std::expected<Table, std::string>> {
        if (!extra_group_by.empty() || rows == 0) {
            return std::nullopt;
        }
        for (const auto& agg : aggregations) {
            switch (agg.func) {
                case ir::AggFunc::Sum:
                case ir::AggFunc::Mean:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                case ir::AggFunc::Count:
                case ir::AggFunc::First:
                case ir::AggFunc::Last:
                    break;
                default:
                    return std::nullopt;  // complex aggregate -> generic path
            }
            if (agg.func == ir::AggFunc::Count) {
                continue;
            }
            const auto* entry = input.find_entry(agg.column.name);
            if (entry == nullptr || entry->validity.has_value()) {
                return std::nullopt;  // missing or nullable -> generic path
            }
            const ColumnValue& cv = *entry->column;
            if (!std::holds_alternative<Column<std::int64_t>>(cv) &&
                !std::holds_alternative<Column<double>>(cv)) {
                return std::nullopt;  // non-numeric -> generic path
            }
        }

        // Bucket boundaries: starts[g] is the first row of bucket g; the trailing
        // sentinel `rows` closes the last bucket.
        std::vector<std::size_t> starts;
        std::vector<std::int64_t> bvals;
        starts.reserve(1024);
        bvals.reserve(1024);
        std::int64_t prev = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            std::int64_t b = bucket_of(i);
            if (i == 0 || b != prev) {
                starts.push_back(i);
                bvals.push_back(b);
                prev = b;
            }
        }
        const std::size_t ng = bvals.size();
        starts.push_back(rows);

        Table out;
        Column<Timestamp> ts_out;
        ts_out.reserve(ng);
        for (std::int64_t b : bvals)
            ts_out.push_back(Timestamp{b});
        out.add_column(ts_name, std::move(ts_out));

        for (const auto& agg : aggregations) {
            if (agg.func == ir::AggFunc::Count) {
                Column<std::int64_t> c;
                c.reserve(ng);
                for (std::size_t g = 0; g < ng; ++g)
                    c.push_back(static_cast<std::int64_t>(starts[g + 1] - starts[g]));
                out.add_column(agg.alias, std::move(c));
                continue;
            }
            const ColumnValue& cv = *input.find_entry(agg.column.name)->column;
            std::visit(
                [&](const auto& src) {
                    using T = typename std::decay_t<decltype(src)>::value_type;
                    if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>) {
                        const bool to_double = (agg.func == ir::AggFunc::Mean);
                        if (to_double) {
                            Column<double> c;
                            c.reserve(ng);
                            for (std::size_t g = 0; g < ng; ++g) {
                                const std::size_t lo = starts[g];
                                const std::size_t hi = starts[g + 1];
                                double acc = 0.0;
                                for (std::size_t j = lo; j < hi; ++j)
                                    acc += static_cast<double>(src[j]);
                                c.push_back(acc / static_cast<double>(hi - lo));
                            }
                            out.add_column(agg.alias, std::move(c));
                        } else {
                            Column<T> c;
                            c.reserve(ng);
                            for (std::size_t g = 0; g < ng; ++g) {
                                const std::size_t lo = starts[g];
                                const std::size_t hi = starts[g + 1];
                                T v = src[lo];
                                switch (agg.func) {
                                    case ir::AggFunc::First:
                                        break;
                                    case ir::AggFunc::Last:
                                        v = src[hi - 1];
                                        break;
                                    case ir::AggFunc::Min:
                                        for (std::size_t j = lo + 1; j < hi; ++j)
                                            v = std::min(v, src[j]);
                                        break;
                                    case ir::AggFunc::Max:
                                        for (std::size_t j = lo + 1; j < hi; ++j)
                                            v = std::max(v, src[j]);
                                        break;
                                    case ir::AggFunc::Sum: {
                                        T s = T{};
                                        for (std::size_t j = lo; j < hi; ++j)
                                            s += src[j];
                                        v = s;
                                        break;
                                    }
                                    default:
                                        break;
                                }
                                c.push_back(v);
                            }
                            out.add_column(agg.alias, std::move(c));
                        }
                    }
                },
                cv);
        }
        out.time_index = ts_name;
        return std::expected<Table, std::string>{std::move(out)};
    };
    if (auto fast = simple_resample(); fast.has_value()) {
        return std::move(*fast);
    }

    // Build bucket column: floor(ts.nanos / dur_ns) * dur_ns
    Column<std::int64_t> bucket_col;
    bucket_col.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        const std::int64_t nanos = (*ts_col)[i].nanos;
        std::int64_t q = nanos / dur_ns;
        if (nanos < 0 && nanos % dur_ns != 0)
            --q;  // floor for negative timestamps
        bucket_col.push_back(q * dur_ns);
    }

    // Clone input, add _bucket column
    Table temp = input;
    temp.add_column("_bucket", std::move(bucket_col));

    // Prepend _bucket to the group-by list
    std::vector<ir::ColumnRef> full_group_by;
    full_group_by.push_back(ir::ColumnRef{.name = "_bucket"});
    full_group_by.insert(full_group_by.end(), extra_group_by.begin(), extra_group_by.end());

    // Run standard aggregation
    auto result = aggregate_table(temp, full_group_by, aggregations);
    if (!result.has_value())
        return result;

    // Convert _bucket (int64) → Timestamp, rename to ts_name
    Table& out = result.value();
    auto it = out.index.find("_bucket");
    if (it == out.index.end())
        return std::unexpected("resample: internal error — _bucket missing from output");
    const std::size_t pos = it->second;

    const auto& i64_col = std::get<Column<std::int64_t>>(*out.columns[pos].column);
    Column<Timestamp> ts_out;
    ts_out.reserve(i64_col.size());
    for (auto v : i64_col)
        ts_out.push_back(Timestamp{v});

    out.rename_column(pos, ts_name);
    out.replace_column(pos, ColumnValue{std::move(ts_out)});
    out.time_index = ts_name;

    return out;
}

// ─── Vectorized RNG ───────────────────────────────────────────────────────────

constexpr auto rng_func_returns_int(std::string_view name) -> bool {
    return name == "rand_bernoulli" || name == "rand_poisson" || name == "rand_int";
}

// get_rng() is defined in rng.hpp (returns thread-local Xoshiro256pp).

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
    if (call.args.size() != 1) {
        return std::unexpected("rep: expected exactly one positional argument (x)");
    }

    // ── parse named args ────────────────────────────────────────────────────
    std::int64_t times = 1;
    std::int64_t each = 1;
    std::int64_t length_out = static_cast<std::int64_t>(rows);  // default = table rows

    for (const auto& narg : call.named_args) {
        const auto* lit = std::get_if<ir::Literal>(&narg.value->node);
        auto as_int = [&]() -> std::expected<std::int64_t, std::string> {
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

    const std::size_t out_len = static_cast<std::size_t>(length_out);
    const std::size_t n_times = static_cast<std::size_t>(times);
    const std::size_t n_each = static_cast<std::size_t>(each);

    // ── scalar literal x ────────────────────────────────────────────────────
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

    // ── column reference x ───────────────────────────────────────────────────
    if (const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node)) {
        const auto* src_col = input.find(col_ref->name);
        if (src_col == nullptr) {
            return std::unexpected("rep: unknown column '" + col_ref->name + "'");
        }
        return std::visit(
            [&](const auto& col) -> ColumnValue {
                using ColT = std::decay_t<decltype(col)>;
                std::size_t src_len = col.size();
                if (src_len == 0) {
                    return ColT{};
                }
                // pattern_len = src_len * each * times (the logical full sequence)
                std::size_t pattern_len = src_len * n_each * n_times;
                ColT result;
                result.reserve(out_len);
                for (std::size_t i = 0; i < out_len; ++i) {
                    // Position within the repeating pattern (cycled via %)
                    std::size_t pos_in_pattern = pattern_len > 0 ? (i % pattern_len) : 0;
                    // Within one "times" cycle: position in (each-repeated sequence)
                    std::size_t pos_in_each_seq = pos_in_pattern % (src_len * n_each);
                    // Index into original column
                    std::size_t src_idx = pos_in_each_seq / n_each;
                    result.push_back(col[src_idx]);
                }
                return result;
            },
            *src_col);
    }

    return std::unexpected("rep: first argument must be a scalar literal or column reference");
}

// Like update_table but evaluates rolling aggregate expressions using the given window duration.
// Non-rolling fields are evaluated via evaluate_field_column (same as regular update_table).
auto windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                           ir::Duration duration, const ScalarRegistry* scalars,
                           const ExternRegistry* externs) -> std::expected<Table, std::string> {
    Table output = std::move(input);
    std::size_t rows = output.rows();
    if (!output.time_index.has_value()) {
        return std::unexpected("window: requires a TimeFrame");
    }
    // Reject mutation of the time index column
    for (const auto& field : fields) {
        if (field.alias == *output.time_index) {
            return std::unexpected("cannot update time index column: " + field.alias);
        }
    }
    for (const auto& field : fields) {
        if (const auto* call = std::get_if<ir::CallExpr>(&field.expr.node)) {
            if (is_rolling_func(call->callee)) {
                auto col = apply_rolling_func(*call, output, duration);
                if (!col) {
                    return std::unexpected(col.error());
                }
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (call->callee == "lag" || call->callee == "lead") {
                auto res =
                    eval_lag_lead_column(*call, output, call->callee == "lag", scalars, externs);
                if (!res) {
                    return std::unexpected(res.error());
                }
                if (res->validity.has_value()) {
                    output.add_column(field.alias, std::move(res->column),
                                      std::move(*res->validity));
                } else {
                    output.add_column(field.alias, std::move(res->column));
                }
                continue;
            }
            if (is_cum_func(call->callee)) {
                auto col = eval_cumsum_cumprod_column(*call, output, call->callee == "cumprod");
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (is_fill_func(call->callee)) {
                auto res = [&]() -> std::expected<FillResult, std::string> {
                    if (call->callee == "fill_null") {
                        return eval_fill_null(*call, output);
                    }
                    if (call->callee == "fill_forward") {
                        return eval_fill_forward(*call, output);
                    }
                    return eval_fill_backward(*call, output);
                }();
                if (!res)
                    return std::unexpected(res.error());
                if (res->validity)
                    output.add_column(field.alias, std::move(res->column),
                                      std::move(*res->validity));
                else
                    output.add_column(field.alias, std::move(res->column));
                continue;
            }
            if (is_float_clean_func(call->callee)) {
                auto res = eval_float_clean(*call, output,
                                            call->callee == "null_if_nan"
                                                ? FloatCleanMode::NullIfNan
                                                : FloatCleanMode::NullIfNotFinite);
                if (!res)
                    return std::unexpected(res.error());
                if (res->validity)
                    output.add_column(field.alias, std::move(res->column),
                                      std::move(*res->validity));
                else
                    output.add_column(field.alias, std::move(res->column));
                continue;
            }
            if (call->callee == "is_nan") {
                auto col = eval_is_nan(*call, output);
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (is_rng_func(call->callee)) {
                auto col = apply_rng_func(*call, output.rows());
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (call->callee == "rep") {
                auto col = apply_rep_func(*call, output, output.rows());
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
        } else if (std::holds_alternative<ir::RankExpr>(field.expr.node)) {
            return std::unexpected("rank(): not supported inside windowed update");
        }
        if (const auto* col_ref = std::get_if<ir::ColumnRef>(&field.expr.node)) {
            const auto* entry = output.find_entry(col_ref->name);
            if (entry != nullptr) {
                if (entry->validity.has_value()) {
                    output.add_column(field.alias, *entry->column, *entry->validity);
                } else {
                    output.add_column(field.alias, *entry->column);
                }
                continue;
            }
            if (scalars != nullptr) {
                if (auto it = scalars->find(col_ref->name); it != scalars->end()) {
                    output.add_column(field.alias, broadcast_scalar_column(it->second, rows));
                    continue;
                }
            }
            return std::unexpected("unknown column '" + col_ref->name + "'");
        }
        auto col = evaluate_field_column(field.expr, output, scalars, externs);
        if (!col) {
            return std::unexpected(col.error());
        }
        output.add_column(field.alias, std::move(col.value()));
    }
    normalize_time_index(output);
    return output;
}

/// Per-group windowed update: partition the input by `group_by`, run the
/// regular `windowed_update_table` on each per-group slice, then scatter the
/// new field columns back into a single full-sized output. The rolling
/// buffer therefore never crosses group boundaries.
///
/// Input rows are assumed time-sorted globally (precondition of TimeFrame),
/// which means within each group the sub-table is also time-sorted.
auto grouped_windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                   ir::Duration duration,
                                   const std::vector<ir::ColumnRef>& group_by,
                                   const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string> {
    if (group_by.empty()) {
        return windowed_update_table(std::move(input), fields, duration, scalars, externs);
    }
    if (!input.time_index.has_value()) {
        return std::unexpected("window: requires a TimeFrame");
    }
    for (const auto& field : fields) {
        if (field.alias == *input.time_index) {
            return std::unexpected("cannot update time index column: " + field.alias);
        }
    }

    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* col = input.find(key.name);
        if (col == nullptr) {
            return std::unexpected("window + by: unknown group key '" + key.name +
                                   "' (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(col);
    }

    const std::size_t rows = input.rows();
    if (rows == 0) {
        return windowed_update_table(std::move(input), fields, duration, scalars, externs);
    }

    // Bucket rows by group key — the row indices land in original
    // (time-sorted) order within each group, which is the precondition the
    // single-buffer rolling implementation relies on.
    robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> group_index;
    std::vector<std::vector<std::size_t>> group_rows;
    for (std::size_t r = 0; r < rows; ++r) {
        Key key;
        key.values.reserve(group_columns.size());
        for (const auto* col : group_columns) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        auto [it, inserted] =
            group_index.emplace(std::move(key), static_cast<std::uint32_t>(group_rows.size()));
        if (inserted) {
            group_rows.emplace_back();
        }
        group_rows[it->second].push_back(r);
    }

    auto run_group =
        [&](const std::vector<std::size_t>& row_idx) -> std::expected<Table, std::string> {
        Table sub;
        for (const auto& entry : input.columns) {
            ColumnValue gathered = gather_column(*entry.column, row_idx.data(), row_idx.size());
            sub.add_column(entry.name, std::move(gathered));
        }
        sub.time_index = input.time_index;
        return windowed_update_table(std::move(sub), fields, duration, scalars, externs);
    };

    // Run the first group to learn the new field column types/names.
    auto first = run_group(group_rows[0]);
    if (!first.has_value()) {
        return std::unexpected(first.error());
    }
    const std::size_t first_new_idx = input.columns.size();
    if (first->columns.size() <= first_new_idx) {
        return std::unexpected("window: grouped update produced no new columns");
    }
    std::vector<std::string> new_field_names;
    new_field_names.reserve(first->columns.size() - first_new_idx);
    for (std::size_t c = first_new_idx; c < first->columns.size(); ++c) {
        new_field_names.push_back(first->columns[c].name);
    }

    // Allocate output's new columns at full size, with the same types as the
    // first sub-result. Strings/categoricals would need a different scatter
    // strategy (per-row write isn't free for flat-buffer strings); rolling /
    // lag / fill ops produce numeric columns in practice, so reject the
    // string/categorical case explicitly until that's needed.
    Table output = input;
    auto allocate_full = [&](const ColumnValue& sample) -> std::expected<ColumnValue, std::string> {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                              std::is_same_v<ColT, Column<Categorical>>) {
                    return std::unexpected(
                        "window + by: scatter of string/Categorical results not yet implemented");
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                } else {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                }
            },
            sample);
    };
    for (const auto& fname : new_field_names) {
        const auto* sample = first->find(fname);
        if (sample == nullptr) {
            return std::unexpected("window: missing new column '" + fname + "' in sub-result");
        }
        auto full = allocate_full(*sample);
        if (!full.has_value()) {
            return std::unexpected(full.error());
        }
        output.add_column(fname, std::move(full.value()));
    }

    auto scatter_into = [](ColumnValue& dst, const ColumnValue& src,
                           const std::vector<std::size_t>& indices) -> std::optional<std::string> {
        return std::visit(
            [&](auto& dcol) -> std::optional<std::string> {
                using DT = std::decay_t<decltype(dcol)>;
                const DT* scol = std::get_if<DT>(&src);
                if (scol == nullptr) {
                    return "window: type mismatch in grouped scatter";
                }
                if constexpr (std::is_same_v<DT, Column<std::string>> ||
                              std::is_same_v<DT, Column<Categorical>>) {
                    return "window: scatter for string/Categorical not implemented";
                } else if constexpr (std::is_same_v<DT, Column<bool>>) {
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dcol.set(indices[i], (*scol)[i]);
                    }
                } else {
                    auto* dp = dcol.data();
                    const auto* sp = scol->data();
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dp[indices[i]] = sp[i];
                    }
                }
                return std::nullopt;
            },
            dst);
    };

    // Lazy-allocated per-field validity bitmaps. We only construct one if at
    // least one group's sub-result has a validity bitmap for that field —
    // most pure-arithmetic outputs stay all-valid and pay nothing.
    std::vector<std::optional<ValidityBitmap>> output_validity(new_field_names.size());

    auto scatter_validity = [&](std::size_t f_idx, const Table& sub_table,
                                const std::vector<std::size_t>& indices) {
        const auto* sub_entry = sub_table.find_entry(new_field_names[f_idx]);
        if (sub_entry == nullptr || !sub_entry->validity.has_value()) {
            return;
        }
        if (!output_validity[f_idx].has_value()) {
            output_validity[f_idx] = ValidityBitmap(rows, true);
        }
        const auto& sub_bm = *sub_entry->validity;
        auto& out_bm = *output_validity[f_idx];
        for (std::size_t i = 0; i < indices.size(); ++i) {
            if (!sub_bm[i]) {
                out_bm.set(indices[i], false);
            }
        }
    };

    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        const auto& fname = new_field_names[f];
        auto* dst = output.find(fname);
        const auto* src = first->find(fname);
        if (auto err = scatter_into(*dst, *src, group_rows[0])) {
            return std::unexpected(*err);
        }
        scatter_validity(f, *first, group_rows[0]);
    }

    for (std::size_t g = 1; g < group_rows.size(); ++g) {
        auto sub = run_group(group_rows[g]);
        if (!sub.has_value()) {
            return std::unexpected(sub.error());
        }
        for (std::size_t f = 0; f < new_field_names.size(); ++f) {
            const auto& fname = new_field_names[f];
            auto* dst = output.find(fname);
            const auto* src = sub->find(fname);
            if (src == nullptr) {
                return std::unexpected("window: missing column '" + fname +
                                       "' in grouped sub-result");
            }
            if (auto err = scatter_into(*dst, *src, group_rows[g])) {
                return std::unexpected(*err);
            }
            scatter_validity(f, *sub, group_rows[g]);
        }
    }

    // Attach the lazy validity bitmaps to their output column entries.
    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        if (!output_validity[f].has_value()) {
            continue;
        }
        auto idx_it = output.index.find(new_field_names[f]);
        if (idx_it != output.index.end()) {
            output.columns[idx_it->second].validity = std::move(output_validity[f]);
        }
    }

    normalize_time_index(output);
    return output;
}

auto update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                  const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string> {
    Table output = std::move(input);
    if (output.time_index.has_value()) {
        for (const auto& field : fields) {
            if (field.alias == *output.time_index) {
                return std::unexpected("cannot update time index column: " + field.alias);
            }
        }
    }
    bool drop_ordering = false;
    if (output.ordering.has_value()) {
        for (const auto& field : fields) {
            for (const auto& key : *output.ordering) {
                if (field.alias == key.name) {
                    drop_ordering = true;
                    break;
                }
            }
            if (drop_ordering) {
                break;
            }
        }
    }
    std::size_t rows = output.rows();
    for (const auto& field : fields) {
        if (const auto* call = std::get_if<ir::CallExpr>(&field.expr.node)) {
            if (call->callee == "lag" || call->callee == "lead") {
                auto res =
                    eval_lag_lead_column(*call, output, call->callee == "lag", scalars, externs);
                if (!res) {
                    return std::unexpected(res.error());
                }
                if (res->validity.has_value()) {
                    output.add_column(field.alias, std::move(res->column),
                                      std::move(*res->validity));
                } else {
                    output.add_column(field.alias, std::move(res->column));
                }
                continue;
            }
            if (is_rolling_func(call->callee)) {
                return std::unexpected(call->callee + ": requires a window clause");
            }
            if (is_cum_func(call->callee)) {
                auto col = eval_cumsum_cumprod_column(*call, output, call->callee == "cumprod");
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (is_fill_func(call->callee)) {
                auto res = [&]() -> std::expected<FillResult, std::string> {
                    if (call->callee == "fill_null") {
                        return eval_fill_null(*call, output);
                    }
                    if (call->callee == "fill_forward") {
                        return eval_fill_forward(*call, output);
                    }
                    return eval_fill_backward(*call, output);
                }();
                if (!res)
                    return std::unexpected(res.error());
                if (res->validity)
                    output.add_column(field.alias, std::move(res->column),
                                      std::move(*res->validity));
                else
                    output.add_column(field.alias, std::move(res->column));
                continue;
            }
            if (is_float_clean_func(call->callee)) {
                auto res = eval_float_clean(*call, output,
                                            call->callee == "null_if_nan"
                                                ? FloatCleanMode::NullIfNan
                                                : FloatCleanMode::NullIfNotFinite);
                if (!res)
                    return std::unexpected(res.error());
                if (res->validity)
                    output.add_column(field.alias, std::move(res->column),
                                      std::move(*res->validity));
                else
                    output.add_column(field.alias, std::move(res->column));
                continue;
            }
            if (call->callee == "is_nan") {
                auto col = eval_is_nan(*call, output);
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (is_rng_func(call->callee)) {
                auto col = apply_rng_func(*call, rows);
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
            if (call->callee == "rep") {
                auto col = apply_rep_func(*call, output, rows);
                if (!col)
                    return std::unexpected(col.error());
                output.add_column(field.alias, std::move(col.value()));
                continue;
            }
        } else if (const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node)) {
            auto res = evaluate_rank_column(output, *rank, {});
            if (!res) {
                return std::unexpected(res.error());
            }
            if (res->validity.has_value()) {
                output.add_column(field.alias, std::move(res->column), std::move(*res->validity));
            } else {
                output.add_column(field.alias, std::move(res->column));
            }
            continue;
        }
        if (const auto* col_ref = std::get_if<ir::ColumnRef>(&field.expr.node)) {
            const auto* entry = output.find_entry(col_ref->name);
            if (entry != nullptr) {
                if (entry->validity.has_value()) {
                    output.add_column(field.alias, *entry->column, *entry->validity);
                } else {
                    output.add_column(field.alias, *entry->column);
                }
                continue;
            }
            if (scalars != nullptr) {
                if (auto it = scalars->find(col_ref->name); it != scalars->end()) {
                    output.add_column(field.alias, broadcast_scalar_column(it->second, rows));
                    continue;
                }
            }
            return std::unexpected("unknown column '" + col_ref->name + "'");
        }
        auto inferred = infer_expr_type(field.expr, output, scalars, externs);
        if (!inferred) {
            return std::unexpected(inferred.error());
        }
        // RNG nested in arithmetic cannot be built per row (eval_expr is pure);
        // evaluate vectorized via eval_value_vec, which materializes each RNG
        // sub-expression as a column (same draw as a bare RNG field). See
        // evaluate_field_column for the same routing on the windowed/select path.
        if (std::holds_alternative<ir::BinaryExpr>(field.expr.node) &&
            expr_contains_rng(field.expr)) {
            auto res = eval_value_vec(field.expr, output, scalars, rows);
            if (!res) {
                return std::unexpected(res.error());
            }
            ColumnValue col = std::holds_alternative<ColumnValue>(res->data)
                                  ? std::move(std::get<ColumnValue>(res->data))
                                  : *std::get<const ColumnValue*>(res->data);
            if (const auto* v = res->get_validity()) {
                output.add_column(field.alias, std::move(col), *v);
            } else {
                output.add_column(field.alias, std::move(col));
            }
            continue;
        }
        if (auto fast = try_fast_update_binary(field.expr, output, rows, inferred.value(), scalars);
            fast.has_value()) {
            auto validity = collect_expr_validity(field.expr, output, rows);
            if (validity.has_value())
                output.add_column(field.alias, std::move(fast.value()), std::move(*validity));
            else
                output.add_column(field.alias, std::move(fast.value()));
            continue;
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
        for (std::size_t row = 0; row < rows; ++row) {
            auto value = eval_expr(field.expr, output, row, scalars, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            std::visit(
                [&](auto& col) {
                    using ColType = std::decay_t<decltype(col)>;
                    using ValueType = typename ColType::value_type;
                    if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(*int_value);
                        } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                            col.push_back(static_cast<std::int64_t>(*double_value));
                        } else {
                            invariant_violation(
                                "update_table_window: expected Int64-compatible expression value");
                        }
                    } else if constexpr (std::is_same_v<ValueType, double>) {
                        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(static_cast<double>(*int_value));
                        } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                            col.push_back(*double_value);
                        } else {
                            invariant_violation(
                                "update_table_window: expected Float64-compatible expression "
                                "value");
                        }
                    } else if constexpr (std::is_same_v<ValueType, bool>) {
                        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(*int_value != 0);
                        } else {
                            invariant_violation(
                                "update_table_window: expected Bool-compatible expression value");
                        }
                    } else if constexpr (std::is_same_v<ValueType, std::string>) {
                        if (const auto* v = std::get_if<std::string>(&value.value())) {
                            col.push_back(*v);
                        } else {
                            invariant_violation(
                                "update_table_window: expected String expression value");
                        }
                    } else if constexpr (std::is_same_v<ValueType, Date>) {
                        if (const auto* v = std::get_if<Date>(&value.value())) {
                            col.push_back(*v);
                        } else if (const auto* int_value =
                                       std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(int64_to_date_checked(*int_value));
                        } else {
                            invariant_violation(
                                "update_table_window: expected Date-compatible expression value");
                        }
                    } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                        if (const auto* v = std::get_if<Timestamp>(&value.value())) {
                            col.push_back(*v);
                        } else if (const auto* int_value =
                                       std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(Timestamp{*int_value});
                        } else {
                            invariant_violation(
                                "update_table_window: expected Timestamp-compatible expression "
                                "value");
                        }
                    }
                },
                new_column);
        }
        auto validity = collect_expr_validity(field.expr, output, rows);
        if (validity.has_value())
            output.add_column(field.alias, std::move(new_column), std::move(*validity));
        else
            output.add_column(field.alias, std::move(new_column));
    }
    if (drop_ordering) {
        output.ordering.reset();
    }
    normalize_time_index(output);
    return output;
}

/// Execute a guarded update `where <predicate> update { ... }`: rows matching
/// the predicate get the field assignments; non-matching rows keep their values
/// (a new column is null off-mask). Each field is evaluated where it is needed —
/// row-local (Scalar-only, `is_subset_evaluable_expr`) fields on just the
/// matching rows (gather/scatter); non-row-local fields (lag/rolling/rank/...)
/// over the full table, then selected by the mask. Both produce the same result;
/// row-locality only decides where the work happens.
auto apply_guarded_update(Table input, const ir::UpdateNode& update, const ScalarRegistry* scalars,
                          const ExternRegistry* externs) -> std::expected<Table, std::string> {
    if (!update.group_by().empty()) {
        return std::unexpected("guarded update (where ... update) does not support `by` yet");
    }
    if (!update.tuple_fields().empty()) {
        return std::unexpected(
            "guarded update (where ... update) does not support tuple-bound fields yet");
    }
    const std::size_t n = input.rows();

    // Mask: a row matches iff the predicate is true AND not null.
    auto mask = compute_mask(*update.guard(), input, scalars, n);
    if (!mask) {
        return std::unexpected(mask.error());
    }
    std::vector<uint8_t> matched(n, 0);
    std::vector<std::size_t> matched_idx;
    for (std::size_t i = 0; i < n; ++i) {
        const bool m = mask->value[i] != 0 && (!mask->valid.has_value() || (*mask->valid)[i] != 0);
        matched[i] = m ? 1U : 0U;
        if (m) {
            matched_idx.push_back(i);
        }
    }

    Table output = std::move(input);
    std::optional<Table> sub;  // matching rows of the original columns (built lazily)

    for (const auto& field : update.fields()) {
        // Snapshot the old column + validity (if the name exists) before overwriting.
        const ColumnEntry* old_entry = output.find_entry(field.alias);
        std::optional<ColumnValue> old_col;
        std::optional<ValidityBitmap> old_valid;
        if (old_entry != nullptr) {
            old_col = *old_entry->column;
            old_valid = old_entry->validity;
        }

        // Evaluate the field. Subset-evaluable fields run on the matching rows
        // only; the rest run over the full table.
        const bool subset = ir::is_subset_evaluable_expr(field.expr);
        ColumnValue new_vals;
        std::optional<ValidityBitmap> new_valid;
        {
            if (subset && !sub.has_value()) {
                sub = gather_rows(output, matched_idx);
            }
            Table src_in = subset ? Table{*sub} : Table{output};
            auto upd = update_table(std::move(src_in), {field}, scalars, externs);
            if (!upd) {
                return std::unexpected(upd.error());
            }
            const ColumnEntry* e = upd->find_entry(field.alias);
            new_vals = *e->column;
            new_valid = e->validity;
        }

        // A guarded assignment may not change the type of an existing column —
        // non-matching rows must keep their old (same-type) values.
        if (old_col.has_value() && old_col->index() != new_vals.index()) {
            return std::unexpected("guarded update cannot change the type of existing column '" +
                                   field.alias + "'");
        }

        // Build the result column by pushing per row: matched -> new value
        // (subset values are aligned with matched_idx; full values indexed by i),
        // non-matched -> old value, or null for a new column.
        auto [result_col, result_valid] = std::visit(
            [&](const auto& src) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
                using Col = std::decay_t<decltype(src)>;
                const Col* oldc = old_col.has_value() ? &std::get<Col>(*old_col) : nullptr;
                Col out;
                out.reserve(n);
                ValidityBitmap valid(n, true);
                bool any_invalid = false;
                std::size_t k = 0;  // running index into `src` for the subset case
                for (std::size_t i = 0; i < n; ++i) {
                    if (matched[i] != 0) {
                        const std::size_t si = subset ? k++ : i;
                        out.push_back(src[si]);
                        const bool v = !new_valid.has_value() || (*new_valid)[si];
                        valid.set(i, v);
                        any_invalid = any_invalid || !v;
                    } else if (oldc != nullptr) {
                        out.push_back((*oldc)[i]);
                        const bool v = !old_valid.has_value() || (*old_valid)[i];
                        valid.set(i, v);
                        any_invalid = any_invalid || !v;
                    } else {
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            out.push_back(std::string_view{});
                        } else {
                            out.push_back(typename Col::value_type{});
                        }
                        valid.set(i, false);
                        any_invalid = true;
                    }
                }
                return {
                    ColumnValue{std::move(out)},
                    any_invalid ? std::optional<ValidityBitmap>{std::move(valid)} : std::nullopt};
            },
            new_vals);

        if (auto it = output.index.find(field.alias); it != output.index.end()) {
            output.replace_column(it->second, std::move(result_col), std::move(result_valid));
        } else if (result_valid.has_value()) {
            output.add_column(field.alias, std::move(result_col), std::move(*result_valid));
        } else {
            output.add_column(field.alias, std::move(result_col));
        }
    }
    return output;
}

/// Per-group update: partition the input by `group_by`, run the regular
/// `update_table` on each per-group slice, then scatter the new field
/// columns back into a single full-sized output. Ordered ops like `lag`,
/// `lead`, `cumsum`, and `fill_forward` therefore see only their group's
/// rows; pure arithmetic gives the same answer per row regardless.
auto grouped_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                          const std::vector<ir::ColumnRef>& group_by, const ScalarRegistry* scalars,
                          const ExternRegistry* externs) -> std::expected<Table, std::string> {
    if (group_by.empty()) {
        return update_table(std::move(input), fields, scalars, externs);
    }
    if (input.time_index.has_value()) {
        for (const auto& field : fields) {
            if (field.alias == *input.time_index) {
                return std::unexpected("cannot update time index column: " + field.alias);
            }
        }
    }

    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* col = input.find(key.name);
        if (col == nullptr) {
            return std::unexpected("update + by: unknown group key '" + key.name +
                                   "' (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(col);
    }

    const std::size_t rows = input.rows();
    if (rows == 0) {
        return update_table(std::move(input), fields, scalars, externs);
    }

    robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> group_index;
    std::vector<std::vector<std::size_t>> group_rows;
    for (std::size_t r = 0; r < rows; ++r) {
        Key key;
        key.values.reserve(group_columns.size());
        for (const auto* col : group_columns) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        auto [it, inserted] =
            group_index.emplace(std::move(key), static_cast<std::uint32_t>(group_rows.size()));
        if (inserted) {
            group_rows.emplace_back();
        }
        group_rows[it->second].push_back(r);
    }

    auto run_group =
        [&](const std::vector<std::size_t>& row_idx) -> std::expected<Table, std::string> {
        Table sub;
        for (const auto& entry : input.columns) {
            ColumnValue gathered = gather_column(*entry.column, row_idx.data(), row_idx.size());
            sub.add_column(entry.name, std::move(gathered));
        }
        sub.time_index = input.time_index;

        std::vector<ir::FieldSpec> pending_row_fields;
        auto flush_pending = [&]() -> std::expected<void, std::string> {
            if (pending_row_fields.empty()) {
                return {};
            }
            auto updated = update_table(std::move(sub), pending_row_fields, scalars, externs);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            sub = std::move(updated.value());
            pending_row_fields.clear();
            return {};
        };

        for (const auto& field : fields) {
            auto aggregate = broadcast_aggregate_column(sub, field, scalars);
            if (!aggregate) {
                return std::unexpected(aggregate.error());
            }
            if (!aggregate->has_value()) {
                pending_row_fields.push_back(field);
                continue;
            }
            if (auto flushed = flush_pending(); !flushed) {
                return std::unexpected(flushed.error());
            }
            if ((*aggregate)->validity.has_value()) {
                sub.add_column(field.alias, std::move((*aggregate)->column),
                               std::move(*(*aggregate)->validity));
            } else {
                sub.add_column(field.alias, std::move((*aggregate)->column));
            }
        }
        if (auto flushed = flush_pending(); !flushed) {
            return std::unexpected(flushed.error());
        }
        return sub;
    };

    auto first = run_group(group_rows[0]);
    if (!first.has_value()) {
        return std::unexpected(first.error());
    }
    const std::size_t first_new_idx = input.columns.size();
    if (first->columns.size() <= first_new_idx) {
        return std::unexpected("update: grouped update produced no new columns");
    }
    std::vector<std::string> new_field_names;
    new_field_names.reserve(first->columns.size() - first_new_idx);
    for (std::size_t c = first_new_idx; c < first->columns.size(); ++c) {
        new_field_names.push_back(first->columns[c].name);
    }

    Table output = input;
    auto allocate_full = [&](const ColumnValue& sample) -> std::expected<ColumnValue, std::string> {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                              std::is_same_v<ColT, Column<Categorical>>) {
                    return std::unexpected(
                        "update + by: scatter of string/Categorical results not yet implemented");
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                } else {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                }
            },
            sample);
    };
    for (const auto& fname : new_field_names) {
        const auto* sample = first->find(fname);
        if (sample == nullptr) {
            return std::unexpected("update: missing new column '" + fname + "' in sub-result");
        }
        auto full = allocate_full(*sample);
        if (!full.has_value()) {
            return std::unexpected(full.error());
        }
        output.add_column(fname, std::move(full.value()));
    }

    auto scatter_into = [](ColumnValue& dst, const ColumnValue& src,
                           const std::vector<std::size_t>& indices) -> std::optional<std::string> {
        return std::visit(
            [&](auto& dcol) -> std::optional<std::string> {
                using DT = std::decay_t<decltype(dcol)>;
                const DT* scol = std::get_if<DT>(&src);
                if (scol == nullptr) {
                    return "update: type mismatch in grouped scatter";
                }
                if constexpr (std::is_same_v<DT, Column<std::string>> ||
                              std::is_same_v<DT, Column<Categorical>>) {
                    return "update: scatter for string/Categorical not implemented";
                } else if constexpr (std::is_same_v<DT, Column<bool>>) {
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dcol.set(indices[i], (*scol)[i]);
                    }
                } else {
                    auto* dp = dcol.data();
                    const auto* sp = scol->data();
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dp[indices[i]] = sp[i];
                    }
                }
                return std::nullopt;
            },
            dst);
    };

    // Same lazy validity scatter as `grouped_windowed_update_table` —
    // ordered ops like `lag(c, 1)` produce a per-group validity bitmap with
    // the first `n` rows marked null; we OR those into the output's column.
    std::vector<std::optional<ValidityBitmap>> output_validity(new_field_names.size());

    auto scatter_validity = [&](std::size_t f_idx, const Table& sub_table,
                                const std::vector<std::size_t>& indices) {
        const auto* sub_entry = sub_table.find_entry(new_field_names[f_idx]);
        if (sub_entry == nullptr || !sub_entry->validity.has_value()) {
            return;
        }
        if (!output_validity[f_idx].has_value()) {
            output_validity[f_idx] = ValidityBitmap(rows, true);
        }
        const auto& sub_bm = *sub_entry->validity;
        auto& out_bm = *output_validity[f_idx];
        for (std::size_t i = 0; i < indices.size(); ++i) {
            if (!sub_bm[i]) {
                out_bm.set(indices[i], false);
            }
        }
    };

    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        const auto& fname = new_field_names[f];
        auto* dst = output.find(fname);
        const auto* src = first->find(fname);
        if (auto err = scatter_into(*dst, *src, group_rows[0])) {
            return std::unexpected(*err);
        }
        scatter_validity(f, *first, group_rows[0]);
    }

    for (std::size_t g = 1; g < group_rows.size(); ++g) {
        auto sub = run_group(group_rows[g]);
        if (!sub.has_value()) {
            return std::unexpected(sub.error());
        }
        for (std::size_t f = 0; f < new_field_names.size(); ++f) {
            const auto& fname = new_field_names[f];
            auto* dst = output.find(fname);
            const auto* src = sub->find(fname);
            if (src == nullptr) {
                return std::unexpected("update: missing column '" + fname +
                                       "' in grouped sub-result");
            }
            if (auto err = scatter_into(*dst, *src, group_rows[g])) {
                return std::unexpected(*err);
            }
            scatter_validity(f, *sub, group_rows[g]);
        }
    }
    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        if (!output_validity[f].has_value()) {
            continue;
        }
        auto idx_it = output.index.find(new_field_names[f]);
        if (idx_it != output.index.end()) {
            output.columns[idx_it->second].validity = std::move(output_validity[f]);
        }
    }
    normalize_time_index(output);
    return output;
}

// NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
auto interpret_node(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    ModelResult* model_out = nullptr) -> std::expected<Table, std::string> {
    switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = static_cast<const ir::ScanNode&>(node);
            auto it = registry.find(scan.source_name());
            if (it == registry.end()) {
                return std::unexpected("unknown table: " + scan.source_name() +
                                       " (available: " + format_tables(registry) + ")");
            }
            Table output = it->second;
            normalize_time_index(output);
            return output;
        }
        case ir::NodeKind::Filter: {
            const auto& filter = static_cast<const ir::FilterNode&>(node);
            if (filter.children().empty()) {
                return std::unexpected("filter node missing child");
            }
            auto child = interpret_node(*filter.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return filter_table(child.value(), filter.predicate(), scalars);
        }
        case ir::NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            if (project.children().empty()) {
                return std::unexpected("project node missing child");
            }
            auto child = interpret_node(*project.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return project_table(child.value(), project.columns());
        }
        case ir::NodeKind::Distinct: {
            if (node.children().empty()) {
                return std::unexpected("distinct node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return distinct_table(child.value());
        }
        case ir::NodeKind::Order: {
            const auto& order = static_cast<const ir::OrderNode&>(node);
            if (order.children().empty()) {
                return std::unexpected("order node missing child");
            }
            auto child = interpret_node(*order.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return order_table(child.value(), order.keys());
        }
        case ir::NodeKind::Head: {
            const auto& head = static_cast<const ir::HeadNode&>(node);
            if (head.children().empty()) {
                return std::unexpected("head node missing child");
            }
            auto count = evaluate_row_count_expr_impl(head.count_expr(), scalars, externs);
            if (!count) {
                return std::unexpected(count.error());
            }
            auto child = interpret_node(*head.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return head_table(child.value(), *count, head.group_by());
        }
        case ir::NodeKind::Tail: {
            const auto& tail = static_cast<const ir::TailNode&>(node);
            if (tail.children().empty()) {
                return std::unexpected("tail node missing child");
            }
            auto count = evaluate_row_count_expr_impl(tail.count_expr(), scalars, externs);
            if (!count) {
                return std::unexpected(count.error());
            }
            auto child = interpret_node(*tail.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return tail_table(child.value(), *count, tail.group_by());
        }
        case ir::NodeKind::Update: {
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            if (update.children().empty()) {
                return std::unexpected("update node missing child");
            }
            auto child = interpret_node(*update.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            if (update.guard() != nullptr) {
                return apply_guarded_update(std::move(child.value()), update, scalars, externs);
            }
            if (!update.group_by().empty()) {
                const bool all_rank = std::all_of(
                    update.fields().begin(), update.fields().end(), [](const ir::FieldSpec& f) {
                        return std::holds_alternative<ir::RankExpr>(f.expr.node);
                    });
                // Pure-rank grouped update has a fast path: rank only needs
                // group keys + ordering, so it skips the gather/scatter dance.
                if (all_rank && update.tuple_fields().empty()) {
                    Table result = std::move(child.value());
                    for (const auto& field : update.fields()) {
                        const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node);
                        auto res = evaluate_rank_column(result, *rank, update.group_by());
                        if (!res) {
                            return std::unexpected(res.error());
                        }
                        if (res->validity.has_value()) {
                            result.add_column(field.alias, std::move(res->column),
                                              std::move(*res->validity));
                        } else {
                            result.add_column(field.alias, std::move(res->column));
                        }
                    }
                    return result;
                }
                if (!update.tuple_fields().empty()) {
                    return std::unexpected(
                        "update + by: tuple-bound fields are not yet supported in grouped updates");
                }
                return grouped_update_table(std::move(child.value()), update.fields(),
                                            update.group_by(), scalars, externs);
            }
            auto result = update_table(std::move(child.value()), update.fields(), scalars, externs);
            if (!result) {
                return result;
            }
            for (const auto& tspec : update.tuple_fields()) {
                auto src = interpret_node(*tspec.source, registry, scalars, externs);
                if (!src) {
                    return std::unexpected(src.error());
                }
                if (tspec.aliases.empty()) {
                    // `update = expr`: merge all columns from the source table.
                    for (const auto& entry : src->columns) {
                        if (entry.validity) {
                            result->add_column(entry.name, *entry.column, *entry.validity);
                        } else {
                            result->add_column(entry.name, *entry.column);
                        }
                    }
                } else {
                    if (src->columns.size() != tspec.aliases.size()) {
                        return std::unexpected(
                            "tuple assignment: expected " + std::to_string(tspec.aliases.size()) +
                            " column(s), got " + std::to_string(src->columns.size()));
                    }
                    for (std::size_t i = 0; i < tspec.aliases.size(); ++i) {
                        const auto& entry = src->columns[i];
                        if (entry.validity) {
                            result->add_column(tspec.aliases[i], *entry.column, *entry.validity);
                        } else {
                            result->add_column(tspec.aliases[i], *entry.column);
                        }
                    }
                }
            }
            return result;
        }
        case ir::NodeKind::Rename: {
            const auto& rename = static_cast<const ir::RenameNode&>(node);
            if (rename.children().empty()) {
                return std::unexpected("rename node missing child");
            }
            auto child = interpret_node(*rename.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return rename_table(child.value(), rename.renames());
        }
        case ir::NodeKind::Aggregate: {
            const auto& agg = static_cast<const ir::AggregateNode&>(node);
            if (agg.children().empty()) {
                return std::unexpected("aggregate node missing child");
            }
            // Fast path: Aggregate(Scan) — pass the registry table by const ref to skip the copy.
            const ir::Node& child_node = *agg.children().front();
            if (child_node.kind() == ir::NodeKind::Scan) {
                const auto& scan = static_cast<const ir::ScanNode&>(child_node);
                auto it = registry.find(scan.source_name());
                if (it == registry.end()) {
                    return std::unexpected("unknown table: " + scan.source_name());
                }
                return aggregate_table(it->second, agg.group_by(), agg.aggregations());
            }
            auto child = interpret_node(child_node, registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return aggregate_table(child.value(), agg.group_by(), agg.aggregations());
        }
        case ir::NodeKind::Resample: {
            const auto& rs = static_cast<const ir::ResampleNode&>(node);
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child.has_value())
                return child;
            return resample_table(child.value(), rs.duration(), rs.group_by(), rs.aggregations());
        }
        case ir::NodeKind::Window: {
            const auto& win = static_cast<const ir::WindowNode&>(node);
            const ir::Node& child_node = *node.children().front();
            // The child must be an UpdateNode produced by the `update` clause.
            if (child_node.kind() != ir::NodeKind::Update) {
                return std::unexpected(
                    "window: only 'update' is currently supported inside a window block");
            }
            const auto& update_node = static_cast<const ir::UpdateNode&>(child_node);
            // Evaluate the source (grandchild) without the window context.
            auto source =
                interpret_node(*child_node.children().front(), registry, scalars, externs);
            if (!source.has_value()) {
                return source;
            }
            if (!source->time_index.has_value()) {
                return std::unexpected(
                    "window requires a TimeFrame — use as_timeframe() to designate a timestamp "
                    "column");
            }
            if (!update_node.group_by().empty()) {
                return grouped_windowed_update_table(std::move(source.value()),
                                                     update_node.fields(), win.duration(),
                                                     update_node.group_by(), scalars, externs);
            }
            return windowed_update_table(std::move(source.value()), update_node.fields(),
                                         win.duration(), scalars, externs);
        }
        case ir::NodeKind::AsTimeframe: {
            const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child.has_value()) {
                return child;
            }
            Table& t = child.value();
            const auto* col = t.find(atf.column());
            if (col == nullptr) {
                return std::unexpected("as_timeframe: column '" + atf.column() + "' not found");
            }
            // Accept Int columns as nanosecond timestamps so CSV-loaded integer
            // time columns work without a plugin.
            if (const auto* int_col = std::get_if<Column<std::int64_t>>(col)) {
                Column<Timestamp> ts_col;
                ts_col.reserve(int_col->size());
                for (auto v : *int_col)
                    ts_col.push_back(Timestamp{v});
                auto idx_it = t.index.find(atf.column());
                if (idx_it != t.index.end()) {
                    t.replace_column(idx_it->second, ColumnValue{std::move(ts_col)});
                    col = t.find(atf.column());
                }
            }
            if (!std::holds_alternative<Column<Timestamp>>(*col) &&
                !std::holds_alternative<Column<Date>>(*col)) {
                return std::unexpected("as_timeframe: column '" + atf.column() +
                                       "' must be Timestamp, Date, or Int");
            }
            auto sorted = order_table(t, {{.name = atf.column(), .ascending = true}});
            if (!sorted.has_value()) {
                return sorted;
            }
            sorted->time_index = atf.column();
            normalize_time_index(*sorted);
            return sorted;
        }
        case ir::NodeKind::Ascribe: {
            const auto& asc = static_cast<const ir::AscribeNode&>(node);
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child.has_value()) {
                return child;
            }
            const Table& t = child.value();
            auto type_matches = [](const ColumnValue& col, ir::ColumnType type) -> bool {
                switch (type) {
                    case ir::ColumnType::Int32:
                    case ir::ColumnType::Int64:
                        return std::holds_alternative<Column<std::int64_t>>(col);
                    case ir::ColumnType::Float32:
                    case ir::ColumnType::Float64:
                        return std::holds_alternative<Column<double>>(col);
                    case ir::ColumnType::Bool:
                        return std::holds_alternative<Column<bool>>(col);
                    case ir::ColumnType::String:
                        return std::holds_alternative<Column<std::string>>(col) ||
                               std::holds_alternative<Column<Categorical>>(col);
                    case ir::ColumnType::Date:
                        return std::holds_alternative<Column<Date>>(col);
                    case ir::ColumnType::Timestamp:
                        return std::holds_alternative<Column<Timestamp>>(col);
                }
                return false;
            };
            for (const auto& field : asc.schema()) {
                const auto* col = t.find(field.name);
                if (col == nullptr) {
                    return std::unexpected("schema ascription: missing column '" + field.name +
                                           "'");
                }
                if (field.type.has_value() && !type_matches(*col, *field.type)) {
                    return std::unexpected("schema ascription: column '" + field.name +
                                           "' has the wrong type");
                }
            }
            // An exact (non-wildcard) ascription forbids columns not listed.
            if (!asc.open()) {
                for (const auto& entry : t.index) {
                    const bool listed = std::any_of(
                        asc.schema().begin(), asc.schema().end(),
                        [&](const ir::SchemaField& f) { return f.name == entry.first; });
                    if (!listed) {
                        return std::unexpected("schema ascription: input has extra column '" +
                                               entry.first +
                                               "' not in the ascribed schema (add `*` to allow "
                                               "extras)");
                    }
                }
            }
            return child;
        }
        case ir::NodeKind::Columns: {
            if (node.children().empty()) {
                return std::unexpected("columns node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child.has_value()) {
                return child;
            }
            return columns_table(child.value());
        }
        case ir::NodeKind::ExternCall: {
            const auto& ec = static_cast<const ir::ExternCallNode&>(node);
            auto result = invoke_extern_call(ec, scalars, externs);
            if (!result.has_value()) {
                return std::unexpected(std::move(result.error()));
            }
            if (auto* table = std::get_if<Table>(&result.value())) {
                return std::move(*table);
            }
            if (externs != nullptr) {
                const auto* fn = externs->find(ec.callee());
                if (fn != nullptr && fn->kind != ExternReturnKind::Table) {
                    return std::unexpected("extern function does not return a table: " +
                                           ec.callee());
                }
            }
            return std::unexpected("extern function did not return a table: " + ec.callee());
        }
        case ir::NodeKind::Join: {
            const auto& join = static_cast<const ir::JoinNode&>(node);
            if (join.children().size() != 2) {
                return std::unexpected("join node expects exactly two children");
            }
            auto left = interpret_node(*join.children()[0], registry, scalars, externs);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = interpret_node(*join.children()[1], registry, scalars, externs);
            if (!right) {
                return std::unexpected(right.error());
            }
            const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
            return join_table_impl(left.value(), right.value(), join.kind(), join.keys(), pred,
                                   scalars, compute_mask);
        }
        case ir::NodeKind::Melt: {
            const auto& mn = static_cast<const ir::MeltNode&>(node);
            if (mn.children().empty()) {
                return std::unexpected("melt node missing child");
            }
            auto child = interpret_node(*mn.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return melt_table(child.value(), mn.id_columns(), mn.measure_columns());
        }
        case ir::NodeKind::Dcast: {
            const auto& dn = static_cast<const ir::DcastNode&>(node);
            if (dn.children().empty()) {
                return std::unexpected("dcast node missing child");
            }
            auto child = interpret_node(*dn.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return dcast_table(child.value(), dn.pivot_column(), dn.value_column(), dn.row_keys());
        }
        case ir::NodeKind::Cov: {
            if (node.children().empty()) {
                return std::unexpected("cov node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return cov_table(child.value());
        }
        case ir::NodeKind::Corr: {
            if (node.children().empty()) {
                return std::unexpected("corr node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return corr_table(child.value());
        }
        case ir::NodeKind::Transpose: {
            if (node.children().empty()) {
                return std::unexpected("transpose node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return transpose_table(child.value());
        }
        case ir::NodeKind::Matmul: {
            if (node.children().size() != 2) {
                return std::unexpected("matmul node expects exactly two children");
            }
            auto left = interpret_node(*node.children()[0], registry, scalars, externs);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = interpret_node(*node.children()[1], registry, scalars, externs);
            if (!right) {
                return std::unexpected(right.error());
            }
            return matmul_table(left.value(), right.value());
        }
        case ir::NodeKind::Stream: {
            const auto& sn = static_cast<const ir::StreamNode&>(node);
            if (externs == nullptr) {
                return std::unexpected("stream node requires an extern registry");
            }
            if (sn.children().empty()) {
                return std::unexpected("stream node has no transform child");
            }

            // Resolve source and sink functions.
            const auto* source_fn = externs->find(sn.source_callee());
            if (source_fn == nullptr) {
                return std::unexpected("unknown stream source: " + sn.source_callee());
            }
            if (source_fn->kind != ExternReturnKind::Table) {
                return std::unexpected("stream source must return a table: " + sn.source_callee());
            }
            const auto* sink_fn = externs->find(sn.sink_callee());
            if (sink_fn == nullptr) {
                return std::unexpected("unknown stream sink: " + sn.sink_callee());
            }
            if (!sink_fn->first_arg_is_table) {
                return std::unexpected("stream sink must be a table-consumer extern: " +
                                       sn.sink_callee());
            }

            // Pre-evaluate scalar args (literals — no row context needed).
            ExternArgs source_args;
            for (const auto& arg : sn.source_args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val)
                    return std::unexpected(val.error());
                source_args.push_back(std::move(val.value()));
            }
            ExternArgs sink_scalar_args;
            for (const auto& arg : sn.sink_args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val)
                    return std::unexpected(val.error());
                sink_scalar_args.push_back(std::move(val.value()));
            }

            const ir::Node& transform_ir = sn.transform_ir();

            // Append every row of `src` into `dst`, initialising dst schema on first call.
            auto append_table = [&](Table& dst,
                                    const Table& src) -> std::expected<void, std::string> {
                if (src.rows() == 0)
                    return {};
                if (dst.columns.empty()) {
                    for (const auto& entry : src.columns) {
                        dst.add_column(entry.name, make_empty_like(*entry.column));
                    }
                    dst.time_index = src.time_index;
                    dst.ordering = src.ordering;
                }
                for (std::size_t row = 0; row < src.rows(); ++row) {
                    for (std::size_t col = 0; col < src.columns.size(); ++col) {
                        if (col >= dst.columns.size()) {
                            return std::unexpected("stream: source schema changed mid-stream");
                        }
                        auto& dst_col = dst.mutable_column(col);
                        append_value(dst_col, *src.columns[col].column, row);
                        bool null = is_null(src.columns[col], row);
                        if (null) {
                            if (!dst.columns[col].validity.has_value()) {
                                dst.columns[col].validity =
                                    ValidityBitmap(column_size(dst_col) - 1, true);
                            }
                            dst.columns[col].validity->push_back(false);
                        } else if (dst.columns[col].validity.has_value()) {
                            dst.columns[col].validity->push_back(true);
                        }
                    }
                }
                return {};
            };

            // Slice a single row out of `src` into a new one-row Table.
            auto slice_row = [&](const Table& src, std::size_t r) -> Table {
                Table out;
                for (const auto& entry : src.columns) {
                    out.add_column(entry.name, make_empty_like(*entry.column));
                    const std::size_t out_pos = out.columns.size() - 1;
                    append_value(out.mutable_column(out_pos), *entry.column, r);
                    if (is_null(entry, r)) {
                        out.columns.back().validity = ValidityBitmap{false};
                    }
                }
                out.time_index = src.time_index;
                return out;
            };

            // Get the nanosecond timestamp of the last row (for bucket detection).
            auto get_last_ts_ns = [&](const Table& t) -> std::optional<std::int64_t> {
                if (t.rows() == 0 || !t.time_index.has_value())
                    return std::nullopt;
                const auto* col = t.find(*t.time_index);
                if (col == nullptr)
                    return std::nullopt;
                std::size_t last = t.rows() - 1;
                return std::visit(
                    [last](const auto& c) -> std::optional<std::int64_t> {
                        using C = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<C, Column<Timestamp>>) {
                            return static_cast<std::int64_t>(c[last].nanos);
                        } else if constexpr (std::is_same_v<C, Column<std::int64_t>>) {
                            return c[last];
                        }
                        return std::nullopt;
                    },
                    *col);
            };

            // Run the transform over `buf` and emit the result to the sink.
            auto emit_buffer = [&](const Table& buf) -> std::expected<void, std::string> {
                if (buf.rows() == 0)
                    return {};
                TableRegistry stream_reg = registry;
                stream_reg["__stream_input__"] = buf;
                auto output = interpret_node(transform_ir, stream_reg, scalars, externs);
                if (!output)
                    return std::unexpected(output.error());
                if (output->rows() == 0)
                    return {};
                auto sr = sink_fn->table_consumer_func(*output, sink_scalar_args);
                if (!sr)
                    return std::unexpected(sr.error());
                return {};
            };

            // ── Event loop ──────────────────────────────────────────────────────
            Table buffer;
            std::int64_t open_bucket_ns = -1;
            std::int64_t bucket_open_wall_ns = -1;  // wall-clock ns when current bucket was opened
            const std::int64_t bucket_ns =
                sn.stream_kind() == ir::StreamKind::TimeBucket
                    ? static_cast<std::int64_t>(sn.bucket_duration().count())
                    : 0;

            // Returns current wall-clock time in nanoseconds.
            auto wall_now_ns = []() -> std::int64_t {
                auto now = std::chrono::system_clock::now();
                auto ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
                return static_cast<std::int64_t>(ns.count());
            };

            while (true) {
                auto src_result = source_fn->func(source_args);
                if (!src_result)
                    return std::unexpected(src_result.error());

                // StreamTimeout: the source had a receive timeout — no data arrived but
                // it is not done.  Run the wall-clock flush check and keep listening.
                const bool is_timeout = std::holds_alternative<StreamTimeout>(src_result.value());

                if (!is_timeout) {
                    const auto* batch = std::get_if<Table>(&src_result.value());
                    if (batch == nullptr) {
                        return std::unexpected("stream source did not return a table");
                    }
                    if (batch->rows() == 0)
                        break;  // source signalled EOF
                }

                if (sn.stream_kind() == ir::StreamKind::TimeBucket && bucket_ns > 0) {
                    if (open_bucket_ns >= 0 && buffer.rows() > 0 &&
                        wall_now_ns() - bucket_open_wall_ns >= bucket_ns) {
                        auto er = emit_buffer(buffer);
                        if (!er)
                            return std::unexpected(er.error());
                        buffer = Table{};
                        open_bucket_ns = -1;
                        bucket_open_wall_ns = -1;
                    }

                    if (!is_timeout) {
                        const auto& batch = std::get<Table>(src_result.value());
                        for (std::size_t r = 0; r < batch.rows(); ++r) {
                            Table row_tbl = slice_row(batch, r);
                            auto ts_opt = get_last_ts_ns(row_tbl);
                            std::int64_t row_bucket =
                                ts_opt ? ((*ts_opt / bucket_ns) * bucket_ns) : -1;

                            if (open_bucket_ns >= 0 && row_bucket >= 0 &&
                                row_bucket > open_bucket_ns) {
                                auto er = emit_buffer(buffer);
                                if (!er)
                                    return std::unexpected(er.error());
                                buffer = Table{};
                            }
                            if (row_bucket >= 0) {
                                if (row_bucket != open_bucket_ns) {
                                    bucket_open_wall_ns = wall_now_ns();
                                }
                                open_bucket_ns = row_bucket;
                            }
                            auto app = append_table(buffer, row_tbl);
                            if (!app)
                                return std::unexpected(app.error());
                        }
                    }
                } else if (!is_timeout) {
                    const auto& batch = std::get<Table>(src_result.value());
                    auto app = append_table(buffer, batch);
                    if (!app)
                        return std::unexpected(app.error());
                    auto er = emit_buffer(buffer);
                    if (!er)
                        return std::unexpected(er.error());
                }
            }

            if (sn.stream_kind() == ir::StreamKind::TimeBucket && buffer.rows() > 0) {
                auto er = emit_buffer(buffer);
                if (!er)
                    return std::unexpected(er.error());
            }

            return Table{};
        }
        case ir::NodeKind::Construct: {
            const auto& cn = static_cast<const ir::ConstructNode&>(node);
            // `Table(n)` form: an empty frame carrying an explicit row count.
            if (cn.row_count().has_value()) {
                auto n = evaluate_row_count_expr_impl(*cn.row_count(), scalars, externs);
                if (!n.has_value()) {
                    return std::unexpected(n.error());
                }
                Table empty;
                empty.logical_rows = *n;
                return empty;
            }
            Table result;
            for (const auto& col : cn.columns()) {
                if (col.expr_node) {
                    // Expression column: evaluate the sub-node to produce a Table,
                    // then extract the target column from it.
                    auto sub = interpret_node(*col.expr_node, registry, scalars, externs);
                    if (!sub.has_value())
                        return std::unexpected(sub.error());
                    if (sub->columns.size() == 1) {
                        // Single-column result: use it regardless of its name.
                        ColumnEntry entry = sub->columns[0];
                        entry.name = col.name;
                        result.index[col.name] = result.columns.size();
                        result.columns.push_back(std::move(entry));
                    } else if (auto it = sub->index.find(col.name); it != sub->index.end()) {
                        // Multi-column result: extract the column matching col.name.
                        ColumnEntry entry = sub->columns[it->second];
                        entry.name = col.name;
                        result.index[col.name] = result.columns.size();
                        result.columns.push_back(std::move(entry));
                    } else {
                        return std::unexpected(
                            "Table constructor: expression for column '" + col.name +
                            "' must produce a single-column result or a table containing"
                            " a column named '" +
                            col.name + "'");
                    }
                    continue;
                }
                if (col.elements.empty()) {
                    // Empty array literal: default to Int64
                    result.add_column(col.name, Column<std::int64_t>{});
                    continue;
                }
                // Literal column: build from inline values.
                ColumnValue cv = std::visit(
                    [&](const auto& first_val) -> ColumnValue {
                        using T = std::decay_t<decltype(first_val)>;
                        Column<T> col_data;
                        col_data.reserve(col.elements.size());
                        for (const auto& lit : col.elements) {
                            col_data.push_back(std::get<T>(lit.value));
                        }
                        return col_data;
                    },
                    col.elements[0].value);
                result.add_column(col.name, std::move(cv));
            }
            // Validate that all columns have the same length.
            if (!result.columns.empty()) {
                std::size_t n_rows =
                    std::visit([](const auto& c) { return c.size(); }, *result.columns[0].column);
                for (std::size_t i = 1; i < result.columns.size(); ++i) {
                    std::size_t len = std::visit([](const auto& c) { return c.size(); },
                                                 *result.columns[i].column);
                    if (len != n_rows) {
                        return std::unexpected(
                            "Table constructor: all columns must have the same length ('" +
                            result.columns[i].name + "' has " + std::to_string(len) +
                            " elements, expected " + std::to_string(n_rows) + ")");
                    }
                }
            }
            return result;
        }
        case ir::NodeKind::Model: {
            const auto& mn = static_cast<const ir::ModelNode&>(node);
            if (mn.children().empty()) {
                return std::unexpected("model node missing child");
            }
            auto child = interpret_node(*mn.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto result =
                fit_model(child.value(), mn.formula(), mn.method(), mn.params(), scalars, externs);
            if (!result) {
                return std::unexpected(result.error());
            }
            // Extract the primary table before potentially moving the whole
            // result. Linear methods expose coefficients; tree models expose
            // feature importance; unsupervised models (e.g. kmeans) have neither,
            // so fall back to the per-row fitted output (e.g. cluster ids).
            Table primary =
                !result.value().coefficients.columns.empty() ? result.value().coefficients
                : !result.value().importance.columns.empty() ? result.value().importance
                                                             : result.value().fitted_values;
            if (model_out != nullptr) {
                *model_out = std::move(result.value());
            }
            return primary;
        }
        case ir::NodeKind::Program: {
            const auto& program = static_cast<const ir::ProgramNode&>(node);
            auto preamble = execute_program_preamble(program.preamble(), scalars, externs);
            if (!preamble.has_value()) {
                return std::unexpected(std::move(preamble.error()));
            }
            return interpret_node(program.main_node(), registry, scalars, externs, model_out);
        }
        case ir::NodeKind::FilterProject: {
            // Fused shape produced by canonicalize R5. Materializing fallback
            // for contexts where chunked build_operator is bypassed: evaluate
            // filter then project sequentially.
            const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
            if (fp.children().empty()) {
                return std::unexpected("filter_project node missing child");
            }
            auto child = interpret_node(*fp.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), fp.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            return project_table(filtered.value(), fp.columns());
        }
        case ir::NodeKind::FilterUpdateProject: {
            // Fused shape produced by canonicalize R6. Materializing fallback.
            const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
            if (fup.children().empty()) {
                return std::unexpected("filter_update_project node missing child");
            }
            auto child = interpret_node(*fup.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), fup.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            auto updated =
                update_table(std::move(filtered.value()), fup.fields(), scalars, externs);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            return project_table(updated.value(), fup.project_columns());
        }
        case ir::NodeKind::TopK: {
            const auto& topk = static_cast<const ir::TopKNode&>(node);
            if (topk.children().empty()) {
                return std::unexpected("topk node missing child");
            }
            auto child = interpret_node(*topk.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto sorted = order_table(child.value(), topk.keys());
            if (!sorted) {
                return std::unexpected(sorted.error());
            }
            if (topk.keep_mode() == ir::TopKNode::KeepMode::First) {
                return head_table(sorted.value(), topk.count(), topk.group_by());
            }
            return tail_table(sorted.value(), topk.count(), topk.group_by());
        }
        case ir::NodeKind::FilterHead: {
            const auto& fh = static_cast<const ir::FilterHeadNode&>(node);
            if (fh.children().empty()) {
                return std::unexpected("filter_head node missing child");
            }
            auto child = interpret_node(*fh.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), fh.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            return head_table(filtered.value(), fh.count(), {});
        }
        case ir::NodeKind::FilterTail: {
            const auto& ft = static_cast<const ir::FilterTailNode&>(node);
            if (ft.children().empty()) {
                return std::unexpected("filter_tail node missing child");
            }
            auto child = interpret_node(*ft.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), ft.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            return tail_table(filtered.value(), ft.count(), {});
        }
    }
    return std::unexpected("unknown node kind");
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace

auto evaluate_row_count_expr(const ir::Expr& expr, const ScalarRegistry* scalars,
                             const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string> {
    return evaluate_row_count_expr_impl(expr, scalars, externs);
}

auto merge_validity_bitmaps(const ValidityBitmap* a, const ValidityBitmap* b, std::size_t n)
    -> std::optional<ValidityBitmap> {
    return merge_validity(a, b, n);
}

void Table::add_column(std::string name, ColumnValue column) {
    if (auto it = index.find(name); it != index.end()) {
        // Reseat the shared_ptr rather than mutating shared data (copy-on-write).
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        columns[it->second].validity.reset();
        return;
    }
    std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{.name = std::move(name),
                                  .column = std::make_shared<ColumnValue>(std::move(column)),
                                  .validity = std::nullopt});
    index[columns.back().name] = pos;
}

void Table::add_column(std::string name, ColumnValue column, ValidityBitmap validity) {
    if (auto it = index.find(name); it != index.end()) {
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        columns[it->second].validity = std::move(validity);
        return;
    }
    std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{.name = std::move(name),
                                  .column = std::make_shared<ColumnValue>(std::move(column)),
                                  .validity = std::move(validity)});
    index[columns.back().name] = pos;
}

void Table::replace_column(std::size_t pos, ColumnValue column) {
    auto& entry = columns.at(pos);
    entry.column = std::make_shared<ColumnValue>(std::move(column));
}

void Table::replace_column(std::size_t pos, ColumnValue column,
                           std::optional<ValidityBitmap> validity) {
    auto& entry = columns.at(pos);
    entry.column = std::make_shared<ColumnValue>(std::move(column));
    entry.validity = std::move(validity);
}

void Table::rename_column(std::size_t pos, std::string name) {
    auto& entry = columns.at(pos);
    if (auto it = index.find(entry.name); it != index.end() && it->second == pos) {
        index.erase(it);
    }
    entry.name = std::move(name);
    index[entry.name] = pos;
}

auto Table::mutable_column(std::size_t pos) -> ColumnValue& {
    auto& column = columns.at(pos).column;
    if (column.use_count() != 1) {
        column = std::make_shared<ColumnValue>(*column);
    }
    return *column;
}

void Table::add_column_shared(std::string name, std::shared_ptr<ColumnValue> column,
                              std::optional<ValidityBitmap> validity) {
    if (auto it = index.find(name); it != index.end()) {
        columns[it->second].column = std::move(column);
        columns[it->second].validity = std::move(validity);
        return;
    }
    std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{
        .name = std::move(name), .column = std::move(column), .validity = std::move(validity)});
    index[columns.back().name] = pos;
}

auto Table::find_entry(const std::string& name) const -> const ColumnEntry* {
    if (auto it = index.find(name); it != index.end()) {
        return &columns[it->second];
    }
    return nullptr;
}

auto Table::find(const std::string& name) -> ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return columns[it->second].column.get();
    }
    return nullptr;
}

auto Table::find(const std::string& name) const -> const ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return columns[it->second].column.get();
    }
    return nullptr;
}

namespace {

auto chunk_to_table(Chunk chunk) -> Table {
    Table t;
    t.columns = std::move(chunk.columns);
    for (std::size_t i = 0; i < t.columns.size(); ++i) {
        t.index[t.columns[i].name] = i;
    }
    t.ordering = std::move(chunk.ordering);
    t.time_index = std::move(chunk.time_index);
    if (t.columns.empty()) {  // logical_rows is only meaningful when column-less
        t.logical_rows = chunk.logical_rows;
    }
    return t;
}

auto table_to_chunk(Table table) -> Chunk {
    Chunk c;
    c.columns = std::move(table.columns);
    c.ordering = std::move(table.ordering);
    c.time_index = std::move(table.time_index);
    if (c.columns.empty()) {  // logical_rows is only meaningful when column-less
        c.logical_rows = table.logical_rows;
    }
    return c;
}

/// Per-chunk filter: pulls a chunk from the child, wraps it as a `Table`,
/// reuses the existing `filter_table` predicate evaluator, and emits the
/// filtered columns as the next chunk. Chunks that filter to zero rows
/// are skipped — the operator loops until it has a non-empty chunk or
/// the child stream ends.
class ChunkedFilterOperator final : public Operator {
   public:
    ChunkedFilterOperator(OperatorPtr child, const ir::Expr* predicate,
                          const ScalarRegistry* scalars)
        : child_(std::move(child)), predicate_(predicate), scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }
            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_table(t, *predicate_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->columns.empty() && filtered->rows() == 0) {
                continue;
            }
            return std::optional<Chunk>{table_to_chunk(std::move(filtered.value()))};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const ScalarRegistry* scalars_;
};

/// Per-chunk project: pulls a chunk, reuses `project_table` to select
/// and rename columns, and forwards the result. Stateless and order
/// preserving; no inter-chunk coordination is needed.
class ChunkedProjectOperator final : public Operator {
   public:
    ChunkedProjectOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* columns)
        : child_(std::move(child)), columns_(columns) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        Table t = chunk_to_table(std::move(*chunk_res.value()));
        auto projected = project_table(t, *columns_);
        if (!projected.has_value()) {
            return std::unexpected(std::move(projected.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(projected.value()))};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* columns_;
};

/// Fused filter→project: computes the filter mask once per chunk and gathers
/// only the projected columns. Skips materializing columns that the surrounding
/// `select` would discard, which is the main win over running `Filter` then
/// `Project` as independent chunked operators.
class ChunkedFilterProjectOperator final : public Operator {
   public:
    ChunkedFilterProjectOperator(OperatorPtr child, const ir::Expr* predicate,
                                 const std::vector<ir::ColumnRef>* columns,
                                 const ScalarRegistry* scalars)
        : child_(std::move(child)), predicate_(predicate), columns_(columns), scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }
            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto out = filter_project_table(t, *predicate_, *columns_, scalars_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (!out->columns.empty() && out->rows() == 0) {
                continue;
            }
            return std::optional<Chunk>{table_to_chunk(std::move(out.value()))};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const std::vector<ir::ColumnRef>* columns_;
    const ScalarRegistry* scalars_;
};

/// Fused filter→head(n): pushes the row limit into the per-chunk filter so
/// gather stops as soon as `n` surviving rows are produced, and short-circuits
/// pulling from the child once the limit is reached. Only used for global
/// `head` (no group_by); grouped head still uses ChunkedHeadOperator.
class ChunkedFilterHeadOperator final : public Operator {
   public:
    ChunkedFilterHeadOperator(OperatorPtr child, const ir::Expr* predicate, std::size_t count,
                              const ScalarRegistry* scalars)
        : child_(std::move(child)),
          predicate_(predicate),
          count_(count),
          remaining_(count),
          scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        while (true) {
            if (remaining_ == 0) {
                done_ = true;
                return std::optional<Chunk>{};
            }
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                done_ = true;
                return std::optional<Chunk>{};
            }
            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto out = filter_table_limit(t, *predicate_, remaining_, scalars_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            const std::size_t produced = out->rows();
            if (!out->columns.empty() && produced == 0) {
                continue;
            }
            remaining_ -= produced;
            if (remaining_ == 0) {
                done_ = true;
            }
            (void)count_;
            return std::optional<Chunk>{table_to_chunk(std::move(out.value()))};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    std::size_t count_;
    std::size_t remaining_;
    const ScalarRegistry* scalars_;
    bool done_ = false;
};

/// Fused `Tail(Filter(x))`: filters each incoming chunk, then keeps only the
/// last `n` matching rows in a rolling buffer so we never hold the full
/// filtered result in memory (the prior materializing path built the entire
/// filter output and sliced the last `n`). We must still drain the child —
/// `tail` is inherently a read-all operator — but peak memory is O(n) rather
/// than O(matches). Only wired for global `tail` (empty group_by); grouped
/// tail still goes through the materializing path.
class ChunkedFilterTailOperator final : public Operator {
   public:
    ChunkedFilterTailOperator(OperatorPtr child, const ir::Expr* predicate, std::size_t count,
                              const ScalarRegistry* scalars)
        : child_(std::move(child)), predicate_(predicate), count_(count), scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_table(t, *predicate_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (filtered->columns.empty()) {
                continue;
            }
            if (filtered->rows() == 0) {
                continue;
            }
            buffered_rows_ += filtered->rows();
            buffered_.push_back(std::move(filtered.value()));
            trim_to_limit();
        }
        done_ = true;
        if (buffered_.empty()) {
            return std::optional<Chunk>{};
        }
        if (buffered_.size() == 1) {
            return std::optional<Chunk>{table_to_chunk(std::move(buffered_.front()))};
        }
        auto concat = concat_buffered();
        if (!concat.has_value()) {
            return std::unexpected(std::move(concat.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(concat.value()))};
    }

   private:
    // Drop or slice from the front of `buffered_` until its combined row count
    // is ≤ count_. Full-chunk drops are cheap (pointer-level pop); only one
    // partial slice (gather_rows on the front) is ever needed per trim.
    auto trim_to_limit() -> void {
        while (buffered_rows_ > count_ && !buffered_.empty()) {
            const std::size_t front_rows = buffered_.front().rows();
            if (buffered_rows_ - front_rows >= count_) {
                buffered_rows_ -= front_rows;
                buffered_.pop_front();
                continue;
            }
            const std::size_t excess = buffered_rows_ - count_;
            const std::size_t keep = front_rows - excess;
            std::vector<std::size_t> idx;
            idx.reserve(keep);
            for (std::size_t i = excess; i < front_rows; ++i) {
                idx.push_back(i);
            }
            buffered_.front() = gather_rows(buffered_.front(), idx);
            buffered_rows_ = count_;
            break;
        }
    }

    auto concat_buffered() -> std::expected<Table, std::string> {
        Table out = std::move(buffered_.front());
        buffered_.pop_front();
        const std::size_t n_cols = out.columns.size();
        while (!buffered_.empty()) {
            Table& src_t = buffered_.front();
            if (src_t.columns.size() != n_cols) {
                return std::unexpected("tail: chunk schema mismatch (column count)");
            }
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (src_t.columns[i].name != out.columns[i].name) {
                    return std::unexpected("tail: chunk schema mismatch (column name)");
                }
                if (src_t.columns[i].column->index() != out.columns[i].column->index()) {
                    return std::unexpected("tail: chunk schema mismatch (column type)");
                }
                auto& dst_col = out.mutable_column(i);
                std::visit(
                    [&](auto& dst) {
                        using Col = std::decay_t<decltype(dst)>;
                        auto& src = std::get<Col>(*src_t.columns[i].column);
                        dst.reserve(dst.size() + src.size());
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_code(src.code_at(r));
                            }
                        } else {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_back(src[r]);
                            }
                        }
                    },
                    dst_col);
            }
            buffered_.pop_front();
        }
        return out;
    }

    OperatorPtr child_;
    const ir::Expr* predicate_;
    std::size_t count_;
    const ScalarRegistry* scalars_;
    std::deque<Table> buffered_;
    std::size_t buffered_rows_ = 0;
    bool done_ = false;
};

/// Fused filter→update→project: evaluates the predicate per chunk, gathers
/// only the columns needed (referenced by any update expression, or in the
/// final projection but not produced by the update), then runs the row-local
/// update and final projection. Skips materializing columns the surrounding
/// select would discard — the same win as ChunkedFilterProjectOperator, but
/// allowing computed fields in the select.
class ChunkedFilterUpdateProjectOperator final : public Operator {
   public:
    ChunkedFilterUpdateProjectOperator(OperatorPtr child, const ir::Expr* predicate,
                                       const std::vector<ir::FieldSpec>* fields,
                                       const std::vector<ir::ColumnRef>* project_columns,
                                       std::vector<ir::ColumnRef> gather_columns,
                                       const ScalarRegistry* scalars, const ExternRegistry* externs)
        : child_(std::move(child)),
          predicate_(predicate),
          fields_(fields),
          project_columns_(project_columns),
          gather_columns_(std::move(gather_columns)),
          scalars_(scalars),
          externs_(externs) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }
            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_project_table(t, *predicate_, gather_columns_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->columns.empty() && filtered->rows() == 0) {
                continue;
            }
            auto updated = update_table(std::move(filtered.value()), *fields_, scalars_, externs_);
            if (!updated.has_value()) {
                return std::unexpected(std::move(updated.error()));
            }
            auto projected = project_table(updated.value(), *project_columns_);
            if (!projected.has_value()) {
                return std::unexpected(std::move(projected.error()));
            }
            return std::optional<Chunk>{table_to_chunk(std::move(projected.value()))};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const std::vector<ir::FieldSpec>* fields_;
    const std::vector<ir::ColumnRef>* project_columns_;
    std::vector<ir::ColumnRef> gather_columns_;
    const ScalarRegistry* scalars_;
    const ExternRegistry* externs_;
};

class ChunkedRenameOperator final : public Operator {
   public:
    ChunkedRenameOperator(OperatorPtr child, const std::vector<ir::RenameSpec>* renames)
        : child_(std::move(child)), renames_(renames) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        Chunk chunk = std::move(*chunk_res.value());
        for (const auto& spec : *renames_) {
            bool found = false;
            for (auto& col : chunk.columns) {
                if (col.name == spec.old_name) {
                    col.name = spec.new_name;
                    found = true;
                    break;
                }
            }
            if (!found) {
                return std::unexpected("rename: column not found: " + spec.old_name);
            }
        }
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::RenameSpec>* renames_;
};

using ir::collect_expr_column_refs;
using ir::is_row_local_update_expr;

/// Per-chunk update for row-local field expressions. `build_operator()` only
/// routes here when all of the UpdateNode's field expressions are row-local
/// (per `is_row_local_update_expr`) and there are no tuple_fields or
/// group_by clauses — the subset where running `update_table` per chunk is
/// equivalent to running it on the materialized table.
class ChunkedUpdateOperator final : public Operator {
   public:
    ChunkedUpdateOperator(OperatorPtr child, const std::vector<ir::FieldSpec>* fields,
                          const ScalarRegistry* scalars, const ExternRegistry* externs)
        : child_(std::move(child)), fields_(fields), scalars_(scalars), externs_(externs) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        Table t = chunk_to_table(std::move(*chunk_res.value()));
        auto out = update_table(std::move(t), *fields_, scalars_, externs_);
        if (!out.has_value()) {
            return std::unexpected(std::move(out.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(out.value()))};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::FieldSpec>* fields_;
    const ScalarRegistry* scalars_;
    const ExternRegistry* externs_;
};

class ChunkedHeadOperator final : public Operator {
   public:
    ChunkedHeadOperator(OperatorPtr child, std::size_t count,
                        const std::vector<ir::ColumnRef>* group_by)
        : child_(std::move(child)), count_(count), group_by_(group_by), remaining_(count) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        if (count_ == 0 && group_by_->empty()) {
            done_ = true;
            return std::optional<Chunk>{};
        }

        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                done_ = true;
                return std::optional<Chunk>{};
            }

            Chunk chunk = std::move(*chunk_res.value());
            if (count_ == 0) {
                done_ = true;
                Table t = chunk_to_table(std::move(chunk));
                std::vector<std::size_t> idx;
                return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
            }

            if (group_by_->empty()) {
                return take_global_rows(std::move(chunk));
            }

            auto filtered = take_grouped_rows(std::move(chunk));
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->has_value()) {
                continue;
            }
            return filtered;
        }
    }

   private:
    auto take_global_rows(Chunk chunk) -> std::expected<std::optional<Chunk>, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows <= remaining_) {
            remaining_ -= rows;
            if (remaining_ == 0) {
                done_ = true;
            }
            return std::optional<Chunk>{std::move(chunk)};
        }

        Table t = chunk_to_table(std::move(chunk));
        std::vector<std::size_t> idx(remaining_);
        std::iota(idx.begin(), idx.end(), std::size_t{0});
        remaining_ = 0;
        done_ = true;
        return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
    }

    auto take_grouped_rows(Chunk chunk) -> std::expected<std::optional<Chunk>, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return std::optional<Chunk>{std::move(chunk)};
        }

        Table t = chunk_to_table(std::move(chunk));
        std::vector<std::size_t> idx;
        idx.reserve(std::min(rows, count_ * std::max<std::size_t>(1, group_by_->size())));

        for (std::size_t row = 0; row < rows; ++row) {
            Key key;
            key.values.reserve(group_by_->size());
            for (const auto& ref : *group_by_) {
                const auto* column = t.find(ref.name);
                if (column == nullptr) {
                    return std::unexpected("head group-by column not found: " + ref.name +
                                           " (available: " + format_columns(t) + ")");
                }
                key.values.push_back(scalar_from_column(*column, row));
            }
            auto& seen = seen_counts_[key];
            if (seen >= count_) {
                continue;
            }
            ++seen;
            idx.push_back(row);
        }

        if (idx.empty()) {
            return std::optional<Chunk>{};
        }
        if (idx.size() == rows) {
            return std::optional<Chunk>{table_to_chunk(std::move(t))};
        }
        return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
    }

    OperatorPtr child_;
    std::size_t count_;
    const std::vector<ir::ColumnRef>* group_by_;
    std::size_t remaining_;
    bool done_ = false;
    robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> seen_counts_;
};

auto compare_scalar_for_order(const ScalarValue& lhs, const ScalarValue& rhs) -> int {
    return std::visit(
        [](const auto& l, const auto& r) -> int {
            using L = std::decay_t<decltype(l)>;
            using R = std::decay_t<decltype(r)>;
            if constexpr (std::is_same_v<L, R>) {
                if (l < r) {
                    return -1;
                }
                if (r < l) {
                    return 1;
                }
                return 0;
            } else {
                invariant_violation("compare_scalar_for_order: mismatched scalar types");
            }
        },
        lhs, rhs);
}

auto scalar_at_for_order(const ColumnValue& col, std::size_t row) -> ScalarValue {
    return std::visit(
        [row](const auto& c) -> ScalarValue {
            using T = typename std::decay_t<decltype(c)>::value_type;
            if constexpr (std::is_same_v<T, bool>) {
                return static_cast<std::int64_t>(c[row] ? 1 : 0);
            } else if constexpr (std::is_same_v<T, std::string_view>) {
                return std::string(c[row]);
            } else {
                return c[row];
            }
        },
        col);
}

auto evaluate_rank_column(const Table& input, const ir::RankExpr& rank,
                          const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<ComputedColumn, std::string> {
    const std::size_t rows = input.rows();
    auto order_keys = ordering_keys_for_table(input, rank.order_keys);
    if (order_keys.empty()) {
        return std::unexpected("rank(): expected at least one order key");
    }

    struct ResolvedKey {
        const ColumnEntry* entry = nullptr;
        bool ascending = true;
    };
    std::vector<ResolvedKey> resolved_keys;
    resolved_keys.reserve(order_keys.size());
    for (const auto& key : order_keys) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("rank(): order column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        resolved_keys.push_back(ResolvedKey{.entry = entry, .ascending = key.ascending});
    }

    std::vector<const ColumnEntry*> group_entries;
    group_entries.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("rank(): group column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        group_entries.push_back(entry);
    }

    auto is_null_row_for_keys = [&](std::size_t row) -> bool {
        return std::ranges::any_of(resolved_keys, [row](const auto& key) {
            return key.entry->validity.has_value() && !(*key.entry->validity)[row];
        });
    };

    auto same_group = [&](std::size_t lhs, std::size_t rhs) -> bool {
        auto entry_matches = [&](const ColumnEntry* entry) -> bool {
            if (entry->validity.has_value()) {
                const bool lv = (*entry->validity)[lhs];
                const bool rv = (*entry->validity)[rhs];
                if (lv != rv) {
                    return false;
                }
                if (!lv) {
                    return true;
                }
            }

            return compare_scalar_for_order(scalar_at_for_order(*entry->column, lhs),
                                            scalar_at_for_order(*entry->column, rhs)) == 0;
        };

        return std::ranges::all_of(group_entries, entry_matches);
    };

    auto compare_rows = [&](std::size_t lhs, std::size_t rhs) -> bool {
        const bool lhs_null = is_null_row_for_keys(lhs);
        const bool rhs_null = is_null_row_for_keys(rhs);
        if (lhs_null || rhs_null) {
            if (lhs_null != rhs_null) {
                if (rank.na_option == ir::RankNaOption::Top) {
                    return lhs_null;
                }
                if (rank.na_option == ir::RankNaOption::Bottom) {
                    return !lhs_null;
                }
                return lhs < rhs;
            }
            return lhs < rhs;
        }
        for (const auto& key : resolved_keys) {
            int cmp = compare_scalar_for_order(scalar_at_for_order(*key.entry->column, lhs),
                                               scalar_at_for_order(*key.entry->column, rhs));
            if (cmp != 0) {
                return key.ascending ? (cmp < 0) : (cmp > 0);
            }
        }
        return lhs < rhs;
    };

    auto equal_rank_keys = [&](std::size_t lhs, std::size_t rhs) -> bool {
        const bool lhs_null = is_null_row_for_keys(lhs);
        const bool rhs_null = is_null_row_for_keys(rhs);
        if (lhs_null || rhs_null) {
            return lhs_null == rhs_null;
        }
        return std::ranges::all_of(resolved_keys, [lhs, rhs](const auto& key) {
            return compare_scalar_for_order(scalar_at_for_order(*key.entry->column, lhs),
                                            scalar_at_for_order(*key.entry->column, rhs)) == 0;
        });
    };

    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), 0);
    std::stable_sort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
        if (!group_entries.empty()) {
            const bool same = same_group(lhs, rhs);
            // NOLINTNEXTLINE(readability-suspicious-call-argument)
            const bool reverse_same = same_group(rhs, lhs);
            if (!same || !reverse_same) {
                for (const auto* entry : group_entries) {
                    if (entry->validity.has_value()) {
                        const bool lv = (*entry->validity)[lhs];
                        const bool rv = (*entry->validity)[rhs];
                        if (lv != rv) {
                            return lv < rv;
                        }
                        if (!lv) {
                            continue;
                        }
                    }
                    int cmp = compare_scalar_for_order(scalar_at_for_order(*entry->column, lhs),
                                                       scalar_at_for_order(*entry->column, rhs));
                    if (cmp != 0) {
                        return cmp < 0;
                    }
                }
            }
        }
        return compare_rows(lhs, rhs);
    });

    std::vector<double> rank_values(rows, 0.0);
    ValidityBitmap validity(rows, true);

    std::size_t pos = 0;
    while (pos < rows) {
        std::size_t group_end = pos + 1;
        while (group_end < rows && same_group(idx[pos], idx[group_end])) {
            ++group_end;
        }

        std::size_t dense_rank = 1;
        std::size_t ordinal = 1;
        std::size_t i = pos;
        while (i < group_end) {
            std::size_t tie_end = i + 1;
            while (tie_end < group_end && equal_rank_keys(idx[i], idx[tie_end])) {
                ++tie_end;
            }

            const bool null_tie = is_null_row_for_keys(idx[i]);
            double assigned = 0.0;
            if (null_tie && rank.na_option == ir::RankNaOption::Keep) {
                for (std::size_t k = i; k < tie_end; ++k) {
                    validity.set(idx[k], false);
                }
            } else {
                switch (rank.method) {
                    case ir::RankMethod::Average: {
                        const double first_rank = static_cast<double>(ordinal);
                        const double last_rank = static_cast<double>(ordinal + (tie_end - i) - 1);
                        assigned = (first_rank + last_rank) / 2.0;
                        break;
                    }
                    case ir::RankMethod::Min:
                    case ir::RankMethod::Dense:
                        assigned = static_cast<double>(
                            rank.method == ir::RankMethod::Dense ? dense_rank : ordinal);
                        break;
                    case ir::RankMethod::Max:
                        assigned = static_cast<double>(ordinal + (tie_end - i) - 1);
                        break;
                    case ir::RankMethod::First:
                        break;
                }
                if (rank.method == ir::RankMethod::First) {
                    for (std::size_t k = i; k < tie_end; ++k) {
                        double value = static_cast<double>(ordinal + (k - i));
                        rank_values[idx[k]] =
                            rank.pct ? value / static_cast<double>(group_end - pos) : value;
                    }
                } else {
                    if (rank.pct) {
                        assigned /= static_cast<double>(group_end - pos);
                    }
                    for (std::size_t k = i; k < tie_end; ++k) {
                        rank_values[idx[k]] = assigned;
                    }
                }
            }

            ordinal += (tie_end - i);
            if (!null_tie || rank.na_option != ir::RankNaOption::Keep) {
                ++dense_rank;
            }
            i = tie_end;
        }

        pos = group_end;
    }

    const bool integral = !rank.pct && rank.method != ir::RankMethod::Average;
    if (integral) {
        Column<std::int64_t> out;
        out.reserve(rows);
        for (double value : rank_values) {
            out.push_back(static_cast<std::int64_t>(value));
        }
        ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
        if (rank.na_option == ir::RankNaOption::Keep) {
            result.validity = std::move(validity);
        }
        return result;
    }

    Column<double> out;
    out.reserve(rows);
    for (double value : rank_values) {
        out.push_back(value);
    }
    ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
    if (rank.na_option == ir::RankNaOption::Keep) {
        result.validity = std::move(validity);
    }
    return result;
}

/// Chunk-preserving `Order`: buffers incoming chunks, validates sortedness
/// on-the-fly, and at EOF either emits the buffered chunks unchanged (with
/// `ordering` stamped) or falls back to `order_table` on the concatenated
/// input. Downstream operators see a chunked stream either way — the win
/// over the materializing path is avoiding the final big concat+sort when
/// the input is already ordered, plus preserving chunk shape for whatever
/// runs next.
class ChunkedOrderOperator final : public Operator {
   public:
    ChunkedOrderOperator(OperatorPtr child, const std::vector<ir::OrderKey>* keys)
        : child_(std::move(child)), keys_(keys) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (mode_ == Mode::Ingest) {
            auto drained = drain_and_check();
            if (!drained.has_value()) {
                return std::unexpected(std::move(drained.error()));
            }
        }
        if (mode_ == Mode::EmitSorted) {
            if (emit_idx_ >= buffered_.size()) {
                mode_ = Mode::Done;
                return std::optional<Chunk>{};
            }
            Chunk out = std::move(buffered_[emit_idx_++]);
            out.ordering = resolved_keys_;
            return std::optional<Chunk>{std::move(out)};
        }
        if (mode_ == Mode::EmitUnsorted) {
            mode_ = Mode::Done;
            if (!sorted_result_.has_value()) {
                return std::optional<Chunk>{};
            }
            Chunk out = table_to_chunk(std::move(*sorted_result_));
            sorted_result_.reset();
            return std::optional<Chunk>{std::move(out)};
        }
        return std::optional<Chunk>{};
    }

   private:
    enum class Mode : std::uint8_t { Ingest, EmitSorted, EmitUnsorted, Done };

    auto drain_and_check() -> std::expected<void, std::string> {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());
            if (chunk.rows() == 0) {
                continue;
            }
            if (resolved_keys_.empty()) {
                if (chunk.time_index.has_value()) {
                    if (keys_->size() != 1 || (*keys_)[0].name != *chunk.time_index ||
                        !(*keys_)[0].ascending) {
                        return std::unexpected(
                            "order on TimeFrame must be by time index ascending");
                    }
                }
                auto resolved = resolve_keys(chunk);
                if (!resolved.has_value()) {
                    return std::unexpected(std::move(resolved.error()));
                }
                resolved_keys_ = std::move(*resolved);
            }
            if (still_sorted_) {
                auto ok = validate_chunk(chunk);
                if (!ok.has_value()) {
                    return std::unexpected(std::move(ok.error()));
                }
                if (!*ok) {
                    still_sorted_ = false;
                }
            }
            buffered_.push_back(std::move(chunk));
        }

        if (buffered_.empty()) {
            mode_ = Mode::Done;
            return {};
        }
        if (still_sorted_) {
            mode_ = Mode::EmitSorted;
            return {};
        }
        // Fallback: concat everything into one Table and sort.
        Table concat;
        auto concatenated = concat_buffered(concat);
        buffered_.clear();
        if (!concatenated.has_value()) {
            return std::unexpected(std::move(concatenated.error()));
        }
        auto sorted = order_table(concat, *keys_);
        if (!sorted.has_value()) {
            return std::unexpected(std::move(sorted.error()));
        }
        sorted_result_ = std::move(*sorted);
        mode_ = Mode::EmitUnsorted;
        return {};
    }

    auto resolve_keys(const Chunk& chunk) -> std::expected<std::vector<ir::OrderKey>, std::string> {
        if (!keys_->empty()) {
            return *keys_;
        }
        std::vector<ir::OrderKey> resolved;
        resolved.reserve(chunk.columns.size());
        for (const auto& entry : chunk.columns) {
            resolved.push_back(ir::OrderKey{.name = entry.name, .ascending = true});
        }
        return resolved;
    }

    // Returns true if the chunk is internally sorted on the resolved keys and
    // its first row is ordered correctly relative to the last row of the
    // previously buffered chunk (if any).
    auto validate_chunk(const Chunk& chunk) -> std::expected<bool, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return true;
        }
        // Index of each key within this chunk's column list.
        std::vector<std::size_t> key_idx;
        key_idx.reserve(resolved_keys_.size());
        for (const auto& key : resolved_keys_) {
            std::size_t found = chunk.columns.size();
            for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
                if (chunk.columns[i].name == key.name) {
                    found = i;
                    break;
                }
            }
            if (found == chunk.columns.size()) {
                return std::unexpected("order column not found in chunk: " + key.name);
            }
            key_idx.push_back(found);
        }

        // Boundary check against last row of previous chunk.
        if (!prev_last_.empty()) {
            auto cmp = compare_keys_cross(prev_last_, chunk, 0, key_idx);
            if (cmp > 0) {
                return false;
            }
        }

        // Internal sort check. Single-key fast path uses typed column access
        // to avoid the per-row scalar_from_column + variant dispatch cost
        // (which dominates pre-sorted runs: a 2M-row scan goes from ~10 ms
        // with scalars to ~0.5 ms with typed compare).
        if (resolved_keys_.size() == 1) {
            const bool asc = resolved_keys_[0].ascending;
            const auto& col_var = *chunk.columns[key_idx[0]].column;
            bool sorted = true;
            bool handled = false;
            std::visit(
                [&](const auto& col) {
                    using ColT = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            bool bad = asc ? (col[i].nanos < col[i - 1].nanos)
                                           : (col[i].nanos > col[i - 1].nanos);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            bool bad = asc ? (col[i].days < col[i - 1].days)
                                           : (col[i].days > col[i - 1].days);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>> ||
                                         std::is_same_v<ColT, Column<double>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            bool bad = asc ? (col[i] < col[i - 1]) : (col[i] > col[i - 1]);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    }
                },
                col_var);
            if (handled) {
                if (!sorted) {
                    return false;
                }
            } else {
                for (std::size_t r = 1; r < rows; ++r) {
                    if (compare_keys_within(chunk, r - 1, r, key_idx) > 0) {
                        return false;
                    }
                }
            }
        } else {
            for (std::size_t r = 1; r < rows; ++r) {
                if (compare_keys_within(chunk, r - 1, r, key_idx) > 0) {
                    return false;
                }
            }
        }

        // Snapshot last row for next boundary check.
        prev_last_.clear();
        prev_last_.reserve(resolved_keys_.size());
        for (std::size_t i = 0; i < resolved_keys_.size(); ++i) {
            prev_last_.push_back(scalar_from_column(*chunk.columns[key_idx[i]].column, rows - 1));
        }
        return true;
    }

    // Lexicographic comparison of two rows within the same chunk, honoring
    // per-key `ascending`. Returns >0 if lhs > rhs in the chosen order
    // (i.e. out-of-order), 0 if equal, <0 otherwise.
    auto compare_keys_within(const Chunk& chunk, std::size_t a, std::size_t b,
                             const std::vector<std::size_t>& key_idx) -> int {
        for (std::size_t i = 0; i < resolved_keys_.size(); ++i) {
            const auto& col = *chunk.columns[key_idx[i]].column;
            auto sa = scalar_from_column(col, a);
            auto sb = scalar_from_column(col, b);
            int c = compare_scalar_for_order(sa, sb);
            if (c != 0) {
                return resolved_keys_[i].ascending ? c : -c;
            }
        }
        return 0;
    }

    // Compare a cached row of scalars (previous chunk's last row) to a row of
    // the current chunk. Returns >0 iff cached > current (i.e. boundary
    // violates sort order).
    auto compare_keys_cross(const std::vector<ScalarValue>& cached, const Chunk& chunk,
                            std::size_t row, const std::vector<std::size_t>& key_idx) -> int {
        for (std::size_t i = 0; i < resolved_keys_.size(); ++i) {
            const auto& col = *chunk.columns[key_idx[i]].column;
            auto sb = scalar_from_column(col, row);
            int c = compare_scalar_for_order(cached[i], sb);
            if (c != 0) {
                return resolved_keys_[i].ascending ? c : -c;
            }
        }
        return 0;
    }

    auto concat_buffered(Table& out) -> std::expected<void, std::string> {
        Chunk first = std::move(buffered_.front());
        out.columns = std::move(first.columns);
        for (std::size_t i = 0; i < out.columns.size(); ++i) {
            out.index[out.columns[i].name] = i;
        }
        const std::size_t n_cols = out.columns.size();
        for (std::size_t bi = 1; bi < buffered_.size(); ++bi) {
            Chunk& chunk = buffered_[bi];
            if (chunk.columns.size() != n_cols) {
                return std::unexpected("order: chunk schema mismatch (column count)");
            }
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != out.columns[i].name) {
                    return std::unexpected("order: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != out.columns[i].column->index()) {
                    return std::unexpected("order: chunk schema mismatch (column type)");
                }
                auto& dst_col = out.mutable_column(i);
                std::visit(
                    [&](auto& dst) {
                        using Col = std::decay_t<decltype(dst)>;
                        auto& src = std::get<Col>(*chunk.columns[i].column);
                        dst.reserve(dst.size() + src.size());
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_code(src.code_at(r));
                            }
                        } else {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_back(src[r]);
                            }
                        }
                    },
                    dst_col);
            }
        }
        return {};
    }

    OperatorPtr child_;
    const std::vector<ir::OrderKey>* keys_;
    Mode mode_ = Mode::Ingest;
    std::vector<Chunk> buffered_;
    std::vector<ir::OrderKey> resolved_keys_;
    std::vector<ScalarValue> prev_last_;
    std::size_t emit_idx_ = 0;
    std::optional<Table> sorted_result_;
    bool still_sorted_ = true;
};

/// Chunk-preserving `as_timeframe`: buffers incoming chunks, promotes an
/// `Int` time column to `Timestamp` per chunk, validates ascending sortedness
/// on the fly, and either re-emits the buffered chunks with `time_index`
/// stamped (fast path: no sort) or falls back to concat + `order_table`
/// (slow path: SPEC §9.1 says as_timeframe must sort if unsorted, so the
/// full table materialization is unavoidable for that branch).
///
/// The win is real only when the input is already sorted on the time column,
/// which is the overwhelmingly common TimeFrame shape (CSV/parquet ingest,
/// streaming sources). For those we skip the sort entirely and let downstream
/// operators see a chunked TimeFrame.
class ChunkedAsTimeframeOperator final : public Operator {
   public:
    ChunkedAsTimeframeOperator(OperatorPtr child, std::string column)
        : child_(std::move(child)), column_(std::move(column)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (mode_ == Mode::Ingest) {
            auto drained = drain();
            if (!drained.has_value()) {
                return std::unexpected(std::move(drained.error()));
            }
        }
        if (mode_ == Mode::EmitBuffered) {
            if (emit_idx_ >= buffered_.size()) {
                mode_ = Mode::Done;
                return std::optional<Chunk>{};
            }
            Chunk out = std::move(buffered_[emit_idx_++]);
            out.time_index = column_;
            out.ordering = std::vector<ir::OrderKey>{{.name = column_, .ascending = true}};
            return std::optional<Chunk>{std::move(out)};
        }
        if (mode_ == Mode::EmitSorted) {
            mode_ = Mode::Done;
            if (!sorted_result_.has_value()) {
                return std::optional<Chunk>{};
            }
            Chunk out = table_to_chunk(std::move(*sorted_result_));
            sorted_result_.reset();
            return std::optional<Chunk>{std::move(out)};
        }
        return std::optional<Chunk>{};
    }

   private:
    enum class Mode : std::uint8_t { Ingest, EmitBuffered, EmitSorted, Done };

    auto drain() -> std::expected<void, std::string> {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());
            if (chunk.rows() == 0) {
                continue;
            }

            // Locate the time column in this chunk.
            std::size_t col_idx = chunk.columns.size();
            for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
                if (chunk.columns[i].name == column_) {
                    col_idx = i;
                    break;
                }
            }
            if (col_idx == chunk.columns.size()) {
                return std::unexpected("as_timeframe: column '" + column_ + "' not found");
            }

            // On the first chunk, decide whether Int promotion is needed and
            // reject invalid types.
            if (!type_checked_) {
                const auto& col = *chunk.columns[col_idx].column;
                if (std::holds_alternative<Column<std::int64_t>>(col)) {
                    needs_promotion_ = true;
                } else if (!std::holds_alternative<Column<Timestamp>>(col) &&
                           !std::holds_alternative<Column<Date>>(col)) {
                    return std::unexpected("as_timeframe: column '" + column_ +
                                           "' must be Timestamp, Date, or Int");
                }
                type_checked_ = true;
            }

            // Promote Int → Timestamp per chunk (cheap — same row count, same
            // layout — and keeps downstream operators seeing Timestamp).
            if (needs_promotion_) {
                const auto& ints = std::get<Column<std::int64_t>>(*chunk.columns[col_idx].column);
                Column<Timestamp> ts_col;
                ts_col.reserve(ints.size());
                for (auto v : ints) {
                    ts_col.push_back(Timestamp{v});
                }
                chunk.replace_column(col_idx, ColumnValue{std::move(ts_col)});
            }

            if (still_sorted_) {
                auto ok = validate_chunk(chunk, col_idx);
                if (!ok.has_value()) {
                    return std::unexpected(std::move(ok.error()));
                }
                if (!*ok) {
                    still_sorted_ = false;
                }
            }

            buffered_.push_back(std::move(chunk));
        }

        if (buffered_.empty()) {
            mode_ = Mode::Done;
            return {};
        }
        if (still_sorted_) {
            mode_ = Mode::EmitBuffered;
            return {};
        }

        // Fallback: concat all buffered chunks and run the full sort. SPEC
        // §9.1 requires as_timeframe to sort its input when unsorted, so this
        // materialization is intentional.
        Table concat;
        Chunk first = std::move(buffered_.front());
        concat.columns = std::move(first.columns);
        for (std::size_t i = 0; i < concat.columns.size(); ++i) {
            concat.index[concat.columns[i].name] = i;
        }
        const std::size_t n_cols = concat.columns.size();
        for (std::size_t bi = 1; bi < buffered_.size(); ++bi) {
            Chunk& chunk = buffered_[bi];
            if (chunk.columns.size() != n_cols) {
                return std::unexpected("as_timeframe: chunk schema mismatch (column count)");
            }
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != concat.columns[i].name) {
                    return std::unexpected("as_timeframe: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != concat.columns[i].column->index()) {
                    return std::unexpected("as_timeframe: chunk schema mismatch (column type)");
                }
                auto& dst_col = concat.mutable_column(i);
                std::visit(
                    [&](auto& dst) {
                        using Col = std::decay_t<decltype(dst)>;
                        auto& src = std::get<Col>(*chunk.columns[i].column);
                        dst.reserve(dst.size() + src.size());
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_code(src.code_at(r));
                            }
                        } else {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_back(src[r]);
                            }
                        }
                    },
                    dst_col);
            }
        }
        buffered_.clear();

        auto sorted = order_table(concat, {{.name = column_, .ascending = true}});
        if (!sorted.has_value()) {
            return std::unexpected(std::move(sorted.error()));
        }
        sorted->time_index = column_;
        normalize_time_index(*sorted);
        sorted_result_ = std::move(*sorted);
        mode_ = Mode::EmitSorted;
        return {};
    }

    // Returns true if the chunk's time column is ascending internally and its
    // first row is ≥ the last row of the previously buffered chunk. Typed
    // dispatch mirrors ChunkedOrderOperator's single-key fast path — the whole
    // point of this operator is to avoid a big sort, so the validation must
    // itself not be expensive.
    auto validate_chunk(const Chunk& chunk, std::size_t col_idx)
        -> std::expected<bool, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return true;
        }
        const auto& col_var = *chunk.columns[col_idx].column;
        bool sorted = true;
        bool handled = false;
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    handled = true;
                    if (prev_last_nanos_.has_value() && col[0].nanos < *prev_last_nanos_) {
                        sorted = false;
                        return;
                    }
                    for (std::size_t i = 1; i < rows; ++i) {
                        if (col[i].nanos < col[i - 1].nanos) {
                            sorted = false;
                            return;
                        }
                    }
                    prev_last_nanos_ = col[rows - 1].nanos;
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    handled = true;
                    if (prev_last_days_.has_value() && col[0].days < *prev_last_days_) {
                        sorted = false;
                        return;
                    }
                    for (std::size_t i = 1; i < rows; ++i) {
                        if (col[i].days < col[i - 1].days) {
                            sorted = false;
                            return;
                        }
                    }
                    prev_last_days_ = col[rows - 1].days;
                }
            },
            col_var);
        if (!handled) {
            // Type already validated on first chunk; downstream schema
            // guarantees stability. Reaching here means inconsistent schema.
            return std::unexpected("as_timeframe: unexpected time column type");
        }
        return sorted;
    }

    OperatorPtr child_;
    std::string column_;
    Mode mode_ = Mode::Ingest;
    std::vector<Chunk> buffered_;
    std::optional<std::int64_t> prev_last_nanos_;
    std::optional<std::int32_t> prev_last_days_;
    std::size_t emit_idx_ = 0;
    std::optional<Table> sorted_result_;
    bool type_checked_ = false;
    bool needs_promotion_ = false;
    bool still_sorted_ = true;
};

class ChunkedOrderedLimitOperator final : public Operator {
   public:
    enum class KeepMode : std::uint8_t { First, Last };

    ChunkedOrderedLimitOperator(OperatorPtr child, const std::vector<ir::OrderKey>* keys,
                                std::size_t count, const std::vector<ir::ColumnRef>* group_by,
                                KeepMode keep_mode)
        : child_(std::move(child)),
          keys_(keys),
          count_(count),
          group_by_(group_by),
          keep_mode_(keep_mode) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }

        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }

            Table t = chunk_to_table(std::move(*chunk_res.value()));
            if (!empty_template_.has_value()) {
                std::vector<std::size_t> idx;
                empty_template_ = gather_rows(t, idx);
            }
            auto err = process_chunk(t);
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
        }

        emitted_ = true;
        if (!empty_template_.has_value()) {
            return std::optional<Chunk>{};
        }
        return std::optional<Chunk>{table_to_chunk(build_output())};
    }

   private:
    struct RowSnapshot {
        std::vector<ScalarValue> values;
        std::vector<uint8_t> valid;
    };

    struct Entry {
        Key key;
        std::size_t sequence = 0;
        RowSnapshot row;
    };

    struct GroupState {
        std::vector<Entry> heap;
    };

    static auto snapshot_row(const Table& chunk, std::size_t row) -> RowSnapshot {
        RowSnapshot snapshot;
        snapshot.values.reserve(chunk.columns.size());
        snapshot.valid.reserve(chunk.columns.size());
        for (const auto& column : chunk.columns) {
            snapshot.values.push_back(scalar_from_column(*column.column, row));
            snapshot.valid.push_back(
                column.validity.has_value() ? static_cast<uint8_t>((*column.validity)[row]) : 1U);
        }
        return snapshot;
    }

    auto row_comes_first(const Entry& lhs, const Entry& rhs) const -> bool {
        for (std::size_t i = 0; i < lhs.key.values.size(); ++i) {
            int cmp = compare_scalar_for_order(lhs.key.values[i], rhs.key.values[i]);
            if (cmp == 0) {
                continue;
            }
            return (*keys_)[i].ascending ? (cmp < 0) : (cmp > 0);
        }
        return lhs.sequence < rhs.sequence;
    }

    auto entry_preferred(const Entry& lhs, const Entry& rhs) const -> bool {
        return keep_mode_ == KeepMode::First ? row_comes_first(lhs, rhs)
                                             // NOLINTNEXTLINE(readability-suspicious-call-argument)
                                             : row_comes_first(rhs, lhs);
    }

    template <typename T>
    auto single_key_better(const T& lhs, std::size_t lhs_sequence, const Entry& rhs,
                           bool ascending) const -> bool {
        const auto* rhs_key = std::get_if<T>(&rhs.key.values.front());
        if (rhs_key == nullptr) {
            invariant_violation("ChunkedOrderedLimitOperator: single-key type mismatch");
        }
        if (lhs == *rhs_key) {
            return keep_mode_ == KeepMode::First ? (lhs_sequence < rhs.sequence)
                                                 : (rhs.sequence < lhs_sequence);
        }
        const bool lhs_first = ascending ? (lhs < *rhs_key) : (lhs > *rhs_key);
        return keep_mode_ == KeepMode::First ? lhs_first : !lhs_first;
    }

    auto push_entry(Entry entry) -> void { push_entry(heap_, std::move(entry)); }

    auto push_entry(std::vector<Entry>& heap, Entry entry) const -> void {
        if (heap.size() < count_) {
            heap.push_back(std::move(entry));
            std::push_heap(heap.begin(), heap.end(),
                           [&](const Entry& a, const Entry& b) { return entry_preferred(a, b); });
            return;
        }

        std::pop_heap(heap.begin(), heap.end(),
                      [&](const Entry& a, const Entry& b) { return entry_preferred(a, b); });
        heap.back() = std::move(entry);
        std::push_heap(heap.begin(), heap.end(),
                       [&](const Entry& a, const Entry& b) { return entry_preferred(a, b); });
    }

    template <typename T>
    auto process_single_key_chunk(const Table& chunk, const Column<T>& key_column, bool ascending,
                                  const std::vector<const ColumnValue*>& group_columns)
        -> std::optional<std::string> {
        for (std::size_t row = 0; row < chunk.rows(); ++row) {
            const std::size_t sequence = next_sequence_++;
            const T& key = key_column[row];

            std::vector<Entry>* heap = &heap_;
            if (!group_by_->empty()) {
                Key group_key;
                group_key.values.reserve(group_columns.size());
                for (const auto* column : group_columns) {
                    group_key.values.push_back(scalar_from_column(*column, row));
                }
                heap = &group_heaps_[std::move(group_key)].heap;
            }

            if (heap->size() == count_ &&
                !single_key_better(key, sequence, heap->front(), ascending)) {
                continue;
            }

            Entry entry;
            entry.key.values.reserve(1);
            entry.key.values.emplace_back(key);
            entry.sequence = sequence;
            entry.row = snapshot_row(chunk, row);
            push_entry(*heap, std::move(entry));
        }
        return std::nullopt;
    }

    auto process_chunk(const Table& chunk) -> std::optional<std::string> {
        if (count_ == 0 || chunk.rows() == 0) {
            return std::nullopt;
        }

        std::vector<const ColumnValue*> group_columns;
        group_columns.reserve(group_by_->size());
        for (const auto& ref : *group_by_) {
            const auto* column = chunk.find(ref.name);
            if (column == nullptr) {
                return "head group-by column not found: " + ref.name +
                       " (available: " + format_columns(chunk) + ")";
            }
            group_columns.push_back(column);
        }

        std::vector<const ColumnValue*> key_columns;
        key_columns.reserve(keys_->size());
        for (const auto& key : *keys_) {
            const auto* column = chunk.find(key.name);
            if (column == nullptr) {
                return "order column not found: " + key.name +
                       " (available: " + format_columns(chunk) + ")";
            }
            key_columns.push_back(column);
        }

        if (keys_->size() == 1) {
            const bool ascending = keys_->front().ascending;
            const ColumnValue& key_column = *key_columns.front();
            if (const auto* col = std::get_if<Column<std::int64_t>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<double>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<bool>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<Date>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<Timestamp>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
        }

        for (std::size_t row = 0; row < chunk.rows(); ++row) {
            std::vector<Entry>* heap = &heap_;
            if (!group_by_->empty()) {
                Key group_key;
                group_key.values.reserve(group_columns.size());
                for (const auto* column : group_columns) {
                    group_key.values.push_back(scalar_from_column(*column, row));
                }
                heap = &group_heaps_[std::move(group_key)].heap;
            }

            Entry entry;
            entry.key.values.reserve(keys_->size());
            for (const auto* column : key_columns) {
                entry.key.values.push_back(scalar_from_column(*column, row));
            }
            entry.sequence = next_sequence_++;
            if (heap->size() == count_ && !entry_preferred(entry, heap->front())) {
                continue;
            }
            entry.row = snapshot_row(chunk, row);
            push_entry(*heap, std::move(entry));
        }
        return std::nullopt;
    }

    auto build_output() -> Table {
        std::vector<Entry> winners;
        if (group_by_->empty()) {
            winners = heap_;
        } else {
            for (auto& [_, state] : group_heaps_) {
                for (auto& entry : state.heap) {
                    winners.push_back(std::move(entry));
                }
            }
        }

        if (count_ == 0 || winners.empty()) {
            return empty_template_.value_or(Table{});
        }

        std::sort(winners.begin(), winners.end(),
                  [&](const Entry& a, const Entry& b) { return row_comes_first(a, b); });

        Table out = empty_template_.value_or(Table{});
        for (const auto& entry : winners) {
            for (std::size_t col = 0; col < out.columns.size(); ++col) {
                auto& out_col = out.mutable_column(col);
                append_scalar(out_col, entry.row.values[col]);
                if (entry.row.valid[col] == 0U) {
                    if (!out.columns[col].validity.has_value()) {
                        out.columns[col].validity = ValidityBitmap(column_size(out_col) - 1, true);
                    }
                    out.columns[col].validity->push_back(false);
                } else if (out.columns[col].validity.has_value()) {
                    out.columns[col].validity->push_back(true);
                }
            }
        }

        out.ordering = *keys_;
        normalize_time_index(out);
        return out;
    }

    OperatorPtr child_;
    const std::vector<ir::OrderKey>* keys_;
    std::size_t count_;
    const std::vector<ir::ColumnRef>* group_by_;
    KeepMode keep_mode_;
    bool emitted_ = false;
    std::size_t next_sequence_ = 0;
    std::vector<Entry> heap_;
    robin_hood::unordered_flat_map<Key, GroupState, KeyHash, KeyEq> group_heaps_;
    std::optional<Table> empty_template_;
};

class ChunkedDistinctOperator final : public Operator {
   public:
    explicit ChunkedDistinctOperator(OperatorPtr child) : child_(std::move(child)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }

            Table t = chunk_to_table(std::move(*chunk_res.value()));
            if (t.columns.empty()) {
                t.ordering.reset();
                t.time_index.reset();
                return std::optional<Chunk>{table_to_chunk(std::move(t))};
            }

            if (t.columns.size() == 1) {
                auto out = process_single_column(std::move(t));
                if (!out.has_value()) {
                    continue;
                }
                return std::optional<Chunk>{table_to_chunk(std::move(*out))};
            }

            const std::size_t rows = t.rows();
            std::vector<std::size_t> idx;
            idx.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                Key key;
                key.values.reserve(t.columns.size());
                for (const auto& entry : t.columns) {
                    key.values.push_back(scalar_from_column(*entry.column, row));
                }
                if (!seen_.insert(std::move(key)).second) {
                    continue;
                }
                idx.push_back(row);
            }

            if (idx.empty()) {
                continue;
            }

            t.ordering.reset();
            t.time_index.reset();
            if (idx.size() == rows) {
                return std::optional<Chunk>{table_to_chunk(std::move(t))};
            }
            return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
        }
    }

   private:
    template <typename T>
    auto gather_distinct_rows(Table t, robin_hood::unordered_flat_set<T>& seen,
                              const Column<T>& col) -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            if (!seen.insert(col[row]).second) {
                continue;
            }
            idx.push_back(row);
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        t.ordering.reset();
        t.time_index.reset();
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto gather_distinct_string_rows(Table t, const Column<std::string>& col)
        -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            std::string_view value = col[row];
            if (seen_strings_.contains(value)) {
                continue;
            }
            owned_strings_.emplace_back(value);
            seen_strings_.insert(std::string_view{owned_strings_.back()});
            idx.push_back(row);
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        t.ordering.reset();
        t.time_index.reset();
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto gather_distinct_categorical_rows(Table t, const Column<Categorical>& col)
        -> std::optional<Table> {
        const void* dict_id = static_cast<const void*>(col.dictionary_ptr().get());
        if (cat_dictionary_id_ == nullptr || cat_dictionary_id_ == dict_id) {
            cat_dictionary_id_ = dict_id;
            const std::size_t rows = t.rows();
            std::vector<std::size_t> idx;
            idx.reserve(rows);
            const auto* codes = col.codes_data();
            for (std::size_t row = 0; row < rows; ++row) {
                if (!seen_cat_codes_.insert(codes[row]).second) {
                    continue;
                }
                idx.push_back(row);
            }
            if (idx.empty()) {
                return std::nullopt;
            }
            t.ordering.reset();
            t.time_index.reset();
            if (idx.size() == rows) {
                return t;
            }
            return gather_rows(t, idx);
        }

        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            std::string_view value = col[row];
            if (seen_strings_.contains(value)) {
                continue;
            }
            owned_strings_.emplace_back(value);
            seen_strings_.insert(std::string_view{owned_strings_.back()});
            idx.push_back(row);
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        t.ordering.reset();
        t.time_index.reset();
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto process_single_column(Table t) -> std::optional<Table> {
        const ColumnValue& column = *t.columns.front().column;
        if (const auto* col = std::get_if<Column<std::int64_t>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_i64_, *col);
        }
        if (const auto* col = std::get_if<Column<double>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_f64_, *col);
        }
        if (const auto* col = std::get_if<Column<bool>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_bool_, *col);
        }
        if (const auto* col = std::get_if<Column<Date>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_date_, *col);
        }
        if (const auto* col = std::get_if<Column<Timestamp>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_timestamp_, *col);
        }
        if (const auto* col = std::get_if<Column<std::string>>(&column)) {
            return gather_distinct_string_rows(std::move(t), *col);
        }
        if (const auto* col = std::get_if<Column<Categorical>>(&column)) {
            return gather_distinct_categorical_rows(std::move(t), *col);
        }
        return std::nullopt;
    }

    OperatorPtr child_;
    robin_hood::unordered_flat_set<Key, KeyHash, KeyEq> seen_;
    robin_hood::unordered_flat_set<std::int64_t> seen_i64_;
    robin_hood::unordered_flat_set<double> seen_f64_;
    robin_hood::unordered_flat_set<bool> seen_bool_;
    robin_hood::unordered_flat_set<Date> seen_date_;
    robin_hood::unordered_flat_set<Timestamp> seen_timestamp_;
    robin_hood::unordered_flat_set<Column<Categorical>::code_type> seen_cat_codes_;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> seen_strings_;
    std::deque<std::string> owned_strings_;
    const void* cat_dictionary_id_ = nullptr;
};

class ChunkedSemiAntiJoinOperator final : public Operator {
   public:
    ChunkedSemiAntiJoinOperator(OperatorPtr left, Table right, ir::JoinKind kind,
                                const std::vector<std::string>* keys)
        : left_(std::move(left)), right_(std::move(right)), kind_(kind), keys_(keys) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!initialized_) {
            auto err = initialize();
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
            initialized_ = true;
        }

        while (true) {
            auto chunk_res = left_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }

            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_chunk(std::move(t));
            if (!filtered.has_value()) {
                continue;
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*filtered))};
        }
    }

   private:
    auto initialize() -> std::optional<std::string> {
        if (keys_->size() != 1) {
            return "ChunkedSemiAntiJoinOperator only supports single-key joins";
        }
        if (right_.columns.empty()) {
            return std::nullopt;
        }
        const ColumnValue* key = right_.find(keys_->front());
        if (key == nullptr) {
            return "join key not found in right table: " + keys_->front();
        }

        if (const auto* col = std::get_if<Column<std::int64_t>>(key)) {
            right_kind_ = ExprType::Int;
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_i64_.insert((*col)[row]);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<double>>(key)) {
            right_kind_ = ExprType::Double;
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_f64_.insert((*col)[row]);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<bool>>(key)) {
            right_kind_ = ExprType::Bool;
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_bool_.insert((*col)[row]);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Date>>(key)) {
            right_kind_ = ExprType::Date;
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_date_.insert((*col)[row]);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Timestamp>>(key)) {
            right_kind_ = ExprType::Timestamp;
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_timestamp_.insert((*col)[row]);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Categorical>>(key)) {
            right_kind_ = ExprType::String;
            right_cat_dictionary_id_ = static_cast<const void*>(col->dictionary_ptr().get());
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_cat_codes_.insert(col->code_at(row));
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<std::string>>(key)) {
            right_kind_ = ExprType::String;
            for (std::size_t row = 0; row < col->size(); ++row) {
                owned_strings_.emplace_back((*col)[row]);
                right_strings_.insert(std::string_view{owned_strings_.back()});
            }
            return std::nullopt;
        }
        return "ChunkedSemiAntiJoinOperator: unsupported key type";
    }

    template <typename Pred>
    auto filter_rows(Table t, Pred pred) -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            if (pred(row)) {
                idx.push_back(row);
            }
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto filter_chunk(Table t) -> std::optional<Table> {
        const ColumnValue* key = t.find(keys_->front());
        if (key == nullptr) {
            return std::nullopt;
        }
        const bool keep_matches = (kind_ == ir::JoinKind::Semi);

        if (right_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_i64_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_f64_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_bool_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_date_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_timestamp_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }

        if (const auto* col = std::get_if<Column<Categorical>>(key);
            col != nullptr &&
            static_cast<const void*>(col->dictionary_ptr().get()) == right_cat_dictionary_id_) {
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_cat_codes_.contains(col->code_at(row));
                return keep_matches ? match : !match;
            });
        }

        if (const auto* col = std::get_if<Column<Categorical>>(key)) {
            const void* left_dict_id = static_cast<const void*>(col->dictionary_ptr().get());
            if (left_cat_dictionary_id_ != left_dict_id) {
                left_cat_dictionary_id_ = left_dict_id;
                left_cat_matches_.assign(col->dictionary().size(), uint8_t{0});
                const auto& dict = col->dictionary();
                for (std::size_t i = 0; i < dict.size(); ++i) {
                    left_cat_matches_[i] =
                        static_cast<uint8_t>(right_strings_.contains(std::string_view{dict[i]}));
                }
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const auto code = static_cast<std::size_t>(col->code_at(row));
                const bool match = left_cat_matches_[code] != 0U;
                return keep_matches ? match : !match;
            });
        }
        if (const auto* col = std::get_if<Column<std::string>>(key)) {
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_strings_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        return std::nullopt;
    }

    OperatorPtr left_;
    Table right_;
    ir::JoinKind kind_;
    const std::vector<std::string>* keys_;
    bool initialized_ = false;
    ExprType right_kind_ = ExprType::Int;

    robin_hood::unordered_flat_set<std::int64_t> right_i64_;
    robin_hood::unordered_flat_set<double> right_f64_;
    robin_hood::unordered_flat_set<bool> right_bool_;
    robin_hood::unordered_flat_set<Date> right_date_;
    robin_hood::unordered_flat_set<Timestamp> right_timestamp_;
    robin_hood::unordered_flat_set<Column<Categorical>::code_type> right_cat_codes_;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> right_strings_;
    std::deque<std::string> owned_strings_;
    const void* right_cat_dictionary_id_ = nullptr;
    const void* left_cat_dictionary_id_ = nullptr;
    std::vector<uint8_t> left_cat_matches_;
};

/// Inner hash join for single-key no-predicate joins.
///
/// Two execution modes:
/// - Stream: right is small (<= kStreamRightThreshold). Build a chained
///   hash index on the materialized right, then probe each left chunk
///   streamed from the child. Matches the classic star-join shape.
/// - Swapped: right is large and n_left < n_right. Materialize left,
///   build the hash index on left, iterate right rows in two phases to
///   emit output in left-row order (baseline's `build_indices_from_right_scan`
///   equivalent). Much better cache behavior when the smaller side fits.
///
/// Name conflicts are resolved with the same `_right` suffix rule as
/// `join_table_impl`.
class ChunkedInnerJoinOperator final : public Operator {
   public:
    ChunkedInnerJoinOperator(OperatorPtr left, Table right, const std::vector<std::string>* keys)
        : left_(std::move(left)), right_(std::move(right)), keys_(keys) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!initialized_) {
            auto err = initialize();
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
            initialized_ = true;
        }

        if (mode_ == Mode::Swapped) {
            if (swapped_emitted_) {
                return std::optional<Chunk>{};
            }
            swapped_emitted_ = true;
            auto out = emit_swapped();
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0) {
                return std::optional<Chunk>{};
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*out))};
        }

        while (true) {
            Table left_chunk;
            if (use_materialized_left_) {
                if (left_materialized_drained_) {
                    return std::optional<Chunk>{};
                }
                left_materialized_drained_ = true;
                left_chunk = std::move(*left_materialized_);
                left_materialized_.reset();
            } else {
                auto chunk_res = left_->next();
                if (!chunk_res.has_value()) {
                    return std::unexpected(std::move(chunk_res.error()));
                }
                if (!chunk_res.value().has_value()) {
                    return std::optional<Chunk>{};
                }
                left_chunk = chunk_to_table(std::move(*chunk_res.value()));
            }
            auto out = probe_chunk_against_right(std::move(left_chunk));
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0) {
                continue;
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*out))};
        }
    }

   private:
    enum class Mode : std::uint8_t { Stream, Swapped };

    static constexpr std::size_t kNil = std::numeric_limits<std::size_t>::max();
    // Build-on-right is preferred when right is small enough that probing
    // it from streaming left chunks is cache-friendly. Above this, we
    // materialize left to pick the smaller build side.
    static constexpr std::size_t kStreamRightThreshold = 65536;

    auto initialize() -> std::optional<std::string> {
        if (keys_->size() != 1) {
            return "ChunkedInnerJoinOperator only supports single-key joins";
        }
        const std::string& key_name = keys_->front();
        const ColumnValue* rkey = right_.find(key_name);
        if (rkey == nullptr) {
            return "join key not found in right table: " + key_name;
        }
        if (auto err = detect_key_kind(*rkey, key_kind_)) {
            return err;
        }

        const std::size_t n_right = right_.rows();

        if (n_right <= kStreamRightThreshold) {
            if (auto err = build_index(right_, key_name)) {
                return err;
            }
            setup_right_emit_schema(key_name);
            return std::nullopt;
        }

        auto left_res = MaterializeOperator(std::move(left_)).run();
        if (!left_res.has_value()) {
            return std::move(left_res.error());
        }
        Table left_table = std::move(*left_res);
        const std::size_t n_left = left_table.rows();

        if (n_left < n_right) {
            left_table_ = std::move(left_table);
            if (auto err = build_index(*left_table_, key_name)) {
                return err;
            }
            setup_right_emit_schema(key_name);
            mode_ = Mode::Swapped;
            return std::nullopt;
        }

        left_materialized_ = std::move(left_table);
        use_materialized_left_ = true;
        if (auto err = build_index(right_, key_name)) {
            return err;
        }
        setup_right_emit_schema(key_name);
        return std::nullopt;
    }

    static auto detect_key_kind(const ColumnValue& col, ExprType& out)
        -> std::optional<std::string> {
        if (std::holds_alternative<Column<std::int64_t>>(col)) {
            out = ExprType::Int;
        } else if (std::holds_alternative<Column<double>>(col)) {
            out = ExprType::Double;
        } else if (std::holds_alternative<Column<bool>>(col)) {
            out = ExprType::Bool;
        } else if (std::holds_alternative<Column<Date>>(col)) {
            out = ExprType::Date;
        } else if (std::holds_alternative<Column<Timestamp>>(col)) {
            out = ExprType::Timestamp;
        } else if (std::holds_alternative<Column<Categorical>>(col) ||
                   std::holds_alternative<Column<std::string>>(col)) {
            out = ExprType::String;
        } else {
            return "ChunkedInnerJoinOperator: unsupported key type";
        }
        return std::nullopt;
    }

    // Build the chained hash index on `build_side` using column `key_name`.
    // `build_idx_` maps each build-row index to the next row with the same
    // key (kNil sentinel terminates the chain).
    auto build_index(const Table& build_side, const std::string& key_name)
        -> std::optional<std::string> {
        const ColumnValue* key = build_side.find(key_name);
        if (key == nullptr) {
            return "join key not found in build side: " + key_name;
        }
        const std::size_t n = build_side.rows();
        chain_next_.assign(n, kNil);

        if (key_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, i64_heads_);
        } else if (key_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, f64_heads_);
        } else if (key_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            bool_heads_.reserve(n);
            for (std::size_t r = n; r-- > 0;) {
                const bool v = (*col)[r];
                auto [it, inserted] = bool_heads_.try_emplace(v, r);
                if (!inserted) {
                    chain_next_[r] = it->second;
                    it->second = r;
                    build_unique_ = false;
                }
            }
        } else if (key_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, date_heads_);
        } else if (key_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, ts_heads_);
        } else if (key_kind_ == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(key)) {
                const auto& dict = c_cat->dictionary();
                string_heads_.reserve(n);
                for (std::size_t r = n; r-- > 0;) {
                    auto code = static_cast<std::size_t>(c_cat->code_at(r));
                    insert_chain_sv(std::string_view{dict[code]}, r);
                }
            } else if (const auto* c_str = std::get_if<Column<std::string>>(key)) {
                string_heads_.reserve(n);
                for (std::size_t r = n; r-- > 0;) {
                    insert_chain_sv((*c_str)[r], r);
                }
            } else {
                return "inner join: build-side key type mismatch";
            }
        }
        return std::nullopt;
    }

    // Iterate the build side in reverse so the chain walks forward during
    // probe, matching the nested-loop inner join's output ordering.
    template <typename ColT, typename Map>
    void build_scalar(const ColT& col, Map& heads) {
        const std::size_t n = col.size();
        heads.reserve(n);
        const auto* data = col.data();
        for (std::size_t r = n; r-- > 0;) {
            auto [it, inserted] = heads.try_emplace(data[r], r);
            if (!inserted) {
                chain_next_[r] = it->second;
                it->second = r;
                build_unique_ = false;
            }
        }
    }

    void insert_chain_sv(std::string_view sv, std::size_t r) {
        auto [it, inserted] = string_heads_.try_emplace(sv, r);
        if (!inserted) {
            chain_next_[r] = it->second;
            it->second = r;
            build_unique_ = false;
        }
    }

    void setup_right_emit_schema(const std::string& key_name) {
        right_emit_idx_.reserve(right_.columns.size());
        for (std::size_t i = 0; i < right_.columns.size(); ++i) {
            if (right_.columns[i].name == key_name) {
                continue;
            }
            right_emit_idx_.push_back(i);
        }
    }

    // Stream mode: walk the probe side (a left chunk), for each row look
    // up the right-keyed chain and append (li, ri) in probe-scan order.
    // Returns true if every probe row matched exactly once (li == 0..n-1).
    // Only possible when the build side was unique; otherwise falls back
    // to the chained walk.
    template <typename Map, typename GetKey>
    auto probe_scalar(const Map& heads, std::size_t n, GetKey get, std::vector<std::size_t>& li,
                      std::vector<std::size_t>& ri) -> bool {
        if (build_unique_) {
            li.resize(n);
            ri.resize(n);
            std::size_t* lp = li.data();
            std::size_t* rp = ri.data();
            std::size_t out = 0;
            for (std::size_t l = 0; l < n; ++l) {
                auto it = heads.find(get(l));
                if (it == heads.end()) {
                    continue;
                }
                lp[out] = l;
                rp[out] = it->second;
                ++out;
            }
            li.resize(out);
            ri.resize(out);
            return out == n;
        }
        for (std::size_t l = 0; l < n; ++l) {
            auto it = heads.find(get(l));
            if (it == heads.end()) {
                continue;
            }
            std::size_t cur = it->second;
            while (cur != kNil) {
                li.push_back(l);
                ri.push_back(cur);
                cur = chain_next_[cur];
            }
        }
        return false;
    }

    auto probe_chunk_against_right(Table left_chunk) -> std::expected<Table, std::string> {
        const ColumnValue* key = left_chunk.find(keys_->front());
        if (key == nullptr) {
            return std::unexpected("join key not found in left chunk: " + keys_->front());
        }

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        const std::size_t n = left_chunk.rows();
        li.reserve(n);
        ri.reserve(n);
        bool li_identity = false;

        if (key_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(i64_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(f64_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            li_identity =
                probe_scalar(bool_heads_, n, [&](std::size_t i) { return (*col)[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(date_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(ts_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(key)) {
                const auto& dict = c_cat->dictionary();
                li_identity = probe_scalar(
                    string_heads_, n,
                    [&](std::size_t i) {
                        return std::string_view{dict[static_cast<std::size_t>(c_cat->code_at(i))]};
                    },
                    li, ri);
            } else if (const auto* c_str = std::get_if<Column<std::string>>(key)) {
                li_identity = probe_scalar(
                    string_heads_, n, [&](std::size_t i) { return (*c_str)[i]; }, li, ri);
            } else {
                return std::unexpected("inner join: left key type mismatch");
            }
        }

        const std::size_t total = li_identity ? ri.size() : li.size();
        return assemble_output(std::move(left_chunk), li.data(), ri.data(), total, li_identity);
    }

    // Swapped mode: the hash index is on the left table. For each right
    // row walk the left chain twice — first to count matches per left
    // row, then to fill (li, ri) in left-scan order.
    auto emit_swapped() -> std::expected<Table, std::string> {
        const ColumnValue* rkey = right_.find(keys_->front());
        if (rkey == nullptr) {
            return std::unexpected("join key not found in right table: " + keys_->front());
        }
        const std::size_t n_left = left_table_->rows();
        const std::size_t n_right = right_.rows();

        std::vector<std::size_t> match_counts(n_left, 0);
        std::size_t total = 0;

        // Phase 1: count.
        auto walk_and_apply = [&](auto&& key_at, const auto& heads, auto&& apply) {
            for (std::size_t r = 0; r < n_right; ++r) {
                auto it = heads.find(key_at(r));
                if (it == heads.end())
                    continue;
                for (std::size_t cur = it->second; cur != kNil; cur = chain_next_[cur]) {
                    apply(cur, r);
                }
            }
        };

        auto do_phase1 = [&](auto&& key_at, const auto& heads) {
            walk_and_apply(key_at, heads, [&](std::size_t lrow, std::size_t) {
                ++match_counts[lrow];
                ++total;
            });
        };

        auto do_phase2 = [&](auto&& key_at, const auto& heads, std::vector<std::size_t>& li,
                             std::vector<std::size_t>& ri) {
            std::vector<std::size_t> offsets(n_left + 1, 0);
            for (std::size_t l = 0; l < n_left; ++l) {
                offsets[l + 1] = offsets[l] + match_counts[l];
            }
            li.assign(total, 0);
            ri.assign(total, 0);
            std::vector<std::size_t> next_off = offsets;
            walk_and_apply(key_at, heads, [&](std::size_t lrow, std::size_t rrow) {
                const std::size_t pos = next_off[lrow]++;
                li[pos] = lrow;
                ri[pos] = rrow;
            });
        };

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;

        if (key_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            auto key_at = [&](std::size_t r) {
                return data[r];
            };
            do_phase1(key_at, i64_heads_);
            do_phase2(key_at, i64_heads_, li, ri);
        } else if (key_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            auto key_at = [&](std::size_t r) {
                return data[r];
            };
            do_phase1(key_at, f64_heads_);
            do_phase2(key_at, f64_heads_, li, ri);
        } else if (key_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            auto key_at = [&](std::size_t r) {
                return (*col)[r];
            };
            do_phase1(key_at, bool_heads_);
            do_phase2(key_at, bool_heads_, li, ri);
        } else if (key_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            auto key_at = [&](std::size_t r) {
                return data[r];
            };
            do_phase1(key_at, date_heads_);
            do_phase2(key_at, date_heads_, li, ri);
        } else if (key_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            auto key_at = [&](std::size_t r) {
                return data[r];
            };
            do_phase1(key_at, ts_heads_);
            do_phase2(key_at, ts_heads_, li, ri);
        } else if (key_kind_ == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(rkey)) {
                const auto& dict = c_cat->dictionary();
                auto key_at = [&](std::size_t r) {
                    return std::string_view{dict[static_cast<std::size_t>(c_cat->code_at(r))]};
                };
                do_phase1(key_at, string_heads_);
                do_phase2(key_at, string_heads_, li, ri);
            } else if (const auto* c_str = std::get_if<Column<std::string>>(rkey)) {
                auto key_at = [&](std::size_t r) {
                    return (*c_str)[r];
                };
                do_phase1(key_at, string_heads_);
                do_phase2(key_at, string_heads_, li, ri);
            } else {
                return std::unexpected("inner join: right key type mismatch");
            }
        }

        Table left_copy;
        left_copy.columns.reserve(left_table_->columns.size());
        for (const auto& c : left_table_->columns) {
            left_copy.add_column(c.name, *c.column);
            left_copy.columns.back().validity = c.validity;
        }
        return assemble_output(std::move(left_copy), li.data(), ri.data(), li.size());
    }

    auto assemble_output(Table left_side, const std::size_t* li, const std::size_t* ri,
                         std::size_t total, bool li_identity = false)
        -> std::expected<Table, std::string> {
        Table output;
        if (total == 0) {
            return output;
        }
        output.columns.reserve(left_side.columns.size() + right_emit_idx_.size());

        std::unordered_set<std::string> out_names;
        out_names.reserve(left_side.columns.size() + right_emit_idx_.size());

        auto gather_with_validity =
            [&](const ColumnValue& src_col, const std::optional<ValidityBitmap>& src_val,
                const std::size_t* idx) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
            ColumnValue gathered = gather_column(src_col, idx, total);
            std::optional<ValidityBitmap> val;
            if (src_val.has_value()) {
                const auto& src_bm = *src_val;
                ValidityBitmap dst(total, false);
                for (std::size_t i = 0; i < total; ++i) {
                    dst.set(i, src_bm[idx[i]]);
                }
                val = std::move(dst);
            }
            return {std::move(gathered), std::move(val)};
        };

        // li_identity: every probe row matched exactly once, so left columns
        // can be passed through directly (shared_ptr share) instead of
        // gathered. Do NOT move the underlying ColumnValue — the shared_ptr
        // may be aliased by upstream state (e.g., re-runnable source).
        if (li_identity && total == left_side.rows()) {
            for (auto& lc : left_side.columns) {
                out_names.insert(lc.name);
                output.index[lc.name] = output.columns.size();
                output.columns.push_back(std::move(lc));
            }
        } else {
            for (const auto& lc : left_side.columns) {
                auto [gathered, val] = gather_with_validity(*lc.column, lc.validity, li);
                out_names.insert(lc.name);
                if (val.has_value()) {
                    output.add_column(lc.name, std::move(gathered), std::move(*val));
                } else {
                    output.add_column(lc.name, std::move(gathered));
                }
            }
        }

        for (std::size_t idx : right_emit_idx_) {
            const auto& rc = right_.columns[idx];
            std::string name = rc.name;
            while (out_names.contains(name)) {
                name += "_right";
            }
            out_names.insert(name);
            auto [gathered, val] = gather_with_validity(*rc.column, rc.validity, ri);
            if (val.has_value()) {
                output.add_column(std::move(name), std::move(gathered), std::move(*val));
            } else {
                output.add_column(std::move(name), std::move(gathered));
            }
        }
        return output;
    }

    OperatorPtr left_;
    Table right_;
    const std::vector<std::string>* keys_;

    bool initialized_ = false;
    Mode mode_ = Mode::Stream;
    ExprType key_kind_ = ExprType::Int;

    // Hash index on the build side (right in Stream, left in Swapped).
    bool build_unique_ = true;
    std::vector<std::size_t> chain_next_;
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> i64_heads_;
    robin_hood::unordered_flat_map<double, std::size_t> f64_heads_;
    robin_hood::unordered_flat_map<bool, std::size_t> bool_heads_;
    robin_hood::unordered_flat_map<Date, std::size_t> date_heads_;
    robin_hood::unordered_flat_map<Timestamp, std::size_t> ts_heads_;
    robin_hood::unordered_flat_map<std::string_view, std::size_t, StringViewHash, StringViewEq>
        string_heads_;
    std::vector<std::size_t> right_emit_idx_;

    // Stream mode: when right > threshold and left >= right, left was
    // materialized to measure but not swapped; replay as a single chunk.
    std::optional<Table> left_materialized_;
    bool left_materialized_drained_ = false;
    bool use_materialized_left_ = false;

    // Swapped mode: materialized left held for later gather.
    std::optional<Table> left_table_;
    bool swapped_emitted_ = false;
};

/// Streaming hash aggregate. Maintains a `robin_hood` group index and
/// per-group `AggState` across chunks: each incoming chunk updates the
/// state per row, the chunk is released, and the final result is
/// emitted as a single output chunk on EOF.
///
/// Eligibility is gated at `build_operator` time to the common subset
/// that streams cleanly: `Count`, `Sum`, `Min`, `Max`, `Mean` on
/// numeric (int/double) inputs. Nullable agg inputs are handled — null
/// rows skip the update, and an all-null group emits a null result.
/// Nullable group-by columns are not supported yet; they fall back to
/// `aggregate_table` via `interpret_node`. Complex aggs (Median, etc.)
/// and string aggs also fall back.
///
/// The first chunk's group-by column types are snapshotted (including
/// the Categorical dictionary pointer when applicable) and reused when
/// building output; the chunked csv source shares dictionaries across
/// chunks, matching MaterializeOperator's existing assumption.
class ChunkedAggregateOperator final : public Operator {
   public:
    ChunkedAggregateOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                             const std::vector<ir::AggSpec>* aggregations)
        : child_(std::move(child)), group_by_(group_by), aggregations_(aggregations) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());
            if (auto err = process_chunk(chunk)) {
                return std::unexpected(*err);
            }
            // `chunk` goes out of scope here, releasing its memory
            // before we pull the next one from the child.
        }
        emitted_ = true;
        return build_output_chunk();
    }

   private:
    auto process_chunk(const Chunk& chunk) -> std::optional<std::string> {
        std::vector<const ColumnEntry*> group_entries;
        group_entries.reserve(group_by_->size());
        for (const auto& key : *group_by_) {
            const ColumnEntry* entry = nullptr;
            for (const auto& e : chunk.columns) {
                if (e.name == key.name) {
                    entry = &e;
                    break;
                }
            }
            if (entry == nullptr) {
                return "group-by column not found: " + key.name;
            }
            group_entries.push_back(entry);
        }

        std::vector<const ColumnEntry*> agg_entries(aggregations_->size(), nullptr);
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            const auto& agg = (*aggregations_)[i];
            if (agg.func == ir::AggFunc::Count) {
                continue;
            }
            const ColumnEntry* entry = nullptr;
            for (const auto& e : chunk.columns) {
                if (e.name == agg.column.name) {
                    entry = &e;
                    break;
                }
            }
            if (entry == nullptr) {
                return "aggregate column not found: " + agg.column.name;
            }
            ExprType kind = expr_type_for_column(*entry->column);
            if (kind != ExprType::Int && kind != ExprType::Double) {
                return "ChunkedAggregateOperator: non-numeric aggregation not supported";
            }
            agg_entries[i] = entry;
        }

        if (!initialized_) {
            n_aggs_ = aggregations_->size();
            plan_.reserve(n_aggs_);
            for (std::size_t i = 0; i < n_aggs_; ++i) {
                SlotPlan p;
                p.func = (*aggregations_)[i].func;
                if (p.func == ir::AggFunc::Count) {
                    p.kind = ExprType::Int;
                } else {
                    p.kind = expr_type_for_column(*agg_entries[i]->column);
                }
                plan_.push_back(p);
            }
            group_templates_.reserve(group_entries.size());
            bool all_cat = true;
            for (const auto* e : group_entries) {
                group_templates_.push_back(make_empty_like(*e->column));
                if (!std::holds_alternative<Column<Categorical>>(*e->column) ||
                    e->validity.has_value()) {
                    all_cat = false;
                }
            }
            cat_fast_path_ = all_cat && !group_entries.empty();
            // Single-string-key fast path: avoids the generic `Key`/ScalarValue
            // variant path used by `process_rows_generic`. High-cardinality
            // `sum by user_id` (~100K distinct strings in 2M rows) was spending
            // most of its time constructing per-row ScalarValue variants and
            // hashing them; the string path uses a string_view map keyed against
            // an owned char/offset dictionary instead.
            str_fast_path_ =
                group_entries.size() == 1 &&
                std::holds_alternative<Column<std::string>>(*group_entries[0]->column) &&
                !group_entries[0]->validity.has_value();
            initialized_ = true;
        } else {
            for (std::size_t i = 0; i < n_aggs_; ++i) {
                if (plan_[i].func == ir::AggFunc::Count) {
                    continue;
                }
                ExprType kind = expr_type_for_column(*agg_entries[i]->column);
                if (kind != plan_[i].kind) {
                    return "ChunkedAggregateOperator: aggregate column type changed across chunks";
                }
            }
            for (std::size_t i = 0; i < group_entries.size(); ++i) {
                if (group_entries[i]->column->index() != group_templates_[i].index()) {
                    return "ChunkedAggregateOperator: group-by column type changed across chunks";
                }
            }
        }

        const std::size_t rows = chunk.rows();

        if (cat_fast_path_) {
            return process_rows_cat(group_entries, agg_entries, rows);
        }
        if (str_fast_path_) {
            return process_rows_str(group_entries, agg_entries, rows);
        }
        return process_rows_generic(group_entries, agg_entries, rows);
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_str(const std::vector<const ColumnEntry*>& group_entries,
                          const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        const auto& col = std::get<Column<std::string>>(*group_entries[0]->column);
        const char* src_chars = col.chars_data();
        const std::uint32_t* src_off = col.offsets_data();

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

        // Run-length shortcut: sorted or chunked CSV often has adjacent
        // repeats; skip the hash lookup when the key matches the previous row.
        std::string_view prev_key;
        std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
        for (std::size_t row = 0; row < rows; ++row) {
            std::string_view key{src_chars + src_off[row], src_off[row + 1] - src_off[row]};
            std::uint32_t gid{};
            if (key == prev_key) {
                gid = prev_gid;
            } else {
                // Transparent lookup on string_view avoids constructing a
                // std::string per probe. Insertions pay one std::string
                // construction per novel key — with libstdc++'s 15-char SSO,
                // 11-char user_id strings stay inline (no heap alloc).
                auto it = str_index_.find(key);
                if (it == str_index_.end()) {
                    gid = static_cast<std::uint32_t>(n_groups_);
                    str_index_.emplace(std::string(key), gid);
                    str_order_.emplace_back(key);
                    ++n_groups_;
                    flat_slots_.resize(n_groups_ * n_aggs_);
                } else {
                    gid = static_cast<std::uint32_t>(it->second);
                }
                prev_key = key;
                prev_gid = gid;
            }
            gids[row] = gid;
        }

        accumulate_columns(gids, agg_entries, rows);
        return std::nullopt;
    }

    auto alloc_group() -> std::uint32_t {
        auto gid = static_cast<std::uint32_t>(n_groups_);
        ++n_groups_;
        flat_slots_.resize(n_groups_ * n_aggs_);
        return gid;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_cat(const std::vector<const ColumnEntry*>& group_entries,
                          const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        std::vector<const Column<Categorical>*> cat_cols;
        cat_cols.reserve(group_entries.size());
        for (const auto* e : group_entries) {
            cat_cols.push_back(&std::get<Column<Categorical>>(*e->column));
        }
        const std::size_t n_keys = cat_cols.size();
        const bool single_key = n_keys == 1;

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();
        if (single_key) {
            // A Categorical code is already a dense index into [0, dict_size),
            // so map code → gid with a direct array instead of hashing. Dicts
            // only grow and never reorder across chunks, so existing gids stay
            // valid and new dict entries just extend the array with sentinels.
            const auto* codes = cat_cols[0]->codes_data();
            const std::size_t dict_size = cat_cols[0]->dictionary().size();
            if (cat_dense_gid_.size() < dict_size) {
                cat_dense_gid_.resize(dict_size, kNoGid);
            }
            std::uint32_t* dense = cat_dense_gid_.data();
            for (std::size_t row = 0; row < rows; ++row) {
                const auto code = codes[row];
                std::uint32_t gid = dense[code];
                if (gid == kNoGid) {
                    gid = alloc_group();
                    dense[code] = gid;
                    cat_order_.push_back(code);
                }
                gids[row] = gid;
            }
        } else {
            // Multi-key: encode each row as a uint64_t Cartesian cell.
            // Strides may grow across chunks if a chunk introduces new dict
            // entries; we recompute per chunk and rebuild the index when that
            // happens (rare — Categorical dicts are usually stable).
            std::vector<std::uint64_t> dict_sizes(n_keys);
            for (std::size_t c = 0; c < n_keys; ++c) {
                dict_sizes[c] = static_cast<std::uint64_t>(cat_cols[c]->dictionary().size());
                if (dict_sizes[c] == 0)
                    dict_sizes[c] = 1;  // avoid stride collapse
            }
            // Strides: cell = c0*s0 + c1*s1 + … with s_{n-1} = 1.
            std::vector<std::uint64_t> strides(n_keys);
            std::uint64_t total_cells = 1;
            {
                std::uint64_t s = 1;
                for (int ci = static_cast<int>(n_keys) - 1; ci >= 0; --ci) {
                    strides[static_cast<std::size_t>(ci)] = s;
                    s *= dict_sizes[static_cast<std::size_t>(ci)];
                }
                total_cells = s;
            }

            // Hoist raw code pointers out of the row loop.
            std::vector<const Column<Categorical>::code_type*> raws(n_keys);
            for (std::size_t c = 0; c < n_keys; ++c)
                raws[c] = cat_cols[c]->codes_data();

            const auto cell_of_group = [&](std::size_t g) -> std::uint64_t {
                std::uint64_t cell = 0;
                for (std::size_t c = 0; c < n_keys; ++c) {
                    cell += static_cast<std::uint64_t>(multi_cat_codes_flat_[(g * n_keys) + c]) *
                            strides[c];
                }
                return cell;
            };
            const auto new_group = [&](std::size_t row) -> std::uint32_t {
                for (std::size_t c = 0; c < n_keys; ++c)
                    multi_cat_codes_flat_.push_back(raws[c][row]);
                return alloc_group();
            };

            // When the Cartesian cell space is bounded, index a dense array
            // (one load per row, no hashing). If a later chunk grows the dicts
            // past the limit, migrate existing groups into the hash map once
            // and stay there — dicts only grow, so total_cells never shrinks.
            if (multi_dense_ && total_cells > kDenseCellLimit) {
                multi_cat_cell_index_.clear();
                multi_cat_cell_index_.reserve(n_groups_);
                for (std::size_t g = 0; g < n_groups_; ++g)
                    multi_cat_cell_index_.emplace(cell_of_group(g), static_cast<std::uint32_t>(g));
                std::vector<std::uint32_t>().swap(multi_cat_cell_dense_);
                multi_dense_ = false;
                multi_cat_strides_ = strides;
            }

            if (multi_dense_) {
                // Rebuild the dense array when strides change (new dict entries).
                if (multi_cat_strides_ != strides) {
                    multi_cat_cell_dense_.assign(static_cast<std::size_t>(total_cells), kNoGid);
                    for (std::size_t g = 0; g < n_groups_; ++g)
                        multi_cat_cell_dense_[cell_of_group(g)] = static_cast<std::uint32_t>(g);
                    multi_cat_strides_ = strides;
                }
                std::uint32_t* dense = multi_cat_cell_dense_.data();
                if (n_keys == 2) {
                    const auto* k0 = raws[0];
                    const auto* k1 = raws[1];
                    const std::uint64_t s0 = strides[0];
                    const std::uint64_t s1 = strides[1];
                    for (std::size_t row = 0; row < rows; ++row) {
                        const std::uint64_t cell = (static_cast<std::uint64_t>(k0[row]) * s0) +
                                                   (static_cast<std::uint64_t>(k1[row]) * s1);
                        std::uint32_t gid = dense[cell];
                        if (gid == kNoGid) {
                            gid = new_group(row);
                            dense[cell] = gid;
                        }
                        gids[row] = gid;
                    }
                } else {
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint64_t cell = 0;
                        for (std::size_t c = 0; c < n_keys; ++c)
                            cell += static_cast<std::uint64_t>(raws[c][row]) * strides[c];
                        std::uint32_t gid = dense[cell];
                        if (gid == kNoGid) {
                            gid = new_group(row);
                            dense[cell] = gid;
                        }
                        gids[row] = gid;
                    }
                }
            } else {
                // Hash fallback for unbounded cell spaces. Rebuild on stride
                // change (new dict entries) just like the dense path.
                if (multi_cat_strides_ != strides) {
                    multi_cat_cell_index_.clear();
                    multi_cat_cell_index_.reserve(n_groups_);
                    for (std::size_t g = 0; g < n_groups_; ++g)
                        multi_cat_cell_index_.emplace(cell_of_group(g),
                                                      static_cast<std::uint32_t>(g));
                    multi_cat_strides_ = strides;
                }
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint64_t cell = 0;
                    for (std::size_t c = 0; c < n_keys; ++c)
                        cell += static_cast<std::uint64_t>(raws[c][row]) * strides[c];
                    auto [it, inserted] =
                        multi_cat_cell_index_.emplace(cell, static_cast<std::uint32_t>(n_groups_));
                    if (inserted) {
                        gids[row] = new_group(row);
                    } else {
                        gids[row] = it->second;
                    }
                }
            }
        }

        accumulate_columns(gids, agg_entries, rows);
        return std::nullopt;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_generic(const std::vector<const ColumnEntry*>& group_entries,
                              const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();
        for (std::size_t row = 0; row < rows; ++row) {
            Key key;
            key.values.reserve(group_entries.size());
            for (const auto* e : group_entries) {
                key.values.push_back(scalar_from_column(*e->column, row));
            }
            auto [it, inserted] = index_.emplace(std::move(key), n_groups_);
            if (inserted) {
                group_order_.push_back(it->first);
                gids[row] = alloc_group();
            } else {
                gids[row] = static_cast<std::uint32_t>(it->second);
            }
        }

        accumulate_columns(gids, agg_entries, rows);
        return std::nullopt;
    }

    void accumulate_columns(const std::uint32_t* gids,
                            const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows) {
        AggSlot* fs = flat_slots_.data();
        for (std::size_t agg_i = 0; agg_i < n_aggs_; ++agg_i) {
            const auto slot_for = [&](std::uint32_t g) -> AggSlot& {
                return fs[(static_cast<std::size_t>(g) * n_aggs_) + agg_i];
            };

            if (plan_[agg_i].func == ir::AggFunc::Count) {
                for (std::size_t row = 0; row < rows; ++row) {
                    slot_for(gids[row]).count++;
                }
                continue;
            }

            const auto& entry = *agg_entries[agg_i];
            const bool has_nulls = entry.validity.has_value();

            if (plan_[agg_i].kind == ExprType::Double) {
                const double* data = std::get<Column<double>>(*entry.column).data();
                switch (plan_[agg_i].func) {
                    case ir::AggFunc::Sum:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += data[row];
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.sum += data[row];
                            slot.count++;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            double v = data[row];
                            slot.double_value = slot.has_value ? std::min(slot.double_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            double v = data[row];
                            slot.double_value = slot.has_value ? std::max(slot.double_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), data[row]);
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            agg_update_moments(slot_for(gids[row]), data[row]);
                        }
                        break;
                    default:
                        break;
                }
            } else {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                switch (plan_[agg_i].func) {
                    case ir::AggFunc::Sum:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.int_value += data[row];
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.sum += static_cast<double>(data[row]);
                            slot.count++;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.has_value ? std::min(slot.int_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.has_value ? std::max(slot.int_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*entry.validity)[row])
                                continue;
                            agg_update_moments(slot_for(gids[row]), static_cast<double>(data[row]));
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    auto build_output_chunk() -> std::expected<std::optional<Chunk>, std::string> {
        Chunk out;
        out.columns.reserve(group_by_->size() + aggregations_->size());

        if (!initialized_) {
            // No input rows at all — emit a chunk with empty columns of
            // the expected schema where possible. Without any chunk we
            // have no types to build group columns; return an empty
            // optional so the sink finalizes an empty table.
            return std::optional<Chunk>{};
        }

        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            out.add_column((*group_by_)[i].name, make_empty_like(group_templates_[i]));
        }
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            const auto& agg = (*aggregations_)[i];
            ColumnValue column;
            switch (agg.func) {
                case ir::AggFunc::Count:
                    column = Column<std::int64_t>{};
                    break;
                case ir::AggFunc::Mean:
                case ir::AggFunc::Stddev:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    column = Column<double>{};
                    break;
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                    if (plan_[i].kind == ExprType::Double) {
                        column = Column<double>{};
                    } else {
                        column = Column<std::int64_t>{};
                    }
                    break;
                default:
                    return std::unexpected(
                        "ChunkedAggregateOperator: unsupported agg in build_output");
            }
            out.add_column(agg.alias, std::move(column));
        }

        for (std::size_t i = 0; i < out.columns.size(); ++i) {
            std::visit([&](auto& c) { c.reserve(n_groups_); }, out.mutable_column(i));
        }

        std::vector<ValidityBitmap> agg_validity(aggregations_->size());
        std::vector<std::uint8_t> track_validity(aggregations_->size(), 0U);
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            if (chunked_agg_tracks_validity(plan_[i].func)) {
                track_validity[i] = 1U;
                agg_validity[i].reserve(n_groups_);
            }
        }

        const AggSlot* fs = flat_slots_.data();
        for (std::size_t g = 0; g < n_groups_; ++g) {
            if (cat_fast_path_) {
                const bool single_key = group_by_->size() == 1;
                if (single_key) {
                    auto& cat_col = std::get<Column<Categorical>>(out.mutable_column(0));
                    cat_col.push_code(cat_order_[g]);
                } else {
                    const std::size_t n_keys = group_by_->size();
                    for (std::size_t ci = 0; ci < n_keys; ++ci) {
                        auto& cat_col = std::get<Column<Categorical>>(out.mutable_column(ci));
                        cat_col.push_code(multi_cat_codes_flat_[(g * n_keys) + ci]);
                    }
                }
            } else if (str_fast_path_) {
                auto& str_col = std::get<Column<std::string>>(out.mutable_column(0));
                str_col.push_back(str_order_[g]);
            } else {
                const Key& key = group_order_[g];
                for (std::size_t ci = 0; ci < key.values.size(); ++ci) {
                    append_scalar(out.mutable_column(ci), key.values[ci]);
                }
            }
            for (std::size_t i = 0; i < aggregations_->size(); ++i) {
                auto& column = out.mutable_column(group_by_->size() + i);
                const AggSlot& slot = fs[(g * n_aggs_) + i];
                if (track_validity[i] != 0U) {
                    agg_validity[i].push_back(chunked_agg_valid(plan_[i].func, slot));
                }
                switch (plan_[i].func) {
                    case ir::AggFunc::Count:
                        append_scalar(column, slot.count);
                        break;
                    case ir::AggFunc::Mean:
                        append_scalar(column, slot.count == 0
                                                  ? 0.0
                                                  : slot.sum / static_cast<double>(slot.count));
                        break;
                    case ir::AggFunc::Sum:
                    case ir::AggFunc::Min:
                    case ir::AggFunc::Max:
                        if (plan_[i].kind == ExprType::Double) {
                            append_scalar(column, slot.double_value);
                        } else {
                            append_scalar(column, slot.int_value);
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        append_scalar(column, agg_finalize_stddev(slot));
                        break;
                    case ir::AggFunc::Skew:
                        append_scalar(column, agg_finalize_skew(slot));
                        break;
                    case ir::AggFunc::Kurtosis:
                        append_scalar(column, agg_finalize_kurtosis(slot));
                        break;
                    default:
                        break;
                }
            }
        }

        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            if (track_validity[i] == 0U || agg_validity[i].size() == 0) {
                continue;
            }
            bool has_null = false;
            for (std::size_t r = 0; r < agg_validity[i].size(); ++r) {
                if (!agg_validity[i][r]) {
                    has_null = true;
                    break;
                }
            }
            if (has_null) {
                out.columns[group_by_->size() + i].validity = std::move(agg_validity[i]);
            }
        }

        return std::optional<Chunk>{std::move(out)};
    }

    struct SlotPlan {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
    };

    struct CatKey {
        std::vector<Column<Categorical>::code_type> codes;
        auto operator==(const CatKey& o) const noexcept -> bool { return codes == o.codes; }
    };

    // Transparent hash/eq: lets `str_index_.find(string_view)` skip the
    // allocation of a temporary std::string on every probe.
    struct StrViewHash {
        using is_transparent = void;
        auto operator()(std::string_view s) const noexcept -> std::size_t {
            return robin_hood::hash_bytes(s.data(), s.size());
        }
        auto operator()(const std::string& s) const noexcept -> std::size_t {
            return robin_hood::hash_bytes(s.data(), s.size());
        }
    };
    struct StrViewEq {
        using is_transparent = void;
        auto operator()(const std::string& a, const std::string& b) const noexcept -> bool {
            return a == b;
        }
        auto operator()(const std::string& a, std::string_view b) const noexcept -> bool {
            return std::string_view(a) == b;
        }
        auto operator()(std::string_view a, const std::string& b) const noexcept -> bool {
            return a == std::string_view(b);
        }
    };
    struct CatKeyHash {
        auto operator()(const CatKey& k) const noexcept -> std::size_t {
            std::size_t h = 0;
            for (auto c : k.codes) {
                h ^= robin_hood::hash<Column<Categorical>::code_type>{}(c) + 0x9e3779b9 + (h << 6) +
                     (h >> 2);
            }
            return h;
        }
    };

    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* group_by_;
    const std::vector<ir::AggSpec>* aggregations_;
    bool emitted_ = false;

    bool initialized_ = false;
    bool cat_fast_path_ = false;
    bool str_fast_path_ = false;
    std::size_t n_aggs_ = 0;
    std::size_t n_groups_ = 0;
    std::vector<SlotPlan> plan_;
    std::vector<ColumnValue> group_templates_;

    // Flat accumulator storage: n_groups_ × n_aggs_ contiguous AggSlots.
    std::vector<AggSlot> flat_slots_;

    // Reusable per-chunk gids buffer to avoid repeated heap allocations.
    std::vector<std::uint32_t> gids_buf_;

    // Generic path (non-Categorical group keys).
    robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> index_;
    std::vector<Key> group_order_;

    // Sentinel for "no group assigned yet" in the dense index arrays.
    static constexpr std::uint32_t kNoGid = std::numeric_limits<std::uint32_t>::max();
    // Cartesian cell-space size below which multi-key grouping uses a dense
    // array (one load per row) instead of hashing. 4M cells = 16 MB of u32.
    static constexpr std::uint64_t kDenseCellLimit = 4'000'000ULL;

    // Single-Categorical fast path: code → gid via direct array (codes are a
    // dense [0, dict_size) index, so no hashing is needed).
    using cat_code = Column<Categorical>::code_type;
    std::vector<std::uint32_t> cat_dense_gid_;
    std::vector<cat_code> cat_order_;

    // Multi-Categorical fast path: cell-encoded. Dense array while the cell
    // space stays under kDenseCellLimit; spills to the hash map otherwise.
    bool multi_dense_ = true;
    std::vector<std::uint32_t> multi_cat_cell_dense_;
    robin_hood::unordered_flat_map<std::uint64_t, std::uint32_t> multi_cat_cell_index_;
    std::vector<Column<Categorical>::code_type> multi_cat_codes_flat_;  // n_groups_ × n_keys
    std::vector<std::uint64_t> multi_cat_strides_;  // last-seen strides for rebuild detection

    // Single-string-key fast path.
    robin_hood::unordered_flat_map<std::string, std::size_t, StrViewHash, StrViewEq> str_index_;
    std::vector<std::string> str_order_;
};

/// Replays one buffered chunk ahead of the rest of a child stream. Used by
/// ChunkedSortedAggregateOperator to hand the already-pulled first chunk back
/// to a fallback operator without losing it.
class PrependChunkOperator final : public Operator {
   public:
    PrependChunkOperator(Chunk first, OperatorPtr rest)
        : first_(std::move(first)), rest_(std::move(rest)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!emitted_first_) {
            emitted_first_ = true;
            return std::optional<Chunk>{std::move(first_)};
        }
        return rest_->next();
    }

   private:
    Chunk first_;
    OperatorPtr rest_;
    bool emitted_first_ = false;
};

/// Streaming aggregate for input already sorted on the group-by keys.
///
/// When the child's chunks declare an `ordering` whose leading keys cover the
/// group_by columns, every group's rows are contiguous in the stream. We then
/// keep accumulators for only the *current* group, emit each group as soon as
/// its run ends, and produce output already sorted by the group keys. Peak
/// memory is O(one group + one output chunk) instead of O(all groups), and
/// there is no hashing — group changes are detected by a typed equality scan.
///
/// Eligibility is decided from the first non-empty chunk. If the input is not
/// sorted on the group_by keys (no `ordering`, or it doesn't cover them, or a
/// group key is nullable), the operator transparently falls back to the
/// hash-based ChunkedAggregateOperator by replaying the already-pulled chunk
/// ahead of the remaining child. The supported agg subset matches
/// ChunkedAggregateOperator (Count/Sum/Min/Max/Mean on numeric columns);
/// build_operator only routes that subset here.
class ChunkedSortedAggregateOperator final : public Operator {
   public:
    ChunkedSortedAggregateOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                                   const std::vector<ir::AggSpec>* aggregations)
        : child_(std::move(child)), group_by_(group_by), aggregations_(aggregations) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (fallback_) {
            return fallback_->next();
        }
        if (!decided_) {
            auto decided = decide_strategy();
            if (!decided.has_value()) {
                return std::unexpected(std::move(decided.error()));
            }
            if (fallback_) {
                return fallback_->next();
            }
        }
        return next_sorted();
    }

   private:
    struct SlotPlan {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
    };

    // Pull chunks until the first non-empty one, then choose sorted vs fallback.
    auto decide_strategy() -> std::expected<void, std::string> {
        decided_ = true;
        Chunk first;
        bool have = false;
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;  // EOF before any rows
            }
            if (chunk_res.value()->rows() == 0) {
                continue;  // skip empty chunks
            }
            first = std::move(*chunk_res.value());
            have = true;
            break;
        }
        if (!have) {
            done_ = true;  // empty input → no output, matching the hash operator
            input_eof_ = true;
            return {};
        }
        if (!sorted_on_group_by(first)) {
            fallback_ = std::make_unique<ChunkedAggregateOperator>(
                std::make_unique<PrependChunkOperator>(std::move(first), std::move(child_)),
                group_by_, aggregations_);
            return {};
        }
        if (auto err = init_plan(first)) {
            return std::unexpected(*err);
        }
        if (auto err = consume(first)) {
            return std::unexpected(*err);
        }
        return {};
    }

    // The input is grouped-contiguous iff the first |group_by| ordering keys
    // are exactly the group_by columns (as a set; direction and intra-prefix
    // order don't matter for contiguity). Nullable group keys fall back, since
    // the streaming key compare ignores validity.
    auto sorted_on_group_by(const Chunk& chunk) const -> bool {
        if (group_by_->empty()) {
            return false;  // global aggregate: let the hash path handle it
        }
        if (!chunk.ordering.has_value() || chunk.ordering->size() < group_by_->size()) {
            return false;
        }
        const auto& ordering = *chunk.ordering;
        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            bool in_group = false;
            for (const auto& g : *group_by_) {
                if (g.name == ordering[i].name) {
                    in_group = true;
                    break;
                }
            }
            if (!in_group) {
                return false;
            }
        }
        return std::ranges::all_of(*group_by_, [&chunk](const auto& g) {
            const ColumnEntry* entry = find_entry(chunk, g.name);
            return entry != nullptr && !entry->validity.has_value();
        });
    }

    static auto find_entry(const Chunk& chunk, const std::string& name) -> const ColumnEntry* {
        for (const auto& e : chunk.columns) {
            if (e.name == name) {
                return &e;
            }
        }
        return nullptr;
    }

    auto init_plan(const Chunk& first) -> std::optional<std::string> {
        n_aggs_ = aggregations_->size();
        plan_.resize(n_aggs_);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            const auto& agg = (*aggregations_)[i];
            plan_[i].func = agg.func;
            if (agg.func == ir::AggFunc::Count) {
                plan_[i].kind = ExprType::Int;
                continue;
            }
            const ColumnEntry* entry = find_entry(first, agg.column.name);
            if (entry == nullptr) {
                return "aggregate column not found: " + agg.column.name;
            }
            ExprType kind = expr_type_for_column(*entry->column);
            if (kind != ExprType::Int && kind != ExprType::Double) {
                return "ChunkedSortedAggregateOperator: non-numeric aggregation not supported";
            }
            plan_[i].kind = kind;
        }
        key_templates_.clear();
        key_templates_.reserve(group_by_->size());
        for (const auto& g : *group_by_) {
            key_templates_.push_back(make_empty_like(*find_entry(first, g.name)->column));
        }
        track_validity_.assign(n_aggs_, 0U);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            track_validity_[i] = chunked_agg_tracks_validity(plan_[i].func) ? 1U : 0U;
        }
        // Capture the leading ordering keys so emitted chunks can advertise the
        // group-sorted order they preserve (lets a downstream `order` skip work).
        if (first.ordering.has_value()) {
            out_ordering_.assign(
                first.ordering->begin(),
                first.ordering->begin() + static_cast<std::ptrdiff_t>(group_by_->size()));
        }
        cur_slots_.assign(n_aggs_, AggSlot{});
        reset_output();
        return std::nullopt;
    }

    void reset_output() {
        out_columns_.clear();
        out_columns_.reserve(group_by_->size() + n_aggs_);
        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            ColumnEntry entry;
            entry.name = (*group_by_)[i].name;
            entry.column = std::make_shared<ColumnValue>(make_empty_like(key_templates_[i]));
            std::visit([&](auto& c) { c.reserve(kEmitThreshold); }, *entry.column);
            out_columns_.push_back(std::move(entry));
        }
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            ColumnValue column;
            switch (plan_[i].func) {
                case ir::AggFunc::Count:
                    column = Column<std::int64_t>{};
                    break;
                case ir::AggFunc::Mean:
                case ir::AggFunc::Stddev:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    column = Column<double>{};
                    break;
                default:  // Sum / Min / Max
                    column = plan_[i].kind == ExprType::Double
                                 ? ColumnValue{Column<double>{}}
                                 : ColumnValue{Column<std::int64_t>{}};
                    break;
            }
            std::visit([&](auto& c) { c.reserve(kEmitThreshold); }, column);
            ColumnEntry entry;
            entry.name = (*aggregations_)[i].alias;
            entry.column = std::make_shared<ColumnValue>(std::move(column));
            out_columns_.push_back(std::move(entry));
        }
        out_validity_.assign(n_aggs_, ValidityBitmap{});
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            if (track_validity_[i] != 0U) {
                out_validity_[i].reserve(kEmitThreshold);
            }
        }
        pending_rows_ = 0;
    }

    // Drive input until we have a full output batch or hit EOF, then emit.
    auto next_sorted() -> std::expected<std::optional<Chunk>, std::string> {
        if (done_) {
            return std::optional<Chunk>{};
        }
        while (!input_eof_ && pending_rows_ < kEmitThreshold) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                input_eof_ = true;
                break;
            }
            if (chunk_res.value()->rows() == 0) {
                continue;
            }
            if (auto err = consume(*chunk_res.value())) {
                return std::unexpected(*err);
            }
        }
        if (input_eof_ && open_) {
            close_group();
            open_ = false;
        }
        if (pending_rows_ == 0) {
            done_ = true;
            return std::optional<Chunk>{};
        }
        Chunk out = take_pending();
        if (input_eof_) {
            done_ = true;
        }
        return std::optional<Chunk>{std::move(out)};
    }

    // Fold one chunk into the streaming state. Rows are scanned as runs of
    // equal group keys; each run is accumulated columnwise into the open
    // group's slots, and a group-key change closes the open group.
    auto consume(const Chunk& chunk) -> std::optional<std::string> {
        std::vector<const ColumnValue*> key_cols;
        key_cols.reserve(group_by_->size());
        for (const auto& g : *group_by_) {
            const ColumnEntry* entry = find_entry(chunk, g.name);
            if (entry == nullptr) {
                return "group-by column not found: " + g.name;
            }
            key_cols.push_back(entry->column.get());
        }
        std::vector<const ColumnEntry*> agg_entries(n_aggs_, nullptr);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            if (plan_[i].func == ir::AggFunc::Count) {
                continue;
            }
            const ColumnEntry* entry = find_entry(chunk, (*aggregations_)[i].column.name);
            if (entry == nullptr) {
                return "aggregate column not found: " + (*aggregations_)[i].column.name;
            }
            if (expr_type_for_column(*entry->column) != plan_[i].kind) {
                return "ChunkedSortedAggregateOperator: aggregate column type changed across "
                       "chunks";
            }
            agg_entries[i] = entry;
        }

        const std::size_t rows = chunk.rows();
        std::size_t r = 0;
        while (r < rows) {
            if (!open_) {
                start_group(key_cols, r);
            } else if (!row_matches_open(key_cols, r)) {
                close_group();
                start_group(key_cols, r);
            }
            std::size_t e = r + 1;
            while (e < rows && cells_equal(key_cols, r, e)) {
                ++e;
            }
            accumulate_range(agg_entries, r, e);
            r = e;
        }
        return std::nullopt;
    }

    void start_group(const std::vector<const ColumnValue*>& key_cols, std::size_t row) {
        open_key_.clear();
        open_key_.reserve(key_cols.size());
        for (const auto* col : key_cols) {
            open_key_.push_back(scalar_from_column(*col, row));
        }
        std::fill(cur_slots_.begin(), cur_slots_.end(), AggSlot{});
        open_ = true;
    }

    // Whether `row` continues the currently open group. Only called at run
    // anchors (group boundaries and chunk starts), so the scalar build is
    // paid per group, not per row.
    auto row_matches_open(const std::vector<const ColumnValue*>& key_cols, std::size_t row) const
        -> bool {
        for (std::size_t i = 0; i < key_cols.size(); ++i) {
            if (scalar_from_column(*key_cols[i], row) != open_key_[i]) {
                return false;
            }
        }
        return true;
    }

    static auto cell_equal(const ColumnValue& col, std::size_t a, std::size_t b) -> bool {
        return std::visit(
            [&](const auto& c) -> bool {
                using ColT = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    return c.code_at(a) == c.code_at(b);
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    return c[a].days == c[b].days;
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    return c[a].nanos == c[b].nanos;
                } else {
                    return c[a] == c[b];
                }
            },
            col);
    }

    [[nodiscard]] static auto cells_equal(const std::vector<const ColumnValue*>& key_cols,
                                          std::size_t a, std::size_t b) -> bool {
        return std::ranges::all_of(key_cols,
                                   [a, b](const auto* col) { return cell_equal(*col, a, b); });
    }

    // Accumulate the contiguous row range [start, end) — all one group — into
    // the open group's slots, branch-hoisted per aggregation.
    void accumulate_range(const std::vector<const ColumnEntry*>& agg_entries, std::size_t start,
                          std::size_t end) {
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            AggSlot& slot = cur_slots_[i];
            if (plan_[i].func == ir::AggFunc::Count) {
                slot.count += static_cast<std::int64_t>(end - start);
                continue;
            }
            const auto& entry = *agg_entries[i];
            const bool has_nulls = entry.validity.has_value();
            if (plan_[i].kind == ExprType::Double) {
                const double* data = std::get<Column<double>>(*entry.column).data();
                accumulate_typed(slot, plan_[i].func, data, entry, has_nulls, start, end);
            } else {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                accumulate_typed(slot, plan_[i].func, data, entry, has_nulls, start, end);
            }
        }
    }

    template <typename T>
    static void accumulate_typed(AggSlot& slot, ir::AggFunc func, const T* data,
                                 const ColumnEntry& entry, bool has_nulls, std::size_t start,
                                 std::size_t end) {
        const auto valid = [&](std::size_t row) {
            return !has_nulls || (*entry.validity)[row];
        };
        switch (func) {
            case ir::AggFunc::Sum:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value += data[row];
                    } else {
                        slot.int_value += data[row];
                    }
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Mean:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    slot.sum += static_cast<double>(data[row]);
                    slot.count++;
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Min:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value =
                            slot.has_value ? std::min(slot.double_value, data[row]) : data[row];
                    } else {
                        slot.int_value =
                            slot.has_value ? std::min(slot.int_value, data[row]) : data[row];
                    }
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Max:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value =
                            slot.has_value ? std::max(slot.double_value, data[row]) : data[row];
                    } else {
                        slot.int_value =
                            slot.has_value ? std::max(slot.int_value, data[row]) : data[row];
                    }
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Stddev:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    agg_update_stddev(slot, static_cast<double>(data[row]));
                }
                break;
            case ir::AggFunc::Skew:
            case ir::AggFunc::Kurtosis:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    agg_update_moments(slot, static_cast<double>(data[row]));
                }
                break;
            default:
                break;
        }
    }

    // Flush the open group's key + aggregate values into the output buffers.
    void close_group() {
        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            append_scalar(*out_columns_[i].column, open_key_[i]);
        }
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            ColumnValue& column = *out_columns_[group_by_->size() + i].column;
            const AggSlot& slot = cur_slots_[i];
            if (track_validity_[i] != 0U) {
                out_validity_[i].push_back(chunked_agg_valid(plan_[i].func, slot));
            }
            switch (plan_[i].func) {
                case ir::AggFunc::Count:
                    append_scalar(column, ScalarValue{slot.count});
                    break;
                case ir::AggFunc::Mean:
                    append_scalar(
                        column,
                        ScalarValue{slot.count == 0 ? 0.0
                                                    : slot.sum / static_cast<double>(slot.count)});
                    break;
                case ir::AggFunc::Stddev:
                    append_scalar(column, ScalarValue{agg_finalize_stddev(slot)});
                    break;
                case ir::AggFunc::Skew:
                    append_scalar(column, ScalarValue{agg_finalize_skew(slot)});
                    break;
                case ir::AggFunc::Kurtosis:
                    append_scalar(column, ScalarValue{agg_finalize_kurtosis(slot)});
                    break;
                default:  // Sum / Min / Max
                    if (plan_[i].kind == ExprType::Double) {
                        append_scalar(column, ScalarValue{slot.double_value});
                    } else {
                        append_scalar(column, ScalarValue{slot.int_value});
                    }
                    break;
            }
        }
        ++pending_rows_;
    }

    auto take_pending() -> Chunk {
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            if (track_validity_[i] == 0U || out_validity_[i].empty()) {
                continue;
            }
            bool has_null = false;
            for (std::size_t r = 0; r < out_validity_[i].size(); ++r) {
                if (!out_validity_[i][r]) {
                    has_null = true;
                    break;
                }
            }
            if (has_null) {
                out_columns_[group_by_->size() + i].validity = std::move(out_validity_[i]);
            }
        }
        Chunk out;
        out.columns = std::move(out_columns_);
        if (!out_ordering_.empty()) {
            out.ordering = out_ordering_;
        }
        reset_output();
        return out;
    }

    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* group_by_;
    const std::vector<ir::AggSpec>* aggregations_;

    bool decided_ = false;
    bool done_ = false;
    bool input_eof_ = false;
    bool open_ = false;
    OperatorPtr fallback_;

    static constexpr std::size_t kEmitThreshold = 8192;

    std::size_t n_aggs_ = 0;
    std::vector<SlotPlan> plan_;
    std::vector<ColumnValue> key_templates_;
    std::vector<std::uint8_t> track_validity_;
    std::vector<ir::OrderKey> out_ordering_;

    // Open-group state.
    std::vector<AggSlot> cur_slots_;
    std::vector<ScalarValue> open_key_;

    // Output buffers for closed groups awaiting emission.
    std::vector<ColumnEntry> out_columns_;
    std::vector<ValidityBitmap> out_validity_;
    std::size_t pending_rows_ = 0;
};

auto build_operator(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    ModelResult* model_out) -> std::expected<OperatorPtr, std::string>;

auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string> {
    MaterializeOperator sink{std::move(op)};
    return sink.run();
}

template <typename Fn>
auto build_unary_materializing_operator(const ir::Node& child_node, const TableRegistry& registry,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, ModelResult* model_out,
                                        Fn fn) -> std::expected<OperatorPtr, std::string> {
    auto child_op = build_operator(child_node, registry, scalars, externs, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    auto materialized = materialize_operator(std::move(child_op.value()));
    if (!materialized.has_value()) {
        return std::unexpected(std::move(materialized.error()));
    }
    auto result = fn(std::move(materialized.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return std::make_unique<TableSourceOperator>(std::move(result.value()));
}

template <typename Fn>
auto build_binary_materializing_operator(const ir::Node& left_node, const ir::Node& right_node,
                                         const TableRegistry& registry,
                                         const ScalarRegistry* scalars,
                                         const ExternRegistry* externs, ModelResult* model_out,
                                         Fn fn) -> std::expected<OperatorPtr, std::string> {
    auto left_op = build_operator(left_node, registry, scalars, externs, model_out);
    if (!left_op.has_value()) {
        return std::unexpected(std::move(left_op.error()));
    }
    auto right_op = build_operator(right_node, registry, scalars, externs, model_out);
    if (!right_op.has_value()) {
        return std::unexpected(std::move(right_op.error()));
    }
    auto left = materialize_operator(std::move(left_op.value()));
    if (!left.has_value()) {
        return std::unexpected(std::move(left.error()));
    }
    auto right = materialize_operator(std::move(right_op.value()));
    if (!right.has_value()) {
        return std::unexpected(std::move(right.error()));
    }
    auto result = fn(std::move(left.value()), std::move(right.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return std::make_unique<TableSourceOperator>(std::move(result.value()));
}

auto eval_extern_args(const std::vector<ir::Expr>& exprs, const ScalarRegistry* scalars,
                      const ExternRegistry* externs) -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(exprs.size());
    for (const auto& arg : exprs) {
        auto val = eval_expr(arg, Table{}, 0, scalars, externs);
        if (!val.has_value()) {
            return std::unexpected(std::move(val.error()));
        }
        args.push_back(std::move(val.value()));
    }
    return args;
}

auto invoke_extern_call(const ir::ExternCallNode& ec, const ScalarRegistry* scalars,
                        const ExternRegistry* externs) -> std::expected<ExternValue, std::string> {
    if (externs == nullptr) {
        return std::unexpected("extern call with no registry: " + ec.callee());
    }
    const auto* fn = externs->find(ec.callee());
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + ec.callee());
    }
    if (fn->first_arg_is_table) {
        return std::unexpected("extern function requires a table input: " + ec.callee());
    }
    auto args = eval_extern_args(ec.args(), scalars, externs);
    if (!args.has_value()) {
        return std::unexpected(std::move(args.error()));
    }
    if (fn->kind == ExternReturnKind::Table && fn->chunked_table_func) {
        auto source = fn->chunked_table_func(args.value());
        if (source.has_value()) {
            auto materialized = materialize_operator(std::move(source.value()));
            if (!materialized.has_value()) {
                return std::unexpected(std::move(materialized.error()));
            }
            return ExternValue{std::move(materialized.value())};
        }
    }
    auto result = fn->func(args.value());
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return result;
}

auto execute_program_preamble(const std::vector<ir::NodePtr>& preamble,
                              const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<void, std::string> {
    for (const auto& node : preamble) {
        if (node->kind() != ir::NodeKind::ExternCall) {
            return std::unexpected("program preamble only supports extern calls");
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& ec = static_cast<const ir::ExternCallNode&>(*node);
        auto result = invoke_extern_call(ec, scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
    }
    return {};
}

/// Planner seam: returns a pull-based operator that, when drained,
/// produces the logical result of `node`. Chunked operators exist
/// today for node kinds that are safe and useful to stream; any other
/// node kind falls back to the full-table `interpret_node` path and
/// is wrapped in a `TableSourceOperator` so downstream chunked
/// operators see a uniform pull-based interface.
// NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
// Order-delay past Filter/Project/Rename, and Head/Tail pushdown past
// Project/Rename, are handled by the IR canonicalize pass
// (src/ir/canonicalize.cpp). IR arrives here in canonical form, so
// build_operator only needs one branch per NodeKind and the shapes it
// matches are the post-canonicalization shapes (e.g. Project(Filter(x))
// for the fused operator, not Project(Filter(Order(x)))).

auto build_operator(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    if (node.kind() == ir::NodeKind::Filter) {
        const auto& filter = static_cast<const ir::FilterNode&>(node);
        if (filter.children().empty()) {
            return std::unexpected("filter node missing child");
        }
        auto child_op =
            build_operator(*filter.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterOperator>(std::move(child_op.value()),
                                                       &filter.predicate(), scalars);
    }

    if (node.kind() == ir::NodeKind::Project) {
        const auto& project = static_cast<const ir::ProjectNode&>(node);
        if (project.children().empty()) {
            return std::unexpected("project node missing child");
        }
        auto child_op =
            build_operator(*project.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedProjectOperator>(std::move(child_op.value()),
                                                        &project.columns());
    }

    // Fused Project(Filter(x)) produced by canonicalize R5.
    if (node.kind() == ir::NodeKind::FilterProject) {
        const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
        if (fp.children().empty()) {
            return std::unexpected("filter_project node missing child");
        }
        auto child_op =
            build_operator(*fp.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterProjectOperator>(
            std::move(child_op.value()), &fp.predicate(), &fp.columns(), scalars);
    }

    // Fused Head(Filter(x)) / Tail(Filter(x)) produced by canonicalize R7/R8.
    if (node.kind() == ir::NodeKind::FilterHead) {
        const auto& fh = static_cast<const ir::FilterHeadNode&>(node);
        if (fh.children().empty()) {
            return std::unexpected("filter_head node missing child");
        }
        auto child_op =
            build_operator(*fh.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterHeadOperator>(std::move(child_op.value()),
                                                           &fh.predicate(), fh.count(), scalars);
    }
    if (node.kind() == ir::NodeKind::FilterTail) {
        const auto& ft = static_cast<const ir::FilterTailNode&>(node);
        if (ft.children().empty()) {
            return std::unexpected("filter_tail node missing child");
        }
        auto child_op =
            build_operator(*ft.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterTailOperator>(std::move(child_op.value()),
                                                           &ft.predicate(), ft.count(), scalars);
    }

    // Fused Project(Update(Filter(x))) produced by canonicalize R6. The
    // gather set (columns the update reads ∪ projected columns not produced
    // by the update) is recomputed here from the node payload.
    if (node.kind() == ir::NodeKind::FilterUpdateProject) {
        const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
        if (fup.children().empty()) {
            return std::unexpected("filter_update_project node missing child");
        }
        std::unordered_set<std::string> update_outputs;
        std::unordered_set<std::string> needed;
        for (const auto& f : fup.fields()) {
            update_outputs.insert(f.alias);
            collect_expr_column_refs(f.expr, needed);
        }
        for (const auto& col : fup.project_columns()) {
            if (update_outputs.find(col.name) == update_outputs.end()) {
                needed.insert(col.name);
            }
        }
        std::vector<ir::ColumnRef> gather_cols;
        gather_cols.reserve(needed.size());
        for (const auto& name : needed) {
            gather_cols.push_back(ir::ColumnRef{.name = name});
        }
        auto child_op =
            build_operator(*fup.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterUpdateProjectOperator>(
            std::move(child_op.value()), &fup.predicate(), &fup.fields(), &fup.project_columns(),
            std::move(gather_cols), scalars, externs);
    }

    if (node.kind() == ir::NodeKind::Rename) {
        const auto& rename = static_cast<const ir::RenameNode&>(node);
        if (rename.children().empty()) {
            return std::unexpected("rename node missing child");
        }
        auto child_op =
            build_operator(*rename.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedRenameOperator>(std::move(child_op.value()),
                                                       &rename.renames());
    }

    if (node.kind() == ir::NodeKind::ExternCall && externs != nullptr) {
        const auto& ec = static_cast<const ir::ExternCallNode&>(node);
        const auto* fn = externs->find(ec.callee());
        if (fn != nullptr && fn->chunked_table_func) {
            ExternArgs args;
            args.reserve(ec.args().size());
            bool args_ok = true;
            for (const auto& arg : ec.args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val.has_value()) {
                    args_ok = false;
                    break;
                }
                args.push_back(std::move(val.value()));
            }
            if (args_ok) {
                auto op = fn->chunked_table_func(args);
                if (op.has_value()) {
                    return std::move(op.value());
                }
            }
        }
    }

    if (node.kind() == ir::NodeKind::Distinct) {
        if (node.children().empty()) {
            return std::unexpected("distinct node missing child");
        }
        auto child_op =
            build_operator(*node.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedDistinctOperator>(std::move(child_op.value()));
    }

    if (node.kind() == ir::NodeKind::Order) {
        const auto& order = static_cast<const ir::OrderNode&>(node);
        if (order.children().empty()) {
            return std::unexpected("order node missing child");
        }
        auto child_op =
            build_operator(*order.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedOrderOperator>(std::move(child_op.value()), &order.keys());
    }

    if (node.kind() == ir::NodeKind::Aggregate) {
        const auto& agg = static_cast<const ir::AggregateNode&>(node);
        if (agg.children().empty()) {
            return std::unexpected("aggregate node missing child");
        }
        bool streamable = true;
        for (const auto& spec : agg.aggregations()) {
            switch (spec.func) {
                case ir::AggFunc::Count:
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                case ir::AggFunc::Mean:
                case ir::AggFunc::Stddev:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    break;
                default:
                    // Median/Quantile need all values; First/Last need
                    // type-preserving output; Ewma is row-order coupled — these
                    // stay on the materializing path.
                    streamable = false;
                    break;
            }
            if (!streamable) {
                break;
            }
        }
        if (streamable) {
            auto child_op =
                build_operator(*agg.children().front(), registry, scalars, externs, model_out);
            if (!child_op.has_value()) {
                return std::unexpected(std::move(child_op.error()));
            }
            // The sorted operator streams group-at-a-time when the child's
            // chunks arrive sorted on the group keys, and otherwise replays the
            // first chunk into a hash ChunkedAggregateOperator — so it is safe
            // to route the whole streamable subset here.
            return std::make_unique<ChunkedSortedAggregateOperator>(
                std::move(child_op.value()), &agg.group_by(), &agg.aggregations());
        }
    }

    if (node.kind() == ir::NodeKind::TopK) {
        // Fused Head(Order(x)) / Tail(Order(x)) — canonicalize R16. The
        // chunked implementation uses a partial heap-select (O(n log k)).
        const auto& topk = static_cast<const ir::TopKNode&>(node);
        if (topk.children().empty()) {
            return std::unexpected("topk node missing child");
        }
        auto child_op =
            build_operator(*topk.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        const auto keep = (topk.keep_mode() == ir::TopKNode::KeepMode::First)
                              ? ChunkedOrderedLimitOperator::KeepMode::First
                              : ChunkedOrderedLimitOperator::KeepMode::Last;
        return std::make_unique<ChunkedOrderedLimitOperator>(
            std::move(child_op.value()), &topk.keys(), topk.count(), &topk.group_by(), keep);
    }

    if (node.kind() == ir::NodeKind::Head) {
        const auto& head = static_cast<const ir::HeadNode&>(node);
        if (head.children().empty()) {
            return std::unexpected("head node missing child");
        }
        auto count = evaluate_row_count_expr_impl(head.count_expr(), scalars, externs);
        if (!count.has_value()) {
            return std::unexpected(count.error());
        }
        // Head(Order(x)) is rewritten by canonicalize R16 into TopK(x);
        // Head(Filter(x)) with no group_by is rewritten by R7 into FilterHead(x);
        // Head past Project/Rename is handled by R4.
        auto child_op =
            build_operator(*head.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedHeadOperator>(std::move(child_op.value()), *count,
                                                     &head.group_by());
    }

    if (node.kind() == ir::NodeKind::Tail) {
        const auto& tail = static_cast<const ir::TailNode&>(node);
        if (tail.children().empty()) {
            return std::unexpected("tail node missing child");
        }
        auto count = evaluate_row_count_expr_impl(tail.count_expr(), scalars, externs);
        if (!count.has_value()) {
            return std::unexpected(count.error());
        }
        // Tail(Order(x)) → TopK via R16; Tail(Filter(x)) no-group_by → FilterTail via R8;
        // Tail past Project/Rename via R4.
        return build_unary_materializing_operator(
            *tail.children().front(), registry, scalars, externs, model_out,
            [&](Table input) { return tail_table(input, *count, tail.group_by()); });
    }

    if (node.kind() == ir::NodeKind::Columns) {
        if (node.children().empty()) {
            return std::unexpected("columns node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, model_out,
                                                  [](Table input) { return columns_table(input); });
    }

    if (node.kind() == ir::NodeKind::Melt) {
        const auto& mn = static_cast<const ir::MeltNode&>(node);
        if (mn.children().empty()) {
            return std::unexpected("melt node missing child");
        }
        return build_unary_materializing_operator(
            *mn.children().front(), registry, scalars, externs, model_out,
            [&](Table input) { return melt_table(input, mn.id_columns(), mn.measure_columns()); });
    }

    if (node.kind() == ir::NodeKind::Dcast) {
        const auto& dn = static_cast<const ir::DcastNode&>(node);
        if (dn.children().empty()) {
            return std::unexpected("dcast node missing child");
        }
        return build_unary_materializing_operator(
            *dn.children().front(), registry, scalars, externs, model_out, [&](Table input) {
                return dcast_table(input, dn.pivot_column(), dn.value_column(), dn.row_keys());
            });
    }

    if (node.kind() == ir::NodeKind::Cov) {
        if (node.children().empty()) {
            return std::unexpected("cov node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, model_out,
                                                  [](Table input) { return cov_table(input); });
    }

    if (node.kind() == ir::NodeKind::Corr) {
        if (node.children().empty()) {
            return std::unexpected("corr node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, model_out,
                                                  [](Table input) { return corr_table(input); });
    }

    if (node.kind() == ir::NodeKind::Transpose) {
        if (node.children().empty()) {
            return std::unexpected("transpose node missing child");
        }
        return build_unary_materializing_operator(
            *node.children().front(), registry, scalars, externs, model_out,
            [](Table input) { return transpose_table(input); });
    }

    if (node.kind() == ir::NodeKind::Join) {
        const auto& join = static_cast<const ir::JoinNode&>(node);
        if (join.children().size() != 2) {
            return std::unexpected("join node expects exactly two children");
        }
        const bool streamable_semi_anti =
            (join.kind() == ir::JoinKind::Semi || join.kind() == ir::JoinKind::Anti) &&
            !join.predicate().has_value() && join.keys().size() == 1;
        if (streamable_semi_anti) {
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            auto right_op =
                build_operator(*join.children()[1], registry, scalars, externs, model_out);
            if (!right_op.has_value()) {
                return std::unexpected(std::move(right_op.error()));
            }
            auto right = materialize_operator(std::move(right_op.value()));
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return std::make_unique<ChunkedSemiAntiJoinOperator>(
                std::move(left_op.value()), std::move(right.value()), join.kind(), &join.keys());
        }
        const bool streamable_inner = join.kind() == ir::JoinKind::Inner &&
                                      !join.predicate().has_value() && join.keys().size() == 1;
        if (streamable_inner) {
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            auto right_op =
                build_operator(*join.children()[1], registry, scalars, externs, model_out);
            if (!right_op.has_value()) {
                return std::unexpected(std::move(right_op.error()));
            }
            auto right = materialize_operator(std::move(right_op.value()));
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return std::make_unique<ChunkedInnerJoinOperator>(
                std::move(left_op.value()), std::move(right.value()), &join.keys());
        }
        const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
        return build_binary_materializing_operator(
            *join.children()[0], *join.children()[1], registry, scalars, externs, model_out,
            [&](Table left, Table right) {
                return join_table_impl(left, right, join.kind(), join.keys(), pred, scalars,
                                       compute_mask);
            });
    }

    if (node.kind() == ir::NodeKind::Matmul) {
        if (node.children().size() != 2) {
            return std::unexpected("matmul node expects exactly two children");
        }
        return build_binary_materializing_operator(
            *node.children()[0], *node.children()[1], registry, scalars, externs, model_out,
            [](Table left, Table right) { return matmul_table(left, right); });
    }

    if (node.kind() == ir::NodeKind::Update) {
        const auto& update = static_cast<const ir::UpdateNode&>(node);
        if (update.children().empty()) {
            return std::unexpected("update node missing child");
        }
        if (update.guard() != nullptr) {
            return build_unary_materializing_operator(
                *update.children().front(), registry, scalars, externs, model_out,
                [&](Table input) -> std::expected<Table, std::string> {
                    return apply_guarded_update(std::move(input), update, scalars, externs);
                });
        }
        if (!update.group_by().empty()) {
            const bool all_rank = std::all_of(
                update.fields().begin(), update.fields().end(), [](const ir::FieldSpec& f) {
                    return std::holds_alternative<ir::RankExpr>(f.expr.node);
                });
            if (!all_rank && update.tuple_fields().empty()) {
                return build_unary_materializing_operator(
                    *update.children().front(), registry, scalars, externs, model_out,
                    [&](Table input) -> std::expected<Table, std::string> {
                        return grouped_update_table(std::move(input), update.fields(),
                                                    update.group_by(), scalars, externs);
                    });
            }
            if (!all_rank || !update.tuple_fields().empty()) {
                return std::unexpected(
                    "update + by: tuple-bound fields are not yet supported in grouped updates");
            }
            return build_unary_materializing_operator(
                *update.children().front(), registry, scalars, externs, model_out,
                [&](Table input) -> std::expected<Table, std::string> {
                    Table result = std::move(input);
                    for (const auto& field : update.fields()) {
                        const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node);
                        auto res = evaluate_rank_column(result, *rank, update.group_by());
                        if (!res) {
                            return std::unexpected(res.error());
                        }
                        if (res->validity.has_value()) {
                            result.add_column(field.alias, std::move(res->column),
                                              std::move(*res->validity));
                        } else {
                            result.add_column(field.alias, std::move(res->column));
                        }
                    }
                    return std::expected<Table, std::string>{std::move(result)};
                });
        }
        // Route to a streaming ChunkedUpdateOperator when every field is
        // row-local and there are no table-valued tuple assignments.
        const bool all_row_local =
            std::all_of(update.fields().begin(), update.fields().end(),
                        [](const ir::FieldSpec& f) { return is_row_local_update_expr(f.expr); });
        if (all_row_local && update.tuple_fields().empty()) {
            auto child_op =
                build_operator(*update.children().front(), registry, scalars, externs, model_out);
            if (!child_op.has_value()) {
                return std::unexpected(std::move(child_op.error()));
            }
            return std::make_unique<ChunkedUpdateOperator>(std::move(child_op.value()),
                                                           &update.fields(), scalars, externs);
        }
        auto child = build_unary_materializing_operator(
            *update.children().front(), registry, scalars, externs, model_out, [&](Table input) {
                return update_table(std::move(input), update.fields(), scalars, externs);
            });
        if (!child.has_value()) {
            return std::unexpected(std::move(child.error()));
        }
        auto result = materialize_operator(std::move(child.value()));
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        for (const auto& tspec : update.tuple_fields()) {
            auto src = interpret_node(*tspec.source, registry, scalars, externs);
            if (!src.has_value()) {
                return std::unexpected(std::move(src.error()));
            }
            if (tspec.aliases.empty()) {
                for (const auto& entry : src->columns) {
                    if (entry.validity) {
                        result->add_column(entry.name, *entry.column, *entry.validity);
                    } else {
                        result->add_column(entry.name, *entry.column);
                    }
                }
            } else {
                if (src->columns.size() != tspec.aliases.size()) {
                    return std::unexpected(
                        "tuple assignment: expected " + std::to_string(tspec.aliases.size()) +
                        " column(s), got " + std::to_string(src->columns.size()));
                }
                for (std::size_t i = 0; i < tspec.aliases.size(); ++i) {
                    const auto& entry = src->columns[i];
                    if (entry.validity) {
                        result->add_column(tspec.aliases[i], *entry.column, *entry.validity);
                    } else {
                        result->add_column(tspec.aliases[i], *entry.column);
                    }
                }
            }
        }
        return std::make_unique<TableSourceOperator>(std::move(result.value()));
    }

    if (node.kind() == ir::NodeKind::Resample) {
        const auto& rs = static_cast<const ir::ResampleNode&>(node);
        if (node.children().empty()) {
            return std::unexpected("resample node missing child");
        }
        return build_unary_materializing_operator(
            *node.children().front(), registry, scalars, externs, model_out, [&](Table input) {
                return resample_table(input, rs.duration(), rs.group_by(), rs.aggregations());
            });
    }

    if (node.kind() == ir::NodeKind::Window) {
        const auto& win = static_cast<const ir::WindowNode&>(node);
        if (node.children().empty()) {
            return std::unexpected("window node missing child");
        }
        const ir::Node& child_node = *node.children().front();
        if (child_node.kind() != ir::NodeKind::Update) {
            return std::unexpected(
                "window: only 'update' is currently supported inside a window block");
        }
        const auto& update_node = static_cast<const ir::UpdateNode&>(child_node);
        if (child_node.children().empty()) {
            return std::unexpected("window: update node missing child");
        }
        auto source_op =
            build_operator(*child_node.children().front(), registry, scalars, externs, model_out);
        if (!source_op.has_value()) {
            return std::unexpected(std::move(source_op.error()));
        }
        auto source = materialize_operator(std::move(source_op.value()));
        if (!source.has_value()) {
            return std::unexpected(std::move(source.error()));
        }
        if (!source->time_index.has_value()) {
            return std::unexpected(
                "window requires a TimeFrame — use as_timeframe() to designate a timestamp column");
        }
        auto result = update_node.group_by().empty()
                          ? windowed_update_table(std::move(source.value()), update_node.fields(),
                                                  win.duration(), scalars, externs)
                          : grouped_windowed_update_table(std::move(source.value()),
                                                          update_node.fields(), win.duration(),
                                                          update_node.group_by(), scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        return std::make_unique<TableSourceOperator>(std::move(result.value()));
    }

    if (node.kind() == ir::NodeKind::AsTimeframe) {
        const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
        if (node.children().empty()) {
            return std::unexpected("as_timeframe node missing child");
        }
        auto child_op =
            build_operator(*node.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedAsTimeframeOperator>(std::move(child_op.value()),
                                                            atf.column());
    }

    if (node.kind() == ir::NodeKind::Model) {
        const auto& mn = static_cast<const ir::ModelNode&>(node);
        if (mn.children().empty()) {
            return std::unexpected("model node missing child");
        }
        auto child_op =
            build_operator(*mn.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        auto input = materialize_operator(std::move(child_op.value()));
        if (!input.has_value()) {
            return std::unexpected(std::move(input.error()));
        }
        auto result =
            fit_model(input.value(), mn.formula(), mn.method(), mn.params(), scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        // Linear methods expose coefficients; tree models expose importance;
        // unsupervised models (e.g. kmeans) have neither, so fall back to the
        // per-row fitted output (e.g. cluster ids).
        Table primary = !result.value().coefficients.columns.empty() ? result.value().coefficients
                        : !result.value().importance.columns.empty() ? result.value().importance
                                                                     : result.value().fitted_values;
        if (model_out != nullptr) {
            *model_out = std::move(result.value());
        }
        return std::make_unique<TableSourceOperator>(std::move(primary));
    }

    if (node.kind() == ir::NodeKind::Construct || node.kind() == ir::NodeKind::Stream) {
        auto table = interpret_node(node, registry, scalars, externs, model_out);
        if (!table.has_value()) {
            return std::unexpected(std::move(table.error()));
        }
        return std::make_unique<TableSourceOperator>(std::move(table.value()));
    }

    if (node.kind() == ir::NodeKind::Program) {
        const auto& program = static_cast<const ir::ProgramNode&>(node);
        auto preamble = execute_program_preamble(program.preamble(), scalars, externs);
        if (!preamble.has_value()) {
            return std::unexpected(std::move(preamble.error()));
        }
        return build_operator(program.main_node(), registry, scalars, externs, model_out);
    }

    // Remaining node kinds fall through to interpret_node. Scan is already
    // handled as a source by the caller.
    auto table = interpret_node(node, registry, scalars, externs, model_out);
    if (!table.has_value()) {
        return std::unexpected(std::move(table.error()));
    }
    return std::make_unique<TableSourceOperator>(std::move(table.value()));
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace

auto interpret(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
               const ExternRegistry* externs, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    tune_allocator_once();
    auto op = build_operator(node, registry, scalars, externs, model_out);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    MaterializeOperator sink{std::move(op.value())};
    return sink.run();
}

auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                 const std::vector<std::string>& keys, const ir::Expr* predicate,
                 const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    return join_table_impl(left, right, kind, keys, predicate, scalars, compute_mask);
}

auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string> {
    if (table.rows() != 1) {
        return std::unexpected("scalar() requires exactly one row");
    }
    const auto* col = table.find(column);
    if (col == nullptr) {
        return std::unexpected("column not found: " + column);
    }
    return scalar_from_column(*col, 0);
}

}  // namespace ibex::runtime
