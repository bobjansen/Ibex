#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/rng.hpp>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <emmintrin.h>
#include <immintrin.h>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <robin_hood.h>
#include <set>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ibex::runtime {

namespace {

auto is_simple_identifier(std::string_view name) -> bool {
    if (name.empty()) {
        return false;
    }
    auto is_alpha = [](unsigned char ch) -> bool {
        return std::isalpha(ch) != 0;
    };
    auto is_alnum = [](unsigned char ch) -> bool {
        return std::isalnum(ch) != 0;
    };
    unsigned char first = static_cast<unsigned char>(name.front());
    if (!is_alpha(first) && first != '_') {
        return false;
    }
    for (std::size_t i = 1; i < name.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(name[i]);
        if (!is_alnum(ch) && ch != '_') {
            return false;
        }
    }
    return true;
}

auto format_columns(const Table& table) -> std::string {
    if (table.columns.empty()) {
        return "<none>";
    }
    std::string out;
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        const auto& name = table.columns[i].name;
        if (is_simple_identifier(name)) {
            out.append(name);
        } else {
            out.push_back('`');
            out.append(name);
            out.push_back('`');
        }
    }
    return out;
}

auto ordering_keys_present(const std::vector<ir::OrderKey>& keys,
                           const std::unordered_map<std::string, std::size_t>& index) -> bool {
    for (const auto& key : keys) {
        if (!index.contains(key.name)) {
            return false;
        }
    }
    return true;
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

auto normalize_time_index(Table& table) -> void {
    if (!table.time_index.has_value()) {
        return;
    }
    if (table.ordering.has_value() && table.ordering->size() == 1 &&
        table.ordering->front().name == *table.time_index && table.ordering->front().ascending) {
        return;
    }
    table.ordering = std::vector<ir::OrderKey>{{.name = *table.time_index, .ascending = true}};
}

auto int64_to_date_checked(std::int64_t value) -> Date {
    if (value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
        throw std::runtime_error("date out of range");
    }
    return Date{static_cast<std::int32_t>(value)};
}

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

auto column_size(const ColumnValue& column) -> std::size_t {
    return std::visit([](const auto& col) { return col.size(); }, column);
}

[[noreturn]] void invariant_violation(std::string_view detail) {
    // This is triggered by a severe bug, everything in here is on a best effort basis
    (void)std::fputs("ibex internal invariant violated (runtime/interpreter): ", stderr);
    (void)std::fwrite(detail.data(), sizeof(char), detail.size(), stderr);
    (void)std::fputc('\n', stderr);
    std::abort();
}

auto append_value(ColumnValue& out, const ColumnValue& src, std::size_t index) -> void {
    std::visit(
        [&](auto& dst_col) {
            using ColType = std::decay_t<decltype(dst_col)>;
            const auto* src_col = std::get_if<ColType>(&src);
            if (src_col == nullptr) {
                invariant_violation("append_value: source/destination column type mismatch");
            }
            if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                dst_col.push_code(src_col->code_at(index));
            } else {
                dst_col.push_back((*src_col)[index]);
            }
        },
        out);
}

auto make_empty_like(const ColumnValue& src) -> ColumnValue {
    return std::visit(
        [](const auto& col) -> ColumnValue {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                return Column<Categorical>{col.dictionary_ptr(), col.index_ptr(), {}};
            }
            return ColType{};
        },
        src);
}

// ─── Vectorized filter ────────────────────────────────────────────────────────
//
// Instead of evaluating the FilterExpr tree once per row (N × tree-depth
// variant dispatches), we:
//   1. compute_mask()  — walk the tree once, producing a uint8_t[N] mask via
//                        tight typed loops the compiler can auto-vectorize.
//   2. gather()        — a single pass over each column, copying only the rows
//                        where mask[i] != 0.
//
// For the common column-vs-literal case (e.g. price > 500.0) the literal is
// held as a scalar — no broadcast allocation, just a hoisted constant in the
// comparison loop.

// Boolean filter mask with optional validity (for 3VL null propagation).
// valid == nullopt  → all rows are valid (common non-null path, zero overhead).
struct Mask {
    std::vector<uint8_t> value;
    std::optional<std::vector<uint8_t>> valid;  // nullopt = all rows valid

    void apply_validity(const ValidityBitmap* v, std::size_t n) {
        if (v == nullptr)
            return;
        valid.emplace(n, uint8_t{1});
        for (std::size_t i = 0; i < n; ++i)
            (*valid)[i] = static_cast<uint8_t>((*v)[i]);
    }
};

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
auto arith_into(ir::ArithmeticOp op, const L* __restrict__ lp, const R* __restrict__ rp,
                std::common_type_t<L, R>* __restrict__ dp, std::size_t n) -> void {
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
                    dp[i] = rp[i] ? static_cast<Out>(lp[i]) / static_cast<Out>(rp[i]) : Out{0};
            else
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = static_cast<Out>(lp[i]) / static_cast<Out>(rp[i]);
            break;
        case ir::ArithmeticOp::Mod:
            if constexpr (std::is_integral_v<Out>)
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = rp[i] ? static_cast<Out>(lp[i]) % static_cast<Out>(rp[i]) : Out{0};
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
    return std::unexpected("filter: arithmetic on string column");
}

// Element-wise comparison between a column and a scalar literal.
// The scalar is hoisted out of the loop — no broadcast allocation.
template <typename ColT, typename LitT>
auto cmp_col_scalar_into(ir::CompareOp op, const ColT* __restrict__ cp, LitT rv,
                         uint8_t* __restrict__ mp, std::size_t n) -> void {
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
auto cmp_col_scalar_into_double_op(const double* __restrict__ cp, double rv,
                                   uint8_t* __restrict__ mp, std::size_t n) -> void {
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

auto cmp_col_scalar_into_double(ir::CompareOp op, const double* __restrict__ cp, double rv,
                                uint8_t* __restrict__ mp, std::size_t n) -> void {
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
auto cmp_into(ir::CompareOp op, const L* __restrict__ lp, const R* __restrict__ rp,
              uint8_t* __restrict__ mp, std::size_t n) -> void {
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
                const auto lv = l->data()[i].days;
                const auto rv = r->data()[i].days;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[i] = lv == rv;
                        break;
                    case ir::CompareOp::Ne:
                        mp[i] = lv != rv;
                        break;
                    case ir::CompareOp::Lt:
                        mp[i] = lv < rv;
                        break;
                    case ir::CompareOp::Le:
                        mp[i] = lv <= rv;
                        break;
                    case ir::CompareOp::Gt:
                        mp[i] = lv > rv;
                        break;
                    case ir::CompareOp::Ge:
                        mp[i] = lv >= rv;
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
                const auto lv = l->data()[i].nanos;
                const auto rv = r->data()[i].nanos;
                switch (op) {
                    case ir::CompareOp::Eq:
                        mp[i] = lv == rv;
                        break;
                    case ir::CompareOp::Ne:
                        mp[i] = lv != rv;
                        break;
                    case ir::CompareOp::Lt:
                        mp[i] = lv < rv;
                        break;
                    case ir::CompareOp::Le:
                        mp[i] = lv <= rv;
                        break;
                    case ir::CompareOp::Gt:
                        mp[i] = lv > rv;
                        break;
                    case ir::CompareOp::Ge:
                        mp[i] = lv >= rv;
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

auto try_extract_numeric_cmp_spec(const ir::FilterExpr& expr, const Table& table)
    -> std::optional<NumericCmpSpec> {
    const auto* cmp = std::get_if<ir::FilterCmp>(&expr.node);
    if (cmp == nullptr) {
        return std::nullopt;
    }

    const ir::FilterColumn* col_node = nullptr;
    const ir::FilterLiteral* lit_node = nullptr;
    ir::CompareOp op = cmp->op;
    if (const auto* lcol = std::get_if<ir::FilterColumn>(&cmp->left->node)) {
        if (const auto* rlit = std::get_if<ir::FilterLiteral>(&cmp->right->node)) {
            col_node = lcol;
            lit_node = rlit;
        }
    }
    if (col_node == nullptr) {
        if (const auto* llit = std::get_if<ir::FilterLiteral>(&cmp->left->node)) {
            if (const auto* rcol = std::get_if<ir::FilterColumn>(&cmp->right->node)) {
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
    if (const auto* c = std::get_if<Column<std::int64_t>>(entry.column.get())) {
        spec.kind = NumericSpecKind::Int64;
        spec.i64 = c->data();
    } else if (const auto* c = std::get_if<Column<double>>(entry.column.get())) {
        spec.kind = NumericSpecKind::Double;
        spec.dbl = c->data();
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
auto cmp_pair_mask(const L* __restrict__ lhs_data, LLit lhs_lit, const R* __restrict__ rhs_data,
                   RLit rhs_lit, uint8_t* __restrict__ out, std::size_t n) -> void {
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

// Evaluate a value sub-expression over all n rows, returning a column.
// Returns a pointer into the table for simple column references (zero-copy),
// or an owned ColumnValue for computed intermediates.
auto eval_value_vec(const ir::FilterExpr& expr, const Table& table, const ScalarRegistry* scalars,
                    std::size_t n) -> std::expected<ColResult, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<ColResult, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::FilterColumn>) {
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
            } else if constexpr (std::is_same_v<T, ir::FilterLiteral>) {
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
            } else if constexpr (std::is_same_v<T, ir::FilterArith>) {
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
            } else {
                return std::unexpected("filter: not a value expression");
            }
        },
        expr.node);
}

// Compute a boolean Mask for all n rows, with 3-valued logic (3VL) for nulls.
// valid==nullopt means all rows are valid (common non-null path, zero overhead).
auto compute_mask(const ir::FilterExpr& expr, const Table& table, const ScalarRegistry* scalars,
                  std::size_t n) -> std::expected<Mask, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<Mask, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::FilterCmp>) {
                // Fast path: column/expr op literal (no broadcast needed).
                if (const auto* lit = std::get_if<ir::FilterLiteral>(&node.right->node)) {
                    auto lhs = eval_value_vec(*node.left, table, scalars, n);
                    if (!lhs)
                        return std::unexpected(lhs.error());
                    return compare_col_scalar(node.op, deref_col(*lhs), lit->value, n,
                                              lhs->get_validity());
                }
                // Fast path: literal op column/expr (flip the operator).
                if (const auto* lit = std::get_if<ir::FilterLiteral>(&node.left->node)) {
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
            } else if constexpr (std::is_same_v<T, ir::FilterAnd>) {
                // Fast path: two numeric (column cmp literal) terms without nulls.
                // Evaluate both comparisons and combine (AND) in a single pass.
                if (auto lspec = try_extract_numeric_cmp_spec(*node.left, table);
                    lspec.has_value()) {
                    if (auto rspec = try_extract_numeric_cmp_spec(*node.right, table);
                        rspec.has_value()) {
                        Mask fused;
                        fused.value.resize(n);
                        uint8_t* out = fused.value.data();
                        dispatch_numeric_cmp_pair_kernel<true>(*lspec, *rspec, out, n);
                        return fused;
                    }
                }

                // 3VL AND truth table:
                //   T & T = T (valid), T & F = F (valid), T & null = null
                //   F & T = F (valid), F & F = F (valid), F & null = F (valid!)
                //   null & T = null,   null & F = F (valid!), null & null = null
                auto left = compute_mask(*node.left, table, scalars, n);
                if (!left)
                    return std::unexpected(left.error());
                auto right = compute_mask(*node.right, table, scalars, n);
                if (!right)
                    return std::unexpected(right.error());
                const uint8_t* lp = left->value.data();
                const uint8_t* rp = right->value.data();
                for (std::size_t i = 0; i < n; ++i)
                    left->value[i] = lp[i] & rp[i];
                if (left->valid || right->valid) {
                    if (!left->valid)
                        left->valid.emplace(n, uint8_t{1});
                    if (!right->valid)
                        right->valid.emplace(n, uint8_t{1});
                    const uint8_t* lval = left->valid->data();
                    const uint8_t* rval = right->valid->data();
                    for (std::size_t i = 0; i < n; ++i) {
                        // Row is definitively false if either side is a known false
                        uint8_t a_false = lval[i] & (1U - lp[i]);
                        uint8_t b_false = rval[i] & (1U - rp[i]);
                        (*left->valid)[i] = (lval[i] & rval[i]) | a_false | b_false;
                    }
                }
                return std::move(*left);
            } else if constexpr (std::is_same_v<T, ir::FilterOr>) {
                // Fast path: two numeric (column cmp literal) terms without nulls.
                // Evaluate both comparisons and combine (OR) in a single pass.
                if (auto lspec = try_extract_numeric_cmp_spec(*node.left, table);
                    lspec.has_value()) {
                    if (auto rspec = try_extract_numeric_cmp_spec(*node.right, table);
                        rspec.has_value()) {
                        Mask fused;
                        fused.value.resize(n);
                        uint8_t* out = fused.value.data();
                        dispatch_numeric_cmp_pair_kernel<false>(*lspec, *rspec, out, n);
                        return fused;
                    }
                }

                // 3VL OR truth table:
                //   T | T = T (valid), T | F = T (valid), T | null = T (valid!)
                //   F | T = T (valid), F | F = F (valid), F | null = null
                //   null | T = T (valid!), null | F = null, null | null = null
                auto left = compute_mask(*node.left, table, scalars, n);
                if (!left)
                    return std::unexpected(left.error());
                auto right = compute_mask(*node.right, table, scalars, n);
                if (!right)
                    return std::unexpected(right.error());
                const uint8_t* lp = left->value.data();
                const uint8_t* rp = right->value.data();
                for (std::size_t i = 0; i < n; ++i)
                    left->value[i] = lp[i] | rp[i];
                if (left->valid || right->valid) {
                    if (!left->valid)
                        left->valid.emplace(n, uint8_t{1});
                    if (!right->valid)
                        right->valid.emplace(n, uint8_t{1});
                    const uint8_t* lval = left->valid->data();
                    const uint8_t* rval = right->valid->data();
                    for (std::size_t i = 0; i < n; ++i) {
                        // Row is definitively true if either side is a known true
                        uint8_t a_true = lval[i] & lp[i];
                        uint8_t b_true = rval[i] & rp[i];
                        (*left->valid)[i] = (lval[i] & rval[i]) | a_true | b_true;
                    }
                }
                return std::move(*left);
            } else if constexpr (std::is_same_v<T, ir::FilterNot>) {
                // NOT null = null; NOT true = false; NOT false = true
                auto mask = compute_mask(*node.operand, table, scalars, n);
                if (!mask)
                    return std::unexpected(mask.error());
                for (auto& v : mask->value)
                    v ^= 1U;
                // valid stays as-is (null propagates)
                return std::move(*mask);
            } else if constexpr (std::is_same_v<T, ir::FilterIsNull>) {
                // IS NULL: true where the column has no valid value
                Mask m;
                m.value.resize(n, uint8_t{0});
                if (const auto* col_node = std::get_if<ir::FilterColumn>(&node.operand->node)) {
                    auto it = table.index.find(col_node->name);
                    if (it != table.index.end()) {
                        const auto& entry = table.columns[it->second];
                        if (entry.validity.has_value()) {
                            const auto& bm = *entry.validity;
                            for (std::size_t i = 0; i < n; ++i)
                                m.value[i] = static_cast<uint8_t>(!bm[i]);
                        }
                        // no validity bitmap → all rows valid → none are null → stays 0
                    }
                    return m;
                }
                return std::unexpected("filter: 'is null' operand must be a column reference");
            } else if constexpr (std::is_same_v<T, ir::FilterIsNotNull>) {
                // IS NOT NULL: true where the column has a valid value
                Mask m;
                m.value.resize(n, uint8_t{1});
                if (const auto* col_node = std::get_if<ir::FilterColumn>(&node.operand->node)) {
                    auto it = table.index.find(col_node->name);
                    if (it != table.index.end()) {
                        const auto& entry = table.columns[it->second];
                        if (entry.validity.has_value()) {
                            const auto& bm = *entry.validity;
                            for (std::size_t i = 0; i < n; ++i)
                                m.value[i] = static_cast<uint8_t>(bm[i]);
                        }
                        // no validity bitmap → all rows valid → all are not null → stays 1
                    }
                    return m;
                }
                return std::unexpected("filter: 'is not null' operand must be a column reference");
            } else {
                // FilterColumn, FilterLiteral, FilterArith — not boolean expressions
                return std::unexpected("filter: not a boolean expression");
            }
        },
        expr.node);
}

auto filter_table(const Table& input, const ir::FilterExpr& predicate,
                  const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    const std::size_t n = input.rows();

    auto mask_result = compute_mask(predicate, input, scalars, n);
    if (!mask_result)
        return std::unexpected(mask_result.error());

    // 3VL: keep row iff value[i]==1 AND (no valid vector OR valid[i]==1)
    const uint8_t* mp = mask_result->value.data();
    const uint8_t* vp = mask_result->valid ? mask_result->valid->data() : nullptr;

    // Block-wise compaction: keep bits per 64-row chunk + popcount for out size.
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
        keep_words[w] = bits;
        out_n += static_cast<std::size_t>(std::popcount(bits));
    }

    // Gather: for each column, copy only the matching rows.
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(*entry.column));
    }

    auto for_each_selected = [&](auto&& fn) {
        for (std::size_t w = 0; w < n_words; ++w) {
            std::uint64_t bits = keep_words[w];
            const std::size_t base = w * 64;
            while (bits != 0) {
                const unsigned bit = std::countr_zero(bits);
                fn(base + static_cast<std::size_t>(bit));
                bits &= (bits - 1);
            }
        }
    };

    auto copy_column = [&](std::size_t col_idx) {
        const auto& src_entry = input.columns[col_idx];
        auto& dst_entry = output.columns[col_idx];
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
                    // Two-pass flat-buffer gather: compute total bytes, then bulk-memcpy slabs.
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

    for (std::size_t c = 0; c < input.columns.size(); ++c) {
        copy_column(c);
    }

    // Propagate validity bitmaps using the same selected row set.
    for (std::size_t c = 0; c < input.columns.size(); ++c) {
        if (input.columns[c].validity.has_value()) {
            const auto& src_bm = *input.columns[c].validity;
            ValidityBitmap dst_bm(out_n, false);
            std::size_t j = 0;
            for_each_selected([&](std::size_t si) { dst_bm.set(j++, src_bm[si]); });
            output.columns[c].validity = std::move(dst_bm);
        }
    }

    output.ordering = input.ordering;
    output.time_index = input.time_index;
    normalize_time_index(output);
    return output;
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
        output.add_column(col.name, *entry->column);
        if (entry->validity.has_value()) {
            output.columns.back().validity = entry->validity;
        }
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
        output.add_column(out_name, *entry.column);
        if (entry.validity.has_value()) {
            output.columns.back().validity = entry.validity;
        }
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

// Transparent hasher so unordered_map<string, ...> can be looked up via string_view.
// hash<string_view> == hash<string> for equal content (C++17 guarantee).
struct StringViewHash {
    using is_transparent = void;
    auto operator()(std::string_view sv) const noexcept -> std::size_t {
        return std::hash<std::string_view>{}(sv);
    }
};

struct StringViewEq {
    using is_transparent = void;
    auto operator()(std::string_view a, std::string_view b) const noexcept -> bool {
        return a == b;
    }
};

enum class ExprType : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Date,
    Timestamp,
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
    double m2 = 0.0;     ///< Welford M2 accumulator for sample stddev.
    double param = 0.0;  ///< Function-specific parameter (e.g. EWMA alpha).
    ScalarValue first_value;
    ScalarValue last_value;
    std::vector<double> values;  ///< Collected values for median.
};

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

auto scalar_from_column(const ColumnValue& column, std::size_t row) -> ScalarValue {
    return std::visit(
        [&](const auto& col) -> ScalarValue {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColType, Column<Categorical>> ||
                          std::is_same_v<ColType, Column<std::string>>) {
                return std::string(col[row]);
            } else if constexpr (std::is_same_v<ColType, Column<Date>> ||
                                 std::is_same_v<ColType, Column<Timestamp>>) {
                return col[row];
            } else {
                return col[row];
            }
        },
        column);
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
            append_value(*output.columns[col].column, *input.columns[col].column, row);
        }
    }
    output.ordering.reset();
    output.time_index.reset();
    return output;
}

auto column_kind(const ColumnValue& column) -> ExprType;

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

    std::array<std::size_t, 256> cnt;
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
        constexpr std::size_t kPrefetchDist = 8;
        for (std::size_t i = 0; i < rows; ++i) {
            if (i + kPrefetchDist < rows) {
                std::size_t pb = (src_keys[i + kPrefetchDist] >> shift) & 0xFFU;
                __builtin_prefetch(&dst_keys[cnt[pb]], 1, 1);
                __builtin_prefetch(&dst_idx[cnt[pb]], 1, 1);
            }
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
        FlatKind kind;
        std::vector<std::uint64_t> u64;  // Int / Date.days / Timestamp.nanos, sign-flipped
        std::vector<double> f64;
        std::vector<std::string_view> str;  // views into original column storage
        bool ascending;
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

    // Column-major gather parameterised on index type (uint32_t or size_t).
    auto do_gather = [&]<typename Idx>(const std::vector<Idx>& idx) -> Table {
        Table output;
        output.columns.reserve(input.columns.size());
        for (const auto& entry : input.columns) {
            ColumnValue gathered = std::visit(
                [&](const auto& src) -> ColumnValue {
                    using ColT = std::decay_t<decltype(src)>;
                    if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                        // Gather codes only; dictionary and index are shared.
                        std::vector<Column<Categorical>::code_type> codes(rows);
                        const auto* sp = src.codes_data();
                        for (std::size_t pos = 0; pos < rows; ++pos)
                            codes[pos] = sp[static_cast<std::size_t>(idx[pos])];
                        return Column<Categorical>(src.dictionary_ptr(), src.index_ptr(),
                                                   std::move(codes));
                    } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                        // Two-pass flat-buffer gather: compute total bytes, then memcpy slabs.
                        const auto* src_off = src.offsets_data();
                        const auto* src_char = src.chars_data();
                        std::size_t total_chars = 0;
                        for (std::size_t pos = 0; pos < rows; ++pos) {
                            std::size_t si = static_cast<std::size_t>(idx[pos]);
                            total_chars += src_off[si + 1] - src_off[si];
                        }
                        ColT dst;
                        dst.resize_for_gather(rows, total_chars);
                        auto* dst_off = dst.offsets_data();
                        auto* dst_char = dst.chars_data();
                        dst_off[0] = 0;
                        std::uint32_t cur = 0;
                        for (std::size_t pos = 0; pos < rows; ++pos) {
                            std::size_t si = static_cast<std::size_t>(idx[pos]);
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
        output.ordering = std::move(resolved_keys);
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    };

    // Fast path: single ascending I64 key — radix sort (pre-sorted case already handled above).
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::I64 && flat_keys[0].ascending) {
        auto sort_result = radix_sort_u64_asc(std::move(flat_keys[0].u64), rows);
        return std::visit(
            [&]<typename Idx>(const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                return do_gather(idx);
            },
            sort_result);
    }

    // General path: multi-key or non-I64 or descending — comparison-based stable sort.
    // u64 keys compare correctly with unsigned < because sign-flip preserves order.
    auto compare_row = [&](std::size_t lhs, std::size_t rhs) -> bool {
        for (const auto& fk : flat_keys) {
            switch (fk.kind) {
                case FlatKind::I64: {
                    auto l = fk.u64[lhs], r = fk.u64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::F64: {
                    auto l = fk.f64[lhs], r = fk.f64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::Str: {
                    auto l = fk.str[lhs], r = fk.str[rhs];
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
    return do_gather(idx);
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
    return std::visit(
        [](const auto& v) -> ScalarValue {
            using T = std::decay_t<decltype(v)>;
            return v;
        },
        literal.value);
}

auto resolve_fast_operand(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::optional<FastOperand> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (const auto* source = input.find(col->name); source != nullptr) {
            return FastOperand{true, source, ScalarValue{}, expr_type_for_column(*source)};
        }
        if (scalars != nullptr) {
            if (auto it = scalars->find(col->name); it != scalars->end()) {
                return FastOperand{false, nullptr, it->second, scalar_kind_from_value(it->second)};
            }
        }
        return std::nullopt;
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        ScalarValue value = scalar_from_literal(*lit);
        return FastOperand{false, nullptr, value, scalar_kind_from_value(value)};
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
            return lhs / rhs;
        case ir::ArithmeticOp::Mod:
            return lhs % rhs;
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
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a / b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Mod:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a % b; }, lp,
                                           ls, rp, rs);
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

auto column_kind(const ColumnValue& column) -> ExprType {
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

auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                     const std::vector<std::string>& keys,
                     const ir::FilterExpr* predicate = nullptr,
                     const ScalarRegistry* scalars = nullptr) -> std::expected<Table, std::string> {
    if (kind != ir::JoinKind::Cross && keys.empty() && predicate == nullptr) {
        return std::unexpected("join requires at least one key");
    }

    std::vector<const ColumnValue*> left_keys;
    std::vector<const ColumnValue*> right_keys;
    left_keys.reserve(keys.size());
    right_keys.reserve(keys.size());
    for (const auto& key : keys) {
        const auto* left_col = left.find(key);
        if (left_col == nullptr) {
            return std::unexpected("join key not found in left: " + key +
                                   " (available: " + format_columns(left) + ")");
        }
        const auto* right_col = right.find(key);
        if (right_col == nullptr) {
            return std::unexpected("join key not found in right: " + key +
                                   " (available: " + format_columns(right) + ")");
        }
        if (column_kind(*left_col) != column_kind(*right_col)) {
            return std::unexpected("join key type mismatch for " + key);
        }
        left_keys.push_back(left_col);
        right_keys.push_back(right_col);
    }

    std::optional<std::size_t> asof_time_key_pos;
    if (kind == ir::JoinKind::Asof) {
        if (!left.time_index.has_value() || !right.time_index.has_value()) {
            return std::unexpected("asof join requires both operands to be TimeFrame");
        }
        if (*left.time_index != *right.time_index) {
            return std::unexpected(
                "asof join requires both TimeFrames to share the same time index "
                "column");
        }
        const std::string& time_key = *left.time_index;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == time_key) {
                asof_time_key_pos = i;
                break;
            }
        }
        if (!asof_time_key_pos.has_value()) {
            return std::unexpected("asof join requires the 'on' keys to include the time index '" +
                                   time_key + "'");
        }
    }

    std::unordered_set<std::string> key_set(keys.begin(), keys.end());

    const bool preserve_left_rows =
        (kind == ir::JoinKind::Left || kind == ir::JoinKind::Outer || kind == ir::JoinKind::Asof);
    const bool preserve_right_rows = (kind == ir::JoinKind::Right || kind == ir::JoinKind::Outer);
    const bool semi_join = (kind == ir::JoinKind::Semi);
    const bool anti_join = (kind == ir::JoinKind::Anti);

    Table output;
    output.columns.reserve(left.columns.size() + right.columns.size());

    std::unordered_set<std::string> out_names;
    out_names.reserve(left.columns.size() + right.columns.size());

    for (const auto& entry : left.columns) {
        out_names.insert(entry.name);
        output.add_column(entry.name, make_empty_like(*entry.column));
    }

    struct RightOut {
        const ColumnValue* column = nullptr;
        std::size_t out_index = 0;
    };
    std::vector<RightOut> right_out;
    right_out.reserve(right.columns.size());
    for (const auto& entry : right.columns) {
        if (semi_join || anti_join || key_set.contains(entry.name)) {
            continue;
        }
        std::string name = entry.name;
        while (out_names.contains(name)) {
            name += "_right";
        }
        out_names.insert(name);
        output.add_column(name, make_empty_like(*entry.column));
        right_out.push_back(RightOut{entry.column.get(), output.columns.size() - 1});
    }

    if (kind == ir::JoinKind::Cross) {
        for (std::size_t l = 0; l < left.rows(); ++l) {
            for (std::size_t r = 0; r < right.rows(); ++r) {
                for (std::size_t i = 0; i < left.columns.size(); ++i) {
                    append_value(*output.columns[i].column, *left.columns[i].column, l);
                }
                for (const auto& item : right_out) {
                    append_value(*output.columns[item.out_index].column, *item.column, r);
                }
            }
        }
        normalize_time_index(output);
        return output;
    }

    // Validity bitmaps for left-side output columns.
    // Needed for Right/Outer joins where unmatched right rows make left columns null.
    const bool track_left_nulls = preserve_right_rows;
    bool left_had_nulls = false;
    std::vector<ValidityBitmap> left_validity;
    if (track_left_nulls) {
        left_validity.resize(left.columns.size());
    }

    auto append_left_row = [&](std::size_t row) {
        for (std::size_t i = 0; i < left.columns.size(); ++i) {
            append_value(*output.columns[i].column, *left.columns[i].column, row);
        }
        if (track_left_nulls) {
            for (auto& bm : left_validity)
                bm.push_back(true);
        }
    };

    // Validity bitmaps for right-side output columns.
    // Needed for Left/Asof/Outer joins where unmatched left rows make right columns null.
    const bool track_right_nulls = preserve_left_rows;
    bool right_had_nulls = false;
    std::vector<ValidityBitmap> right_validity;
    if (track_right_nulls) {
        right_validity.resize(right_out.size());
    }

    auto append_right_row = [&](std::size_t row) {
        for (const auto& item : right_out) {
            append_value(*output.columns[item.out_index].column, *item.column, row);
        }
        if (track_right_nulls) {
            for (auto& bm : right_validity)
                bm.push_back(true);
        }
    };

    auto append_right_defaults = [&]() {
        // Push type-default directly without going through ScalarValue to avoid
        // constructing a temporary std::string for string columns.
        for (const auto& item : right_out) {
            std::visit(
                [](auto& col) {
                    using ColType = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                        col.push_back(std::int64_t{0});
                    } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                        col.push_back(0.0);
                    } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                        col.push_back(std::string_view{});
                    } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                        col.push_code(0);  // placeholder — row is marked null by bitmap
                    } else {
                        col.push_back({});
                    }
                },
                *output.columns[item.out_index].column);
        }
        if (track_right_nulls) {
            right_had_nulls = true;
            for (auto& bm : right_validity)
                bm.push_back(false);
        }
    };

    auto append_left_defaults_from_right = [&](std::size_t right_row) {
        for (std::size_t i = 0; i < left.columns.size(); ++i) {
            const auto& left_entry = left.columns[i];
            if (key_set.contains(left_entry.name)) {
                const auto* right_key_col = right.find(left_entry.name);
                if (right_key_col == nullptr) {
                    throw std::runtime_error("join key not found in right during row emission: " +
                                             left_entry.name);
                }
                append_value(*output.columns[i].column, *right_key_col, right_row);
                if (track_left_nulls)
                    left_validity[i].push_back(true);
                continue;
            }

            std::visit(
                [](auto& col) {
                    using ColType = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                        col.push_back(std::int64_t{0});
                    } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                        col.push_back(0.0);
                    } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                        col.push_back(std::string_view{});
                    } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                        col.push_code(0);
                    } else {
                        col.push_back({});
                    }
                },
                *output.columns[i].column);
            if (track_left_nulls)
                left_validity[i].push_back(false);
        }
        if (track_left_nulls) {
            left_had_nulls = true;
        }
    };

    auto finalize_left_validity = [&]() {
        if (left_had_nulls) {
            for (std::size_t i = 0; i < left.columns.size(); ++i) {
                output.columns[i].validity = std::move(left_validity[i]);
            }
        }
    };

    auto finalize_right_validity = [&]() {
        if (right_had_nulls) {
            for (std::size_t i = 0; i < right_out.size(); ++i) {
                output.columns[right_out[i].out_index].validity = std::move(right_validity[i]);
            }
        }
    };

    // Build-left path for equijoins when the left side is smaller.
    // To preserve the language's row-ordering contract, scan the right side
    // twice: first count matches per left row and track matched right rows,
    // then fill a flat right-row buffer grouped by left row, and finally emit
    // left rows in left order before any unmatched right rows.
    auto emit_preserving_rows_from_right_scan = [&](auto&& for_each_left_match_for_right_row) {
        std::vector<std::size_t> match_counts(left.rows(), 0);
        std::size_t total_matches = 0;
        std::vector<std::uint8_t> right_matched_build_left;
        if (preserve_right_rows) {
            right_matched_build_left.assign(right.rows(), 0U);
        }

        for (std::size_t r = 0; r < right.rows(); ++r) {
            bool right_has_match = false;
            for_each_left_match_for_right_row(r, [&](std::size_t l) {
                ++match_counts[l];
                ++total_matches;
                right_has_match = true;
            });
            if (preserve_right_rows && right_has_match) {
                right_matched_build_left[r] = 1U;
            }
        }

        std::vector<std::size_t> match_offsets(left.rows() + 1, 0);
        std::size_t left_phase_rows = 0;
        for (std::size_t l = 0; l < left.rows(); ++l) {
            match_offsets[l + 1] = match_offsets[l] + match_counts[l];
            left_phase_rows +=
                match_counts[l] == 0 ? (preserve_left_rows ? 1U : 0U) : match_counts[l];
        }

        std::vector<std::size_t> right_matches(total_matches);
        std::vector<std::size_t> next_offsets = match_offsets;
        for (std::size_t r = 0; r < right.rows(); ++r) {
            for_each_left_match_for_right_row(
                r, [&](std::size_t l) { right_matches[next_offsets[l]++] = r; });
        }

        std::size_t unmatched_right_rows = 0;
        if (preserve_right_rows) {
            unmatched_right_rows = static_cast<std::size_t>(std::count(
                right_matched_build_left.begin(), right_matched_build_left.end(), std::uint8_t{0}));
        }
        const std::size_t total_output_rows = left_phase_rows + unmatched_right_rows;
        for (auto& ce : output.columns) {
            std::visit([total_output_rows](auto& c) { c.reserve(total_output_rows); }, *ce.column);
        }

        for (std::size_t l = 0; l < left.rows(); ++l) {
            if (match_counts[l] == 0) {
                if (preserve_left_rows) {
                    append_left_row(l);
                    append_right_defaults();
                }
                continue;
            }
            for (std::size_t idx = match_offsets[l]; idx < match_offsets[l + 1]; ++idx) {
                append_left_row(l);
                append_right_row(right_matches[idx]);
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < right.rows(); ++r) {
                if (right_matched_build_left[r] == 0U) {
                    append_left_defaults_from_right(r);
                    append_right_row(r);
                }
            }
        }
    };

    // Build-left path for semi/anti joins when the left side is smaller.
    // These joins only need left-row membership, so scan the right side once,
    // mark matching left rows, and then emit the left subset in left order.
    auto emit_membership_rows_from_right_scan = [&](auto&& for_each_left_match_for_right_row) {
        std::vector<std::uint8_t> left_matched(left.rows(), 0U);
        for (std::size_t r = 0; r < right.rows(); ++r) {
            for_each_left_match_for_right_row(r, [&](std::size_t l) { left_matched[l] = 1U; });
        }

        for (auto& ce : output.columns) {
            std::visit([n = left.rows()](auto& c) { c.reserve(n); }, *ce.column);
        }

        for (std::size_t l = 0; l < left.rows(); ++l) {
            const bool has_match = left_matched[l] != 0U;
            if ((semi_join && has_match) || (anti_join && !has_match)) {
                append_left_row(l);
            }
        }
    };

    // ─── Non-equijoin / theta-join path (predicate present) ──────────────────
    // Uses a nested-loop join: for each left row l, broadcast it N_right times
    // alongside the full right table, evaluate the predicate mask vectorised,
    // then emit matching pairs with the appropriate null-padding semantics.
    if (predicate != nullptr) {
        const std::size_t N_right = right.rows();

        // Build a right-column → batch-name mapping covering ALL right columns,
        // not just the output columns in right_out.  Semi/anti joins exclude
        // right columns from right_out but the predicate still references them.
        struct NLJRightCol {
            const ColumnValue* column;
            std::string batch_name;
        };
        std::vector<NLJRightCol> nlj_right;
        nlj_right.reserve(right.columns.size());
        {
            std::unordered_set<std::string> batch_left_names;
            for (const auto& e : left.columns)
                batch_left_names.insert(e.name);
            for (const auto& entry : right.columns) {
                std::string name = entry.name;
                while (batch_left_names.contains(name))
                    name += "_right";
                batch_left_names.insert(name);
                nlj_right.push_back({entry.column.get(), std::move(name)});
            }
        }

        std::vector<std::uint8_t> right_matched_pred;
        if (preserve_right_rows) {
            right_matched_pred.assign(N_right, 0U);
        }

        for (std::size_t l = 0; l < left.rows(); ++l) {
            // Build batch table: left row l repeated N_right times, right as-is.
            // Column names follow the same renaming convention as the cross join
            // (right columns that collide with left receive a "_right" suffix).
            Table batch;
            batch.columns.reserve(left.columns.size() + nlj_right.size());

            for (std::size_t ci = 0; ci < left.columns.size(); ++ci) {
                auto col = make_empty_like(*left.columns[ci].column);
                for (std::size_t j = 0; j < N_right; ++j) {
                    append_value(col, *left.columns[ci].column, l);
                }
                batch.add_column(output.columns[ci].name, std::move(col));
            }
            for (const auto& item : nlj_right) {
                batch.add_column(item.batch_name, *item.column);
            }

            // Evaluate the predicate against all N_right rows at once.
            auto mask_res = compute_mask(*predicate, batch, scalars, N_right);
            if (!mask_res) {
                return std::unexpected(mask_res.error());
            }

            bool l_had_match = false;
            for (std::size_t j = 0; j < N_right; ++j) {
                const bool match =
                    mask_res->value[j] != 0 && (!mask_res->valid || (*mask_res->valid)[j] != 0);
                if (!match)
                    continue;
                l_had_match = true;
                if (semi_join) {
                    append_left_row(l);
                    break;  // emit left row at most once
                }
                if (anti_join) {
                    break;  // match found — don't emit; skip remaining right rows
                }
                append_left_row(l);
                append_right_row(j);
                if (preserve_right_rows) {
                    right_matched_pred[j] = 1U;
                }
            }

            if (!l_had_match) {
                if (anti_join) {
                    append_left_row(l);  // anti-join: keep left rows with no match
                } else if (preserve_left_rows) {
                    append_left_row(l);
                    append_right_defaults();  // left/outer: null-pad right side
                }
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < N_right; ++r) {
                if (right_matched_pred[r] == 0U) {
                    append_left_defaults_from_right(r);
                    append_right_row(r);
                }
            }
        }

        finalize_left_validity();
        finalize_right_validity();
        normalize_time_index(output);
        return output;
    }
    // ─────────────────────────────────────────────────────────────────────────

    // Pre-reserve output columns for joins that preserve one side.
    if (preserve_left_rows || preserve_right_rows) {
        const auto n = std::max(left.rows(), right.rows());
        for (auto& ce : output.columns) {
            std::visit([n](auto& c) { c.reserve(n); }, *ce.column);
        }
        for (auto& bm : right_validity)
            bm.reserve(n);
    }

    // ─── Fast path: single-key string / categorical join ─────────────────────
    // Avoids constructing a Key struct + copying a std::string for every one of
    // the (potentially millions of) left rows.
    //
    // • Categorical left key: translate each dict entry once to a right-row
    //   pointer, then the hot loop becomes a plain integer array lookup — zero
    //   string hashing or allocation in the 4M-row pass.
    // • String left key: use std::string_view for transparent lookup, so no
    //   heap allocation per row (C++20 heterogeneous find).
    if (kind != ir::JoinKind::Asof && keys.size() == 1 &&
        (std::holds_alternative<Column<std::string>>(*left_keys[0]) ||
         std::holds_alternative<Column<Categorical>>(*left_keys[0]))) {
        if (left.rows() < right.rows()) {
            std::unordered_map<std::string_view, std::vector<std::size_t>, StringViewHash,
                               std::equal_to<std::string_view>>
                left_sv_index;
            left_sv_index.reserve(left.rows());
            if (const auto* ls = std::get_if<Column<std::string>>(left_keys[0])) {
                for (std::size_t l = 0; l < left.rows(); ++l) {
                    left_sv_index[(*ls)[l]].push_back(l);
                }
            } else if (const auto* lc = std::get_if<Column<Categorical>>(left_keys[0])) {
                for (std::size_t l = 0; l < left.rows(); ++l) {
                    left_sv_index[(*lc)[l]].push_back(l);
                }
            }

            auto emit_left_matches_for_right_row = [&](std::size_t r, auto&& emit_left_row) {
                std::string_view sv;
                if (const auto* rs = std::get_if<Column<std::string>>(right_keys[0])) {
                    sv = (*rs)[r];
                } else {
                    sv = std::get<Column<Categorical>>(*right_keys[0])[r];
                }
                auto it = left_sv_index.find(sv);
                if (it == left_sv_index.end()) {
                    return;
                }
                for (auto l : it->second) {
                    emit_left_row(l);
                }
            };

            if (semi_join || anti_join) {
                emit_membership_rows_from_right_scan(emit_left_matches_for_right_row);
            } else {
                emit_preserving_rows_from_right_scan(emit_left_matches_for_right_row);
            }
            finalize_left_validity();
            finalize_right_validity();
            return output;
        }

        // Build right-side index: string_view → matching right-row indices.
        // Keys are views into the right column's storage (stable during join).
        std::unordered_map<std::string_view, std::vector<std::size_t>, StringViewHash,
                           std::equal_to<std::string_view>>
            right_sv_index;
        right_sv_index.reserve(right.rows());
        if (const auto* rs = std::get_if<Column<std::string>>(right_keys[0])) {
            for (std::size_t r = 0; r < right.rows(); ++r)
                right_sv_index[(*rs)[r]].push_back(r);
        } else if (const auto* rc = std::get_if<Column<Categorical>>(right_keys[0])) {
            for (std::size_t r = 0; r < right.rows(); ++r)
                right_sv_index[(*rc)[r]].push_back(r);
        }

        std::vector<std::uint8_t> right_matched;
        if (preserve_right_rows) {
            right_matched.assign(right.rows(), 0U);
        }

        auto run_match = [&](std::string_view sv, std::size_t l) {
            auto it = right_sv_index.find(sv);
            if (it == right_sv_index.end()) {
                if (preserve_left_rows) {
                    append_left_row(l);
                    append_right_defaults();
                }
                return;
            }
            for (auto r : it->second) {
                append_left_row(l);
                append_right_row(r);
                if (preserve_right_rows) {
                    right_matched[r] = 1U;
                }
            }
        };

        if (const auto* lc = std::get_if<Column<Categorical>>(left_keys[0])) {
            // Translate each dictionary entry once → O(dict_size) string hashes.
            // Hot loop uses integer array indexing, no strings at all.
            const auto& dict = *lc->dictionary_ptr();
            std::vector<const std::vector<std::size_t>*> code_to_right(dict.size(), nullptr);
            for (std::size_t code = 0; code < dict.size(); ++code) {
                auto it = right_sv_index.find(std::string_view{dict[code]});
                if (it != right_sv_index.end())
                    code_to_right[code] = &it->second;
            }
            const auto* codes = lc->codes_data();
            for (std::size_t l = 0; l < left.rows(); ++l) {
                const auto* matches = code_to_right[static_cast<std::size_t>(codes[l])];
                if (matches == nullptr) {
                    if (preserve_left_rows) {
                        append_left_row(l);
                        append_right_defaults();
                    }
                    continue;
                }
                for (auto r : *matches) {
                    append_left_row(l);
                    append_right_row(r);
                    if (preserve_right_rows) {
                        right_matched[r] = 1U;
                    }
                }
            }
        } else {
            const auto& ls = std::get<Column<std::string>>(*left_keys[0]);
            for (std::size_t l = 0; l < left.rows(); ++l) {
                run_match(ls[l], l);  // string_view lookup — no allocation
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < right.rows(); ++r) {
                if (right_matched[r] == 0U) {
                    append_left_defaults_from_right(r);
                    append_right_row(r);
                }
            }
        }

        finalize_left_validity();
        finalize_right_validity();
        return output;
    }
    // ─────────────────────────────────────────────────────────────────────────

    // ─── Fast path: single-key Int64 join ────────────────────────────────────
    // Avoids constructing a Key { vector<ScalarValue> } for every row in the
    // common single-Int64 equijoin case.
    if (kind != ir::JoinKind::Asof && keys.size() == 1 &&
        std::holds_alternative<Column<std::int64_t>>(*left_keys[0])) {
        const auto& left_ints = std::get<Column<std::int64_t>>(*left_keys[0]);
        const auto& right_ints = std::get<Column<std::int64_t>>(*right_keys[0]);

        if (left.rows() < right.rows()) {
            std::unordered_map<std::int64_t, std::vector<std::size_t>> left_int_index;
            left_int_index.reserve(left.rows());
            for (std::size_t l = 0; l < left.rows(); ++l) {
                left_int_index[left_ints[l]].push_back(l);
            }

            auto emit_left_matches_for_right_row = [&](std::size_t r, auto&& emit_left_row) {
                auto it = left_int_index.find(right_ints[r]);
                if (it == left_int_index.end()) {
                    return;
                }
                for (auto l : it->second) {
                    emit_left_row(l);
                }
            };

            if (semi_join || anti_join) {
                emit_membership_rows_from_right_scan(emit_left_matches_for_right_row);
            } else {
                emit_preserving_rows_from_right_scan(emit_left_matches_for_right_row);
            }
            finalize_left_validity();
            finalize_right_validity();
            return output;
        }

        std::unordered_map<std::int64_t, std::vector<std::size_t>> right_int_index;
        right_int_index.reserve(right.rows());
        for (std::size_t r = 0; r < right.rows(); ++r) {
            right_int_index[right_ints[r]].push_back(r);
        }

        std::vector<std::uint8_t> right_matched;
        if (preserve_right_rows) {
            right_matched.assign(right.rows(), 0U);
        }

        for (std::size_t l = 0; l < left.rows(); ++l) {
            auto it = right_int_index.find(left_ints[l]);
            const bool has_match = (it != right_int_index.end());
            if (semi_join) {
                if (has_match) {
                    append_left_row(l);
                }
                continue;
            }
            if (anti_join) {
                if (!has_match) {
                    append_left_row(l);
                }
                continue;
            }
            if (!has_match) {
                if (preserve_left_rows) {
                    append_left_row(l);
                    append_right_defaults();
                }
                continue;
            }
            for (auto r : it->second) {
                append_left_row(l);
                append_right_row(r);
                if (preserve_right_rows) {
                    right_matched[r] = 1U;
                }
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < right.rows(); ++r) {
                if (right_matched[r] == 0U) {
                    append_left_defaults_from_right(r);
                    append_right_row(r);
                }
            }
        }

        finalize_left_validity();
        finalize_right_validity();
        return output;
    }
    // ─────────────────────────────────────────────────────────────────────────

    if (kind == ir::JoinKind::Asof) {
        const std::size_t time_pos = *asof_time_key_pos;
        const auto* left_time_col = left_keys[time_pos];
        const auto* right_time_col = right_keys[time_pos];

        auto time_value = [](const ColumnValue& col,
                             std::size_t row) -> std::optional<std::int64_t> {
            if (const auto* ts = std::get_if<Column<Timestamp>>(&col)) {
                return (*ts)[row].nanos;
            }
            if (const auto* day = std::get_if<Column<Date>>(&col)) {
                return static_cast<std::int64_t>((*day)[row].days);
            }
            if (const auto* ints = std::get_if<Column<std::int64_t>>(&col)) {
                return (*ints)[row];
            }
            return std::nullopt;
        };

        std::vector<std::int64_t> left_times;
        left_times.reserve(left.rows());
        for (std::size_t l = 0; l < left.rows(); ++l) {
            auto v = time_value(*left_time_col, l);
            if (!v.has_value()) {
                return std::unexpected(
                    "asof join requires a Timestamp/Date/Int time index column in left operand");
            }
            left_times.push_back(*v);
        }

        std::vector<std::int64_t> right_times;
        right_times.reserve(right.rows());
        for (std::size_t r = 0; r < right.rows(); ++r) {
            auto v = time_value(*right_time_col, r);
            if (!v.has_value()) {
                return std::unexpected(
                    "asof join requires a Timestamp/Date/Int time index column in right operand");
            }
            right_times.push_back(*v);
        }

        if (!std::is_sorted(left_times.begin(), left_times.end()) ||
            !std::is_sorted(right_times.begin(), right_times.end())) {
            return std::unexpected(
                "asof join requires both TimeFrames to be sorted ascending by "
                "their time index");
        }

        std::vector<const ColumnValue*> left_eq_keys;
        std::vector<const ColumnValue*> right_eq_keys;
        left_eq_keys.reserve(keys.size() - 1);
        right_eq_keys.reserve(keys.size() - 1);
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (i == time_pos) {
                continue;
            }
            left_eq_keys.push_back(left_keys[i]);
            right_eq_keys.push_back(right_keys[i]);
        }

        std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_groups;
        right_groups.reserve(right.rows());
        for (std::size_t r = 0; r < right.rows(); ++r) {
            Key group;
            group.values.reserve(right_eq_keys.size());
            for (const auto* col : right_eq_keys) {
                group.values.push_back(scalar_from_column(*col, r));
            }
            right_groups[group].push_back(r);
        }

        std::unordered_map<Key, std::size_t, KeyHash, KeyEq> right_pos;
        right_pos.reserve(right_groups.size());

        for (std::size_t l = 0; l < left.rows(); ++l) {
            append_left_row(l);

            Key group;
            group.values.reserve(left_eq_keys.size());
            for (const auto* col : left_eq_keys) {
                group.values.push_back(scalar_from_column(*col, l));
            }

            auto it = right_groups.find(group);
            if (it == right_groups.end()) {
                append_right_defaults();
                continue;
            }

            auto [pos_it, _] = right_pos.try_emplace(group, 0);
            std::size_t& pos = pos_it->second;
            const auto& rows = it->second;
            while (pos < rows.size() && right_times[rows[pos]] <= left_times[l]) {
                ++pos;
            }

            if (pos == 0) {
                append_right_defaults();
            } else {
                append_right_row(rows[pos - 1]);
            }
        }

        output.time_index = left.time_index;
        normalize_time_index(output);
        finalize_right_validity();
        return output;
    }

    std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_index;
    if (left.rows() < right.rows()) {
        std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> left_index;
        left_index.reserve(left.rows());
        for (std::size_t l = 0; l < left.rows(); ++l) {
            Key key;
            key.values.reserve(keys.size());
            for (const auto* col : left_keys) {
                key.values.push_back(scalar_from_column(*col, l));
            }
            left_index[key].push_back(l);
        }

        auto emit_left_matches_for_right_row = [&](std::size_t r, auto&& emit_left_row) {
            Key key;
            key.values.reserve(keys.size());
            for (const auto* col : right_keys) {
                key.values.push_back(scalar_from_column(*col, r));
            }
            auto it = left_index.find(key);
            if (it == left_index.end()) {
                return;
            }
            for (auto l : it->second) {
                emit_left_row(l);
            }
        };

        if (semi_join || anti_join) {
            emit_membership_rows_from_right_scan(emit_left_matches_for_right_row);
        } else {
            emit_preserving_rows_from_right_scan(emit_left_matches_for_right_row);
        }
        finalize_left_validity();
        finalize_right_validity();
        return output;
    }

    right_index.reserve(right.rows());
    for (std::size_t r = 0; r < right.rows(); ++r) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : right_keys) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        right_index[key].push_back(r);
    }

    std::vector<std::uint8_t> right_matched;
    if (preserve_right_rows) {
        right_matched.assign(right.rows(), 0U);
    }

    for (std::size_t l = 0; l < left.rows(); ++l) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : left_keys) {
            key.values.push_back(scalar_from_column(*col, l));
        }
        auto it = right_index.find(key);
        const bool has_match = (it != right_index.end());
        if (semi_join) {
            if (has_match) {
                append_left_row(l);
            }
            continue;
        }
        if (anti_join) {
            if (!has_match) {
                append_left_row(l);
            }
            continue;
        }
        if (!has_match) {
            if (preserve_left_rows) {
                append_left_row(l);
                append_right_defaults();
            }
            continue;
        }
        for (auto r : it->second) {
            append_left_row(l);
            append_right_row(r);
            if (preserve_right_rows) {
                right_matched[r] = 1U;
            }
        }
    }

    if (preserve_right_rows) {
        for (std::size_t r = 0; r < right.rows(); ++r) {
            if (right_matched[r] == 0U) {
                append_left_defaults_from_right(r);
                append_right_row(r);
            }
        }
    }

    finalize_left_validity();
    finalize_right_validity();
    return output;
}

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
                double x;
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
                double x;
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
                double x;
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
                    slot.double_value = slot.param * x + (1.0 - slot.param) * slot.double_value;
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
                        std::sort(sorted.begin(), sorted.end());
                        std::size_t n = sorted.size();
                        double med = (n % 2 == 1) ? sorted[n / 2]
                                                  : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
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
                        append_scalar(*column, sorted[lo] + frac * (sorted[hi] - sorted[lo]));
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
                        double m2 = 0.0, m3 = 0.0;
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
                        double m2 = 0.0, m4 = 0.0;
                        for (double x : slot.values) {
                            double d = x - mean;
                            double d2 = d * d;
                            m2 += d2;
                            m4 += d2 * d2;
                        }
                        if (m2 == 0.0) {
                            append_scalar(*column, 0.0);
                        } else {
                            double dn = static_cast<double>(n);
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
                    std::uint32_t gid;
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

            // Pass 2: Per-aggregation column scans with flat accumulator arrays.
            // n_groups is typically small, so the flat accumulators stay in L1 cache,
            // and the inner loops are simple scatter-updates: accum[gids[row]] op= data[row].
            for (std::size_t agg_i = 0; agg_i < plan.size(); ++agg_i) {
                const auto& item = plan[agg_i];
                const auto slot_for = [&](std::uint32_t g) -> AggSlot& {
                    return fs[(static_cast<std::size_t>(g) * n_aggs) + agg_i];
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
                            std::vector<double> acc(n_groups,
                                                    std::numeric_limits<double>::infinity());
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
                            std::vector<double> acc(n_groups,
                                                    -std::numeric_limits<double>::infinity());
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
            robin_hood::unordered_flat_map<std::string_view, std::uint32_t> key_to_gid;
            key_to_gid.reserve(1024);
            std::vector<std::uint32_t> dict_offsets;
            dict_offsets.reserve(1025);
            dict_offsets.push_back(0);
            std::vector<char> dict_chars;
            dict_chars.reserve(8192);
            // Flat AggSlot array: one contiguous allocation replaces n_groups heap allocations.
            // Layout: flat_slots[g * n_aggs + agg_i] is the slot for group g, aggregation agg_i.
            const std::size_t n_aggs = plan.size();
            AggState tmpl = make_state();
            std::vector<AggSlot> flat_slots;
            flat_slots.reserve(1024 * (n_aggs == 0 ? 1 : n_aggs));
            std::uint32_t n_groups = 0;
            std::vector<std::uint32_t> group_ids(rows);
            {
                std::string_view prev_key;
                std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
                for (std::size_t row = 0; row < rows; ++row) {
                    std::string_view key{src_chars + src_off[row], src_off[row + 1] - src_off[row]};
                    std::uint32_t gid;
                    if (key == prev_key) {
                        gid = prev_gid;
                    } else {
                        auto it = key_to_gid.find(key);
                        if (it == key_to_gid.end()) {
                            gid = n_groups++;
                            key_to_gid.emplace(key, gid);
                            dict_chars.insert(dict_chars.end(), key.begin(), key.end());
                            dict_offsets.push_back(static_cast<std::uint32_t>(dict_chars.size()));
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

            const std::uint32_t* gids = group_ids.data();
            AggSlot* fs = flat_slots.data();

            // Pass 2: flat accumulators — same structure as the categorical fast path.
            for (std::size_t agg_i = 0; agg_i < n_aggs; ++agg_i) {
                const auto& item = plan[agg_i];
                const auto slot_for = [&](std::uint32_t g) -> AggSlot& {
                    return fs[(static_cast<std::size_t>(g) * n_aggs) + agg_i];
                };

                if (item.func == ir::AggFunc::Count) {
                    std::vector<std::int64_t> acc(n_groups, 0);
                    for (std::size_t row = 0; row < rows; ++row)
                        acc[gids[row]]++;
                    for (std::uint32_t g = 0; g < n_groups; ++g)
                        slot_for(g).count = acc[g];
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
                            slot_for(g).int_value = acc[g];
                            slot_for(g).has_value = true;
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
                            slot_for(g).double_value = acc[g];
                            slot_for(g).has_value = true;
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
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = (*item.int_col)[row];
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).int_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = data[row];
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).double_value = acc[g];
                            slot_for(g).has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = (*item.str_col)[row];
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            slot_for(g).last_value = std::move(acc[g]);
                            slot_for(g).has_value = true;
                        }
                    } else if (item.cat_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = std::string((*item.cat_col)[row]);
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
                            for (std::size_t row = 0; row < rows; ++row)
                                acc[gids[row]] += data[row];
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
                            std::vector<double> acc(n_groups,
                                                    std::numeric_limits<double>::infinity());
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
                            std::vector<double> acc(n_groups,
                                                    -std::numeric_limits<double>::infinity());
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
                            for (std::size_t row = 0; row < rows; ++row)
                                acc[gids[row]] += data[row];
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
                                fs[(static_cast<std::size_t>(g) * n_aggs) + agg_i].int_value =
                                    acc[g];
                                fs[(static_cast<std::size_t>(g) * n_aggs) + agg_i].has_value = true;
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
                if (auto err = append_agg_values_flat(*output,
                                                      fs + static_cast<std::size_t>(g) * n_aggs)) {
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
                        std::uint32_t code;
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
                        std::uint32_t code;
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
        for (std::size_t row = 0; row < rows; ++row) {
            std::uint32_t cell = 0;
            for (std::size_t ci = 0; ci < n_keys; ++ci)
                cell += per_col[ci].code_at(row) * static_cast<std::uint32_t>(strides[ci]);
            std::uint32_t gid = cell_to_gid[cell];
            if (gid == std::numeric_limits<std::uint32_t>::max()) {
                gid = n_groups_m++;
                cell_to_gid[cell] = gid;
                flat_slots.insert(flat_slots.end(), tmpl.slots.begin(), tmpl.slots.end());
                for (std::size_t ci = 0; ci < n_keys; ++ci)
                    group_col_codes_flat.push_back(per_col[ci].code_at(row));
            }
            compound_gids[row] = gid;
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

    // ── Pass 2: per-aggregation scatter-accumulate into flat AggSlot[] ───────
    for (std::size_t agg_i = 0; agg_i < plan.size(); ++agg_i) {
        const auto& item = plan[agg_i];
        const auto slot_for = [&](std::uint32_t g) -> AggSlot& {
            return fs[(static_cast<std::size_t>(g) * n_aggs) + agg_i];
        };

        if (item.func == ir::AggFunc::Count) {
            std::vector<std::int64_t> acc(n_groups_m, 0);
            for (std::size_t row = 0; row < rows; ++row)
                acc[gids[row]]++;
            for (std::uint32_t g = 0; g < n_groups_m; ++g)
                slot_for(g).count = acc[g];
            continue;
        }

        if (item.func == ir::AggFunc::First) {
            std::vector<std::uint8_t> found(n_groups_m, 0U);
            if (item.int_col != nullptr) {
                std::vector<std::int64_t> acc(n_groups_m, 0);
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (found[g] == 0U) {
                        acc[g] = (*item.int_col)[row];
                        found[g] = 1U;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    auto& slot = slot_for(g);
                    slot.int_value = acc[g];
                    slot.has_value = true;
                }
            } else if (item.dbl_col != nullptr) {
                std::vector<double> acc(n_groups_m, 0.0);
                const double* data = item.dbl_col->data();
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (found[g] == 0U) {
                        acc[g] = data[row];
                        found[g] = 1U;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    auto& slot = slot_for(g);
                    slot.double_value = acc[g];
                    slot.has_value = true;
                }
            } else if (item.str_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (found[g] == 0U) {
                        acc[g] = (*item.str_col)[row];
                        found[g] = 1U;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    auto& slot = slot_for(g);
                    slot.first_value = std::move(acc[g]);
                    slot.has_value = true;
                }
            } else if (item.cat_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (found[g] == 0U) {
                        acc[g] = std::string((*item.cat_col)[row]);
                        found[g] = 1U;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    auto& slot = slot_for(g);
                    slot.first_value = std::move(acc[g]);
                    slot.has_value = true;
                }
            }
            continue;
        }

        if (item.func == ir::AggFunc::Last) {
            if (item.int_col != nullptr) {
                std::vector<std::int64_t> acc(n_groups_m, 0);
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = (*item.int_col)[row];
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    slot_for(g).int_value = acc[g];
                    slot_for(g).has_value = true;
                }
            } else if (item.dbl_col != nullptr) {
                std::vector<double> acc(n_groups_m, 0.0);
                const double* data = item.dbl_col->data();
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = data[row];
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    slot_for(g).double_value = acc[g];
                    slot_for(g).has_value = true;
                }
            } else if (item.str_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = (*item.str_col)[row];
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    slot_for(g).last_value = std::move(acc[g]);
                    slot_for(g).has_value = true;
                }
            } else if (item.cat_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = std::string((*item.cat_col)[row]);
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
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
                    std::vector<double> acc(n_groups_m, 0.0);
                    for (std::size_t row = 0; row < rows; ++row)
                        acc[gids[row]] += data[row];
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        slot_for(g).double_value = acc[g];
                        slot_for(g).has_value = true;
                    }
                    break;
                }
                case ir::AggFunc::Mean: {
                    std::vector<double> acc(n_groups_m, 0.0);
                    std::vector<std::int64_t> counts(n_groups_m, 0);
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        acc[g] += data[row];
                        counts[g]++;
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        slot_for(g).sum = acc[g];
                        slot_for(g).count = counts[g];
                    }
                    break;
                }
                case ir::AggFunc::Min: {
                    std::vector<double> acc(n_groups_m, std::numeric_limits<double>::infinity());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        acc[g] = std::min(data[row], acc[g]);
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        slot_for(g).double_value = acc[g];
                        slot_for(g).has_value = true;
                    }
                    break;
                }
                case ir::AggFunc::Max: {
                    std::vector<double> acc(n_groups_m, -std::numeric_limits<double>::infinity());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        acc[g] = std::max(data[row], acc[g]);
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
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
                    std::vector<std::int64_t> acc(n_groups_m, 0);
                    for (std::size_t row = 0; row < rows; ++row)
                        acc[gids[row]] += data[row];
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        slot_for(g).int_value = acc[g];
                        slot_for(g).has_value = true;
                    }
                    break;
                }
                case ir::AggFunc::Mean: {
                    std::vector<double> acc(n_groups_m, 0.0);
                    std::vector<std::int64_t> counts(n_groups_m, 0);
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        acc[g] += static_cast<double>(data[row]);
                        counts[g]++;
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        slot_for(g).sum = acc[g];
                        slot_for(g).count = counts[g];
                    }
                    break;
                }
                case ir::AggFunc::Min: {
                    std::vector<std::int64_t> acc(n_groups_m,
                                                  std::numeric_limits<std::int64_t>::max());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        acc[g] = std::min(data[row], acc[g]);
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        slot_for(g).int_value = acc[g];
                        slot_for(g).has_value = true;
                    }
                    break;
                }
                case ir::AggFunc::Max: {
                    std::vector<std::int64_t> acc(n_groups_m,
                                                  std::numeric_limits<std::int64_t>::min());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        acc[g] = std::max(data[row], acc[g]);
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
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

    // ── Output reconstruction ─────────────────────────────────────────────────
    auto output = build_output();
    if (!output.has_value()) {
        return std::unexpected(output.error());
    }
    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
        const std::uint32_t* gc =
            group_col_codes_flat.data() + static_cast<std::size_t>(g) * n_keys;
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

constexpr auto is_rng_func(std::string_view name) -> bool;
constexpr auto rng_func_returns_int(std::string_view name) -> bool;
constexpr auto is_cum_func(std::string_view name) -> bool;
constexpr auto is_fill_func(std::string_view name) -> bool;
constexpr auto is_float_clean_func(std::string_view name) -> bool;

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
        if (call->callee == "is_nan") {
            if (call->args.size() != 1) {
                return std::unexpected("is_nan: expected 1 argument");
            }
            const auto* col_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
            if (!col_ref) {
                return std::unexpected("is_nan: argument must be a column name");
            }
            const auto* source = input.find(col_ref->name);
            if (!source) {
                return std::unexpected("is_nan: unknown column '" + col_ref->name + "'");
            }
            auto kind = expr_type_for_column(*source);
            if (kind != ExprType::Double) {
                return std::unexpected("is_nan: column must be Float64");
            }
            return ExprType::Bool;
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
                    return lhs / rhs;
                case ir::ArithmeticOp::Mod:
                    return lhs % rhs;
            }
        }
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if (call->callee == "is_nan") {
            if (call->args.size() != 1) {
                return std::unexpected("is_nan: expected 1 argument");
            }
            auto arg = eval_expr(*call->args[0], input, row, scalars, externs);
            if (!arg) {
                return arg;
            }
            if (const auto* value = std::get_if<double>(&arg.value())) {
                return static_cast<std::int64_t>(std::isnan(*value));
            }
            return std::unexpected("is_nan: argument must be Float64");
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
                    if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(*v);
                    } else if (const auto* v = std::get_if<double>(&value.value())) {
                        col.push_back(static_cast<std::int64_t>(*v));
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Int64-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, double>) {
                    if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(static_cast<double>(*v));
                    } else if (const auto* v = std::get_if<double>(&value.value())) {
                        col.push_back(*v);
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Float64-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, bool>) {
                    if (const auto* v = std::get_if<bool>(&value.value())) {
                        col.push_back(*v);
                    } else if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(*v != 0);
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
                    if (const auto* v = std::get_if<Date>(&value.value())) {
                        col.push_back(*v);
                    } else if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(int64_to_date_checked(*v));
                    } else {
                        invariant_violation(
                            "eval_expr_column: expected Date-compatible expression value");
                    }
                } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                    if (const auto* v = std::get_if<Timestamp>(&value.value())) {
                        col.push_back(*v);
                    } else if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(Timestamp{*v});
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
auto eval_lag_lead_column(const ir::CallExpr& call, const Table& input, bool is_lag)
    -> std::expected<ColumnValue, std::string> {
    const std::string fname = is_lag ? "lag" : "lead";
    if (call.args.size() != 2) {
        return std::unexpected(fname + ": expected 2 arguments");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (!col_ref) {
        return std::unexpected(fname + ": first argument must be a column name");
    }
    const auto* offset_lit = std::get_if<ir::Literal>(&call.args[1]->node);
    const std::int64_t* offset_val =
        offset_lit ? std::get_if<std::int64_t>(&offset_lit->value) : nullptr;
    if (offset_val == nullptr || *offset_val < 0) {
        return std::unexpected(fname + ": second argument must be a non-negative integer literal");
    }
    const auto* src = input.find(col_ref->name);
    if (!src) {
        return std::unexpected(fname + ": unknown column '" + col_ref->name + "'");
    }
    std::size_t n = static_cast<std::size_t>(*offset_val);
    std::size_t rows = input.rows();
    return std::visit(
        [&](const auto& col) -> ColumnValue {
            using ColT = std::decay_t<decltype(col)>;
            ColT result;
            if constexpr (std::is_same_v<ColT, Column<Categorical>> ||
                          std::is_same_v<ColT, Column<std::string>>) {
                // Categorical/string: element-wise fallback (no plain memcpy).
                result.reserve(rows);
                for (std::size_t i = 0; i < rows; ++i) {
                    if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                        result.push_back(is_lag ? (i >= n ? col[i - n] : std::string_view{})
                                                : (i + n < rows ? col[i + n] : std::string_view{}));
                    } else {
                        using T = typename ColT::value_type;
                        result.push_back(is_lag ? (i >= n ? col[i - n] : T{})
                                                : (i + n < rows ? col[i + n] : T{}));
                    }
                }
            } else {
                // POD column: zero-fill then bulk-copy the shifted region.
                using T = typename ColT::value_type;
                result.resize(rows);  // zero-initialises
                if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    if (is_lag) {
                        for (std::size_t i = n; i < rows; ++i) {
                            result.set(i, col[i - n]);
                        }
                    } else {
                        for (std::size_t i = 0; i + n < rows; ++i) {
                            result.set(i, col[i + n]);
                        }
                    }
                } else {
                    if (is_lag) {
                        if (n < rows)
                            std::memcpy(result.data() + n, col.data(), (rows - n) * sizeof(T));
                    } else {
                        if (n < rows)
                            std::memcpy(result.data(), col.data() + n, (rows - n) * sizeof(T));
                    }
                }
            }
            return result;
        },
        *src);
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
                if (const auto* v = std::get_if<std::int64_t>(&fill_lit->value))
                    maybe_fill = *v;
                else if (const auto* v = std::get_if<double>(&fill_lit->value))
                    maybe_fill = static_cast<std::int64_t>(*v);
            } else if constexpr (std::is_same_v<T, double>) {
                if (const auto* v = std::get_if<double>(&fill_lit->value))
                    maybe_fill = *v;
                else if (const auto* v = std::get_if<std::int64_t>(&fill_lit->value))
                    maybe_fill = static_cast<double>(*v);
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
        std::size_t lo = 0, hi = row;
        while (lo < hi) {
            std::size_t mid = lo + (hi - lo) / 2;
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
    std::size_t lo = 0, hi = row;
    while (lo < hi) {
        std::size_t mid = lo + (hi - lo) / 2;
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
                    Column<double> result;
                    result.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        std::size_t n = i - lo + 1;
                        if (n < 2) {
                            result[i] = 0.0;
                            continue;
                        }
                        // Welford's online algorithm over the window.
                        double mean = 0.0;
                        double m2 = 0.0;
                        std::int64_t cnt = 0;
                        for (std::size_t j = lo; j <= i; ++j) {
                            double x = static_cast<double>(col[j]);
                            ++cnt;
                            double delta = x - mean;
                            mean += delta / static_cast<double>(cnt);
                            m2 += delta * (x - mean);
                        }
                        result[i] = std::sqrt(m2 / static_cast<double>(n - 1));
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
                    Column<double> result;
                    result.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        double ewma = static_cast<double>(col[lo]);
                        for (std::size_t j = lo + 1; j <= i; ++j)
                            ewma = alpha * static_cast<double>(col[j]) + (1.0 - alpha) * ewma;
                        result[i] = ewma;
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
                        result[i] = window[idx_lo] + frac * (window[idx_hi] - window[idx_lo]);
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
                        double m2 = 0.0, m3 = 0.0;
                        for (std::size_t j = lo; j <= i; ++j) {
                            double d = static_cast<double>(col[j]) - mean;
                            m2 += d * d;
                            m3 += d * d * d;
                        }
                        if (m2 == 0.0) {
                            result[i] = 0.0;
                        } else {
                            double dn = static_cast<double>(n);
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
                        double m2 = 0.0, m4 = 0.0;
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

    // Build bucket column: floor(ts.nanos / dur_ns) * dur_ns
    const auto rows = input.rows();
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

    out.columns[pos].name = ts_name;
    out.columns[pos].column = std::make_shared<ColumnValue>(std::move(ts_out));
    out.index.erase(it);
    out.index[ts_name] = pos;
    out.time_index = ts_name;

    return out;
}

constexpr auto is_rolling_func(std::string_view name) -> bool {
    return name == "rolling_sum" || name == "rolling_mean" || name == "rolling_min" ||
           name == "rolling_max" || name == "rolling_count" || name == "rolling_median" ||
           name == "rolling_std" || name == "rolling_ewma" || name == "rolling_quantile" ||
           name == "rolling_skew" || name == "rolling_kurtosis";
}

constexpr auto is_cum_func(std::string_view name) -> bool {
    return name == "cumsum" || name == "cumprod";
}

// ─── Vectorized RNG ───────────────────────────────────────────────────────────

constexpr auto is_rng_func(std::string_view name) -> bool {
    return name == "rand_uniform" || name == "rand_normal" || name == "rand_student_t" ||
           name == "rand_gamma" || name == "rand_exponential" || name == "rand_bernoulli" ||
           name == "rand_poisson" || name == "rand_int";
}

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
                    if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                        result.push_back(col[src_idx]);
                    } else {
                        result.push_back(col[src_idx]);
                    }
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
                auto col = eval_lag_lead_column(*call, output, call->callee == "lag");
                if (!col) {
                    return std::unexpected(col.error());
                }
                output.add_column(field.alias, std::move(col.value()));
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
                std::expected<FillResult, std::string> res =
                    call->callee == "fill_null"      ? eval_fill_null(*call, output)
                    : call->callee == "fill_forward" ? eval_fill_forward(*call, output)
                                                     : eval_fill_backward(*call, output);
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
                if (!output.time_index.has_value()) {
                    return std::unexpected(call->callee + ": requires a TimeFrame");
                }
                auto col = eval_lag_lead_column(*call, output, call->callee == "lag");
                if (!col) {
                    return std::unexpected(col.error());
                }
                output.add_column(field.alias, std::move(col.value()));
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
                std::expected<FillResult, std::string> res =
                    call->callee == "fill_null"      ? eval_fill_null(*call, output)
                    : call->callee == "fill_forward" ? eval_fill_forward(*call, output)
                                                     : eval_fill_backward(*call, output);
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

// ─── Matrix Operations ────────────────────────────────────────────────────────

/// Return the numeric columns of `input` as parallel vectors of double.
/// Int32/Int64/Float32/Float64 columns are included (widened to double).
/// All other types are silently skipped.
static auto extract_numeric(const Table& input)
    -> std::pair<std::vector<std::string>, std::vector<std::vector<double>>> {
    std::vector<std::string> names;
    std::vector<std::vector<double>> data;
    const std::size_t rows = input.rows();
    for (const auto& entry : input.columns) {
        std::vector<double> col;
        col.reserve(rows);
        bool ok = std::visit(
            [&](const auto& c) -> bool {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, Column<double>>) {
                    for (std::size_t i = 0; i < rows; ++i)
                        col.push_back(c[i]);
                    return true;
                } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                    for (std::size_t i = 0; i < rows; ++i)
                        col.push_back(static_cast<double>(c[i]));
                    return true;
                } else {
                    return false;
                }
            },
            *entry.column);
        if (ok) {
            names.push_back(entry.name);
            data.push_back(std::move(col));
        }
    }
    return {std::move(names), std::move(data)};
}

auto cov_table(const Table& input) -> std::expected<Table, std::string> {
    auto [names, data] = extract_numeric(input);
    const std::size_t n = names.size();
    const std::size_t rows = data.empty() ? 0 : data[0].size();

    if (n == 0) {
        return std::unexpected("cov: no numeric columns found");
    }
    if (rows < 2) {
        return std::unexpected("cov: need at least 2 rows to compute covariance");
    }

    // Compute column means.
    std::vector<double> mean(n, 0.0);
    for (std::size_t j = 0; j < n; ++j) {
        for (std::size_t i = 0; i < rows; ++i)
            mean[j] += data[j][i];
        mean[j] /= static_cast<double>(rows);
    }

    // Compute upper-triangle covariances; mirror for symmetry.
    const double denom = static_cast<double>(rows - 1);
    std::vector<std::vector<double>> cov(n, std::vector<double>(n, 0.0));
    for (std::size_t a = 0; a < n; ++a) {
        for (std::size_t b = a; b < n; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < rows; ++i) {
                s += (data[a][i] - mean[a]) * (data[b][i] - mean[b]);
            }
            cov[a][b] = cov[b][a] = s / denom;
        }
    }

    // Build result Table.
    Table out;
    // Leading "column" label column.
    Column<std::string> label_col;
    for (const auto& nm : names)
        label_col.push_back(nm);
    out.add_column("column", std::move(label_col));
    // N Float64 columns.
    for (std::size_t b = 0; b < n; ++b) {
        Column<double> col_data(std::vector<double>(cov[b].begin(), cov[b].end()));
        out.add_column(names[b], std::move(col_data));
    }
    return out;
}

auto corr_table(const Table& input) -> std::expected<Table, std::string> {
    auto [names, data] = extract_numeric(input);
    const std::size_t n = names.size();
    const std::size_t rows = data.empty() ? 0 : data[0].size();

    if (n == 0) {
        return std::unexpected("corr: no numeric columns found");
    }
    if (rows < 2) {
        return std::unexpected("corr: need at least 2 rows to compute correlation");
    }

    std::vector<double> mean(n, 0.0);
    for (std::size_t j = 0; j < n; ++j) {
        for (std::size_t i = 0; i < rows; ++i)
            mean[j] += data[j][i];
        mean[j] /= static_cast<double>(rows);
    }

    const double denom = static_cast<double>(rows - 1);
    std::vector<std::vector<double>> cov(n, std::vector<double>(n, 0.0));
    for (std::size_t a = 0; a < n; ++a) {
        for (std::size_t b = a; b < n; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < rows; ++i) {
                s += (data[a][i] - mean[a]) * (data[b][i] - mean[b]);
            }
            cov[a][b] = cov[b][a] = s / denom;
        }
    }

    // Normalise to correlation.
    std::vector<std::vector<double>> corr_mat(n, std::vector<double>(n, 0.0));
    for (std::size_t a = 0; a < n; ++a) {
        for (std::size_t b = 0; b < n; ++b) {
            const double sigma_a = std::sqrt(cov[a][a]);
            const double sigma_b = std::sqrt(cov[b][b]);
            if (sigma_a == 0.0 || sigma_b == 0.0) {
                corr_mat[a][b] = (a == b) ? 1.0 : 0.0;
            } else {
                corr_mat[a][b] = cov[a][b] / (sigma_a * sigma_b);
            }
        }
    }

    Table out;
    Column<std::string> label_col;
    for (const auto& nm : names)
        label_col.push_back(nm);
    out.add_column("column", std::move(label_col));
    for (std::size_t b = 0; b < n; ++b) {
        Column<double> col_data(std::vector<double>(corr_mat[b].begin(), corr_mat[b].end()));
        out.add_column(names[b], std::move(col_data));
    }
    return out;
}

auto transpose_table(const Table& input) -> std::expected<Table, std::string> {
    if (input.columns.empty()) {
        return std::unexpected("transpose: input table has no columns");
    }

    // Identify the label column: the first String or Categorical column.
    // All other columns must share the same type (homogeneous constraint).
    int label_idx = -1;
    for (int i = 0; i < static_cast<int>(input.columns.size()); ++i) {
        const auto& cv = *input.columns[static_cast<std::size_t>(i)].column;
        if (std::holds_alternative<Column<std::string>>(cv) ||
            std::holds_alternative<Column<Categorical>>(cv)) {
            if (label_idx == -1) {
                label_idx = i;
                break;
            }
        }
    }

    // Collect data column indices (exclude label column).
    std::vector<std::size_t> data_idxs;
    for (std::size_t i = 0; i < input.columns.size(); ++i) {
        if (static_cast<int>(i) != label_idx)
            data_idxs.push_back(i);
    }

    if (data_idxs.empty()) {
        return std::unexpected("transpose: no data columns to transpose");
    }

    // Validate all data columns share the same type.
    std::size_t first_type = input.columns[data_idxs[0]].column->index();
    for (std::size_t idx : data_idxs) {
        if (input.columns[idx].column->index() != first_type) {
            return std::unexpected(
                "transpose: all data columns must have the same type (found mixed types)");
        }
    }

    const std::size_t n_data_cols = data_idxs.size();  // becomes number of output rows
    const std::size_t n_rows = input.rows();           // becomes number of output columns

    // Determine output column names: from label column values, or "r0", "r1", ...
    std::vector<std::string> out_col_names;
    out_col_names.reserve(n_rows);
    if (label_idx >= 0) {
        const auto& label_entry = input.columns[static_cast<std::size_t>(label_idx)];
        if (const auto* sc = std::get_if<Column<std::string>>(&*label_entry.column)) {
            for (std::size_t i = 0; i < n_rows; ++i)
                out_col_names.push_back(std::string((*sc)[i]));
        } else if (const auto* cc = std::get_if<Column<Categorical>>(&*label_entry.column)) {
            for (std::size_t i = 0; i < n_rows; ++i)
                out_col_names.push_back(std::string((*cc)[i]));
        }
    } else {
        for (std::size_t i = 0; i < n_rows; ++i)
            out_col_names.push_back("r" + std::to_string(i));
    }

    // Check for duplicate output column names (would make the output table inconsistent).
    {
        std::unordered_set<std::string> seen;
        for (const auto& name : out_col_names) {
            if (!seen.insert(name).second) {
                return std::unexpected("transpose: duplicate label value '" + name +
                                       "' — output column names must be unique");
            }
        }
    }

    // Build output table: n_rows columns, each with n_data_cols rows.
    // Leading "column" label column contains the original data column names.
    Table out;
    Column<std::string> row_labels;
    for (std::size_t i : data_idxs)
        row_labels.push_back(input.columns[i].name);
    out.add_column("column", std::move(row_labels));

    // For each original row (= output column), extract the value from each data column.
    // We dispatch by the concrete column type so we can build typed output columns.
    std::visit(
        [&](const auto& first_col_ref) {
            using ColT = std::decay_t<decltype(first_col_ref)>;
            for (std::size_t r = 0; r < n_rows; ++r) {
                if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                              std::is_same_v<ColT, Column<Categorical>>) {
                    // String-like: emit a Column<std::string> (decode Categoricals).
                    Column<std::string> out_col;
                    out_col.reserve(n_data_cols);
                    for (std::size_t ci : data_idxs) {
                        const auto& src = std::get<ColT>(*input.columns[ci].column);
                        out_col.push_back(src[r]);  // src[r] returns string_view for both
                    }
                    out.add_column(out_col_names[r], std::move(out_col));
                } else {
                    using ElemT = typename ColT::value_type;
                    Column<ElemT> out_col;
                    out_col.reserve(n_data_cols);
                    for (std::size_t ci : data_idxs) {
                        const auto& src = std::get<ColT>(*input.columns[ci].column);
                        out_col.push_back(src[r]);
                    }
                    out.add_column(out_col_names[r], std::move(out_col));
                }
            }
        },
        *input.columns[data_idxs[0]].column);

    return out;
}

auto matmul_table(const Table& left, const Table& right) -> std::expected<Table, std::string> {
    auto [left_names, left_data] = extract_numeric(left);
    auto [right_names, right_data] = extract_numeric(right);

    const std::size_t m = left_data.empty() ? 0 : left_data[0].size();  // rows of left
    const std::size_t k = left_names.size();   // cols of left = rows of right
    const std::size_t n = right_names.size();  // cols of right

    if (k == 0 || n == 0) {
        return std::unexpected("matmul: no numeric columns found in one or both operands");
    }
    if (right_data.empty() || right_data[0].size() != k) {
        return std::unexpected("matmul: inner dimensions do not match — left has " +
                               std::to_string(k) + " numeric columns but right has " +
                               std::to_string(right_data[0].size()) + " rows");
    }

    // Standard triple-loop matrix multiply: C[i,j] = sum_p A[i,p] * B[p,j]
    // left_data[col][row], right_data[col][row]
    std::vector<std::vector<double>> result(n, std::vector<double>(m, 0.0));
    for (std::size_t j = 0; j < n; ++j) {
        for (std::size_t p = 0; p < k; ++p) {
            const double bpj = right_data[j][p];
            for (std::size_t i = 0; i < m; ++i) {
                result[j][i] += left_data[p][i] * bpj;
            }
        }
    }

    Table out;
    for (std::size_t j = 0; j < n; ++j) {
        Column<double> col_data(result[j]);
        out.add_column(right_names[j], std::move(col_data));
    }
    return out;
}

// ─── Model Fitting ───────────────────────────────────────────────────────────

/// Build a design matrix from a DataFrame using a model formula.
/// Returns column names and a row-major matrix (vector of column vectors).
/// Categorical/String columns are dummy-encoded (treatment coding: first level dropped when
/// intercept is present).
static auto build_model_matrix(const Table& input, const ir::ModelFormula& formula)
    -> std::expected<std::pair<std::vector<std::string>, std::vector<std::vector<double>>>,
                     std::string> {
    const std::size_t n = input.rows();
    if (n == 0) {
        return std::unexpected("model: input table has no rows");
    }

    // Resolve which columns to include from the formula terms.
    std::vector<std::string> requested_columns;
    bool has_dot = false;
    for (const auto& term : formula.terms) {
        if (term.is_dot) {
            has_dot = true;
        } else {
            for (const auto& col : term.columns) {
                requested_columns.push_back(col);
            }
        }
    }

    // If `.` is used, add all columns except the response.
    if (has_dot) {
        for (const auto& entry : input.columns) {
            if (entry.name != formula.response) {
                bool already = false;
                for (const auto& r : requested_columns) {
                    if (r == entry.name) {
                        already = true;
                        break;
                    }
                }
                if (!already) {
                    requested_columns.push_back(entry.name);
                }
            }
        }
    }

    std::vector<std::string> col_names;
    std::vector<std::vector<double>> columns;

    // Intercept column.
    if (formula.has_intercept) {
        col_names.push_back("(intercept)");
        columns.push_back(std::vector<double>(n, 1.0));
    }

    // Process each formula term.
    for (const auto& term : formula.terms) {
        if (term.is_dot) {
            // Dot expands to all non-response columns (handled above).
            // Process each as a main-effect term.
            for (const auto& entry : input.columns) {
                if (entry.name == formula.response)
                    continue;
                // Check if already included from explicit terms.
                bool from_explicit = false;
                for (const auto& t2 : formula.terms) {
                    if (!t2.is_dot && t2.columns.size() == 1 && t2.columns[0] == entry.name) {
                        from_explicit = true;
                        break;
                    }
                }
                if (from_explicit)
                    continue;

                auto ok = std::visit(
                    [&](const auto& c) -> bool {
                        using T = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<T, Column<double>>) {
                            std::vector<double> v(n);
                            for (std::size_t i = 0; i < n; ++i)
                                v[i] = c[i];
                            col_names.push_back(entry.name);
                            columns.push_back(std::move(v));
                            return true;
                        } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                            std::vector<double> v(n);
                            for (std::size_t i = 0; i < n; ++i)
                                v[i] = static_cast<double>(c[i]);
                            col_names.push_back(entry.name);
                            columns.push_back(std::move(v));
                            return true;
                        } else if constexpr (std::is_same_v<T, Column<std::string>>) {
                            // Dummy-encode string column.
                            std::vector<std::string> levels;
                            for (std::size_t i = 0; i < n; ++i) {
                                bool found = false;
                                for (const auto& lv : levels) {
                                    if (lv == c[i]) {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                    levels.push_back(std::string(c[i]));
                            }
                            std::size_t start = formula.has_intercept ? 1 : 0;
                            for (std::size_t li = start; li < levels.size(); ++li) {
                                std::vector<double> dummy(n, 0.0);
                                for (std::size_t i = 0; i < n; ++i) {
                                    if (c[i] == levels[li])
                                        dummy[i] = 1.0;
                                }
                                col_names.push_back(entry.name + "_" + levels[li]);
                                columns.push_back(std::move(dummy));
                            }
                            return true;
                        } else {
                            return false;
                        }
                    },
                    *entry.column);
                if (!ok) {
                    return std::unexpected("model: column '" + entry.name +
                                           "' has unsupported type for model matrix");
                }
            }
            continue;
        }

        if (term.columns.size() == 1) {
            // Main effect.
            const auto& name = term.columns[0];
            const auto* entry = input.find_entry(name);
            if (entry == nullptr) {
                return std::unexpected("model: column not found: " + name);
            }
            auto ok = std::visit(
                [&](const auto& c) -> bool {
                    using T = std::decay_t<decltype(c)>;
                    if constexpr (std::is_same_v<T, Column<double>>) {
                        std::vector<double> v(n);
                        for (std::size_t i = 0; i < n; ++i)
                            v[i] = c[i];
                        col_names.push_back(name);
                        columns.push_back(std::move(v));
                        return true;
                    } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                        std::vector<double> v(n);
                        for (std::size_t i = 0; i < n; ++i)
                            v[i] = static_cast<double>(c[i]);
                        col_names.push_back(name);
                        columns.push_back(std::move(v));
                        return true;
                    } else if constexpr (std::is_same_v<T, Column<std::string>>) {
                        // Dummy-encode.
                        std::vector<std::string> levels;
                        for (std::size_t i = 0; i < n; ++i) {
                            bool found = false;
                            for (const auto& lv : levels) {
                                if (lv == c[i]) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                                levels.push_back(std::string(c[i]));
                        }
                        std::size_t start = formula.has_intercept ? 1 : 0;
                        for (std::size_t li = start; li < levels.size(); ++li) {
                            std::vector<double> dummy(n, 0.0);
                            for (std::size_t i = 0; i < n; ++i) {
                                if (std::string_view(c[i]) == levels[li])
                                    dummy[i] = 1.0;
                            }
                            col_names.push_back(name + "_" + levels[li]);
                            columns.push_back(std::move(dummy));
                        }
                        return true;
                    } else {
                        return false;
                    }
                },
                *entry->column);
            if (!ok) {
                return std::unexpected("model: column '" + name +
                                       "' has unsupported type for model matrix");
            }
        } else {
            // Interaction term: element-wise product of numeric columns.
            // First, build each factor as a double vector.
            std::vector<std::vector<double>> factors;
            std::string interaction_name;
            for (std::size_t fi = 0; fi < term.columns.size(); ++fi) {
                const auto& name = term.columns[fi];
                if (fi > 0)
                    interaction_name += ":";
                interaction_name += name;
                const auto* entry = input.find_entry(name);
                if (entry == nullptr) {
                    return std::unexpected("model: column not found: " + name);
                }
                std::vector<double> v(n);
                bool ok = std::visit(
                    [&](const auto& c) -> bool {
                        using T = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<T, Column<double>>) {
                            for (std::size_t i = 0; i < n; ++i)
                                v[i] = c[i];
                            return true;
                        } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                            for (std::size_t i = 0; i < n; ++i)
                                v[i] = static_cast<double>(c[i]);
                            return true;
                        } else {
                            return false;
                        }
                    },
                    *entry->column);
                if (!ok) {
                    return std::unexpected("model: interaction term requires numeric columns, '" +
                                           name + "' is not numeric");
                }
                factors.push_back(std::move(v));
            }
            // Element-wise product of all factors.
            std::vector<double> product(n, 1.0);
            for (const auto& f : factors) {
                for (std::size_t i = 0; i < n; ++i) {
                    product[i] *= f[i];
                }
            }
            col_names.push_back(interaction_name);
            columns.push_back(std::move(product));
        }
    }

    if (columns.empty()) {
        return std::unexpected("model: design matrix has no columns");
    }
    return std::make_pair(std::move(col_names), std::move(columns));
}

/// Extract the response column as a double vector.
static auto extract_response(const Table& input, const std::string& name)
    -> std::expected<std::vector<double>, std::string> {
    const auto* entry = input.find_entry(name);
    if (entry == nullptr) {
        return std::unexpected("model: response column not found: " + name);
    }
    const std::size_t n = input.rows();
    std::vector<double> y(n);
    bool ok = std::visit(
        [&](const auto& c) -> bool {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, Column<double>>) {
                for (std::size_t i = 0; i < n; ++i)
                    y[i] = c[i];
                return true;
            } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                for (std::size_t i = 0; i < n; ++i)
                    y[i] = static_cast<double>(c[i]);
                return true;
            } else {
                return false;
            }
        },
        *entry->column);
    if (!ok) {
        return std::unexpected("model: response column '" + name + "' must be numeric");
    }
    return y;
}

/// Solve a linear system A * x = b using Cholesky decomposition (A must be SPD).
/// A is p×p (column-major: A[col][row]), b is length p. Returns x.
static auto solve_spd(const std::vector<std::vector<double>>& A, const std::vector<double>& b)
    -> std::expected<std::vector<double>, std::string> {
    const std::size_t p = A.size();

    // Cholesky factorisation: A = L * L^T.
    std::vector<std::vector<double>> L(p, std::vector<double>(p, 0.0));
    for (std::size_t j = 0; j < p; ++j) {
        double sum = 0.0;
        for (std::size_t k = 0; k < j; ++k)
            sum += L[j][k] * L[j][k];
        double diag = A[j][j] - sum;
        if (diag <= 0.0) {
            return std::unexpected(
                "model: design matrix is rank-deficient (not positive definite)");
        }
        L[j][j] = std::sqrt(diag);
        for (std::size_t i = j + 1; i < p; ++i) {
            double s = 0.0;
            for (std::size_t k = 0; k < j; ++k)
                s += L[i][k] * L[j][k];
            L[i][j] = (A[j][i] - s) / L[j][j];  // A[j][i] = A[col=j][row=i]
        }
    }

    // Forward substitution: L * z = b.
    std::vector<double> z(p);
    for (std::size_t i = 0; i < p; ++i) {
        double s = 0.0;
        for (std::size_t k = 0; k < i; ++k)
            s += L[i][k] * z[k];
        z[i] = (b[i] - s) / L[i][i];
    }

    // Back substitution: L^T * x = z.
    std::vector<double> x(p);
    for (std::size_t i = p; i-- > 0;) {
        double s = 0.0;
        for (std::size_t k = i + 1; k < p; ++k)
            s += L[k][i] * x[k];
        x[i] = (z[i] - s) / L[i][i];
    }
    return x;
}

/// Compute X^T * X (p×p) and X^T * y (p×1) from column-major X.
static auto compute_XtX_Xty(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
                            std::size_t n, std::size_t p)
    -> std::pair<std::vector<std::vector<double>>, std::vector<double>> {
    std::vector<std::vector<double>> XtX(p, std::vector<double>(p, 0.0));
    std::vector<double> Xty(p, 0.0);
    for (std::size_t a = 0; a < p; ++a) {
        for (std::size_t b = a; b < p; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < n; ++i)
                s += X[a][i] * X[b][i];
            XtX[a][b] = s;
            XtX[b][a] = s;
        }
        for (std::size_t i = 0; i < n; ++i)
            Xty[a] += X[a][i] * y[i];
    }
    return {std::move(XtX), std::move(Xty)};
}

/// Build a full ModelResult from coefficients, design matrix, and response.
static auto build_model_result(const std::vector<std::string>& col_names,
                               const std::vector<std::vector<double>>& X,
                               const std::vector<double>& y, const std::vector<double>& beta,
                               const ir::ModelFormula& formula, const std::string& method)
    -> ModelResult {
    const std::size_t n = y.size();
    const std::size_t p = beta.size();

    // Compute fitted values and residuals.
    std::vector<double> fitted(n, 0.0);
    std::vector<double> resid(n, 0.0);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = 0; j < p; ++j) {
            fitted[i] += X[j][i] * beta[j];
        }
        resid[i] = y[i] - fitted[i];
    }

    // R² and adjusted R².
    double y_mean = 0.0;
    for (double v : y)
        y_mean += v;
    y_mean /= static_cast<double>(n);
    double ss_tot = 0.0;
    double ss_res = 0.0;
    for (std::size_t i = 0; i < n; ++i) {
        ss_tot += (y[i] - y_mean) * (y[i] - y_mean);
        ss_res += resid[i] * resid[i];
    }
    double r2 = (ss_tot > 0.0) ? 1.0 - ss_res / ss_tot : 0.0;
    double adj_r2 = (n > p && ss_tot > 0.0) ? 1.0 - (ss_res / static_cast<double>(n - p)) /
                                                        (ss_tot / static_cast<double>(n - 1))
                                            : 0.0;

    // Standard errors of coefficients.
    double sigma2 = (n > p) ? ss_res / static_cast<double>(n - p) : 0.0;

    // Compute (X^T X)^{-1} diagonal for standard errors.
    // We already have X^T X; solve for each unit vector to get inverse diagonal.
    auto [XtX, _unused] = compute_XtX_Xty(X, y, n, p);
    std::vector<double> std_errors(p, 0.0);
    for (std::size_t j = 0; j < p; ++j) {
        std::vector<double> ej(p, 0.0);
        ej[j] = 1.0;
        auto inv_col = solve_spd(XtX, ej);
        if (inv_col.has_value()) {
            std_errors[j] = std::sqrt(sigma2 * (*inv_col)[j]);
        }
    }

    // Build coefficients table: term | estimate
    Table coef_table;
    Column<std::string> term_col;
    Column<double> est_col;
    for (std::size_t j = 0; j < p; ++j) {
        term_col.push_back(col_names[j]);
        est_col.push_back(beta[j]);
    }
    coef_table.add_column("term", std::move(term_col));
    coef_table.add_column("estimate", std::move(est_col));

    // Build summary table: term | estimate | std_error | t_stat | p_value
    Table summary_table;
    Column<std::string> s_term;
    Column<double> s_est;
    Column<double> s_se;
    Column<double> s_tstat;
    Column<double> s_pval;
    for (std::size_t j = 0; j < p; ++j) {
        s_term.push_back(col_names[j]);
        s_est.push_back(beta[j]);
        s_se.push_back(std_errors[j]);
        double t = (std_errors[j] > 0.0) ? beta[j] / std_errors[j] : 0.0;
        s_tstat.push_back(t);
        // Approximate two-sided p-value using normal distribution (large-sample).
        double p_val = 2.0 * std::erfc(std::abs(t) / std::sqrt(2.0));
        s_pval.push_back(p_val);
    }
    summary_table.add_column("term", std::move(s_term));
    summary_table.add_column("estimate", std::move(s_est));
    summary_table.add_column("std_error", std::move(s_se));
    summary_table.add_column("t_stat", std::move(s_tstat));
    summary_table.add_column("p_value", std::move(s_pval));

    // Build fitted values table.
    Table fitted_table;
    fitted_table.add_column("fitted", Column<double>(fitted));

    // Build residuals table.
    Table resid_table;
    resid_table.add_column("residual", Column<double>(resid));

    return ModelResult{
        .coefficients = std::move(coef_table),
        .summary = std::move(summary_table),
        .fitted_values = std::move(fitted_table),
        .residuals = std::move(resid_table),
        .formula = formula,
        .method = method,
        .r_squared = r2,
        .adj_r_squared = adj_r2,
        .n_obs = n,
        .n_params = p,
    };
}

/// Fit a model using the specified method.
static auto fit_model(const Table& input, const ir::ModelFormula& formula,
                      const std::string& method, const std::vector<ir::ModelParamSpec>& params,
                      const ScalarRegistry* /*scalars*/, const ExternRegistry* externs)
    -> std::expected<ModelResult, std::string> {
    // Build design matrix.
    auto matrix = build_model_matrix(input, formula);
    if (!matrix) {
        return std::unexpected(matrix.error());
    }
    auto& [col_names, X] = matrix.value();
    const std::size_t n = input.rows();
    const std::size_t p = col_names.size();

    // Extract response.
    auto y = extract_response(input, formula.response);
    if (!y) {
        return std::unexpected(y.error());
    }

    if (n <= p) {
        return std::unexpected("model: need more observations (" + std::to_string(n) +
                               ") than parameters (" + std::to_string(p) + ")");
    }

    auto [XtX, Xty] = compute_XtX_Xty(X, y.value(), n, p);

    if (method == "ols") {
        auto beta = solve_spd(XtX, Xty);
        if (!beta) {
            return std::unexpected(beta.error());
        }
        return build_model_result(col_names, X, y.value(), beta.value(), formula, method);
    }

    if (method == "ridge") {
        // Extract lambda parameter.
        double lambda = 1.0;
        for (const auto& param : params) {
            if (param.name == "lambda") {
                if (const auto* lit = std::get_if<ir::Literal>(&param.value.node)) {
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        lambda = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        lambda = static_cast<double>(*iv);
                    }
                }
            }
        }
        // Add lambda * I to diagonal (skip intercept if present).
        for (std::size_t j = 0; j < p; ++j) {
            if (formula.has_intercept && j == 0)
                continue;  // don't penalize intercept
            XtX[j][j] += lambda;
        }
        auto beta = solve_spd(XtX, Xty);
        if (!beta) {
            return std::unexpected(beta.error());
        }
        return build_model_result(col_names, X, y.value(), beta.value(), formula, method);
    }

    if (method == "wls") {
        // Extract weights column name.
        std::string weights_col;
        for (const auto& param : params) {
            if (param.name == "weights") {
                if (const auto* ref = std::get_if<ir::ColumnRef>(&param.value.node)) {
                    weights_col = ref->name;
                }
            }
        }
        if (weights_col.empty()) {
            return std::unexpected("wls: requires weights parameter (e.g. weights = w)");
        }
        // Extract weights as double vector.
        auto w_result = extract_response(input, weights_col);
        if (!w_result) {
            return std::unexpected("wls: " + w_result.error());
        }
        const auto& w = w_result.value();

        // WLS: minimise sum w_i*(y_i - x_i'*beta)^2
        // Normal equations: X^T W X beta = X^T W y
        std::vector<std::vector<double>> XtWX(p, std::vector<double>(p, 0.0));
        std::vector<double> XtWy(p, 0.0);
        for (std::size_t a = 0; a < p; ++a) {
            for (std::size_t b = a; b < p; ++b) {
                double s = 0.0;
                for (std::size_t i = 0; i < n; ++i)
                    s += w[i] * X[a][i] * X[b][i];
                XtWX[a][b] = s;
                XtWX[b][a] = s;
            }
            for (std::size_t i = 0; i < n; ++i)
                XtWy[a] += w[i] * X[a][i] * y.value()[i];
        }
        auto beta = solve_spd(XtWX, XtWy);
        if (!beta) {
            return std::unexpected(beta.error());
        }
        return build_model_result(col_names, X, y.value(), beta.value(), formula, method);
    }

    if (externs != nullptr) {
        const std::string fn_name = "model_" + method;
        const auto* fn = externs->find(fn_name);
        if (fn != nullptr) {
            if (!fn->first_arg_is_table || !fn->table_consumer_func) {
                return std::unexpected("model: plugin method '" + fn_name +
                                       "' must be registered as a table consumer");
            }

            Table design_table;
            for (std::size_t j = 0; j < p; ++j) {
                design_table.add_column(col_names[j], Column<double>(X[j]));
            }
            design_table.add_column("__response", Column<double>(y.value()));

            ExternArgs plugin_args;
            plugin_args.emplace_back(ScalarValue{std::string("__response")});
            for (const auto& param : params) {
                if (param.name == "method") {
                    continue;
                }
                plugin_args.emplace_back(ScalarValue{param.name});
                if (const auto* lit = std::get_if<ir::Literal>(&param.value.node)) {
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*dv});
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*iv});
                    } else if (const auto* sv = std::get_if<std::string>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*sv});
                    } else if (const auto* date = std::get_if<Date>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*date});
                    } else if (const auto* ts = std::get_if<Timestamp>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*ts});
                    } else {
                        return std::unexpected("model: unsupported literal parameter type for '" +
                                               param.name + "'");
                    }
                } else if (const auto* ref = std::get_if<ir::ColumnRef>(&param.value.node)) {
                    plugin_args.emplace_back(ScalarValue{std::string("column:") + ref->name});
                } else {
                    return std::unexpected("model: plugin parameter '" + param.name +
                                           "' must be a literal or column reference");
                }
            }

            auto plugin_result = fn->table_consumer_func(design_table, plugin_args);
            if (!plugin_result) {
                return std::unexpected("model (" + method + "): " + plugin_result.error());
            }

            const auto* coef_table = std::get_if<Table>(&plugin_result.value());
            if (coef_table == nullptr) {
                return std::unexpected("model (" + method +
                                       "): plugin must return a coefficients table");
            }
            const auto* term_any = coef_table->find("term");
            const auto* estimate_any = coef_table->find("estimate");
            if (term_any == nullptr || estimate_any == nullptr) {
                return std::unexpected("model (" + method +
                                       "): coefficients table must include 'term' and 'estimate'");
            }
            const auto* term_col = std::get_if<Column<std::string>>(term_any);
            const auto* estimate_col = std::get_if<Column<double>>(estimate_any);
            if (term_col == nullptr || estimate_col == nullptr) {
                return std::unexpected(
                    "model (" + method +
                    "): coefficient columns must be term:String and estimate:Float64");
            }
            if (term_col->size() != estimate_col->size()) {
                return std::unexpected("model (" + method + "): coefficients length mismatch");
            }

            std::unordered_map<std::string, double> beta_by_term;
            beta_by_term.reserve(term_col->size());
            for (std::size_t i = 0; i < term_col->size(); ++i) {
                beta_by_term.insert_or_assign(std::string((*term_col)[i]), (*estimate_col)[i]);
            }

            std::vector<double> beta;
            beta.reserve(col_names.size());
            for (const auto& name : col_names) {
                auto it = beta_by_term.find(name);
                if (it == beta_by_term.end()) {
                    return std::unexpected("model (" + method +
                                           "): missing coefficient for term '" + name + "'");
                }
                beta.push_back(it->second);
            }
            return build_model_result(col_names, X, y.value(), std::move(beta), formula, method);
        }
    }

    return std::unexpected("model: unknown method '" + method +
                           "' (supported built-ins: ols, ridge, wls; plugins: model_<method>)");
}

// ─── Melt (wide → long) ──────────────────────────────────────────────────────

auto melt_table(const Table& input, const std::vector<std::string>& id_columns,
                const std::vector<std::string>& measure_columns)
    -> std::expected<Table, std::string> {
    // Resolve id column indices.
    std::vector<std::size_t> id_indices;
    id_indices.reserve(id_columns.size());
    for (const auto& name : id_columns) {
        auto it = input.index.find(name);
        if (it == input.index.end()) {
            return std::unexpected("melt: id column not found: " + name +
                                   " (available: " + format_columns(input) + ")");
        }
        id_indices.push_back(it->second);
    }

    // Resolve measure column indices — if empty, use all non-id columns.
    std::unordered_set<std::string> id_set(id_columns.begin(), id_columns.end());
    std::vector<std::size_t> measure_indices;
    std::vector<std::string> measure_names;
    if (measure_columns.empty()) {
        for (std::size_t i = 0; i < input.columns.size(); ++i) {
            if (!id_set.contains(input.columns[i].name)) {
                measure_indices.push_back(i);
                measure_names.push_back(input.columns[i].name);
            }
        }
    } else {
        measure_indices.reserve(measure_columns.size());
        measure_names.reserve(measure_columns.size());
        for (const auto& name : measure_columns) {
            auto it = input.index.find(name);
            if (it == input.index.end()) {
                return std::unexpected("melt: measure column not found: " + name +
                                       " (available: " + format_columns(input) + ")");
            }
            measure_indices.push_back(it->second);
            measure_names.push_back(name);
        }
    }

    if (measure_indices.empty()) {
        return std::unexpected("melt: no measure columns to melt");
    }

    // Validate all measure columns have the same type variant index.
    std::size_t first_type = input.columns[measure_indices[0]].column->index();
    for (std::size_t i = 1; i < measure_indices.size(); ++i) {
        if (input.columns[measure_indices[i]].column->index() != first_type) {
            return std::unexpected("melt: all measure columns must have the same type");
        }
    }

    std::size_t rows = input.rows();
    std::size_t n_measures = measure_indices.size();
    std::size_t out_rows = rows * n_measures;

    // Build output table.
    Table output;

    // Id columns: repeat each source row value n_measures times.
    for (std::size_t id_idx : id_indices) {
        const auto& entry = input.columns[id_idx];
        auto col = std::visit(
            [&](const auto& src_col) -> ColumnValue {
                using SrcCol = std::decay_t<decltype(src_col)>;
                if constexpr (std::is_same_v<SrcCol, Column<std::string>>) {
                    Column<std::string> out_col;
                    const auto* src_offs = src_col.offsets_data();
                    const char* src_chars = src_col.chars_data();
                    const std::size_t total_chars =
                        rows > 0 ? static_cast<std::size_t>(src_offs[rows]) * n_measures : 0;
                    out_col.resize_for_gather(out_rows, total_chars);
                    auto* dst_offs = out_col.offsets_data();
                    char* dst_chars = out_col.chars_data();
                    dst_offs[0] = 0;
                    std::size_t out_i = 0;
                    std::size_t out_char = 0;
                    auto emit_repeat_n = [&]<std::size_t N>() {
                        for (std::size_t r = 0; r < rows; ++r) {
                            const std::size_t start = static_cast<std::size_t>(src_offs[r]);
                            const std::size_t end = static_cast<std::size_t>(src_offs[r + 1]);
                            const std::size_t len = end - start;
                            const char* p = src_chars + start;
                            const std::size_t row_char_base = out_char;
                            if (len > 0) {
                                if constexpr (N >= 1) {
                                    std::memcpy(dst_chars + row_char_base, p, len);
                                }
                                if constexpr (N >= 2) {
                                    std::memcpy(dst_chars + row_char_base + len, p, len);
                                }
                                if constexpr (N >= 3) {
                                    std::memcpy(dst_chars + row_char_base + 2 * len, p, len);
                                }
                                if constexpr (N >= 4) {
                                    std::memcpy(dst_chars + row_char_base + 3 * len, p, len);
                                }
                            }
                            if constexpr (N >= 1) {
                                dst_offs[++out_i] = static_cast<std::uint32_t>(row_char_base + len);
                            }
                            if constexpr (N >= 2) {
                                dst_offs[++out_i] =
                                    static_cast<std::uint32_t>(row_char_base + 2 * len);
                            }
                            if constexpr (N >= 3) {
                                dst_offs[++out_i] =
                                    static_cast<std::uint32_t>(row_char_base + 3 * len);
                            }
                            if constexpr (N >= 4) {
                                dst_offs[++out_i] =
                                    static_cast<std::uint32_t>(row_char_base + 4 * len);
                            }
                            out_char += len * N;
                        }
                    };
                    switch (n_measures) {
                        case 1:
                            emit_repeat_n.template operator()<1>();
                            break;
                        case 2:
                            emit_repeat_n.template operator()<2>();
                            break;
                        case 3:
                            emit_repeat_n.template operator()<3>();
                            break;
                        case 4:
                            emit_repeat_n.template operator()<4>();
                            break;
                        default:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t start = static_cast<std::size_t>(src_offs[r]);
                                const std::size_t end = static_cast<std::size_t>(src_offs[r + 1]);
                                const std::size_t len = end - start;
                                const char* p = src_chars + start;
                                const std::size_t row_char_base = out_char;
                                const std::size_t repeated_chars = len * n_measures;
                                if (len > 0 && n_measures > 0) {
                                    std::memcpy(dst_chars + row_char_base, p, len);
                                    std::size_t copied = len;
                                    while (copied < repeated_chars) {
                                        const std::size_t chunk =
                                            std::min(copied, repeated_chars - copied);
                                        std::memcpy(dst_chars + row_char_base + copied,
                                                    dst_chars + row_char_base, chunk);
                                        copied += chunk;
                                    }
                                }
                                for (std::size_t m = 0; m < n_measures; ++m) {
                                    dst_offs[++out_i] =
                                        static_cast<std::uint32_t>(row_char_base + (m + 1) * len);
                                }
                                out_char += repeated_chars;
                            }
                            break;
                    }
                    return out_col;
                } else if constexpr (std::is_same_v<SrcCol, Column<Categorical>>) {
                    Column<Categorical> out_col{src_col.dictionary_ptr(), src_col.index_ptr(), {}};
                    out_col.resize(out_rows);
                    auto* dst_codes = out_col.codes_data();
                    std::size_t out_i = 0;
                    for (std::size_t r = 0; r < rows; ++r) {
                        auto code = src_col.code_at(r);
                        for (std::size_t m = 0; m < n_measures; ++m) {
                            dst_codes[out_i++] = code;
                        }
                    }
                    return out_col;
                } else {
                    SrcCol out_col;
                    out_col.reserve(out_rows);
                    out_col.resize(out_rows);
                    if constexpr (std::is_same_v<SrcCol, Column<bool>>) {
                        switch (n_measures) {
                            case 1:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    out_col.set(r, src_col[r]);
                                }
                                break;
                            case 2:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    const std::size_t base = r * 2;
                                    out_col.set(base, v);
                                    out_col.set(base + 1, v);
                                }
                                break;
                            case 3:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    const std::size_t base = r * 3;
                                    out_col.set(base, v);
                                    out_col.set(base + 1, v);
                                    out_col.set(base + 2, v);
                                }
                                break;
                            case 4:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    const std::size_t base = r * 4;
                                    out_col.set(base, v);
                                    out_col.set(base + 1, v);
                                    out_col.set(base + 2, v);
                                    out_col.set(base + 3, v);
                                }
                                break;
                            default: {
                                std::size_t out_i = 0;
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    for (std::size_t m = 0; m < n_measures; ++m) {
                                        out_col.set(out_i++, v);
                                    }
                                }
                                break;
                            }
                        }
                    } else {
                        auto* dst = out_col.data();
                        switch (n_measures) {
                            case 1:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    dst[r] = src_col[r];
                                }
                                break;
                            case 2:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    const std::size_t base = r * 2;
                                    dst[base] = v;
                                    dst[base + 1] = v;
                                }
                                break;
                            case 3:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    const std::size_t base = r * 3;
                                    dst[base] = v;
                                    dst[base + 1] = v;
                                    dst[base + 2] = v;
                                }
                                break;
                            case 4:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    const std::size_t base = r * 4;
                                    dst[base] = v;
                                    dst[base + 1] = v;
                                    dst[base + 2] = v;
                                    dst[base + 3] = v;
                                }
                                break;
                            default: {
                                std::size_t out_i = 0;
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    for (std::size_t m = 0; m < n_measures; ++m) {
                                        dst[out_i++] = v;
                                    }
                                }
                                break;
                            }
                        }
                    }
                    return out_col;
                }
            },
            *entry.column);

        if (entry.validity.has_value()) {
            ValidityBitmap validity;
            validity.reserve(out_rows);
            for (std::size_t r = 0; r < rows; ++r) {
                bool valid = (*entry.validity)[r];
                for (std::size_t m = 0; m < n_measures; ++m) {
                    validity.push_back(valid);
                }
            }
            output.add_column(entry.name, std::move(col), std::move(validity));
        } else {
            output.add_column(entry.name, std::move(col));
        }
    }
    // "variable" column: Categorical with n_measures dictionary entries.
    // Using Categorical rather than Column<std::string> avoids writing O(N) character
    // data for what is at most n_measures distinct strings — only a cyclic int32 code
    // array is needed (64 MB at 4 M rows × 4 measures), saving ~76 MB of char writes.
    {
        // Build with the dict-vector constructor, which calls rebuild_index() internally.
        Column<Categorical> var_col{std::vector<std::string>(measure_names)};
        var_col.resize(out_rows);
        auto* codes = var_col.codes_data();
        // Fill with repeating cycle [0, 1, 2, ..., n_measures-1, 0, 1, ...].
        // Write one period then exponentially copy the rest.
        for (std::size_t mi = 0; mi < n_measures; ++mi)
            codes[mi] = static_cast<Column<Categorical>::code_type>(mi);
        std::size_t copied = n_measures;
        while (copied < out_rows) {
            const std::size_t chunk = std::min(copied, out_rows - copied);
            std::memcpy(codes + copied, codes, chunk * sizeof(*codes));
            copied += chunk;
        }
        output.add_column("variable", std::move(var_col));
    }
    // "value" column: interleave measure columns in row-major order.
    bool any_measure_validity = false;
    for (std::size_t mi = 0; mi < n_measures; ++mi) {
        if (input.columns[measure_indices[mi]].validity.has_value()) {
            any_measure_validity = true;
            break;
        }
    }

    auto value_col = std::visit(
        [&](const auto& first_col) -> ColumnValue {
            using SrcCol = std::decay_t<decltype(first_col)>;
            if constexpr (std::is_same_v<SrcCol, Column<std::string>>) {
                Column<std::string> out_col;
                std::vector<const Column<std::string>*> measures;
                measures.reserve(n_measures);
                std::size_t total_chars = 0;
                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                    const auto& entry = input.columns[measure_indices[mi]];
                    const auto* src = std::get_if<Column<std::string>>(entry.column.get());
                    if (src == nullptr) {
                        invariant_violation(
                            "melt_table: measure column type mismatch after upfront validation");
                    }
                    measures.push_back(src);
                    const auto* offs = src->offsets_data();
                    total_chars += rows > 0 ? static_cast<std::size_t>(offs[rows]) : 0;
                }
                out_col.resize_for_gather(out_rows, total_chars);
                auto* dst_offs = out_col.offsets_data();
                char* dst_chars = out_col.chars_data();
                dst_offs[0] = 0;
                std::size_t out_i = 0;
                std::size_t out_char = 0;
                for (std::size_t r = 0; r < rows; ++r) {
                    for (std::size_t mi = 0; mi < n_measures; ++mi) {
                        const auto* src_offs = measures[mi]->offsets_data();
                        const char* src_chars = measures[mi]->chars_data();
                        const std::size_t start = static_cast<std::size_t>(src_offs[r]);
                        const std::size_t end = static_cast<std::size_t>(src_offs[r + 1]);
                        const std::size_t len = end - start;
                        if (len > 0) {
                            std::memcpy(dst_chars + out_char, src_chars + start, len);
                        }
                        out_char += len;
                        dst_offs[++out_i] = static_cast<std::uint32_t>(out_char);
                    }
                }
                return out_col;
            } else if constexpr (std::is_same_v<SrcCol, Column<Categorical>>) {
                Column<Categorical> out_col{first_col.dictionary_ptr(), first_col.index_ptr(), {}};
                std::vector<const Column<Categorical>*> measures;
                measures.reserve(n_measures);
                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                    const auto& entry = input.columns[measure_indices[mi]];
                    const auto* src = std::get_if<Column<Categorical>>(entry.column.get());
                    if (src == nullptr) {
                        invariant_violation(
                            "melt_table: measure column type mismatch after upfront validation");
                    }
                    measures.push_back(src);
                }
                out_col.resize(out_rows);
                auto* dst_codes = out_col.codes_data();
                std::size_t out_i = 0;
                for (std::size_t r = 0; r < rows; ++r) {
                    for (std::size_t mi = 0; mi < n_measures; ++mi) {
                        dst_codes[out_i++] = measures[mi]->code_at(r);
                    }
                }
                return out_col;
            } else {
                SrcCol out_col;
                std::vector<const SrcCol*> measures;
                measures.reserve(n_measures);
                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                    const auto& entry = input.columns[measure_indices[mi]];
                    const auto* src = std::get_if<SrcCol>(entry.column.get());
                    if (src == nullptr) {
                        invariant_violation(
                            "melt_table: measure column type mismatch after upfront validation");
                    }
                    measures.push_back(src);
                }
                out_col.resize(out_rows);
                if constexpr (std::is_same_v<SrcCol, Column<bool>>) {
                    switch (n_measures) {
                        case 1:
                            for (std::size_t r = 0; r < rows; ++r) {
                                out_col.set(r, (*measures[0])[r]);
                            }
                            break;
                        case 2:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 2;
                                out_col.set(base, (*measures[0])[r]);
                                out_col.set(base + 1, (*measures[1])[r]);
                            }
                            break;
                        case 3:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 3;
                                out_col.set(base, (*measures[0])[r]);
                                out_col.set(base + 1, (*measures[1])[r]);
                                out_col.set(base + 2, (*measures[2])[r]);
                            }
                            break;
                        case 4:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 4;
                                out_col.set(base, (*measures[0])[r]);
                                out_col.set(base + 1, (*measures[1])[r]);
                                out_col.set(base + 2, (*measures[2])[r]);
                                out_col.set(base + 3, (*measures[3])[r]);
                            }
                            break;
                        default: {
                            std::size_t out_i = 0;
                            for (std::size_t r = 0; r < rows; ++r) {
                                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                                    out_col.set(out_i++, (*measures[mi])[r]);
                                }
                            }
                            break;
                        }
                    }
                } else {
                    auto* dst = out_col.data();
                    switch (n_measures) {
                        case 1: {
                            const auto* m0 = measures[0]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                dst[r] = m0[r];
                            }
                            break;
                        }
                        case 2: {
                            const auto* m0 = measures[0]->data();
                            const auto* m1 = measures[1]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 2;
                                dst[base] = m0[r];
                                dst[base + 1] = m1[r];
                            }
                            break;
                        }
                        case 3: {
                            const auto* m0 = measures[0]->data();
                            const auto* m1 = measures[1]->data();
                            const auto* m2 = measures[2]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 3;
                                dst[base] = m0[r];
                                dst[base + 1] = m1[r];
                                dst[base + 2] = m2[r];
                            }
                            break;
                        }
                        case 4: {
                            const auto* m0 = measures[0]->data();
                            const auto* m1 = measures[1]->data();
                            const auto* m2 = measures[2]->data();
                            const auto* m3 = measures[3]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 4;
                                dst[base] = m0[r];
                                dst[base + 1] = m1[r];
                                dst[base + 2] = m2[r];
                                dst[base + 3] = m3[r];
                            }
                            break;
                        }
                        default: {
                            std::size_t out_i = 0;
                            for (std::size_t r = 0; r < rows; ++r) {
                                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                                    dst[out_i++] = (*measures[mi])[r];
                                }
                            }
                            break;
                        }
                    }
                }
                return out_col;
            }
        },
        *input.columns[measure_indices[0]].column);

    if (any_measure_validity) {
        ValidityBitmap value_validity(out_rows, true);
        std::size_t out_i = 0;
        for (std::size_t r = 0; r < rows; ++r) {
            for (std::size_t mi = 0; mi < n_measures; ++mi, ++out_i) {
                if (is_null(input.columns[measure_indices[mi]], r)) {
                    value_validity.set(out_i, false);
                }
            }
        }
        output.add_column("value", std::move(value_col), std::move(value_validity));
    } else {
        output.add_column("value", std::move(value_col));
    }

    return output;
}

// ─── Dcast (long → wide) ────────────────────────────────────────────────────

// Composite row-key for dcast: up to 8 int64 slots stored inline.
// String columns are pre-interned to int32 codes so all key fields are integers.
// Null is encoded as INT64_MIN (reserved sentinel; never a valid interned code).
// n stores the actual number of key columns so hash and equality only touch the
// used slots — avoids wasting cache on zero padding for typical 1-2 key cases.
// Stored inline in robin_hood::unordered_flat_map — no per-key heap allocation.
struct DcastKey {
    static constexpr std::size_t kMaxCols = 8;
    std::array<std::int64_t, kMaxCols> v{};
    std::uint8_t n{0};
    auto operator==(const DcastKey& o) const noexcept -> bool {
        return std::memcmp(v.data(), o.v.data(), n * sizeof(std::int64_t)) == 0;
    }
};
struct DcastKeyHash {
    auto operator()(const DcastKey& k) const noexcept -> std::size_t {
        // splitmix64-style mixing over the n used int64 slots — fast and low-collision.
        std::uint64_t h = 0;
        for (std::size_t i = 0; i < k.n; ++i) {
            h ^= static_cast<std::uint64_t>(k.v[i]);
            h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
            h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
            h ^= h >> 31;
        }
        return static_cast<std::size_t>(h);
    }
};

auto dcast_table(const Table& input, const std::string& pivot_column,
                 const std::string& value_column, const std::vector<std::string>& row_keys)
    -> std::expected<Table, std::string> {
    constexpr std::size_t kMissingPivot = std::numeric_limits<std::size_t>::max();
    constexpr std::size_t kMissingCell = std::numeric_limits<std::size_t>::max();

    // Validate columns exist.
    auto pivot_it = input.index.find(pivot_column);
    if (pivot_it == input.index.end()) {
        return std::unexpected("dcast: pivot column not found: " + pivot_column +
                               " (available: " + format_columns(input) + ")");
    }
    auto value_it = input.index.find(value_column);
    if (value_it == input.index.end()) {
        return std::unexpected("dcast: value column not found: " + value_column +
                               " (available: " + format_columns(input) + ")");
    }
    std::vector<std::size_t> key_indices;
    key_indices.reserve(row_keys.size());
    for (const auto& name : row_keys) {
        auto it = input.index.find(name);
        if (it == input.index.end()) {
            return std::unexpected("dcast: row key column not found: " + name +
                                   " (available: " + format_columns(input) + ")");
        }
        key_indices.push_back(it->second);
    }

    std::size_t pivot_idx = pivot_it->second;
    std::size_t value_idx = value_it->second;
    std::size_t rows = input.rows();

    // ── Pass 1: collect distinct pivot values, build fast inline lookup ───────
    // We keep typed lookup structures so the main loop can resolve the pivot index
    // per row without a separate row_pivot_index[rows] array (avoids O(rows) allocation).
    std::vector<std::string> pivot_values;
    const auto& pivot_col = *input.columns[pivot_idx].column;

    // Only one of these will be populated depending on the pivot column type:
    std::vector<std::size_t> cat_code_to_pvi;  // Categorical: code → pivot_idx
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> int_pvi_map;  // Int64
    robin_hood::unordered_flat_map<std::string, std::size_t, StringViewHash, StringViewEq>
        str_pvi_map;  // String/other

    if (const auto* cat_col = std::get_if<Column<Categorical>>(&pivot_col)) {
        const auto& dict = cat_col->dictionary();
        cat_code_to_pvi.assign(dict.size(), kMissingPivot);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[pivot_idx], r))
                continue;
            const auto code = cat_col->code_at(r);
            if (code < 0)
                continue;
            const auto ci = static_cast<std::size_t>(code);
            if (ci >= dict.size())
                continue;
            if (cat_code_to_pvi[ci] == kMissingPivot) {
                cat_code_to_pvi[ci] = pivot_values.size();
                pivot_values.push_back(dict[ci]);
            }
        }
    } else if (const auto* int_col = std::get_if<Column<std::int64_t>>(&pivot_col)) {
        int_pvi_map.reserve(16);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[pivot_idx], r))
                continue;
            const std::int64_t pv = (*int_col)[r];
            auto [it, inserted] = int_pvi_map.try_emplace(pv, pivot_values.size());
            if (inserted)
                pivot_values.push_back(std::to_string(pv));
        }
    } else {
        str_pvi_map.reserve(16);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[pivot_idx], r))
                continue;
            std::string pv = std::visit(
                [r](const auto& col) -> std::string {
                    using ColType = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                        return std::string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                        return std::string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                        return std::to_string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                        return std::to_string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<bool>>) {
                        return col[r] ? "true" : "false";
                    } else {
                        return std::to_string(r);
                    }
                },
                pivot_col);
            auto [it, inserted] = str_pvi_map.try_emplace(std::move(pv), pivot_values.size());
            if (inserted)
                pivot_values.push_back(it->first);
        }
    }

    if (pivot_values.empty()) {
        // No non-null pivot values; return just the key columns.
        Table output;
        for (std::size_t ki : key_indices) {
            const auto& entry = input.columns[ki];
            output.add_column(entry.name, *entry.column);
            if (entry.validity.has_value()) {
                output.columns.back().validity = entry.validity;
            }
        }
        return output;
    }

    std::size_t n_pivots = pivot_values.size();

    // ── Pass 2a: intern string key columns ───────────────────────────────────
    // Convert each Column<std::string> key column to a dense int32 code array so
    // the main loop works entirely with integers — no string copies, no SSO issues.
    // Column<Categorical> and Column<int64> are used directly (no pre-pass needed).
    //
    // Each str_intern[k] is either empty (non-string column) or has `rows` entries.
    constexpr std::int64_t kNullKey = std::numeric_limits<std::int64_t>::min();
    const std::size_t n_keys = key_indices.size();
    std::vector<std::vector<std::int32_t>> str_intern(n_keys);

    for (std::size_t k = 0; k < n_keys; ++k) {
        const std::size_t ki = key_indices[k];
        const auto* str_col = std::get_if<Column<std::string>>(input.columns[ki].column.get());
        if (str_col == nullptr)
            continue;
        auto& codes = str_intern[k];
        codes.reserve(rows);
        robin_hood::unordered_flat_map<std::string_view, std::int32_t, StringViewHash, StringViewEq>
            sv_to_code;
        sv_to_code.reserve(rows / n_pivots + 1);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[ki], r)) {
                codes.push_back(-1);
                continue;
            }
            const std::string_view sv = (*str_col)[r];
            auto [it, inserted] =
                sv_to_code.try_emplace(sv, static_cast<std::int32_t>(sv_to_code.size()));
            codes.push_back(it->second);
        }
    }

    // ── Pass 2b: group by row_keys + populate cell_rows ──────────────────────
    // Two key-path implementations share the same output structures:
    //   fast  (n_keys ≤ 8): DcastKey inline int64 array — no per-key heap alloc.
    //   fallback (n_keys > 8): raw-bytes std::string key — correct for any arity.
    // Both use a previous-key cache to skip hash lookups for consecutive same-key
    // rows (common in melt output where n_pivots consecutive rows share one key).
    const std::size_t est_out_rows = rows / n_pivots + 1;

    std::vector<std::size_t> first_input_row;
    first_input_row.reserve(est_out_rows);
    // Dense map: (out_row * n_pivots + pvi) → input_row, or kMissingCell.
    std::vector<std::size_t> cell_rows(est_out_rows * n_pivots, kMissingCell);

    // Helper: encode key column k at row r as a single int64.
    const auto encode_key = [&](std::size_t k, std::size_t r) -> std::int64_t {
        const std::size_t ki = key_indices[k];
        if (is_null(input.columns[ki], r))
            return kNullKey;
        if (!str_intern[k].empty())
            return str_intern[k][r];  // interned string code
        return std::visit(
            [r](const auto& c) -> std::int64_t {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, Column<Categorical>>)
                    return static_cast<std::int64_t>(c.code_at(r));
                else if constexpr (std::is_same_v<T, Column<std::int64_t>>)
                    return c[r];
                else if constexpr (std::is_same_v<T, Column<double>>)
                    return static_cast<std::int64_t>(c[r]);
                else if constexpr (std::is_same_v<T, Column<bool>>)
                    return static_cast<std::int64_t>(c[r] ? 1 : 0);
                else if constexpr (std::is_same_v<T, Column<Date>>)
                    return static_cast<std::int64_t>(c[r].days);
                else if constexpr (std::is_same_v<T, Column<Timestamp>>)
                    return c[r].nanos;
                else
                    return kNullKey;  // Column<std::string> — should be pre-interned
            },
            *input.columns[ki].column);
    };

    // ── helper: resolve pivot index for row r (shared by both key paths) ──────
    const auto resolve_pvi = [&](std::size_t r) -> std::size_t {
        if (is_null(input.columns[pivot_idx], r))
            return kMissingPivot;
        if (!cat_code_to_pvi.empty()) {
            const auto* cat_col = std::get_if<Column<Categorical>>(&pivot_col);
            const auto code = cat_col->code_at(r);
            if (code >= 0) {
                const auto ci = static_cast<std::size_t>(code);
                if (ci < cat_code_to_pvi.size())
                    return cat_code_to_pvi[ci];
            }
            return kMissingPivot;
        }
        if (!int_pvi_map.empty()) {
            const auto* int_col = std::get_if<Column<std::int64_t>>(&pivot_col);
            auto it = int_pvi_map.find((*int_col)[r]);
            return it != int_pvi_map.end() ? it->second : kMissingPivot;
        }
        if (const auto* str_col_ptr = std::get_if<Column<std::string>>(&pivot_col)) {
            auto it = str_pvi_map.find((*str_col_ptr)[r]);
            return it != str_pvi_map.end() ? it->second : kMissingPivot;
        }
        const std::string pv = std::visit(
            [r](const auto& col) -> std::string {
                using ColType = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColType, Column<Categorical>>)
                    return std::string(col[r]);
                else if constexpr (std::is_same_v<ColType, Column<double>>)
                    return std::to_string(col[r]);
                else if constexpr (std::is_same_v<ColType, Column<bool>>)
                    return col[r] ? "true" : "false";
                else
                    return std::to_string(r);
            },
            pivot_col);
        auto it = str_pvi_map.find(pv);
        return it != str_pvi_map.end() ? it->second : kMissingPivot;
    };

    if (n_keys <= DcastKey::kMaxCols) {
        // ── fast path: DcastKey (inline int64 array, no heap per key) ────────
        robin_hood::unordered_flat_map<DcastKey, std::size_t, DcastKeyHash> key_to_row;
        key_to_row.reserve(est_out_rows);
        DcastKey prev_key{};
        prev_key.n = static_cast<std::uint8_t>(n_keys);
        std::size_t prev_out_row = std::numeric_limits<std::size_t>::max();

        for (std::size_t r = 0; r < rows; ++r) {
            const std::size_t pvi = resolve_pvi(r);
            if (pvi == kMissingPivot)
                continue;

            DcastKey key{};
            key.n = static_cast<std::uint8_t>(n_keys);
            for (std::size_t k = 0; k < n_keys; ++k)
                key.v[k] = encode_key(k, r);

            std::size_t out_row;
            if (key == prev_key && prev_out_row != std::numeric_limits<std::size_t>::max()) {
                out_row = prev_out_row;
            } else {
                auto [it, inserted] = key_to_row.try_emplace(key, first_input_row.size());
                if (inserted) {
                    first_input_row.push_back(r);
                    const std::size_t needed = first_input_row.size() * n_pivots;
                    if (needed > cell_rows.size())
                        cell_rows.resize(std::max(needed, cell_rows.size() * 2), kMissingCell);
                }
                out_row = it->second;
                prev_key = key;
                prev_out_row = out_row;
            }
            cell_rows[out_row * n_pivots + pvi] = r;
        }
    } else {
        // ── fallback path: raw-byte string key for >8 key columns ────────────
        // Each key is n_keys*8 bytes (the raw int64 values from encode_key).
        // Slightly slower due to string allocation but correct for any key count.
        robin_hood::unordered_map<std::string, std::size_t> key_to_row_str;
        key_to_row_str.reserve(est_out_rows);
        const std::size_t key_bytes = n_keys * sizeof(std::int64_t);
        std::string prev_key_str(key_bytes, '\0');
        std::size_t prev_out_row = std::numeric_limits<std::size_t>::max();

        for (std::size_t r = 0; r < rows; ++r) {
            const std::size_t pvi = resolve_pvi(r);
            if (pvi == kMissingPivot)
                continue;

            std::string key(key_bytes, '\0');
            for (std::size_t k = 0; k < n_keys; ++k) {
                std::int64_t v = encode_key(k, r);
                std::memcpy(key.data() + k * sizeof(std::int64_t), &v, sizeof(v));
            }

            std::size_t out_row;
            if (key == prev_key_str && prev_out_row != std::numeric_limits<std::size_t>::max()) {
                out_row = prev_out_row;
            } else {
                auto [it, inserted] = key_to_row_str.try_emplace(key, first_input_row.size());
                if (inserted) {
                    first_input_row.push_back(r);
                    const std::size_t needed = first_input_row.size() * n_pivots;
                    if (needed > cell_rows.size())
                        cell_rows.resize(std::max(needed, cell_rows.size() * 2), kMissingCell);
                }
                out_row = it->second;
                prev_key_str = std::move(key);
                prev_out_row = out_row;
            }
            cell_rows[out_row * n_pivots + pvi] = r;
        }
    }

    std::size_t out_rows = first_input_row.size();

    // Build output table.
    Table output;

    // Row key columns — take the first row for each group.
    for (std::size_t k = 0; k < row_keys.size(); ++k) {
        std::size_t ki = key_indices[k];
        const auto& entry = input.columns[ki];
        auto col = make_empty_like(*entry.column);
        std::visit([out_rows](auto& c) { c.reserve(out_rows); }, col);
        for (std::size_t or_idx = 0; or_idx < out_rows; ++or_idx) {
            append_value(col, *entry.column, first_input_row[or_idx]);
        }
        output.add_column(entry.name, std::move(col));
    }

    // Pivot value columns.
    const auto& value_entry = input.columns[value_idx];
    for (std::size_t pi = 0; pi < n_pivots; ++pi) {
        auto col = make_empty_like(*value_entry.column);
        std::visit([out_rows](auto& c) { c.reserve(out_rows); }, col);
        ValidityBitmap validity(out_rows, false);
        bool has_nulls = false;

        for (std::size_t or_idx = 0; or_idx < out_rows; ++or_idx) {
            std::size_t cell_key = or_idx * n_pivots + pi;
            const std::size_t input_row = cell_rows[cell_key];
            if (input_row != kMissingCell) {
                append_value(col, *value_entry.column, input_row);
                bool val_null = is_null(value_entry, input_row);
                validity.set(or_idx, !val_null);
                if (val_null)
                    has_nulls = true;
            } else {
                // Missing cell — append a default value and mark as null.
                std::visit([](auto& c) { c.push_back({}); }, col);
                has_nulls = true;
            }
        }

        if (has_nulls) {
            output.add_column(pivot_values[pi], std::move(col), std::move(validity));
        } else {
            output.add_column(pivot_values[pi], std::move(col));
        }
    }

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
        case ir::NodeKind::Update: {
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            if (update.children().empty()) {
                return std::unexpected("update node missing child");
            }
            if (!update.group_by().empty()) {
                return std::unexpected("grouped update not supported in interpreter");
            }
            auto child = interpret_node(*update.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
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
                    t.columns[idx_it->second].column =
                        std::make_shared<ColumnValue>(std::move(ts_col));
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
        case ir::NodeKind::ExternCall: {
            const auto& ec = static_cast<const ir::ExternCallNode&>(node);
            if (externs == nullptr) {
                return std::unexpected("extern call with no registry: " + ec.callee());
            }
            const auto* fn = externs->find(ec.callee());
            if (fn == nullptr) {
                return std::unexpected("unknown extern function: " + ec.callee());
            }
            if (fn->kind != ExternReturnKind::Table) {
                return std::unexpected("extern function does not return a table: " + ec.callee());
            }
            ExternArgs args;
            args.reserve(ec.args().size());
            for (const auto& arg : ec.args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val)
                    return std::unexpected(val.error());
                args.push_back(std::move(val.value()));
            }
            auto result = fn->func(args);
            if (!result)
                return std::unexpected(result.error());
            if (auto* table = std::get_if<Table>(&result.value())) {
                return std::move(*table);
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
            const ir::FilterExpr* pred =
                join.predicate().has_value() ? join.predicate()->get() : nullptr;
            return join_table_impl(left.value(), right.value(), join.kind(), join.keys(), pred,
                                   scalars);
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
                        dst.columns.push_back(ColumnEntry{
                            .name = entry.name,
                            .column = std::make_shared<ColumnValue>(make_empty_like(*entry.column)),
                            .validity = std::nullopt});
                        dst.index[entry.name] = dst.columns.size() - 1;
                    }
                    dst.time_index = src.time_index;
                    dst.ordering = src.ordering;
                }
                for (std::size_t row = 0; row < src.rows(); ++row) {
                    for (std::size_t col = 0; col < src.columns.size(); ++col) {
                        if (col >= dst.columns.size()) {
                            return std::unexpected("stream: source schema changed mid-stream");
                        }
                        append_value(*dst.columns[col].column, *src.columns[col].column, row);
                        bool null = is_null(src.columns[col], row);
                        if (null) {
                            if (!dst.columns[col].validity.has_value()) {
                                dst.columns[col].validity =
                                    ValidityBitmap(column_size(*dst.columns[col].column) - 1, true);
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
                    out.columns.push_back(ColumnEntry{
                        .name = entry.name,
                        .column = std::make_shared<ColumnValue>(make_empty_like(*entry.column)),
                        .validity = std::nullopt});
                    out.index[entry.name] = out.columns.size() - 1;
                    append_value(*out.columns.back().column, *entry.column, r);
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

            // Returns current CLOCK_REALTIME in nanoseconds.
            auto wall_now_ns = []() -> std::int64_t {
                struct timespec ts{};
                clock_gettime(CLOCK_REALTIME, &ts);
                return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000LL +
                       static_cast<std::int64_t>(ts.tv_nsec);
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
                    // Wall-clock end-of-bucket flush: emit the open bucket as soon as
                    // bucket_ns wall-clock time has elapsed since it was opened.  This
                    // fires on both normal data batches and StreamTimeout returns so
                    // that idle periods do not delay delivery of the closed bucket.
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
                        // Process row-by-row so we can also detect bucket boundaries from
                        // data timestamps (handles replayed/historical data where wall-clock
                        // does not apply).
                        for (std::size_t r = 0; r < batch.rows(); ++r) {
                            Table row_tbl = slice_row(batch, r);
                            auto ts_opt = get_last_ts_ns(row_tbl);
                            std::int64_t row_bucket =
                                ts_opt ? ((*ts_opt / bucket_ns) * bucket_ns) : -1;

                            if (open_bucket_ns >= 0 && row_bucket >= 0 &&
                                row_bucket > open_bucket_ns) {
                                // Bucket boundary crossed — flush the closed bucket.
                                auto er = emit_buffer(buffer);
                                if (!er)
                                    return std::unexpected(er.error());
                                buffer = Table{};
                            }
                            if (row_bucket >= 0) {
                                if (row_bucket != open_bucket_ns) {
                                    // New bucket — record the wall-clock open time.
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
                    // PerRow — append batch to rolling buffer, run transform, emit last rows.
                    const auto& batch = std::get<Table>(src_result.value());
                    auto app = append_table(buffer, batch);
                    if (!app)
                        return std::unexpected(app.error());
                    auto er = emit_buffer(buffer);
                    if (!er)
                        return std::unexpected(er.error());
                }
            }

            // Flush any remaining buffered rows (TimeBucket only).
            if (sn.stream_kind() == ir::StreamKind::TimeBucket && buffer.rows() > 0) {
                auto er = emit_buffer(buffer);
                if (!er)
                    return std::unexpected(er.error());
            }

            return Table{};  // stream execution produces no result table
        }
        case ir::NodeKind::Construct: {
            const auto& cn = static_cast<const ir::ConstructNode&>(node);
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
            // Extract coefficients before potentially moving the whole result.
            Table coef = result.value().coefficients;
            if (model_out != nullptr) {
                *model_out = std::move(result.value());
            }
            return coef;
        }
        case ir::NodeKind::Program:
            return std::unexpected("ProgramNode cannot be interpreted at runtime");
    }
    return std::unexpected("unknown node kind");
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace

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

auto Table::rows() const noexcept -> std::size_t {
    if (columns.empty()) {
        return 0;
    }
    return column_size(*columns.front().column);
}

auto interpret(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
               const ExternRegistry* externs, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    return interpret_node(node, registry, scalars, externs, model_out);
}

auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                 const std::vector<std::string>& keys, const ir::FilterExpr* predicate,
                 const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    return join_table_impl(left, right, kind, keys, predicate, scalars);
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
