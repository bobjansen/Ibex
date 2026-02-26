#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <emmintrin.h>
#include <limits>
#include <numeric>
#include <optional>
#include <robin_hood.h>
#include <stdexcept>
#include <string_view>
#include <thread>
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

auto append_value(ColumnValue& out, const ColumnValue& src, std::size_t index) -> void {
    std::visit(
        [&](auto& dst_col) {
            using ColType = std::decay_t<decltype(dst_col)>;
            const auto* src_col = std::get_if<ColType>(&src);
            if (src_col == nullptr) {
                throw std::runtime_error("column type mismatch");
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

// Either a pointer into the table (zero-copy) or an owned computed column.
using ColResult = std::variant<const ColumnValue*, ColumnValue>;

auto deref_col(const ColResult& r) -> const ColumnValue& {
    return std::visit(
        [](const auto& v) -> const ColumnValue& {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, const ColumnValue*>)
                return *v;
            else
                return v;
        },
        r);
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

// Dispatch column-vs-scalar comparison over all type combinations.
using LitVal = std::variant<std::int64_t, double, std::string, Date, Timestamp>;
template <typename T>
constexpr bool is_string_like_v =
    std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>;
auto compare_col_scalar(ir::CompareOp op, const ColumnValue& col, const LitVal& lit, std::size_t n)
    -> std::expected<std::vector<uint8_t>, std::string> {
    std::vector<uint8_t> mask(n);
    uint8_t* mp = mask.data();
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
            return mask;
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
                return mask;
            }
            if (op == ir::CompareOp::Eq || op == ir::CompareOp::Ne) {
                uint8_t v = (op == ir::CompareOp::Ne) ? 1 : 0;
                std::fill(mp, mp + n, v);
                return mask;
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
            return mask;
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
            return mask;
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
            return mask;
        }
        return std::unexpected("filter: cannot compare timestamp and non-timestamp");
    }

    if (const auto* int_col = std::get_if<Column<std::int64_t>>(&col)) {
        const std::int64_t* cp = int_col->data();
        if (const auto* i = std::get_if<std::int64_t>(&lit)) {
            cmp_col_scalar_into(op, cp, *i, mp, n);
            return mask;
        }
        if (const auto* d = std::get_if<double>(&lit)) {
            cmp_col_scalar_into(op, cp, *d, mp, n);
            return mask;
        }
    }
    if (const auto* dbl_col = std::get_if<Column<double>>(&col)) {
        const double* cp = dbl_col->data();
        if (const auto* i = std::get_if<std::int64_t>(&lit)) {
            cmp_col_scalar_into(op, cp, *i, mp, n);
            return mask;
        }
        if (const auto* d = std::get_if<double>(&lit)) {
            cmp_col_scalar_into(op, cp, *d, mp, n);
            return mask;
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
            return mask;
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
            return mask;
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
            return mask;
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
            return mask;
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
auto compare_vec(ir::CompareOp op, const ColumnValue& lhs, const ColumnValue& rhs, std::size_t n)
    -> std::expected<std::vector<uint8_t>, std::string> {
    std::vector<uint8_t> mask(n);
    uint8_t* mp = mask.data();
    if (const auto* l = std::get_if<Column<std::int64_t>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            return mask;
        }
        if (const auto* r = std::get_if<Column<double>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            return mask;
        }
    }
    if (const auto* l = std::get_if<Column<double>>(&lhs)) {
        if (const auto* r = std::get_if<Column<double>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            return mask;
        }
        if (const auto* r = std::get_if<Column<std::int64_t>>(&rhs)) {
            cmp_into(op, l->data(), r->data(), mp, n);
            return mask;
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
            return mask;
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
            return mask;
        }
    }
    auto cmp_string_views = [&](auto&& lcol, auto&& rcol) -> std::vector<uint8_t> {
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
        return mask;
    };

    if (const auto* l = std::get_if<Column<std::string>>(&lhs)) {
        if (const auto* r = std::get_if<Column<std::string>>(&rhs)) {
            return cmp_string_views(*l, *r);
        }
        if (const auto* r = std::get_if<Column<Categorical>>(&rhs)) {
            return cmp_string_views(*l, *r);
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
                return mask;
            }
            return cmp_string_views(*l, *r);
        }
        if (const auto* r = std::get_if<Column<std::string>>(&rhs)) {
            return cmp_string_views(*l, *r);
        }
    }
    return std::unexpected("filter: incompatible column types in comparison");
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
                if (const auto* col = table.find(node.name))
                    return ColResult{col};
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
                return ColResult{std::move(*result)};
            } else {
                return std::unexpected("filter: not a value expression");
            }
        },
        expr.node);
}

// Compute a uint8_t boolean mask for all n rows.
// For FilterCmp with a literal on either side, uses the scalar fast-path
// (compare_col_scalar) which avoids allocating a broadcast column.
auto compute_mask(const ir::FilterExpr& expr, const Table& table, const ScalarRegistry* scalars,
                  std::size_t n) -> std::expected<std::vector<uint8_t>, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<std::vector<uint8_t>, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::FilterCmp>) {
                // Fast path: column/expr op literal (no broadcast needed).
                if (const auto* lit = std::get_if<ir::FilterLiteral>(&node.right->node)) {
                    auto lhs = eval_value_vec(*node.left, table, scalars, n);
                    if (!lhs)
                        return std::unexpected(lhs.error());
                    return compare_col_scalar(node.op, deref_col(*lhs), lit->value, n);
                }
                // Fast path: literal op column/expr (flip the operator).
                if (const auto* lit = std::get_if<ir::FilterLiteral>(&node.left->node)) {
                    auto rhs = eval_value_vec(*node.right, table, scalars, n);
                    if (!rhs)
                        return std::unexpected(rhs.error());
                    return compare_col_scalar(flip_cmp(node.op), deref_col(*rhs), lit->value, n);
                }
                // General: both sides are column expressions.
                auto lhs = eval_value_vec(*node.left, table, scalars, n);
                if (!lhs)
                    return std::unexpected(lhs.error());
                auto rhs = eval_value_vec(*node.right, table, scalars, n);
                if (!rhs)
                    return std::unexpected(rhs.error());
                return compare_vec(node.op, deref_col(*lhs), deref_col(*rhs), n);
            } else if constexpr (std::is_same_v<T, ir::FilterAnd>) {
                auto left = compute_mask(*node.left, table, scalars, n);
                if (!left)
                    return std::unexpected(left.error());
                auto right = compute_mask(*node.right, table, scalars, n);
                if (!right)
                    return std::unexpected(right.error());
                uint8_t* lp = left->data();
                const uint8_t* rp = right->data();
                for (std::size_t i = 0; i < n; ++i)
                    lp[i] &= rp[i];
                return std::move(*left);
            } else if constexpr (std::is_same_v<T, ir::FilterOr>) {
                auto left = compute_mask(*node.left, table, scalars, n);
                if (!left)
                    return std::unexpected(left.error());
                auto right = compute_mask(*node.right, table, scalars, n);
                if (!right)
                    return std::unexpected(right.error());
                uint8_t* lp = left->data();
                const uint8_t* rp = right->data();
                for (std::size_t i = 0; i < n; ++i)
                    lp[i] |= rp[i];
                return std::move(*left);
            } else if constexpr (std::is_same_v<T, ir::FilterNot>) {
                auto mask = compute_mask(*node.operand, table, scalars, n);
                if (!mask)
                    return std::unexpected(mask.error());
                for (auto& v : *mask)
                    v ^= 1;
                return std::move(*mask);
            } else {
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
    const uint8_t* mp = mask_result->data();

    // Count matching rows for pre-allocation.
    std::size_t out_n = 0;
    for (std::size_t i = 0; i < n; ++i)
        out_n += mp[i];

    std::vector<std::size_t> selected;
    selected.resize(out_n);
    {
        std::size_t j = 0;
        for (std::size_t i = 0; i < n; ++i) {
            if (mp[i]) {
                selected[j++] = i;
            }
        }
    }

    // Gather: for each column, copy only the matching rows.
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(*entry.column));
    }

    constexpr std::size_t kParallelRows = 200'000;
    const bool use_parallel = out_n >= kParallelRows && input.columns.size() >= 2 &&
                              std::thread::hardware_concurrency() > 1;

    auto copy_column = [&](std::size_t col_idx) {
        const auto& src_entry = input.columns[col_idx];
        auto& dst_entry = output.columns[col_idx];
        std::visit(
            [&](const auto& src) {
                using ColT = std::decay_t<decltype(src)>;
                auto* dst = std::get_if<ColT>(dst_entry.column.get());
                if (dst == nullptr) {
                    throw std::runtime_error("filter: column type mismatch");
                }
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    dst->resize(out_n);
                    const auto* sp = src.codes_data();
                    auto* dp = dst->codes_data();
                    for (std::size_t j = 0; j < out_n; ++j) {
                        dp[j] = sp[selected[j]];
                    }
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    // Two-pass flat-buffer gather: compute total bytes, then bulk-memcpy slabs.
                    const uint32_t* src_off = src.offsets_data();
                    std::size_t total_chars = 0;
                    for (std::size_t j = 0; j < out_n; ++j) {
                        std::size_t si = selected[j];
                        total_chars += src_off[si + 1] - src_off[si];
                    }
                    dst->resize_for_gather(out_n, total_chars);
                    uint32_t* dst_off = dst->offsets_data();
                    char* dst_char = dst->chars_data();
                    const char* src_char = src.chars_data();
                    dst_off[0] = 0;
                    uint32_t cur = 0;
                    for (std::size_t j = 0; j < out_n; ++j) {
                        std::size_t si = selected[j];
                        uint32_t len = src_off[si + 1] - src_off[si];
                        std::memcpy(dst_char + cur, src_char + src_off[si], len);
                        cur += len;
                        dst_off[j + 1] = cur;
                    }
                } else {
                    using T = typename ColT::value_type;
                    dst->resize(out_n);
                    const T* sp = src.data();
                    T* dp = dst->data();
                    for (std::size_t j = 0; j < out_n; ++j) {
                        dp[j] = sp[selected[j]];
                    }
                }
            },
            *src_entry.column);
    };

    if (use_parallel) {
        const std::size_t cols = input.columns.size();
        const std::size_t hw = std::max<unsigned>(1, std::thread::hardware_concurrency());
        const std::size_t threads = std::min(cols, hw);
        const std::size_t chunk = (cols + threads - 1) / threads;
        std::vector<std::thread> workers;
        workers.reserve(threads);
        for (std::size_t t = 0; t < threads; ++t) {
            std::size_t start = t * chunk;
            if (start >= cols) {
                break;
            }
            std::size_t end = std::min(cols, start + chunk);
            workers.emplace_back([&, start, end] {
                for (std::size_t c = start; c < end; ++c) {
                    copy_column(c);
                }
            });
        }
        for (auto& th : workers) {
            th.join();
        }
    } else {
        for (std::size_t c = 0; c < input.columns.size(); ++c) {
            copy_column(c);
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
        const auto* source = input.find(col.name);
        if (source == nullptr) {
            return std::unexpected("select column not found: " + col.name +
                                   " (available: " + format_columns(input) + ")");
        }
        output.add_column(col.name, *source);
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

enum class ExprType : std::uint8_t {
    Int,
    Double,
    String,
    Date,
    Timestamp,
};

using ExprValue = std::variant<std::int64_t, double, std::string, Date, Timestamp>;

struct AggSlot {
    ir::AggFunc func = ir::AggFunc::Sum;
    ExprType kind = ExprType::Int;
    bool has_value = false;
    std::int64_t count = 0;
    std::int64_t int_value = 0;
    double double_value = 0.0;
    double sum = 0.0;
    ScalarValue first_value;
    ScalarValue last_value;
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
            ++hists[p][(k >> (p * 8u)) & 0xFFu];
    }

    std::vector<std::uint64_t> dst_keys(rows);
    std::vector<Idx> src_idx(rows), dst_idx(rows);
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

        auto shift = pass * 8u;
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
                std::size_t pb = (src_keys[i + kPrefetchDist] >> shift) & 0xFFu;
                __builtin_prefetch(&dst_keys[cnt[pb]], 1, 1);
                __builtin_prefetch(&dst_idx[cnt[pb]], 1, 1);
            }
            std::size_t bucket = (src_keys[i] >> shift) & 0xFFu;
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
        }
        output.ordering = std::move(resolved_keys);
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    };

    // Fast path: single ascending I64 key (covers all TimeFrame as_timeframe sorts).
    // Moves flat_keys[0].u64 into the sort — no key copy.
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::I64 && flat_keys[0].ascending) {
        // Pre-sorted check: one sequential scan over the already-extracted u64 keys.
        // For already-sorted input (common in TimeFrame) this avoids both the radix
        // sort and the gather — O(n) scan + O(n) table copy instead of O(n·passes) sort.
        const auto& u64 = flat_keys[0].u64;
        bool already_sorted = true;
        for (std::size_t i = 1; i < rows; ++i) {
            if (u64[i] < u64[i - 1]) {
                already_sorted = false;
                break;
            }
        }
        if (already_sorted) {
            Table output = input;
            output.ordering = std::move(resolved_keys);
            output.time_index = input.time_index;
            normalize_time_index(output);
            return output;
        }
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
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ValueType, double>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(static_cast<double>(*int_value));
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(*double_value);
                } else {
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ValueType, std::string_view>) {
                // Column<std::string> flat-buffer specialization uses value_type=string_view.
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ValueType, Date>) {
                if (const auto* date_value = std::get_if<Date>(&value)) {
                    col.push_back(*date_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(int64_to_date_checked(*int_value));
                } else {
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                if (const auto* ts_value = std::get_if<Timestamp>(&value)) {
                    col.push_back(*ts_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(Timestamp{*int_value});
                } else {
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    throw std::runtime_error("type mismatch");
                }
            }
        },
        column);
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
    throw std::runtime_error("type mismatch");
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
    throw std::runtime_error("type mismatch");
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

auto default_scalar_for_column(const ColumnValue& column) -> ScalarValue {
    return std::visit(
        [](const auto& col) -> ScalarValue {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                return std::int64_t{0};
            } else if constexpr (std::is_same_v<ValueType, double>) {
                return 0.0;
            } else if constexpr (std::is_same_v<ValueType, Date>) {
                return Date{};
            } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                return Timestamp{};
            } else {
                return std::string{};
            }
        },
        column);
}

auto column_kind(const ColumnValue& column) -> ExprType {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return ExprType::Double;
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
                     const std::vector<std::string>& keys) -> std::expected<Table, std::string> {
    if (kind == ir::JoinKind::Asof) {
        return std::unexpected("asof join is not supported yet");
    }
    if (keys.empty()) {
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

    std::unordered_set<std::string> key_set(keys.begin(), keys.end());

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
        if (key_set.contains(entry.name)) {
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

    std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_index;
    right_index.reserve(right.rows());
    for (std::size_t r = 0; r < right.rows(); ++r) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : right_keys) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        right_index[key].push_back(r);
    }

    auto append_left_row = [&](std::size_t row) {
        for (std::size_t i = 0; i < left.columns.size(); ++i) {
            append_value(*output.columns[i].column, *left.columns[i].column, row);
        }
    };

    auto append_right_row = [&](std::size_t row) {
        for (const auto& item : right_out) {
            append_value(*output.columns[item.out_index].column, *item.column, row);
        }
    };

    auto append_right_defaults = [&]() {
        for (const auto& item : right_out) {
            append_scalar(*output.columns[item.out_index].column,
                          default_scalar_for_column(*item.column));
        }
    };

    for (std::size_t l = 0; l < left.rows(); ++l) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : left_keys) {
            key.values.push_back(scalar_from_column(*col, l));
        }
        auto it = right_index.find(key);
        if (it == right_index.end()) {
            if (kind == ir::JoinKind::Left) {
                append_left_row(l);
                append_right_defaults();
            }
            continue;
        }
        for (auto r : it->second) {
            append_left_row(l);
            append_right_row(r);
        }
    }

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
    for (const auto& agg : aggregations) {
        if (agg.func == ir::AggFunc::Count) {
            agg_columns.push_back(nullptr);
            continue;
        }
        const auto* column = input.find(agg.column.name);
        if (column == nullptr) {
            return std::unexpected("aggregate column not found: " + agg.column.name +
                                   " (available: " + format_columns(input) + ")");
        }
        agg_columns.push_back(column);
    }

    struct AggPlanItem {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
        const Column<std::int64_t>* int_col = nullptr;
        const Column<double>* dbl_col = nullptr;
        const Column<std::string>* str_col = nullptr;
        const Column<Categorical>* cat_col = nullptr;
    };

    std::vector<AggPlanItem> plan;
    plan.reserve(aggregations.size());
    bool numeric_only = true;
    for (std::size_t i = 0; i < aggregations.size(); ++i) {
        const auto& agg = aggregations[i];
        AggPlanItem item;
        item.func = agg.func;
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
             agg.func == ir::AggFunc::Min || agg.func == ir::AggFunc::Max)) {
            return std::unexpected("string aggregation not supported");
        }

        if (agg.func == ir::AggFunc::First || agg.func == ir::AggFunc::Last) {
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
            }
        }
        return std::nullopt;
    };

    std::size_t rows = input.rows();
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

                if (item.func == ir::AggFunc::Count) {
                    std::vector<std::int64_t> acc(n_groups, 0);
                    for (std::size_t row = 0; row < rows; ++row) {
                        acc[gids[row]]++;
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = acc[g];
                    }
                    continue;
                }

                if (item.func == ir::AggFunc::First) {
                    std::vector<bool> found(n_groups, false);
                    if (item.int_col != nullptr) {
                        std::vector<std::int64_t> acc(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = (*item.int_col)[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = data[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = (*item.str_col)[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].first_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.cat_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = std::string((*item.cat_col)[row]);
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].first_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] = data[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] = (*item.str_col)[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].last_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.cat_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] = std::string((*item.cat_col)[row]);
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].last_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value =
                                    acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].sum = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = counts[g];
                            }
                            break;
                        }
                        case ir::AggFunc::Min: {
                            std::vector<double> acc(n_groups,
                                                    std::numeric_limits<double>::infinity());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] < acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value =
                                    acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Max: {
                            std::vector<double> acc(n_groups,
                                                    -std::numeric_limits<double>::infinity());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] > acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value =
                                    acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].sum = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = counts[g];
                            }
                            break;
                        }
                        case ir::AggFunc::Min: {
                            std::vector<std::int64_t> acc(n_groups,
                                                          std::numeric_limits<std::int64_t>::max());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] < acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Max: {
                            std::vector<std::int64_t> acc(n_groups,
                                                          std::numeric_limits<std::int64_t>::min());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] > acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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

                if (item.func == ir::AggFunc::Count) {
                    std::vector<std::int64_t> acc(n_groups, 0);
                    for (std::size_t row = 0; row < rows; ++row)
                        acc[gids[row]]++;
                    for (std::uint32_t g = 0; g < n_groups; ++g)
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = acc[g];
                    continue;
                }

                if (item.func == ir::AggFunc::First) {
                    std::vector<bool> found(n_groups, false);
                    if (item.int_col != nullptr) {
                        std::vector<std::int64_t> acc(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = (*item.int_col)[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = data[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = (*item.str_col)[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].first_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.cat_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = std::string((*item.cat_col)[row]);
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].first_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = data[row];
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = (*item.str_col)[row];
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].last_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                        }
                    } else if (item.cat_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row)
                            acc[gids[row]] = std::string((*item.cat_col)[row]);
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].last_value =
                                std::move(acc[g]);
                            fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value =
                                    acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].sum = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = counts[g];
                            }
                            break;
                        }
                        case ir::AggFunc::Min: {
                            std::vector<double> acc(n_groups,
                                                    std::numeric_limits<double>::infinity());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] < acc[g])
                                    acc[g] = data[row];
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value =
                                    acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Max: {
                            std::vector<double> acc(n_groups,
                                                    -std::numeric_limits<double>::infinity());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] > acc[g])
                                    acc[g] = data[row];
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value =
                                    acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].sum = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = counts[g];
                            }
                            break;
                        }
                        case ir::AggFunc::Min: {
                            std::vector<std::int64_t> acc(n_groups,
                                                          std::numeric_limits<std::int64_t>::max());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] < acc[g])
                                    acc[g] = data[row];
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Max: {
                            std::vector<std::int64_t> acc(n_groups,
                                                          std::numeric_limits<std::int64_t>::min());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] > acc[g])
                                    acc[g] = data[row];
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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

        if (item.func == ir::AggFunc::Count) {
            std::vector<std::int64_t> acc(n_groups_m, 0);
            for (std::size_t row = 0; row < rows; ++row)
                acc[gids[row]]++;
            for (std::uint32_t g = 0; g < n_groups_m; ++g)
                fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = acc[g];
            continue;
        }

        if (item.func == ir::AggFunc::First) {
            std::vector<bool> found(n_groups_m, false);
            if (item.int_col != nullptr) {
                std::vector<std::int64_t> acc(n_groups_m, 0);
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (!found[g]) {
                        acc[g] = (*item.int_col)[row];
                        found[g] = true;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                }
            } else if (item.dbl_col != nullptr) {
                std::vector<double> acc(n_groups_m, 0.0);
                const double* data = item.dbl_col->data();
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (!found[g]) {
                        acc[g] = data[row];
                        found[g] = true;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                }
            } else if (item.str_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (!found[g]) {
                        acc[g] = (*item.str_col)[row];
                        found[g] = true;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].first_value =
                        std::move(acc[g]);
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                }
            } else if (item.cat_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row) {
                    std::uint32_t g = gids[row];
                    if (!found[g]) {
                        acc[g] = std::string((*item.cat_col)[row]);
                        found[g] = true;
                    }
                }
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].first_value =
                        std::move(acc[g]);
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                }
            } else if (item.dbl_col != nullptr) {
                std::vector<double> acc(n_groups_m, 0.0);
                const double* data = item.dbl_col->data();
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = data[row];
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                }
            } else if (item.str_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = (*item.str_col)[row];
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].last_value = std::move(acc[g]);
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                }
            } else if (item.cat_col != nullptr) {
                std::vector<std::string> acc(n_groups_m);
                for (std::size_t row = 0; row < rows; ++row)
                    acc[gids[row]] = std::string((*item.cat_col)[row]);
                for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].last_value = std::move(acc[g]);
                    fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].sum = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = counts[g];
                    }
                    break;
                }
                case ir::AggFunc::Min: {
                    std::vector<double> acc(n_groups_m, std::numeric_limits<double>::infinity());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (data[row] < acc[g])
                            acc[g] = data[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                    }
                    break;
                }
                case ir::AggFunc::Max: {
                    std::vector<double> acc(n_groups_m, -std::numeric_limits<double>::infinity());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (data[row] > acc[g])
                            acc[g] = data[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].double_value = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].sum = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].count = counts[g];
                    }
                    break;
                }
                case ir::AggFunc::Min: {
                    std::vector<std::int64_t> acc(n_groups_m,
                                                  std::numeric_limits<std::int64_t>::max());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (data[row] < acc[g])
                            acc[g] = data[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
                    }
                    break;
                }
                case ir::AggFunc::Max: {
                    std::vector<std::int64_t> acc(n_groups_m,
                                                  std::numeric_limits<std::int64_t>::min());
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint32_t g = gids[row];
                        if (data[row] > acc[g])
                            acc[g] = data[row];
                    }
                    for (std::uint32_t g = 0; g < n_groups_m; ++g) {
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].int_value = acc[g];
                        fs[static_cast<std::size_t>(g) * n_aggs + agg_i].has_value = true;
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
        if (call->callee == "rolling_mean") {
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
                        throw std::runtime_error("type mismatch");
                    }
                } else if constexpr (std::is_same_v<ValueType, double>) {
                    if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(static_cast<double>(*v));
                    } else if (const auto* v = std::get_if<double>(&value.value())) {
                        col.push_back(*v);
                    } else {
                        throw std::runtime_error("type mismatch");
                    }
                } else if constexpr (std::is_same_v<ValueType, std::string>) {
                    if (const auto* v = std::get_if<std::string>(&value.value())) {
                        col.push_back(*v);
                    } else {
                        throw std::runtime_error("type mismatch");
                    }
                } else if constexpr (std::is_same_v<ValueType, Date>) {
                    if (const auto* v = std::get_if<Date>(&value.value())) {
                        col.push_back(*v);
                    } else if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(int64_to_date_checked(*v));
                    } else {
                        throw std::runtime_error("type mismatch");
                    }
                } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                    if (const auto* v = std::get_if<Timestamp>(&value.value())) {
                        col.push_back(*v);
                    } else if (const auto* v = std::get_if<std::int64_t>(&value.value())) {
                        col.push_back(Timestamp{*v});
                    } else {
                        throw std::runtime_error("type mismatch");
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
                if (is_lag) {
                    if (n < rows)
                        std::memcpy(result.data() + n, col.data(), (rows - n) * sizeof(T));
                } else {
                    if (n < rows)
                        std::memcpy(result.data(), col.data() + n, (rows - n) * sizeof(T));
                }
            }
            return result;
        },
        *src);
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

    // Flatten the time column to int64 and express duration in the same unit.
    // Timestamp: nanoseconds.  Date: days.
    std::vector<std::int64_t> time_vals(rows);
    std::int64_t dur_val = 0;
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&time_col)) {
        for (std::size_t i = 0; i < rows; ++i)
            time_vals[i] = (*ts_col)[i].nanos;
        dur_val = duration.count();
    } else {
        const auto& date_col = std::get<Column<Date>>(time_col);
        for (std::size_t i = 0; i < rows; ++i)
            time_vals[i] = date_col[i].days;
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

constexpr auto is_rolling_func(std::string_view name) -> bool {
    return name == "rolling_sum" || name == "rolling_mean" || name == "rolling_min" ||
           name == "rolling_max" || name == "rolling_count";
}

// Like update_table but evaluates rolling aggregate expressions using the given window duration.
// Non-rolling fields are evaluated via evaluate_field_column (same as regular update_table).
auto windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                           ir::Duration duration, const ScalarRegistry* scalars,
                           const ExternRegistry* externs) -> std::expected<Table, std::string> {
    Table output = std::move(input);
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
        }
        auto inferred = infer_expr_type(field.expr, output, scalars, externs);
        if (!inferred) {
            return std::unexpected(inferred.error());
        }
        if (auto fast = try_fast_update_binary(field.expr, output, rows, inferred.value(), scalars);
            fast.has_value()) {
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
                            throw std::runtime_error("type mismatch");
                        }
                    } else if constexpr (std::is_same_v<ValueType, double>) {
                        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(static_cast<double>(*int_value));
                        } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                            col.push_back(*double_value);
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    } else if constexpr (std::is_same_v<ValueType, std::string>) {
                        if (const auto* v = std::get_if<std::string>(&value.value())) {
                            col.push_back(*v);
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    } else if constexpr (std::is_same_v<ValueType, Date>) {
                        if (const auto* v = std::get_if<Date>(&value.value())) {
                            col.push_back(*v);
                        } else if (const auto* int_value =
                                       std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(int64_to_date_checked(*int_value));
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                        if (const auto* v = std::get_if<Timestamp>(&value.value())) {
                            col.push_back(*v);
                        } else if (const auto* int_value =
                                       std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(Timestamp{*int_value});
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    }
                },
                new_column);
        }
        output.add_column(field.alias, std::move(new_column));
    }
    if (drop_ordering) {
        output.ordering.reset();
    }
    normalize_time_index(output);
    return output;
}

// NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
auto interpret_node(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string> {
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
            return update_table(std::move(child.value()), update.fields(), scalars, externs);
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
            if (!std::holds_alternative<Column<Timestamp>>(*col) &&
                !std::holds_alternative<Column<Date>>(*col)) {
                return std::unexpected("as_timeframe: column '" + atf.column() +
                                       "' must be Timestamp or Date");
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
            return join_table_impl(left.value(), right.value(), join.kind(), join.keys());
        }
    }
    return std::unexpected("unknown node kind");
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace

void Table::add_column(std::string name, ColumnValue column) {
    if (auto it = index.find(name); it != index.end()) {
        // Reseat the shared_ptr rather than mutating shared data (copy-on-write).
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        return;
    }
    std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{.name = std::move(name),
                                  .column = std::make_shared<ColumnValue>(std::move(column))});
    index[columns.back().name] = pos;
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
               const ExternRegistry* externs) -> std::expected<Table, std::string> {
    return interpret_node(node, registry, scalars, externs);
}

auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                 const std::vector<std::string>& keys) -> std::expected<Table, std::string> {
    return join_table_impl(left, right, kind, keys);
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
