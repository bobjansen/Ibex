// aggregate.cpp — grouped and global aggregation: aggregate_table, the
// aggregate-call parser/spec builder, compound aggregate scalar evaluation,
// and the broadcast (update+by mixed aggregate) path.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <limits>
#include <optional>
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

// An all-default-payload column of the given type; pair it with an all-false
// ValidityBitmap to broadcast a null scalar (the payloads are never read).
auto default_column_for(ExprType type, std::size_t rows) -> ColumnValue {
    switch (type) {
        case ExprType::Int: {
            Column<std::int64_t> c;
            c.resize(rows);
            return ColumnValue{std::move(c)};
        }
        case ExprType::Double: {
            Column<double> c;
            c.resize(rows);
            return ColumnValue{std::move(c)};
        }
        case ExprType::Bool: {
            Column<bool> c;
            c.resize(rows);
            return ColumnValue{std::move(c)};
        }
        case ExprType::String: {
            Column<std::string> c;
            for (std::size_t i = 0; i < rows; ++i) {
                c.push_back(std::string_view{});
            }
            return ColumnValue{std::move(c)};
        }
        case ExprType::Date: {
            Column<Date> c;
            c.resize(rows);
            return ColumnValue{std::move(c)};
        }
        case ExprType::Timestamp: {
            Column<Timestamp> c;
            c.resize(rows);
            return ColumnValue{std::move(c)};
        }
    }
    Column<std::int64_t> c;  // unreachable; keeps MSVC C4715 quiet
    c.resize(rows);
    return ColumnValue{std::move(c)};
}

}  // namespace

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

    auto make_state = [&] -> AggState {
        AggState state;
        state.slots.reserve(aggregations.size());
        for (auto& i : plan) {
            AggSlot slot;
            slot.func = i.func;
            slot.kind = i.kind;
            slot.param = i.param;
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
                    ScalarValue value = scalar_from_column(column, row);
                    // append_agg_values_flat reads int_value/double_value for a
                    // numeric slot.kind (fast, no ScalarValue on the hot path
                    // elsewhere); keep this row-wise path's slot consistent with
                    // that contract instead of only writing first_value, which
                    // finalize would silently ignore for numeric columns.
                    if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                        slot.int_value = *iv;
                    } else if (const auto* dv = std::get_if<double>(&value)) {
                        slot.double_value = *dv;
                    } else {
                        slot.first_value = std::move(value);
                    }
                }
                slot.has_value = true;
                continue;
            }
            if (agg.func == ir::AggFunc::Last) {
                ScalarValue value = scalar_from_column(column, row);
                if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                    slot.int_value = *iv;
                } else if (const auto* dv = std::get_if<double>(&value)) {
                    slot.double_value = *dv;
                } else {
                    slot.last_value = std::move(value);
                }
                slot.has_value = true;
                continue;
            }
            if (agg.func == ir::AggFunc::Median || agg.func == ir::AggFunc::Quantile ||
                agg.func == ir::AggFunc::Skew || agg.func == ir::AggFunc::Kurtosis) {
                // Collected contiguously after grouping (reserve-then-fill from the
                // value column directly), not per row here — see the collect pass
                // below. Per-row push_back into one growing vector per group was the
                // dominant cost of median/quantile by-group.
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
                const double delta = x - slot.double_value;
                slot.double_value += delta / static_cast<double>(slot.count);
                const double delta2 = x - slot.double_value;
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

    auto build_output = [&] -> std::expected<Table, std::string> {
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

    // Non-const slots: Median/Quantile select in place (std::nth_element) on
    // slot.values rather than copying + fully sorting it. Every caller passes a
    // mutable, about-to-be-discarded slot array, so the in-place reorder is safe.
    auto append_agg_values_flat = [&](Table& output, AggSlot* slots) -> std::optional<std::string> {
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto& agg = aggregations[i];
            auto* column = output.find(agg.alias);
            if (column == nullptr) {
                return "missing aggregate column in output";
            }
            AggSlot& slot = slots[i];
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
                // Median/Quantile/Skew/Kurtosis are reduced in the contiguous
                // collect pass (above); the scalar result is already in
                // slot.double_value, so just emit it.
                case ir::AggFunc::Median:
                    append_scalar(*column, slot.double_value);
                    break;
                case ir::AggFunc::Stddev:
                    if (slot.count < 2) {
                        append_scalar(*column, 0.0);
                    } else {
                        append_scalar(*column,
                                      std::sqrt(slot.m2 / static_cast<double>(slot.count - 1)));
                    }
                    break;
                case ir::AggFunc::Ewma:
                case ir::AggFunc::Quantile:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    append_scalar(*column, slot.double_value);
                    break;
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
            // Median/Quantile/Skew/Kurtosis are reduced in the contiguous collect
            // pass, which records the group's value count in slot.count.
            case ir::AggFunc::Median:
            case ir::AggFunc::Quantile:
                return slot.count > 0;
            case ir::AggFunc::Stddev:
                return slot.count >= 2;
            case ir::AggFunc::Ewma:
                return slot.has_value;
            case ir::AggFunc::Skew:
                return slot.count >= 3;
            case ir::AggFunc::Kurtosis:
                return slot.count >= 4;
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

        // Collect-aggregates (median/quantile/skew/kurtosis) gather every value
        // into a per-group buffer. Doing that per row (push_back into one growing
        // vector per group, via scalar_from_column) dominates their cost. Instead
        // record each row's group id during grouping and fill the buffers in a
        // contiguous reserve-then-scatter pass below, reading the value column
        // directly. Only allocate row_gid when a collect-aggregate is present.
        std::vector<std::size_t> collect_aggs;
        for (std::size_t i = 0; i < plan.size(); ++i) {
            if (plan[i].func == ir::AggFunc::Median || plan[i].func == ir::AggFunc::Quantile ||
                plan[i].func == ir::AggFunc::Skew || plan[i].func == ir::AggFunc::Kurtosis) {
                collect_aggs.push_back(i);
            }
        }
        std::vector<std::uint32_t> row_gid;
        if (!collect_aggs.empty()) {
            row_gid.resize(rows);
        }

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
            if (!collect_aggs.empty()) {
                row_gid[row] = static_cast<std::uint32_t>(it->second);
            }

            if (auto err = update_state(states[it->second].slots.data(), row)) {
                return std::unexpected(*err);
            }
        }

        // Contiguous collect pass: per aggregate, histogram group sizes into a
        // prefix-sum offset table, scatter the value column into ONE flat buffer
        // by group id (buf[cursor[g]++] = v — no per-group vector, no pointer
        // chase), then reduce each contiguous group slice in place. The result
        // lands in slot.double_value and the group size in slot.count, so the
        // finalize below and validity just read those.
        if (!collect_aggs.empty() && rows > 0) {
            const std::size_t n_groups = states.size();
            std::vector<std::size_t> offsets(n_groups + 1);
            std::vector<std::size_t> cursor(n_groups);
            std::vector<double> buf;
            for (const std::size_t ai : collect_aggs) {
                const ColumnValue& col = *agg_columns[ai];
                const ValidityBitmap* vb = agg_validity[ai];
                std::ranges::fill(offsets, std::size_t{0});
                for (std::size_t row = 0; row < rows; ++row) {
                    if (vb == nullptr || (*vb)[row]) {
                        ++offsets[row_gid[row] + 1];
                    }
                }
                for (std::size_t g = 0; g < n_groups; ++g) {
                    offsets[g + 1] += offsets[g];
                }
                buf.resize(offsets[n_groups]);
                std::copy(offsets.begin(), offsets.end() - 1, cursor.begin());
                auto scatter = [&](const auto& typed) {
                    const auto* data = typed.data();
                    for (std::size_t row = 0; row < rows; ++row) {
                        if (vb == nullptr || (*vb)[row]) {
                            buf[cursor[row_gid[row]]++] = static_cast<double>(data[row]);
                        }
                    }
                };
                if (const auto* dc = std::get_if<Column<double>>(&col)) {
                    scatter(*dc);
                } else if (const auto* ic = std::get_if<Column<std::int64_t>>(&col)) {
                    scatter(*ic);
                }

                const ir::AggFunc f = plan[ai].func;
                const double p = plan[ai].param;
                for (std::size_t g = 0; g < n_groups; ++g) {
                    double* lo = buf.data() + offsets[g];
                    double* hi = buf.data() + offsets[g + 1];
                    const auto n = static_cast<std::size_t>(hi - lo);
                    AggSlot& slot = states[g].slots[ai];
                    slot.count = static_cast<std::int64_t>(n);
                    double result = 0.0;
                    if (f == ir::AggFunc::Median) {
                        if (n > 0) {
                            const std::size_t mid = n / 2;
                            std::nth_element(lo, lo + static_cast<std::ptrdiff_t>(mid), hi);
                            const double upper = lo[mid];
                            result = (n % 2 == 0)
                                         ? (*std::max_element(
                                                lo, lo + static_cast<std::ptrdiff_t>(mid)) +
                                            upper) /
                                               2.0
                                         : upper;
                        }
                    } else if (f == ir::AggFunc::Quantile) {
                        if (n > 0) {
                            const double idx = p * static_cast<double>(n - 1);
                            const auto qlo = static_cast<std::size_t>(idx);
                            const double frac = idx - static_cast<double>(qlo);
                            std::nth_element(lo, lo + static_cast<std::ptrdiff_t>(qlo), hi);
                            const double vlo = lo[qlo];
                            const double vhi =
                                (qlo + 1 < n) ? *std::min_element(
                                                    lo + static_cast<std::ptrdiff_t>(qlo + 1), hi)
                                              : vlo;
                            result = vlo + (frac * (vhi - vlo));
                        }
                    } else if (f == ir::AggFunc::Skew) {
                        if (n >= 3) {
                            double mean = 0.0;
                            for (const double* it = lo; it != hi; ++it)
                                mean += *it;
                            mean /= static_cast<double>(n);
                            double m2 = 0.0;
                            double m3 = 0.0;
                            for (const double* it = lo; it != hi; ++it) {
                                const double d = *it - mean;
                                m2 += d * d;
                                m3 += d * d * d;
                            }
                            if (m2 != 0.0) {
                                const auto dn = static_cast<double>(n);
                                result = (dn * std::sqrt(dn - 1.0) / (dn - 2.0)) *
                                         (m3 / std::pow(m2, 1.5));
                            }
                        }
                    } else {  // Kurtosis
                        if (n >= 4) {
                            double mean = 0.0;
                            for (const double* it = lo; it != hi; ++it)
                                mean += *it;
                            mean /= static_cast<double>(n);
                            double m2 = 0.0;
                            double m4 = 0.0;
                            for (const double* it = lo; it != hi; ++it) {
                                const double d = *it - mean;
                                const double d2 = d * d;
                                m2 += d2;
                                m4 += d2 * d2;
                            }
                            if (m2 != 0.0) {
                                const auto dn = static_cast<double>(n);
                                result = (dn - 1.0) / ((dn - 2.0) * (dn - 3.0)) *
                                         (((dn + 1.0) * dn * m4 / (m2 * m2)) - (3.0 * (dn - 1.0)));
                            }
                        }
                    }
                    slot.double_value = result;
                }
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

            AggSlot* slots = states[g].slots.data();
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

            // Pass 1: Assign group IDs. Categorical codes are dense dictionary indexes,
            // so use a direct code -> gid table instead of hashing each distinct code.
            constexpr std::uint32_t no_gid = std::numeric_limits<std::uint32_t>::max();
            std::vector<std::uint32_t> code_to_id(col.dictionary().size(), no_gid);
            std::vector<Column<Categorical>::code_type> order;
            order.reserve(std::min<std::size_t>(rows, col.dictionary().size()));
            // Flat AggSlot array: one contiguous allocation replaces n_groups heap allocations.
            // Layout: flat_slots[g * n_aggs + agg_i] is the slot for group g, aggregation agg_i.
            const std::size_t n_aggs = plan.size();
            AggState tmpl = make_state();
            std::vector<AggSlot> flat_slots;
            flat_slots.reserve(order.capacity() * (n_aggs == 0 ? 1 : n_aggs));
            std::vector<std::uint32_t> group_ids(rows);
            {
                Column<Categorical>::code_type prev_key = -1;
                std::uint32_t prev_gid = no_gid;
                for (std::size_t row = 0; row < rows; ++row) {
                    auto key = codes[row];
                    std::uint32_t gid{};
                    if (key == prev_key) {
                        gid = prev_gid;
                    } else {
                        if (key < 0 || static_cast<std::size_t>(key) >= code_to_id.size()) {
                            return std::unexpected("invalid categorical code in group-by column: " +
                                                   group_by.front().name);
                        }
                        gid = code_to_id[static_cast<std::size_t>(key)];
                        if (gid == no_gid) {
                            gid = static_cast<std::uint32_t>(order.size());
                            code_to_id[static_cast<std::size_t>(key)] = gid;
                            order.push_back(key);
                            flat_slots.insert(flat_slots.end(), tmpl.slots.begin(),
                                              tmpl.slots.end());
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
                for (const int i : order) {
                    auto* column = output->find(group_by.front().name);
                    if (column == nullptr) {
                        return std::unexpected("missing group-by column in output");
                    }
                    append_scalar(*column,
                                  std::string(col.dictionary()[static_cast<std::size_t>(i)]));
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
                const double key = col[row];
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
                    const std::string_view key{src_chars + src_off[row],
                                               src_off[row + 1] - src_off[row]};
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
                const std::string_view key_sv{dict_chars.data() + dict_offsets[g],
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
                using T = ColType::value_type;
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
                        const std::string_view key{col[row]};
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

/// Reduce a single aggregate call (e.g. `mean(price)`, `sum(p*w)`) over the
/// per-group `input` to one scalar. Shared by the scalar-collapse path
/// (`eval_aggregate_scalar`) and the mixed per-row/aggregate path
/// (`fold_aggregates_to_columns`). Returns Null when the aggregate has no
/// value (an all-null group) — read from the result's validity, never from
/// the undefined payload.
auto eval_aggregate_call_scalar(const ir::CallExpr& node, const Table& input,
                                const ScalarRegistry* scalars)
    -> std::expected<ExprValue, std::string> {
    auto func = parse_aggregate_func(node.callee);
    if (!func.has_value()) {
        return std::unexpected("update + by: non-aggregate function '" + node.callee +
                               "' in aggregate-broadcast field expression");
    }
    // count() takes no arguments.
    if (node.callee == "count") {
        ir::AggSpec spec{
            .func = *func,
            .column = ir::ColumnRef{.name = ""},
            .alias = "__agg_broadcast",
        };
        auto agg = aggregate_table(input, {}, std::vector<ir::AggSpec>{std::move(spec)});
        if (!agg) {
            return std::unexpected(agg.error());
        }
        const auto* entry = agg->find_entry("__agg_broadcast");
        return expr_from_scalar(scalar_from_column(*entry->column, 0));
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
            return std::unexpected(node.callee + "(): second argument must be a numeric literal");
        }
        if (const auto* dv = std::get_if<double>(&lit->value)) {
            param = *dv;
        } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
            param = static_cast<double>(*iv);
        } else {
            return std::unexpected(node.callee + "(): second argument must be a numeric literal");
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
            agg_col_name += '_';
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
        // Carry the computed argument's validity — the null-aware aggregate
        // kernels must skip its null cells, not accumulate their payloads.
        if (const auto* v = col_result->get_validity()) {
            working.add_column(agg_col_name, std::move(materialised), *v);
        } else {
            working.add_column(agg_col_name, std::move(materialised));
        }
        effective_input = &working;
    }
    ir::AggSpec spec{
        .func = *func,
        .column = ir::ColumnRef{.name = agg_col_name},
        .alias = "__agg_broadcast",
        .param = param,
    };
    auto agg = aggregate_table(*effective_input, {}, std::vector<ir::AggSpec>{std::move(spec)});
    if (!agg) {
        return std::unexpected(agg.error());
    }
    const auto* entry = agg->find_entry("__agg_broadcast");
    if (entry == nullptr || entry->column == nullptr || column_size(*entry->column) != 1) {
        return std::unexpected("update + by: internal error computing aggregate broadcast");
    }
    if (entry->validity.has_value() && !(*entry->validity)[0]) {
        return ExprValue{Null{}};  // no valid observations in the group
    }
    return expr_from_scalar(scalar_from_column(*entry->column, 0));
}

/// Evaluate a compound aggregate expression to a single scalar. Each
/// aggregate sub-call is computed against the (per-group) `input`; other
/// nodes compose via the existing column-arithmetic helpers on 1-row
/// columns so int/double promotion matches the column path. Bare
/// ColumnRefs resolve only against `scalars` — a column reference outside
/// an aggregate would not collapse to a per-group scalar.
auto eval_aggregate_scalar(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::expected<ExprValue, std::string> {
    return std::visit(
        [&](const auto& node) -> std::expected<ExprValue, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::Literal>) {
                return std::visit([](const auto& v) -> ExprValue { return v; }, node.value);
            } else if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                if (scalars != nullptr) {
                    auto it = scalars->find(node.name);
                    if (it != scalars->end()) {
                        return expr_from_scalar(it->second);
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
                // Null propagates through the scalar arithmetic, same as the
                // per-row evaluator: a null aggregate nulls the compound.
                if (std::holds_alternative<Null>(*lhs) || std::holds_alternative<Null>(*rhs)) {
                    return ExprValue{Null{}};
                }
                const ColumnValue lhs_col = broadcast_scalar_column(*scalar_from_expr(*lhs), 1);
                const ColumnValue rhs_col = broadcast_scalar_column(*scalar_from_expr(*rhs), 1);
                auto result = arith_vec(node.op, lhs_col, rhs_col, 1);
                if (!result) {
                    return std::unexpected(result.error());
                }
                return expr_from_scalar(scalar_from_column(*result, 0));
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                return eval_aggregate_call_scalar(node, input, scalars);
            } else {
                return std::unexpected(
                    "update + by: unsupported expression shape in aggregate-broadcast field");
            }
        },
        expr.node);
}

/// True iff `expr` has a column reference that is *not* enclosed in an
/// aggregate call. In an `update + by` field this distinguishes a per-row
/// result (`price - mean(price)` — `price` is bare) from one that collapses to
/// a single per-group scalar (`sum(p*w) / sum(w)` — every column sits inside an
/// aggregate). Aggregate arguments are deliberately skipped: a column inside
/// `mean(...)` is consumed by the reduction, not read row-wise.
auto expr_has_bare_column(const ir::Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                return true;
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (parse_aggregate_func(node.callee).has_value()) {
                    return false;  // columns inside an aggregate are not bare
                }
                return std::ranges::any_of(
                    node.args, [](const auto& arg) { return expr_has_bare_column(*arg); });
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                 std::is_same_v<T, ir::CompareExpr>) {
                return expr_has_bare_column(*node.left) || expr_has_bare_column(*node.right);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                return expr_has_bare_column(*node.left) ||
                       (node.right && expr_has_bare_column(*node.right));
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return expr_has_bare_column(*node.operand);
            } else {
                return false;
            }
        },
        expr.node);
}

/// Rewrite `expr` in place for the mixed per-row/aggregate case: each aggregate
/// sub-call is reduced over the per-group `group_input` to a scalar, broadcast
/// into a fresh column appended to `working`, and the call node is replaced by a
/// reference to that column. The rewritten expression then evaluates row-wise
/// via `eval_value_vec`, so `price - mean(price)` becomes `price - __agg_bcast_0`
/// with `__agg_bcast_0` a constant column holding the group mean.
auto fold_aggregates_to_columns(ir::Expr& expr, const Table& group_input, Table& working,
                                const ScalarRegistry* scalars, int& counter)
    -> std::expected<void, std::string> {
    return std::visit(
        [&](auto& node) -> std::expected<void, std::string> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::CallExpr>) {
                if (parse_aggregate_func(node.callee).has_value()) {
                    auto scalar = eval_aggregate_call_scalar(node, group_input, scalars);
                    if (!scalar) {
                        return std::unexpected(scalar.error());
                    }
                    std::string name = "__agg_bcast_" + std::to_string(counter++);
                    while (working.find(name) != nullptr) {
                        name += '_';
                    }
                    if (std::holds_alternative<Null>(*scalar)) {
                        // A null aggregate (all-null group) broadcasts as an
                        // all-invalid column of the call's inferred type.
                        auto ty = infer_expr_type(expr, group_input, scalars, nullptr);
                        if (!ty) {
                            return std::unexpected(ty.error());
                        }
                        working.add_column(name, default_column_for(*ty, working.rows()),
                                           ValidityBitmap(working.rows(), false));
                    } else {
                        working.add_column(name, broadcast_scalar_column(*scalar_from_expr(*scalar),
                                                                         working.rows()));
                    }
                    expr.node = ir::ColumnRef{.name = std::move(name)};
                    return {};
                }
                for (auto& arg : node.args) {
                    if (auto r = fold_aggregates_to_columns(*arg, group_input, working, scalars,
                                                            counter);
                        !r) {
                        return r;
                    }
                }
                return {};
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                 std::is_same_v<T, ir::CompareExpr>) {
                if (auto r = fold_aggregates_to_columns(*node.left, group_input, working, scalars,
                                                        counter);
                    !r) {
                    return r;
                }
                return fold_aggregates_to_columns(*node.right, group_input, working, scalars,
                                                  counter);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                if (auto r = fold_aggregates_to_columns(*node.left, group_input, working, scalars,
                                                        counter);
                    !r) {
                    return r;
                }
                if (node.right) {
                    return fold_aggregates_to_columns(*node.right, group_input, working, scalars,
                                                      counter);
                }
                return {};
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return fold_aggregates_to_columns(*node.operand, group_input, working, scalars,
                                                  counter);
            } else {
                return {};  // Literal, ColumnRef, RankExpr — left as-is
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
    // No aggregate at all: a pure per-row field, handled by `update_table`.
    if (!expr_contains_aggregate_call(field.expr)) {
        return std::optional<BroadcastAggregateColumn>{};
    }
    // Mixed path: the expression both reduces (an aggregate call) and reads a
    // column row-wise (e.g. `price - mean(price)`, the demean/z-score kernel).
    // Fold each aggregate into a per-group broadcast column, then evaluate the
    // rewritten expression row-wise so the result is a full per-group column.
    if (expr_has_bare_column(field.expr)) {
        Table working = input;            // shallow: columns are shared_ptr-backed
        ir::Expr rewritten = field.expr;  // deep copy (ExprPtr has value semantics)
        int counter = 0;
        if (auto folded = fold_aggregates_to_columns(rewritten, input, working, scalars, counter);
            !folded) {
            return std::unexpected(folded.error());
        }
        auto col = eval_value_vec(rewritten, working, scalars, input.rows());
        if (!col) {
            return std::unexpected(col.error());
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
            col->data);
        BroadcastAggregateColumn result{
            .column = std::move(materialised),
            .validity = std::nullopt,
        };
        if (const auto* v = col->get_validity()) {
            result.validity = *v;
        }
        return std::optional<BroadcastAggregateColumn>{std::move(result)};
    }
    // Compound scalar path: every column sits inside an aggregate (e.g.
    // `sum(p*w) / sum(w)`, or any aggregate-UDF body inlined to such a shape).
    // Evaluate to a single scalar, then broadcast over the group's rows.
    auto scalar = eval_aggregate_scalar(field.expr, input, scalars);
    if (!scalar) {
        return std::unexpected(scalar.error());
    }
    if (std::holds_alternative<Null>(*scalar)) {
        // A null compound aggregate broadcasts as an all-invalid column of
        // the expression's inferred type.
        auto ty = infer_expr_type(field.expr, input, scalars, nullptr);
        if (!ty) {
            return std::unexpected(ty.error());
        }
        BroadcastAggregateColumn result{
            .column = default_column_for(*ty, input.rows()),
            .validity = ValidityBitmap(input.rows(), false),
        };
        return std::optional<BroadcastAggregateColumn>{std::move(result)};
    }
    BroadcastAggregateColumn result{
        .column = broadcast_scalar_column(*scalar_from_expr(*scalar), input.rows()),
        .validity = std::nullopt,
    };
    return std::optional<BroadcastAggregateColumn>{std::move(result)};
}

}  // namespace ibex::runtime
