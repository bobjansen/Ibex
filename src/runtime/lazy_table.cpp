#include <ibex/ir/expr_predicates.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <cstddef>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "runtime_internal.hpp"

namespace ibex::runtime {

LazyTable::LazyTable(Table schema, std::size_t rows, ColumnDecodeFn decode)
    : schema_(std::move(schema)), rows_(rows), decode_(std::move(decode)) {}

auto LazyTable::project(const std::set<std::string>& names) -> std::expected<Table, std::string> {
    std::vector<std::string> missing;
    for (const auto& entry : schema_.columns) {
        if (names.contains(entry.name) && !cache_.contains(entry.name)) {
            missing.push_back(entry.name);
        }
    }

    if (!missing.empty()) {
        auto decoded = decode_(missing, nullptr);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        for (auto& entry : decoded->columns) {
            auto name = entry.name;
            cache_.insert_or_assign(std::move(name), std::move(entry));
        }
        for (const auto& name : missing) {
            if (!cache_.contains(name)) {
                return std::unexpected("lazy source did not produce requested column '" + name +
                                       "'");
            }
        }
    }

    // Emit in schema order, so a projected table's column order matches the
    // source's regardless of the order columns happened to be decoded in.
    Table out;
    for (const auto& field : schema_.columns) {
        if (!names.contains(field.name)) {
            continue;
        }
        const auto& entry = cache_.at(field.name);
        out.add_column_shared(entry.name, entry.column, entry.validity);
    }
    // A plan may need the row count without needing any column — `count()` over
    // an unfiltered scan, say. Carry it so such a projection stays empty rather
    // than forcing a column to be decoded for its length alone.
    out.logical_rows = rows_;
    return out;
}

auto LazyTable::project_where(const std::set<std::string>& names,
                              const std::vector<ir::Expr>& conjuncts, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string> {
    if (conjuncts.empty()) {
        return project(names);
    }

    robin_hood::unordered_set<std::string> referenced;
    for (const auto& conjunct : conjuncts) {
        ir::collect_expr_column_refs(conjunct, referenced);
    }

    std::vector<std::string> predicate_names;
    predicate_names.reserve(referenced.size());
    for (const auto& field : schema_.columns) {
        if (referenced.contains(field.name)) {
            predicate_names.push_back(field.name);
        }
    }

    Table predicates;
    if (!predicate_names.empty()) {
        auto decoded = decode_(predicate_names, nullptr);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        predicates = std::move(*decoded);
        for (const auto& name : predicate_names) {
            if (predicates.find_entry(name) == nullptr) {
                return std::unexpected("lazy source did not produce predicate column '" + name +
                                       "'");
            }
        }
    }
    predicates.logical_rows = rows_;
    if (!predicates.columns.empty() && predicates.rows() != rows_) {
        return std::unexpected("lazy source produced predicate columns with the wrong row count");
    }

    auto selected = filter_selection(predicates, conjuncts, scalars);
    if (!selected) {
        return std::unexpected(selected.error());
    }
    const bool all_rows = selected->size() == rows_;

    std::vector<std::string> remaining;
    for (const auto& field : schema_.columns) {
        if (names.contains(field.name) && !referenced.contains(field.name)) {
            remaining.push_back(field.name);
        }
    }

    Table decoded_remaining;
    if (!remaining.empty()) {
        auto decoded = decode_(remaining, all_rows ? nullptr : &*selected);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        decoded_remaining = std::move(*decoded);
        for (const auto& name : remaining) {
            if (decoded_remaining.find_entry(name) == nullptr) {
                return std::unexpected("lazy source did not produce requested column '" + name +
                                       "'");
            }
        }
        if (decoded_remaining.rows() != selected->size()) {
            return std::unexpected(
                "lazy source produced selected columns with the wrong row count");
        }
    }

    robin_hood::unordered_map<std::string, ColumnEntry> selected_columns;
    selected_columns.reserve(names.size());
    for (const auto& entry : predicates.columns) {
        if (!names.contains(entry.name)) {
            continue;
        }
        ColumnEntry gathered;
        gathered.name = entry.name;
        if (all_rows) {
            gathered.column = entry.column;
            gathered.validity = entry.validity;
        } else {
            gathered.column = std::make_shared<ColumnValue>(
                gather_column(*entry.column, selected->data(), selected->size()));
            if (entry.validity.has_value()) {
                ValidityBitmap validity(selected->size(), true);
                for (std::size_t row = 0; row < selected->size(); ++row) {
                    validity.set(row, (*entry.validity)[(*selected)[row]]);
                }
                gathered.validity = std::move(validity);
            }
        }
        selected_columns.insert_or_assign(gathered.name, std::move(gathered));
    }
    for (auto& entry : decoded_remaining.columns) {
        auto name = entry.name;
        selected_columns.insert_or_assign(std::move(name), std::move(entry));
    }

    Table out;
    for (const auto& field : schema_.columns) {
        if (!names.contains(field.name)) {
            continue;
        }
        auto it = selected_columns.find(field.name);
        if (it == selected_columns.end()) {
            return std::unexpected("lazy source did not produce requested column '" + field.name +
                                   "'");
        }
        out.add_column_shared(it->second.name, it->second.column, it->second.validity);
    }
    out.logical_rows = selected->size();
    return out;
}

auto LazyTable::materialize() -> std::expected<Table, std::string> {
    std::set<std::string> names;
    for (const auto& entry : schema_.columns) {
        names.insert(entry.name);
    }
    return project(names);
}

}  // namespace ibex::runtime
