#include <ibex/runtime/lazy_table.hpp>

#include <cstddef>
#include <set>
#include <string>
#include <utility>
#include <vector>

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
        auto decoded = decode_(missing);
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

auto LazyTable::materialize() -> std::expected<Table, std::string> {
    std::set<std::string> names;
    for (const auto& entry : schema_.columns) {
        names.insert(entry.name);
    }
    return project(names);
}

}  // namespace ibex::runtime
