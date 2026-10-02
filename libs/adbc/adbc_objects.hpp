// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Reading the results of `AdbcConnectionGetObjects` (depth tables) and
// `AdbcConnectionGetInfo`: nested lists and a union, which Ibex's table
// importer does not take, walked here by hand.
//
// Header-only and free of any ADBC dependency so the walk, including arrays
// with offsets that the SQLite and PostgreSQL drivers never produce, can be
// tested in builds without a driver manager.

#pragma once

#include <ibex/interop/arrow_c_data.hpp>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <expected>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::adbc {

/// A borrowed Arrow array and its schema, for walking the nested result of
/// `AdbcConnectionGetObjects`, which Ibex's table importer does not take.
/// Row `i` of a view is element `offset + base + i` of its array; `base`
/// carries the offset of the struct the array is a field of.
class ArrowView {
   public:
    ArrowView(const ::ArrowArray& array, const ::ArrowSchema& schema, std::int64_t base,
              std::int64_t length)
        : array_(&array), schema_(&schema), base_(base), length_(length) {}

    [[nodiscard]] auto length() const noexcept -> std::int64_t { return length_; }

    [[nodiscard]] auto format() const -> std::string_view {
        return schema_->format != nullptr ? schema_->format : "";
    }

    /// The struct field `name`, checked to have one of `formats`.
    [[nodiscard]] auto field(std::string_view name,
                             std::initializer_list<std::string_view> formats) const
        -> std::expected<ArrowView, std::string> {
        for (std::int64_t i = 0; i < schema_->n_children && i < array_->n_children; ++i) {
            const ::ArrowSchema& child = *schema_->children[i];
            if (child.name == nullptr || child.name != name) {
                continue;
            }
            ArrowView view(*array_->children[i], child, array_->offset + base_, length_);
            if (std::ranges::find(formats, view.format()) == formats.end()) {
                return std::unexpected("field `" + std::string(name) + "` has unexpected type " +
                                       ibex::interop::describe_arrow_type(child));
            }
            return view;
        }
        return std::unexpected("field `" + std::string(name) + "` is missing");
    }

    /// The values of a list array, indexed by `list_range`.
    [[nodiscard]] auto list_values() const -> ArrowView {
        const ::ArrowArray& values = *array_->children[0];
        return {values, *schema_->children[0], 0, values.length};
    }

    [[nodiscard]] auto is_null(std::int64_t row) const -> bool {
        const auto* bits =
            array_->n_buffers > 0 ? static_cast<const std::uint8_t*>(array_->buffers[0]) : nullptr;
        if (bits == nullptr) {
            return false;
        }
        const std::int64_t at = position(row);
        return (bits[at / 8] & (1U << (at % 8))) == 0;
    }

    /// A `utf8` or `large_utf8` value, or nullopt for null.
    [[nodiscard]] auto text(std::int64_t row) const -> std::optional<std::string> {
        if (is_null(row)) {
            return std::nullopt;
        }
        const auto [begin, end] = offsets(row);
        const auto* data = static_cast<const char*>(array_->buffers[2]);
        return std::string(data + begin, static_cast<std::size_t>(end - begin));
    }

    /// A `uint32` value; the caller checks the format.
    [[nodiscard]] auto uint32_at(std::int64_t row) const -> std::uint32_t {
        return static_cast<const std::uint32_t*>(array_->buffers[1])[position(row)];
    }

    /// The `[begin, end)` range of `list_values()` a list row holds; empty
    /// for null.
    [[nodiscard]] auto list_range(std::int64_t row) const -> std::pair<std::int64_t, std::int64_t> {
        if (is_null(row)) {
            return {0, 0};
        }
        return offsets(row);
    }

   private:
    [[nodiscard]] auto position(std::int64_t row) const -> std::int64_t {
        return array_->offset + base_ + row;
    }

    /// Offsets `row` and `row + 1`: 32-bit for `utf8` and `list`, 64-bit for
    /// their large variants.
    [[nodiscard]] auto offsets(std::int64_t row) const -> std::pair<std::int64_t, std::int64_t> {
        const std::int64_t at = position(row);
        if (format() == "U" || format() == "+L") {
            const auto* values = static_cast<const std::int64_t*>(array_->buffers[1]);
            return {values[at], values[at + 1]};
        }
        const auto* values = static_cast<const std::int32_t*>(array_->buffers[1]);
        return {values[at], values[at + 1]};
    }

    const ::ArrowArray* array_;
    const ::ArrowSchema* schema_;
    std::int64_t base_;
    std::int64_t length_;
};

/// One table or view of a GetObjects result.
struct ObjectRow {
    std::optional<std::string> catalog;
    std::optional<std::string> db_schema;
    std::optional<std::string> table;
    std::optional<std::string> type;
};

/// Call `emit` with each table of one GetObjects batch, in order. The layout
/// is the one the ADBC specification fixes: catalog_name,
/// catalog_db_schemas: list<db_schema_name, db_schema_tables:
/// list<table_name, table_type, ...>>. A different layout is an error.
template <typename Emit>
auto read_object_rows(const ::ArrowArray& batch, const ::ArrowSchema& schema, Emit&& emit)
    -> std::expected<void, std::string> {
    const auto string_formats = {std::string_view("u"), std::string_view("U")};
    const auto list_formats = {std::string_view("+l"), std::string_view("+L")};
    const ArrowView root(batch, schema, 0, batch.length);
    auto catalog_name = root.field("catalog_name", string_formats);
    auto db_schemas = root.field("catalog_db_schemas", list_formats);
    if (!catalog_name || !db_schemas) {
        return std::unexpected(!catalog_name ? catalog_name.error() : db_schemas.error());
    }
    const ArrowView schema_rows = db_schemas->list_values();
    auto schema_name = schema_rows.field("db_schema_name", string_formats);
    auto tables = schema_rows.field("db_schema_tables", list_formats);
    if (!schema_name || !tables) {
        return std::unexpected(!schema_name ? schema_name.error() : tables.error());
    }
    const ArrowView table_rows = tables->list_values();
    auto table_name = table_rows.field("table_name", string_formats);
    auto table_type = table_rows.field("table_type", string_formats);
    if (!table_name || !table_type) {
        return std::unexpected(!table_name ? table_name.error() : table_type.error());
    }
    for (std::int64_t c = 0; c < root.length(); ++c) {
        const auto catalog = catalog_name->text(c);
        const auto [schema_begin, schema_end] = db_schemas->list_range(c);
        if (schema_begin < 0 || schema_end > schema_rows.length()) {
            return std::unexpected("a schema list is out of range");
        }
        for (std::int64_t d = schema_begin; d < schema_end; ++d) {
            const auto db_schema = schema_name->text(d);
            const auto [table_begin, table_end] = tables->list_range(d);
            if (table_begin < 0 || table_end > table_rows.length()) {
                return std::unexpected("a table list is out of range");
            }
            for (std::int64_t t = table_begin; t < table_end; ++t) {
                emit(ObjectRow{.catalog = catalog,
                               .db_schema = db_schema,
                               .table = table_name->text(t),
                               .type = table_type->text(t)});
            }
        }
    }
    return {};
}

/// Call `emit(code, text)` with each string entry of one GetInfo batch, in
/// order. The layout is the one the ADBC specification fixes: info_name:
/// uint32, info_value: dense_union<string_value: utf8, ...>. Entries of the
/// union's other kinds are skipped; a different layout is an error.
template <typename Emit>
auto read_info_strings(const ::ArrowArray& batch, const ::ArrowSchema& schema, Emit&& emit)
    -> std::expected<void, std::string> {
    const ArrowView root(batch, schema, 0, batch.length);
    auto names = root.field("info_name", {std::string_view("I")});
    if (!names) {
        return std::unexpected(names.error());
    }
    const ::ArrowArray* values = nullptr;
    const ::ArrowSchema* values_schema = nullptr;
    for (std::int64_t i = 0; i < schema.n_children && i < batch.n_children; ++i) {
        if (schema.children[i]->name != nullptr &&
            std::string_view(schema.children[i]->name) == "info_value") {
            values = batch.children[i];
            values_schema = schema.children[i];
        }
    }
    constexpr std::string_view kDenseUnion = "+ud:";
    if (values == nullptr) {
        return std::unexpected("field `info_value` is missing");
    }
    const std::string_view format = values_schema->format != nullptr ? values_schema->format : "";
    if (!format.starts_with(kDenseUnion) || values->n_buffers < 2) {
        return std::unexpected("field `info_value` has unexpected type " +
                               ibex::interop::describe_arrow_type(*values_schema));
    }
    // The format lists each child's type id: "+ud:0,1,2,...".
    std::vector<std::int8_t> type_ids;
    for (std::string_view rest = format.substr(kDenseUnion.size()); !rest.empty();) {
        const auto comma = rest.find(',');
        const auto id = rest.substr(0, comma);
        int value = 0;
        const auto [end, error] = std::from_chars(id.data(), id.data() + id.size(), value);
        if (error != std::errc{} || end != id.data() + id.size()) {
            return std::unexpected("field `info_value` has malformed union type ids");
        }
        type_ids.push_back(static_cast<std::int8_t>(value));
        rest = comma == std::string_view::npos ? std::string_view{} : rest.substr(comma + 1);
    }
    if (static_cast<std::int64_t>(type_ids.size()) != values_schema->n_children ||
        values->n_children != values_schema->n_children) {
        return std::unexpected("field `info_value` has mismatched union children");
    }
    std::optional<std::int8_t> string_id;
    std::optional<ArrowView> strings;
    for (std::size_t c = 0; c < type_ids.size(); ++c) {
        const ::ArrowSchema& child = *values_schema->children[c];
        if (child.name != nullptr && std::string_view(child.name) == "string_value") {
            const ArrowView view(*values->children[c], child, 0, values->children[c]->length);
            if (view.format() != "u") {
                return std::unexpected("field `string_value` has unexpected type " +
                                       ibex::interop::describe_arrow_type(child));
            }
            string_id = type_ids[c];
            strings = view;
        }
    }
    if (!strings.has_value()) {
        return std::unexpected("field `string_value` is missing");
    }
    // Union children are not offset by the union: its offsets index them.
    const auto* ids = static_cast<const std::int8_t*>(values->buffers[0]);
    const auto* offsets = static_cast<const std::int32_t*>(values->buffers[1]);
    for (std::int64_t row = 0; row < root.length(); ++row) {
        const std::int64_t at = values->offset + batch.offset + row;
        if (ids[at] != *string_id) {
            continue;
        }
        const std::int64_t index = offsets[at];
        if (index < 0 || index >= strings->length()) {
            return std::unexpected("a string value is out of range");
        }
        emit(names->uint32_at(row), strings->text(index));
    }
    return {};
}

}  // namespace ibex::adbc
