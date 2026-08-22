// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_filter.hpp"

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"

namespace ibex::runtime::kernel {

namespace {

auto chunk_from_filtered(std::expected<Table, std::string> filtered, std::uint64_t sequence,
                         std::size_t row_offset) -> std::expected<Chunk, std::string> {
    if (!filtered.has_value()) {
        return std::unexpected(std::move(filtered.error()));
    }
    Chunk output = table_to_chunk(std::move(filtered.value()));
    output.sequence = sequence;
    output.row_offset = row_offset;
    return output;
}

}  // namespace

auto filter_chunk(Chunk input, const ir::Expr& predicate, const ScalarRegistry* scalars)
    -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    return chunk_from_filtered(filter_table(chunk_to_table(std::move(input)), predicate, scalars),
                               sequence, row_offset);
}

auto filter_project_chunk(Chunk input, const ir::Expr& predicate,
                          const std::vector<ir::ColumnRef>& columns, const ScalarRegistry* scalars)
    -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    return chunk_from_filtered(
        filter_project_table(chunk_to_table(std::move(input)), predicate, columns, scalars),
        sequence, row_offset);
}

auto filter_limit_chunk(Chunk input, const ir::Expr& predicate, std::size_t row_limit,
                        const ScalarRegistry* scalars) -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    return chunk_from_filtered(
        filter_table_limit(chunk_to_table(std::move(input)), predicate, row_limit, scalars),
        sequence, row_offset);
}

auto project_chunk(Chunk input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Chunk, std::string> {
    const ChunkView view(input);
    std::vector<MappedChunkColumn> map;
    map.reserve(columns.size());
    for (const auto& column : columns) {
        if (column.name.empty()) {
            if (const auto& time_index = view.properties().time_index();
                time_index.has_value() && std::ranges::none_of(map, [&](const auto& mapped) {
                    return mapped.name == *time_index;
                })) {
                const auto position = view.find_column(*time_index);
                if (!position.has_value())
                    return std::unexpected("select column not found: " + *time_index);
                map.push_back({.source_position = *position, .name = *time_index});
            }
            continue;
        }
        const auto position = view.find_column(column.name);
        if (!position.has_value())
            return std::unexpected("select column not found: " + column.name);
        map.push_back({.source_position = *position, .name = column.name});
    }
    const auto properties = TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return std::ranges::any_of(map, [&](const auto& mapped) { return mapped.name == name; })
                       ? KeyFate::kept(name)
                       : KeyFate::dropped();
        },
        RowTransform::Preserve);
    return map_chunk(view, map, properties);
}

auto filter_update_project_chunk(Chunk input, const ir::Expr& predicate,
                                 const std::vector<ir::FieldSpec>& fields,
                                 const std::vector<ir::ColumnRef>& project_columns,
                                 const std::vector<ir::ColumnRef>& gather_columns,
                                 const ScalarRegistry* scalars, const ExternRegistry* externs,
                                 const ExecutionContext& exec)
    -> std::expected<Chunk, std::string> {
    auto filtered = filter_project_chunk(std::move(input), predicate, gather_columns, scalars);
    if (!filtered.has_value())
        return std::unexpected(std::move(filtered.error()));
    auto updated =
        update_row_local_chunk(std::move(filtered.value()), fields, scalars, externs, exec);
    if (!updated.has_value())
        return std::unexpected(std::move(updated.error()));
    return project_chunk(std::move(updated.value()), project_columns);
}

}  // namespace ibex::runtime::kernel
