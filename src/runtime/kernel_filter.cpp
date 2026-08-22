// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_filter.hpp"

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"

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

}  // namespace ibex::runtime::kernel
