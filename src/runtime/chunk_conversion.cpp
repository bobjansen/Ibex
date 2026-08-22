// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "chunk_conversion_internal.hpp"

namespace ibex::runtime {

auto chunk_to_table(Chunk chunk) -> Table {
    Table table;
    table.columns = std::move(chunk.columns);
    for (std::size_t index = 0; index < table.columns.size(); ++index) {
        table.index[table.columns[index].name] = index;
    }
    table.set_properties(chunk.properties());
    if (table.columns.empty()) {
        table.logical_rows = chunk.logical_rows;
    }
    return table;
}

auto table_to_chunk(Table table) -> Chunk {
    Chunk chunk;
    chunk.columns = std::move(table.columns);
    chunk.set_properties(table.properties());
    if (chunk.columns.empty()) {
        chunk.logical_rows = table.logical_rows;
    }
    return chunk;
}

}  // namespace ibex::runtime
