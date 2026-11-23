// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

struct ArrowSchema;

namespace ibex::runtime {
class ExternRegistry;
struct Table;
}  // namespace ibex::runtime

namespace ibex::parquet {

/// Register Ibex's first-party Parquet reader, lazy source, chunked source,
/// and writer with a host registry. Registration is idempotent.
void register_backend(runtime::ExternRegistry& registry);

class ChunkWriter {
   public:
    virtual ~ChunkWriter() = default;
    virtual void push(runtime::Table table) = 0;
    virtual auto finish() -> std::int64_t = 0;
};

/// An optional Arrow C schema supplies the declared output names and types.
auto open_writer(std::string_view path, std::string_view compression, std::int64_t level,
                 std::int64_t row_group_size, bool statistics, std::int64_t page_size,
                 bool create_parents, ArrowSchema* schema = nullptr)
    -> std::unique_ptr<ChunkWriter>;

}  // namespace ibex::parquet
