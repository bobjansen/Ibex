#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <string>

extern "C" {

/// Arrow C Data Interface schema descriptor.
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    std::int64_t flags;
    std::int64_t n_children;
    ArrowSchema** children;
    ArrowSchema* dictionary;
    void (*release)(ArrowSchema*);
    void* private_data;
};

/// Arrow C Data Interface array descriptor.
struct ArrowArray {
    std::int64_t length;
    std::int64_t null_count;
    std::int64_t offset;
    std::int64_t n_buffers;
    std::int64_t n_children;
    const void** buffers;
    ArrowArray** children;
    ArrowArray* dictionary;
    void (*release)(ArrowArray*);
    void* private_data;
};

}  // extern "C"

namespace ibex::interop {

constexpr std::int64_t kArrowFlagDictionaryOrdered = 0x01;
constexpr std::int64_t kArrowFlagNullable = 0x02;
constexpr std::int64_t kArrowFlagMapKeysSorted = 0x04;

/// Release an Arrow schema previously exported by Ibex.
auto release_arrow_schema(ArrowSchema* schema) noexcept -> void;

/// Release an Arrow array previously exported by Ibex.
auto release_arrow_array(ArrowArray* array) noexcept -> void;

/// Export a Table as an Arrow struct array plus schema.
///
/// The `const Table&` overload keeps the export alive by taking an internal
/// shared snapshot of the table metadata and validity bitmaps. Column payloads
/// remain shared where possible.
[[nodiscard]] auto export_table_to_arrow(const runtime::Table& table, ArrowArray* out_array,
                                         ArrowSchema* out_schema)
    -> std::expected<void, std::string>;

/// Export a Table as an Arrow struct array plus schema, keeping the original
/// table alive via shared ownership for true zero-copy export.
[[nodiscard]] auto export_table_to_arrow(std::shared_ptr<const runtime::Table> table,
                                         ArrowArray* out_array, ArrowSchema* out_schema)
    -> std::expected<void, std::string>;

}  // namespace ibex::interop
