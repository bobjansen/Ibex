// Arrow C Data Interface requires C-style arrays for ABI compatibility.
// NOLINTBEGIN(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
#include <ibex/interop/arrow_c_data.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ibex::interop {

namespace {

static_assert(std::endian::native == std::endian::little,
              "Arrow C Data export currently requires little-endian hosts");
static_assert(sizeof(Date) == sizeof(std::int32_t));
static_assert(offsetof(Date, days) == 0);
static_assert(sizeof(Timestamp) == sizeof(std::int64_t));
static_assert(offsetof(Timestamp, nanos) == 0);
static_assert(sizeof(Column<Categorical>::code_type) == sizeof(std::int32_t));

struct StringBufferBacking {
    std::vector<std::uint32_t> offsets;
    std::vector<char> chars;
};

struct SchemaExportState {
    std::string format;
    std::string name;
    std::string metadata;
    std::vector<std::unique_ptr<ArrowSchema>> children_storage;
    std::unique_ptr<ArrowSchema*[]> children;
    std::unique_ptr<ArrowSchema> dictionary;
};

struct ArrayExportState {
    std::shared_ptr<const runtime::Table> table_owner;
    std::shared_ptr<void> extra_owner;
    std::vector<const void*> buffers_storage;
    std::unique_ptr<const void*[]> buffers;
    std::vector<std::unique_ptr<ArrowArray>> children_storage;
    std::unique_ptr<ArrowArray*[]> children;
    std::unique_ptr<ArrowArray> dictionary;
};

auto clear_schema(ArrowSchema* schema) noexcept -> void {
    if (schema == nullptr) {
        return;
    }
    schema->format = nullptr;
    schema->name = nullptr;
    schema->metadata = nullptr;
    schema->flags = 0;
    schema->n_children = 0;
    schema->children = nullptr;
    schema->dictionary = nullptr;
    schema->release = nullptr;
    schema->private_data = nullptr;
}

auto clear_array(ArrowArray* array) noexcept -> void {
    if (array == nullptr) {
        return;
    }
    array->length = 0;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 0;
    array->n_children = 0;
    array->buffers = nullptr;
    array->children = nullptr;
    array->dictionary = nullptr;
    array->release = nullptr;
    array->private_data = nullptr;
}

auto count_nulls(const runtime::ValidityBitmap& validity) noexcept -> std::int64_t {
    const std::size_t n = validity.size();
    const std::size_t word_bits = sizeof(runtime::ValidityBitmap::word_type) * 8;
    const std::size_t full_words = n / word_bits;
    const std::size_t tail_bits = n % word_bits;
    const auto* words = validity.words_data();
    std::size_t valid_count = 0;
    for (std::size_t i = 0; i < full_words; ++i) {
        valid_count += static_cast<std::size_t>(std::popcount(words[i]));
    }
    if (tail_bits != 0) {
        const auto mask = (runtime::ValidityBitmap::word_type{1} << tail_bits) - 1;
        valid_count += static_cast<std::size_t>(std::popcount(words[full_words] & mask));
    }
    return static_cast<std::int64_t>(n - valid_count);
}

auto append_i32_le(std::string& out, std::int32_t value) -> void {
    for (int shift = 0; shift < 32; shift += 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

auto encode_metadata(const std::vector<std::pair<std::string, std::string>>& pairs) -> std::string {
    if (pairs.empty()) {
        return {};
    }
    std::string encoded;
    encoded.reserve(16 * pairs.size());
    append_i32_le(encoded, static_cast<std::int32_t>(pairs.size()));
    for (const auto& [key, value] : pairs) {
        append_i32_le(encoded, static_cast<std::int32_t>(key.size()));
        encoded.append(key);
        append_i32_le(encoded, static_cast<std::int32_t>(value.size()));
        encoded.append(value);
    }
    return encoded;
}

auto ordering_metadata(const std::optional<std::vector<ir::OrderKey>>& ordering) -> std::string {
    if (!ordering.has_value() || ordering->empty()) {
        return {};
    }
    std::string out;
    for (std::size_t i = 0; i < ordering->size(); ++i) {
        if (i != 0) {
            out.push_back(',');
        }
        out.append((*ordering)[i].name);
        out.push_back(':');
        out.append((*ordering)[i].ascending ? "asc" : "desc");
    }
    return out;
}

auto build_dictionary_strings(const Column<Categorical>& col)
    -> std::shared_ptr<StringBufferBacking> {
    auto backing = std::make_shared<StringBufferBacking>();
    const auto& dict = col.dictionary();
    backing->offsets.reserve(dict.size() + 1);
    backing->offsets.push_back(0);
    std::size_t total_chars = 0;
    for (const auto& s : dict) {
        total_chars += s.size();
    }
    backing->chars.reserve(total_chars);
    for (const auto& s : dict) {
        backing->chars.insert(backing->chars.end(), s.begin(), s.end());
        backing->offsets.push_back(static_cast<std::uint32_t>(backing->chars.size()));
    }
    return backing;
}

auto finalize_schema(ArrowSchema* out, std::unique_ptr<SchemaExportState> state) -> void {
    out->format = state->format.c_str();
    out->name = state->name.empty() ? nullptr : state->name.c_str();
    out->metadata = state->metadata.empty() ? nullptr : state->metadata.data();
    out->flags = 0;
    out->n_children = static_cast<std::int64_t>(state->children_storage.size());
    out->children = state->children.get();
    out->dictionary = state->dictionary.get();
    out->release = &release_arrow_schema;
    out->private_data = state.release();
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto finalize_array(ArrowArray* out, std::unique_ptr<ArrayExportState> state, std::int64_t length,
                    std::int64_t null_count) -> void {
    out->length = length;
    out->null_count = null_count;
    out->offset = 0;
    out->n_buffers = static_cast<std::int64_t>(state->buffers_storage.size());
    out->n_children = static_cast<std::int64_t>(state->children_storage.size());
    out->buffers = state->buffers.get();
    out->children = state->children.get();
    out->dictionary = state->dictionary.get();
    out->release = &release_arrow_array;
    out->private_data = state.release();
}

auto export_column_schema(const runtime::ColumnEntry& entry, ArrowSchema* out_schema)
    -> std::expected<void, std::string>;
auto export_column_array(const runtime::ColumnEntry& entry,
                         std::shared_ptr<const runtime::Table> owner, ArrowArray* out_array)
    -> std::expected<void, std::string>;

auto export_column_schema(const runtime::ColumnEntry& entry, ArrowSchema* out_schema)
    -> std::expected<void, std::string> {
    auto state = std::make_unique<SchemaExportState>();
    state->name = entry.name;
    const bool nullable = entry.validity.has_value();

    auto set_format = [&](std::string format) {
        state->format = std::move(format);
        finalize_schema(out_schema, std::move(state));
        out_schema->flags = nullable ? kArrowFlagNullable : 0;
    };

    return std::visit(
        [&](const auto& col) -> std::expected<void, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                set_format("l");
            } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                set_format("g");
            } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                set_format("b");
            } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                set_format("tdD");
            } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                set_format("tsn:");
            } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                set_format("u");
            } else if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                state->format = "i";
                state->dictionary = std::make_unique<ArrowSchema>();
                clear_schema(state->dictionary.get());
                auto dict_state = std::make_unique<SchemaExportState>();
                dict_state->format = "u";
                finalize_schema(state->dictionary.get(), std::move(dict_state));
                finalize_schema(out_schema, std::move(state));
                out_schema->flags =
                    (nullable ? kArrowFlagNullable : 0) | (kArrowFlagDictionaryOrdered * 0);
            } else {
                return std::unexpected("unsupported column type for Arrow schema export");
            }
            return {};
        },
        *entry.column);
}

template <typename T>
auto primitive_buffers(const runtime::ColumnEntry& entry, const T* values)
    -> std::unique_ptr<ArrayExportState> {
    auto state = std::make_unique<ArrayExportState>();
    state->buffers_storage.reserve(2);
    state->buffers_storage.push_back(entry.validity.has_value()
                                         ? static_cast<const void*>(entry.validity->words_data())
                                         : nullptr);
    state->buffers_storage.push_back(static_cast<const void*>(values));
    state->buffers = std::make_unique<const void*[]>(state->buffers_storage.size());
    for (std::size_t i = 0; i < state->buffers_storage.size(); ++i) {
        state->buffers[i] = state->buffers_storage[i];
    }
    return state;
}

auto export_column_array(const runtime::ColumnEntry& entry,
                         std::shared_ptr<const runtime::Table> owner, ArrowArray* out_array)
    -> std::expected<void, std::string> {
    return std::visit(
        [&](const auto& col) -> std::expected<void, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            std::int64_t null_count = entry.validity.has_value() ? count_nulls(*entry.validity) : 0;

            if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                auto state = primitive_buffers(entry, col.data());
                state->table_owner = std::move(owner);
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                auto state = primitive_buffers(entry, col.data());
                state->table_owner = std::move(owner);
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                auto state = primitive_buffers(entry, col.words_data());
                state->table_owner = std::move(owner);
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                auto state = primitive_buffers(
                    entry, reinterpret_cast<const std::int32_t*>(
                               col.data()));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                state->table_owner = std::move(owner);
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                auto state = primitive_buffers(
                    entry, reinterpret_cast<const std::int64_t*>(
                               col.data()));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                state->table_owner = std::move(owner);
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                auto state = std::make_unique<ArrayExportState>();
                state->table_owner = std::move(owner);
                state->buffers_storage = {
                    entry.validity.has_value()
                        ? static_cast<const void*>(entry.validity->words_data())
                        : nullptr,
                    static_cast<const void*>(col.offsets_data()),
                    static_cast<const void*>(col.chars_data())};
                state->buffers = std::make_unique<const void*[]>(state->buffers_storage.size());
                for (std::size_t i = 0; i < state->buffers_storage.size(); ++i) {
                    state->buffers[i] = state->buffers_storage[i];
                }
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                auto state = std::make_unique<ArrayExportState>();
                state->table_owner = owner;
                state->buffers_storage = {
                    entry.validity.has_value()
                        ? static_cast<const void*>(entry.validity->words_data())
                        : nullptr,
                    static_cast<const void*>(col.codes_data())};
                state->buffers = std::make_unique<const void*[]>(state->buffers_storage.size());
                for (std::size_t i = 0; i < state->buffers_storage.size(); ++i) {
                    state->buffers[i] = state->buffers_storage[i];
                }

                auto dict_backing = build_dictionary_strings(col);
                auto dict_array = std::make_unique<ArrowArray>();
                clear_array(dict_array.get());
                auto dict_state = std::make_unique<ArrayExportState>();
                dict_state->extra_owner = dict_backing;
                dict_state->buffers_storage = {
                    nullptr, static_cast<const void*>(dict_backing->offsets.data()),
                    static_cast<const void*>(dict_backing->chars.data())};
                dict_state->buffers =
                    std::make_unique<const void*[]>(dict_state->buffers_storage.size());
                for (std::size_t i = 0; i < dict_state->buffers_storage.size(); ++i) {
                    dict_state->buffers[i] = dict_state->buffers_storage[i];
                }
                finalize_array(dict_array.get(), std::move(dict_state),
                               static_cast<std::int64_t>(dict_backing->offsets.size() - 1), 0);
                state->dictionary = std::move(dict_array);

                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count);
            } else {
                return std::unexpected("unsupported column type for Arrow array export");
            }
            return {};
        },
        *entry.column);
}

auto ensure_export_target(ArrowArray* out_array, ArrowSchema* out_schema)
    -> std::expected<void, std::string> {
    if (out_array == nullptr || out_schema == nullptr) {
        return std::unexpected("Arrow export requires non-null ArrowArray and ArrowSchema");
    }
    if (out_array->release != nullptr || out_schema->release != nullptr) {
        return std::unexpected("Arrow export target must be empty (release == nullptr)");
    }
    clear_array(out_array);
    clear_schema(out_schema);
    return {};
}

auto export_table_impl(const std::shared_ptr<const runtime::Table>& table, ArrowArray* out_array,
                       ArrowSchema* out_schema) -> std::expected<void, std::string> {
    auto ready = ensure_export_target(out_array, out_schema);
    if (!ready) {
        return std::unexpected(ready.error());
    }

    auto schema_state = std::make_unique<SchemaExportState>();
    schema_state->format = "+s";
    std::vector<std::pair<std::string, std::string>> metadata;
    if (table->time_index.has_value()) {
        metadata.emplace_back("ibex.time_index", *table->time_index);
    }
    if (auto ord = ordering_metadata(table->ordering); !ord.empty()) {
        metadata.emplace_back("ibex.ordering", std::move(ord));
    }
    schema_state->metadata = encode_metadata(metadata);
    schema_state->children_storage.reserve(table->columns.size());
    schema_state->children = std::make_unique<ArrowSchema*[]>(table->columns.size());

    auto array_state = std::make_unique<ArrayExportState>();
    array_state->table_owner = table;
    array_state->buffers_storage = {nullptr};
    array_state->buffers = std::make_unique<const void*[]>(1);
    array_state->buffers[0] = nullptr;
    array_state->children_storage.reserve(table->columns.size());
    array_state->children = std::make_unique<ArrowArray*[]>(table->columns.size());

    for (std::size_t i = 0; i < table->columns.size(); ++i) {
        auto child_schema = std::make_unique<ArrowSchema>();
        auto child_array = std::make_unique<ArrowArray>();
        clear_schema(child_schema.get());
        clear_array(child_array.get());
        auto schema_result = export_column_schema(table->columns[i], child_schema.get());
        if (!schema_result) {
            return std::unexpected(schema_result.error());
        }
        auto array_result = export_column_array(table->columns[i], table, child_array.get());
        if (!array_result) {
            return std::unexpected(array_result.error());
        }
        schema_state->children[i] = child_schema.get();
        array_state->children[i] = child_array.get();
        schema_state->children_storage.push_back(std::move(child_schema));
        array_state->children_storage.push_back(std::move(child_array));
    }

    finalize_schema(out_schema, std::move(schema_state));
    finalize_array(out_array, std::move(array_state), static_cast<std::int64_t>(table->rows()), 0);
    return {};
}

}  // namespace

auto release_arrow_schema(ArrowSchema* schema) noexcept -> void {
    if (schema == nullptr || schema->release == nullptr) {
        return;
    }
    auto* state = static_cast<SchemaExportState*>(schema->private_data);
    schema->release = nullptr;
    if (state != nullptr) {
        if (state->dictionary && state->dictionary->release != nullptr) {
            state->dictionary->release(state->dictionary.get());
        }
        for (auto& child : state->children_storage) {
            if (child && child->release != nullptr) {
                child->release(child.get());
            }
        }
        delete state;  // NOLINT(cppcoreguidelines-owning-memory)
    }
    clear_schema(schema);
}

auto release_arrow_array(ArrowArray* array) noexcept -> void {
    if (array == nullptr || array->release == nullptr) {
        return;
    }
    auto* state = static_cast<ArrayExportState*>(array->private_data);
    array->release = nullptr;
    if (state != nullptr) {
        if (state->dictionary && state->dictionary->release != nullptr) {
            state->dictionary->release(state->dictionary.get());
        }
        for (auto& child : state->children_storage) {
            if (child && child->release != nullptr) {
                child->release(child.get());
            }
        }
        delete state;  // NOLINT(cppcoreguidelines-owning-memory)
    }
    clear_array(array);
}

auto export_table_to_arrow(const runtime::Table& table, ArrowArray* out_array,
                           ArrowSchema* out_schema) -> std::expected<void, std::string> {
    return export_table_impl(std::make_shared<runtime::Table>(table), out_array, out_schema);
}

auto export_table_to_arrow(const std::shared_ptr<const runtime::Table>& table,
                           ArrowArray* out_array, ArrowSchema* out_schema)
    -> std::expected<void, std::string> {
    if (!table) {
        return std::unexpected("Arrow export requires a non-null table");
    }
    return export_table_impl(table, out_array, out_schema);
}

}  // namespace ibex::interop
// NOLINTEND(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
