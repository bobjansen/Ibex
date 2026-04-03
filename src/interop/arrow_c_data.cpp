// Arrow C Data Interface requires C-style arrays for ABI compatibility.
// NOLINTBEGIN(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
#include <ibex/interop/arrow_c_data.hpp>

#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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

auto read_i32_le(const char* p) -> std::int32_t {
    return static_cast<std::int32_t>(static_cast<unsigned char>(p[0])) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[1])) << 8) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[2])) << 16) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[3])) << 24);
}

auto decode_metadata(const char* metadata)
    -> std::expected<std::vector<std::pair<std::string, std::string>>, std::string> {
    std::vector<std::pair<std::string, std::string>> out;
    if (metadata == nullptr) {
        return out;
    }

    const char* p = metadata;
    const auto pair_count = read_i32_le(p);
    if (pair_count < 0) {
        return std::unexpected("Arrow metadata has a negative pair count");
    }
    p += 4;
    out.reserve(static_cast<std::size_t>(pair_count));
    for (std::int32_t i = 0; i < pair_count; ++i) {
        const auto key_len = read_i32_le(p);
        p += 4;
        if (key_len < 0) {
            return std::unexpected("Arrow metadata has a negative key length");
        }
        std::string key(p, p + key_len);
        p += key_len;

        const auto value_len = read_i32_le(p);
        p += 4;
        if (value_len < 0) {
            return std::unexpected("Arrow metadata has a negative value length");
        }
        std::string value(p, p + value_len);
        p += value_len;
        out.emplace_back(std::move(key), std::move(value));
    }
    return out;
}

auto find_metadata_value(const std::vector<std::pair<std::string, std::string>>& metadata,
                         std::string_view key) -> std::optional<std::string> {
    for (const auto& [k, v] : metadata) {
        if (k == key) {
            return v;
        }
    }
    return std::nullopt;
}

auto parse_ordering(std::string_view text)
    -> std::expected<std::optional<std::vector<ir::OrderKey>>, std::string> {
    if (text.empty()) {
        return std::nullopt;
    }

    std::vector<ir::OrderKey> ordering;
    std::size_t pos = 0;
    while (pos < text.size()) {
        const std::size_t next = text.find(',', pos);
        const std::string_view item =
            next == std::string_view::npos ? text.substr(pos) : text.substr(pos, next - pos);
        const std::size_t colon = item.rfind(':');
        if (colon == std::string_view::npos || colon == 0 || colon + 1 >= item.size()) {
            return std::unexpected("invalid ibex.ordering metadata");
        }
        const std::string_view direction = item.substr(colon + 1);
        if (direction != "asc" && direction != "desc") {
            return std::unexpected("invalid ibex.ordering direction");
        }
        ordering.push_back(
            {.name = std::string(item.substr(0, colon)), .ascending = direction == "asc"});
        if (next == std::string_view::npos) {
            break;
        }
        pos = next + 1;
    }
    return ordering;
}

auto read_bitmap_bit(const std::uint8_t* bytes, std::int64_t index) -> bool {
    const auto byte_index = static_cast<std::size_t>(index / 8);
    const auto bit_index = static_cast<unsigned>(index % 8);
    return ((bytes[byte_index] >> bit_index) & 0x01U) != 0U;
}

auto import_validity(const ArrowArray& array) -> std::optional<runtime::ValidityBitmap> {
    if (array.null_count == 0 || array.buffers == nullptr || array.n_buffers < 1 ||
        array.buffers[0] == nullptr) {
        return std::nullopt;
    }

    runtime::ValidityBitmap validity(static_cast<std::size_t>(array.length), false);
    const auto* bitmap = static_cast<const std::uint8_t*>(array.buffers[0]);
    for (std::int64_t i = 0; i < array.length; ++i) {
        validity.set(static_cast<std::size_t>(i), read_bitmap_bit(bitmap, array.offset + i));
    }
    return validity;
}

auto validate_child(const ArrowArray& array, const ArrowSchema& schema, std::string_view where)
    -> std::expected<void, std::string> {
    if (schema.release == nullptr) {
        return std::unexpected(std::string(where) + ": Arrow schema is released");
    }
    if (array.release == nullptr) {
        return std::unexpected(std::string(where) + ": Arrow array is released");
    }
    if (array.length < 0 || array.offset < 0) {
        return std::unexpected(std::string(where) + ": negative Arrow length or offset");
    }
    return {};
}

template <typename T>
auto import_plain_column(const ArrowArray& array, std::size_t data_buffer_index)
    -> std::expected<Column<T>, std::string> {
    if (array.buffers == nullptr ||
        array.n_buffers <= static_cast<std::int64_t>(data_buffer_index) ||
        array.buffers[data_buffer_index] == nullptr) {
        return std::unexpected("Arrow array is missing a primitive data buffer");
    }

    const auto* values = static_cast<const T*>(array.buffers[data_buffer_index]);
    Column<T> column;
    column.reserve(static_cast<std::size_t>(array.length));
    for (std::int64_t i = 0; i < array.length; ++i) {
        column.push_back(values[array.offset + i]);
    }
    return column;
}

template <typename T>
auto import_primitive_column(const ArrowArray& array, std::size_t data_buffer_index)
    -> std::expected<runtime::ColumnValue, std::string> {
    auto column = import_plain_column<T>(array, data_buffer_index);
    if (!column) {
        return std::unexpected(column.error());
    }
    return runtime::ColumnValue{std::move(*column)};
}

auto import_bool_column(const ArrowArray& array)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (array.buffers == nullptr || array.n_buffers < 2 || array.buffers[1] == nullptr) {
        return std::unexpected("Arrow bool array is missing a data buffer");
    }

    const auto* bitmap = static_cast<const std::uint8_t*>(array.buffers[1]);
    Column<bool> column;
    column.reserve(static_cast<std::size_t>(array.length));
    for (std::int64_t i = 0; i < array.length; ++i) {
        column.push_back(read_bitmap_bit(bitmap, array.offset + i));
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_string_column(const ArrowArray& array)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (array.buffers == nullptr || array.n_buffers < 3 || array.buffers[1] == nullptr ||
        array.buffers[2] == nullptr) {
        return std::unexpected("Arrow utf8 array is missing offsets or char buffers");
    }

    const auto* offsets = static_cast<const std::int32_t*>(array.buffers[1]);
    const auto* chars = static_cast<const char*>(array.buffers[2]);
    const auto start_base = offsets[array.offset];

    Column<std::string> column;
    std::size_t total_chars = 0;
    for (std::int64_t i = 0; i < array.length; ++i) {
        const auto start = offsets[array.offset + i];
        const auto end = offsets[array.offset + i + 1];
        if (start < start_base || end < start) {
            return std::unexpected("Arrow utf8 array has invalid offsets");
        }
        total_chars += static_cast<std::size_t>(end - start);
    }
    column.reserve(static_cast<std::size_t>(array.length), total_chars);

    for (std::int64_t i = 0; i < array.length; ++i) {
        const auto start = offsets[array.offset + i];
        const auto end = offsets[array.offset + i + 1];
        column.push_back(std::string_view(chars + start, static_cast<std::size_t>(end - start)));
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_dictionary_strings(const ArrowArray& dictionary)
    -> std::expected<std::vector<std::string>, std::string> {
    if (dictionary.buffers == nullptr || dictionary.n_buffers < 3 ||
        dictionary.buffers[1] == nullptr || dictionary.buffers[2] == nullptr) {
        return std::unexpected("Arrow dictionary array is missing utf8 buffers");
    }

    const auto* offsets = static_cast<const std::int32_t*>(dictionary.buffers[1]);
    const auto* chars = static_cast<const char*>(dictionary.buffers[2]);
    std::vector<std::string> values;
    values.reserve(static_cast<std::size_t>(dictionary.length));
    for (std::int64_t i = 0; i < dictionary.length; ++i) {
        const auto start = offsets[dictionary.offset + i];
        const auto end = offsets[dictionary.offset + i + 1];
        if (end < start) {
            return std::unexpected("Arrow dictionary utf8 array has invalid offsets");
        }
        values.emplace_back(chars + start, chars + end);
    }
    return values;
}

auto import_categorical_column(const ArrowArray& array, const ArrowSchema& schema)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (schema.dictionary == nullptr || array.dictionary == nullptr) {
        return std::unexpected("Arrow dictionary column is missing dictionary storage");
    }
    if (std::string_view(schema.dictionary->format != nullptr ? schema.dictionary->format : "") !=
        "u") {
        return std::unexpected("Arrow dictionary column currently requires utf8 dictionary values");
    }
    auto dict_values = import_dictionary_strings(*array.dictionary);
    if (!dict_values.has_value()) {
        return std::unexpected(dict_values.error());
    }

    if (array.buffers == nullptr || array.n_buffers < 2 || array.buffers[1] == nullptr) {
        return std::unexpected("Arrow dictionary column is missing indices");
    }
    const auto* codes = static_cast<const std::int32_t*>(array.buffers[1]);
    Column<Categorical> column(std::move(*dict_values));
    column.reserve(static_cast<std::size_t>(array.length));
    for (std::int64_t i = 0; i < array.length; ++i) {
        const auto code = codes[array.offset + i];
        if (code < 0 || static_cast<std::size_t>(code) >= column.dictionary().size()) {
            return std::unexpected("Arrow dictionary column has an out-of-range index");
        }
        column.push_code(code);
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_column(const ArrowArray& array, const ArrowSchema& schema)
    -> std::expected<std::pair<runtime::ColumnValue, std::optional<runtime::ValidityBitmap>>,
                     std::string> {
    auto ready = validate_child(array, schema, "Arrow column import");
    if (!ready) {
        return std::unexpected(ready.error());
    }

    const std::string_view format = schema.format != nullptr ? schema.format : "";
    std::expected<runtime::ColumnValue, std::string> column =
        std::unexpected("unsupported Arrow column format");

    if (format == "l") {
        column = import_primitive_column<std::int64_t>(array, 1);
    } else if (format == "g") {
        column = import_primitive_column<double>(array, 1);
    } else if (format == "b") {
        column = import_bool_column(array);
    } else if (format == "tdD") {
        auto raw = import_plain_column<std::int32_t>(array, 1);
        if (!raw) {
            return std::unexpected(raw.error());
        }
        Column<Date> dates;
        dates.reserve(raw->size());
        for (std::size_t i = 0; i < raw->size(); ++i) {
            dates.push_back(Date{(*raw)[i]});
        }
        column = runtime::ColumnValue{std::move(dates)};
    } else if (format == "tsn:") {
        auto raw = import_plain_column<std::int64_t>(array, 1);
        if (!raw) {
            return std::unexpected(raw.error());
        }
        Column<Timestamp> ts;
        ts.reserve(raw->size());
        for (std::size_t i = 0; i < raw->size(); ++i) {
            ts.push_back(Timestamp{(*raw)[i]});
        }
        column = runtime::ColumnValue{std::move(ts)};
    } else if (format == "u") {
        column = import_string_column(array);
    } else if (format == "i") {
        column = import_categorical_column(array, schema);
    }

    if (!column) {
        return std::unexpected(column.error());
    }
    return std::pair{std::move(*column), import_validity(array)};
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

auto import_table_from_arrow(const ArrowArray& array, const ArrowSchema& schema)
    -> std::expected<runtime::Table, std::string> {
    auto ready = validate_child(array, schema, "Arrow table import");
    if (!ready) {
        return std::unexpected(ready.error());
    }

    if (std::string_view(schema.format != nullptr ? schema.format : "") != "+s") {
        return std::unexpected("Arrow table import currently requires a struct schema");
    }
    if (schema.n_children != array.n_children) {
        return std::unexpected("Arrow table import requires matching child counts");
    }
    if (schema.children == nullptr || array.children == nullptr) {
        if (schema.n_children != 0 || array.n_children != 0) {
            return std::unexpected("Arrow table import is missing child arrays or schemas");
        }
    }

    runtime::Table table;
    for (std::int64_t i = 0; i < schema.n_children; ++i) {
        const ArrowSchema* child_schema = schema.children[i];
        const ArrowArray* child_array = array.children[i];
        if (child_schema == nullptr || child_array == nullptr) {
            return std::unexpected("Arrow table import encountered a null child");
        }
        if (child_array->length != array.length) {
            return std::unexpected(
                "Arrow table import requires every column to match table length");
        }
        auto imported = import_column(*child_array, *child_schema);
        if (!imported) {
            return std::unexpected(imported.error());
        }
        const std::string name = child_schema->name != nullptr ? child_schema->name : "";
        if (name.empty()) {
            return std::unexpected("Arrow table import requires named child columns");
        }
        auto& [column, validity] = *imported;
        if (validity.has_value()) {
            table.add_column(name, std::move(column), std::move(*validity));
        } else {
            table.add_column(name, std::move(column));
        }
    }

    auto metadata = decode_metadata(schema.metadata);
    if (!metadata) {
        return std::unexpected(metadata.error());
    }
    if (auto time_index = find_metadata_value(*metadata, "ibex.time_index");
        time_index.has_value()) {
        table.time_index = *time_index;
    }
    if (auto ordering_text = find_metadata_value(*metadata, "ibex.ordering");
        ordering_text.has_value()) {
        auto ordering = parse_ordering(*ordering_text);
        if (!ordering) {
            return std::unexpected(ordering.error());
        }
        table.ordering = std::move(*ordering);
    }

    return table;
}

}  // namespace ibex::interop
// NOLINTEND(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
