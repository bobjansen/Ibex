// Arrow C Data Interface requires C-style arrays for ABI compatibility.
// NOLINTBEGIN(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
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

struct AdoptedArrayOwner {
    ArrowArray array{};
    bool owns = false;

    explicit AdoptedArrayOwner(const ArrowArray& source) : array(source) {}

    AdoptedArrayOwner(const AdoptedArrayOwner&) = delete;
    auto operator=(const AdoptedArrayOwner&) -> AdoptedArrayOwner& = delete;

    ~AdoptedArrayOwner() {
        if (owns && array.release != nullptr) {
            array.release(&array);
        }
    }
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

auto clear_stream(ArrowArrayStream* stream) noexcept -> void {
    if (stream == nullptr) {
        return;
    }
    stream->get_schema = nullptr;
    stream->get_next = nullptr;
    stream->get_last_error = nullptr;
    stream->release = nullptr;
    stream->private_data = nullptr;
}

auto count_nulls(const runtime::ValidityBitmap& validity) noexcept -> std::int64_t {
    if (validity.buffer_offset() != 0) {
        std::size_t valid_count = 0;
        for (std::size_t i = 0; i < validity.size(); ++i) {
            valid_count += validity[i] ? 1U : 0U;
        }
        return static_cast<std::int64_t>(validity.size() - valid_count);
    }

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

auto import_validity(const ArrowArray& array, const std::shared_ptr<const void>& owner)
    -> std::optional<runtime::ValidityBitmap> {
    if (array.null_count == 0 || array.buffers == nullptr || array.n_buffers < 1 ||
        array.buffers[0] == nullptr) {
        return std::nullopt;
    }

    if (owner) {
        return runtime::ValidityBitmap::from_external(
            owner, static_cast<const std::uint8_t*>(array.buffers[0]),
            static_cast<std::size_t>(array.offset), static_cast<std::size_t>(array.length));
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
auto import_primitive_column(const ArrowArray& array, std::size_t data_buffer_index,
                             const std::shared_ptr<const void>& owner)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (owner) {
        if (array.buffers == nullptr ||
            array.n_buffers <= static_cast<std::int64_t>(data_buffer_index) ||
            array.buffers[data_buffer_index] == nullptr) {
            return std::unexpected("Arrow array is missing a primitive data buffer");
        }
        const auto* values = static_cast<const T*>(array.buffers[data_buffer_index]);
        return runtime::ColumnValue{
            Column<T>::from_external(owner, values, static_cast<std::size_t>(array.offset),
                                     static_cast<std::size_t>(array.length))};
    }

    auto column = import_plain_column<T>(array, data_buffer_index);
    if (!column) {
        return std::unexpected(column.error());
    }
    return runtime::ColumnValue{std::move(*column)};
}

template <typename Temporal, typename Raw>
auto import_temporal_column(const ArrowArray& array, const std::shared_ptr<const void>& owner)
    -> std::expected<runtime::ColumnValue, std::string> {
    static_assert(std::is_trivially_copyable_v<Temporal>);
    static_assert(std::is_standard_layout_v<Temporal>);
    static_assert(sizeof(Temporal) == sizeof(Raw));
    static_assert(alignof(Temporal) == alignof(Raw));

    if (array.buffers == nullptr || array.n_buffers < 2 ||
        (array.buffers[1] == nullptr && array.length != 0)) {
        return std::unexpected("Arrow temporal array is missing a data buffer");
    }

    const auto* values = static_cast<const Raw*>(array.buffers[1]);
    if (owner) {
        return runtime::ColumnValue{Column<Temporal>::from_external(
            owner, reinterpret_cast<const Temporal*>(values),
            static_cast<std::size_t>(array.offset), static_cast<std::size_t>(array.length))};
    }

    Column<Temporal> column;
    column.reserve(static_cast<std::size_t>(array.length));
    for (std::int64_t i = 0; i < array.length; ++i) {
        if constexpr (std::is_same_v<Temporal, Date>) {
            column.push_back(Date{values[array.offset + i]});
        } else {
            column.push_back(Timestamp{values[array.offset + i]});
        }
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_bool_column(const ArrowArray& array, const std::shared_ptr<const void>& owner)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (array.buffers == nullptr || array.n_buffers < 2 ||
        (array.buffers[1] == nullptr && array.length != 0)) {
        return std::unexpected("Arrow bool array is missing a data buffer");
    }

    const auto* bitmap = static_cast<const std::uint8_t*>(array.buffers[1]);
    if (owner) {
        return runtime::ColumnValue{
            Column<bool>::from_external(owner, bitmap, static_cast<std::size_t>(array.offset),
                                        static_cast<std::size_t>(array.length))};
    }
    Column<bool> column;
    column.reserve(static_cast<std::size_t>(array.length));
    for (std::int64_t i = 0; i < array.length; ++i) {
        column.push_back(read_bitmap_bit(bitmap, array.offset + i));
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_string_column(const ArrowArray& array, const std::shared_ptr<const void>& owner)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (array.buffers == nullptr || array.n_buffers < 3 || array.buffers[1] == nullptr) {
        return std::unexpected("Arrow utf8 array is missing offsets or char buffers");
    }

    const auto* offsets = static_cast<const std::int32_t*>(array.buffers[1]);
    const auto* chars = static_cast<const char*>(array.buffers[2]);
    const auto start_base = offsets[array.offset];
    if (start_base < 0) {
        return std::unexpected("Arrow utf8 array has invalid offsets");
    }
    if (chars == nullptr && offsets[array.offset + array.length] != 0) {
        return std::unexpected("Arrow utf8 array is missing a character buffer");
    }

    std::size_t total_chars = 0;
    for (std::int64_t i = 0; i < array.length; ++i) {
        const auto start = offsets[array.offset + i];
        const auto end = offsets[array.offset + i + 1];
        if (start < start_base || end < start) {
            return std::unexpected("Arrow utf8 array has invalid offsets");
        }
        total_chars += static_cast<std::size_t>(end - start);
    }

    if (owner) {
        return runtime::ColumnValue{Column<std::string>::from_external(
            owner, reinterpret_cast<const std::uint32_t*>(offsets), chars,
            static_cast<std::size_t>(array.offset), static_cast<std::size_t>(array.length))};
    }

    Column<std::string> column;
    column.reserve(static_cast<std::size_t>(array.length), total_chars);

    for (std::int64_t i = 0; i < array.length; ++i) {
        const auto start = offsets[array.offset + i];
        const auto end = offsets[array.offset + i + 1];
        column.push_back(
            start == end ? std::string_view{}
                         : std::string_view(chars + start, static_cast<std::size_t>(end - start)));
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_dictionary_strings(const ArrowArray& dictionary)
    -> std::expected<std::vector<std::string>, std::string> {
    if (dictionary.buffers == nullptr || dictionary.n_buffers < 3 ||
        dictionary.buffers[1] == nullptr) {
        return std::unexpected("Arrow dictionary array is missing utf8 buffers");
    }

    const auto* offsets = static_cast<const std::int32_t*>(dictionary.buffers[1]);
    const auto* chars = static_cast<const char*>(dictionary.buffers[2]);
    if (chars == nullptr && offsets[dictionary.offset + dictionary.length] != 0) {
        return std::unexpected("Arrow dictionary array is missing a character buffer");
    }
    std::vector<std::string> values;
    values.reserve(static_cast<std::size_t>(dictionary.length));
    for (std::int64_t i = 0; i < dictionary.length; ++i) {
        const auto start = offsets[dictionary.offset + i];
        const auto end = offsets[dictionary.offset + i + 1];
        if (end < start) {
            return std::unexpected("Arrow dictionary utf8 array has invalid offsets");
        }
        if (start == end) {
            values.emplace_back();
        } else {
            values.emplace_back(chars + start, chars + end);
        }
    }
    return values;
}

auto import_categorical_column(const ArrowArray& array, const ArrowSchema& schema,
                               const std::shared_ptr<const void>& owner)
    -> std::expected<runtime::ColumnValue, std::string> {
    if (schema.dictionary == nullptr || array.dictionary == nullptr) {
        return std::unexpected("Arrow dictionary column is missing dictionary storage");
    }
    if (std::string_view(schema.dictionary->format != nullptr ? schema.dictionary->format : "") !=
        "u") {
        return std::unexpected("Arrow dictionary column currently requires utf8 dictionary values");
    }
    if (array.buffers == nullptr || array.n_buffers < 2 ||
        (array.buffers[1] == nullptr && array.length != 0)) {
        return std::unexpected("Arrow dictionary column is missing indices");
    }
    const auto* codes = static_cast<const std::int32_t*>(array.buffers[1]);

    const ArrowArray& dictionary = *array.dictionary;
    if (dictionary.buffers == nullptr || dictionary.n_buffers < 3 ||
        dictionary.buffers[1] == nullptr) {
        return std::unexpected("Arrow dictionary array is missing utf8 buffers");
    }
    const auto* dict_offsets = static_cast<const std::int32_t*>(dictionary.buffers[1]);
    const auto* dict_chars = static_cast<const char*>(dictionary.buffers[2]);
    if (dict_chars == nullptr && dict_offsets[dictionary.offset + dictionary.length] != 0) {
        return std::unexpected("Arrow dictionary array is missing a character buffer");
    }
    if (dict_offsets[dictionary.offset] < 0) {
        return std::unexpected("Arrow dictionary utf8 array has invalid offsets");
    }
    for (std::int64_t i = 0; i < dictionary.length; ++i) {
        const auto start = dict_offsets[dictionary.offset + i];
        const auto end = dict_offsets[dictionary.offset + i + 1];
        if (start < 0 || end < start) {
            return std::unexpected("Arrow dictionary utf8 array has invalid offsets");
        }
    }

    const auto* validity = static_cast<const std::uint8_t*>(array.buffers[0]);
    for (std::int64_t i = 0; i < array.length; ++i) {
        if (validity != nullptr && !read_bitmap_bit(validity, array.offset + i)) {
            // Arrow leaves payload bytes at null positions unspecified. In
            // particular, nanoarrow's factor converter writes INT32_MIN here.
            continue;
        }
        const auto code = codes[array.offset + i];
        if (code < 0 || code >= dictionary.length) {
            return std::unexpected("Arrow dictionary column has an out-of-range index");
        }
    }

    if (owner) {
        return runtime::ColumnValue{Column<Categorical>::from_external(
            owner, codes, static_cast<std::size_t>(array.offset),
            static_cast<std::size_t>(array.length),
            reinterpret_cast<const std::uint32_t*>(dict_offsets), dict_chars,
            static_cast<std::size_t>(dictionary.offset),
            static_cast<std::size_t>(dictionary.length))};
    }

    auto dict_values = import_dictionary_strings(dictionary);
    if (!dict_values.has_value()) {
        return std::unexpected(dict_values.error());
    }
    Column<Categorical> column(std::move(*dict_values));
    column.reserve(static_cast<std::size_t>(array.length));
    for (std::int64_t i = 0; i < array.length; ++i) {
        column.push_code(codes[array.offset + i]);
    }
    return runtime::ColumnValue{std::move(column)};
}

auto import_column(const ArrowArray& array, const ArrowSchema& schema,
                   const std::shared_ptr<const void>& owner)
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
        column = import_primitive_column<std::int64_t>(array, 1, owner);
    } else if (format == "g") {
        column = import_primitive_column<double>(array, 1, owner);
    } else if (format == "b") {
        column = import_bool_column(array, owner);
    } else if (format == "tdD") {
        column = import_temporal_column<Date, std::int32_t>(array, owner);
    } else if (format == "tsn:") {
        column = import_temporal_column<Timestamp, std::int64_t>(array, owner);
    } else if (format == "u") {
        column = import_string_column(array, owner);
    } else if (format == "i") {
        column = import_categorical_column(array, schema, owner);
    }

    if (!column) {
        return std::unexpected(column.error());
    }
    const bool buffer_adopted =
        owner && (format == "l" || format == "g" || format == "b" || format == "tdD" ||
                  format == "tsn:" || format == "u" || format == "i");
    return std::pair{
        std::move(*column),
        import_validity(array, buffer_adopted ? owner : std::shared_ptr<const void>{})};
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
                    std::int64_t null_count, std::int64_t offset = 0) -> void {
    out->length = length;
    out->null_count = null_count;
    out->offset = offset;
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
auto primitive_buffers(const runtime::ColumnEntry& entry, const Column<T>& column)
    -> std::expected<std::unique_ptr<ArrayExportState>, std::string> {
    if (entry.validity.has_value() && entry.validity->buffer_offset() != column.buffer_offset()) {
        return std::unexpected(
            "Arrow export requires primitive value and validity offsets to match");
    }

    auto state = std::make_unique<ArrayExportState>();
    state->buffers_storage.reserve(2);
    state->buffers_storage.push_back(entry.validity.has_value()
                                         ? static_cast<const void*>(entry.validity->buffer_data())
                                         : nullptr);
    state->buffers_storage.push_back(static_cast<const void*>(column.buffer_data()));
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
                auto state = primitive_buffers(entry, col);
                if (!state) {
                    return std::unexpected(state.error());
                }
                (*state)->table_owner = std::move(owner);
                finalize_array(out_array, std::move(*state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
            } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                auto state = primitive_buffers(entry, col);
                if (!state) {
                    return std::unexpected(state.error());
                }
                (*state)->table_owner = std::move(owner);
                finalize_array(out_array, std::move(*state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
            } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                if (entry.validity.has_value() &&
                    entry.validity->buffer_offset() != col.buffer_offset()) {
                    return std::unexpected(
                        "Arrow export requires bool value and validity offsets to match");
                }
                auto state = std::make_unique<ArrayExportState>();
                state->buffers_storage = {
                    entry.validity.has_value()
                        ? static_cast<const void*>(entry.validity->buffer_data())
                        : nullptr,
                    static_cast<const void*>(col.buffer_data())};
                state->buffers = std::make_unique<const void*[]>(state->buffers_storage.size());
                for (std::size_t i = 0; i < state->buffers_storage.size(); ++i) {
                    state->buffers[i] = state->buffers_storage[i];
                }
                state->table_owner = std::move(owner);
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
            } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                auto state = primitive_buffers(entry, col);
                if (!state) {
                    return std::unexpected(state.error());
                }
                (*state)->table_owner = std::move(owner);
                finalize_array(out_array, std::move(*state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
            } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                auto state = primitive_buffers(entry, col);
                if (!state) {
                    return std::unexpected(state.error());
                }
                (*state)->table_owner = std::move(owner);
                finalize_array(out_array, std::move(*state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
            } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                if (entry.validity.has_value() &&
                    entry.validity->buffer_offset() != col.buffer_offset()) {
                    return std::unexpected(
                        "Arrow export requires string value and validity offsets to match");
                }
                auto state = std::make_unique<ArrayExportState>();
                state->table_owner = std::move(owner);
                state->buffers_storage = {
                    entry.validity.has_value()
                        ? static_cast<const void*>(entry.validity->buffer_data())
                        : nullptr,
                    static_cast<const void*>(col.offsets_buffer_data()),
                    static_cast<const void*>(col.chars_buffer_data())};
                state->buffers = std::make_unique<const void*[]>(state->buffers_storage.size());
                for (std::size_t i = 0; i < state->buffers_storage.size(); ++i) {
                    state->buffers[i] = state->buffers_storage[i];
                }
                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
            } else if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                if (entry.validity.has_value() &&
                    entry.validity->buffer_offset() != col.buffer_offset()) {
                    return std::unexpected(
                        "Arrow export requires categorical value and validity offsets to match");
                }
                auto state = std::make_unique<ArrayExportState>();
                state->table_owner = owner;
                state->buffers_storage = {
                    entry.validity.has_value()
                        ? static_cast<const void*>(entry.validity->buffer_data())
                        : nullptr,
                    static_cast<const void*>(col.codes_buffer_data())};
                state->buffers = std::make_unique<const void*[]>(state->buffers_storage.size());
                for (std::size_t i = 0; i < state->buffers_storage.size(); ++i) {
                    state->buffers[i] = state->buffers_storage[i];
                }

                auto dict_array = std::make_unique<ArrowArray>();
                clear_array(dict_array.get());
                auto dict_state = std::make_unique<ArrayExportState>();
                std::int64_t dictionary_offset = 0;
                std::int64_t dictionary_length = 0;
                if (col.dictionary_is_external()) {
                    dict_state->table_owner = owner;
                    dict_state->buffers_storage = {
                        nullptr, static_cast<const void*>(col.dictionary_offsets_buffer_data()),
                        static_cast<const void*>(col.dictionary_chars_buffer_data())};
                    dictionary_offset = static_cast<std::int64_t>(col.dictionary_buffer_offset());
                    dictionary_length = static_cast<std::int64_t>(col.dictionary_size());
                } else {
                    auto dict_backing = build_dictionary_strings(col);
                    dictionary_length = static_cast<std::int64_t>(dict_backing->offsets.size() - 1);
                    dict_state->extra_owner = dict_backing;
                    dict_state->buffers_storage = {
                        nullptr, static_cast<const void*>(dict_backing->offsets.data()),
                        static_cast<const void*>(dict_backing->chars.data())};
                }
                dict_state->buffers =
                    std::make_unique<const void*[]>(dict_state->buffers_storage.size());
                for (std::size_t i = 0; i < dict_state->buffers_storage.size(); ++i) {
                    dict_state->buffers[i] = dict_state->buffers_storage[i];
                }
                finalize_array(dict_array.get(), std::move(dict_state), dictionary_length, 0,
                               dictionary_offset);
                state->dictionary = std::move(dict_array);

                finalize_array(out_array, std::move(state), static_cast<std::int64_t>(col.size()),
                               null_count, static_cast<std::int64_t>(col.buffer_offset()));
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
    if (schema->release != &release_arrow_schema) {
        auto* release = schema->release;
        schema->release = nullptr;
        release(schema);
        clear_schema(schema);
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
    if (array->release != &release_arrow_array) {
        auto* release = array->release;
        array->release = nullptr;
        release(array);
        clear_array(array);
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

auto release_arrow_stream(ArrowArrayStream* stream) noexcept -> void {
    if (stream == nullptr || stream->release == nullptr) {
        return;
    }
    auto* release = stream->release;
    stream->release = nullptr;
    release(stream);
    clear_stream(stream);
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

auto import_table_impl(const ArrowArray& array, const ArrowSchema& schema,
                       const std::shared_ptr<const void>& owner)
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
        ArrowArray child_view = *child_array;
        if (child_view.length != array.length) {
            // A canonical sliced struct keeps full-length children and applies
            // the struct offset to each child logically. Also accept already-
            // sliced children for producers that materialize that view.
            if (array.offset < 0 || child_view.length < array.offset + array.length) {
                return std::unexpected(
                    "Arrow table import requires every column to cover the table slice");
            }
            child_view.offset += array.offset;
            child_view.length = array.length;
        }
        auto imported = import_column(child_view, *child_schema, owner);
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

auto import_table_from_arrow(const ArrowArray& array, const ArrowSchema& schema)
    -> std::expected<runtime::Table, std::string> {
    return import_table_impl(array, schema, {});
}

auto adopt_table_from_arrow(ArrowArray* array, const ArrowSchema& schema)
    -> std::expected<runtime::Table, std::string> {
    if (array == nullptr) {
        return std::unexpected("Arrow table adoption requires a non-null array");
    }

    auto owner = std::make_shared<AdoptedArrayOwner>(*array);
    auto imported = import_table_impl(owner->array, schema, owner);
    if (!imported) {
        return std::unexpected(imported.error());
    }

    owner->owns = true;
    clear_array(array);
    return imported;
}

}  // namespace ibex::interop
// NOLINTEND(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
