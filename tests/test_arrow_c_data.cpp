#include <ibex/interop/arrow_c_data.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <interpreter_internal.hpp>
#include <limits>
#include <memory>
#include <robin_hood.h>
#include <string>
#include <utility>

namespace {

auto read_i32_le(const char* p) -> std::int32_t {
    return static_cast<std::int32_t>(static_cast<unsigned char>(p[0])) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[1])) << 8) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[2])) << 16) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[3])) << 24);
}

auto decode_metadata(const char* metadata) -> robin_hood::unordered_map<std::string, std::string> {
    robin_hood::unordered_map<std::string, std::string> out;
    if (metadata == nullptr) {
        return out;
    }
    const char* p = metadata;
    const auto pairs = read_i32_le(p);
    p += 4;
    for (std::int32_t i = 0; i < pairs; ++i) {
        const auto key_len = read_i32_le(p);
        p += 4;
        std::string key(p, p + key_len);
        p += key_len;
        const auto value_len = read_i32_le(p);
        p += 4;
        std::string value(p, p + value_len);
        p += value_len;
        out.emplace(std::move(key), std::move(value));
    }
    return out;
}

auto string_at(const ArrowArray& array, std::int64_t index) -> std::string {
    const auto* offsets = static_cast<const std::uint32_t*>(array.buffers[1]);
    const auto* chars = static_cast<const char*>(array.buffers[2]);
    const auto start = static_cast<std::size_t>(offsets[index]);
    const auto end = static_cast<std::size_t>(offsets[index + 1]);
    return std::string(chars + start, chars + end);
}

/// The zone of a table's first column, as a name.
auto zone_of(const ibex::runtime::Table& table, std::size_t col = 0) -> std::optional<std::string> {
    const auto& zone =
        std::get<ibex::Column<ibex::Timestamp>>(*table.columns[col].column).meta().zone;
    if (!zone.has_value()) {
        return std::nullopt;
    }
    return ibex::zone_name(*zone);
}

struct FakeArrowStreamState {
    bool released = false;
};

struct FakeArrowArrayState {
    bool released = false;
};

struct FakeArrowSchemaState {
    bool released = false;
};

auto release_fake_array(ArrowArray* array) -> void {
    auto* state = static_cast<FakeArrowArrayState*>(array->private_data);
    if (state != nullptr) {
        state->released = true;
    }
    array->release = nullptr;
}

auto release_fake_schema(ArrowSchema* schema) -> void {
    auto* state = static_cast<FakeArrowSchemaState*>(schema->private_data);
    if (state != nullptr) {
        state->released = true;
    }
    schema->release = nullptr;
}

auto release_fake_stream(ArrowArrayStream* stream) -> void {
    auto* state = static_cast<FakeArrowStreamState*>(stream->private_data);
    if (state != nullptr) {
        state->released = true;
    }
    stream->release = nullptr;
}

}  // namespace

TEST_CASE("Arrow C Data export preserves zero-copy buffers and table metadata",
          "[interop][arrow]") {
    ibex::runtime::Table table;
    auto ids = ibex::Column<std::int64_t>{10, 20, 30};
    auto flags = ibex::Column<bool>{true, false, true};
    auto names = ibex::Column<std::string>{"alpha", "beta", "gamma"};
    auto dates = ibex::Column<ibex::Date>{{ibex::Date{1}, ibex::Date{2}, ibex::Date{3}}};
    auto ts = ibex::Column<ibex::Timestamp>{
        {ibex::Timestamp{100}, ibex::Timestamp{200}, ibex::Timestamp{300}}};

    table.add_column("id", ids);
    table.add_column("flag", flags);
    table.add_column("name", names);
    table.add_column("trade_date", dates);
    table.add_column("ts", ts);
    table.set_properties(ibex::runtime::TableProperties::recovered(
        std::vector<ibex::ir::OrderKey>{{"ts", true}, {"id", false}}, "ts", {}));

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(table, &array, &schema);
    REQUIRE(exported.has_value());

    REQUIRE(std::string(schema.format) == "+s");
    REQUIRE(array.length == 3);
    REQUIRE(schema.n_children == 5);
    REQUIRE(array.n_children == 5);

    const auto metadata = decode_metadata(schema.metadata);
    REQUIRE(metadata.at("ibex.time_index") == "ts");
    REQUIRE(metadata.at("ibex.ordering") == "ts:asc,id:desc");

    REQUIRE(std::string(schema.children[0]->name) == "id");
    REQUIRE(std::string(schema.children[0]->format) == "l");

    const auto* id_col = std::get_if<ibex::Column<std::int64_t>>(table.find("id"));
    const auto* flag_col = std::get_if<ibex::Column<bool>>(table.find("flag"));
    const auto* name_col = std::get_if<ibex::Column<std::string>>(table.find("name"));
    const auto* date_col = std::get_if<ibex::Column<ibex::Date>>(table.find("trade_date"));
    const auto* ts_col = std::get_if<ibex::Column<ibex::Timestamp>>(table.find("ts"));
    REQUIRE(id_col != nullptr);
    REQUIRE(flag_col != nullptr);
    REQUIRE(name_col != nullptr);
    REQUIRE(date_col != nullptr);
    REQUIRE(ts_col != nullptr);

    REQUIRE(std::string(schema.children[1]->format) == "b");
    REQUIRE(std::string(schema.children[2]->format) == "u");
    REQUIRE(std::string(schema.children[3]->format) == "tdD");
    REQUIRE(std::string(schema.children[4]->format) == "tsn:");

    REQUIRE(array.children[0]->buffers[1] == static_cast<const void*>(id_col->data()));
    REQUIRE(array.children[1]->buffers[1] == static_cast<const void*>(flag_col->words_data()));
    REQUIRE(array.children[2]->buffers[1] == static_cast<const void*>(name_col->offsets_data()));
    REQUIRE(array.children[2]->buffers[2] == static_cast<const void*>(name_col->chars_data()));
    REQUIRE(array.children[3]->buffers[1] == static_cast<const void*>(date_col->data()));
    REQUIRE(array.children[4]->buffers[1] == static_cast<const void*>(ts_col->data()));

    schema.release(&schema);
    array.release(&array);
    REQUIRE(schema.release == nullptr);
    REQUIRE(array.release == nullptr);
}

TEST_CASE("Arrow C Data export maps categoricals to dictionary arrays", "[interop][arrow]") {
    ibex::runtime::Table table;
    ibex::Column<ibex::Categorical> cat;
    cat.push_back("AAPL");
    cat.push_back("MSFT");
    cat.push_back("AAPL");
    table.add_column("symbol", std::move(cat));

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(table, &array, &schema);
    REQUIRE(exported.has_value());

    REQUIRE(schema.n_children == 1);
    REQUIRE(std::string(schema.children[0]->format) == "i");
    REQUIRE(schema.children[0]->dictionary != nullptr);
    REQUIRE(std::string(schema.children[0]->dictionary->format) == "u");

    const ArrowArray& child = *array.children[0];
    REQUIRE(child.dictionary != nullptr);
    REQUIRE(child.length == 3);
    REQUIRE(child.dictionary->length == 2);

    REQUIRE(string_at(*child.dictionary, 0) == "AAPL");
    REQUIRE(string_at(*child.dictionary, 1) == "MSFT");

    const auto* cat_col = std::get_if<ibex::Column<ibex::Categorical>>(table.find("symbol"));
    REQUIRE(cat_col != nullptr);
    REQUIRE(child.buffers[1] == static_cast<const void*>(cat_col->codes_data()));

    schema.release(&schema);
    array.release(&array);
}

TEST_CASE("Arrow C Data import round-trips values, validity, and table metadata",
          "[interop][arrow]") {
    ibex::runtime::Table table;
    table.add_column("id", ibex::Column<std::int64_t>{10, 20, 30});

    ibex::Column<std::string> names{"alpha", "", "gamma"};
    ibex::runtime::ValidityBitmap name_validity{true, false, true};
    table.add_column("name", std::move(names), std::move(name_validity));

    ibex::Column<bool> flags{true, false, true};
    ibex::runtime::ValidityBitmap flag_validity{true, true, false};
    table.add_column("flag", std::move(flags), std::move(flag_validity));

    table.add_column("trade_date",
                     ibex::Column<ibex::Date>{{ibex::Date{1}, ibex::Date{2}, ibex::Date{3}}});
    table.add_column("ts", ibex::Column<ibex::Timestamp>{
                               {ibex::Timestamp{100}, ibex::Timestamp{200}, ibex::Timestamp{300}}});
    table.set_properties(ibex::runtime::TableProperties::recovered(
        std::vector<ibex::ir::OrderKey>{{"ts", true}, {"id", false}}, "ts", {}));

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(table, &array, &schema);
    REQUIRE(exported.has_value());

    auto imported = ibex::interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());

    REQUIRE(imported->time_index() == table.time_index());
    REQUIRE(imported->ordering().has_value());
    REQUIRE(table.ordering().has_value());
    REQUIRE(imported->ordering()->size() == table.ordering()->size());
    for (std::size_t i = 0; i < imported->ordering()->size(); ++i) {
        CHECK((*imported->ordering())[i].name == (*table.ordering())[i].name);
        CHECK((*imported->ordering())[i].ascending == (*table.ordering())[i].ascending);
    }

    const auto* ids = std::get_if<ibex::Column<std::int64_t>>(imported->find("id"));
    const auto* imported_names = std::get_if<ibex::Column<std::string>>(imported->find("name"));
    const auto* imported_flags = std::get_if<ibex::Column<bool>>(imported->find("flag"));
    REQUIRE(ids != nullptr);
    REQUIRE(imported_names != nullptr);
    REQUIRE(imported_flags != nullptr);

    REQUIRE((*ids)[0] == 10);
    REQUIRE((*ids)[1] == 20);
    REQUIRE((*imported_names)[0] == "alpha");
    REQUIRE((*imported_names)[1] == "");
    REQUIRE((*imported_names)[2] == "gamma");
    REQUIRE((*imported_flags)[0]);
    REQUIRE(!(*imported_flags)[1]);
    REQUIRE((*imported_flags)[2]);

    const auto* imported_name_entry = imported->find_entry("name");
    const auto* imported_flag_entry = imported->find_entry("flag");
    REQUIRE(imported_name_entry != nullptr);
    REQUIRE(imported_flag_entry != nullptr);
    REQUIRE(imported_name_entry->validity.has_value());
    REQUIRE(imported_flag_entry->validity.has_value());
    CHECK((*imported_name_entry->validity)[0]);
    CHECK(!(*imported_name_entry->validity)[1]);
    CHECK((*imported_name_entry->validity)[2]);
    CHECK((*imported_flag_entry->validity)[0]);
    CHECK((*imported_flag_entry->validity)[1]);
    CHECK(!(*imported_flag_entry->validity)[2]);

    schema.release(&schema);
    array.release(&array);
}

// ── Narrow integers ─────────────────────────────────────────────────────────
//
// Arrow spells int32 and the usual dictionary INDEX type identically ("i"), and
// only the presence of dictionary storage tells them apart. Reading "i" as
// "always categorical" made a plain int32 column -- an ordinary R integer among
// others -- fail with a complaint about missing dictionary storage.

namespace {

/// A released Arrow object is one whose callback is null, so a hand-built array
/// needs a no-op rather than nothing: the storage belongs to the fixture.
inline void noop_release_array(ArrowArray* array) {
    array->release = nullptr;
}
inline void noop_release_schema(ArrowSchema* schema) {
    schema->release = nullptr;
}

/// A one-column struct array over caller-owned integer storage. Hand-built
/// because Ibex has no narrow integer column to export from.
template <typename Raw>
struct ForeignIntColumn {
    std::vector<Raw> values;
    std::array<const void*, 2> child_buffers{};
    std::array<ArrowArray*, 1> children{};
    std::array<ArrowSchema*, 1> schema_children{};
    ArrowArray child{};
    ArrowArray array{};
    ArrowSchema child_schema{};
    ArrowSchema schema{};

    ForeignIntColumn(std::initializer_list<Raw> init, const char* format) : values(init) {
        child_buffers = {nullptr, values.data()};
        child.length = static_cast<std::int64_t>(values.size());
        child.null_count = 0;
        child.n_buffers = 2;
        child.buffers = child_buffers.data();
        child.release = noop_release_array;
        children = {&child};

        array.length = child.length;
        array.n_children = 1;
        array.children = children.data();
        array.release = noop_release_array;

        child_schema.format = format;
        child_schema.name = "n";
        child_schema.release = noop_release_schema;
        schema_children = {&child_schema};

        schema.format = "+s";
        schema.n_children = 1;
        schema.children = schema_children.data();
        schema.release = noop_release_schema;
    }
};

}  // namespace

TEST_CASE("Arrow C Data imports a plain int32 column", "[interop][arrow][int]") {
    ForeignIntColumn<std::int32_t> source({7, -3, 0}, "i");

    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());

    const auto* values = std::get_if<ibex::Column<std::int64_t>>(imported->find("n"));
    REQUIRE(values != nullptr);
    CHECK((*values)[0] == 7);
    CHECK((*values)[1] == -3);
    CHECK((*values)[2] == 0);
}

TEST_CASE("Arrow C Data widens every lossless integer width", "[interop][arrow][int]") {
    SECTION("int8") {
        ForeignIntColumn<std::int8_t> source({-128, 127}, "c");
        auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
        REQUIRE(imported.has_value());
        const auto* values = std::get_if<ibex::Column<std::int64_t>>(imported->find("n"));
        REQUIRE(values != nullptr);
        CHECK((*values)[0] == -128);
        CHECK((*values)[1] == 127);
    }

    SECTION("uint32 uses its full range") {
        // The value that would be negative if the width were read as signed.
        ForeignIntColumn<std::uint32_t> source({4'294'967'295U}, "I");
        auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
        REQUIRE(imported.has_value());
        const auto* values = std::get_if<ibex::Column<std::int64_t>>(imported->find("n"));
        REQUIRE(values != nullptr);
        CHECK((*values)[0] == 4'294'967'295LL);
    }
}

TEST_CASE("Arrow C Data import round-trips dictionary encoded categoricals", "[interop][arrow]") {
    ibex::runtime::Table table;
    ibex::Column<ibex::Categorical> cat;
    cat.push_back("AAPL");
    cat.push_back("MSFT");
    cat.push_back("AAPL");
    table.add_column("symbol", std::move(cat));

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(table, &array, &schema);
    REQUIRE(exported.has_value());

    auto imported = ibex::interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());

    const auto* symbols = std::get_if<ibex::Column<ibex::Categorical>>(imported->find("symbol"));
    REQUIRE(symbols != nullptr);
    REQUIRE(symbols->dictionary().size() == 2);
    REQUIRE(symbols->dictionary()[0] == "AAPL");
    REQUIRE(symbols->dictionary()[1] == "MSFT");
    REQUIRE((*symbols)[0] == "AAPL");
    REQUIRE((*symbols)[1] == "MSFT");
    REQUIRE((*symbols)[2] == "AAPL");

    schema.release(&schema);
    array.release(&array);
}

TEST_CASE("Arrow C Data adoption keeps sliced primitive buffers zero-copy until mutation",
          "[interop][arrow][adopt]") {
    ibex::runtime::Table source;
    source.add_column("id", ibex::Column<std::int64_t>{10, 20, 30, 40},
                      ibex::runtime::ValidityBitmap{true, false, true, true});

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(source, &array, &schema);
    REQUIRE(exported.has_value());

    const auto* exported_values = static_cast<const std::int64_t*>(array.children[0]->buffers[1]);
    const auto* exported_validity = static_cast<const std::uint8_t*>(array.children[0]->buffers[0]);
    array.length = 2;
    array.children[0]->length = 2;
    array.children[0]->offset = 1;

    auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
    REQUIRE(imported.has_value());
    REQUIRE(array.release == nullptr);

    const auto& borrowed_table = std::as_const(*imported);
    const auto* borrowed_values =
        std::get_if<ibex::Column<std::int64_t>>(borrowed_table.find("id"));
    REQUIRE(borrowed_values != nullptr);
    REQUIRE(borrowed_values->is_external());
    REQUIRE(borrowed_values->data() == exported_values + 1);
    REQUIRE((*borrowed_values)[0] == 20);
    REQUIRE((*borrowed_values)[1] == 30);
    const auto* borrowed_entry = borrowed_table.find_entry("id");
    REQUIRE(borrowed_entry != nullptr);
    REQUIRE(borrowed_entry->validity.has_value());
    REQUIRE(borrowed_entry->validity->is_external());
    REQUIRE(borrowed_entry->validity->buffer_data() == exported_validity);
    REQUIRE(borrowed_entry->validity->buffer_offset() == 1);
    REQUIRE_FALSE((*borrowed_entry->validity)[0]);
    REQUIRE((*borrowed_entry->validity)[1]);

    ArrowArray roundtrip_array{};
    ArrowSchema roundtrip_schema{};
    auto roundtrip_export =
        ibex::interop::export_table_to_arrow(borrowed_table, &roundtrip_array, &roundtrip_schema);
    REQUIRE(roundtrip_export.has_value());
    REQUIRE(roundtrip_array.children[0]->offset == 1);
    REQUIRE(roundtrip_array.children[0]->buffers[0] == exported_validity);
    REQUIRE(roundtrip_array.children[0]->buffers[1] == exported_values);
    roundtrip_schema.release(&roundtrip_schema);
    roundtrip_array.release(&roundtrip_array);

    auto& mutable_values = std::get<ibex::Column<std::int64_t>>(imported->mutable_column(0));
    mutable_values[0] = 200;

    REQUIRE_FALSE(mutable_values.is_external());
    REQUIRE(std::as_const(mutable_values).data() != exported_values + 1);
    REQUIRE(std::as_const(mutable_values)[0] == 200);
    REQUIRE(exported_values[1] == 20);

    auto& mutable_validity = *imported->columns[0].validity;
    mutable_validity.set(0, true);
    REQUIRE_FALSE(mutable_validity.is_external());
    REQUIRE(mutable_validity[0]);
    REQUIRE_FALSE((source.columns[0].validity.value())[1]);

    schema.release(&schema);
}

TEST_CASE("Arrow C Data adoption keeps sliced temporal buffers zero-copy until mutation",
          "[interop][arrow][adopt]") {
    ibex::runtime::Table source;
    const ibex::runtime::ValidityBitmap validity{true, false, true, true};
    source.add_column(
        "day",
        ibex::Column<ibex::Date>{ibex::Date{10}, ibex::Date{11}, ibex::Date{12}, ibex::Date{13}},
        validity);
    source.add_column("ts",
                      ibex::Column<ibex::Timestamp>{ibex::Timestamp{100}, ibex::Timestamp{200},
                                                    ibex::Timestamp{300}, ibex::Timestamp{400}},
                      validity);

    ArrowArray array{};
    ArrowSchema schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());

    const auto* date_values = static_cast<const ibex::Date*>(array.children[0]->buffers[1]);
    const auto* timestamp_values =
        static_cast<const ibex::Timestamp*>(array.children[1]->buffers[1]);

    array.length = 2;
    array.offset = 1;

    auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
    REQUIRE(imported.has_value());
    REQUIRE(array.release == nullptr);

    const auto& borrowed = std::as_const(*imported);
    const auto* dates = std::get_if<ibex::Column<ibex::Date>>(borrowed.find("day"));
    const auto* timestamps = std::get_if<ibex::Column<ibex::Timestamp>>(borrowed.find("ts"));
    REQUIRE(dates != nullptr);
    REQUIRE(timestamps != nullptr);

    REQUIRE(dates->is_external());
    REQUIRE(dates->buffer_data() == date_values);
    REQUIRE(dates->buffer_offset() == 1);
    CHECK((*dates)[0] == ibex::Date{11});
    CHECK((*dates)[1] == ibex::Date{12});

    REQUIRE(timestamps->is_external());
    REQUIRE(timestamps->buffer_data() == timestamp_values);
    REQUIRE(timestamps->buffer_offset() == 1);
    CHECK((*timestamps)[0] == ibex::Timestamp{200});
    CHECK((*timestamps)[1] == ibex::Timestamp{300});

    for (const auto& entry : borrowed.columns) {
        REQUIRE(entry.validity.has_value());
        REQUIRE(entry.validity->is_external());
        REQUIRE(entry.validity->buffer_offset() == 1);
    }

    ArrowArray roundtrip_array{};
    ArrowSchema roundtrip_schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(borrowed, &roundtrip_array, &roundtrip_schema)
                .has_value());
    REQUIRE(roundtrip_array.children[0]->offset == 1);
    REQUIRE(roundtrip_array.children[0]->buffers[1] == date_values);
    REQUIRE(roundtrip_array.children[1]->offset == 1);
    REQUIRE(roundtrip_array.children[1]->buffers[1] == timestamp_values);
    roundtrip_schema.release(&roundtrip_schema);
    roundtrip_array.release(&roundtrip_array);

    auto& mutable_dates = std::get<ibex::Column<ibex::Date>>(imported->mutable_column(0));
    mutable_dates[0] = ibex::Date{99};
    REQUIRE_FALSE(mutable_dates.is_external());
    CHECK(std::as_const(mutable_dates)[0] == ibex::Date{99});
    CHECK(date_values[1] == ibex::Date{11});

    auto& mutable_timestamps = std::get<ibex::Column<ibex::Timestamp>>(imported->mutable_column(1));
    mutable_timestamps.push_back(ibex::Timestamp{500});
    REQUIRE_FALSE(mutable_timestamps.is_external());
    CHECK(std::as_const(mutable_timestamps)[0] == ibex::Timestamp{200});
    CHECK(std::as_const(mutable_timestamps)[2] == ibex::Timestamp{500});
    CHECK(timestamp_values[1] == ibex::Timestamp{200});

    schema.release(&schema);
}

TEST_CASE("Arrow C Data adoption keeps sliced bool, utf8, and categorical buffers zero-copy",
          "[interop][arrow][adopt]") {
    ibex::runtime::Table source;
    const ibex::runtime::ValidityBitmap validity{true, false, true, true};
    source.add_column("flag", ibex::Column<bool>{false, true, false, true}, validity);
    source.add_column("name", ibex::Column<std::string>{"zero", "one", "two", "three"}, validity);

    ibex::Column<ibex::Categorical> symbols;
    symbols.push_back("A");
    symbols.push_back("B");
    symbols.push_back("A");
    symbols.push_back("C");
    source.add_column("symbol", std::move(symbols), validity);

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(source, &array, &schema);
    REQUIRE(exported.has_value());

    const auto* flag_bits = static_cast<const std::uint8_t*>(array.children[0]->buffers[1]);
    const auto* name_offsets = static_cast<const std::uint32_t*>(array.children[1]->buffers[1]);
    const auto* name_chars = static_cast<const char*>(array.children[1]->buffers[2]);
    const auto* symbol_codes = static_cast<const ibex::Column<ibex::Categorical>::code_type*>(
        array.children[2]->buffers[1]);
    const auto* dictionary_offsets =
        static_cast<const std::uint32_t*>(array.children[2]->dictionary->buffers[1]);
    const auto* dictionary_chars =
        static_cast<const char*>(array.children[2]->dictionary->buffers[2]);

    array.length = 2;
    array.offset = 1;

    auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
    REQUIRE(imported.has_value());
    REQUIRE(array.release == nullptr);

    const auto& borrowed = std::as_const(*imported);
    const auto* flags = std::get_if<ibex::Column<bool>>(borrowed.find("flag"));
    const auto* names = std::get_if<ibex::Column<std::string>>(borrowed.find("name"));
    const auto* cats = std::get_if<ibex::Column<ibex::Categorical>>(borrowed.find("symbol"));
    REQUIRE(flags != nullptr);
    REQUIRE(names != nullptr);
    REQUIRE(cats != nullptr);

    REQUIRE(flags->is_external());
    REQUIRE(flags->buffer_data() == flag_bits);
    REQUIRE(flags->buffer_offset() == 1);
    CHECK((*flags)[0]);
    CHECK_FALSE((*flags)[1]);

    REQUIRE(names->is_external());
    REQUIRE(names->offsets_buffer_data() == name_offsets);
    REQUIRE(names->chars_buffer_data() == name_chars);
    REQUIRE(names->buffer_offset() == 1);
    CHECK((*names)[0] == "one");
    CHECK((*names)[1] == "two");

    REQUIRE(cats->codes_are_external());
    REQUIRE(cats->dictionary_is_external());
    REQUIRE(cats->codes_buffer_data() == symbol_codes);
    REQUIRE(cats->buffer_offset() == 1);
    REQUIRE(cats->dictionary_offsets_buffer_data() == dictionary_offsets);
    REQUIRE(cats->dictionary_chars_buffer_data() == dictionary_chars);
    CHECK((*cats)[0] == "B");
    CHECK((*cats)[1] == "A");

    for (const auto& entry : borrowed.columns) {
        REQUIRE(entry.validity.has_value());
        REQUIRE(entry.validity->is_external());
        REQUIRE(entry.validity->buffer_offset() == 1);
    }

    ArrowArray roundtrip_array{};
    ArrowSchema roundtrip_schema{};
    auto roundtrip =
        ibex::interop::export_table_to_arrow(borrowed, &roundtrip_array, &roundtrip_schema);
    REQUIRE(roundtrip.has_value());
    REQUIRE(roundtrip_array.children[0]->offset == 1);
    REQUIRE(roundtrip_array.children[0]->buffers[1] == flag_bits);
    REQUIRE(roundtrip_array.children[1]->offset == 1);
    REQUIRE(roundtrip_array.children[1]->buffers[1] == name_offsets);
    REQUIRE(roundtrip_array.children[1]->buffers[2] == name_chars);
    REQUIRE(roundtrip_array.children[2]->offset == 1);
    REQUIRE(roundtrip_array.children[2]->buffers[1] == symbol_codes);
    REQUIRE(roundtrip_array.children[2]->dictionary->buffers[1] == dictionary_offsets);
    REQUIRE(roundtrip_array.children[2]->dictionary->buffers[2] == dictionary_chars);
    roundtrip_schema.release(&roundtrip_schema);
    roundtrip_array.release(&roundtrip_array);

    auto& mutable_flags = std::get<ibex::Column<bool>>(imported->mutable_column(0));
    mutable_flags.set(0, false);
    REQUIRE_FALSE(mutable_flags.is_external());
    CHECK_FALSE(std::as_const(mutable_flags)[0]);
    CHECK((flag_bits[0] & 0x02U) != 0U);

    auto& mutable_names = std::get<ibex::Column<std::string>>(imported->mutable_column(1));
    mutable_names.push_back("tail");
    REQUIRE_FALSE(mutable_names.is_external());
    CHECK(std::as_const(mutable_names)[0] == "one");
    CHECK(std::as_const(mutable_names)[2] == "tail");
    CHECK(name_offsets[1] == 4);

    auto& mutable_cats = std::get<ibex::Column<ibex::Categorical>>(imported->mutable_column(2));
    mutable_cats.push_code(1);
    REQUIRE_FALSE(mutable_cats.codes_are_external());
    REQUIRE(mutable_cats.dictionary_is_external());
    mutable_cats.push_back("NEW");
    REQUIRE_FALSE(mutable_cats.is_external());
    CHECK(std::as_const(mutable_cats)[0] == "B");
    CHECK(std::as_const(mutable_cats)[3] == "NEW");
    CHECK(symbol_codes[1] == 1);

    schema.release(&schema);
}

TEST_CASE("Arrow C Data adoption accepts empty and zero-byte value buffers",
          "[interop][arrow][adopt]") {
    SECTION("zero rows") {
        ibex::runtime::Table source;
        source.add_column("flag", ibex::Column<bool>{});
        source.add_column("day", ibex::Column<ibex::Date>{});
        source.add_column("ts", ibex::Column<ibex::Timestamp>{});
        source.add_column("name", ibex::Column<std::string>{});
        source.add_column("symbol", ibex::Column<ibex::Categorical>{});

        ArrowArray array{};
        ArrowSchema schema{};
        REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());

        auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
        REQUIRE(imported.has_value());
        REQUIRE(imported->rows() == 0);
        REQUIRE(array.release == nullptr);
        schema.release(&schema);
    }

    SECTION("non-empty columns whose string payload buffers have zero bytes") {
        ibex::runtime::Table source;
        source.add_column("flag", ibex::Column<bool>{false, true});
        source.add_column("name", ibex::Column<std::string>{"", ""});
        ibex::Column<ibex::Categorical> symbols;
        symbols.push_back("");
        symbols.push_back("");
        source.add_column("symbol", std::move(symbols));

        ArrowArray array{};
        ArrowSchema schema{};
        REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());

        auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
        REQUIRE(imported.has_value());
        const auto& borrowed = std::as_const(*imported);
        const auto* names = std::get_if<ibex::Column<std::string>>(borrowed.find("name"));
        const auto* cats = std::get_if<ibex::Column<ibex::Categorical>>(borrowed.find("symbol"));
        REQUIRE(names != nullptr);
        REQUIRE(cats != nullptr);
        CHECK((*names)[0].empty());
        CHECK((*names)[1].empty());
        CHECK((*cats)[0].empty());
        CHECK((*cats)[1].empty());
        REQUIRE(array.release == nullptr);
        schema.release(&schema);
    }
}

TEST_CASE("Arrow C Data adoption retains and releases producer ownership",
          "[interop][arrow][adopt]") {
    auto source = std::make_shared<ibex::runtime::Table>();
    source->add_column("value", ibex::Column<double>{1.5, 2.5});
    std::weak_ptr<ibex::runtime::Table> source_lifetime = source;

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(
        std::static_pointer_cast<const ibex::runtime::Table>(source), &array, &schema);
    REQUIRE(exported.has_value());
    source.reset();

    {
        auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
        REQUIRE(imported.has_value());
        REQUIRE_FALSE(source_lifetime.expired());
    }

    REQUIRE(source_lifetime.expired());
    schema.release(&schema);
}

TEST_CASE("Arrow C Data adoption ignores dictionary indices in null slots",
          "[interop][arrow][adopt][categorical]") {
    ibex::runtime::Table source;
    ibex::Column<ibex::Categorical> symbols;
    symbols.push_back("A");
    symbols.push_back("B");
    symbols.push_back("A");
    source.add_column("symbol", std::move(symbols),
                      ibex::runtime::ValidityBitmap{true, false, true});

    ArrowArray array{};
    ArrowSchema schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());

    auto* codes = const_cast<ibex::Column<ibex::Categorical>::code_type*>(
        static_cast<const ibex::Column<ibex::Categorical>::code_type*>(
            array.children[0]->buffers[1]));
    codes[1] = std::numeric_limits<ibex::Column<ibex::Categorical>::code_type>::min();
    const auto* source_codes = codes;

    auto imported = ibex::interop::adopt_table_from_arrow(&array, schema);
    REQUIRE(imported.has_value());
    const auto& borrowed = std::as_const(*imported);
    const auto* categorical = std::get_if<ibex::Column<ibex::Categorical>>(borrowed.find("symbol"));
    REQUIRE(categorical != nullptr);
    REQUIRE(categorical->codes_are_external());
    CHECK(categorical->codes_buffer_data() == source_codes);
    REQUIRE(borrowed.columns[0].validity.has_value());
    CHECK_FALSE((*borrowed.columns[0].validity)[1]);

    schema.release(&schema);
}

TEST_CASE("Arrow C Data adoption leaves ownership with caller on failure",
          "[interop][arrow][adopt]") {
    ibex::runtime::Table source;
    source.add_column("id", ibex::Column<std::int64_t>{10, 20});

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(source, &array, &schema);
    REQUIRE(exported.has_value());

    ArrowSchema invalid_schema = schema;
    invalid_schema.format = "l";
    auto imported = ibex::interop::adopt_table_from_arrow(&array, invalid_schema);

    REQUIRE_FALSE(imported.has_value());
    REQUIRE(array.release != nullptr);

    schema.release(&schema);
    array.release(&array);
}

TEST_CASE("Arrow C stream release clears callbacks and calls producer release",
          "[interop][arrow]") {
    FakeArrowStreamState state{};
    ArrowArrayStream stream{};
    stream.release = release_fake_stream;
    stream.private_data = &state;

    ibex::interop::release_arrow_stream(&stream);

    REQUIRE(state.released);
    REQUIRE(stream.release == nullptr);
    REQUIRE(stream.get_schema == nullptr);
    REQUIRE(stream.get_next == nullptr);
    REQUIRE(stream.get_last_error == nullptr);
    REQUIRE(stream.private_data == nullptr);
}

TEST_CASE("Arrow C release wrappers handle foreign arrays and schemas", "[interop][arrow]") {
    FakeArrowArrayState array_state{};
    ArrowArray array{};
    array.release = release_fake_array;
    array.private_data = &array_state;

    FakeArrowSchemaState schema_state{};
    ArrowSchema schema{};
    schema.release = release_fake_schema;
    schema.private_data = &schema_state;

    ibex::interop::release_arrow_array(&array);
    ibex::interop::release_arrow_schema(&schema);

    REQUIRE(array_state.released);
    REQUIRE(schema_state.released);
    REQUIRE(array.release == nullptr);
    REQUIRE(schema.release == nullptr);
    REQUIRE(array.private_data == nullptr);
    REQUIRE(schema.private_data == nullptr);
}

// ── Timestamp resolutions ───────────────────────────────────────────────────
//
// Arrow timestamps come in four resolutions and may carry an IANA zone, so the
// format string is `ts{s|m|u|n}:{zone}`. Ibex stores an instant in nanoseconds
// (SPEC 2.4), so every resolution is accepted and rescaled, and the zone is
// dropped -- an Arrow value is UTC-relative whenever a zone is present, so the
// instant survives. R is the motivating producer: it emits `tsu:UTC` or
// `tsu:America/New_York` for a POSIXct and nothing else.

namespace {

/// Export one Timestamp column, then relabel its Arrow type. The exported
/// schema owns its state through `private_data`; `format` is only ever read, so
/// pointing it at a literal is safe until release.
struct RelabelledTimestamps {
    ArrowArray array{};
    ArrowSchema schema{};

    RelabelledTimestamps(std::initializer_list<ibex::Timestamp> values, const char* format,
                         const ibex::runtime::ValidityBitmap* validity = nullptr) {
        ibex::runtime::Table source;
        if (validity != nullptr) {
            source.add_column("ts", ibex::Column<ibex::Timestamp>{values}, *validity);
        } else {
            source.add_column("ts", ibex::Column<ibex::Timestamp>{values});
        }
        REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());
        schema.children[0]->format = format;
    }

    ~RelabelledTimestamps() {
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
        if (array.release != nullptr) {
            array.release(&array);
        }
    }

    RelabelledTimestamps(const RelabelledTimestamps&) = delete;
    auto operator=(const RelabelledTimestamps&) -> RelabelledTimestamps& = delete;
};

auto imported_nanos(const ibex::runtime::Table& table, std::size_t index) -> std::int64_t {
    const auto* column = std::get_if<ibex::Column<ibex::Timestamp>>(table.find("ts"));
    REQUIRE(column != nullptr);
    return (*column)[index].nanos;
}

}  // namespace

TEST_CASE("Arrow C Data import rescales every timestamp resolution to nanoseconds",
          "[interop][arrow][timestamp]") {
    SECTION("seconds") {
        RelabelledTimestamps source({ibex::Timestamp{1}, ibex::Timestamp{-2}}, "tss:");
        auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
        REQUIRE(imported.has_value());
        CHECK(imported_nanos(*imported, 0) == 1'000'000'000);
        CHECK(imported_nanos(*imported, 1) == -2'000'000'000);
    }

    SECTION("milliseconds") {
        RelabelledTimestamps source({ibex::Timestamp{1}}, "tsm:");
        auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
        REQUIRE(imported.has_value());
        CHECK(imported_nanos(*imported, 0) == 1'000'000);
    }

    SECTION("microseconds") {
        RelabelledTimestamps source({ibex::Timestamp{1}}, "tsu:");
        auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
        REQUIRE(imported.has_value());
        CHECK(imported_nanos(*imported, 0) == 1'000);
    }

    SECTION("nanoseconds pass through") {
        RelabelledTimestamps source({ibex::Timestamp{1}}, "tsn:");
        auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
        REQUIRE(imported.has_value());
        CHECK(imported_nanos(*imported, 0) == 1);
    }
}

TEST_CASE("Arrow C Data import keeps the instant of a zoned timestamp",
          "[interop][arrow][timestamp]") {
    // An Arrow timestamp with a zone is already UTC-relative, so the zone is
    // metadata about rendering and the instant needs no adjustment. This is
    // exactly the type R hands over for a POSIXct.
    RelabelledTimestamps source({ibex::Timestamp{1'357'000'000'000'000}}, "tsu:America/New_York");
    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());
    CHECK(imported_nanos(*imported, 0) == 1'357'000'000'000'000'000);
}

TEST_CASE("Arrow C Data adoption is zero-copy only at nanosecond resolution",
          "[interop][arrow][timestamp][adopt]") {
    SECTION("nanoseconds adopt the producer buffer") {
        RelabelledTimestamps source({ibex::Timestamp{7}}, "tsn:");
        auto imported = ibex::interop::adopt_table_from_arrow(&source.array, source.schema);
        REQUIRE(imported.has_value());
        const auto* column =
            std::get_if<ibex::Column<ibex::Timestamp>>(std::as_const(*imported).find("ts"));
        REQUIRE(column != nullptr);
        CHECK(column->is_external());
    }

    SECTION("a rescaled resolution must materialize") {
        RelabelledTimestamps source({ibex::Timestamp{7}}, "tsu:");
        auto imported = ibex::interop::adopt_table_from_arrow(&source.array, source.schema);
        REQUIRE(imported.has_value());
        const auto* column =
            std::get_if<ibex::Column<ibex::Timestamp>>(std::as_const(*imported).find("ts"));
        REQUIRE(column != nullptr);
        CHECK_FALSE(column->is_external());
        CHECK((*column)[0].nanos == 7'000);
    }
}

TEST_CASE("Arrow C Data import rejects a timestamp that cannot be held in nanoseconds",
          "[interop][arrow][timestamp]") {
    RelabelledTimestamps source({ibex::Timestamp{std::numeric_limits<std::int64_t>::max() / 2}},
                                "tss:");
    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE_FALSE(imported.has_value());
    CHECK(imported.error().find("nanoseconds") != std::string::npos);
}

TEST_CASE("Arrow C Data import ignores payload bytes in null timestamp slots",
          "[interop][arrow][timestamp]") {
    // Arrow leaves the payload at a null position unspecified. A rescaling
    // import must not range-check that garbage, or a valid array with a null in
    // it would be rejected.
    const ibex::runtime::ValidityBitmap validity{true, false};
    RelabelledTimestamps source(
        {ibex::Timestamp{1}, ibex::Timestamp{std::numeric_limits<std::int64_t>::max()}},
        "tss:", &validity);

    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());
    CHECK(imported_nanos(*imported, 0) == 1'000'000'000);
    REQUIRE(imported->columns[0].validity.has_value());
    CHECK_FALSE((*imported->columns[0].validity)[1]);
}

TEST_CASE("Arrow C Data round-trips a timestamp column's zone", "[interop][arrow][timestamp]") {
    // The zone is what tells a reader which wall clock to render the instant
    // on. Losing it hands a zoned producer back its own data relabelled UTC.
    RelabelledTimestamps source({ibex::Timestamp{1'357'000'000'000'000}}, "tsu:America/New_York");
    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());
    REQUIRE(imported->columns.size() == 1);
    CHECK(zone_of(*imported) == std::optional<std::string>{"America/New_York"});

    ArrowArray out_array{};
    ArrowSchema out_schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(*imported, &out_array, &out_schema).has_value());
    CHECK(std::string(out_schema.children[0]->format) == "tsn:America/New_York");
    out_schema.release(&out_schema);
    out_array.release(&out_array);
}

TEST_CASE("A zone-less timestamp column stays zone-less", "[interop][arrow][timestamp]") {
    RelabelledTimestamps source({ibex::Timestamp{5}}, "tsn:");
    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());
    CHECK_FALSE(zone_of(*imported).has_value());

    ArrowArray out_array{};
    ArrowSchema out_schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(*imported, &out_array, &out_schema).has_value());
    CHECK(std::string(out_schema.children[0]->format) == "tsn:");
    out_schema.release(&out_schema);
    out_array.release(&out_array);
}

TEST_CASE("A zone belongs to the column, not to the name", "[interop][arrow][timestamp]") {
    // Metadata that describes the DATA has to travel with the data. Installing
    // different storage under the same name is a different column, and it
    // carries its own (absent) zone rather than inheriting one that described
    // values it never held.
    RelabelledTimestamps source({ibex::Timestamp{5}}, "tsu:Europe/Amsterdam");
    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());
    REQUIRE(zone_of(*imported) == std::optional<std::string>{"Europe/Amsterdam"});

    imported->add_column("ts", ibex::Column<ibex::Timestamp>{ibex::Timestamp{1}});
    CHECK_FALSE(zone_of(*imported).has_value());
}

TEST_CASE("A gathered column keeps its zone", "[interop][arrow][timestamp]") {
    // The single reason this design was chosen over carrying the zone on the
    // table: every helper that builds one column out of another routes through
    // `with_meta_of`, so a row gather cannot silently strip it.
    RelabelledTimestamps source({ibex::Timestamp{5}, ibex::Timestamp{6}}, "tsu:Europe/Amsterdam");
    auto imported = ibex::interop::import_table_from_arrow(source.array, source.schema);
    REQUIRE(imported.has_value());

    const std::vector<std::size_t> idx{1};
    auto gathered = ibex::runtime::gather_rows(*imported, idx);
    CHECK(zone_of(gathered) == std::optional<std::string>{"Europe/Amsterdam"});
}

TEST_CASE("Arrow C Data round-trips the group-major claim", "[interop][arrow][properties]") {
    // `grouped_by` is the flag that stops an unpartitioned lag/rolling call from
    // reading across a group boundary. It also gates `normalize_time_index`, so
    // losing it across the boundary does two things: it disarms the guard, and
    // it lets the re-imported table have its true (group keys..., time) ordering
    // rewritten to the false "time index ascending".
    ibex::runtime::Table source;
    source.add_column("symbol", ibex::Column<std::int64_t>{1, 1, 2, 2});
    source.add_column("ts", ibex::Column<ibex::Timestamp>{ibex::Timestamp{1}, ibex::Timestamp{2},
                                                          ibex::Timestamp{1}, ibex::Timestamp{2}});
    source.set_properties(ibex::runtime::TableProperties::recovered(
        std::vector<ibex::ir::OrderKey>{{.name = "symbol", .ascending = true},
                                        {.name = "ts", .ascending = true}},
        "ts", {"symbol"}));

    ArrowArray array{};
    ArrowSchema schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());

    auto imported = ibex::interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());

    CHECK(imported->grouped_by() == std::vector<std::string>{"symbol"});
    CHECK(imported->time_index() == std::optional<std::string>{"ts"});
    REQUIRE(imported->ordering().has_value());
    REQUIRE(imported->ordering()->size() == 2);
    CHECK((*imported->ordering())[0].name == "symbol");
    CHECK((*imported->ordering())[1].name == "ts");

    schema.release(&schema);
    array.release(&array);
}

// Export reaches for a column's raw buffer — `codes_buffer_data()` for a
// categorical, `words_data()` for a bool. Both used to answer from a member
// mirrored by every mutation; they now resolve the storage in use instead, and
// a column built in Ibex (never adopted from Arrow) is the case that mirror was
// hiding. Getting it wrong ships a stale or null pointer to the consumer, which
// is silent corruption rather than a crash — and nothing covered it.
TEST_CASE("Owned categorical and bool columns export their own buffers",
          "[interop][arrow][export]") {
    ibex::runtime::Table source;
    ibex::Column<ibex::Categorical> tags;
    tags.push_back("alpha");
    tags.push_back("beta");
    tags.push_back("alpha");
    source.add_column("tag", std::move(tags));
    source.add_column("flag", ibex::Column<bool>{true, false, true});

    ArrowArray array{};
    ArrowSchema schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());

    auto imported = ibex::interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());

    const auto* tag = std::get_if<ibex::Column<ibex::Categorical>>(imported->find("tag"));
    REQUIRE(tag != nullptr);
    REQUIRE(tag->size() == 3);
    CHECK((*tag)[0] == "alpha");
    CHECK((*tag)[1] == "beta");
    CHECK((*tag)[2] == "alpha");

    const auto* flag = std::get_if<ibex::Column<bool>>(imported->find("flag"));
    REQUIRE(flag != nullptr);
    REQUIRE(flag->size() == 3);
    CHECK((*flag)[0]);
    CHECK_FALSE((*flag)[1]);
    CHECK((*flag)[2]);

    schema.release(&schema);
    array.release(&array);
}

TEST_CASE("A dictionary-encoded index is still a categorical", "[interop][arrow][int]") {
    // The regression guard for the fix above: "i" with dictionary storage must
    // keep going to the categorical importer, not be read as a plain int32.
    ibex::runtime::Table source;
    ibex::Column<ibex::Categorical> symbols;
    symbols.push_back("a");
    symbols.push_back("b");
    symbols.push_back("a");
    source.add_column("sym", std::move(symbols));

    ArrowArray array{};
    ArrowSchema schema{};
    REQUIRE(ibex::interop::export_table_to_arrow(source, &array, &schema).has_value());
    REQUIRE(std::string(schema.children[0]->format) == "i");
    REQUIRE(schema.children[0]->dictionary != nullptr);

    auto imported = ibex::interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());
    const auto* codes = std::get_if<ibex::Column<ibex::Categorical>>(imported->find("sym"));
    REQUIRE(codes != nullptr);
    CHECK((*codes)[0] == "a");
    CHECK((*codes)[1] == "b");

    schema.release(&schema);
    array.release(&array);
}
