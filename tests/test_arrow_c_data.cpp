#include <ibex/interop/arrow_c_data.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstring>
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
    table.time_index = "ts";
    table.ordering = std::vector<ibex::ir::OrderKey>{{"ts", true}, {"id", false}};

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
    table.time_index = "ts";
    table.ordering = std::vector<ibex::ir::OrderKey>{{"ts", true}, {"id", false}};

    ArrowArray array{};
    ArrowSchema schema{};
    auto exported = ibex::interop::export_table_to_arrow(table, &array, &schema);
    REQUIRE(exported.has_value());

    auto imported = ibex::interop::import_table_from_arrow(array, schema);
    REQUIRE(imported.has_value());

    REQUIRE(imported->time_index == table.time_index);
    REQUIRE(imported->ordering.has_value());
    REQUIRE(table.ordering.has_value());
    REQUIRE(imported->ordering->size() == table.ordering->size());
    for (std::size_t i = 0; i < imported->ordering->size(); ++i) {
        CHECK((*imported->ordering)[i].name == (*table.ordering)[i].name);
        CHECK((*imported->ordering)[i].ascending == (*table.ordering)[i].ascending);
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
