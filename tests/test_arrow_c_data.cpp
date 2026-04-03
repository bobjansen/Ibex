#include <ibex/interop/arrow_c_data.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <string>
#include <unordered_map>

namespace {

auto read_i32_le(const char* p) -> std::int32_t {
    return static_cast<std::int32_t>(static_cast<unsigned char>(p[0])) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[1])) << 8) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[2])) << 16) |
           (static_cast<std::int32_t>(static_cast<unsigned char>(p[3])) << 24);
}

auto decode_metadata(const char* metadata) -> std::unordered_map<std::string, std::string> {
    std::unordered_map<std::string, std::string> out;
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
