// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Walking AdbcConnectionGetObjects and GetInfo results. Header-only, so this runs in
// every build, including those without an ADBC driver manager. The SQLite and
// PostgreSQL drivers only produce arrays with offset 0; the batches here have
// offsets at every level that can carry one.

#include <catch2/catch_test_macros.hpp>

#include <adbc_objects.hpp>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

namespace {

using ibex::adbc::ObjectRow;
using ibex::adbc::read_info_strings;
using ibex::adbc::read_object_rows;

/// Owns the nodes and buffers of a hand-built Arrow array, which only borrow.
class Builder {
   public:
    struct Node {
        ::ArrowArray* array;
        ::ArrowSchema* schema;
    };

    /// A utf8 array over `values`, all valid.
    auto utf8(const char* name, const std::vector<std::string>& values, std::int64_t offset,
              std::int64_t length) -> Node {
        auto& offsets = int32s_.emplace_back();
        auto& data = bytes_.emplace_back();
        offsets.push_back(0);
        for (const auto& value : values) {
            data += value;
            offsets.push_back(static_cast<std::int32_t>(data.size()));
        }
        return node(name, "u", offset, length, {nullptr, offsets.data(), data.data()}, {});
    }

    /// A list array of `values` with `offsets`, and a validity bitmap when
    /// `validity` is given.
    auto list(const char* name, std::vector<std::int32_t> offsets, std::int64_t offset,
              std::int64_t length, Node values, std::optional<std::uint8_t> validity = {}) -> Node {
        auto& stored = int32s_.emplace_back(std::move(offsets));
        const void* bits = nullptr;
        if (validity.has_value()) {
            bits = &bitmaps_.emplace_back(*validity);
        }
        return node(name, "+l", offset, length, {bits, stored.data()}, {values});
    }

    /// A uint32 array over `values`, all valid.
    auto uint32s(const char* name, std::vector<std::uint32_t> values, std::int64_t offset,
                 std::int64_t length) -> Node {
        auto& stored = uint32s_.emplace_back(std::move(values));
        return node(name, "I", offset, length, {nullptr, stored.data()}, {});
    }

    /// An int64 array over `values`, all valid.
    auto int64s(const char* name, std::vector<std::int64_t> values) -> Node {
        auto& stored = int64s_.emplace_back(std::move(values));
        const auto length = static_cast<std::int64_t>(stored.size());
        return node(name, "l", 0, length, {nullptr, stored.data()}, {});
    }

    /// A dense union of `children`; `format` lists their type ids.
    auto dense_union(const char* name, const char* format, std::vector<std::int8_t> type_ids,
                     std::vector<std::int32_t> offsets, std::int64_t offset, std::int64_t length,
                     std::vector<Node> children) -> Node {
        auto& ids = int8s_.emplace_back(std::move(type_ids));
        auto& stored = int32s_.emplace_back(std::move(offsets));
        return node(name, format, offset, length, {ids.data(), stored.data()}, std::move(children));
    }

    auto structure(const char* name, std::int64_t offset, std::int64_t length,
                   std::vector<Node> children) -> Node {
        return node(name, "+s", offset, length, {nullptr}, std::move(children));
    }

   private:
    auto node(const char* name, const char* format, std::int64_t offset, std::int64_t length,
              std::vector<const void*> buffers, const std::vector<Node>& children) -> Node {
        auto& array = arrays_.emplace_back();
        auto& schema = schemas_.emplace_back();
        auto& stored_buffers = buffers_.emplace_back(std::move(buffers));
        auto& child_arrays = child_arrays_.emplace_back();
        auto& child_schemas = child_schemas_.emplace_back();
        for (const auto& child : children) {
            child_arrays.push_back(child.array);
            child_schemas.push_back(child.schema);
        }
        array.length = length;
        array.offset = offset;
        array.n_buffers = static_cast<std::int64_t>(stored_buffers.size());
        array.buffers = stored_buffers.data();
        array.n_children = static_cast<std::int64_t>(child_arrays.size());
        array.children = child_arrays.data();
        schema.format = format;
        schema.name = name;
        schema.n_children = static_cast<std::int64_t>(child_schemas.size());
        schema.children = child_schemas.data();
        return {&array, &schema};
    }

    // Deques: growing one never moves what earlier nodes point into.
    std::deque<::ArrowArray> arrays_;
    std::deque<::ArrowSchema> schemas_;
    std::deque<std::vector<const void*>> buffers_;
    std::deque<std::vector<::ArrowArray*>> child_arrays_;
    std::deque<std::vector<::ArrowSchema*>> child_schemas_;
    std::deque<std::vector<std::int32_t>> int32s_;
    std::deque<std::vector<std::uint32_t>> uint32s_;
    std::deque<std::vector<std::int64_t>> int64s_;
    std::deque<std::vector<std::int8_t>> int8s_;
    std::deque<std::string> bytes_;
    std::deque<std::uint8_t> bitmaps_;
};

auto read(const Builder::Node& root) -> std::vector<ObjectRow> {
    std::vector<ObjectRow> rows;
    auto status = read_object_rows(*root.array, *root.schema,
                                   [&](const ObjectRow& row) { rows.push_back(row); });
    INFO((status ? std::string{} : status.error()));
    REQUIRE(status.has_value());
    return rows;
}

}  // namespace

TEST_CASE("GetObjects rows are read through every offset", "[adbc][discovery]") {
    Builder b;
    // Tables: table_name has an offset of its own; row 0 of it is "z".
    auto tables = b.structure("item", 0, 3,
                              {b.utf8("table_name", {"z", "a", "b", "c"}, 1, 3),
                               b.utf8("table_type", {"table", "table", "view"}, 0, 3)});
    // Schemas: a struct with offset 1 over two physical rows, the first junk.
    // Its table lists are [0, 1) (null, bit 0) and [1, 3) (valid, bit 1).
    auto schemas =
        b.structure("item", 1, 1,
                    {b.utf8("db_schema_name", {"junk", "public"}, 0, 2),
                     b.list("db_schema_tables", {0, 1, 3}, 0, 2, tables, std::uint8_t{0b10})});
    // Catalogs: offset 1 again; the second physical row lists schema 0.
    auto root = b.structure("", 1, 1,
                            {b.utf8("catalog_name", {"junk", "main"}, 0, 2),
                             b.list("catalog_db_schemas", {0, 0, 1}, 0, 2, schemas)});

    const auto rows = read(root);
    REQUIRE(rows.size() == 2);
    for (const auto& row : rows) {
        CHECK(row.catalog == "main");
        CHECK(row.db_schema == "public");
    }
    CHECK(rows[0].table == "b");
    CHECK(rows[0].type == "table");
    CHECK(rows[1].table == "c");
    CHECK(rows[1].type == "view");
}

TEST_CASE("A null list holds no rows", "[adbc][discovery]") {
    Builder b;
    auto tables = b.structure(
        "item", 0, 1, {b.utf8("table_name", {"t"}, 0, 1), b.utf8("table_type", {"table"}, 0, 1)});
    auto schemas = b.structure("item", 0, 1,
                               {b.utf8("db_schema_name", {"s"}, 0, 1),
                                b.list("db_schema_tables", {0, 1}, 0, 1, tables, std::uint8_t{0})});
    auto root = b.structure(
        "", 0, 1,
        {b.utf8("catalog_name", {"c"}, 0, 1), b.list("catalog_db_schemas", {0, 1}, 0, 1, schemas)});
    CHECK(read(root).empty());
}

TEST_CASE("A GetObjects batch of another layout is refused", "[adbc][discovery]") {
    Builder b;
    auto schemas = b.structure("item", 0, 0, {});
    auto root = b.structure("", 0, 1,
                            {b.list("catalog_name", {0, 0}, 0, 1, schemas),
                             b.list("catalog_db_schemas", {0, 0}, 0, 1, schemas)});
    auto status = read_object_rows(*root.array, *root.schema, [](const ObjectRow&) {});
    REQUIRE_FALSE(status.has_value());
    CHECK(status.error() == "field `catalog_name` has unexpected type list<struct>");
}

namespace {

using InfoEntry = std::pair<std::uint32_t, std::optional<std::string>>;

auto read_info(const Builder::Node& root) -> std::vector<InfoEntry> {
    std::vector<InfoEntry> entries;
    auto status = read_info_strings(
        *root.array, *root.schema, [&](std::uint32_t code, const std::optional<std::string>& text) {
            entries.emplace_back(code, text);
        });
    INFO((status ? std::string{} : status.error()));
    REQUIRE(status.has_value());
    return entries;
}

}  // namespace

TEST_CASE("GetInfo strings are read through the union", "[adbc][discovery]") {
    Builder b;
    // Type ids 3 (string_value) and 7 (int64_value), not the child indexes.
    // The struct has offset 1, the union another 1: logical rows 0..2 are
    // physical rows 2..4, of which the int64 one is skipped.
    auto values = b.dense_union("info_value", "+ud:3,7", {0, 0, 3, 7, 3}, {0, 0, 1, 0, 0}, 1, 4,
                                {b.utf8("string_value", {"junk", "duckdb", "v1.5.6"}, 1, 2),
                                 b.int64s("int64_value", {42})});
    auto root = b.structure("", 1, 3, {b.uint32s("info_name", {9, 0, 100, 101, 1}, 1, 4), values});
    // Union offsets index the string child, whose own offset of 1 applies.
    const auto entries = read_info(root);
    REQUIRE(entries.size() == 2);
    CHECK(entries[0] == InfoEntry{100, "v1.5.6"});
    CHECK(entries[1] == InfoEntry{1, "duckdb"});
}

TEST_CASE("A GetInfo batch of another layout is refused", "[adbc][discovery]") {
    Builder b;
    auto root = b.structure("", 0, 1,
                            {b.uint32s("info_name", {0}, 0, 1), b.utf8("info_value", {"x"}, 0, 1)});
    auto status = read_info_strings(*root.array, *root.schema,
                                    [](std::uint32_t, const std::optional<std::string>&) {});
    REQUIRE_FALSE(status.has_value());
    CHECK(status.error() == "field `info_value` has unexpected type utf8");
}
