// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Walking an AdbcConnectionGetObjects result. Header-only, so this runs in
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
