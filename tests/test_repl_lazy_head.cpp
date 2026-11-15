// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../src/repl/lazy_head.hpp"

namespace {

/// Four 1000-row units. Refuses to decode the whole source unselected, and
/// records how many rows each decode asked for.
class RecordingUnitReader final : public ibex::runtime::LazySourceReader {
   public:
    explicit RecordingUnitReader(std::shared_ptr<std::vector<std::size_t>> decoded)
        : decoded_(std::move(decoded)) {}

    auto decode_units() -> std::vector<ibex::runtime::SourceUnit> override {
        std::vector<ibex::runtime::SourceUnit> units;
        units.reserve(4);
        for (std::size_t u = 0; u < 4; ++u) {
            units.push_back({.start = u * 1000, .rows = 1000});
        }
        return units;
    }

    auto decode(const std::vector<std::string>& names, const ibex::runtime::Selection* selection,
                const ibex::runtime::SourceUnit* unit,
                const ibex::runtime::ExecutionContext& /*exec*/)
        -> std::expected<ibex::runtime::Table, std::string> override {
        if (unit == nullptr && selection == nullptr) {
            return std::unexpected("whole-source decode forbidden");
        }
        const std::size_t begin = unit == nullptr ? 0 : unit->start;
        const std::size_t end = unit == nullptr ? 4000 : unit->start + unit->rows;
        decoded_->push_back(selection == nullptr ? end - begin : selection->size());
        std::vector<std::int64_t> values;
        for (auto row = begin; row < end; ++row) {
            if (selection == nullptr || std::ranges::find(*selection, row) != selection->end()) {
                values.push_back(static_cast<std::int64_t>(row) * 10);
            }
        }
        ibex::runtime::Table out;
        out.logical_rows = values.size();
        for (const auto& name : names) {
            out.add_column(name, ibex::Column<std::int64_t>{values});
        }
        return out;
    }

   private:
    std::shared_ptr<std::vector<std::size_t>> decoded_;
};

}  // namespace

TEST_CASE(":head of a lazy source decodes only the leading unit", "[repl][lazy]") {
    // `:head` of a lazy binding materialized every column of the whole source
    // first: 2.7 s for three rows of SF-10 lineitem.
    const auto decoded = std::make_shared<std::vector<std::size_t>>();
    ibex::runtime::Table schema;
    schema.add_column("a", ibex::Column<std::int64_t>{});
    schema.add_column("b", ibex::Column<std::int64_t>{});
    ibex::runtime::LazyTable lazy(
        std::move(schema), 4000,
        [decoded] -> std::expected<ibex::runtime::LazySourceReaderPtr, std::string> {
            return std::make_unique<RecordingUnitReader>(decoded);
        });
    const ibex::runtime::ExecutionContext exec;
    const auto text = ibex::repl::detail::format_lazy_head(lazy, 3, exec);
    INFO((text.has_value() ? std::string{} : text.error()));
    REQUIRE(text.has_value());
    CHECK(text->starts_with("rows: 4000\n"));  // the source's rows, not the three shown
    CHECK(text->contains("| 20 "));            // row 2
    CHECK_FALSE(text->contains("| 30 "));      // row 3 is not shown
    // Asked for the three leading rows (a Parquet reader then reads only the
    // row group they lie in), never for the whole source.
    CHECK(*decoded == std::vector<std::size_t>{3});
}
