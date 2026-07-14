#include <ibex/core/time.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

using namespace ibex;

// filter_selection evaluates the conjuncts a scan pushdown hands it. Range
// conjuncts on the same column are fused into a single lo/hi test, which is a
// pure optimization — these cases pin the semantics that fusion must preserve:
// strict vs inclusive edges, contradictory bounds, nulls, and the types the
// fused kernels cover (Int64/Double/Date/Timestamp) versus the ones that fall
// back to the generic path.

namespace {

auto cmp(const std::string& column, ir::CompareOp op, ir::Literal lit) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{
                        .op = op,
                        .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = column}}),
                        .right = ir::make_expr_ptr(ir::Expr{.node = std::move(lit)})}};
}

auto lit_i(std::int64_t v) -> ir::Literal {
    return ir::Literal{.value = v};
}
auto lit_d(double v) -> ir::Literal {
    return ir::Literal{.value = v};
}

auto select(const runtime::Table& table, const std::vector<ir::Expr>& conjuncts)
    -> std::vector<std::size_t> {
    auto out = runtime::filter_selection(table, conjuncts, nullptr);
    REQUIRE(out.has_value());
    return *out;
}

// 0..9 in every column, so a bound's arithmetic is easy to read off.
auto ramp_table() -> runtime::Table {
    std::vector<std::int64_t> ints;
    std::vector<double> doubles;
    std::vector<Date> dates;
    std::vector<Timestamp> stamps;
    for (std::int64_t i = 0; i < 10; ++i) {
        ints.push_back(i);
        doubles.push_back(static_cast<double>(i));
        dates.push_back(Date{static_cast<std::int32_t>(i)});
        stamps.push_back(Timestamp{i});
    }
    runtime::Table table;
    table.add_column("i", Column<std::int64_t>{std::move(ints)});
    table.add_column("d", Column<double>{std::move(doubles)});
    table.add_column("dt", Column<Date>{std::move(dates)});
    table.add_column("ts", Column<Timestamp>{std::move(stamps)});
    return table;
}

}  // namespace

TEST_CASE("filter_selection: two bounds on one column fuse into a range", "[filter][selection]") {
    const auto table = ramp_table();

    // The q06 shape: an inclusive range written as two conjuncts.
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Ge, lit_i(3)), cmp("i", ir::CompareOp::Le, lit_i(5))}) ==
          std::vector<std::size_t>{3, 4, 5});

    // Strict edges must stay strict once fused.
    CHECK(select(table, {cmp("i", ir::CompareOp::Gt, lit_i(3)),
                         cmp("i", ir::CompareOp::Lt, lit_i(5))}) == std::vector<std::size_t>{4});

    // A repeated bound on the same side keeps the tighter one, whichever order
    // it arrives in.
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Ge, lit_i(2)), cmp("i", ir::CompareOp::Ge, lit_i(7))}) ==
          std::vector<std::size_t>{7, 8, 9});
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Ge, lit_i(7)), cmp("i", ir::CompareOp::Ge, lit_i(2))}) ==
          std::vector<std::size_t>{7, 8, 9});

    // Same edge, one strict: strictness wins.
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Ge, lit_i(4)), cmp("i", ir::CompareOp::Gt, lit_i(4))}) ==
          std::vector<std::size_t>{5, 6, 7, 8, 9});

    // Eq pins both ends.
    CHECK(select(table, {cmp("i", ir::CompareOp::Eq, lit_i(6))}) == std::vector<std::size_t>{6});
}

TEST_CASE("filter_selection: contradictory bounds select nothing", "[filter][selection]") {
    const auto table = ramp_table();

    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Gt, lit_i(5)), cmp("i", ir::CompareOp::Lt, lit_i(3))})
              .empty());
    // Touching but empty: `x >= 5 && x < 5`.
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Ge, lit_i(5)), cmp("i", ir::CompareOp::Lt, lit_i(5))})
              .empty());
    // Two different equalities cannot both hold.
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Eq, lit_i(2)), cmp("i", ir::CompareOp::Eq, lit_i(3))})
              .empty());
}

TEST_CASE("filter_selection: ranges on several columns intersect", "[filter][selection]") {
    const auto table = ramp_table();

    // The full q06 shape: bounds on three columns at once.
    CHECK(
        select(table, {cmp("i", ir::CompareOp::Ge, lit_i(2)), cmp("i", ir::CompareOp::Le, lit_i(8)),
                       cmp("d", ir::CompareOp::Lt, lit_d(6.0)),
                       cmp("ts", ir::CompareOp::Gt, ir::Literal{.value = Timestamp{3}})}) ==
        std::vector<std::size_t>{4, 5});
}

TEST_CASE("filter_selection: Date and Timestamp columns fuse", "[filter][selection]") {
    const auto table = ramp_table();

    CHECK(select(table, {cmp("dt", ir::CompareOp::Ge, ir::Literal{.value = Date{4}}),
                         cmp("dt", ir::CompareOp::Lt, ir::Literal{.value = Date{7}})}) ==
          std::vector<std::size_t>{4, 5, 6});

    CHECK(select(table, {cmp("ts", ir::CompareOp::Ge, ir::Literal{.value = Timestamp{8}})}) ==
          std::vector<std::size_t>{8, 9});
}

TEST_CASE("filter_selection: doubles fuse against int literals", "[filter][selection]") {
    const auto table = ramp_table();

    // `d >= 3 && d <= 5` with integer literals against a Double column.
    CHECK(select(table,
                 {cmp("d", ir::CompareOp::Ge, lit_i(3)), cmp("d", ir::CompareOp::Le, lit_i(5))}) ==
          std::vector<std::size_t>{3, 4, 5});
}

TEST_CASE("filter_selection: a null never survives a bound", "[filter][selection]") {
    std::vector<std::int64_t> values{1, 2, 3, 4, 5};
    runtime::ValidityBitmap validity(values.size(), true);
    validity.set(2, false);  // the row that would otherwise match

    runtime::Table table;
    table.add_column("i", Column<std::int64_t>{std::move(values)}, std::move(validity));

    // Row 2 holds 3 but is null, so the range must skip it.
    CHECK(select(table, {cmp("i", ir::CompareOp::Ge, lit_i(2)),
                         cmp("i", ir::CompareOp::Le, lit_i(4))}) == std::vector<std::size_t>{1, 3});
}

TEST_CASE("filter_selection: non-fusable conjuncts still apply", "[filter][selection]") {
    const auto table = ramp_table();

    // `!=` is not a range: it has to survive as a leftover conjunct rather than
    // be dropped or folded into a bound.
    CHECK(select(table,
                 {cmp("i", ir::CompareOp::Ge, lit_i(4)), cmp("i", ir::CompareOp::Le, lit_i(7)),
                  cmp("i", ir::CompareOp::Ne, lit_i(6))}) == std::vector<std::size_t>{4, 5, 7});

    // An Int64 column against a fractional literal is left to the generic path
    // rather than fused with a guessed rounding: `i < 4.5` keeps 4.
    CHECK(select(table, {cmp("i", ir::CompareOp::Ge, lit_i(3)),
                         cmp("i", ir::CompareOp::Lt, lit_d(4.5))}) ==
          std::vector<std::size_t>{3, 4});

    // A comparison against NaN is false for every row, fused or not.
    CHECK(select(table,
                 {cmp("d", ir::CompareOp::Gt, lit_d(std::numeric_limits<double>::quiet_NaN()))})
              .empty());
}

TEST_CASE("filter_selection: literal-first comparisons fuse", "[filter][selection]") {
    const auto table = ramp_table();

    // `3 <= i && i <= 5` — the literal is on the left, so the op flips.
    auto lit_left = [](ir::Literal lit, ir::CompareOp op, const std::string& column) {
        return ir::Expr{
            .node = ir::CompareExpr{
                .op = op,
                .left = ir::make_expr_ptr(ir::Expr{.node = std::move(lit)}),
                .right = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = column}})}};
    };

    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(lit_left(lit_i(3), ir::CompareOp::Le, "i"));
    conjuncts.push_back(lit_left(lit_i(5), ir::CompareOp::Ge, "i"));
    CHECK(select(table, conjuncts) == std::vector<std::size_t>{3, 4, 5});
}

TEST_CASE("filter_selection: no conjuncts selects every row", "[filter][selection]") {
    const auto table = ramp_table();
    CHECK(select(table, {}).size() == 10);
}

TEST_CASE("filter_selection: fused ranges hold across block boundaries", "[filter][selection]") {
    // The fused pass walks the column in blocks; a range that straddles a block
    // boundary must not lose or duplicate rows.
    constexpr std::size_t kRows = 10000;  // > one 4096-row block
    std::vector<std::int64_t> values;
    values.reserve(kRows);
    for (std::size_t i = 0; i < kRows; ++i) {
        values.push_back(static_cast<std::int64_t>(i));
    }
    runtime::Table table;
    table.add_column("i", Column<std::int64_t>{std::move(values)});

    auto selected = select(table, {cmp("i", ir::CompareOp::Ge, lit_i(4000)),
                                   cmp("i", ir::CompareOp::Lt, lit_i(9000))});
    REQUIRE(selected.size() == 5000);
    CHECK(selected.front() == 4000);
    CHECK(selected.back() == 8999);
    for (std::size_t i = 0; i < selected.size(); ++i) {
        REQUIRE(selected[i] == 4000 + i);
    }
}

// ── IN-lists ─────────────────────────────────────────────────────────────────
//
// SQL's `col IN (a, b)` reaches Ibex as `col == a || col == b`, and a pushed
// filter's text predicates fuse into the same blocked pass as the ranges: a
// Categorical column is tested against one byte per dictionary code, so the
// per-row cost is one indexed load however long the list is.

namespace {

auto cat_column(const std::vector<std::string>& dict, const std::vector<std::size_t>& codes)
    -> Column<Categorical> {
    Column<Categorical> col(dict);
    for (auto code : codes) {
        col.push_code(static_cast<Column<Categorical>::code_type>(code));
    }
    return col;
}

// mode: AIR, AIR REG, SHIP, RAIL, AIR — plus a parallel int for range mixing.
auto ship_table() -> runtime::Table {
    runtime::Table table;
    table.add_column("mode", cat_column({"AIR", "AIR REG", "SHIP", "RAIL"}, {0, 1, 2, 3, 0}));
    table.add_column("plain", Column<std::string>{});
    auto& plain = std::get<Column<std::string>>(*table.columns[1].column);
    for (const auto* value : {"AIR", "AIR REG", "SHIP", "RAIL", "AIR"}) {
        plain.push_back(value);
    }
    table.add_column("qty", Column<std::int64_t>{1, 2, 3, 4, 5});
    return table;
}

auto in_list(const std::string& column, const std::vector<std::string>& values) -> ir::Expr {
    ir::Expr acc = cmp(column, ir::CompareOp::Eq, ir::Literal{.value = values.front()});
    for (std::size_t i = 1; i < values.size(); ++i) {
        ir::Expr next = cmp(column, ir::CompareOp::Eq, ir::Literal{.value = values[i]});
        acc = ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::Or,
                                               .left = ir::make_expr_ptr(std::move(acc)),
                                               .right = ir::make_expr_ptr(std::move(next))}};
    }
    return acc;
}

auto lit_s(const std::string& v) -> ir::Literal {
    return ir::Literal{.value = v};
}

}  // namespace

TEST_CASE("filter_selection: an IN-list over a categorical column fuses", "[filter][selection]") {
    const auto table = ship_table();

    CHECK(select(table, {in_list("mode", {"AIR", "AIR REG"})}) ==
          std::vector<std::size_t>{0, 1, 4});
    // A single equality is the one-element case.
    CHECK(select(table, {cmp("mode", ir::CompareOp::Eq, lit_s("SHIP"))}) ==
          std::vector<std::size_t>{2});
    // Order within the list is irrelevant, and a value absent from the data is
    // simply not matched.
    CHECK(select(table, {in_list("mode", {"RAIL", "NOPE", "SHIP"})}) ==
          std::vector<std::size_t>{2, 3});
}

TEST_CASE("filter_selection: IN-lists work on plain string columns too", "[filter][selection]") {
    const auto table = ship_table();
    CHECK(select(table, {in_list("plain", {"AIR", "RAIL"})}) == std::vector<std::size_t>{0, 3, 4});
    CHECK(select(table, {cmp("plain", ir::CompareOp::Ne, lit_s("AIR"))}) ==
          std::vector<std::size_t>{1, 2, 3});
}

TEST_CASE("filter_selection: a literal outside the dictionary selects nothing",
          "[filter][selection]") {
    const auto table = ship_table();
    // "TRUCK" is not a value `mode` can take, so no code matches it.
    CHECK(select(table, {cmp("mode", ir::CompareOp::Eq, lit_s("TRUCK"))}).empty());
    // Two different equalities on one column cannot both hold.
    CHECK(select(table, {cmp("mode", ir::CompareOp::Eq, lit_s("AIR")),
                         cmp("mode", ir::CompareOp::Eq, lit_s("SHIP"))})
              .empty());
}

TEST_CASE("filter_selection: conjuncts narrow an IN-list rather than widen it",
          "[filter][selection]") {
    const auto table = ship_table();
    // `mode IN (AIR, AIR REG, SHIP) && mode != AIR` -> {AIR REG, SHIP}
    CHECK(select(table, {in_list("mode", {"AIR", "AIR REG", "SHIP"}),
                         cmp("mode", ir::CompareOp::Ne, lit_s("AIR"))}) ==
          std::vector<std::size_t>{1, 2});
    // Two IN-lists on one column intersect.
    CHECK(select(table, {in_list("mode", {"AIR", "AIR REG", "SHIP"}),
                         in_list("mode", {"SHIP", "RAIL"})}) == std::vector<std::size_t>{2});
}

TEST_CASE("filter_selection: an IN-list fuses alongside a numeric range", "[filter][selection]") {
    const auto table = ship_table();
    // The q19 shape: a text membership test and a range in one pass.
    CHECK(select(table,
                 {in_list("mode", {"AIR", "AIR REG"}), cmp("qty", ir::CompareOp::Ge, lit_i(2))}) ==
          std::vector<std::size_t>{1, 4});
}

TEST_CASE("filter_selection: a null survives no membership test", "[filter][selection]") {
    runtime::Table table;
    runtime::ValidityBitmap validity(3, true);
    validity.set(1, false);
    table.add_column("mode", cat_column({"AIR", "SHIP"}, {0, 0, 1}), std::move(validity));

    // Row 1's code says AIR, but it is null, so the membership test drops it.
    CHECK(select(table, {in_list("mode", {"AIR", "SHIP"})}) == std::vector<std::size_t>{0, 2});
}

TEST_CASE("filter_selection: an OR across two columns is not a membership test",
          "[filter][selection]") {
    const auto table = ship_table();
    // `mode == AIR || plain == RAIL` is a genuine disjunction and must not be
    // mistaken for an IN-list on either column.
    ir::Expr disjunction{
        .node = ir::LogicalExpr{
            .op = ir::LogicalOp::Or,
            .left = ir::make_expr_ptr(cmp("mode", ir::CompareOp::Eq, lit_s("AIR"))),
            .right = ir::make_expr_ptr(cmp("plain", ir::CompareOp::Eq, lit_s("RAIL")))}};
    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(std::move(disjunction));
    CHECK(select(table, conjuncts) == std::vector<std::size_t>{0, 3, 4});
}

TEST_CASE("filter_selection: the fused pass left-packs across block boundaries",
          "[filter][selection]") {
    // Survivors are left-packed per 4096-row block and bulk-copied out, so the
    // block seam is where a mispacked cursor would show up: rows are selected at
    // an irregular stride (nothing lines up with the block size) and every
    // boundary lands mid-run.
    constexpr std::size_t kRows = 20000;
    std::vector<std::int64_t> values;
    values.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        values.push_back(static_cast<std::int64_t>(row % 7));  // keep 3 of every 7
    }
    runtime::Table table;
    table.add_column("i", Column<std::int64_t>{std::move(values)});

    std::vector<std::size_t> want;
    for (std::size_t row = 0; row < kRows; ++row) {
        if (row % 7 >= 4) {
            want.push_back(row);
        }
    }

    CHECK(select(table, {cmp("i", ir::CompareOp::Ge, lit_i(4))}) == want);

    // Everything, and nothing: the packing cursor's two extremes.
    CHECK(select(table, {cmp("i", ir::CompareOp::Ge, lit_i(0))}).size() == kRows);
    CHECK(select(table, {cmp("i", ir::CompareOp::Gt, lit_i(6))}).empty());
}
