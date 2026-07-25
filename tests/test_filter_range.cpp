// Range-aware filtering: `filter_table_range` must be indistinguishable from
// gathering the range into a table of its own and filtering that.
//
// This is the correctness proof for the zero-copy morsel path (runtime
// multithreading plan, Phase 2). The rest of the suite only ever evaluates
// whole ranges, so it cannot see an offset that is applied to some column reads
// but not others — the exact defect class this file targets. The oracle is a
// deliberately naive, test-local gather: reusing the runtime's own slicing
// helper would make the comparison circular.

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <vector>

#include "interpreter_internal.hpp"

using namespace ibex;
using runtime::ColumnValue;
using runtime::RowRange;

namespace {

auto col_ref(const char* name) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = name}});
}
auto ilit(std::int64_t v) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = v}});
}
auto dlit(double v) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = v}});
}
auto slit(const char* v) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{v}}});
}
auto cmp(ir::CompareOp op, ir::ExprPtr l, ir::ExprPtr r) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{.op = op, .left = std::move(l), .right = std::move(r)}};
}
auto logical(ir::LogicalOp op, ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = op,
                                            .left = ir::make_expr_ptr(std::move(l)),
                                            .right = ir::make_expr_ptr(std::move(r))}};
}
auto arith(ir::ArithmeticOp op, ir::ExprPtr l, ir::ExprPtr r) -> ir::ExprPtr {
    return ir::make_expr_ptr(
        ir::Expr{.node = ir::BinaryExpr{.op = op, .left = std::move(l), .right = std::move(r)}});
}

/// A table wide enough that every column kind the gather special-cases is
/// covered: fixed-width, bool (bit-packed), string (flat buffer), categorical
/// (dictionary codes), and a nullable column.
auto make_table(std::size_t rows) -> runtime::Table {
    runtime::Table t;
    Column<std::int64_t> id;
    Column<double> price;
    Column<bool> flag;
    Column<std::string> name;
    Column<std::int64_t> nullable;
    runtime::ValidityBitmap valid(rows, true);
    std::vector<std::string> dict{"alpha", "beta", "gamma"};
    std::vector<std::int32_t> codes;
    codes.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        id.push_back(static_cast<std::int64_t>(i));
        price.push_back(static_cast<double>(i % 37) * 1.5);
        flag.push_back((i % 3) == 0);
        name.push_back("row-" + std::to_string(i % 11));
        nullable.push_back(static_cast<std::int64_t>(i * 2));
        if ((i % 5) == 0) {
            valid.set(i, false);
        }
        codes.push_back(static_cast<std::int32_t>(i % 3));
    }

    t.add_column("id", ColumnValue{std::move(id)});
    t.add_column("price", ColumnValue{std::move(price)});
    t.add_column("flag", ColumnValue{std::move(flag)});
    t.add_column("name", ColumnValue{std::move(name)});
    t.add_column("nullable", ColumnValue{std::move(nullable)}, std::move(valid));
    t.add_column("kind", ColumnValue{Column<Categorical>{std::move(dict), std::move(codes)}});
    return t;
}

/// Independent oracle: copy rows [begin, begin+count) one at a time.
auto gather_rows(const runtime::Table& src, RowRange rows) -> runtime::Table {
    runtime::Table out;
    for (const auto& entry : src.columns) {
        ColumnValue col = std::visit(
            [&](const auto& c) -> ColumnValue {
                using ColT = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    Column<Categorical> dst{std::vector<std::string>(c.dictionary())};
                    for (std::size_t i = 0; i < rows.count; ++i) {
                        dst.push_code(c.code_at(rows.begin + i));
                    }
                    return ColumnValue{std::move(dst)};
                } else {
                    ColT dst;
                    for (std::size_t i = 0; i < rows.count; ++i) {
                        dst.push_back(c[rows.begin + i]);
                    }
                    return ColumnValue{std::move(dst)};
                }
            },
            *entry.column);
        if (entry.validity.has_value()) {
            runtime::ValidityBitmap bm(rows.count, true);
            for (std::size_t i = 0; i < rows.count; ++i) {
                bm.set(i, (*entry.validity)[rows.begin + i]);
            }
            out.add_column(entry.name, std::move(col), std::move(bm));
        } else {
            out.add_column(entry.name, std::move(col));
        }
    }
    return out;
}

/// The contract, asserted for one predicate over one range.
void check_range(const runtime::Table& table, const ir::Expr& predicate, RowRange rows,
                 const char* label) {
    INFO(label << " over [" << rows.begin << ", " << rows.end() << ")");

    auto ranged = runtime::filter_table_range(table, predicate, rows, nullptr);
    auto gathered = gather_rows(table, rows);
    auto expected = runtime::filter_table(gathered, predicate, nullptr);

    REQUIRE(expected.has_value());
    REQUIRE(ranged.has_value());
    auto mismatch = runtime::compare_tables(*expected, *ranged);
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}

struct NamedPredicate {
    const char* label;
    ir::Expr expr;
};

auto predicates() -> std::vector<NamedPredicate> {
    std::vector<NamedPredicate> out;
    // Each entry targets a distinct evaluator path, all of which read column
    // data at a different point and so can drop an offset independently.
    out.push_back({"fused numeric cmp spec", cmp(ir::CompareOp::Gt, col_ref("id"), ilit(20))});
    out.push_back(
        {"fused arith cmp spec",
         cmp(ir::CompareOp::Lt, arith(ir::ArithmeticOp::Mul, col_ref("id"), ilit(2)), ilit(90))});
    out.push_back({"fused numeric cmp pair",
                   logical(ir::LogicalOp::And, cmp(ir::CompareOp::Ge, col_ref("id"), ilit(10)),
                           cmp(ir::CompareOp::Le, col_ref("id"), ilit(60)))});
    out.push_back(
        {"double column vs literal", cmp(ir::CompareOp::Gt, col_ref("price"), dlit(20.0))});
    out.push_back(
        {"string column vs literal", cmp(ir::CompareOp::Eq, col_ref("name"), slit("row-3"))});
    out.push_back(
        {"categorical vs literal", cmp(ir::CompareOp::Eq, col_ref("kind"), slit("beta"))});
    out.push_back({"categorical IN-list",
                   logical(ir::LogicalOp::Or, cmp(ir::CompareOp::Eq, col_ref("kind"), slit("beta")),
                           cmp(ir::CompareOp::Eq, col_ref("kind"), slit("gamma")))});
    out.push_back({"bare boolean column", ir::Expr{.node = ir::ColumnRef{.name = "flag"}}});
    out.push_back(
        {"3VL over a nullable column", cmp(ir::CompareOp::Gt, col_ref("nullable"), ilit(40))});
    out.push_back({"is null", ir::Expr{.node = ir::IsNullExpr{.operand = col_ref("nullable"),
                                                              .negated = false}}});
    out.push_back({"is not null", ir::Expr{.node = ir::IsNullExpr{.operand = col_ref("nullable"),
                                                                  .negated = true}}});
    out.push_back({"column vs column", cmp(ir::CompareOp::Lt, col_ref("id"), col_ref("nullable"))});
    out.push_back(
        {"negated predicate",
         ir::Expr{.node = ir::LogicalExpr{
                      .op = ir::LogicalOp::Not,
                      .left = ir::make_expr_ptr(cmp(ir::CompareOp::Gt, col_ref("id"), ilit(20))),
                      .right = nullptr}}});
    return out;
}

}  // namespace

TEST_CASE("filter_table_range matches filtering a gathered range", "[filter][range]") {
    constexpr std::size_t kRows = 200;
    const auto table = make_table(kRows);

    for (const auto& p : predicates()) {
        // Grains chosen so ranges start both on and off a 64-row word boundary
        // — the bit-packed bool gather gets its source bits from one word only
        // when the range is word-aligned.
        for (const std::size_t grain : {1U, 7U, 63U, 64U, 65U, 128U, 200U}) {
            for (std::size_t begin = 0; begin < kRows; begin += grain) {
                const std::size_t count = std::min(grain, kRows - begin);
                check_range(table, p.expr, RowRange{.begin = begin, .count = count}, p.label);
            }
        }
    }
}

TEST_CASE("filter_table_range handles degenerate ranges", "[filter][range]") {
    const auto table = make_table(64);
    const auto predicate = cmp(ir::CompareOp::Gt, col_ref("id"), ilit(10));

    SECTION("empty range at the start") {
        check_range(table, predicate, RowRange{.begin = 0, .count = 0}, "empty");
    }
    SECTION("empty range at the end") {
        check_range(table, predicate, RowRange{.begin = 64, .count = 0}, "empty");
    }
    SECTION("single row that fails the predicate") {
        check_range(table, predicate, RowRange{.begin = 0, .count = 1}, "single");
    }
    SECTION("single row that passes the predicate") {
        check_range(table, predicate, RowRange{.begin = 63, .count = 1}, "single");
    }
    SECTION("whole range takes the offset-free fast paths") {
        check_range(table, predicate, RowRange::whole(64), "whole");
    }
}

TEST_CASE("filter_table_range partitions reassemble the whole-table result", "[filter][range]") {
    constexpr std::size_t kRows = 137;
    const auto table = make_table(kRows);
    const auto predicate =
        logical(ir::LogicalOp::And, cmp(ir::CompareOp::Ge, col_ref("id"), ilit(5)),
                cmp(ir::CompareOp::Eq, col_ref("kind"), slit("alpha")));

    auto whole = runtime::filter_table(table, predicate, nullptr);
    REQUIRE(whole.has_value());

    for (const std::size_t grain : {1U, 13U, 64U, 100U}) {
        std::size_t total = 0;
        for (std::size_t begin = 0; begin < kRows; begin += grain) {
            const std::size_t count = std::min(grain, kRows - begin);
            auto part = runtime::filter_table_range(
                table, predicate, RowRange{.begin = begin, .count = count}, nullptr);
            REQUIRE(part.has_value());
            total += part->rows();
        }
        INFO("grain " << grain);
        // Every input row belongs to exactly one range, so the surviving rows
        // partition too — no row is dropped at a boundary or counted twice.
        CHECK(total == whole->rows());
    }
}
