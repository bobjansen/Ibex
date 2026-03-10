#include <ibex/ir/builder.hpp>
#include <ibex/ir/optimizer.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

TEST_CASE("optimizer: can_cse requires pure callable and const args", "[ir][optimizer]") {
    ir::CallableSummary pure_const{
        .effects = ir::EffectSummary{},
        .arg_modes = {ir::ArgMode::Const, ir::ArgMode::Const},
    };
    CHECK(ir::can_cse(pure_const));

    ir::CallableSummary pure_mutable{
        .effects = ir::EffectSummary{},
        .arg_modes = {ir::ArgMode::Const, ir::ArgMode::Mutable},
    };
    CHECK_FALSE(ir::can_cse(pure_mutable));

    ir::CallableSummary effectful{
        .effects =
            ir::EffectSummary{
                .mask = ir::kEffNondet,
                .io_read_unscoped = false,
                .io_write_unscoped = false,
                .io_read_resources = {},
                .io_write_resources = {},
            },
        .arg_modes = {ir::ArgMode::Const},
    };
    CHECK_FALSE(ir::can_cse(effectful));
}

TEST_CASE("optimizer: resource-aware IO reorder checks", "[ir][optimizer]") {
    ir::EffectSummary read_file{
        .mask = ir::kEffIoRead,
        .io_read_unscoped = false,
        .io_write_unscoped = false,
        .io_read_resources = {"file"},
        .io_write_resources = {},
    };
    ir::EffectSummary write_ws{
        .mask = ir::kEffIoWrite,
        .io_read_unscoped = false,
        .io_write_unscoped = false,
        .io_read_resources = {},
        .io_write_resources = {"ws"},
    };
    CHECK(ir::is_reorderable(read_file, write_ws));

    ir::EffectSummary write_file{
        .mask = ir::kEffIoWrite,
        .io_read_unscoped = false,
        .io_write_unscoped = false,
        .io_read_resources = {},
        .io_write_resources = {"file"},
    };
    CHECK_FALSE(ir::is_reorderable(read_file, write_file));

    ir::EffectSummary nondet{
        .mask = ir::kEffNondet,
        .io_read_unscoped = false,
        .io_write_unscoped = false,
        .io_read_resources = {},
        .io_write_resources = {},
    };
    CHECK_FALSE(ir::is_reorderable(nondet, read_file));
}

TEST_CASE("optimizer: dead pure preamble pass", "[ir][optimizer]") {
    ir::Builder builder;
    auto pure_call = builder.extern_call("noop", {ir::Expr{ir::Literal{std::int64_t{1}}}});
    std::vector<ir::NodePtr> preamble;
    preamble.push_back(std::move(pure_call));
    auto root = builder.program(std::move(preamble), builder.scan("df"));

    ir::OptimizationContext context;
    context.callee_summaries.insert_or_assign("noop", ir::CallableSummary{});

    ir::OptimizationStats stats;
    auto optimized = ir::optimize_plan(std::move(root), context, &stats);
    REQUIRE(optimized != nullptr);
    CHECK(optimized->kind() == ir::NodeKind::Scan);
    CHECK(stats.removed_dead_preamble_calls == 1);
}
