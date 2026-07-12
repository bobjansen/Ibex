#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>
#include <ibex/runtime/operator.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <memory>
#include <utility>

namespace {

using namespace ibex;

auto require_ir(const char* source) -> ir::NodePtr {
    auto result = parser::parse(source);
    REQUIRE(result.has_value());
    auto lowered = parser::lower(result.value());
    REQUIRE(lowered.has_value());
    return std::move(lowered.value());
}

/// Restores a clean flag around each test so a failing REQUIRE cannot leak
/// a pending interrupt into later tests.
struct InterruptGuard {
    InterruptGuard() { runtime::clear_interrupt(); }
    InterruptGuard(const InterruptGuard&) = delete;
    InterruptGuard(InterruptGuard&&) = delete;
    auto operator=(const InterruptGuard&) -> InterruptGuard& = delete;
    auto operator=(InterruptGuard&&) -> InterruptGuard& = delete;
    ~InterruptGuard() { runtime::clear_interrupt(); }
};

TEST_CASE("Interrupt flag request/consume lifecycle", "[runtime][interrupt]") {
    const InterruptGuard guard;

    REQUIRE_FALSE(runtime::interrupt_requested());

    runtime::request_interrupt();
    REQUIRE(runtime::interrupt_requested());
    // Plain checks observe without consuming so every layer can unwind.
    REQUIRE(runtime::interrupt_requested());

    REQUIRE(runtime::consume_interrupt());
    REQUIRE_FALSE(runtime::interrupt_requested());
    REQUIRE_FALSE(runtime::consume_interrupt());
}

TEST_CASE("Interpret unwinds with interrupted error when flag is set", "[runtime][interrupt]") {
    const InterruptGuard guard;

    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 15];");

    runtime::request_interrupt();
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == runtime::interrupt_message());

    // The check does not consume the flag; the driver clears it and the
    // same plan then succeeds.
    runtime::clear_interrupt();
    auto rerun = runtime::interpret(*ir, registry);
    REQUIRE(rerun.has_value());
    REQUIRE(rerun->rows() == 2);
}

TEST_CASE("MaterializeOperator stops between chunks when interrupted", "[runtime][interrupt]") {
    const InterruptGuard guard;

    /// Emits int chunks forever; interruption is the only way to stop it
    /// short. Sets the flag itself after `chunks_before_interrupt` pulls to
    /// simulate a Ctrl+C arriving mid-stream.
    class EndlessSource final : public runtime::Operator {
       public:
        explicit EndlessSource(int chunks_before_interrupt) : remaining_(chunks_before_interrupt) {}

        [[nodiscard]] auto next()
            -> std::expected<std::optional<runtime::Chunk>, std::string> override {
            if (remaining_-- == 0) {
                runtime::request_interrupt();
            }
            runtime::Chunk chunk;
            runtime::ColumnEntry entry;
            entry.name = "x";
            entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{1, 2, 3});
            chunk.columns.push_back(std::move(entry));
            return std::optional<runtime::Chunk>{std::move(chunk)};
        }

       private:
        int remaining_;
    };

    runtime::MaterializeOperator sink{std::make_unique<EndlessSource>(2)};
    auto result = sink.run();
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == runtime::interrupt_message());
}

}  // namespace
