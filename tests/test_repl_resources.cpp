// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Resources (`extern type`) through the REPL statement path, against an
// instrumented fake connection that counts live objects and logs every call.

#include <ibex/core/column.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

using ibex::runtime::ExternArgs;
using ibex::runtime::ExternValue;

struct FakeState {
    int live = 0;
    int opened = 0;
    std::vector<std::string> log;
};

class FakeConn final : public ibex::runtime::Resource {
   public:
    FakeConn(FakeState& state, std::string name) : state_(state), name_(std::move(name)) {
        ++state_.live;
        ++state_.opened;
    }
    FakeConn(const FakeConn&) = delete;
    FakeConn(FakeConn&&) = delete;
    auto operator=(const FakeConn&) -> FakeConn& = delete;
    auto operator=(FakeConn&&) -> FakeConn& = delete;
    ~FakeConn() override { --state_.live; }

    [[nodiscard]] auto type_name() const noexcept -> std::string_view override {
        return "FakeConn";
    }
    [[nodiscard]] auto name() const -> const std::string& { return name_; }

    bool closed = false;

   private:
    FakeState& state_;
    std::string name_;
};

class OtherConn final : public ibex::runtime::Resource {
   public:
    [[nodiscard]] auto type_name() const noexcept -> std::string_view override {
        return "OtherConn";
    }
};

constexpr std::string_view kDecls = R"(
extern type FakeConn from "fake_resource.hpp";
extern type OtherConn from "fake_resource.hpp";
extern fn fake_open(name: String) -> FakeConn from "fake_resource.hpp";
extern fn fake_query(mutable c: FakeConn, n: Int) -> DataFrame from "fake_resource.hpp";
extern fn fake_close(mutable c: FakeConn) -> Int from "fake_resource.hpp";
extern fn other_open() -> OtherConn from "fake_resource.hpp";
)";

void register_fake(ibex::runtime::ExternRegistry& registry, FakeState& state) {
    registry.register_resource(
        "fake_open", [&state](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            const auto& name = std::get<std::string>(args.at(0));
            state.log.push_back("open " + name);
            return ExternValue{std::make_shared<FakeConn>(state, name)};
        });
    registry.register_table(
        "fake_query", [&state](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            auto conn = args.resource_as<FakeConn>(0);
            if (conn == nullptr) {
                return std::unexpected("fake_query: not a FakeConn");
            }
            if (conn->closed) {
                return std::unexpected("fake_query: connection is closed");
            }
            const auto n = std::get<std::int64_t>(args.at(1));
            state.log.push_back("query " + conn->name());
            ibex::Column<std::int64_t> values;
            for (std::int64_t i = 0; i < n; ++i) {
                values.push_back(i);
            }
            ibex::runtime::Table table;
            table.add_column("v", std::move(values));
            return ExternValue{std::move(table)};
        });
    registry.register_scalar(
        "fake_close", ibex::runtime::ScalarKind::Int,
        [&state](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            auto conn = args.resource_as<FakeConn>(0);
            if (conn == nullptr) {
                return std::unexpected("fake_close: not a FakeConn");
            }
            if (conn->closed) {
                return ExternValue{ibex::runtime::ScalarValue{std::int64_t{0}}};
            }
            conn->closed = true;
            state.log.push_back("close " + conn->name());
            return ExternValue{ibex::runtime::ScalarValue{std::int64_t{1}}};
        });
    registry.register_resource("other_open",
                               [](const ExternArgs&) -> std::expected<ExternValue, std::string> {
                                   return ExternValue{std::make_shared<OtherConn>()};
                               });
}

auto scalar_int(const ibex::repl::ExecutionResult& result) -> std::int64_t {
    REQUIRE(result.scalar.has_value());
    return std::get<std::int64_t>(*result.scalar);
}

}  // namespace

TEST_CASE("A resource binding is reused across statements and closed once", "[repl][resource]") {
    FakeState state;
    ibex::runtime::ExternRegistry registry;
    register_fake(registry, state);
    {
        ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
        REQUIRE(session.execute(std::string(kDecls) + "let db = fake_open(\"a\");").ok);
        REQUIRE(state.opened == 1);

        const auto bound = session.execute("let t = fake_query(db, 3); t[select { n = count() }];");
        REQUIRE(bound.ok);
        REQUIRE(bound.table.has_value());

        // A resource call as a table operand runs first; the query sees a table.
        const auto filtered = session.execute("fake_query(db, 5)[filter v >= 2];");
        REQUIRE(filtered.ok);
        REQUIRE(filtered.table.has_value());
        REQUIRE(filtered.table->rows() == 3);

        REQUIRE(session.execute("let alias = db;").ok);
        REQUIRE(scalar_int(session.execute("fake_close(alias);")) == 1);
        REQUIRE(scalar_int(session.execute("fake_close(db);")) == 0);

        const auto after_close = session.execute("fake_query(db, 1);");
        REQUIRE_FALSE(after_close.ok);
        REQUIRE(after_close.error.contains("connection is closed"));

        REQUIRE(state.opened == 1);
        REQUIRE(state.log == std::vector<std::string>{"open a", "query a", "query a", "close a"});

        // The object lives as long as some binding holds it.
        REQUIRE(session.erase("db"));
        REQUIRE(state.live == 1);
        REQUIRE(session.erase("alias"));
        REQUIRE(state.live == 0);
    }
    REQUIRE(state.live == 0);
}

TEST_CASE("Rebinding a resource name releases it; the session releases the rest",
          "[repl][resource]") {
    FakeState state;
    ibex::runtime::ExternRegistry registry;
    register_fake(registry, state);
    {
        ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
        REQUIRE(session
                    .execute(std::string(kDecls) + "let a = fake_open(\"a\");\n"
                                                   "let b = fake_open(\"b\");")
                    .ok);
        REQUIRE(state.live == 2);

        // A failed rebinding leaves the connection bound.
        REQUIRE_FALSE(session.execute("let a = no_such_table;").ok);
        REQUIRE(state.live == 2);

        REQUIRE(session.execute("let a = 1;").ok);
        REQUIRE(state.live == 1);

        // Two connections are independent.
        REQUIRE(session.execute("fake_query(b, 1);").ok);
        REQUIRE(state.log.back() == "query b");
    }
    REQUIRE(state.live == 0);
}

TEST_CASE("Misplaced resource calls are rejected before any plugin call", "[repl][resource]") {
    FakeState state;
    ibex::runtime::ExternRegistry registry;
    register_fake(registry, state);
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session.execute(std::string(kDecls) + "let db = fake_open(\"a\");").ok);
    state.log.clear();

    const auto in_clause = session.execute("fake_query(db, 3)[filter v < fake_close(db)];");
    REQUIRE_FALSE(in_clause.ok);
    REQUIRE(in_clause.error.contains("fake_close can be called only as"));

    const auto in_arithmetic = session.execute("let n = fake_close(db) + 1;");
    REQUIRE_FALSE(in_arithmetic.ok);

    const auto in_scalar_arg = session.execute("fake_query(db, fake_close(db));");
    REQUIRE_FALSE(in_scalar_arg.ok);

    REQUIRE(state.log.empty());

    const auto in_function = session.execute(
        "fn closer(n: Int) -> Int { fake_close(db); }\n"
        "closer(1);");
    REQUIRE_FALSE(in_function.ok);
    REQUIRE(in_function.error.contains("inside a function"));
    REQUIRE(state.log.empty());
}

TEST_CASE("Resource arguments are checked by type and binding", "[repl][resource]") {
    FakeState state;
    ibex::runtime::ExternRegistry registry;
    register_fake(registry, state);
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session
                .execute(std::string(kDecls) + "let other = other_open();\n"
                                               "let x = 1;")
                .ok);

    const auto wrong_type = session.execute("fake_query(other, 1);");
    REQUIRE_FALSE(wrong_type.ok);
    REQUIRE(wrong_type.error.contains("expects FakeConn, but 'other' is OtherConn"));

    const auto scalar = session.execute("fake_query(x, 1);");
    REQUIRE_FALSE(scalar.ok);
    REQUIRE(scalar.error.contains("'x' is not a resource binding"));

    // A nested call can open the resource a parameter takes.
    const auto nested = session.execute("fake_query(fake_open(\"tmp\"), 2);");
    REQUIRE(nested.ok);
    REQUIRE(nested.table.has_value());
    REQUIRE(nested.table->rows() == 2);
    REQUIRE(state.live == 0);

    // A resource is not a table.
    const auto as_table = session.execute("other_open()[filter true];");
    REQUIRE_FALSE(as_table.ok);
}

TEST_CASE("A script using resources runs on the statement path", "[repl][resource]") {
    FakeState state;
    ibex::runtime::ExternRegistry registry;
    register_fake(registry, state);
    const std::string script = std::string(kDecls) +
                               "let db = fake_open(\"s\");\n"
                               "let t = fake_query(db, 4);\n"
                               "fake_close(db);\n"
                               "t[select { n = count() }];\n";
    REQUIRE(ibex::repl::execute_script(script, registry));
    REQUIRE(state.log == std::vector<std::string>{"open s", "query s", "close s"});
    REQUIRE(state.live == 0);
}
