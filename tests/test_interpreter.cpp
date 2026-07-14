#include <ibex/ir/builder.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/rng.hpp>
#include <ibex/runtime/safe_arith.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cmath>
#include <limits>
#include <map>
#include <sstream>

namespace {

using namespace ibex;

auto require_program(const char* source) -> parser::Program {
    auto result = parser::parse(source);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

auto require_ir(const char* source) -> ir::NodePtr {
    auto program = require_program(source);
    auto lowered = parser::lower(program);
    REQUIRE(lowered.has_value());
    return std::move(lowered.value());
}

auto date_from_ymd(int y, unsigned m, unsigned d) -> Date {
    using namespace std::chrono;
    auto day_point = sys_days{year{y} / month{m} / std::chrono::day{d}};
    return Date{static_cast<std::int32_t>(day_point.time_since_epoch().count())};
}

auto ts_from_nanos(std::int64_t nanos) -> Timestamp {
    return Timestamp{nanos};
}

auto make_int_chunk(const std::string& name, std::vector<std::int64_t> values) -> runtime::Chunk {
    runtime::Chunk chunk;
    runtime::ColumnEntry entry;
    entry.name = name;
    entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
    auto& col = std::get<Column<std::int64_t>>(*entry.column);
    col.reserve(values.size());
    for (auto v : values) {
        col.push_back(v);
    }
    chunk.columns.push_back(std::move(entry));
    return chunk;
}

auto make_str_int_chunk(const std::string& key_name, std::vector<std::string> keys,
                        const std::string& value_name, std::vector<std::int64_t> values)
    -> runtime::Chunk {
    runtime::Chunk chunk;

    runtime::ColumnEntry key_entry;
    key_entry.name = key_name;
    key_entry.column = std::make_shared<runtime::ColumnValue>(Column<std::string>{});
    auto& key_col = std::get<Column<std::string>>(*key_entry.column);
    key_col.reserve(keys.size());
    for (const auto& v : keys) {
        key_col.push_back(v);
    }
    chunk.columns.push_back(std::move(key_entry));

    runtime::ColumnEntry value_entry;
    value_entry.name = value_name;
    value_entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
    auto& value_col = std::get<Column<std::int64_t>>(*value_entry.column);
    value_col.reserve(values.size());
    for (auto v : values) {
        value_col.push_back(v);
    }
    chunk.columns.push_back(std::move(value_entry));

    return chunk;
}

class VectorSource final : public runtime::Operator {
   public:
    explicit VectorSource(std::vector<runtime::Chunk> chunks) : chunks_(std::move(chunks)) {}

    auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
        if (pos_ >= chunks_.size()) {
            return std::optional<runtime::Chunk>{};
        }
        return std::optional<runtime::Chunk>{std::move(chunks_[pos_++])};
    }

   private:
    std::vector<runtime::Chunk> chunks_;
    std::size_t pos_ = 0;
};

}  // namespace

TEST_CASE("Table mutable_column detaches shared column storage", "[runtime][table]") {
    auto shared = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{1, 2});

    runtime::Table left;
    left.add_column_shared("x", shared);
    runtime::Table right;
    right.add_column_shared("x", shared);

    auto& left_col = std::get<Column<std::int64_t>>(left.mutable_column(0));
    left_col.push_back(3);

    const auto& right_col = std::get<Column<std::int64_t>>(*right.columns[0].column);
    REQUIRE(right_col.size() == 2);
    REQUIRE(std::get<Column<std::int64_t>>(*left.columns[0].column).size() == 3);
    REQUIRE(left.columns[0].column.get() != right.columns[0].column.get());
}

TEST_CASE("Interpret filter + select") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 15, select { price }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_col != nullptr);
    REQUIRE(price_ints->size() == 2);
    REQUIRE((*price_ints)[0] == 20);
    REQUIRE((*price_ints)[1] == 30);
}

TEST_CASE("Interpret Program executes preamble extern calls before main node") {
    ir::Builder builder;

    runtime::Table df;
    df.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("df", df);

    int init_calls = 0;
    runtime::ExternRegistry externs;
    externs.register_scalar(
        "init", runtime::ScalarKind::Int,
        [&](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            REQUIRE(args.size() == 1);
            REQUIRE(std::get<std::int64_t>(args[0]) == 7);
            ++init_calls;
            return runtime::ExternValue{runtime::ScalarValue{std::int64_t{0}}};
        });

    std::vector<ir::NodePtr> preamble;
    preamble.push_back(builder.extern_call("init", {ir::Expr{ir::Literal{std::int64_t{7}}}}));
    auto program = builder.program(std::move(preamble), builder.scan("df"));

    auto result = runtime::interpret(*program, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(init_calls == 1);
    REQUIRE(result->rows() == 3);
    REQUIRE(result->find("x") != nullptr);
}

TEST_CASE("Interpret filter with scalar predicate") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("t", static_cast<std::int64_t>(15));

    auto ir = require_ir("trades[filter price > t, select { price }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(price_ints->size() == 2);
    REQUIRE((*price_ints)[0] == 20);
    REQUIRE((*price_ints)[1] == 30);
}

TEST_CASE("Interpret filter with date literal") {
    runtime::Table table;
    table.add_column("day", Column<Date>{
                                date_from_ymd(2024, 1, 1),
                                date_from_ymd(2024, 1, 2),
                                date_from_ymd(2024, 1, 3),
                            });

    runtime::TableRegistry registry;
    registry.emplace("calendar", table);

    auto ir = require_ir("calendar[filter day >= date\"2024-01-02\", select { day }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* day_col = result->find("day");
    REQUIRE(day_col != nullptr);
    const auto* days = std::get_if<Column<Date>>(day_col);
    REQUIRE(days != nullptr);
    REQUIRE(days->size() == 2);
    REQUIRE((*days)[0].days == date_from_ymd(2024, 1, 2).days);
    REQUIRE((*days)[1].days == date_from_ymd(2024, 1, 3).days);
}

TEST_CASE("Interpret update alias") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{5, 7});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { p = price }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);

    const auto* price_col = result->find("price");
    const auto* alias_col = result->find("p");
    REQUIRE(price_col != nullptr);
    REQUIRE(alias_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    const auto* alias_ints = std::get_if<Column<std::int64_t>>(alias_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(alias_ints != nullptr);
    REQUIRE(price_ints->size() == alias_ints->size());
    REQUIRE((*alias_ints)[0] == 5);
    REQUIRE((*alias_ints)[1] == 7);
}

TEST_CASE("Interpret rank shorthand with dense descending order") {
    runtime::Table table;
    table.add_column("score", Column<std::int64_t>{100, 100, 90, 80});

    runtime::TableRegistry registry;
    registry.emplace("scores", table);

    auto ir =
        require_ir(R"(scores[update { r = rank(score, method = dense, ascending = false) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* ranks = std::get_if<Column<std::int64_t>>(result->find("r"));
    REQUIRE(ranks != nullptr);
    REQUIRE(ranks->size() == 4);
    REQUIRE((*ranks)[0] == 1);
    REQUIRE((*ranks)[1] == 1);
    REQUIRE((*ranks)[2] == 2);
    REQUIRE((*ranks)[3] == 3);
}

TEST_CASE("Interpret rank with explicit multi-key order") {
    runtime::Table table;
    table.add_column("score", Column<std::int64_t>{100, 100, 90, 90});
    table.add_column("ts", Column<std::int64_t>{2, 1, 5, 3});

    runtime::TableRegistry registry;
    registry.emplace("scores", table);

    auto ir =
        require_ir(R"(scores[update { r = rank(order { score desc, ts asc }, method = first) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* ranks = std::get_if<Column<std::int64_t>>(result->find("r"));
    REQUIRE(ranks != nullptr);
    REQUIRE(ranks->size() == 4);
    REQUIRE((*ranks)[0] == 2);
    REQUIRE((*ranks)[1] == 1);
    REQUIRE((*ranks)[2] == 4);
    REQUIRE((*ranks)[3] == 3);
}

TEST_CASE("Interpret grouped rank with dense descending order") {
    runtime::Table table;
    table.add_column("dept", Column<std::string>{"A", "A", "A", "B", "B"});
    table.add_column("score", Column<std::int64_t>{100, 90, 90, 50, 40});

    runtime::TableRegistry registry;
    registry.emplace("scores", table);

    auto ir = require_ir(
        R"(scores[update { r = rank(score, method = dense, ascending = false) }, by dept];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* ranks = std::get_if<Column<std::int64_t>>(result->find("r"));
    REQUIRE(ranks != nullptr);
    REQUIRE(ranks->size() == 5);
    REQUIRE((*ranks)[0] == 1);
    REQUIRE((*ranks)[1] == 2);
    REQUIRE((*ranks)[2] == 2);
    REQUIRE((*ranks)[3] == 1);
    REQUIRE((*ranks)[4] == 2);
}

TEST_CASE("Interpret compile-time map expansion in aggregate select") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "A", "B"});
    table.add_column("price", Column<double>{10.0, 14.0, 20.0});
    table.add_column("fee", Column<double>{1.0, 3.0, 5.0});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir(R"(
let measures = ["price", "fee"];
trades[
    select { symbol, map m in measures => `avg_${m}` = mean(get(m)) },
    by symbol,
    order symbol
];
)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& sym = std::get<Column<std::string>>(*result->find("symbol"));
    const auto& avg_price = std::get<Column<double>>(*result->find("avg_price"));
    const auto& avg_fee = std::get<Column<double>>(*result->find("avg_fee"));

    REQUIRE(sym.size() == 2);
    REQUIRE(sym[0] == "A");
    REQUIRE(sym[1] == "B");
    REQUIRE(avg_price[0] == Catch::Approx(12.0));
    REQUIRE(avg_price[1] == Catch::Approx(20.0));
    REQUIRE(avg_fee[0] == Catch::Approx(2.0));
    REQUIRE(avg_fee[1] == Catch::Approx(5.0));
}

TEST_CASE("Interpret columns returns one-column metadata table") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B"});
    table.add_column("price", Column<double>{10.0, 20.0});
    table.add_column("qty", Column<std::int64_t>{1, 2});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("columns(trades);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);
    REQUIRE(result->columns[0].name == "name");

    const auto& names = std::get<Column<std::string>>(*result->find("name"));
    REQUIRE(names.size() == 3);
    REQUIRE(names[0] == "symbol");
    REQUIRE(names[1] == "price");
    REQUIRE(names[2] == "qty");
}

TEST_CASE("Interpret update with arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { price = price + 1 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(price_ints->size() == 3);
    REQUIRE((*price_ints)[0] == 2);
    REQUIRE((*price_ints)[1] == 3);
    REQUIRE((*price_ints)[2] == 4);
}

TEST_CASE("Interpret distinct") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 10, 20, 20});
    table.add_column("symbol", Column<std::string>{"A", "A", "B", "B"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[distinct { symbol, price }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);

    const auto* symbol_col = result->find("symbol");
    const auto* price_col = result->find("price");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(price_col != nullptr);
    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* prices = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices != nullptr);
    REQUIRE(symbols->size() == 2);
    REQUIRE(prices->size() == 2);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*prices)[1] == 20);
}

TEST_CASE("Interpret order") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{20, 10, 20});
    table.add_column("symbol", Column<std::string>{"B", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[order { price asc, symbol asc }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* price_col = result->find("price");
    const auto* symbol_col = result->find("symbol");
    REQUIRE(price_col != nullptr);
    REQUIRE(symbol_col != nullptr);
    const auto* prices = std::get_if<Column<std::int64_t>>(price_col);
    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    REQUIRE(prices != nullptr);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*symbols)[1] == "A");
    REQUIRE((*prices)[2] == 20);
    REQUIRE((*symbols)[2] == "B");
}

TEST_CASE("Interpret select with function call") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ExternRegistry externs;
    externs.register_scalar(
        "square", runtime::ScalarKind::Int,
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("square() expects 1 argument");
            }
            const auto* value = std::get_if<std::int64_t>(args.data());
            if (value == nullptr) {
                return std::unexpected("square() expects int argument");
            }
            return runtime::ExternValue{(*value) * (*value)};
        });

    auto ir = require_ir("trades[select { foo = square(price) }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);

    const auto* foo_col = result->find("foo");
    REQUIRE(foo_col != nullptr);
    const auto* foo_ints = std::get_if<Column<std::int64_t>>(foo_col);
    REQUIRE(foo_ints != nullptr);
    REQUIRE(foo_ints->size() == 2);
    REQUIRE((*foo_ints)[0] == 4);
    REQUIRE((*foo_ints)[1] == 9);
}

TEST_CASE("Interpret grouped aggregation") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "C"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, total = sum(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* total_col = result->find("total");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(total_col != nullptr);

    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* totals = std::get_if<Column<std::int64_t>>(total_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(totals != nullptr);
    REQUIRE(symbols->size() == 3);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*totals)[0] == 40);
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*totals)[1] == 20);
    REQUIRE((*symbols)[2] == "C");
    REQUIRE((*totals)[2] == 25);
}

TEST_CASE("Interpret grouped aggregation many string keys interleaved",
          "[interpreter][aggregate][string]") {
    // Reproduces the benchmark shape: many distinct string keys (>>cache-line),
    // many rows per key, repeated/interleaved order. The bug to guard against
    // was that the string fast path produced one *result row per (key, run)*
    // instead of one per distinct key, because string_view keys stored in the
    // hash map were silently invalidated.
    constexpr std::size_t kSymbols = 252;
    constexpr std::size_t kRepeats = 16000;

    auto make_symbol = [](std::size_t i) -> std::string {
        std::string s(3, 'A');
        s[0] = static_cast<char>('A' + (i / (26 * 26)) % 26);
        s[1] = static_cast<char>('A' + (i / 26) % 26);
        s[2] = static_cast<char>('A' + i % 26);
        return s;
    };

    std::vector<std::string> symbols;
    std::vector<std::int64_t> prices;
    const std::size_t total_rows = kSymbols * kRepeats;
    symbols.reserve(total_rows);
    prices.reserve(total_rows);
    // Pseudo-random index sequence (LCG) so symbol order is shuffled like the
    // benchmark CSV — same symbol can appear many rows apart, matching the bug
    // shape where the string fast path mismatches re-encountered keys.
    std::uint64_t lcg = 0x9E3779B97F4A7C15ULL;
    for (std::size_t i = 0; i < total_rows; ++i) {
        lcg = (lcg * 6364136223846793005ULL) + 1442695040888963407ULL;
        const std::size_t s = static_cast<std::size_t>(lcg >> 32) % kSymbols;
        symbols.push_back(make_symbol(s));
        prices.push_back(static_cast<std::int64_t>(s + 1));
    }
    std::vector<std::int64_t> expected_counts(kSymbols, 0);
    std::vector<std::int64_t> expected_sums(kSymbols, 0);
    for (std::size_t i = 0; i < total_rows; ++i) {
        const std::size_t s =
            static_cast<std::size_t>(static_cast<unsigned char>(symbols[i][0] - 'A')) * 26 * 26 +
            static_cast<std::size_t>(static_cast<unsigned char>(symbols[i][1] - 'A')) * 26 +
            static_cast<std::size_t>(static_cast<unsigned char>(symbols[i][2] - 'A'));
        expected_counts[s] += 1;
        expected_sums[s] += prices[i];
    }

    runtime::Table table;
    table.add_column("symbol", Column<std::string>(symbols));
    table.add_column("price", Column<std::int64_t>(prices));

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, n = count(), s = sum(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* n_col = result->find("n");
    const auto* s_col = result->find("s");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(n_col != nullptr);
    REQUIRE(s_col != nullptr);

    const auto* sym_strs = std::get_if<Column<std::string>>(symbol_col);
    const auto* counts = std::get_if<Column<std::int64_t>>(n_col);
    const auto* sums = std::get_if<Column<std::int64_t>>(s_col);
    REQUIRE(sym_strs != nullptr);
    REQUIRE(counts != nullptr);
    REQUIRE(sums != nullptr);

    REQUIRE(sym_strs->size() == kSymbols);
    REQUIRE(counts->size() == kSymbols);
    REQUIRE(sums->size() == kSymbols);

    // Every distinct symbol must appear exactly once with the expected counts.
    std::vector<bool> seen(kSymbols, false);
    for (std::size_t row = 0; row < sym_strs->size(); ++row) {
        const std::string_view sym = (*sym_strs)[row];
        REQUIRE(sym.size() == 3);
        std::size_t idx = (static_cast<std::size_t>(sym[0] - 'A') * 26 * 26) +
                          (static_cast<std::size_t>(sym[1] - 'A') * 26) +
                          static_cast<std::size_t>(sym[2] - 'A');
        REQUIRE(idx < kSymbols);
        REQUIRE_FALSE(seen[idx]);
        seen[idx] = true;
        CHECK((*counts)[row] == expected_counts[idx]);
        CHECK((*sums)[row] == expected_sums[idx]);
    }
}

TEST_CASE("Interpret aggregate arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "C"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, avg = sum(price) / count() }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* avg_col = result->find("avg");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(avg_col != nullptr);

    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* avgs = std::get_if<Column<double>>(avg_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(avgs != nullptr);
    REQUIRE(symbols->size() == 3);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*avgs)[0] == Catch::Approx(20.0));
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*avgs)[1] == Catch::Approx(20.0));
    REQUIRE((*symbols)[2] == "C");
    REQUIRE((*avgs)[2] == Catch::Approx(25.0));
}

TEST_CASE("Interpret first and last aggregation") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir(
        "trades[select { symbol, first_price = first(price), last_price = last(price) }, by "
        "symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* first_col = result->find("first_price");
    const auto* last_col = result->find("last_price");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(first_col != nullptr);
    REQUIRE(last_col != nullptr);

    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* firsts = std::get_if<Column<std::int64_t>>(first_col);
    const auto* lasts = std::get_if<Column<std::int64_t>>(last_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(firsts != nullptr);
    REQUIRE(lasts != nullptr);

    REQUIRE(symbols->size() == 2);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*firsts)[0] == 10);
    REQUIRE((*lasts)[0] == 25);
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*firsts)[1] == 20);
    REQUIRE((*lasts)[1] == 20);
}

TEST_CASE("Extract scalar from single-row table") {
    runtime::Table table;
    table.add_column("total", Column<std::int64_t>{42});

    auto result = runtime::extract_scalar(table, "total");
    REQUIRE(result.has_value());
    REQUIRE(std::get<std::int64_t>(result.value()) == 42);
}

TEST_CASE("Extract scalar errors on multi-row table") {
    runtime::Table table;
    table.add_column("total", Column<std::int64_t>{1, 2});

    auto result = runtime::extract_scalar(table, "total");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Interpret update with scalar reference") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("offset", std::int64_t{10});

    auto ir = require_ir("trades[update { price = price + offset }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(price_ints->size() == 3);
    REQUIRE((*price_ints)[0] == 11);
    REQUIRE((*price_ints)[1] == 12);
    REQUIRE((*price_ints)[2] == 13);
}

TEST_CASE("Interpret update with bool scalar reference") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("enabled", true);

    auto ir = require_ir("trades[update { keep = enabled }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());

    const auto* keep_col = result->find("keep");
    REQUIRE(keep_col != nullptr);
    const auto* keep_bools = std::get_if<Column<bool>>(keep_col);
    REQUIRE(keep_bools != nullptr);
    REQUIRE(keep_bools->size() == 3);
    REQUIRE((*keep_bools)[0] == true);
    REQUIRE((*keep_bools)[1] == true);
    REQUIRE((*keep_bools)[2] == true);
}

TEST_CASE("Interpret order descending") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 30, 20});
    table.add_column("symbol", Column<std::string>{"A", "B", "C"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[order { price desc }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 30);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 10);
}

TEST_CASE("Interpret order on empty table") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[order { price asc }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Interpret global head preserves current order") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{10, 20, 30, 40});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[head 2];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE(x->size() == 2);
    REQUIRE((*x)[0] == 10);
    REQUIRE((*x)[1] == 20);
}

TEST_CASE("Interpret global head over chunked extern source stops after requested rows") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    int source_calls = 0;
    externs.register_chunked_table("stream_nums", [&](const runtime::ExternArgs&) {
        ++source_calls;
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_int_chunk("x", {10, 20, 30}));
        chunks.push_back(make_int_chunk("x", {40, 50, 60}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir =
        require_ir("extern fn stream_nums() -> DataFrame from \"x.hpp\"; stream_nums()[head 4];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(source_calls == 1);

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE(x->size() == 4);
    REQUIRE((*x)[0] == 10);
    REQUIRE((*x)[1] == 20);
    REQUIRE((*x)[2] == 30);
    REQUIRE((*x)[3] == 40);
}

TEST_CASE("Interpret grouped head keeps first rows per group in encounter order") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "A", "B"});
    table.add_column("x", Column<std::int64_t>{10, 20, 30, 40, 50});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[head 2, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(symbol != nullptr);
    REQUIRE(x != nullptr);
    REQUIRE(symbol->size() == 4);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*x)[0] == 10);
    REQUIRE((*symbol)[1] == "B");
    REQUIRE((*x)[1] == 20);
    REQUIRE((*symbol)[2] == "A");
    REQUIRE((*x)[2] == 30);
    REQUIRE((*symbol)[3] == "B");
    REQUIRE((*x)[3] == 50);
}

TEST_CASE("Interpret grouped head over chunked extern source tracks groups across chunks") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("stream_grouped", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_str_int_chunk("symbol", {"A", "B", "A"}, "x", {10, 20, 30}));
        chunks.push_back(make_str_int_chunk("symbol", {"A", "B", "C"}, "x", {40, 50, 60}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_grouped() -> DataFrame from \"x.hpp\"; "
        "stream_grouped()[head 2, by symbol];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(symbol != nullptr);
    REQUIRE(x != nullptr);
    REQUIRE(symbol->size() == 5);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*x)[0] == 10);
    REQUIRE((*symbol)[1] == "B");
    REQUIRE((*x)[1] == 20);
    REQUIRE((*symbol)[2] == "A");
    REQUIRE((*x)[2] == 30);
    REQUIRE((*symbol)[3] == "B");
    REQUIRE((*x)[3] == 50);
    REQUIRE((*symbol)[4] == "C");
    REQUIRE((*x)[4] == 60);
}

TEST_CASE("Interpret ordered head over chunked extern source keeps top-k without full sort") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("stream_unsorted", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_int_chunk("x", {5, 1, 7}));
        chunks.push_back(make_int_chunk("x", {3, 9, 2}));
        chunks.push_back(make_int_chunk("x", {8, 4, 6}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_unsorted() -> DataFrame from \"x.hpp\"; "
        "stream_unsorted()[order x desc, head 4];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE(x->size() == 4);
    REQUIRE((*x)[0] == 9);
    REQUIRE((*x)[1] == 8);
    REQUIRE((*x)[2] == 7);
    REQUIRE((*x)[3] == 6);
}

TEST_CASE("Interpret ordered grouped head returns top-k per group") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "A", "B", "A", "B"});
    table.add_column("score", Column<std::int64_t>{5, 7, 1, 6, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[order score desc, head 2, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* score = std::get_if<Column<std::int64_t>>(result->find("score"));
    REQUIRE(symbol != nullptr);
    REQUIRE(score != nullptr);
    REQUIRE(symbol->size() == 4);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*score)[0] == 7);
    REQUIRE((*symbol)[1] == "A");
    REQUIRE((*score)[1] == 6);
    REQUIRE((*symbol)[2] == "B");
    REQUIRE((*score)[2] == 3);
    REQUIRE((*symbol)[3] == "B");
    REQUIRE((*score)[3] == 1);
}

TEST_CASE("Interpret ordered grouped head over chunked extern source keeps top-k per group") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("stream_grouped_scores", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_str_int_chunk("symbol", {"A", "B", "A"}, "score", {5, 2, 9}));
        chunks.push_back(make_str_int_chunk("symbol", {"B", "C", "A"}, "score", {8, 1, 7}));
        chunks.push_back(make_str_int_chunk("symbol", {"C", "B", "C"}, "score", {6, 4, 3}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_grouped_scores() -> DataFrame from \"x.hpp\"; "
        "stream_grouped_scores()[order score desc, head 2, by symbol];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* score = std::get_if<Column<std::int64_t>>(result->find("score"));
    REQUIRE(symbol != nullptr);
    REQUIRE(score != nullptr);
    REQUIRE(symbol->size() == 6);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*score)[0] == 9);
    REQUIRE((*symbol)[1] == "B");
    REQUIRE((*score)[1] == 8);
    REQUIRE((*symbol)[2] == "A");
    REQUIRE((*score)[2] == 7);
    REQUIRE((*symbol)[3] == "C");
    REQUIRE((*score)[3] == 6);
    REQUIRE((*symbol)[4] == "B");
    REQUIRE((*score)[4] == 4);
    REQUIRE((*symbol)[5] == "C");
    REQUIRE((*score)[5] == 3);
}

TEST_CASE("Interpret ordered tail over chunked extern source keeps bottom-k without full sort") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("stream_unsorted", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_int_chunk("x", {5, 1, 7}));
        chunks.push_back(make_int_chunk("x", {3, 9, 2}));
        chunks.push_back(make_int_chunk("x", {8, 4, 6}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_unsorted() -> DataFrame from \"x.hpp\"; "
        "stream_unsorted()[order x desc, tail 4];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE(x->size() == 4);
    REQUIRE((*x)[0] == 4);
    REQUIRE((*x)[1] == 3);
    REQUIRE((*x)[2] == 2);
    REQUIRE((*x)[3] == 1);
}

TEST_CASE("Interpret ordered grouped tail over chunked extern source keeps bottom-k per group") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("stream_grouped_scores", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_str_int_chunk("symbol", {"A", "B", "A"}, "score", {5, 2, 9}));
        chunks.push_back(make_str_int_chunk("symbol", {"B", "C", "A"}, "score", {8, 1, 7}));
        chunks.push_back(make_str_int_chunk("symbol", {"C", "B", "C"}, "score", {6, 4, 3}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_grouped_scores() -> DataFrame from \"x.hpp\"; "
        "stream_grouped_scores()[order score desc, tail 2, by symbol];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* score = std::get_if<Column<std::int64_t>>(result->find("score"));
    REQUIRE(symbol != nullptr);
    REQUIRE(score != nullptr);
    REQUIRE(symbol->size() == 6);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*score)[0] == 7);
    REQUIRE((*symbol)[1] == "A");
    REQUIRE((*score)[1] == 5);
    REQUIRE((*symbol)[2] == "B");
    REQUIRE((*score)[2] == 4);
    REQUIRE((*symbol)[3] == "C");
    REQUIRE((*score)[3] == 3);
    REQUIRE((*symbol)[4] == "B");
    REQUIRE((*score)[4] == 2);
    REQUIRE((*symbol)[5] == "C");
    REQUIRE((*score)[5] == 1);
}

TEST_CASE("Interpret distinct over chunked extern source keeps first occurrence order") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("stream_distinct_symbols", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_str_int_chunk("symbol", {"A", "B", "A"}, "score", {1, 2, 1}));
        chunks.push_back(make_str_int_chunk("symbol", {"C", "B", "D"}, "score", {3, 2, 4}));
        chunks.push_back(make_str_int_chunk("symbol", {"D", "E", "A"}, "score", {4, 5, 1}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_distinct_symbols() -> DataFrame from \"x.hpp\"; "
        "stream_distinct_symbols()[distinct { symbol, score }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* score = std::get_if<Column<std::int64_t>>(result->find("score"));
    REQUIRE(symbol != nullptr);
    REQUIRE(score != nullptr);
    REQUIRE(symbol->size() == 5);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*score)[0] == 1);
    REQUIRE((*symbol)[1] == "B");
    REQUIRE((*score)[1] == 2);
    REQUIRE((*symbol)[2] == "C");
    REQUIRE((*score)[2] == 3);
    REQUIRE((*symbol)[3] == "D");
    REQUIRE((*score)[3] == 4);
    REQUIRE((*symbol)[4] == "E");
    REQUIRE((*score)[4] == 5);
}

TEST_CASE("Interpret global tail preserves current order of last rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{10, 20, 30, 40});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[tail 2];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE(x->size() == 2);
    REQUIRE((*x)[0] == 30);
    REQUIRE((*x)[1] == 40);
}

TEST_CASE("Interpret grouped tail keeps last rows per group in encounter order") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "A", "B"});
    table.add_column("x", Column<std::int64_t>{10, 20, 30, 40, 50});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[tail 2, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(symbol != nullptr);
    REQUIRE(x != nullptr);
    REQUIRE(symbol->size() == 4);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*x)[0] == 30);
    REQUIRE((*symbol)[1] == "A");
    REQUIRE((*x)[1] == 40);
    REQUIRE((*symbol)[2] == "B");
    REQUIRE((*x)[2] == 20);
    REQUIRE((*symbol)[3] == "B");
    REQUIRE((*x)[3] == 50);
}

TEST_CASE("Interpret ordered grouped tail returns bottom-k per group in current order") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "A", "B", "A", "B"});
    table.add_column("score", Column<std::int64_t>{5, 7, 1, 6, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[order score desc, tail 2, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* score = std::get_if<Column<std::int64_t>>(result->find("score"));
    REQUIRE(symbol != nullptr);
    REQUIRE(score != nullptr);
    REQUIRE(symbol->size() == 4);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*score)[0] == 6);
    REQUIRE((*symbol)[1] == "A");
    REQUIRE((*score)[1] == 5);
    REQUIRE((*symbol)[2] == "B");
    REQUIRE((*score)[2] == 3);
    REQUIRE((*symbol)[3] == "B");
    REQUIRE((*score)[3] == 1);
}

TEST_CASE("Interpret distinct on empty table") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[distinct { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Interpret order then distinct") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{20, 10, 20, 10});
    table.add_column("symbol", Column<std::string>{"B", "A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // order first so distinct result order is deterministic
    auto ir = require_ir("trades[order { price asc, symbol asc }, distinct { price, symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    const auto* symbols = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(prices != nullptr);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices->size() == 2);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*symbols)[1] == "B");
}

TEST_CASE("Interpret distinct preserves first occurrence without order") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "C", "B"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[distinct { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbols = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(symbols != nullptr);
    REQUIRE(symbols->size() == 3);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*symbols)[2] == "C");
}

TEST_CASE("Interpret filter on empty table") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 10];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Interpret aggregate on single row") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{42});
    table.add_column("symbol", Column<std::string>{"X"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, total = sum(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* totals = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(totals != nullptr);
    REQUIRE(totals->size() == 1);
    REQUIRE((*totals)[0] == 42);
}

TEST_CASE("Interpret update with double arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<double>{1.5, 2.5, 3.5});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { price_x2 = price * 2 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = std::get_if<Column<double>>(result->find("price_x2"));
    REQUIRE(col != nullptr);
    REQUIRE(col->size() == 3);
    REQUIRE((*col)[0] == Catch::Approx(3.0));
    REQUIRE((*col)[1] == Catch::Approx(5.0));
    REQUIRE((*col)[2] == Catch::Approx(7.0));
}

TEST_CASE("Interpret update fuses nested numeric arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<double>{1.5, 2.5, 3.5});
    table.add_column("qty", Column<std::int64_t>{2, 3, 4});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { score = (price * 2) + (qty - 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = std::get_if<Column<double>>(result->find("score"));
    REQUIRE(col != nullptr);
    REQUIRE(col->size() == 3);
    REQUIRE((*col)[0] == Catch::Approx(4.0));
    REQUIRE((*col)[1] == Catch::Approx(7.0));
    REQUIRE((*col)[2] == Catch::Approx(10.0));
}

TEST_CASE("Interpret blocked nested numeric update across block boundaries") {
    constexpr std::size_t rows = 513;
    Column<double> price;
    Column<double> discount;
    Column<double> tax;
    Column<std::int64_t> quantity;
    Column<std::int64_t> divisor;
    price.reserve(rows);
    discount.reserve(rows);
    tax.reserve(rows);
    quantity.reserve(rows);
    divisor.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        price.push_back(10.0 + static_cast<double>(i % 31));
        discount.push_back(static_cast<double>(i % 7) / 100.0);
        tax.push_back(static_cast<double>(i % 5) / 100.0);
        quantity.push_back(static_cast<std::int64_t>(i % 19));
        divisor.push_back(i % 11 == 0 ? 0 : static_cast<std::int64_t>((i % 5) + 1));
    }

    runtime::Table table;
    table.add_column("price", price);
    table.add_column("discount", discount);
    table.add_column("tax", tax);
    table.add_column("quantity", quantity);
    table.add_column("divisor", divisor);
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    auto ir = require_ir(
        "t[update { revenue = price * (1.0 - discount) * (1.0 + tax), "
        "mixed = (quantity + 2) * price, "
        "int_expr = ((quantity % divisor) + 3) * (quantity - 1), "
        "rounded = round((price * 2.0) + discount, nearest), "
        "clipped = pmin(pmax(price + tax, -3.0), 35.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& revenue = std::get<Column<double>>(*result->find("revenue"));
    const auto& mixed = std::get<Column<double>>(*result->find("mixed"));
    const auto& int_expr = std::get<Column<std::int64_t>>(*result->find("int_expr"));
    const auto& rounded = std::get<Column<std::int64_t>>(*result->find("rounded"));
    const auto& clipped = std::get<Column<double>>(*result->find("clipped"));
    for (std::size_t i = 0; i < rows; ++i) {
        const double p = price[i];
        const double d = discount[i];
        const double t = tax[i];
        const std::int64_t q = quantity[i];
        const std::int64_t div = divisor[i];
        const std::int64_t expected_mod = div == 0 ? 0 : q % div;
        CHECK(revenue[i] == Catch::Approx(p * (1.0 - d) * (1.0 + t)));
        CHECK(mixed[i] == Catch::Approx(static_cast<double>(q + 2) * p));
        CHECK(int_expr[i] == (expected_mod + 3) * (q - 1));
        CHECK(rounded[i] == static_cast<std::int64_t>(std::llround((p * 2.0) + d)));
        CHECK(clipped[i] == Catch::Approx(std::min(std::max(p + t, -3.0), 35.0)));
    }
}

TEST_CASE("Interpret update fused numeric arithmetic preserves int div and mod semantics") {
    runtime::Table table;
    table.add_column("a", Column<std::int64_t>{10, std::numeric_limits<std::int64_t>::min(), 7});
    table.add_column("b", Column<std::int64_t>{3, -1, 0});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { q = (a / b) + 1, r = (a % b) + 2 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* q = std::get_if<Column<double>>(result->find("q"));
    REQUIRE(q != nullptr);
    REQUIRE((*q)[0] == Catch::Approx((10.0 / 3.0) + 1.0));
    REQUIRE(
        (*q)[1] ==
        Catch::Approx(static_cast<double>(std::numeric_limits<std::int64_t>::min()) / -1.0 + 1.0));
    REQUIRE(std::isinf((*q)[2]));

    const auto* r = std::get_if<Column<std::int64_t>>(result->find("r"));
    REQUIRE(r != nullptr);
    CHECK((*r)[0] == 3);
    CHECK((*r)[1] == 2);
    CHECK((*r)[2] == 2);
}

TEST_CASE("Interpret update rejects out-of-range int64 to Date coercion") {
    runtime::Table table;
    table.add_column("id", Column<std::int64_t>{1});

    runtime::TableRegistry registry;
    registry.emplace("rows", table);

    runtime::ExternRegistry externs;
    externs.register_scalar(
        "bad_date", runtime::ScalarKind::Date,
        [](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{runtime::ScalarValue{
                static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max()) + 1}};
        });

    auto ir = require_ir("rows[update { d = bad_date() }];");
    REQUIRE_THROWS(runtime::interpret(*ir, registry, nullptr, &externs));
}

TEST_CASE("Interpret compound filter: AND") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{5, 15, 25});
    table.add_column("qty", Column<std::int64_t>{3, 8, 2});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // price > 10 && qty < 5 -> only row with price=25, qty=2 passes
    auto ir = require_ir("trades[filter price > 10 && qty < 5];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 25);
}

TEST_CASE("Interpret compound filter: OR") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B", "C"});
    table.add_column("price", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // symbol == "A" || symbol == "C"
    auto ir = require_ir("trades[filter symbol == \"A\" || symbol == \"C\"];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* syms = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(syms != nullptr);
    REQUIRE((*syms)[0] == "A");
    REQUIRE((*syms)[1] == "C");
}

TEST_CASE("Interpret compound filter: arithmetic in predicate") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 60, 40});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // price * 2 > 100 -> only price=60 passes
    auto ir = require_ir("trades[filter price * 2 > 100];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 60);
}

TEST_CASE("Interpret compound filter: NOT") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // !(price > 15) -> price=10 passes
    auto ir = require_ir("trades[filter !(price > 15)];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
}

TEST_CASE("Interpret compound filter: three-way AND") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{5, 15, 25, 35});
    table.add_column("qty", Column<std::int64_t>{1, 4, 3, 2});
    table.add_column("flag", Column<std::int64_t>{1, 1, 0, 1});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // price > 10 && qty < 5 && flag == 1 -> price=15 (qty=4,flag=1) and price=35 (qty=2,flag=1)
    auto ir = require_ir("trades[filter price > 10 && qty < 5 && flag == 1];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 15);
    REQUIRE((*prices)[1] == 35);
}

// --- TimeFrame / as_timeframe tests ------------------------------------------

TEST_CASE("as_timeframe on Timestamp column sets time_index and sorts ascending") {
    runtime::Table table;
    table.add_column("ts",
                     Column<Timestamp>{ts_from_nanos(300), ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{30, 10, 20});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "ts");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");

    const auto* ts_col = std::get_if<Column<Timestamp>>(result->find("ts"));
    REQUIRE(ts_col != nullptr);
    REQUIRE((*ts_col)[0].nanos == 100);
    REQUIRE((*ts_col)[1].nanos == 200);
    REQUIRE((*ts_col)[2].nanos == 300);
}

TEST_CASE("as_timeframe on Date column sets time_index and sorts ascending") {
    runtime::Table table;
    table.add_column("day", Column<Date>{date_from_ymd(2024, 1, 3), date_from_ymd(2024, 1, 1),
                                         date_from_ymd(2024, 1, 2)});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "day");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "day");

    const auto* day_col = std::get_if<Column<Date>>(result->find("day"));
    REQUIRE(day_col != nullptr);
    REQUIRE((*day_col)[0].days == date_from_ymd(2024, 1, 1).days);
    REQUIRE((*day_col)[1].days == date_from_ymd(2024, 1, 2).days);
    REQUIRE((*day_col)[2].days == date_from_ymd(2024, 1, 3).days);
}

TEST_CASE("as_timeframe on non-existent column returns error") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "missing");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("not found") != std::string::npos);
}

TEST_CASE("as_timeframe on non-timestamp column returns error") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 20.0, 30.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "price");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("must be Timestamp, Date, or Int") != std::string::npos);
}

TEST_CASE("as_timeframe on Int column treats values as nanoseconds") {
    runtime::Table table;
    // 0 ns, 1s, 2s
    table.add_column("ts", Column<std::int64_t>{0, 1'000'000'000LL, 2'000'000'000LL});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "ts");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index == "ts");
    REQUIRE(result->rows() == 3);
    // ts column should now be Timestamp
    REQUIRE(result->find("ts") != nullptr);
    REQUIRE(std::holds_alternative<Column<Timestamp>>(*result->find("ts")));
}

TEST_CASE("Filter on TimeFrame preserves time_index") {
    runtime::Table table;
    table.add_column("ts",
                     Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200), ts_from_nanos(300)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[filter val > 15];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
    REQUIRE(result->rows() == 2);
}

TEST_CASE("Project keeping timestamp col preserves time_index") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[select { ts, val }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
}

TEST_CASE("Project dropping timestamp col clears time_index") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[select { val }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE_FALSE(result->time_index.has_value());
}

TEST_CASE("Order by non-time-col on TimeFrame returns error") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[order val asc];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("order on TimeFrame") != std::string::npos);
}

TEST_CASE("Window on plain DataFrame returns TimeFrame error") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // window + update on a plain DataFrame (no time_index) -> error
    auto ir = require_ir("data[window 5m, update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a TimeFrame") != std::string::npos);
}

TEST_CASE("Window with no update clause returns unsupported error") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // window without any update node -> "only 'update' is currently supported"
    auto ir = require_ir("data[window 5m];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("only 'update'") != std::string::npos);
}

// --- lag / lead tests ---------------------------------------------------------

TEST_CASE("lag(val, 1) on TimeFrame shifts values and fills default at start") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { prev = lag(val, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prev = std::get_if<Column<std::int64_t>>(result->find("prev"));
    REQUIRE(prev != nullptr);
    REQUIRE((*prev)[0] == 0);  // default
    REQUIRE((*prev)[1] == 10);
    REQUIRE((*prev)[2] == 20);
}

TEST_CASE("lead(val, 1) on TimeFrame shifts values and fills default at end") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { nxt = lead(val, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* nxt = std::get_if<Column<std::int64_t>>(result->find("nxt"));
    REQUIRE(nxt != nullptr);
    REQUIRE((*nxt)[0] == 20);
    REQUIRE((*nxt)[1] == 30);
    REQUIRE((*nxt)[2] == 0);  // default
}

TEST_CASE("lag(val, 0) is identity") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1)});
    table.add_column("val", Column<std::int64_t>{42, 99});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { same = lag(val, 0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* same = std::get_if<Column<std::int64_t>>(result->find("same"));
    REQUIRE(same != nullptr);
    REQUIRE((*same)[0] == 42);
    REQUIRE((*same)[1] == 99);
}

TEST_CASE("lag nested in arithmetic in one update field", "[lag][nested]") {
    // (close - lag(close,1)) / lag(close,1) — a non-row-local call (lag) nested
    // inside arithmetic, in a single field. Row 0 is null (lag has no prior).
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3)});
    table.add_column("close", Column<double>{10.0, 11.0, 12.0, 13.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { ret = (close - lag(close,1)) / lag(close,1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* ret_entry = result->find_entry("ret");
    REQUIRE(ret_entry != nullptr);
    REQUIRE(ret_entry->validity.has_value());
    const auto& ret = std::get<Column<double>>(*ret_entry->column);
    REQUIRE(ret.size() == 4);
    CHECK((*ret_entry->validity)[0] == false);  // lag(close,1)[0] is null -> ret null
    CHECK((*ret_entry->validity)[1] == true);
    CHECK(ret[1] == Catch::Approx(1.0 / 10.0));
    CHECK(ret[2] == Catch::Approx(1.0 / 11.0));
    CHECK(ret[3] == Catch::Approx(1.0 / 12.0));
}

TEST_CASE("scalar function wrapping lag preserves the null", "[lag][nested]") {
    // abs(lag(c,1)) — a transform inside a scalar call. The scalar path can't
    // see lag, so the argument is materialized; row 0 stays null.
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("c", Column<double>{-5.0, 10.0, -3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { a = abs(lag(c,1)) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* a_entry = result->find_entry("a");
    REQUIRE(a_entry != nullptr);
    REQUIRE(a_entry->validity.has_value());
    const auto& a = std::get<Column<double>>(*a_entry->column);
    CHECK((*a_entry->validity)[0] == false);
    CHECK(a[1] == Catch::Approx(5.0));   // abs(-5)
    CHECK(a[2] == Catch::Approx(10.0));  // abs(10)
}

TEST_CASE("RNG nested inside a scalar function in an update field", "[rng][nested]") {
    // abs(rand_normal(0,1)) — a Generator nested inside a scalar call. Every
    // value is the absolute value of a draw, so all are >= 0.
    runtime::reseed(42);
    runtime::Table table;
    table.add_column("k", Column<std::int64_t>{0, 0, 0, 0, 0, 0, 0, 0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { x = abs(rand_normal(0.0, 1.0)) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* x = std::get_if<Column<double>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE(x->size() == 8);
    for (double v : *x) {
        CHECK(v >= 0.0);
    }
}

TEST_CASE("lag on bool column preserves packed values", "[lag][bool]") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("flag", Column<bool>{true, false, true});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { prev = lag(flag, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prev = std::get_if<Column<bool>>(result->find("prev"));
    REQUIRE(prev != nullptr);
    REQUIRE((*prev)[0] == false);
    REQUIRE((*prev)[1] == true);
    REQUIRE((*prev)[2] == false);
}

TEST_CASE("lag on DataFrame uses current row order") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { prev = lag(val, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prev = std::get_if<Column<std::int64_t>>(result->find("prev"));
    REQUIRE(prev != nullptr);
    REQUIRE((*prev)[0] == 0);
    REQUIRE((*prev)[1] == 10);
    REQUIRE((*prev)[2] == 20);
}

TEST_CASE("lag offset accepts scalar expression") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30, 40});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("n", std::int64_t{1});

    auto ir = require_ir("data[update { prev = lag(val, n + 1) }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());

    const auto* prev = std::get_if<Column<std::int64_t>>(result->find("prev"));
    REQUIRE(prev != nullptr);
    REQUIRE((*prev)[0] == 0);
    REQUIRE((*prev)[1] == 0);
    REQUIRE((*prev)[2] == 10);
    REQUIRE((*prev)[3] == 20);
}

TEST_CASE("filter accepts lag and lead directly") {
    runtime::Table table;
    table.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7});
    table.add_column("num", Column<std::string>{"1", "1", "1", "2", "1", "1", "1"});

    runtime::TableRegistry registry;
    registry.emplace("logs", table);

    auto ir = require_ir(
        "logs[filter num == lag(num, 1) && num == lag(num, 2), distinct { ConsecutiveNums = num "
        "}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* consecutive = std::get_if<Column<std::string>>(result->find("ConsecutiveNums"));
    REQUIRE(consecutive != nullptr);
    REQUIRE((*consecutive)[0] == "1");
}

TEST_CASE("filter lag offset accepts scalar expression") {
    runtime::Table table;
    table.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7});
    table.add_column("num", Column<std::string>{"1", "1", "1", "2", "1", "1", "1"});

    runtime::TableRegistry registry;
    registry.emplace("logs", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("n", std::int64_t{1});

    auto ir = require_ir(
        "logs[filter num == lag(num, n) && num == lag(num, n + 1), distinct { ConsecutiveNums = "
        "num }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* consecutive = std::get_if<Column<std::string>>(result->find("ConsecutiveNums"));
    REQUIRE(consecutive != nullptr);
    REQUIRE((*consecutive)[0] == "1");
}

TEST_CASE("filter supports Date subtraction for lag day deltas") {
    runtime::Table table;
    table.add_column("id", Column<std::int64_t>{1, 2, 3, 4});
    table.add_column("recordDate",
                     Column<Date>{date_from_ymd(2015, 1, 1), date_from_ymd(2015, 1, 2),
                                  date_from_ymd(2015, 1, 3), date_from_ymd(2015, 1, 4)});
    table.add_column("temperature", Column<std::int64_t>{10, 25, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("weather", table);

    auto ir = require_ir(
        "weather[order recordDate][filter (recordDate - lag(recordDate, 1)) == 1 && temperature > "
        "lag(temperature, 1), select { Id = id }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* ids = std::get_if<Column<std::int64_t>>(result->find("Id"));
    REQUIRE(ids != nullptr);
    REQUIRE((*ids)[0] == 2);
    REQUIRE((*ids)[1] == 4);
}

// --- cumsum / cumprod tests ---------------------------------------------------

TEST_CASE("cumsum on Int column produces running sum") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3, 4});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cs = std::get_if<Column<std::int64_t>>(result->find("cs"));
    REQUIRE(cs != nullptr);
    REQUIRE((*cs)[0] == 1);
    REQUIRE((*cs)[1] == 3);
    REQUIRE((*cs)[2] == 6);
    REQUIRE((*cs)[3] == 10);
}

TEST_CASE("cumsum on Float column produces running sum") {
    runtime::Table table;
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cs = std::get_if<Column<double>>(result->find("cs"));
    REQUIRE(cs != nullptr);
    REQUIRE((*cs)[0] == Catch::Approx(1.0));
    REQUIRE((*cs)[1] == Catch::Approx(3.0));
    REQUIRE((*cs)[2] == Catch::Approx(6.0));
}

TEST_CASE("cumprod on Int column produces running product") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3, 4});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cp = cumprod(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cp = std::get_if<Column<std::int64_t>>(result->find("cp"));
    REQUIRE(cp != nullptr);
    REQUIRE((*cp)[0] == 1);
    REQUIRE((*cp)[1] == 2);
    REQUIRE((*cp)[2] == 6);
    REQUIRE((*cp)[3] == 24);
}

TEST_CASE("cumprod on Float column produces running product") {
    runtime::Table table;
    table.add_column("val", Column<double>{2.0, 3.0, 4.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cp = cumprod(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cp = std::get_if<Column<double>>(result->find("cp"));
    REQUIRE(cp != nullptr);
    REQUIRE((*cp)[0] == Catch::Approx(2.0));
    REQUIRE((*cp)[1] == Catch::Approx(6.0));
    REQUIRE((*cp)[2] == Catch::Approx(24.0));
}

TEST_CASE("cumsum on TimeFrame works without window clause") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cs = std::get_if<Column<std::int64_t>>(result->find("cs"));
    REQUIRE(cs != nullptr);
    REQUIRE((*cs)[0] == 10);
    REQUIRE((*cs)[1] == 30);
    REQUIRE((*cs)[2] == 60);
}

TEST_CASE("cumsum on non-numeric column returns error") {
    runtime::Table table;
    table.add_column("val", Column<std::string>{"a", "b", "c"});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("must be numeric") != std::string::npos);
}

TEST_CASE("rolling_sum outside window clause returns error") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a window clause") != std::string::npos);
}

TEST_CASE("update + by partitions lag per group with null boundary",
          "[interpreter][update][grouped]") {
    // Per-symbol lag is the canonical use case for `update + by`. Two things
    // matter: (1) the lag boundary is the group, not the table — symbol B's
    // first row never reaches into symbol A; (2) that boundary row is
    // *null*, not 0/empty, so downstream arithmetic doesn't silently produce
    // a meaningful-looking wrong number.
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(1), ts_from_nanos(2), ts_from_nanos(3),
                                             ts_from_nanos(1), ts_from_nanos(2), ts_from_nanos(3)});
    table.add_column("symbol", Column<std::string>{"A", "A", "A", "B", "B", "B"});
    table.add_column("close", Column<double>{10.0, 11.0, 12.0, 100.0, 105.0, 102.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("as_timeframe(data, \"ts\")[update { prev = lag(close, 1) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* prev_entry = result->find_entry("prev");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(prev_entry != nullptr);
    REQUIRE(prev_entry->validity.has_value());
    const auto& prev_col = std::get<Column<double>>(*prev_entry->column);
    REQUIRE(prev_col.size() == 6);
    const auto& prev_validity = *prev_entry->validity;

    std::map<std::string, std::vector<double>> expected_values = {
        {"A", {0.0, 10.0, 11.0}},
        {"B", {0.0, 100.0, 105.0}},
    };
    std::map<std::string, std::vector<bool>> expected_valid = {
        {"A", {false, true, true}},
        {"B", {false, true, true}},
    };
    std::map<std::string, std::size_t> seen;
    for (std::size_t r = 0; r < prev_col.size(); ++r) {
        std::string sym{(*symbol_col)[r]};
        std::size_t i = seen[sym]++;
        CHECK(prev_validity[r] == expected_valid.at(sym).at(i));
        if (expected_valid.at(sym).at(i)) {
            CHECK(prev_col[r] == Catch::Approx(expected_values.at(sym).at(i)));
        }
    }
}

TEST_CASE("per-row evaluation propagates null without reading payloads",
          "[interpreter][update][null]") {
    // The per-row evaluator represents a null cell as an ExprValue Null read
    // from the validity bitmap (never touching the undefined payload) and
    // propagates it: null in -> null out, through operators and scalar
    // builtins alike. String interpolation is per-row-only, so it pins the
    // path; abs() pins the registry-call propagation.
    runtime::Table table;
    table.add_column("x", Column<double>{1.5, 0.0, 3.5}, std::vector<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { msg = `v=${x}`, a = abs(x), y = x + 1.0 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    for (const auto* name : {"msg", "a", "y"}) {
        const auto* entry = result->find_entry(name);
        REQUIRE(entry != nullptr);
        REQUIRE(entry->validity.has_value());
        CHECK((*entry->validity)[0] == true);
        CHECK((*entry->validity)[1] == false);
        CHECK((*entry->validity)[2] == true);
    }
    const auto& msg = std::get<Column<std::string>>(*result->find("msg"));
    CHECK(msg[0] == "v=1.5");
    CHECK(msg[2] == "v=3.5");
    const auto& a = std::get<Column<double>>(*result->find("a"));
    CHECK(a[0] == Catch::Approx(1.5));
    CHECK(a[2] == Catch::Approx(3.5));
}

TEST_CASE("null-handling scalars evaluate per-row with NullPolicy::Handles",
          "[interpreter][update][null]") {
    // fill_null / null_if_* / coalesce are Scalar entries whose eval opts
    // into receiving Null (their column kernels remain as fast paths for the
    // bare-column form and must agree). Two things this pins: a computed
    // argument works (previously "argument must be a column name"), and
    // validity survives a null-handler nested inside an ordinary scalar call
    // (no post-hoc argument-mask AND re-nulling filled rows).
    runtime::Table table;
    table.add_column("x", Column<double>{1.0, 2.0, 3.0}, std::vector<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir =
        require_ir("t[update { filled = fill_null(x * 1.0, 9.0), af = abs(fill_null(x, 4.0)) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* filled_entry = result->find_entry("filled");
    REQUIRE(filled_entry != nullptr);
    CHECK_FALSE(filled_entry->validity.has_value());  // every null filled
    const auto& filled = std::get<Column<double>>(*filled_entry->column);
    CHECK(filled[0] == Catch::Approx(1.0));
    CHECK(filled[1] == Catch::Approx(9.0));
    CHECK(filled[2] == Catch::Approx(3.0));

    const auto* af_entry = result->find_entry("af");
    REQUIRE(af_entry != nullptr);
    CHECK_FALSE(af_entry->validity.has_value());
    const auto& af = std::get<Column<double>>(*af_entry->column);
    CHECK(af[1] == Catch::Approx(4.0));
}

TEST_CASE("Int division is float division on every evaluation path",
          "[interpreter][update][grouped]") {
    // `/` yields Float64 regardless of operand types (only `%` stays
    // integral). The fused numeric and per-row paths always did this; the
    // vectorized arith_vec used to truncate Int/Int, so an aggregate-broadcast
    // field (sum(x*x)/sum(x)) and a vectorized field (coalesce(a,b)/b) gave
    // 3 where plain a/b gave 3.888...
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6});
    table.add_column("g", Column<std::int64_t>{1, 2, 1, 2, 1, 2});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    SECTION("aggregate-broadcast compound expression") {
        // g=1: sum(x*x)=35, sum(x)=9 -> 3.888...; g=2: 56/12 -> 4.666...
        auto ir = require_ir("data[update { w = sum(x*x) / sum(x) }, by g];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* g = std::get_if<Column<std::int64_t>>(result->find("g"));
        const auto* w = std::get_if<Column<double>>(result->find("w"));
        REQUIRE(g != nullptr);
        REQUIRE(w != nullptr);
        for (std::size_t r = 0; r < w->size(); ++r) {
            CHECK((*w)[r] == Catch::Approx((*g)[r] == 1 ? 35.0 / 9.0 : 56.0 / 12.0));
        }
    }
    SECTION("vectorized value expression") {
        // coalesce forces the vectorized evaluator; x/g must still be float.
        auto ir = require_ir("data[update { d = coalesce(x, g) / g }];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
        const auto* g = std::get_if<Column<std::int64_t>>(result->find("g"));
        const auto* d = std::get_if<Column<double>>(result->find("d"));
        REQUIRE(x != nullptr);
        REQUIRE(g != nullptr);
        REQUIRE(d != nullptr);
        for (std::size_t r = 0; r < d->size(); ++r) {
            CHECK((*d)[r] ==
                  Catch::Approx(static_cast<double>((*x)[r]) / static_cast<double>((*g)[r])));
        }
    }
}

TEST_CASE("aggregates over an all-null column reduce to null, not garbage",
          "[interpreter][aggregate][null]") {
    // An aggregate with no valid observations has no value: first/last (which
    // used to emit a valid default 0) and the compound scalar-collapse path
    // (which used to read the null result's undefined payload and broadcast
    // it as a valid nan) must all produce null. Mixed groups stay exact.
    runtime::Table table;
    table.add_column("x", Column<double>{1.0, 0.0, 2.0, 0.0},
                     std::vector<bool>{true, false, true, false});
    table.add_column("g", Column<std::int64_t>{1, 2, 1, 2});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    SECTION("select: first/last/mean on the all-null slice") {
        auto ir = require_ir("t[select { f = first(x), l = last(x), m = mean(x) }, by g];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* g = std::get_if<Column<std::int64_t>>(result->find("g"));
        REQUIRE(g != nullptr);
        for (const auto* name : {"f", "l", "m"}) {
            const auto* entry = result->find_entry(name);
            REQUIRE(entry != nullptr);
            REQUIRE(entry->validity.has_value());
            for (std::size_t r = 0; r < g->size(); ++r) {
                CHECK((*entry->validity)[r] == ((*g)[r] == 1));  // group 2 is all null
            }
        }
    }
    SECTION("grouped update: bare, compound, and mixed broadcast") {
        auto ir =
            require_ir("t[update { w = mean(x), v = sum(x) / count(), d = x - mean(x) }, by g];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* g = std::get_if<Column<std::int64_t>>(result->find("g"));
        REQUIRE(g != nullptr);
        for (const auto* name : {"w", "v", "d"}) {
            const auto* entry = result->find_entry(name);
            REQUIRE(entry != nullptr);
            REQUIRE(entry->validity.has_value());
            for (std::size_t r = 0; r < g->size(); ++r) {
                CHECK((*entry->validity)[r] == ((*g)[r] == 1));
            }
        }
        const auto& w = std::get<Column<double>>(*result->find("w"));
        const auto& v = std::get<Column<double>>(*result->find("v"));
        for (std::size_t r = 0; r < g->size(); ++r) {
            if ((*g)[r] == 1) {
                CHECK(w[r] == Catch::Approx(1.5));  // mean{1, 2}
                CHECK(v[r] == Catch::Approx(1.5));  // sum 3 / count 2
            }
        }
    }
    SECTION("extract_scalar refuses a null cell") {
        auto ir = require_ir("t[select { f = first(x) }, by g];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        // Reduce to the all-null group's single row, then extract.
        runtime::Table one_row;
        const auto* g = std::get_if<Column<std::int64_t>>(result->find("g"));
        const auto* f_entry = result->find_entry("f");
        REQUIRE(g != nullptr);
        REQUIRE(f_entry != nullptr);
        std::size_t null_row = (*g)[0] == 2 ? 0 : 1;
        Column<double> f_col;
        f_col.push_back(std::get<Column<double>>(*f_entry->column)[null_row]);
        one_row.add_column("f", runtime::ColumnValue{std::move(f_col)},
                           runtime::ValidityBitmap(1, false));
        auto scalar = runtime::extract_scalar(one_row, "f");
        REQUIRE_FALSE(scalar.has_value());
        CHECK(scalar.error().find("null") != std::string::npos);
    }
}

TEST_CASE("short generator errors instead of leaving a truncated column",
          "[interpreter][update][generator]") {
    // rep with length_out shorter than the frame used to add a short column
    // whose missing tail read out-of-bounds garbage; now it errors. The
    // rows == 0 data-gen bootstrap (growing an empty frame) stays allowed.
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    SECTION("length_out mismatch on a populated frame errors") {
        auto ir = require_ir("t[update { r = rep(9, length_out=2) }];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("generates 2 rows") != std::string::npos);
    }
    SECTION("generator on an empty frame grows it") {
        runtime::TableRegistry empty_registry;
        empty_registry.emplace("e", runtime::Table{});
        auto ir = require_ir("e[update { r = rep(9, length_out=3) }];");
        auto result = runtime::interpret(*ir, empty_registry);
        REQUIRE(result.has_value());
        const auto* r = std::get_if<Column<std::int64_t>>(result->find("r"));
        REQUIRE(r != nullptr);
        CHECK(r->size() == 3);
    }
}

TEST_CASE("is_nan accepts a computed argument", "[interpreter][update][nan]") {
    // Previously the update path intercepted is_nan with a bare-column-only
    // kernel, so is_nan(x * 1.0) errored "argument must be a column name".
    // It now evaluates per-row through the scalar registry.
    runtime::Table table;
    table.add_column("x", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(), 3.0});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { bad = is_nan(x * 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* bad = std::get_if<Column<bool>>(result->find("bad"));
    REQUIRE(bad != nullptr);
    CHECK((*bad)[0] == false);
    CHECK((*bad)[1] == true);
    CHECK((*bad)[2] == false);
}

TEST_CASE("windowed update keeps validity on plain computed fields",
          "[interpreter][window][null]") {
    // A computed field inside `window ... update` must carry input nulls the
    // same way a regular update does. This used to silently drop validity:
    // the windowed fallthrough evaluated fields through a column-only helper
    // with no validity, so `val + 1` over a nullable column lost the null.
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(1), ts_from_nanos(2), ts_from_nanos(3)});
    table.add_column("val", Column<double>{10.0, 0.0, 30.0}, std::vector<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(
        "as_timeframe(data, \"ts\")[window 2ns, update { m = rolling_count(), shifted = val + 1.0 "
        "}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("shifted");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0] == true);
    CHECK((*entry->validity)[1] == false);
    CHECK((*entry->validity)[2] == true);
    const auto& shifted = std::get<Column<double>>(*entry->column);
    CHECK(shifted[0] == Catch::Approx(11.0));
    CHECK(shifted[2] == Catch::Approx(31.0));
}

TEST_CASE("windowed rolling partitions per `by` group", "[interpreter][window][grouped]") {
    // Symbol A: ts = {1,2,3} val = {10,20,30}; window 1ns -> mean = {10, 15, 25}
    // Symbol B: ts = {1,2,3} val = {100,200,300}; window 1ns -> mean = {100, 150, 250}
    // Rows are interleaved in the time-sorted output (A, B, A, B, A, B).
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(1), ts_from_nanos(2), ts_from_nanos(3),
                                             ts_from_nanos(1), ts_from_nanos(2), ts_from_nanos(3)});
    table.add_column("symbol", Column<std::string>{"A", "A", "A", "B", "B", "B"});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0, 100.0, 200.0, 300.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(
        "as_timeframe(data, \"ts\")[window 1ns, by symbol, "
        "update { m = rolling_mean(val), n = rolling_count() }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* mean_col = std::get_if<Column<double>>(result->find("m"));
    const auto* count_col = std::get_if<Column<std::int64_t>>(result->find("n"));
    REQUIRE(symbol_col != nullptr);
    REQUIRE(mean_col != nullptr);
    REQUIRE(count_col != nullptr);
    REQUIRE(mean_col->size() == 6);

    std::map<std::string, std::vector<double>> expected_means = {
        {"A", {10.0, 15.0, 25.0}},
        {"B", {100.0, 150.0, 250.0}},
    };
    std::map<std::string, std::vector<std::int64_t>> expected_counts = {
        {"A", {1, 2, 2}},
        {"B", {1, 2, 2}},
    };
    std::map<std::string, std::size_t> seen;
    for (std::size_t r = 0; r < mean_col->size(); ++r) {
        std::string sym{(*symbol_col)[r]};
        std::size_t i = seen[sym]++;
        CHECK((*mean_col)[r] == Catch::Approx(expected_means.at(sym).at(i)));
        CHECK((*count_col)[r] == expected_counts.at(sym).at(i));
    }
}

// --- rolling aggregate tests --------------------------------------------------
// Timestamps: 0ns, 1ns, 2ns  Values: 10, 20, 30
// Window 1ns: [t-1, t]
//   row 0 (t=0): [0-1,0]=[-1,0] -> only row 0
//   row 1 (t=1): [0,1]          -> rows 0,1
//   row 2 (t=2): [1,2]          -> rows 1,2

TEST_CASE("rolling_sum with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(*result->time_index == "ts");

    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    REQUIRE(s != nullptr);
    REQUIRE((*s)[0] == 10);  // row 0 only
    REQUIRE((*s)[1] == 30);  // rows 0+1
    REQUIRE((*s)[2] == 50);  // rows 1+2
}

TEST_CASE("rolling_mean with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { m = rolling_mean(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    REQUIRE((*m)[0] == Catch::Approx(10.0));
    REQUIRE((*m)[1] == Catch::Approx(15.0));
    REQUIRE((*m)[2] == Catch::Approx(25.0));
}

TEST_CASE("per-call count window: rolling_mean(val, 2) needs no TimeFrame", "[rolling][window]") {
    // A count window is valid on a plain frame with no time index and no
    // enclosing `window` clause.
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30, 40});
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { m = rolling_mean(val, 2) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[0] == Catch::Approx(10.0));  // {10}       (expanding at start)
    CHECK((*m)[1] == Catch::Approx(15.0));  // {10,20}
    CHECK((*m)[2] == Catch::Approx(25.0));  // {20,30}
    CHECK((*m)[3] == Catch::Approx(35.0));  // {30,40}
}

TEST_CASE("per-call count window: rolling_sum and rolling_count", "[rolling][window]") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { s = rolling_sum(val, 3), c = rolling_count(3) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    const auto* c = std::get_if<Column<std::int64_t>>(result->find("c"));
    REQUIRE(s != nullptr);
    REQUIRE(c != nullptr);
    CHECK((*s)[0] == 1);   // {1}
    CHECK((*s)[1] == 3);   // {1,2}
    CHECK((*s)[2] == 6);   // {1,2,3}
    CHECK((*s)[3] == 9);   // {2,3,4}
    CHECK((*s)[4] == 12);  // {3,4,5}
    CHECK((*c)[0] == 1);
    CHECK((*c)[1] == 2);
    CHECK((*c)[2] == 3);
    CHECK((*c)[3] == 3);
    CHECK((*c)[4] == 3);
}

TEST_CASE("per-call duration window matches the `window` block form", "[rolling][window]") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // rolling_mean(val, 1ns) as a per-call arg must equal `window 1ns`.
    auto ir = require_ir("data[update { m = rolling_mean(val, 1ns) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[0] == Catch::Approx(10.0));
    CHECK((*m)[1] == Catch::Approx(15.0));
    CHECK((*m)[2] == Catch::Approx(25.0));
}

TEST_CASE("per-call duration window without a TimeFrame is an error", "[rolling][window]") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { m = rolling_mean(val, 5s) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("rolling with no window clause and no per-call window is an error", "[rolling][window]") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { m = rolling_mean(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("rolling_mean skips NULL elements; their payload is never read", "[rolling][null]") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10), ts_from_nanos(20),
                                             ts_from_nanos(30)});
    // val[1] is NULL — its payload (a poison value) must be ignored, not summed.
    table.add_column("val", Column<double>{10.0, 1e300, 30.0, 40.0},
                     runtime::ValidityBitmap{true, false, true, true});
    table.time_index = "ts";
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1000ns, update { m = rolling_mean(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[0] == Catch::Approx(10.0));        // {10}
    CHECK((*m)[1] == Catch::Approx(10.0));        // {10, NULL} -> {10}
    CHECK((*m)[2] == Catch::Approx(20.0));        // {10, 30}
    CHECK((*m)[3] == Catch::Approx(80.0 / 3.0));  // {10, 30, 40}
    // No window was entirely null, so the result carries no nulls.
    CHECK_FALSE(result->find_entry("m")->validity.has_value());
}

TEST_CASE("rolling_mean: a window of only NULLs yields a NULL result", "[rolling][null]") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10)});
    table.add_column("val", Column<double>{7.0, 20.0}, runtime::ValidityBitmap{false, true});
    table.time_index = "ts";
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // 1ns window: each row sees only itself.
    auto ir = require_ir("data[window 1ns, update { m = rolling_mean(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* entry = result->find_entry("m");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK(runtime::is_null(*entry, 0));        // window {NULL} -> NULL
    CHECK_FALSE(runtime::is_null(*entry, 1));  // window {20} -> 20
    CHECK((*std::get_if<Column<double>>(result->find("m")))[1] == Catch::Approx(20.0));
}

TEST_CASE("rolling_mean: a real NaN propagates within its window, then recovers on exit",
          "[rolling][nan]") {
    // val[1] is a genuine NaN (validity=true) — distinct from a NULL. It must
    // poison only the windows it overlaps, then clear once it ages out (which the
    // running-sum approach can only do by keeping NaNs out of the accumulator).
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10), ts_from_nanos(20),
                                             ts_from_nanos(30)});
    const double nan = std::numeric_limits<double>::quiet_NaN();
    table.add_column("val", Column<double>{10.0, nan, 30.0, 40.0});  // all valid
    table.time_index = "ts";
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 15ns, update { m = rolling_mean(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[0] == Catch::Approx(10.0));  // {10}
    CHECK(std::isnan((*m)[1]));             // {10, NaN}  -> NaN
    CHECK(std::isnan((*m)[2]));             // {NaN, 30}  -> NaN
    CHECK((*m)[3] == Catch::Approx(35.0));  // {30, 40}   -> NaN aged out, recovered
    // A NaN is a present value, not a NULL: no validity bitmap on the result.
    CHECK_FALSE(result->find_entry("m")->validity.has_value());
}

TEST_CASE("rolling_sum/std/min skip NULLs consistently", "[rolling][null]") {
    runtime::Table table;
    table.add_column("ts",
                     Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10), ts_from_nanos(20)});
    table.add_column("vi", Column<std::int64_t>{10, 999, 30},
                     runtime::ValidityBitmap{true, false, true});
    table.add_column("vd", Column<double>{5.0, 1e300, 2.0},
                     runtime::ValidityBitmap{true, false, true});
    table.time_index = "ts";
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(
        "data[window 1000ns, update { s = rolling_sum(vi), sd = rolling_std(vd), "
        "mn = rolling_min(vd) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    REQUIRE(s != nullptr);
    CHECK((*s)[0] == 10);  // {10}
    CHECK((*s)[1] == 10);  // {10, NULL} -> {10}
    CHECK((*s)[2] == 40);  // {10, 30}

    const auto* mn = std::get_if<Column<double>>(result->find("mn"));
    REQUIRE(mn != nullptr);
    CHECK((*mn)[0] == Catch::Approx(5.0));  // {5}
    CHECK((*mn)[1] == Catch::Approx(5.0));  // {5, NULL} -> {5}
    CHECK((*mn)[2] == Catch::Approx(2.0));  // {5, 2}

    const auto* sd = std::get_if<Column<double>>(result->find("sd"));
    REQUIRE(sd != nullptr);
    CHECK((*sd)[2] == Catch::Approx(std::sqrt(4.5)));  // std{5,2} sample = sqrt(4.5)
}

TEST_CASE("grouped windowed rolling carries per-group input validity", "[rolling][null][grouped]") {
    // Two symbols interleaved; val is NULL on each symbol's first row. The grouped
    // windowed update must carry that validity into each per-symbol slice — else
    // rolling reads the undefined null payload and the result is garbage. This is
    // the log_return_momentum shape (lag → null first return) without coalesce.
    runtime::Table table;
    table.add_column("sym", Column<std::string>{"A", "B", "A", "B"});
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10), ts_from_nanos(20),
                                             ts_from_nanos(30)});
    table.add_column("val", Column<double>{1e300, 1e300, 30.0, 40.0},
                     runtime::ValidityBitmap{false, false, true, true});
    table.time_index = "ts";
    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1000ns, update { m = rolling_mean(val) }, by sym];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("m");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK(runtime::is_null(*entry, 0));        // A's first row: window {NULL} -> NULL
    CHECK(runtime::is_null(*entry, 1));        // B's first row: window {NULL} -> NULL
    CHECK_FALSE(runtime::is_null(*entry, 2));  // A: {NULL, 30} -> 30
    CHECK_FALSE(runtime::is_null(*entry, 3));  // B: {NULL, 40} -> 40
    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[2] == Catch::Approx(30.0));
    CHECK((*m)[3] == Catch::Approx(40.0));
}

TEST_CASE("rolling_count with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { c = rolling_count() }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* c = std::get_if<Column<std::int64_t>>(result->find("c"));
    REQUIRE(c != nullptr);
    REQUIRE((*c)[0] == 1);
    REQUIRE((*c)[1] == 2);
    REQUIRE((*c)[2] == 2);
}

TEST_CASE("rolling_min and rolling_max with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{30, 10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir_min = require_ir("data[window 1ns, update { mn = rolling_min(val) }];");
    auto res_min = runtime::interpret(*ir_min, registry);
    REQUIRE(res_min.has_value());
    const auto* mn = std::get_if<Column<std::int64_t>>(res_min->find("mn"));
    REQUIRE(mn != nullptr);
    REQUIRE((*mn)[0] == 30);  // row 0 only
    REQUIRE((*mn)[1] == 10);  // min(30,10)
    REQUIRE((*mn)[2] == 10);  // min(10,20)

    auto ir_max = require_ir("data[window 1ns, update { mx = rolling_max(val) }];");
    auto res_max = runtime::interpret(*ir_max, registry);
    REQUIRE(res_max.has_value());
    const auto* mx = std::get_if<Column<std::int64_t>>(res_max->find("mx"));
    REQUIRE(mx != nullptr);
    REQUIRE((*mx)[0] == 30);  // row 0 only
    REQUIRE((*mx)[1] == 30);  // max(30,10)
    REQUIRE((*mx)[2] == 20);  // max(10,20)
}

TEST_CASE("rolling_min and rolling_max handle sparse and full time windows", "[rolling][window]") {
    runtime::Table sparse;
    sparse.add_column("ts",
                      Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10), ts_from_nanos(20)});
    sparse.add_column("val", Column<std::int64_t>{30, 10, 20});
    sparse.time_index = "ts";

    runtime::Table full;
    Column<Timestamp> ts;
    Column<std::int64_t> val;
    for (std::int64_t i = 0; i < 64; ++i) {
        ts.push_back(ts_from_nanos(i));
        val.push_back(64 - i);
    }
    full.add_column("ts", std::move(ts));
    full.add_column("val", std::move(val));
    full.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("sparse", sparse);
    registry.emplace("full", full);

    auto sparse_ir =
        require_ir("sparse[window 1ns, update { mn = rolling_min(val), mx = rolling_max(val) }];");
    auto sparse_res = runtime::interpret(*sparse_ir, registry);
    REQUIRE(sparse_res.has_value());
    const auto* sparse_mn = std::get_if<Column<std::int64_t>>(sparse_res->find("mn"));
    const auto* sparse_mx = std::get_if<Column<std::int64_t>>(sparse_res->find("mx"));
    REQUIRE(sparse_mn != nullptr);
    REQUIRE(sparse_mx != nullptr);
    CHECK((*sparse_mn)[0] == 30);
    CHECK((*sparse_mn)[1] == 10);
    CHECK((*sparse_mn)[2] == 20);
    CHECK((*sparse_mx)[0] == 30);
    CHECK((*sparse_mx)[1] == 10);
    CHECK((*sparse_mx)[2] == 20);

    auto full_ir =
        require_ir("full[window 1000ns, update { mn = rolling_min(val), mx = rolling_max(val) }];");
    auto full_res = runtime::interpret(*full_ir, registry);
    REQUIRE(full_res.has_value());
    const auto* full_mn = std::get_if<Column<std::int64_t>>(full_res->find("mn"));
    const auto* full_mx = std::get_if<Column<std::int64_t>>(full_res->find("mx"));
    REQUIRE(full_mn != nullptr);
    REQUIRE(full_mx != nullptr);
    CHECK((*full_mn)[0] == 64);
    CHECK((*full_mx)[0] == 64);
    CHECK((*full_mn)[63] == 1);
    CHECK((*full_mx)[63] == 64);
}

TEST_CASE("rolling_min and rolling_max return null for all-null time windows",
          "[rolling][window][null]") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(10)});
    table.add_column("val", Column<double>{1e300, 2e300}, runtime::ValidityBitmap{false, false});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir =
        require_ir("data[window 1ns, update { mn = rolling_min(val), mx = rolling_max(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* mn_entry = result->find_entry("mn");
    const auto* mx_entry = result->find_entry("mx");
    REQUIRE(mn_entry != nullptr);
    REQUIRE(mx_entry != nullptr);
    REQUIRE(mn_entry->validity.has_value());
    REQUIRE(mx_entry->validity.has_value());
    CHECK(runtime::is_null(*mn_entry, 0));
    CHECK(runtime::is_null(*mn_entry, 1));
    CHECK(runtime::is_null(*mx_entry, 0));
    CHECK(runtime::is_null(*mx_entry, 1));
}

TEST_CASE("rolling_min and rolling_max reuse deque storage for small count windows",
          "[rolling][window]") {
    runtime::Table table;
    Column<std::int64_t> val;
    for (std::int64_t i = 0; i < 4096; ++i)
        val.push_back((i * 37) % 101);
    table.add_column("val", std::move(val));

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { mn = rolling_min(val, 3), mx = rolling_max(val, 3) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* mn = std::get_if<Column<std::int64_t>>(result->find("mn"));
    const auto* mx = std::get_if<Column<std::int64_t>>(result->find("mx"));
    const auto* src = std::get_if<Column<std::int64_t>>(table.find("val"));
    REQUIRE(mn != nullptr);
    REQUIRE(mx != nullptr);
    REQUIRE(src != nullptr);

    for (std::size_t i = 0; i < src->size(); ++i) {
        const std::size_t lo = i >= 2 ? i - 2 : 0;
        std::int64_t expected_min = (*src)[lo];
        std::int64_t expected_max = (*src)[lo];
        for (std::size_t j = lo + 1; j <= i; ++j) {
            expected_min = std::min(expected_min, (*src)[j]);
            expected_max = std::max(expected_max, (*src)[j]);
        }
        CHECK((*mn)[i] == expected_min);
        CHECK((*mx)[i] == expected_max);
    }
}

// --- resample tests -----------------------------------------------------------

TEST_CASE("resample basic OHLC - 3 two-minute buckets") {
    // 6 ticks: t=0,1,2 in bucket 0; t=3,4,5 in bucket 1; t=6 in bucket 2
    // (using minute-scale nanos: 1 min = 60e9 ns)
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0 * min_ns), ts_from_nanos(1 * min_ns),
                                             ts_from_nanos(2 * min_ns), ts_from_nanos(3 * min_ns),
                                             ts_from_nanos(4 * min_ns), ts_from_nanos(5 * min_ns)});
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0, 50.0, 60.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("tf", table);

    auto ir = require_ir(
        R"(tf[resample 2m, select { open = first(price), high = max(price), low = min(price), close = last(price) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    // 3 buckets: [0..1], [2..3], [4..5]
    REQUIRE(result->rows() == 3);
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");

    const auto* open_col = result->find("open");
    const auto* close_col = result->find("close");
    const auto* high_col = result->find("high");
    const auto* low_col = result->find("low");
    REQUIRE(open_col != nullptr);
    REQUIRE(close_col != nullptr);
    REQUIRE(high_col != nullptr);
    REQUIRE(low_col != nullptr);

    // bucket 0: rows 0,1 -> open=10, close=20, low=10, high=20
    const auto& opens = std::get<Column<double>>(*open_col);
    const auto& closes = std::get<Column<double>>(*close_col);
    const auto& highs = std::get<Column<double>>(*high_col);
    const auto& lows = std::get<Column<double>>(*low_col);
    REQUIRE(opens[0] == 10.0);
    REQUIRE(closes[0] == 20.0);
    REQUIRE(highs[0] == 20.0);
    REQUIRE(lows[0] == 10.0);

    // bucket 1: rows 2,3 -> open=30, close=40
    REQUIRE(opens[1] == 30.0);
    REQUIRE(closes[1] == 40.0);

    // bucket 2: rows 4,5 -> open=50, close=60
    REQUIRE(opens[2] == 50.0);
    REQUIRE(closes[2] == 60.0);
}

TEST_CASE("resample count/sum/mean (fast path, numeric reducers)") {
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0 * min_ns), ts_from_nanos(1 * min_ns),
                                             ts_from_nanos(2 * min_ns), ts_from_nanos(3 * min_ns),
                                             ts_from_nanos(4 * min_ns)});
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0, 50.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("tf", table);

    // 2-minute buckets: [0,1]->{10,20}, [2,3]->{30,40}, [4]->{50}
    auto ir =
        require_ir(R"(tf[resample 2m, select { n = count(), s = sum(price), m = mean(price) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto& n = std::get<Column<std::int64_t>>(*result->find("n"));
    const auto& s = std::get<Column<double>>(*result->find("s"));
    const auto& m = std::get<Column<double>>(*result->find("m"));
    CHECK(n[0] == 2);
    CHECK(n[1] == 2);
    CHECK(n[2] == 1);
    CHECK(s[0] == 30.0);
    CHECK(s[1] == 70.0);
    CHECK(s[2] == 50.0);
    CHECK(m[0] == 15.0);
    CHECK(m[1] == 35.0);
    CHECK(m[2] == 50.0);
}

TEST_CASE("resample with by - one bucket per (bucket, symbol)") {
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1 * min_ns),
                                             ts_from_nanos(0), ts_from_nanos(1 * min_ns)});
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "B", "B"});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("tf", table);

    auto ir = require_ir(R"(tf[resample 1m, select { close = last(price) }, by symbol];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    // 2 symbols x 2 buckets = 4 rows
    REQUIRE(result->rows() == 4);
    REQUIRE(result->find("close") != nullptr);
    REQUIRE(result->find("symbol") != nullptr);
}

TEST_CASE("resample error on non-timeframe") {
    runtime::Table table;
    table.add_column("ts", Column<std::int64_t>{0, 1, 2});
    table.add_column("price", Column<double>{1.0, 2.0, 3.0});
    // No time_index set

    runtime::TableRegistry registry;
    registry.emplace("plain", table);

    auto ir = require_ir(R"(plain[resample 1m, select { close = last(price) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a TimeFrame") != std::string::npos);
}

TEST_CASE("rolling_sum preserves other columns and time_index") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{1, 2, 3});
    table.add_column("label", Column<std::string>{"a", "b", "c"});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
    // original columns still present
    REQUIRE(result->find("val") != nullptr);
    REQUIRE(result->find("label") != nullptr);
    REQUIRE(result->find("s") != nullptr);
}

// --- rolling_median / rolling_std / rolling_ewma -----------------------------
// Same 3-row, 1ns-window setup used by the other rolling tests:
//   ts: 0ns, 1ns, 2ns   val: 10, 20, 30
//   window 1ns -> [t-1, t]
//     row 0: only row 0      -> {10}
//     row 1: rows 0..1       -> {10,20}
//     row 2: rows 1..2       -> {20,30}

TEST_CASE("rolling_median with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { m = rolling_median(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    // row 0: median({10}) = 10
    CHECK((*m)[0] == Catch::Approx(10.0));
    // row 1: median({10,20}) = 15   (even count -> average of two middle values)
    CHECK((*m)[1] == Catch::Approx(15.0));
    // row 2: median({20,30}) = 25
    CHECK((*m)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_median odd window size") {
    // 5 timestamps, window 2ns -> row 2 sees {10,20,30} (odd count)
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3), ts_from_nanos(4)});
    table.add_column("val", Column<double>{10.0, 30.0, 20.0, 40.0, 50.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 2ns, update { m = rolling_median(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    // row 2 (t=2): window [0,2] -> {10,30,20} sorted {10,20,30} -> median = 20
    CHECK((*m)[2] == Catch::Approx(20.0));
    // row 3 (t=3): window [1,3] -> {30,20,40} sorted {20,30,40} -> median = 30
    CHECK((*m)[3] == Catch::Approx(30.0));
}

TEST_CASE("rolling_std with 1ns window") {
    // row 0: {10}      -> n<2, stddev = 0.0
    // row 1: {10,20}   -> mean=15, M2=50, sample std = sqrt(50/1)=sqrt(50)~7.071
    // row 2: {20,30}   -> mean=25, M2=50, sample std = sqrt(50/1)=sqrt(50)~7.071
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_std(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(s != nullptr);
    // single-element window -> 0.0 (undefined sample stddev)
    CHECK((*s)[0] == Catch::Approx(0.0));
    // {10,20}: sample std = sqrt(50) ~ 7.0711
    CHECK((*s)[1] == Catch::Approx(7.0711).epsilon(1e-3));
    // {20,30}: sample std = sqrt(50) ~ 7.0711
    CHECK((*s)[2] == Catch::Approx(7.0711).epsilon(1e-3));
}

TEST_CASE("rolling_std larger window") {
    // {0,2,4}: mean=2, M2=8, sample std = sqrt(8/2) = 2.0
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3), ts_from_nanos(4)});
    table.add_column("val", Column<double>{0.0, 2.0, 4.0, 6.0, 8.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // window 2ns: row 2 sees {0,2,4}
    auto ir = require_ir("data[window 2ns, update { s = rolling_std(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(s != nullptr);
    // row 2: {0,2,4} -> sample std = 2.0
    CHECK((*s)[2] == Catch::Approx(2.0));
}

TEST_CASE("rolling_ewma with 1ns window") {
    // alpha = 0.5
    // row 0: window {10}     -> ewma = 10
    // row 1: window {10,20}  -> ewma starts 10, then 0.5*20 + 0.5*10 = 15
    // row 2: window {20,30}  -> ewma starts 20, then 0.5*30 + 0.5*20 = 25
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { e = rolling_ewma(val, 0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e = std::get_if<Column<double>>(result->find("e"));
    REQUIRE(e != nullptr);
    CHECK((*e)[0] == Catch::Approx(10.0));
    CHECK((*e)[1] == Catch::Approx(15.0));
    CHECK((*e)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_ewma larger window") {
    // window 2ns, alpha = 0.5
    // row 0 (t=0): {10}            -> ewma = 10
    // row 1 (t=1): {10, 20}        -> 10 -> 0.5*20+0.5*10 = 15
    // row 2 (t=2): {10, 20, 30}    -> 10 -> 15 -> 0.5*30+0.5*15 = 22.5
    // row 3 (t=3): {20, 30, 40}    -> 20 -> 0.5*30+0.5*20=25 -> 0.5*40+0.5*25 = 32.5
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 2ns, update { e = rolling_ewma(val, 0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e = std::get_if<Column<double>>(result->find("e"));
    REQUIRE(e != nullptr);
    CHECK((*e)[0] == Catch::Approx(10.0));
    CHECK((*e)[1] == Catch::Approx(15.0));
    CHECK((*e)[2] == Catch::Approx(22.5));
    CHECK((*e)[3] == Catch::Approx(32.5));
}

// --- Phase 1 null semantics ---------------------------------------------------

TEST_CASE("null: right join unmatched rows produce null left columns", "[null][join]") {
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2});
    left.add_column("name", Column<std::string>{"alice", "bob"});

    right.add_column("id", Column<std::int64_t>{2, 3});
    right.add_column("score", Column<double>{20.0, 30.0});

    auto result = runtime::join_tables(left, right, ir::JoinKind::Right, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 2);

    const auto& name_entry = t.columns[t.index.at("name")];
    CHECK_FALSE(runtime::is_null(name_entry, 0));
    CHECK(runtime::is_null(name_entry, 1));

    const auto& id_col = std::get<Column<std::int64_t>>(*t.columns[t.index.at("id")].column);
    CHECK(id_col[0] == 2);
    CHECK(id_col[1] == 3);
}

TEST_CASE("null: outer join unmatched rows produce nulls on both sides", "[null][join]") {
    // left:  id {1, 2},  name  {"alice", "bob"}
    // right: id {2, 3},  score {20.0, 30.0}
    //
    // Full outer join on id emits left rows in left-table order, then any
    // unmatched right rows:
    //   row 0 -> id=1, name="alice" (left-only)  ->  score is NULL
    //   row 1 -> id=2, name="bob"  (matched)     ->  score=20.0
    //   row 2 -> id=3              (right-only)  ->  name is NULL, score=30.0
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2});
    left.add_column("name", Column<std::string>{"alice", "bob"});

    right.add_column("id", Column<std::int64_t>{2, 3});
    right.add_column("score", Column<double>{20.0, 30.0});

    auto result = runtime::join_tables(left, right, ir::JoinKind::Outer, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 3);

    const auto& name_entry = t.columns[t.index.at("name")];
    const auto& score_entry = t.columns[t.index.at("score")];

    // row 0: id=1, left-only -> name valid, score null
    CHECK_FALSE(runtime::is_null(name_entry, 0));
    CHECK(runtime::is_null(score_entry, 0));

    // row 1: id=2, matched  -> both valid
    CHECK_FALSE(runtime::is_null(name_entry, 1));
    CHECK_FALSE(runtime::is_null(score_entry, 1));

    // row 2: id=3, right-only -> name null, score valid
    CHECK(runtime::is_null(name_entry, 2));
    CHECK_FALSE(runtime::is_null(score_entry, 2));
}
TEST_CASE("null: left join unmatched rows produce null right columns", "[null][join]") {
    runtime::Table left, right;
    Column<std::int64_t> lid;
    lid.push_back(1);
    lid.push_back(2);
    lid.push_back(3);
    left.add_column("id", std::move(lid));

    Column<std::int64_t> rid;
    rid.push_back(1);
    rid.push_back(3);
    right.add_column("id", std::move(rid));
    Column<double> scores;
    scores.push_back(10.0);
    scores.push_back(30.0);
    right.add_column("score", std::move(scores));

    auto result = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 3);

    const auto& score_entry = t.columns[t.index.at("score")];
    CHECK_FALSE(runtime::is_null(score_entry, 0));  // id=1 matched
    CHECK(runtime::is_null(score_entry, 1));        // id=2 unmatched -> null
    CHECK_FALSE(runtime::is_null(score_entry, 2));  // id=3 matched
}

TEST_CASE("null: validity bitmap propagates through filter", "[null]") {
    runtime::Table t;
    t.add_column("id", Column<std::int64_t>{1, 2, 3});
    t.add_column("val", Column<double>{1.0, 2.0, 3.0}, std::vector<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    // Filter: keep id >= 1 (all rows) - bitmap must survive unchanged.
    auto ir = require_ir("t[filter id >= 1];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto& val_entry = result->columns[result->index.at("val")];
    CHECK_FALSE(runtime::is_null(val_entry, 0));  // row 0 was valid
    CHECK(runtime::is_null(val_entry, 1));        // row 1 was null -> still null
    CHECK_FALSE(runtime::is_null(val_entry, 2));  // row 2 was valid
}

TEST_CASE("null: validity bitmap propagates through project", "[null]") {
    runtime::Table t;
    t.add_column("id", Column<std::int64_t>{1, 2});
    t.add_column("val", Column<double>{1.0, 2.0}, std::vector<bool>{true, false});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[select { val }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& val_entry = result->columns[result->index.at("val")];
    CHECK_FALSE(runtime::is_null(val_entry, 0));
    CHECK(runtime::is_null(val_entry, 1));
}

TEST_CASE("null: print displays null for null rows", "[null]") {
    runtime::Table left, right;
    Column<std::int64_t> lid;
    lid.push_back(1);
    lid.push_back(2);
    left.add_column("id", std::move(lid));
    Column<std::int64_t> rid;
    rid.push_back(1);
    right.add_column("id", std::move(rid));
    Column<double> vals;
    vals.push_back(99.0);
    right.add_column("val", std::move(vals));

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    std::ostringstream oss;
    ops::print(*joined, oss);
    const std::string out = oss.str();
    CHECK(out.find("null") != std::string::npos);
    CHECK(out.find("99") != std::string::npos);
}

TEST_CASE("null agg: grouped sum/mean/min/max ignore nulls", "[null][agg]") {
    runtime::Table table;
    table.add_column("g", Column<std::int64_t>{1, 1, 2, 2});
    table.add_column("x", Column<double>{10.0, 0.0, 0.0, 0.0},
                     std::vector<bool>{true, false, false, false});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir(
        "t[select { g, sx = sum(x), mx = mean(x), mn = min(x), xx = max(x), n = count() }, by g, "
        "order g];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& g = std::get<Column<std::int64_t>>(*result->columns[result->index.at("g")].column);
    REQUIRE(g[0] == 1);
    REQUIRE(g[1] == 2);

    const auto& sx_entry = result->columns[result->index.at("sx")];
    const auto& mx_entry = result->columns[result->index.at("mx")];
    const auto& mn_entry = result->columns[result->index.at("mn")];
    const auto& xx_entry = result->columns[result->index.at("xx")];
    const auto& n_entry = result->columns[result->index.at("n")];

    const auto& sx = std::get<Column<double>>(*sx_entry.column);
    const auto& mx = std::get<Column<double>>(*mx_entry.column);
    const auto& mn = std::get<Column<double>>(*mn_entry.column);
    const auto& xx = std::get<Column<double>>(*xx_entry.column);
    const auto& n = std::get<Column<std::int64_t>>(*n_entry.column);

    CHECK(sx[0] == Catch::Approx(10.0));
    CHECK(mx[0] == Catch::Approx(10.0));
    CHECK(mn[0] == Catch::Approx(10.0));
    CHECK(xx[0] == Catch::Approx(10.0));
    CHECK_FALSE(runtime::is_null(sx_entry, 0));
    CHECK_FALSE(runtime::is_null(mx_entry, 0));
    CHECK_FALSE(runtime::is_null(mn_entry, 0));
    CHECK_FALSE(runtime::is_null(xx_entry, 0));

    CHECK(runtime::is_null(sx_entry, 1));
    CHECK(runtime::is_null(mx_entry, 1));
    CHECK(runtime::is_null(mn_entry, 1));
    CHECK(runtime::is_null(xx_entry, 1));

    CHECK(n[0] == 2);
    CHECK(n[1] == 2);
}

TEST_CASE("null agg: global all-null aggregate returns null", "[null][agg]") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{0, 0, 0}, std::vector<bool>{false, false, false});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir(
        "t[select { sx = sum(x), mx = mean(x), mn = min(x), xx = max(x), n = count() }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto& sx_entry = result->columns[result->index.at("sx")];
    const auto& mx_entry = result->columns[result->index.at("mx")];
    const auto& mn_entry = result->columns[result->index.at("mn")];
    const auto& xx_entry = result->columns[result->index.at("xx")];
    const auto& n_entry = result->columns[result->index.at("n")];
    const auto& n = std::get<Column<std::int64_t>>(*n_entry.column);

    CHECK(runtime::is_null(sx_entry, 0));
    CHECK(runtime::is_null(mx_entry, 0));
    CHECK(runtime::is_null(mn_entry, 0));
    CHECK(runtime::is_null(xx_entry, 0));
    CHECK(n[0] == 3);
}

TEST_CASE("print: doubles use mixed precision formatting", "[print]") {
    runtime::Table t;
    t.add_column("a", Column<double>{0.1 + 0.2});
    t.add_column("b", Column<double>{1.3100000000000165});
    t.add_column("c", Column<double>{1.0441991347356203e13});

    std::ostringstream oss;
    ops::print(t, oss);
    const std::string out = oss.str();

    CHECK(out.find("0.3") != std::string::npos);
    CHECK(out.find("1.31") != std::string::npos);
    CHECK(out.find("1.044199e13") != std::string::npos);
}

TEST_CASE("null: left join fan-out (duplicate right keys)", "[null][join]") {
    // left: id = {1, 2, 3}
    // right: id = {1, 1, 3}  - id=1 appears twice, id=2 missing
    // expected output: 4 rows (id=1 x2, id=2 x1 null, id=3 x1)
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 1, 3});
    right.add_column("val", Column<double>{10.0, 11.0, 30.0});

    auto result = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 4);

    // Rows: (1,10), (1,11), (2,null), (3,30)
    const auto& id_col = std::get<Column<std::int64_t>>(*t.columns[t.index.at("id")].column);
    const auto& val_entry = t.columns[t.index.at("val")];
    const auto& val_col = std::get<Column<double>>(*val_entry.column);

    CHECK(id_col[0] == 1);
    CHECK_FALSE(runtime::is_null(val_entry, 0));
    CHECK(val_col[0] == 10.0);
    CHECK(id_col[1] == 1);
    CHECK_FALSE(runtime::is_null(val_entry, 1));
    CHECK(val_col[1] == 11.0);
    CHECK(id_col[2] == 2);
    CHECK(runtime::is_null(val_entry, 2));
    CHECK(id_col[3] == 3);
    CHECK_FALSE(runtime::is_null(val_entry, 3));
    CHECK(val_col[3] == 30.0);
}

// --- Phase 2: Nullable Expressions + 3VL Filter tests ------------------------

TEST_CASE("null 3vl: arithmetic propagates null", "[null][3vl]") {
    // price column with a validity bitmap: rows 0,2 valid; row 1 null
    runtime::Table table;
    Column<std::int64_t> price_col{10, 0, 30};
    table.add_column("price", std::move(price_col));
    table.columns[table.index.at("price")].validity = std::vector<bool>{true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // select { p2 = price * 2 }
    auto ir = require_ir("t[select { p2 = price * 2 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& p2_entry = result->columns[result->index.at("p2")];
    const auto& p2 = std::get<Column<std::int64_t>>(*p2_entry.column);
    REQUIRE(p2.size() == 3);
    CHECK(p2[0] == 20);
    CHECK(p2[2] == 60);
    // row 1 should be null
    REQUIRE(p2_entry.validity.has_value());
    CHECK((*p2_entry.validity)[0] == true);
    CHECK((*p2_entry.validity)[1] == false);
    CHECK((*p2_entry.validity)[2] == true);
}

TEST_CASE("null 3vl: update arithmetic propagates null", "[null][3vl][update]") {
    runtime::Table table;
    Column<std::int64_t> price_col{10, 0, 30};
    table.add_column("price", std::move(price_col));
    table.columns[table.index.at("price")].validity = std::vector<bool>{true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { p2 = (price * 2) + 1 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& p2_entry = result->columns[result->index.at("p2")];
    const auto& p2 = std::get<Column<std::int64_t>>(*p2_entry.column);
    REQUIRE(p2.size() == 3);
    CHECK(p2[0] == 21);
    CHECK(p2[2] == 61);
    REQUIRE(p2_entry.validity.has_value());
    CHECK((*p2_entry.validity)[0] == true);
    CHECK((*p2_entry.validity)[1] == false);
    CHECK((*p2_entry.validity)[2] == true);
}

TEST_CASE("null 3vl: comparison with null drops row", "[null][3vl]") {
    // sector column where row 1 is null (left join unmatched)
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());
    REQUIRE(joined->rows() == 3);

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { sector == "Tech" } - null rows should be dropped
    auto ir = require_ir("j[filter { sector == \"Tech\" }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    CHECK(result->rows() == 1);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 1);
}

TEST_CASE("null 3vl: IS NULL predicate keeps null rows", "[null][3vl]") {
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { sector is null } - only id=2 (unmatched) should remain
    auto ir = require_ir("j[filter { sector is null }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    CHECK(result->rows() == 1);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 2);
}

TEST_CASE("null 3vl: is_null(col) function-call form is accepted in filter",
          "[null][3vl][filter]") {
    // Same shape as the postfix `is null` test, but using the pandas/Polars
    // function-call spelling that users coming from those tools reach for.
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    SECTION("is_null(col) keeps the unmatched row") {
        auto ir = require_ir("j[filter is_null(sector)];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        CHECK(result->rows() == 1);
        const auto& id_col =
            std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
        CHECK(id_col[0] == 2);
    }

    SECTION("!is_null(col) keeps the matched rows") {
        auto ir = require_ir("j[filter !is_null(sector)];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        CHECK(result->rows() == 2);
        const auto& id_col =
            std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
        CHECK(id_col[0] == 1);
        CHECK(id_col[1] == 3);
    }

    SECTION("is_not_null(col) is the same as !is_null(col)") {
        auto ir = require_ir("j[filter is_not_null(sector)];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        CHECK(result->rows() == 2);
    }
}

TEST_CASE("null 3vl: IS NOT NULL predicate keeps valid rows", "[null][3vl]") {
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { sector is not null } - id=1 and id=3 should remain
    auto ir = require_ir("j[filter { sector is not null }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    CHECK(result->rows() == 2);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 1);
    CHECK(id_col[1] == 3);
}

TEST_CASE("null 3vl: OR short-circuit with null - true OR null = true", "[null][3vl]") {
    runtime::Table left, right;
    // id=1: price=10, sector="Tech"  (both known)
    // id=2: price=50, sector=null    (price>0 is TRUE, sector is null -> true OR null = true)
    // id=3: price=-1, sector=null    (price>0 is FALSE, sector is null -> false OR null = null ->
    // drop)
    left.add_column("id", Column<std::int64_t>{1, 2, 3});
    left.add_column("price", Column<std::int64_t>{10, 50, -1});

    right.add_column("id", Column<std::int64_t>{1});
    right.add_column("sector", Column<std::string>{"Tech"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());
    REQUIRE(joined->rows() == 3);

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { price > 0 || sector is not null }
    auto ir = require_ir("j[filter { price > 0 || sector is not null }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    // id=1: price>0=true, is not null=true  -> true OR true = true -> keep
    // id=2: price>0=true, is not null=false -> true OR null(=false,invalid) = true (valid) -> keep
    // id=3: price>0=false, is not null=false -> false OR null = null -> drop
    CHECK(result->rows() == 2);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 1);
    CHECK(id_col[1] == 2);
}

TEST_CASE("Interpret rename: basic column renaming") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[rename p = price];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);
    CHECK(result->columns[0].name == "p");
    CHECK(result->columns[1].name == "symbol");
    const auto& p = std::get<Column<std::int64_t>>(*result->columns[0].column);
    CHECK(p[0] == 10);
    CHECK(p[1] == 20);
    CHECK(p[2] == 30);
}

TEST_CASE("Interpret rename: keeps non-renamed columns") {
    runtime::Table table;
    table.add_column("a", Column<std::int64_t>{1, 2});
    table.add_column("b", Column<std::int64_t>{3, 4});
    table.add_column("c", Column<std::int64_t>{5, 6});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[rename x = b];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 3);
    CHECK(result->columns[0].name == "a");
    CHECK(result->columns[1].name == "x");
    CHECK(result->columns[2].name == "c");
}

TEST_CASE("Interpret rename: multiple renames") {
    runtime::Table table;
    table.add_column("foo", Column<std::int64_t>{1, 2});
    table.add_column("bar", Column<std::int64_t>{3, 4});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[rename { x = foo, y = bar }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);
    CHECK(result->columns[0].name == "x");
    CHECK(result->columns[1].name == "y");
}

TEST_CASE("Interpret rename: combined with filter and select") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("qty", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 15, rename p = price, select p];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);
    CHECK(result->columns[0].name == "p");
    const auto& p = std::get<Column<std::int64_t>>(*result->columns[0].column);
    CHECK(p[0] == 20);
    CHECK(p[1] == 30);
}

TEST_CASE("Interpret rename: error on missing column") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[rename p = nonexistent];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("nonexistent") != std::string::npos);
}

// -- Statistical aggregation functions ----------------------------------------

TEST_CASE("Interpret median aggregation (odd count)") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 30.0, 20.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 1);
    CHECK((*meds)[0] == Catch::Approx(20.0));
}

TEST_CASE("Interpret median aggregation (even count)") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 1);
    CHECK((*meds)[0] == Catch::Approx(25.0));
}

TEST_CASE("Interpret median aggregation grouped") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 5.0, 15.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "A", "B", "B"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 2);
    // Group A: {10, 20, 30} -> median = 20
    CHECK((*meds)[0] == Catch::Approx(20.0));
    // Group B: {5, 15} -> median = 10
    CHECK((*meds)[1] == Catch::Approx(10.0));
}

TEST_CASE("Interpret median on integer column") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 3, 2});
    table.add_column("symbol", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 1);
    CHECK((*meds)[0] == Catch::Approx(2.0));
}

TEST_CASE("Interpret sample stddev aggregation") {
    runtime::Table table;
    // {0, 2, 4}: mean=2, M2=8, sample stddev = sqrt(8/2) = 2.0
    table.add_column("v", Column<double>{0.0, 2.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = std(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s_col = result->find("s");
    REQUIRE(s_col != nullptr);
    const auto* sv = std::get_if<Column<double>>(s_col);
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 1);
    CHECK((*sv)[0] == Catch::Approx(2.0));
}

TEST_CASE("Interpret sample stddev grouped") {
    runtime::Table table;
    table.add_column("v", Column<double>{2.0, 4.0, 6.0, 1.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "B", "B"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = std(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s_col = result->find("s");
    REQUIRE(s_col != nullptr);
    const auto* sv = std::get_if<Column<double>>(s_col);
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 2);
    // Group A: {2,4,6} -> mean=4, M2=8, sample std = sqrt(8/2) = 2
    CHECK((*sv)[0] == Catch::Approx(2.0));
    // Group B: {1,3} -> mean=2, M2=2, sample std = sqrt(2/1) = sqrt(2) ~ 1.41421356
    CHECK((*sv)[1] == Catch::Approx(1.41421356).epsilon(1e-5));
}

TEST_CASE("Interpret stddev single element is null") {
    runtime::Table table;
    table.add_column("v", Column<double>{5.0});
    table.add_column("grp", Column<std::string>{"A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = std(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // Single element -> count < 2 -> result should be null
    const auto* s_entry = result->find_entry("s");
    REQUIRE(s_entry != nullptr);
    REQUIRE(s_entry->validity.has_value());
    CHECK((*s_entry->validity)[0] == false);
}

TEST_CASE("Interpret EWMA aggregation") {
    runtime::Table table;
    // alpha=0.5: ewma starts at 1.0, then 0.5*3+0.5*1=2.0, then 0.5*5+0.5*2=3.5
    table.add_column("v", Column<double>{1.0, 3.0, 5.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { e = ewma(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e_col = result->find("e");
    REQUIRE(e_col != nullptr);
    const auto* ev = std::get_if<Column<double>>(e_col);
    REQUIRE(ev != nullptr);
    REQUIRE(ev->size() == 1);
    CHECK((*ev)[0] == Catch::Approx(3.5));
}

TEST_CASE("Interpret EWMA grouped") {
    runtime::Table table;
    // Group A: {1, 3}, alpha=0.5 -> 1.0 then 0.5*3+0.5*1 = 2.0
    // Group B: {2, 4}, alpha=0.5 -> 2.0 then 0.5*4+0.5*2 = 3.0
    table.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "B", "A", "B"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { e = ewma(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e_col = result->find("e");
    REQUIRE(e_col != nullptr);
    const auto* ev = std::get_if<Column<double>>(e_col);
    REQUIRE(ev != nullptr);
    REQUIRE(ev->size() == 2);
    CHECK((*ev)[0] == Catch::Approx(2.0));
    CHECK((*ev)[1] == Catch::Approx(3.0));
}

// --- quantile -----------------------------------------------------------------

TEST_CASE("Interpret quantile aggregation (p=0.5 == median)") {
    runtime::Table table;
    // {10, 20, 30} sorted -> p=0.5 -> idx=1.0 -> 20.0
    table.add_column("v", Column<double>{10.0, 30.0, 20.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { q = quantile(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* q_col = result->find("q");
    REQUIRE(q_col != nullptr);
    const auto* qv = std::get_if<Column<double>>(q_col);
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 1);
    CHECK((*qv)[0] == Catch::Approx(20.0));
}

TEST_CASE("Interpret quantile aggregation (p=0.25 and p=0.75)") {
    runtime::Table table;
    // {1, 2, 3, 4} sorted
    // p=0.25 -> idx=0.75 -> 1 + 0.75*(2-1) = 1.75
    // p=0.75 -> idx=2.25 -> 3 + 0.25*(4-3) = 3.25
    table.add_column("v", Column<double>{1.0, 3.0, 2.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir_lo = require_ir("t[select { q = quantile(v, 0.25) }, by grp];");
    auto result_lo = runtime::interpret(*ir_lo, registry);
    REQUIRE(result_lo.has_value());
    const auto* qv_lo = std::get_if<Column<double>>(result_lo->find("q"));
    REQUIRE(qv_lo != nullptr);
    CHECK((*qv_lo)[0] == Catch::Approx(1.75));

    auto ir_hi = require_ir("t[select { q = quantile(v, 0.75) }, by grp];");
    auto result_hi = runtime::interpret(*ir_hi, registry);
    REQUIRE(result_hi.has_value());
    const auto* qv_hi = std::get_if<Column<double>>(result_hi->find("q"));
    REQUIRE(qv_hi != nullptr);
    CHECK((*qv_hi)[0] == Catch::Approx(3.25));
}

TEST_CASE("Interpret quantile aggregation (p=0 and p=1)") {
    runtime::Table table;
    table.add_column("v", Column<double>{5.0, 1.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir_min = require_ir("t[select { q = quantile(v, 0.0) }, by grp];");
    auto result_min = runtime::interpret(*ir_min, registry);
    REQUIRE(result_min.has_value());
    const auto* qv_min = std::get_if<Column<double>>(result_min->find("q"));
    REQUIRE(qv_min != nullptr);
    CHECK((*qv_min)[0] == Catch::Approx(1.0));

    auto ir_max = require_ir("t[select { q = quantile(v, 1.0) }, by grp];");
    auto result_max = runtime::interpret(*ir_max, registry);
    REQUIRE(result_max.has_value());
    const auto* qv_max = std::get_if<Column<double>>(result_max->find("q"));
    REQUIRE(qv_max != nullptr);
    CHECK((*qv_max)[0] == Catch::Approx(5.0));
}

TEST_CASE("Interpret quantile grouped") {
    runtime::Table table;
    // Group A: {1,3,5} -> p=0.5 -> 3.0
    // Group B: {2,4}   -> p=0.5 -> idx=0.5 -> 2+0.5*(4-2)=3.0
    table.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    table.add_column("grp", Column<std::string>{"A", "B", "A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { q = quantile(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* qv = std::get_if<Column<double>>(result->find("q"));
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 2);
    CHECK((*qv)[0] == Catch::Approx(3.0));
    CHECK((*qv)[1] == Catch::Approx(3.0));
}

// --- skew ----------------------------------------------------------------------

TEST_CASE("Interpret skew aggregation (symmetric -> 0)") {
    runtime::Table table;
    // {1, 2, 3} - symmetric around 2, skew = 0
    table.add_column("v", Column<double>{1.0, 2.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = skew(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s_col = result->find("s");
    REQUIRE(s_col != nullptr);
    const auto* sv = std::get_if<Column<double>>(s_col);
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 1);
    CHECK((*sv)[0] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("Interpret skew aggregation (right-skewed)") {
    runtime::Table table;
    // {1, 1, 1, 4} - right skew
    // mean=1.75, deviations: -0.75,-0.75,-0.75,2.25
    // m2=0.75^2*3+2.25^2 = 1.6875+5.0625=6.75
    // m3=(-0.75)^3*3+(2.25)^3 = -1.265625+11.390625=10.125
    // n=4, skew = (4*sqrt(3)/2) * (10.125 / 6.75^1.5)
    // 6.75^1.5 = sqrt(6.75^3) = sqrt(307.546875) ~ 17.5371
    // skew = (4*1.73205/2) * (10.125/17.5371) ~ 3.4641 * 0.5773 ~ 2.0
    table.add_column("v", Column<double>{1.0, 1.0, 1.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = skew(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* sv = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 1);
    // Expected value matches pandas: pandas.Series([1,1,1,4]).skew() ~ 2.0
    CHECK((*sv)[0] == Catch::Approx(2.0).epsilon(1e-5));
}

TEST_CASE("Interpret skew too few values is null") {
    runtime::Table table;
    // n=2 -> skew is undefined (n<3) -> null
    table.add_column("v", Column<double>{1.0, 2.0});
    table.add_column("grp", Column<std::string>{"A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = skew(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("s");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0] == false);
}

// --- kurtosis ------------------------------------------------------------------

TEST_CASE("Interpret kurtosis aggregation (normal-like -> ~0 excess)") {
    runtime::Table table;
    // {1,2,3,4,5} - excess kurtosis (pandas): ~ -1.3
    table.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { k = kurtosis(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* k_col = result->find("k");
    REQUIRE(k_col != nullptr);
    const auto* kv = std::get_if<Column<double>>(k_col);
    REQUIRE(kv != nullptr);
    REQUIRE(kv->size() == 1);
    // pandas.Series([1,2,3,4,5]).kurtosis() == -1.2
    CHECK((*kv)[0] == Catch::Approx(-1.2).epsilon(1e-5));
}

TEST_CASE("Interpret kurtosis aggregation (leptokurtic)") {
    runtime::Table table;
    // {0,0,0,0,10} - heavy tail, positive excess kurtosis
    table.add_column("v", Column<double>{0.0, 0.0, 0.0, 0.0, 10.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { k = kurtosis(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* kv = std::get_if<Column<double>>(result->find("k"));
    REQUIRE(kv != nullptr);
    // pandas.Series([0,0,0,0,10]).kurtosis() ~ 5.0
    CHECK((*kv)[0] == Catch::Approx(5.0).epsilon(1e-4));
}

TEST_CASE("Interpret kurtosis too few values is null") {
    runtime::Table table;
    // n=3 -> kurtosis undefined (n<4) -> null
    table.add_column("v", Column<double>{1.0, 2.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { k = kurtosis(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("k");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0] == false);
}

// --- rolling_quantile / rolling_skew / rolling_kurtosis -----------------------

TEST_CASE("rolling_quantile with 1ns window") {
    runtime::Table table;
    // {10, 20, 30} - 1ns window = each row sees only itself
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // 1ns window includes boundary: row 1 sees {10,20}, row 2 sees {20,30}
    // p=0.5: row0->10, row1->15, row2->25
    auto ir = require_ir("data[window 1ns, update { q = rolling_quantile(val, 0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* q_col = result->find("q");
    REQUIRE(q_col != nullptr);
    const auto* qv = std::get_if<Column<double>>(q_col);
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 3);
    CHECK((*qv)[0] == Catch::Approx(10.0));
    CHECK((*qv)[1] == Catch::Approx(15.0));
    CHECK((*qv)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_quantile with 2ns window") {
    runtime::Table table;
    // {10, 20, 30, 40} with 2ns window (threshold = t - 2, lo advances when ts < threshold)
    // row 0 (t=0): window {10}        -> p=0.25 -> 10.0
    // row 1 (t=1): window {10,20}     -> idx=0.25 -> 12.5
    // row 2 (t=2): window {10,20,30}  -> idx=0.5  -> 15.0
    // row 3 (t=3): window {20,30,40}  -> idx=0.5  -> 25.0
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 2ns, update { q = rolling_quantile(val, 0.25) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* qv = std::get_if<Column<double>>(result->find("q"));
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 4);
    CHECK((*qv)[0] == Catch::Approx(10.0));
    CHECK((*qv)[1] == Catch::Approx(12.5));
    CHECK((*qv)[2] == Catch::Approx(15.0));
    CHECK((*qv)[3] == Catch::Approx(25.0));
}

TEST_CASE("rolling_skew with 1ns window (single element -> 0)") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_skew(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* sv = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 3);
    // n<3 -> all zeros
    CHECK((*sv)[0] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*sv)[1] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*sv)[2] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("rolling_skew with wide window (symmetric -> 0)") {
    runtime::Table table;
    // {1,2,3} fully in window by row 2 - symmetric -> skew=0
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 10ns, update { s = rolling_skew(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* sv = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 3);
    // row 2: {1,2,3} symmetric -> skew=0
    CHECK((*sv)[2] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("rolling_kurtosis with 1ns window (n<4 -> 0)") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { k = rolling_kurtosis(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* kv = std::get_if<Column<double>>(result->find("k"));
    REQUIRE(kv != nullptr);
    REQUIRE(kv->size() == 3);
    CHECK((*kv)[0] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*kv)[1] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*kv)[2] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("rolling_kurtosis wide window") {
    runtime::Table table;
    // {1,2,3,4,5}: excess kurtosis ~ -1.3 (pandas)
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3), ts_from_nanos(4)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 100ns, update { k = rolling_kurtosis(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* kv = std::get_if<Column<double>>(result->find("k"));
    REQUIRE(kv != nullptr);
    REQUIRE(kv->size() == 5);
    // row 4: {1,2,3,4,5} -> excess kurtosis = -1.2
    CHECK((*kv)[4] == Catch::Approx(-1.2).epsilon(1e-5));
}

// --- Vectorized RNG -----------------------------------------------------------

TEST_CASE("rand_uniform generates correct number of rows in bounds") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(0.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("u");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] >= 0.0);
        CHECK((*dv)[i] < 1.0);
    }
}

TEST_CASE("rand_uniform with integer literals") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(10.0, 20.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("u");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] >= 10.0);
        CHECK((*dv)[i] < 20.0);
    }
}

TEST_CASE("rand_normal generates correct column type and size") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_normal(0.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("n");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 10);
    // Values from N(0,1) are almost certainly in (-10, 10)
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] > -10.0);
        CHECK((*dv)[i] < 10.0);
    }
}

TEST_CASE("rand_student_t generates correct column type and size") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { t_val = rand_student_t(5.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("t_val");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
}

TEST_CASE("rand_gamma generates positive values") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { g = rand_gamma(2.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("g");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] > 0.0);
    }
}

TEST_CASE("rand_exponential generates positive values") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { e = rand_exponential(2.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("e");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] > 0.0);
    }
}

TEST_CASE("rand_bernoulli generates 0 or 1 integers") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("b");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 10);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK(((*iv)[i] == 0 || (*iv)[i] == 1));
    }
}

TEST_CASE("rand_bernoulli p=0 always yields 0") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("b");
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 0);
    }
}

TEST_CASE("rand_bernoulli p=1 always yields 1") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("b");
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 1);
    }
}

TEST_CASE("rand_poisson generates non-negative integers") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { p = rand_poisson(3.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("p");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] >= 0);
    }
}

TEST_CASE("rand_int generates integers in [lo, hi]") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { r = rand_int(1, 6) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("r");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 10);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] >= 1);
        CHECK((*iv)[i] <= 6);
    }
}

TEST_CASE("unary negation: literals, columns, and compound expressions", "[negation]") {
    runtime::Table table;
    table.add_column("x", Column<double>{1.0, 2.0, 3.0});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // Negative literal (folded in the parser), column negation, and negation of
    // a parenthesized expression (lowered as `0 - expr`).
    auto ir = require_ir("t[update { lit = -2.5, negx = -x, grp = -(x + 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& lit = std::get<Column<double>>(*result->find("lit"));
    const auto& negx = std::get<Column<double>>(*result->find("negx"));
    const auto& grp = std::get<Column<double>>(*result->find("grp"));
    REQUIRE(lit.size() == 3);
    for (std::size_t i = 0; i < 3; ++i) {
        const double x = static_cast<double>(i + 1);
        CHECK(lit[i] == -2.5);
        CHECK(negx[i] == -x);
        CHECK(grp[i] == -(x + 1.0));
    }
}

TEST_CASE("scalar registry: casts/ceil/floor/trunc/round in update", "[scalar_registry]") {
    runtime::Table table;
    table.add_column("f", Column<double>{1.4, 2.5, 3.6});
    table.add_column("i", Column<std::int64_t>{1, 2, 3});
    table.add_column("w", Column<double>{2.0, 4.0, 6.0});  // whole-valued

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir(
        "t[update { fi = Float64(i), iw = Int64(w), c = ceil(f), fl = floor(f), tr = trunc(f), "
        "rn = round(f, nearest), rb = round(f, bankers) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& fi = std::get<Column<double>>(*result->find("fi"));
    const auto& iw = std::get<Column<std::int64_t>>(*result->find("iw"));
    const auto& c = std::get<Column<double>>(*result->find("c"));
    const auto& fl = std::get<Column<double>>(*result->find("fl"));
    const auto& tr = std::get<Column<double>>(*result->find("tr"));
    const auto& rn = std::get<Column<std::int64_t>>(*result->find("rn"));
    const auto& rb = std::get<Column<std::int64_t>>(*result->find("rb"));

    CHECK(fi[0] == 1.0);
    CHECK(iw[2] == 6);
    CHECK(c[0] == 2.0);
    CHECK(fl[2] == 3.0);
    CHECK(tr[1] == 2.0);
    CHECK(rn[1] == 3);  // 2.5 nearest, ties away from zero -> 3
    CHECK(rb[1] == 2);  // 2.5 banker's, ties to even -> 2
    CHECK(rn[2] == 4);  // 3.6 -> 4
}

TEST_CASE("scalar registry: Int cast of non-integer Float errors", "[scalar_registry]") {
    runtime::Table table;
    table.add_column("f", Column<double>{1.5, 2.5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { bad = Int64(f) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("cannot cast non-integer Float") != std::string::npos);
}

TEST_CASE("scalar registry: shared by filter predicates", "[scalar_registry]") {
    runtime::Table table;
    table.add_column("f", Column<double>{1.5, 2.5, 3.5});
    table.add_column("w", Column<double>{2.0, 4.0, 6.0});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // Casts and round work in predicate position (same registry as update/select).
    auto ir = require_ir("t[filter Int64(w) > 3 && round(f, nearest) > 2];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto& f = std::get<Column<double>>(*result->find("f"));
    REQUIRE(f.size() == 2);
    CHECK(f[0] == 2.5);
    CHECK(f[1] == 3.5);
}

TEST_CASE("rand_int lo == hi always yields lo") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { r = rand_int(7, 7) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("r");
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 7);
    }
}

TEST_CASE("rand functions produce independent columns") {
    // Two rand_uniform calls in the same query should produce different columns.
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                               11, 12, 13, 14, 15, 16, 17, 18, 19, 20});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { a = rand_uniform(0.0, 1.0), b = rand_uniform(0.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* a_col = result->find("a");
    const auto* b_col = result->find("b");
    REQUIRE(a_col != nullptr);
    REQUIRE(b_col != nullptr);
    const auto* av = std::get_if<Column<double>>(a_col);
    const auto* bv = std::get_if<Column<double>>(b_col);
    REQUIRE(av != nullptr);
    REQUIRE(bv != nullptr);
    // Columns should differ (probability of all 20 values matching is astronomically small)
    bool any_differ = false;
    for (std::size_t i = 0; i < av->size(); ++i) {
        if ((*av)[i] != (*bv)[i]) {
            any_differ = true;
            break;
        }
    }
    CHECK(any_differ);
}

TEST_CASE("rand_uniform invalid arguments") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // low >= high should fail
    auto ir = require_ir("t[update { u = rand_uniform(1.0, 0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("rand_normal invalid stddev") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_normal(0.0, 0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("rand_bernoulli invalid p") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(1.5) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

// --- seed_rng / reseed_rng ----------------------------------------------------

// rand_uniform uses zorro::Rng, so seeding requires reseed().
TEST_CASE("reseed produces identical rand_uniform sequence") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(32)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(0.0, 1.0) }];");

    runtime::reseed(0xDEADBEEF);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<double>>(*r1->find("u"));

    runtime::reseed(0xDEADBEEF);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<double>>(*r2->find("u"));

    REQUIRE(c1.size() == c2.size());
    for (std::size_t i = 0; i < c1.size(); ++i) {
        CHECK(c1[i] == c2[i]);
    }
}

TEST_CASE("reseed with different seeds produces different rand_uniform sequences") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(32)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(0.0, 1.0) }];");

    runtime::reseed(1);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<double>>(*r1->find("u"));

    runtime::reseed(2);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<double>>(*r2->find("u"));

    bool any_different = false;
    for (std::size_t i = 0; i < c1.size(); ++i) {
        if (c1[i] != c2[i]) {
            any_different = true;
            break;
        }
    }
    CHECK(any_different);
}

TEST_CASE("reseed produces identical rand_normal sequence") {
    runtime::Table table;
    // 33 rows: exercises the 8-wide main loop (4 iterations = 32 rows) and
    // the 1-element scalar tail.
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(33)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_normal(0.0, 1.0) }];");

    runtime::reseed(0xCAFEBABE);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<double>>(*r1->find("n"));

    runtime::reseed(0xCAFEBABE);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<double>>(*r2->find("n"));

    REQUIRE(c1.size() == c2.size());
    for (std::size_t i = 0; i < c1.size(); ++i) {
        CHECK(c1[i] == c2[i]);
    }
}

TEST_CASE("reseed produces identical rand_bernoulli sequence") {
    runtime::Table table;
    // 33 rows: exercises the 4-wide main loop and the scalar tail in fill_bernoulli.
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(33)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(0.7) }];");

    runtime::reseed(0xABCDEF01);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<std::int64_t>>(*r1->find("b"));

    runtime::reseed(0xABCDEF01);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<std::int64_t>>(*r2->find("b"));

    REQUIRE(c1.size() == c2.size());
    for (std::size_t i = 0; i < c1.size(); ++i) {
        CHECK(c1[i] == c2[i]);
    }
}

TEST_CASE("reseed produces identical rand_int sequence") {
    runtime::Table table;
    // 33 rows: exercises the 4-wide main loop and the scalar tail in fill_int.
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(33)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_int(1, 100) }];");

    runtime::reseed(0x12345678);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<std::int64_t>>(*r1->find("n"));

    runtime::reseed(0x12345678);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<std::int64_t>>(*r2->find("n"));

    REQUIRE(c1.size() == c2.size());
    for (std::size_t i = 0; i < c1.size(); ++i) {
        CHECK(c1[i] == c2[i]);
    }
}

TEST_CASE("Two-key categorical group-by spills to hash above the dense limit") {
    // The chunked aggregator uses a dense Cartesian-cell array for multi-key
    // categorical grouping while dict0 * dict1 stays under 4M cells, and spills
    // to a hash map beyond that. Build two categorical keys with 2100 distinct
    // values each (2100^2 ≈ 4.41M > 4M) to exercise the spill path, with every
    // group appearing exactly twice so the answer is easy to verify.
    constexpr int kDistinct = 2100;
    std::vector<std::string> dict_a;
    std::vector<std::string> dict_b;
    dict_a.reserve(kDistinct);
    dict_b.reserve(kDistinct);
    for (int i = 0; i < kDistinct; ++i) {
        dict_a.push_back("a" + std::to_string(i));
        dict_b.push_back("b" + std::to_string(i));
    }
    Column<Categorical> a(dict_a);
    Column<Categorical> b(dict_b);
    for (int pass = 0; pass < 2; ++pass) {
        for (int i = 0; i < kDistinct; ++i) {
            a.push_code(static_cast<Column<Categorical>::code_type>(i));
            b.push_code(static_cast<Column<Categorical>::code_type>(i));
        }
    }

    runtime::Table table;
    table.add_column("a", std::move(a));
    table.add_column("b", std::move(b));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { n = count() }, by {a, b}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& counts = std::get<Column<std::int64_t>>(*result->find("n"));
    REQUIRE(counts.size() == static_cast<std::size_t>(kDistinct));
    for (std::size_t g = 0; g < counts.size(); ++g) {
        CHECK(counts[g] == 2);
    }
}

// --- rep ---------------------------------------------------------------------

TEST_CASE("null-aware functions and booleans in value position", "[null_aware]") {
    // `a left join b on k` leaves k=2's `w` column null.
    runtime::Table a;
    a.add_column("k", Column<std::int64_t>{1, 2, 3});
    a.add_column("x", Column<double>{10.0, 20.0, 30.0});
    runtime::Table b;
    b.add_column("k", Column<std::int64_t>{1, 3});
    b.add_column("w", Column<double>{100.0, 300.0});
    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir(
        "(a left join b on k)[update { miss = is_null(w), have = is_not_null(w), "
        "filled = coalesce(w, -1.0), big = x > 15.0 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& miss = std::get<Column<bool>>(*result->find("miss"));
    CHECK_FALSE(miss[0]);
    CHECK(miss[1]);  // unmatched row -> null
    CHECK_FALSE(miss[2]);
    const auto& have = std::get<Column<bool>>(*result->find("have"));
    CHECK(have[0]);
    CHECK_FALSE(have[1]);
    CHECK(have[2]);
    const auto& filled = std::get<Column<double>>(*result->find("filled"));
    CHECK(filled[0] == 100.0);
    CHECK(filled[1] == -1.0);  // null replaced
    CHECK(filled[2] == 300.0);
    const auto& big = std::get<Column<bool>>(*result->find("big"));  // comparison in value position
    CHECK_FALSE(big[0]);
    CHECK(big[1]);
    CHECK(big[2]);
}

TEST_CASE("guarded update: where C update keeps non-matching rows", "[guarded_update]") {
    auto make = [] {
        runtime::Table t;
        t.add_column("a", Column<std::int64_t>{10, 20, 30, 40});
        t.add_column("g", Column<std::string>{"x", "y", "x", "y"});
        runtime::TableRegistry r;
        r.emplace("t", t);
        return r;
    };

    // Row-local field (gather/scatter): only matching rows change.
    {
        auto registry = make();
        auto ir = require_ir(R"(t[where g == "x" update { a = a * 100 }];)");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto& a = std::get<Column<std::int64_t>>(*result->find("a"));
        CHECK(a[0] == 1000);
        CHECK(a[1] == 20);  // non-matching, unchanged
        CHECK(a[2] == 3000);
        CHECK(a[3] == 40);  // non-matching, unchanged
    }

    // Non-row-local field (lag): evaluated over the FULL table, then assigned to
    // matching rows; a new column is null off-mask.
    {
        auto registry = make();
        auto ir = require_ir(R"(t[where g == "y" update { prev = lag(a, 1) }];)");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto& prev = std::get<Column<std::int64_t>>(*result->find("prev"));
        REQUIRE(prev.size() == 4);
        CHECK(prev[1] == 10);  // a[0] — the real previous row, not the previous y-row
        CHECK(prev[3] == 30);  // a[2]
        const auto* entry = result->find_entry("prev");
        REQUIRE(entry->validity.has_value());
        CHECK_FALSE((*entry->validity)[0]);  // x row: null
        CHECK((*entry->validity)[1]);
        CHECK_FALSE((*entry->validity)[2]);  // x row: null
        CHECK((*entry->validity)[3]);
    }

    // A guarded assignment may not change an existing column's type.
    {
        auto registry = make();
        auto ir = require_ir(R"(t[where g == "x" update { a = "big" }];)");
        auto result = runtime::interpret(*ir, registry);
        CHECK_FALSE(result.has_value());
    }
}

TEST_CASE("Table(n) builds an n-row scaffold", "[table_rows]") {
    runtime::TableRegistry registry;

    // Bare Table(n): n rows, no columns. The logical row count survives the
    // chunked-execution round trip (TableSource -> Materialize).
    {
        auto ir = require_ir("Table(5);");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        CHECK(result->columns.empty());
        CHECK(result->rows() == 5);
    }
    // As an update scaffold: generated columns each get n rows.
    {
        auto ir = require_ir("Table(7)[update { i = rep(0, length_out=7) }];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        REQUIRE(result->rows() == 7);
        const auto& col = std::get<Column<std::int64_t>>(*result->find("i"));
        CHECK(col.size() == 7);
    }
    // Table(0) is an empty frame.
    {
        auto ir = require_ir("Table(0);");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        CHECK(result->rows() == 0);
    }
    // A negative count is rejected.
    {
        auto ir = require_ir("Table(0 - 3);");
        auto result = runtime::interpret(*ir, registry);
        CHECK_FALSE(result.has_value());
    }
}

TEST_CASE("safe_idiv / safe_imod guard the UB cases", "[safe_arith]") {
    using ibex::runtime::safe_idiv;
    using ibex::runtime::safe_imod;
    constexpr std::int64_t kMin = std::numeric_limits<std::int64_t>::min();

    // Division / modulo by zero -> 0 (Ibex's defined result), never SIGFPE.
    CHECK(safe_idiv<std::int64_t>(10, 0) == 0);
    CHECK(safe_imod<std::int64_t>(10, 0) == 0);
    // INT_MIN / -1 overflow -> two's-complement wrap, never #DE.
    CHECK(safe_idiv<std::int64_t>(kMin, -1) == kMin);
    CHECK(safe_imod<std::int64_t>(kMin, -1) == 0);
    // Ordinary values are unchanged (sign of % follows the dividend, as in C++).
    CHECK(safe_idiv<std::int64_t>(17, 5) == 3);
    CHECK(safe_imod<std::int64_t>(17, 5) == 2);
    CHECK(safe_idiv<std::int64_t>(-17, 5) == -3);
    CHECK(safe_imod<std::int64_t>(-17, 5) == -2);
}

TEST_CASE("integer modulo by zero is guarded end-to-end (returns 0)", "[safe_arith]") {
    runtime::Table table;
    table.add_column("a", Column<std::int64_t>{10, 20, 30});
    table.add_column("b", Column<std::int64_t>{3, 0, 7});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { r = a % b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto& r = std::get<Column<std::int64_t>>(*result->find("r"));
    REQUIRE(r.size() == 3);
    CHECK(r[0] == 1);  // 10 % 3
    CHECK(r[1] == 0);  // 20 % 0 -> guarded -> 0 (no crash)
    CHECK(r[2] == 2);  // 30 % 7
}

TEST_CASE("RNG composes inside arithmetic", "[rng_compose]") {
    runtime::Table table;
    table.add_column("base", Column<double>{10.0, 20.0, 30.0, 40.0});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // `base + rand_normal(...)` nests an RNG generator inside arithmetic — the
    // whole field is evaluated vectorized. A tiny stddev keeps the result within
    // a wide deterministic window of `base` (the per-row arithmetic is exact).
    runtime::reseed(123);
    auto ir = require_ir("t[update { y = base + rand_normal(0.0, 0.001) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto& y = std::get<Column<double>>(*result->find("y"));
    REQUIRE(y.size() == 4);
    CHECK(std::abs(y[0] - 10.0) < 0.1);
    CHECK(std::abs(y[1] - 20.0) < 0.1);
    CHECK(std::abs(y[2] - 30.0) < 0.1);
    CHECK(std::abs(y[3] - 40.0) < 0.1);

    // Pure-literal arithmetic around RNG also composes: 100 + 0*N -> ~100.
    auto ir2 = require_ir("t[update { z = 100.0 + 2.0 * rand_uniform(0.0, 1.0) }];");
    auto r2 = runtime::interpret(*ir2, registry);
    REQUIRE(r2.has_value());
    const auto& z = std::get<Column<double>>(*r2->find("z"));
    for (std::size_t i = 0; i < z.size(); ++i) {
        CHECK(z[i] >= 100.0);
        CHECK(z[i] < 102.0);
    }
}

TEST_CASE("rep scalar int fills table rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = rep(42) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 42);
    }
}

TEST_CASE("rep scalar float fills table rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = rep(3.14) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 3);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] == Catch::Approx(3.14));
    }
}

TEST_CASE("rep bool true fills boolean mask column") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { mask = rep(true) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("mask");
    REQUIRE(col != nullptr);
    const auto* bv = std::get_if<Column<bool>>(col);
    REQUIRE(bv != nullptr);
    REQUIRE(bv->size() == 4);
    for (std::size_t i = 0; i < bv->size(); ++i) {
        CHECK((*bv)[i] == true);
    }
}

TEST_CASE("rep bool false fills boolean mask column with false") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { mask = rep(false) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("mask");
    REQUIRE(col != nullptr);
    const auto* bv = std::get_if<Column<bool>>(col);
    REQUIRE(bv != nullptr);
    REQUIRE(bv->size() == 3);
    for (std::size_t i = 0; i < bv->size(); ++i) {
        CHECK((*bv)[i] == false);
    }
}

TEST_CASE("rep named arg times truncates to table rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // rep(7, times=100) - scalar, so output is all 7s (length_out defaults to rows)
    auto ir = require_ir("t[update { c = rep(7, times=100) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 7);
    }
}

TEST_CASE("rep named arg length_out overrides row count") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // length_out=3 produces only 3 elements - but update requires same row count,
    // so length_out must equal rows; here we verify via select
    auto ir = require_ir("t[update { c = rep(9, length_out=5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 9);
    }
}

TEST_CASE("rep column reference with each repeats each element") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{10, 20, 30, 40, 50, 60});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // rep(x, each=2) - each element twice, cycling to 6 rows
    // pattern: 10,10,20,20,30,30 (exactly 6 rows)
    auto ir = require_ir("t[update { c = rep(x, each=2) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 6);
    CHECK((*iv)[0] == 10);
    CHECK((*iv)[1] == 10);
    CHECK((*iv)[2] == 20);
    CHECK((*iv)[3] == 20);
    CHECK((*iv)[4] == 30);
    CHECK((*iv)[5] == 30);
}

TEST_CASE("rep unknown named argument returns error") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = rep(1, foo=3) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

// --- fill_null / fill_forward / fill_backward tests --------------------------

TEST_CASE("fill_null replaces null cells with constant (Int)", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{10, 0, 30, 0, 50});
    t.columns[t.index.at("val")].validity = std::vector<bool>{true, false, true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val, 0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("v2");
    REQUIRE(entry != nullptr);
    // No validity bitmap - all rows are now valid.
    CHECK_FALSE(entry->validity.has_value());
    const auto& v2 = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(v2[0] == 10);
    CHECK(v2[1] == 0);  // was null, filled with 0
    CHECK(v2[2] == 30);
    CHECK(v2[3] == 0);  // was null, filled with 0
    CHECK(v2[4] == 50);
}

TEST_CASE("fill_null replaces null cells with constant (Float)", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<double>{1.0, 0.0, 3.0});
    t.columns[t.index.at("val")].validity = std::vector<bool>{true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val, 99.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("v2");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& v2 = std::get<Column<double>>(*entry->column);
    CHECK(v2[0] == Catch::Approx(1.0));
    CHECK(v2[1] == Catch::Approx(99.5));  // was null, filled with 99.5
    CHECK(v2[2] == Catch::Approx(3.0));
}

TEST_CASE("fill_null on column with no nulls is a no-op", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    // No validity bitmap - no nulls.

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val, 99) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("v2");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& v2 = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(v2[0] == 1);
    CHECK(v2[1] == 2);
    CHECK(v2[2] == 3);
}

TEST_CASE("fill_forward carries last valid value forward (LOCF)", "[null][fill]") {
    runtime::Table t;
    //                      valid null  valid null  null  valid
    t.add_column("val", Column<std::int64_t>{10, 0, 20, 0, 0, 30});
    t.columns[t.index.at("val")].validity =
        std::vector<bool>{true, false, true, false, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("fwd");
    REQUIRE(entry != nullptr);
    // No leading nulls - all rows should be valid after fill.
    CHECK_FALSE(entry->validity.has_value());
    const auto& fwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(fwd[0] == 10);
    CHECK(fwd[1] == 10);  // carried from row 0
    CHECK(fwd[2] == 20);
    CHECK(fwd[3] == 20);  // carried from row 2
    CHECK(fwd[4] == 20);  // carried from row 2
    CHECK(fwd[5] == 30);
}

TEST_CASE("fill_forward leaves leading nulls as null", "[null][fill]") {
    runtime::Table t;
    //                      null  null  valid null
    t.add_column("val", Column<std::int64_t>{0, 0, 5, 0});
    t.columns[t.index.at("val")].validity = std::vector<bool>{false, false, true, false};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("fwd");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK(runtime::is_null(*entry, 0));  // leading null - unfillable
    CHECK(runtime::is_null(*entry, 1));  // leading null - unfillable
    CHECK_FALSE(runtime::is_null(*entry, 2));
    CHECK_FALSE(runtime::is_null(*entry, 3));  // filled from row 2
    const auto& fwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(fwd[2] == 5);
    CHECK(fwd[3] == 5);
}

TEST_CASE("fill_forward on column with no nulls is a no-op", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("fwd");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& fwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(fwd[0] == 1);
    CHECK(fwd[1] == 2);
    CHECK(fwd[2] == 3);
}

TEST_CASE("fill_backward carries next valid value backward (NOCB)", "[null][fill]") {
    runtime::Table t;
    //                      valid null  null  valid null  valid
    t.add_column("val", Column<std::int64_t>{10, 0, 0, 20, 0, 30});
    t.columns[t.index.at("val")].validity =
        std::vector<bool>{true, false, false, true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bwd = fill_backward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bwd");
    REQUIRE(entry != nullptr);
    // No trailing nulls - all rows should be valid after fill.
    CHECK_FALSE(entry->validity.has_value());
    const auto& bwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(bwd[0] == 10);
    CHECK(bwd[1] == 20);  // carried from row 3
    CHECK(bwd[2] == 20);  // carried from row 3
    CHECK(bwd[3] == 20);
    CHECK(bwd[4] == 30);  // carried from row 5
    CHECK(bwd[5] == 30);
}

TEST_CASE("fill_backward leaves trailing nulls as null", "[null][fill]") {
    runtime::Table t;
    //                      null  valid null  null
    t.add_column("val", Column<std::int64_t>{0, 5, 0, 0});
    t.columns[t.index.at("val")].validity = std::vector<bool>{false, true, false, false};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bwd = fill_backward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bwd");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK_FALSE(runtime::is_null(*entry, 0));  // filled from row 1
    CHECK_FALSE(runtime::is_null(*entry, 1));
    CHECK(runtime::is_null(*entry, 2));  // trailing null - unfillable
    CHECK(runtime::is_null(*entry, 3));  // trailing null - unfillable
    const auto& bwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(bwd[0] == 5);
    CHECK(bwd[1] == 5);
}

TEST_CASE("fill_backward on column with no nulls is a no-op", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bwd = fill_backward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bwd");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& bwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(bwd[0] == 1);
    CHECK(bwd[1] == 2);
    CHECK(bwd[2] == 3);
}

TEST_CASE("fill_null wrong argument count returns error", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
    CHECK(result.error().find("fill_null") != std::string::npos);
}

TEST_CASE("fill_forward wrong argument count returns error", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val, val) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
    CHECK(result.error().find("fill_forward") != std::string::npos);
}

TEST_CASE("is_nan returns Bool mask for Float64 columns and propagates null", "[nan][clean]") {
    // is_nan is an ordinary null-propagating scalar: NaN is a property of a
    // *present* value, so is_nan(null) is null (unknown), not false. Asking
    // "NaN or missing?" is spelled is_nan(x) || is_null(x).
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(), 3.0, 0.0},
                 std::vector<bool>{true, true, true, false});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bad = is_nan(x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bad");
    REQUIRE(entry != nullptr);
    const auto& bad = std::get<Column<bool>>(*entry->column);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0] == true);
    CHECK((*entry->validity)[1] == true);
    CHECK((*entry->validity)[2] == true);
    CHECK((*entry->validity)[3] == false);
    CHECK(bad[0] == false);
    CHECK(bad[1] == true);
    CHECK(bad[2] == false);
}

TEST_CASE("null_if_nan marks NaN values as null", "[nan][clean]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(), 3.0, 0.0},
                 std::vector<bool>{true, true, true, false});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { x_clean = null_if_nan(x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("x_clean");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    const auto& x_clean = std::get<Column<double>>(*entry->column);
    CHECK(x_clean[0] == Catch::Approx(1.0));
    CHECK(runtime::is_null(*entry, 1));
    CHECK(x_clean[2] == Catch::Approx(3.0));
    CHECK(runtime::is_null(*entry, 3));
}

TEST_CASE("null_if_not_finite marks NaN and infinities as null", "[nan][clean]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(),
                                     std::numeric_limits<double>::infinity(),
                                     -std::numeric_limits<double>::infinity(), 5.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { x_clean = null_if_not_finite(x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("x_clean");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    const auto& x_clean = std::get<Column<double>>(*entry->column);
    CHECK(x_clean[0] == Catch::Approx(1.0));
    CHECK(runtime::is_null(*entry, 1));
    CHECK(runtime::is_null(*entry, 2));
    CHECK(runtime::is_null(*entry, 3));
    CHECK(x_clean[4] == Catch::Approx(5.0));
}

TEST_CASE("aggregates ignore values nulled by null_if_nan", "[nan][clean][agg]") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 1, 2, 2});
    t.add_column("x", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(), 3.0,
                                     std::numeric_limits<double>::quiet_NaN(),
                                     std::numeric_limits<double>::quiet_NaN()});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[update { x_clean = null_if_nan(x) }]"
        "[select { g, sx = sum(x_clean), mx = mean(x_clean) }, by g, order g];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& g = std::get<Column<std::int64_t>>(*result->find("g"));
    const auto* sx_entry = result->find_entry("sx");
    const auto* mx_entry = result->find_entry("mx");
    REQUIRE(sx_entry != nullptr);
    REQUIRE(mx_entry != nullptr);
    const auto& sx = std::get<Column<double>>(*sx_entry->column);
    const auto& mx = std::get<Column<double>>(*mx_entry->column);

    CHECK(g[0] == 1);
    CHECK(sx[0] == Catch::Approx(4.0));
    CHECK(mx[0] == Catch::Approx(2.0));
    CHECK_FALSE(runtime::is_null(*sx_entry, 0));
    CHECK_FALSE(runtime::is_null(*mx_entry, 0));

    CHECK(g[1] == 2);
    CHECK(runtime::is_null(*sx_entry, 1));
    CHECK(runtime::is_null(*mx_entry, 1));
}

TEST_CASE("aggregates accept direct null_if_nan wrapper", "[nan][clean][agg]") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 1, 2, 2});
    t.add_column("x", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(), 3.0,
                                     std::numeric_limits<double>::quiet_NaN(),
                                     std::numeric_limits<double>::quiet_NaN()});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[select { g, sx = sum(null_if_nan(x)), mx = mean(null_if_nan(x)) }, by g, order g];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& g = std::get<Column<std::int64_t>>(*result->find("g"));
    const auto* sx_entry = result->find_entry("sx");
    const auto* mx_entry = result->find_entry("mx");
    REQUIRE(sx_entry != nullptr);
    REQUIRE(mx_entry != nullptr);
    const auto& sx = std::get<Column<double>>(*sx_entry->column);
    const auto& mx = std::get<Column<double>>(*mx_entry->column);

    CHECK(g[0] == 1);
    CHECK(sx[0] == Catch::Approx(4.0));
    CHECK(mx[0] == Catch::Approx(2.0));
    CHECK_FALSE(runtime::is_null(*sx_entry, 0));
    CHECK_FALSE(runtime::is_null(*mx_entry, 0));

    CHECK(g[1] == 2);
    CHECK(runtime::is_null(*sx_entry, 1));
    CHECK(runtime::is_null(*mx_entry, 1));
}

TEST_CASE("aggregates accept direct computed inputs", "[agg]") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 2, 2});
    t.add_column("x", Column<double>{1.0, 3.0, 10.0, 14.0});
    t.add_column("y", Column<double>{2.0, 4.0, 20.0, 22.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[select { g, mx = mean(x + y), sx = sum(x + y) }, by g, order g];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& g = std::get<Column<std::int64_t>>(*result->find("g"));
    const auto& mx = std::get<Column<double>>(*result->find("mx"));
    const auto& sx = std::get<Column<double>>(*result->find("sx"));

    CHECK(g[0] == 1);
    CHECK(mx[0] == Catch::Approx(5.0));
    CHECK(sx[0] == Catch::Approx(10.0));
    CHECK(g[1] == 2);
    CHECK(mx[1] == Catch::Approx(33.0));
    CHECK(sx[1] == Catch::Approx(66.0));
}

TEST_CASE("update: abs and sqrt work in interpreted expressions", "[update][math]") {
    runtime::Table table;
    table.add_column("x", Column<double>{-4.0, -9.0, 16.0});
    table.add_column("y", Column<std::int64_t>{-3, 0, 12});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { abs_x = abs(x), abs_y = abs(y), root = sqrt(abs(x)) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& abs_x = std::get<Column<double>>(*result->find("abs_x"));
    const auto& abs_y = std::get<Column<std::int64_t>>(*result->find("abs_y"));
    const auto& root = std::get<Column<double>>(*result->find("root"));

    CHECK(abs_x[0] == Catch::Approx(4.0));
    CHECK(abs_x[1] == Catch::Approx(9.0));
    CHECK(abs_x[2] == Catch::Approx(16.0));

    CHECK(abs_y[0] == 3);
    CHECK(abs_y[1] == 0);
    CHECK(abs_y[2] == 12);

    CHECK(root[0] == Catch::Approx(2.0));
    CHECK(root[1] == Catch::Approx(3.0));
    CHECK(root[2] == Catch::Approx(4.0));
}

TEST_CASE("update: log and exp work on Float and Int columns and round-trip", "[update][math]") {
    runtime::Table table;
    table.add_column("price", Column<double>{1.0, 10.0, 100.0});
    table.add_column("qty", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { lp = log(price), ep = exp(log(price)), lq = log(qty) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& lp = std::get<Column<double>>(*result->find("lp"));
    const auto& ep = std::get<Column<double>>(*result->find("ep"));
    const auto& lq = std::get<Column<double>>(*result->find("lq"));

    CHECK(lp[0] == Catch::Approx(0.0));
    CHECK(lp[1] == Catch::Approx(std::log(10.0)));
    CHECK(lp[2] == Catch::Approx(std::log(100.0)));

    CHECK(ep[0] == Catch::Approx(1.0));
    CHECK(ep[1] == Catch::Approx(10.0));
    CHECK(ep[2] == Catch::Approx(100.0));

    // log on Int column widens to Float64.
    CHECK(lq[0] == Catch::Approx(0.0));
    CHECK(lq[1] == Catch::Approx(std::log(2.0)));
    CHECK(lq[2] == Catch::Approx(std::log(3.0)));
}

TEST_CASE("update: log/exp reject non-numeric arguments", "[update][math]") {
    runtime::Table table;
    table.add_column("name", Column<std::string>{"a", "b"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { lp = log(name) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("log:") != std::string::npos);
}

TEST_CASE("update: rowwise min/max work in interpreted expressions", "[update][math]") {
    runtime::Table table;
    table.add_column("a", Column<double>{10.0, 10.2});
    table.add_column("b", Column<double>{9.9, 10.1});
    table.add_column("c", Column<double>{10.1, 10.3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir(
        "t[update { max_abc = pmax(a, b, c), min_abc = pmin(a, b, c), "
        "mid_abc = a + b + c - pmax(a, b, c) - pmin(a, b, c) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& max_abc = std::get<Column<double>>(*result->find("max_abc"));
    const auto& min_abc = std::get<Column<double>>(*result->find("min_abc"));
    const auto& mid_abc = std::get<Column<double>>(*result->find("mid_abc"));

    CHECK(max_abc[0] == Catch::Approx(10.1));
    CHECK(min_abc[0] == Catch::Approx(9.9));
    CHECK(mid_abc[0] == Catch::Approx(10.0));
    CHECK(max_abc[1] == Catch::Approx(10.3));
    CHECK(min_abc[1] == Catch::Approx(10.1));
    CHECK(mid_abc[1] == Catch::Approx(10.2));
}

TEST_CASE("rep missing positional argument returns error") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // rep() with no positional args should fail
    auto ir = require_ir("t[update { c = rep(times=3) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("rep array literal cycles elements to table length", "[rep]") {
    // Table(10)[update { g = rep([1,2]) }] -> 1,2,1,2,...
    runtime::Table table;
    table.logical_rows = 10;
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { g = rep([1,2]) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& g = std::get<Column<std::int64_t>>(*result->find("g"));
    REQUIRE(g.size() == 10);
    for (std::size_t i = 0; i < 10; ++i) {
        CHECK(g[i] == static_cast<std::int64_t>((i % 2) + 1));
    }
}

TEST_CASE("rep array literal three elements", "[rep]") {
    runtime::Table table;
    table.logical_rows = 6;
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { g = rep([1,2,3]) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& g = std::get<Column<std::int64_t>>(*result->find("g"));
    REQUIRE(g.size() == 6);
    for (std::size_t i = 0; i < 6; ++i) {
        CHECK(g[i] == static_cast<std::int64_t>((i % 3) + 1));
    }
}

TEST_CASE("rep array literal with each=2 repeats each element", "[rep]") {
    // rep([0,1,2], each=2) -> 0,0,1,1,2,2
    runtime::Table table;
    table.logical_rows = 6;
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { g = rep([0,1,2], each=2) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& g = std::get<Column<std::int64_t>>(*result->find("g"));
    REQUIRE(g.size() == 6);
    const std::int64_t expected[] = {0, 0, 1, 1, 2, 2};
    for (std::size_t i = 0; i < 6; ++i) {
        CHECK(g[i] == expected[i]);
    }
}

TEST_CASE("rep array literal string labels", "[rep]") {
    runtime::Table table;
    table.logical_rows = 7;
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { label = rep([\"a\",\"b\",\"c\"]) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& label = std::get<Column<std::string>>(*result->find("label"));
    REQUIRE(label.size() == 7);
    const std::string_view expected[] = {"a", "b", "c", "a", "b", "c", "a"};
    for (std::size_t i = 0; i < 7; ++i) {
        CHECK(label[i] == expected[i]);
    }
}

TEST_CASE("string interpolation builds a String column in update", "[interp]") {
    // `row ${id}: ${g}` desugars to __interp(...) and produces a String column
    // per row. This also exercises the per-row String-column builder.
    runtime::Table t;
    t.add_column("id", Column<std::int64_t>{1, 2, 3});
    t.add_column("g", Column<std::string>{"a", "b", "c"});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { label = `row ${id}: ${g}` }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* label = std::get_if<Column<std::string>>(result->find("label"));
    REQUIRE(label != nullptr);
    REQUIRE(label->size() == 3);
    CHECK((*label)[0] == "row 1: a");
    CHECK((*label)[1] == "row 2: b");
    CHECK((*label)[2] == "row 3: c");
}

TEST_CASE("string interpolation formats numeric values", "[interp]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.5, 2.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { s = `v=${x}` }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& s = std::get<Column<std::string>>(*result->find("s"));
    REQUIRE(s.size() == 2);
    CHECK(s[0] == "v=1.5");
    CHECK(s[1] == "v=2");
}

TEST_CASE("transcendental builtins in a column update", "[math]") {
    runtime::Table t;
    t.add_column("x", Column<double>{0.0, 1.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[update { s = sin(x), c = cos(x), t2 = tan(0.0), l2 = log2(8.0), "
        "l10 = log10(1000.0), at = atan(0.0), th = tanh(0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& s = std::get<Column<double>>(*result->find("s"));
    const auto& c = std::get<Column<double>>(*result->find("c"));
    const auto& l2 = std::get<Column<double>>(*result->find("l2"));
    const auto& l10 = std::get<Column<double>>(*result->find("l10"));
    CHECK(s[0] == Catch::Approx(0.0));
    CHECK(s[1] == Catch::Approx(std::sin(1.0)));
    CHECK(c[0] == Catch::Approx(1.0));
    CHECK(c[1] == Catch::Approx(std::cos(1.0)));
    CHECK(l2[0] == Catch::Approx(3.0));   // log2(8) = 3
    CHECK(l10[0] == Catch::Approx(3.0));  // log10(1000) = 3
    CHECK(std::get<Column<double>>(*result->find("t2"))[0] == Catch::Approx(0.0));
    CHECK(std::get<Column<double>>(*result->find("at"))[0] == Catch::Approx(0.0));
    CHECK(std::get<Column<double>>(*result->find("th"))[0] == Catch::Approx(0.0));
}

TEST_CASE("transcendental builtin accepts an int column (promotes to Float64)", "[math]") {
    runtime::Table t;
    t.add_column("n", Column<std::int64_t>{1, 100});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { l = log10(n) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& l = std::get<Column<double>>(*result->find("l"));
    CHECK(l[0] == Catch::Approx(0.0));  // log10(1) = 0
    CHECK(l[1] == Catch::Approx(2.0));  // log10(100) = 2
}

// --- Table constructor from column vectors ------------------------------------

TEST_CASE("Table constructor creates table from integer columns") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { price = [10, 20, 30], qty = [1, 2, 3] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* price = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[0] == 10);
    REQUIRE((*price)[1] == 20);
    REQUIRE((*price)[2] == 30);

    const auto* qty = std::get_if<Column<std::int64_t>>(result->find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE((*qty)[0] == 1);
    REQUIRE((*qty)[1] == 2);
    REQUIRE((*qty)[2] == 3);
}

TEST_CASE("Table constructor creates table from string and float columns") {
    runtime::TableRegistry registry;
    auto ir = require_ir(R"(Table { symbol = ["A", "B", "C"], price = [1.5, 2.5, 3.5] };)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(symbol != nullptr);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*symbol)[1] == "B");
    REQUIRE((*symbol)[2] == "C");

    const auto* price = std::get_if<Column<double>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[0] == Catch::Approx(1.5));
    REQUIRE((*price)[1] == Catch::Approx(2.5));
    REQUIRE((*price)[2] == Catch::Approx(3.5));
}

TEST_CASE("Table constructor creates table from bool column") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { active = [true, false, true] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* active = std::get_if<Column<bool>>(result->find("active"));
    REQUIRE(active != nullptr);
    REQUIRE((*active)[0] == true);
    REQUIRE((*active)[1] == false);
    REQUIRE((*active)[2] == true);
}

TEST_CASE("Table constructor creates empty table") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.empty());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Table constructor can be filtered") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { x = [1, 2, 3, 4, 5] }[filter x > 3];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE((*x)[0] == 4);
    REQUIRE((*x)[1] == 5);
}

TEST_CASE("Table constructor mismatched column lengths returns error") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { a = [1, 2, 3], b = [4, 5] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("TimeFrame from Table constructor via as_timeframe") {
    runtime::TableRegistry registry;
    auto ir = require_ir(
        R"(as_timeframe(Table { ts = [1000, 2000, 3000], price = [10, 20, 30] }, "ts");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
    REQUIRE(result->rows() == 3);
}

TEST_CASE("Table constructor with single column") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { vals = [42] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* vals = std::get_if<Column<std::int64_t>>(result->find("vals"));
    REQUIRE(vals != nullptr);
    REQUIRE((*vals)[0] == 42);
}

TEST_CASE("Table constructor with column from existing table (single-column select)") {
    // `prices[select { price }]` produces a single-column Table; Table { p = ... } picks it up.
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> price_col;
    price_col.push_back(10);
    price_col.push_back(20);
    price_col.push_back(30);
    src.add_column("price", price_col);
    registry["prices"] = std::move(src);

    auto ir = require_ir("Table { p = prices[select { price }] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* p = std::get_if<Column<std::int64_t>>(result->find("p"));
    REQUIRE(p != nullptr);
    REQUIRE((*p)[0] == 10);
    REQUIRE((*p)[1] == 20);
    REQUIRE((*p)[2] == 30);
}

TEST_CASE("Table constructor with named column from multi-column expression") {
    // The expression returns a multi-column Table; we extract the column matching the def name.
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> price_col;
    price_col.push_back(1);
    price_col.push_back(2);
    Column<std::int64_t> qty_col;
    qty_col.push_back(10);
    qty_col.push_back(20);
    src.add_column("price", price_col);
    src.add_column("qty", qty_col);
    registry["trades"] = std::move(src);

    auto ir = require_ir("Table { price = trades, qty = trades };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* price = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[0] == 1);
    REQUIRE((*price)[1] == 2);

    const auto* qty = std::get_if<Column<std::int64_t>>(result->find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE((*qty)[0] == 10);
    REQUIRE((*qty)[1] == 20);
}

TEST_CASE("Table constructor mixing literal and expression columns") {
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<double> price_col;
    price_col.push_back(1.5);
    price_col.push_back(2.5);
    price_col.push_back(3.5);
    src.add_column("price", price_col);
    registry["data"] = std::move(src);

    auto ir = require_ir(R"(Table { label = ["a", "b", "c"], price = data[select { price }] };)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* label = std::get_if<Column<std::string>>(result->find("label"));
    REQUIRE(label != nullptr);
    REQUIRE((*label)[0] == "a");

    const auto* price = std::get_if<Column<double>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[2] == Catch::Approx(3.5));
}

TEST_CASE("Table constructor expression column length mismatch returns error") {
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> col;
    col.push_back(1);
    col.push_back(2);
    src.add_column("x", col);
    registry["data"] = std::move(src);

    // Literal has 3 elements; expression column produces 2 rows -> length mismatch.
    auto ir = require_ir("Table { a = [1, 2, 3], b = data[select { x }] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Table constructor expression column not found in multi-column result returns error") {
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> col;
    col.push_back(1);
    src.add_column("x", col);
    registry["data"] = std::move(src);

    // "missing" does not exist in data (which has only "x"); multi-column, no name match.
    // data has one column "x" but the def name is "missing" - single-col path renames it,
    // so we test with two columns to exercise the error path.
    runtime::Table two_col;
    Column<std::int64_t> a_col, b_col;
    a_col.push_back(1);
    b_col.push_back(2);
    two_col.add_column("a", a_col);
    two_col.add_column("b", b_col);
    registry["two"] = std::move(two_col);

    auto ir = require_ir("Table { missing = two };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

// --- Melt (wide -> long) -------------------------------------------------------

TEST_CASE("melt: basic wide-to-long unpivot", "[melt]") {
    // wide: symbol | open | close
    //       AAPL     100    110
    //       GOOG     200    210
    // long: symbol | variable | value
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("open", Column<std::int64_t>{100, 200});
    wide.add_column("close", Column<std::int64_t>{110, 210});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // 2 rows x 2 measures = 4 output rows
    REQUIRE(result->rows() == 4);

    const auto* sym = result->find("symbol");
    REQUIRE(sym != nullptr);
    const auto* var = result->find("variable");
    REQUIRE(var != nullptr);
    const auto* val = result->find("value");
    REQUIRE(val != nullptr);

    const auto& sym_col = std::get<Column<std::string>>(*sym);
    // variable column is now Categorical (n_measures distinct values)
    const auto& var_col = std::get<Column<Categorical>>(*var);
    const auto& val_col = std::get<Column<std::int64_t>>(*val);

    // Row order: for each input row, iterate measures in column order.
    REQUIRE(sym_col[0] == "AAPL");
    REQUIRE(var_col[0] == "open");
    REQUIRE(val_col[0] == 100);

    REQUIRE(sym_col[1] == "AAPL");
    REQUIRE(var_col[1] == "close");
    REQUIRE(val_col[1] == 110);

    REQUIRE(sym_col[2] == "GOOG");
    REQUIRE(var_col[2] == "open");
    REQUIRE(val_col[2] == 200);

    REQUIRE(sym_col[3] == "GOOG");
    REQUIRE(var_col[3] == "close");
    REQUIRE(val_col[3] == 210);
}

TEST_CASE("melt: select restricts measure columns", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("open", Column<std::int64_t>{100, 200});
    wide.add_column("close", Column<std::int64_t>{110, 210});
    wide.add_column("volume", Column<std::int64_t>{1000, 2000});
    registry["wide"] = std::move(wide);

    // Only melt open and close, leave volume out.
    auto ir = require_ir("wide[melt symbol, select { open, close }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);  // 2 rows x 2 measures

    const auto* var = result->find("variable");
    REQUIRE(var != nullptr);
    // variable column is Categorical
    const auto& var_col = std::get<Column<Categorical>>(*var);
    REQUIRE(var_col[0] == "open");
    REQUIRE(var_col[1] == "close");
}

TEST_CASE("melt: bool measure columns preserve row-major values", "[melt][bool]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("halted", Column<bool>{true, false});
    wide.add_column("crossed", Column<bool>{false, true});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    const auto* sym = result->find("symbol");
    const auto* var = result->find("variable");
    const auto* val = result->find("value");
    REQUIRE(sym != nullptr);
    REQUIRE(var != nullptr);
    REQUIRE(val != nullptr);

    const auto& sym_col = std::get<Column<std::string>>(*sym);
    const auto& var_col = std::get<Column<Categorical>>(*var);
    const auto& val_col = std::get<Column<bool>>(*val);

    REQUIRE(sym_col[0] == "AAPL");
    REQUIRE(var_col[0] == "halted");
    REQUIRE(val_col[0] == true);

    REQUIRE(sym_col[1] == "AAPL");
    REQUIRE(var_col[1] == "crossed");
    REQUIRE(val_col[1] == false);

    REQUIRE(sym_col[2] == "GOOG");
    REQUIRE(var_col[2] == "halted");
    REQUIRE(val_col[2] == false);

    REQUIRE(sym_col[3] == "GOOG");
    REQUIRE(var_col[3] == "crossed");
    REQUIRE(val_col[3] == true);
}

TEST_CASE("melt: multiple id columns", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("date", Column<std::int64_t>{1, 2});
    wide.add_column("open", Column<double>{100.0, 200.0});
    wide.add_column("close", Column<double>{110.0, 210.0});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt { symbol, date }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    REQUIRE(result->find("symbol") != nullptr);
    REQUIRE(result->find("date") != nullptr);
    REQUIRE(result->find("variable") != nullptr);
    REQUIRE(result->find("value") != nullptr);
}

TEST_CASE("melt: id column not found returns error", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("open", Column<std::int64_t>{100});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt no_such_col];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("id column not found") != std::string::npos);
}

TEST_CASE("melt: measure type mismatch returns error", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("id", Column<std::string>{"A"});
    wide.add_column("int_col", Column<std::int64_t>{1});
    wide.add_column("float_col", Column<double>{1.0});
    registry["wide"] = std::move(wide);

    // Mixing int and float measure columns is not allowed.
    auto ir = require_ir("wide[melt id, select { int_col, float_col }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("same type") != std::string::npos);
}

// --- Dcast (long -> wide) -----------------------------------------------------

TEST_CASE("dcast: basic long-to-wide pivot", "[dcast]") {
    // long: variable | value
    //       open       100
    //       close      110
    // wide: open | close
    //       100    110
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("variable", Column<std::string>{"open", "close"});
    lng.add_column("value", Column<std::int64_t>{100, 110});
    registry["lng"] = std::move(lng);

    // by {} means no row keys (all rows form one group)
    auto ir = require_ir("lng[dcast variable, select value, by {}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* open_col = result->find("open");
    REQUIRE(open_col != nullptr);
    const auto* close_col = result->find("close");
    REQUIRE(close_col != nullptr);

    REQUIRE(std::get<Column<std::int64_t>>(*open_col)[0] == 100);
    REQUIRE(std::get<Column<std::int64_t>>(*close_col)[0] == 110);
}

TEST_CASE("dcast: round-trips melt", "[dcast][melt]") {
    // wide -> melt -> dcast should reproduce the original wide table structure.
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("open", Column<std::int64_t>{100, 200});
    wide.add_column("close", Column<std::int64_t>{110, 210});
    registry["wide"] = std::move(wide);

    // Melt first.
    auto melted_ir = require_ir("wide[melt symbol];");
    auto melted = runtime::interpret(*melted_ir, registry);
    REQUIRE(melted.has_value());
    registry["long"] = std::move(*melted);

    // Then dcast with symbol as row key.
    auto ir = require_ir("long[dcast variable, select value, by { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    REQUIRE(result->find("open") != nullptr);
    REQUIRE(result->find("close") != nullptr);

    const auto& open_col = std::get<Column<std::int64_t>>(*result->find("open"));
    REQUIRE(open_col[0] == 100);
    REQUIRE(open_col[1] == 200);
}

TEST_CASE("dcast: missing cell filled with null", "[dcast]") {
    // Two symbols; AAPL has both open/close, GOOG only has open.
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("symbol", Column<std::string>{"AAPL", "AAPL", "GOOG"});
    lng.add_column("variable", Column<std::string>{"open", "close", "open"});
    lng.add_column("value", Column<std::int64_t>{100, 110, 200});
    registry["lng"] = std::move(lng);

    auto ir = require_ir("lng[dcast variable, select value, by { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    // close column should have a null for GOOG.
    const auto* close_entry = result->find_entry("close");
    REQUIRE(close_entry != nullptr);
    REQUIRE(close_entry->validity.has_value());

    // Find which row is GOOG.
    const auto& sym_col = std::get<Column<std::string>>(*result->find("symbol"));
    std::size_t goog_row = (sym_col[0] == "GOOG") ? 0 : 1;
    REQUIRE_FALSE((*close_entry->validity)[goog_row]);
}

TEST_CASE("dcast: pivot column not found returns error", "[dcast]") {
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("variable", Column<std::string>{"open"});
    lng.add_column("value", Column<std::int64_t>{100});
    registry["lng"] = std::move(lng);

    auto ir = require_ir("lng[dcast no_such_col, select value, by {}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("pivot column not found") != std::string::npos);
}

TEST_CASE("dcast: value column not found returns error", "[dcast]") {
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("variable", Column<std::string>{"open"});
    lng.add_column("value", Column<std::int64_t>{100});
    registry["lng"] = std::move(lng);

    auto ir = require_ir("lng[dcast variable, select no_such_val, by {}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("value column not found") != std::string::npos);
}

// --- ExternCall node ---------------------------------------------------------

// Helper: extern fn declaration so the lowerer recognises the function.
static constexpr const char* kMakePricesDecl =
    R"(extern fn make_prices(n: Int) -> DataFrame from "prices.hpp"; make_prices(0);)";

TEST_CASE("ExternCall: table-returning extern is resolved and called", "[extern]") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    // Register a simple table-returning extern that ignores its argument.
    externs.register_table("make_prices", [](const runtime::ExternArgs&) {
        runtime::Table t;
        t.add_column("price", Column<std::int64_t>{42, 99});
        return std::expected<runtime::ExternValue, std::string>{t};
    });

    auto ir = require_ir(kMakePricesDecl);
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* price = result->find("price");
    REQUIRE(price != nullptr);
    const auto& prices = std::get<Column<std::int64_t>>(*price);
    REQUIRE(prices[0] == 42);
    REQUIRE(prices[1] == 99);
}

TEST_CASE("ExternCall: no extern registry returns error", "[extern]") {
    runtime::TableRegistry registry;

    // nullptr externs - should get "extern call with no registry" error.
    auto ir = require_ir(kMakePricesDecl);
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("no registry") != std::string::npos);
}

TEST_CASE("ExternCall: unknown extern function returns error", "[extern]") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;  // empty - make_prices not registered

    auto ir = require_ir(kMakePricesDecl);
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("unknown extern function") != std::string::npos);
}

// --- Grouped update with aggregate function ---------------------------------

TEST_CASE("grouped update broadcasts aggregate values per group", "[update]") {
    runtime::TableRegistry registry;
    runtime::Table trades;
    trades.add_column("symbol", Column<std::string>{"AAPL", "AAPL", "GOOG"});
    trades.add_column("price", Column<std::int64_t>{100, 110, 200});
    registry["trades"] = std::move(trades);

    auto ir = require_ir(
        "trades[update { mean_price = mean(price), centered = price - mean_price }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* mean_col = std::get_if<Column<double>>(result->find("mean_price"));
    REQUIRE(mean_col != nullptr);
    REQUIRE((*mean_col)[0] == 105.0);
    REQUIRE((*mean_col)[1] == 105.0);
    REQUIRE((*mean_col)[2] == 200.0);

    const auto* centered_col = std::get_if<Column<double>>(result->find("centered"));
    REQUIRE(centered_col != nullptr);
    REQUIRE((*centered_col)[0] == -5.0);
    REQUIRE((*centered_col)[1] == 5.0);
    REQUIRE((*centered_col)[2] == 0.0);
}

TEST_CASE("update scalar math builtins vectorise (sqrt/abs/floor/ceil/round)", "[update][scalar]") {
    // These row-wise math builtins must take the compiled numeric fast path
    // (SIMD for column args, tree-walk for computed args), not the per-row
    // scalar registry. Checks values, type, round modes, and the computed-arg
    // fallback.
    runtime::TableRegistry registry;
    runtime::Table t;
    t.add_column("x", Column<double>{2.4, -2.5, 3.5, 9.0});
    registry["t"] = std::move(t);

    auto ir = require_ir(
        "t[update { sq = sqrt(abs(x)), fl = floor(x), ce = ceil(x), ab = abs(x), "
        "rn = round(x, nearest), rb = round(x, bankers), comp = sqrt(x * x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* ab = std::get_if<Column<double>>(result->find("ab"));
    REQUIRE(ab != nullptr);
    REQUIRE((*ab)[1] == Catch::Approx(2.5));

    const auto* fl = std::get_if<Column<double>>(result->find("fl"));
    REQUIRE((*fl)[0] == Catch::Approx(2.0));
    REQUIRE((*fl)[1] == Catch::Approx(-3.0));

    const auto* ce = std::get_if<Column<double>>(result->find("ce"));
    REQUIRE((*ce)[0] == Catch::Approx(3.0));
    REQUIRE((*ce)[1] == Catch::Approx(-2.0));

    const auto* sq = std::get_if<Column<double>>(result->find("sq"));
    REQUIRE((*sq)[2] == Catch::Approx(std::sqrt(3.5)));

    // round(x, mode) yields Int64; nearest rounds half away, bankers ties-to-even.
    const auto* rn = std::get_if<Column<std::int64_t>>(result->find("rn"));
    REQUIRE(rn != nullptr);
    REQUIRE((*rn)[1] == -3);  // round(-2.5, nearest)
    REQUIRE((*rn)[2] == 4);   // round(3.5, nearest)
    const auto* rb = std::get_if<Column<std::int64_t>>(result->find("rb"));
    REQUIRE(rb != nullptr);
    REQUIRE((*rb)[1] == -2);  // round(-2.5, bankers) -> ties to even
    REQUIRE((*rb)[2] == 4);   // round(3.5, bankers)  -> ties to even

    // Computed argument exercises the tree-walk UnaryDouble path.
    const auto* comp = std::get_if<Column<double>>(result->find("comp"));
    REQUIRE(comp != nullptr);
    REQUIRE((*comp)[0] == Catch::Approx(2.4));  // sqrt(2.4^2)
    REQUIRE((*comp)[3] == Catch::Approx(9.0));
}

TEST_CASE("round nearest matches llround at the double-rounding boundary", "[update][scalar]") {
    // The vectorised round(nearest) kernel rounds half away from zero via
    // trunc/fabs/copysign, NOT floor(x + 0.5) — the latter mis-rounds the
    // largest double below 0.5 (nextafter(0.5, 0)) up to 1 because x + 0.5
    // rounds to 1.0. Guard that boundary and a few ties against std::llround.
    runtime::TableRegistry registry;
    runtime::Table t;
    const double just_below_half = std::nextafter(0.5, 0.0);  // 0.49999999999999994
    t.add_column("x", Column<double>{just_below_half, -just_below_half, 0.5, -0.5, 2.5});
    registry["t"] = std::move(t);

    auto ir = require_ir("t[update { r = round(x, nearest) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* r = std::get_if<Column<std::int64_t>>(result->find("r"));
    REQUIRE(r != nullptr);
    CHECK((*r)[0] == std::llround(just_below_half));   // 0, not 1
    CHECK((*r)[1] == std::llround(-just_below_half));  // 0
    CHECK((*r)[2] == std::llround(0.5));               // 1 (away from zero)
    CHECK((*r)[3] == std::llround(-0.5));              // -1
    CHECK((*r)[4] == std::llround(2.5));               // 3
}

TEST_CASE("update log/exp match scalar libm (SIMD path or fallback)", "[update][scalar]") {
    // log/exp may use the libmvec SIMD path (where available) or the scalar
    // tree-walk; both must agree with std::log/std::exp to Approx, for a bare
    // column and a computed argument.
    runtime::TableRegistry registry;
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.5, 10.0, 100.0});
    registry["t"] = std::move(t);

    auto ir = require_ir("t[update { lg = log(x), ex = exp(x / 100.0), lg_comp = log(x * 2.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* lg = std::get_if<Column<double>>(result->find("lg"));
    REQUIRE(lg != nullptr);
    const auto* ex = std::get_if<Column<double>>(result->find("ex"));
    REQUIRE(ex != nullptr);
    const auto* lg_comp = std::get_if<Column<double>>(result->find("lg_comp"));
    REQUIRE(lg_comp != nullptr);
    const double xs[] = {1.0, 2.5, 10.0, 100.0};
    for (std::size_t i = 0; i < 4; ++i) {
        REQUIRE((*lg)[i] == Catch::Approx(std::log(xs[i])));
        REQUIRE((*ex)[i] == Catch::Approx(std::exp(xs[i] / 100.0)));
        REQUIRE((*lg_comp)[i] == Catch::Approx(std::log(xs[i] * 2.0)));
    }
}

TEST_CASE("update trig/log transcendentals match scalar libm over a column", "[update][scalar]") {
    // sin/cos/tan/asin/acos/atan/sinh/cosh/tanh/log2/log10 over a bare Double
    // column route through the libmvec SIMD path where available and the scalar
    // tree-walk otherwise; both must agree with std::<fn> to Approx. The column
    // length (10, not a multiple of 4) exercises both the 4-wide body and the
    // scalar tail. Arguments are kept in [-0.9, 0.9] so asin/acos stay in-domain
    // and inputs to log2/log10 are positive.
    const std::vector<double> xs = {-0.9, -0.7, -0.3, -0.1, 0.05, 0.2, 0.4, 0.6, 0.8, 0.9};
    runtime::TableRegistry registry;
    runtime::Table t;
    t.add_column("x", Column<double>(xs));
    // log2/log10 need a strictly positive column.
    std::vector<double> pos;
    pos.reserve(xs.size());
    for (double x : xs) {
        pos.push_back(x + 1.0);  // (0.1, 1.9]
    }
    t.add_column("p", Column<double>(std::move(pos)));
    registry["t"] = std::move(t);

    auto ir = require_ir(
        "t[update { vsin = sin(x), vcos = cos(x), vtan = tan(x), vasin = asin(x), "
        "vacos = acos(x), vatan = atan(x), vsinh = sinh(x), vcosh = cosh(x), vtanh = tanh(x), "
        "vlog2 = log2(p), vlog10 = log10(p) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    auto col = [&](const char* name) -> const Column<double>& {
        const auto* c = std::get_if<Column<double>>(result->find(name));
        REQUIRE(c != nullptr);
        return *c;
    };
    for (std::size_t i = 0; i < xs.size(); ++i) {
        const double x = xs[i];
        const double p = x + 1.0;
        CHECK(col("vsin")[i] == Catch::Approx(std::sin(x)));
        CHECK(col("vcos")[i] == Catch::Approx(std::cos(x)));
        CHECK(col("vtan")[i] == Catch::Approx(std::tan(x)));
        CHECK(col("vasin")[i] == Catch::Approx(std::asin(x)));
        CHECK(col("vacos")[i] == Catch::Approx(std::acos(x)));
        CHECK(col("vatan")[i] == Catch::Approx(std::atan(x)));
        CHECK(col("vsinh")[i] == Catch::Approx(std::sinh(x)));
        CHECK(col("vcosh")[i] == Catch::Approx(std::cosh(x)));
        CHECK(col("vtanh")[i] == Catch::Approx(std::tanh(x)));
        CHECK(col("vlog2")[i] == Catch::Approx(std::log2(p)));
        CHECK(col("vlog10")[i] == Catch::Approx(std::log10(p)));
    }
}

TEST_CASE("grouped update mixes a per-row column with an inline aggregate", "[update]") {
    // The single-expression demean `price - mean(price)` (the aggregate inlined
    // rather than split into a prior field) must evaluate per row within each
    // group: it both reduces (mean) and reads `price` row-wise.
    runtime::TableRegistry registry;
    runtime::Table trades;
    trades.add_column("symbol", Column<std::string>{"AAPL", "AAPL", "AAPL", "GOOG", "GOOG"});
    trades.add_column("price", Column<double>{1.0, 2.0, 3.0, 10.0, 20.0});
    registry["trades"] = std::move(trades);

    auto ir = require_ir("trades[update { d = price - mean(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 5);

    const auto* d = std::get_if<Column<double>>(result->find("d"));
    REQUIRE(d != nullptr);
    REQUIRE((*d)[0] == Catch::Approx(-1.0));  // AAPL mean 2
    REQUIRE((*d)[1] == Catch::Approx(0.0));
    REQUIRE((*d)[2] == Catch::Approx(1.0));
    REQUIRE((*d)[3] == Catch::Approx(-5.0));  // GOOG mean 15
    REQUIRE((*d)[4] == Catch::Approx(5.0));
}

TEST_CASE("update pmin/pmax vectorise (clip, nested, col-col, int)", "[update][pmin]") {
    // pmin/pmax must take the compiled numeric fast path, not the per-row scalar
    // registry. Covers the SIMD 2-arg clip, nested pmin(pmax(...)), column×column,
    // and the integer case (which stays Int).
    runtime::TableRegistry registry;
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 600.0, 500.0, 250.0});
    t.add_column("y", Column<double>{10.0, 20.0, 5.0, 999.0});
    t.add_column("i", Column<std::int64_t>{1, 600, 50, 250});
    registry["t"] = std::move(t);

    auto ir = require_ir(
        "t[update { clip = pmin(x, 500.0), lo = pmax(x, 300.0), "
        "both = pmin(pmax(x, -3.0), 550.0), cc = pmin(x, y), mi = pmin(i, 100) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* clip = std::get_if<Column<double>>(result->find("clip"));
    REQUIRE(clip != nullptr);
    REQUIRE((*clip)[0] == Catch::Approx(1.0));
    REQUIRE((*clip)[1] == Catch::Approx(500.0));
    REQUIRE((*clip)[3] == Catch::Approx(250.0));

    const auto* lo = std::get_if<Column<double>>(result->find("lo"));
    REQUIRE(lo != nullptr);
    REQUIRE((*lo)[0] == Catch::Approx(300.0));
    REQUIRE((*lo)[1] == Catch::Approx(600.0));

    const auto* both = std::get_if<Column<double>>(result->find("both"));
    REQUIRE(both != nullptr);
    REQUIRE((*both)[1] == Catch::Approx(550.0));  // pmin(pmax(600,-3),550)
    REQUIRE((*both)[2] == Catch::Approx(500.0));

    const auto* cc = std::get_if<Column<double>>(result->find("cc"));
    REQUIRE(cc != nullptr);
    REQUIRE((*cc)[1] == Catch::Approx(20.0));  // min(600, 20)
    REQUIRE((*cc)[3] == Catch::Approx(250.0));

    // Integer pmin stays Int (no widening to Double).
    const auto* mi = std::get_if<Column<std::int64_t>>(result->find("mi"));
    REQUIRE(mi != nullptr);
    REQUIRE((*mi)[1] == 100);
    REQUIRE((*mi)[2] == 50);
}

TEST_CASE("grouped update inline z-score divides per-row deviation by group std", "[update]") {
    // Two aggregates in one expression combined with a per-row column:
    // `(price - mean(price)) / std(price)`. Each group's z-scores have mean 0.
    runtime::TableRegistry registry;
    runtime::Table trades;
    trades.add_column("symbol", Column<std::string>{"A", "A", "A", "B", "B", "B"});
    trades.add_column("price", Column<double>{2.0, 4.0, 6.0, 10.0, 20.0, 30.0});
    registry["trades"] = std::move(trades);

    auto ir = require_ir("trades[update { z = (price - mean(price)) / std(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 6);

    const auto* z = std::get_if<Column<double>>(result->find("z"));
    REQUIRE(z != nullptr);
    // Group A: mean 4, sample std 2 -> (-1, 0, 1).
    REQUIRE((*z)[0] == Catch::Approx(-1.0));
    REQUIRE((*z)[1] == Catch::Approx(0.0));
    REQUIRE((*z)[2] == Catch::Approx(1.0));
    // Group B: mean 20, sample std 10 -> (-1, 0, 1).
    REQUIRE((*z)[3] == Catch::Approx(-1.0));
    REQUIRE((*z)[4] == Catch::Approx(0.0));
    REQUIRE((*z)[5] == Catch::Approx(1.0));
}

// --- Filter type coverage: double / string columns ---------------------------
// These exercise template specialisations that the all-int64 tests miss.

TEST_CASE("filter on double column vs float literal", "[filter][types]") {
    // Exercises eval_value_vec<FilterColumn><double> and compare_col_scalar<double, double>.
    runtime::Table table;
    table.add_column("price", Column<double>{1.0, 2.5, 3.0});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 1.5];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& col = std::get<Column<double>>(*result->find("price"));
    REQUIRE(col[0] == Catch::Approx(2.5));
    REQUIRE(col[1] == Catch::Approx(3.0));
}

TEST_CASE("filter int column vs float literal", "[filter][types]") {
    // Exercises cmp_col_scalar_into<int64, double>: int column compared to a float literal.
    runtime::Table table;
    table.add_column("qty", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter qty > 1.5];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    const auto& col = std::get<Column<std::int64_t>>(*result->find("qty"));
    REQUIRE(col[0] == 2);
    REQUIRE(col[1] == 3);
}

TEST_CASE("filter double column vs int literal", "[filter][types]") {
    // Exercises cmp_col_scalar_into<double, int64>: double column compared to an int literal.
    runtime::Table table;
    table.add_column("price", Column<double>{1.5, 2.5, 0.5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter price > 1];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    const auto& col = std::get<Column<double>>(*result->find("price"));
    REQUIRE(col[0] == Catch::Approx(1.5));
    REQUIRE(col[1] == Catch::Approx(2.5));
}

TEST_CASE("filter on string column", "[filter][types]") {
    // Exercises eval_value_vec<FilterColumn><string>.
    runtime::Table table;
    table.add_column("name", Column<std::string>{"Alice", "Bob", "Charlie"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter name > \"B\"];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& col = std::get<Column<std::string>>(*result->find("name"));
    REQUIRE(col[0] == "Bob");
    REQUIRE(col[1] == "Charlie");
}

TEST_CASE("filter by bare boolean column reference keeps true rows", "[filter][types]") {
    // Exercises compute_mask<FilterColumn>'s fallback branch: a bare column
    // reference to a Bool column is a valid predicate (e.g. `filter is_active`,
    // analogous to SQL `WHERE is_active`), not just structurally boolean nodes
    // (compare/logical/is-null).
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.add_column("is_active", Column<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter is_active];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& col = std::get<Column<std::int64_t>>(*result->find("val"));
    REQUIRE(col[0] == 10);
    REQUIRE(col[1] == 30);
}

TEST_CASE("filter on non-boolean column still returns not-a-boolean-expression error",
          "[filter][types]") {
    // Exercises compute_mask<FilterColumn>'s fallback branch: a bare non-Bool
    // column reference is still rejected.
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter val];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("not a boolean expression") != std::string::npos);
}

// --- Mixed-type column arithmetic ---------------------------------------------
// These exercise arith_into<int64,double>, arith_into<double,int64>,
// and arith_into<double,double> template specialisations.

TEST_CASE("update: int column + double column = double", "[update][types]") {
    runtime::Table table;
    table.add_column("a", Column<std::int64_t>{1, 2, 3});
    table.add_column("b", Column<double>{0.5, 1.5, 2.5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = a + b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* c = result->find("c");
    REQUIRE(c != nullptr);
    const auto& col = std::get<Column<double>>(*c);
    REQUIRE(col[0] == Catch::Approx(1.5));
    REQUIRE(col[1] == Catch::Approx(3.5));
    REQUIRE(col[2] == Catch::Approx(5.5));
}

TEST_CASE("update: double column + int column = double", "[update][types]") {
    runtime::Table table;
    table.add_column("a", Column<double>{0.5, 1.5, 2.5});
    table.add_column("b", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = a + b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& col = std::get<Column<double>>(*result->find("c"));
    REQUIRE(col[0] == Catch::Approx(1.5));
    REQUIRE(col[1] == Catch::Approx(3.5));
    REQUIRE(col[2] == Catch::Approx(5.5));
}

TEST_CASE("update: double column * double column = double", "[update][types]") {
    runtime::Table table;
    table.add_column("a", Column<double>{1.0, 2.0, 3.0});
    table.add_column("b", Column<double>{2.0, 3.0, 4.0});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = a * b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& col = std::get<Column<double>>(*result->find("c"));
    REQUIRE(col[0] == Catch::Approx(2.0));
    REQUIRE(col[1] == Catch::Approx(6.0));
    REQUIRE(col[2] == Catch::Approx(12.0));
}

TEST_CASE("update: direct bool column copy preserves values", "[update][bool]") {
    runtime::Table table;
    table.add_column("flag", Column<bool>{true, false, true, true});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { flag_copy = flag }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* copied = std::get_if<Column<bool>>(result->find("flag_copy"));
    REQUIRE(copied != nullptr);
    REQUIRE(copied->size() == 4);
    REQUIRE((*copied)[0] == true);
    REQUIRE((*copied)[1] == false);
    REQUIRE((*copied)[2] == true);
    REQUIRE((*copied)[3] == true);
}

TEST_CASE("update: bare bool literal does not crash", "[update][bool]") {
    // Regression test: a bare Bool literal isn't a ColumnRef (so it misses the
    // column-copy fast path) and isn't Compare/Logical/IsNull (so it misses the
    // vectorized path), so it falls into the generic per-row loop in
    // update_table_window. That loop's Bool branch only checked for an
    // int64_t-holding ExprValue, but eval_expr returns a bool-holding ExprValue
    // for a Bool literal — the mismatch previously tripped an
    // invariant_violation abort instead of a graceful error.
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { flag = true }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = std::get_if<Column<bool>>(result->find("flag"));
    REQUIRE(col != nullptr);
    REQUIRE(col->size() == 3);
    REQUIRE((*col)[0] == true);
    REQUIRE((*col)[1] == true);
    REQUIRE((*col)[2] == true);
}

// --- Matrix Operations --------------------------------------------------------

TEST_CASE("cov: diagonal equals variance", "[cov][matrix]") {
    // x = [1, 2, 3, 4, 5], y = [2, 4, 6, 8, 10]  (y = 2x)
    // var(x) = 2.5, var(y) = 10.0, cov(x,y) = 5.0
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[cov];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);
    REQUIRE(result->find("column") != nullptr);
    REQUIRE(result->find("x") != nullptr);
    REQUIRE(result->find("y") != nullptr);

    const auto& x_col = std::get<Column<double>>(*result->find("x"));
    const auto& y_col = std::get<Column<double>>(*result->find("y"));

    // Row 0 = covariances with x; row 1 = covariances with y
    REQUIRE(x_col[0] == Catch::Approx(2.5));   // var(x)
    REQUIRE(y_col[0] == Catch::Approx(5.0));   // cov(x, y)
    REQUIRE(x_col[1] == Catch::Approx(5.0));   // cov(y, x)
    REQUIRE(y_col[1] == Catch::Approx(10.0));  // var(y)
}

TEST_CASE("cov: drops non-numeric columns silently", "[cov][matrix]") {
    runtime::Table t;
    t.add_column("label", Column<std::string>{"a", "b", "c"});
    t.add_column("v", Column<double>{1.0, 2.0, 3.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[cov];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // Only the "v" column remains; label column is dropped.
    REQUIRE(result->find("v") != nullptr);
    REQUIRE(result->find("label") == nullptr);
    REQUIRE(result->rows() == 1);
}

TEST_CASE("cov: integer columns are widened to double", "[cov][matrix]") {
    runtime::Table t;
    t.add_column("a", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[cov];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->find("a") != nullptr);
    const auto& a_col = std::get<Column<double>>(*result->find("a"));
    REQUIRE(a_col[0] == Catch::Approx(2.5));  // var([1,2,3,4,5])
}

TEST_CASE("corr: diagonal equals 1.0", "[corr][matrix]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});  // perfect correlation
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[corr];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);
    const auto& x_col = std::get<Column<double>>(*result->find("x"));
    const auto& y_col = std::get<Column<double>>(*result->find("y"));

    REQUIRE(x_col[0] == Catch::Approx(1.0));  // corr(x, x) = 1
    REQUIRE(y_col[1] == Catch::Approx(1.0));  // corr(y, y) = 1
    REQUIRE(x_col[1] == Catch::Approx(1.0));  // perfect positive correlation
    REQUIRE(y_col[0] == Catch::Approx(1.0));
}

TEST_CASE("corr: off-diagonal in [-1, 1]", "[corr][matrix]") {
    runtime::Table t;
    t.add_column("a", Column<double>{1.0, 2.0, 3.0, 4.0});
    t.add_column("b", Column<double>{4.0, 3.0, 2.0, 1.0});  // perfect negative correlation
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[corr];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& a_col = std::get<Column<double>>(*result->find("a"));
    const auto& b_col = std::get<Column<double>>(*result->find("b"));
    REQUIRE(a_col[0] == Catch::Approx(1.0));   // corr(a, a)
    REQUIRE(b_col[1] == Catch::Approx(1.0));   // corr(b, b)
    REQUIRE(b_col[0] == Catch::Approx(-1.0));  // perfect negative: corr(a, b)
    REQUIRE(a_col[1] == Catch::Approx(-1.0));  // symmetric
}

TEST_CASE("transpose: basic numeric float64", "[transpose][matrix]") {
    // Input: 3 rows x 2 cols (a, b)
    // After transpose: 2 rows x 3 cols (r0, r1, r2) + leading "column" label
    runtime::Table t;
    t.add_column("a", Column<double>{1.0, 2.0, 3.0});
    t.add_column("b", Column<double>{4.0, 5.0, 6.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[transpose];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);  // 2 original columns -> 2 rows
    // Without a label column: output columns are "column", "r0", "r1", "r2"
    REQUIRE(result->find("column") != nullptr);
    REQUIRE(result->find("r0") != nullptr);
    REQUIRE(result->find("r1") != nullptr);
    REQUIRE(result->find("r2") != nullptr);

    const auto& label = std::get<Column<std::string>>(*result->find("column"));
    REQUIRE(label[0] == "a");
    REQUIRE(label[1] == "b");

    const auto& r0 = std::get<Column<double>>(*result->find("r0"));
    REQUIRE(r0[0] == Catch::Approx(1.0));  // a[0]
    REQUIRE(r0[1] == Catch::Approx(4.0));  // b[0]

    const auto& r1 = std::get<Column<double>>(*result->find("r1"));
    REQUIRE(r1[0] == Catch::Approx(2.0));  // a[1]
    REQUIRE(r1[1] == Catch::Approx(5.0));  // b[1]
}

TEST_CASE("transpose: uses string label column for output column names", "[transpose][matrix]") {
    runtime::Table t;
    t.add_column("symbol", Column<std::string>{"AAPL", "MSFT"});
    t.add_column("open", Column<double>{100.0, 200.0});
    t.add_column("close", Column<double>{110.0, 210.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[transpose];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);  // 2 data columns (open, close)
    REQUIRE(result->find("AAPL") != nullptr);
    REQUIRE(result->find("MSFT") != nullptr);

    const auto& aapl = std::get<Column<double>>(*result->find("AAPL"));
    REQUIRE(aapl[0] == Catch::Approx(100.0));  // open[AAPL]
    REQUIRE(aapl[1] == Catch::Approx(110.0));  // close[AAPL]
}

TEST_CASE("transpose: mixed-type columns returns error", "[transpose][matrix]") {
    runtime::Table t;
    t.add_column("a", Column<double>{1.0, 2.0});
    t.add_column("b", Column<std::int64_t>{3, 4});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[transpose];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("mixed types") != std::string::npos);
}

TEST_CASE("matmul: basic (2x2) * (2x2)", "[matmul][matrix]") {
    // A = [[1, 2], [3, 4]]  ->  columns: c0=[1,3], c1=[2,4]
    // B = [[5, 6], [7, 8]]  ->  columns: c0=[5,7], c1=[6,8]
    // C = A*B = [[1*5+2*7, 1*6+2*8], [3*5+4*7, 3*6+4*8]] = [[19, 22], [43, 50]]
    runtime::Table a, b;
    a.add_column("c0", Column<double>{1.0, 3.0});
    a.add_column("c1", Column<double>{2.0, 4.0});
    b.add_column("c0", Column<double>{5.0, 7.0});
    b.add_column("c1", Column<double>{6.0, 8.0});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("matmul(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);
    REQUIRE(result->find("c0") != nullptr);
    REQUIRE(result->find("c1") != nullptr);

    const auto& out_c0 = std::get<Column<double>>(*result->find("c0"));
    const auto& out_c1 = std::get<Column<double>>(*result->find("c1"));

    REQUIRE(out_c0[0] == Catch::Approx(19.0));
    REQUIRE(out_c0[1] == Catch::Approx(43.0));
    REQUIRE(out_c1[0] == Catch::Approx(22.0));
    REQUIRE(out_c1[1] == Catch::Approx(50.0));
}

TEST_CASE("matmul: identity matrix leaves operand unchanged", "[matmul][matrix]") {
    // A = [[1], [2], [3]]  (3-row, 1-col vector)
    // I = [[1]]             (1x1 identity)
    // Result = [[1], [2], [3]]
    runtime::Table a, identity;
    a.add_column("v", Column<double>{1.0, 2.0, 3.0});
    identity.add_column("v", Column<double>{1.0});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("identity", identity);

    auto ir = require_ir("matmul(a, identity);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 3);
    const auto& v = std::get<Column<double>>(*result->find("v"));
    REQUIRE(v[0] == Catch::Approx(1.0));
    REQUIRE(v[1] == Catch::Approx(2.0));
    REQUIRE(v[2] == Catch::Approx(3.0));
}

TEST_CASE("matmul: inner dimension mismatch returns error", "[matmul][matrix]") {
    runtime::Table a, b;
    a.add_column("c0", Column<double>{1.0, 2.0});
    a.add_column("c1", Column<double>{3.0, 4.0});         // A is 2 rows x 2 cols
    b.add_column("only", Column<double>{5.0, 6.0, 7.0});  // B has 3 rows != 2 cols of A

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("matmul(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("inner dimensions") != std::string::npos);
}

// --- rbind (row-bind) Tests --------------------------------------------------

TEST_CASE("rbind: concatenates matching tables", "[rbind]") {
    runtime::Table a, b;
    a.add_column("x", Column<std::int64_t>{1, 2});
    a.add_column("s", Column<std::string>{"a", "b"});
    b.add_column("x", Column<std::int64_t>{3, 4});
    b.add_column("s", Column<std::string>{"c", "d"});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    const auto& x = std::get<Column<std::int64_t>>(*result->find("x"));
    const auto& s = std::get<Column<std::string>>(*result->find("s"));
    CHECK(x[0] == 1);
    CHECK(x[1] == 2);
    CHECK(x[2] == 3);
    CHECK(x[3] == 4);
    CHECK(s[0] == "a");
    CHECK(s[3] == "d");
}

TEST_CASE("rbind: accepts three or more operands", "[rbind]") {
    runtime::Table a, b, c;
    a.add_column("x", Column<std::int64_t>{1});
    b.add_column("x", Column<std::int64_t>{2});
    c.add_column("x", Column<std::int64_t>{3});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);
    registry.emplace("c", c);

    auto ir = require_ir("rbind(a, b, c);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);
    const auto& x = std::get<Column<std::int64_t>>(*result->find("x"));
    CHECK(x[0] == 1);
    CHECK(x[1] == 2);
    CHECK(x[2] == 3);
}

TEST_CASE("rbind: binds by column name regardless of order", "[rbind]") {
    runtime::Table a, b;
    a.add_column("x", Column<std::int64_t>{1});
    a.add_column("y", Column<std::int64_t>{10});
    // b's columns are in the opposite order but the names/types match.
    b.add_column("y", Column<std::int64_t>{20});
    b.add_column("x", Column<std::int64_t>{2});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    // Output keeps operand 1's column order.
    REQUIRE(result->columns.size() == 2);
    CHECK(result->columns[0].name == "x");
    CHECK(result->columns[1].name == "y");
    const auto& x = std::get<Column<std::int64_t>>(*result->find("x"));
    const auto& y = std::get<Column<std::int64_t>>(*result->find("y"));
    CHECK(x[0] == 1);
    CHECK(x[1] == 2);
    CHECK(y[0] == 10);
    CHECK(y[1] == 20);
}

TEST_CASE("rbind: type mismatch on a shared column is an error", "[rbind]") {
    runtime::Table a, b;
    a.add_column("x", Column<std::int64_t>{1});
    b.add_column("x", Column<double>{1.5});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("Int64") != std::string::npos);
    CHECK(result.error().find("Float64") != std::string::npos);
}

TEST_CASE("rbind: differing column counts is an error", "[rbind]") {
    runtime::Table a, b;
    a.add_column("x", Column<std::int64_t>{1});
    b.add_column("x", Column<std::int64_t>{2});
    b.add_column("y", Column<std::int64_t>{3});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("columns") != std::string::npos);
}

TEST_CASE("rbind: a missing column name is an error", "[rbind]") {
    runtime::Table a, b;
    a.add_column("x", Column<std::int64_t>{1});
    b.add_column("z", Column<std::int64_t>{2});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("missing column 'x'") != std::string::npos);
}

TEST_CASE("rbind: preserves nulls from both operands", "[rbind][null]") {
    runtime::Table a, b;
    a.add_column("v", Column<double>{1.0, 2.0},
                 runtime::ValidityBitmap{std::initializer_list<bool>{true, false}});
    b.add_column("v", Column<double>{3.0, 4.0},
                 runtime::ValidityBitmap{std::initializer_list<bool>{false, true}});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    const auto& entry = result->columns[result->index.at("v")];
    CHECK_FALSE(runtime::is_null(entry, 0));
    CHECK(runtime::is_null(entry, 1));
    CHECK(runtime::is_null(entry, 2));
    CHECK_FALSE(runtime::is_null(entry, 3));
}

TEST_CASE("rbind: remaps categorical dictionaries", "[rbind][categorical]") {
    // Two categorical columns with disjoint-but-overlapping dictionaries.
    Column<Categorical> ca;
    ca.push_back("x");
    ca.push_back("y");
    Column<Categorical> cb;
    cb.push_back("y");
    cb.push_back("z");

    runtime::Table a, b;
    a.add_column("g", std::move(ca));
    b.add_column("g", std::move(cb));

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("rbind(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    const auto& g = std::get<Column<Categorical>>(*result->find("g"));
    CHECK(g[0] == "x");
    CHECK(g[1] == "y");
    CHECK(g[2] == "y");
    CHECK(g[3] == "z");
}

TEST_CASE("rbind: two TimeFrames interleave by time index and stay a TimeFrame",
          "[rbind][timeframe]") {
    runtime::Table a, b;
    a.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(300)});
    a.add_column("val", Column<std::int64_t>{1, 3});
    b.add_column("ts", Column<Timestamp>{ts_from_nanos(200), ts_from_nanos(400)});
    b.add_column("val", Column<std::int64_t>{2, 4});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir(R"(rbind(as_timeframe(a, "ts"), as_timeframe(b, "ts"));)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    // Result remains a TimeFrame indexed on "ts".
    REQUIRE(result->time_index.has_value());
    CHECK(*result->time_index == "ts");

    // Rows from both operands interleave into ascending time order.
    const auto& ts = std::get<Column<Timestamp>>(*result->find("ts"));
    const auto& val = std::get<Column<std::int64_t>>(*result->find("val"));
    CHECK(ts[0].nanos == 100);
    CHECK(ts[1].nanos == 200);
    CHECK(ts[2].nanos == 300);
    CHECK(ts[3].nanos == 400);
    CHECK(val[0] == 1);
    CHECK(val[1] == 2);
    CHECK(val[2] == 3);
    CHECK(val[3] == 4);
}

TEST_CASE("rbind: merging TimeFrames is stable on equal timestamps", "[rbind][timeframe]") {
    // Both operands carry a row at t=200; the merge must keep operand 1's row
    // (val 99) before operand 2's (val 50) — a stable k-way merge.
    runtime::Table a, b;
    a.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    a.add_column("val", Column<std::int64_t>{1, 99});
    b.add_column("ts", Column<Timestamp>{ts_from_nanos(200), ts_from_nanos(300)});
    b.add_column("val", Column<std::int64_t>{50, 3});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir(R"(rbind(as_timeframe(a, "ts"), as_timeframe(b, "ts"));)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    const auto& ts = std::get<Column<Timestamp>>(*result->find("ts"));
    const auto& val = std::get<Column<std::int64_t>>(*result->find("val"));
    CHECK(ts[0].nanos == 100);
    CHECK(ts[1].nanos == 200);
    CHECK(ts[2].nanos == 200);
    CHECK(ts[3].nanos == 300);
    CHECK(val[0] == 1);
    CHECK(val[1] == 99);  // operand 1's t=200 row first
    CHECK(val[2] == 50);  // then operand 2's t=200 row
    CHECK(val[3] == 3);
}

TEST_CASE("rbind: a TimeFrame with a plain DataFrame yields a plain DataFrame",
          "[rbind][timeframe]") {
    runtime::Table a, b;
    a.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(300)});
    a.add_column("val", Column<std::int64_t>{1, 3});
    b.add_column("ts", Column<Timestamp>{ts_from_nanos(200)});
    b.add_column("val", Column<std::int64_t>{2});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    // Only the first operand is a TimeFrame; the result drops the index and
    // preserves operand-append order rather than re-sorting.
    auto ir = require_ir(R"(rbind(as_timeframe(a, "ts"), b);)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);
    CHECK_FALSE(result->time_index.has_value());

    const auto& ts = std::get<Column<Timestamp>>(*result->find("ts"));
    CHECK(ts[0].nanos == 100);
    CHECK(ts[1].nanos == 300);
    CHECK(ts[2].nanos == 200);
}

// --- Model Specification Tests -----------------------------------------------

TEST_CASE("model: OLS simple regression", "[model]") {
    // y = 2*x + 1 (perfect linear relationship)
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());

    // Coefficients table: (intercept) and x
    REQUIRE(result->rows() == 2);
    REQUIRE(result->find("term") != nullptr);
    REQUIRE(result->find("estimate") != nullptr);

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));

    // With intercept: y = 1 + 2*x
    REQUIRE(std::string(terms[0]) == "(intercept)");
    REQUIRE(std::string(terms[1]) == "x");
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));

    // ModelResult should have R^2 = 1.0 for perfect fit
    REQUIRE(model_out.r_squared == Catch::Approx(1.0));
    REQUIRE(model_out.n_obs == 5);
    REQUIRE(model_out.n_params == 2);
}

TEST_CASE("model: omitted method defaults to ols", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));
    REQUIRE(model_out.method == "ols");
}

TEST_CASE("model: OLS multiple regression", "[model]") {
    // y = 1 + 2*x1 + 3*x2
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("x2", Column<double>{0.0, 1.0, 0.0, 1.0, 0.0});
    t.add_column("y", Column<double>{3.0, 8.0, 7.0, 12.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);  // intercept + x1 + x2

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));  // intercept
    REQUIRE(estimates[1] == Catch::Approx(2.0));  // x1
    REQUIRE(estimates[2] == Catch::Approx(3.0));  // x2
}

TEST_CASE("model_predict: built-in OLS applies coefficients", "[model][predict]") {
    // y = 2 + 3*x exactly.
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0});
    t.add_column("y", Column<double>{5.0, 8.0, 11.0, 14.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto fit = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(fit.has_value());

    // Score fresh rows: prediction = 2 + 3*x.
    runtime::Table newdata;
    newdata.add_column("x", Column<double>{10.0, 20.0});

    runtime::ExternRegistry externs;
    auto pred = runtime::predict_model(model_out, newdata, externs);
    REQUIRE(pred.has_value());

    const auto* col = pred->find("prediction");
    REQUIRE(col != nullptr);
    const auto& p = std::get<Column<double>>(*col);
    REQUIRE(p.size() == 2);
    CHECK(p[0] == Catch::Approx(32.0));  // 2 + 3*10
    CHECK(p[1] == Catch::Approx(62.0));  // 2 + 3*20
}

TEST_CASE("model_predict: built-in OLS multiple predictors", "[model][predict]") {
    // y = 1 + 2*x1 - 0.5*x2 exactly.
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("x2", Column<double>{2.0, 0.0, 4.0, 1.0, 3.0});
    t.add_column("y", Column<double>{2.0, 5.0, 5.0, 8.5, 9.5});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2, method = ols }];");
    runtime::ModelResult model_out;
    auto fit = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(fit.has_value());

    runtime::Table newdata;
    newdata.add_column("x1", Column<double>{10.0});
    newdata.add_column("x2", Column<double>{6.0});

    runtime::ExternRegistry externs;
    auto pred = runtime::predict_model(model_out, newdata, externs);
    REQUIRE(pred.has_value());

    const auto& p = std::get<Column<double>>(*pred->find("prediction"));
    REQUIRE(p.size() == 1);
    CHECK(p[0] == Catch::Approx(18.0));  // 1 + 2*10 - 0.5*6
}

TEST_CASE("model: OLS no intercept", "[model]") {
    // y = 2*x (through origin)
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x - 1, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);  // only x, no intercept

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(std::string(terms[0]) == "x");
    REQUIRE(estimates[0] == Catch::Approx(2.0));
}

TEST_CASE("model: OLS with filter", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0, 100.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0, 999.0});
    t.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 1000});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    // Filter out the outlier before fitting
    auto ir = require_ir("t[filter id < 1000, model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));  // intercept
    REQUIRE(estimates[1] == Catch::Approx(2.0));  // x
    REQUIRE(model_out.n_obs == 5);
}

TEST_CASE("model: OLS dot notation (all predictors)", "[model]") {
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("x2", Column<double>{5.0, 4.0, 3.0, 2.0, 1.0});
    t.add_column("y", Column<double>{6.0, 6.0, 6.0, 6.0, 6.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ ., method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);  // intercept + x1 + x2
}

TEST_CASE("model: OLS interaction term", "[model]") {
    // y = 1 + 2*x1 + 3*x2 + 4*x1*x2, with well-spread data
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0});
    t.add_column("x2", Column<double>{1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0});
    // y = 1 + 2*x1 + 3*x2 + 4*x1*x2
    t.add_column("y", Column<double>{10.0, 16.0, 22.0, 17.0, 27.0, 37.0, 24.0, 38.0, 52.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2 + x1:x2, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);  // intercept + x1 + x2 + x1:x2

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    REQUIRE(std::string(terms[3]) == "x1:x2");

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0).margin(0.01));  // intercept
    REQUIRE(estimates[1] == Catch::Approx(2.0).margin(0.01));  // x1
    REQUIRE(estimates[2] == Catch::Approx(3.0).margin(0.01));  // x2
    REQUIRE(estimates[3] == Catch::Approx(4.0).margin(0.01));  // x1:x2
}

TEST_CASE("model: OLS crossing operator (*)", "[model]") {
    // y ~ x1 * x2 should expand to y ~ x1 + x2 + x1:x2
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0});
    t.add_column("x2", Column<double>{1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0});
    t.add_column("y", Column<double>{10.0, 16.0, 22.0, 17.0, 27.0, 37.0, 24.0, 38.0, 52.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 * x2, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);  // intercept + x1 + x2 + x1:x2
}

TEST_CASE("model: ridge regression", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ridge, lambda = 0.1 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    // Ridge should shrink coefficients slightly toward zero compared to OLS
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    // With small lambda, should be close to OLS (intercept ~ 1, slope ~ 2)
    REQUIRE(estimates[1] == Catch::Approx(2.0).margin(0.2));
}

TEST_CASE("model: ridge regression accepts scalar binding parameter", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    runtime::ScalarRegistry scalars;
    scalars.emplace("lambda", runtime::ScalarValue{0.1});

    auto ir = require_ir("t[model { y ~ x, method = ridge, lambda = lambda }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[1] == Catch::Approx(2.0).margin(0.2));
}

TEST_CASE("model: WLS with weights", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});
    t.add_column("w", Column<double>{1.0, 1.0, 1.0, 1.0, 1.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = wls, weights = w }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // With equal weights, WLS = OLS
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));
}

namespace {
// Shared mock of a model plugin registered via register_model. fit returns a
// perfect in-sample fit plus a sentinel native handle; predict applies y = 2x+1
// and asserts the handle round-tripped.
auto register_mock_model(runtime::ExternRegistry& externs) -> void {
    externs.register_model(
        "lightgbm",
        runtime::ModelOps{
            .fit = [](const runtime::Table& design, const std::string& response_col,
                      const runtime::ModelParams& params)
                -> std::expected<runtime::FittedModel, std::string> {
                auto find_param = [&](const std::string& key) -> const runtime::ScalarValue* {
                    for (const auto& [k, v] : params) {
                        if (k == key) {
                            return &v;
                        }
                    }
                    return nullptr;
                };
                REQUIRE(response_col == "__response");
                REQUIRE(find_param("iterations") != nullptr);
                REQUIRE(std::get<std::int64_t>(*find_param("iterations")) == 250);
                REQUIRE(find_param("learning_rate") != nullptr);
                REQUIRE(std::get<double>(*find_param("learning_rate")) == Catch::Approx(0.04));

                const auto* x = std::get_if<Column<double>>(design.find("x"));
                const auto* y = std::get_if<Column<double>>(design.find("__response"));
                REQUIRE(x != nullptr);
                REQUIRE(y != nullptr);
                REQUIRE(x->size() == y->size());

                runtime::Table fitted;
                fitted.add_column("fitted", *y);  // perfect in-sample fit
                runtime::Table importance;
                importance.add_column("term", Column<std::string>{"(intercept)", "x"});
                importance.add_column("gain", Column<double>{0.0, 42.0});
                runtime::Table summary;
                summary.add_column("note", Column<std::string>{"mock"});
                return runtime::FittedModel{
                    .native = std::make_shared<int>(7),  // sentinel handle
                    .fitted = std::move(fitted),
                    .importance = std::move(importance),
                    .summary = std::move(summary),
                };
            },
            .predict = [](const void* native, const runtime::Table& design)
                -> std::expected<runtime::Table, std::string> {
                REQUIRE(native != nullptr);
                REQUIRE(*static_cast<const int*>(native) == 7);
                const auto* x = std::get_if<Column<double>>(design.find("x"));
                REQUIRE(x != nullptr);
                Column<double> preds;
                for (double v : *x) {
                    preds.push_back((2.0 * v) + 1.0);
                }
                runtime::Table out;
                out.add_column("prediction", std::move(preds));
                return out;
            },
        });
}
}  // namespace

TEST_CASE("model: LightGBM plugin fit, accessors, and predict", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    runtime::ExternRegistry externs;
    register_mock_model(externs);

    auto ir = require_ir(
        "t[model { y ~ x, method = lightgbm, iterations = 250, learning_rate = 0.04 }];");
    runtime::ModelResult model;
    auto result = runtime::interpret(*ir, registry, nullptr, &externs, &model);
    REQUIRE(result.has_value());

    // A tree model has no coefficients; the primary table is feature importance.
    const auto& gain = std::get<Column<double>>(*result->find("gain"));
    REQUIRE(gain[1] == Catch::Approx(42.0));

    // ModelResult carries predictions, R², and the native handle.
    const auto& fitted = std::get<Column<double>>(*model.fitted_values.find("fitted"));
    REQUIRE(fitted[0] == Catch::Approx(3.0));
    REQUIRE(model.r_squared == Catch::Approx(1.0));  // perfect in-sample fit
    REQUIRE(model.native != nullptr);
    REQUIRE(model.summary.find("note") != nullptr);  // plugin summary slot flows through

    // model_predict reuses the live native handle on new data.
    runtime::Table nd;
    nd.add_column("x", Column<double>{10.0, 20.0});
    auto preds = runtime::predict_model(model, nd, externs);
    REQUIRE(preds.has_value());
    const auto& pcol = std::get<Column<double>>(*preds->find("prediction"));
    REQUIRE(pcol[0] == Catch::Approx(21.0));
    REQUIRE(pcol[1] == Catch::Approx(41.0));
}

TEST_CASE("model: LightGBM plugin method accepts scalar bindings", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    runtime::ScalarRegistry scalars;
    scalars.emplace("iterations", runtime::ScalarValue{std::int64_t{250}});
    scalars.emplace("learning_rate", runtime::ScalarValue{0.04});

    runtime::ExternRegistry externs;
    register_mock_model(externs);

    auto ir = require_ir(
        "t[model { y ~ x, method = lightgbm, iterations = iterations, learning_rate = "
        "learning_rate "
        "}];");
    auto result = runtime::interpret(*ir, registry, &scalars, &externs);
    REQUIRE(result.has_value());

    const auto& gain = std::get<Column<double>>(*result->find("gain"));
    REQUIRE(gain[1] == Catch::Approx(42.0));
}

TEST_CASE("model: LightGBM method requires plugin import/registration", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = lightgbm }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("plugin") != std::string::npos);
}

TEST_CASE("model: ModelResult accessor tables", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());

    // Summary table has std_error, t_stat, p_value columns
    REQUIRE(model_out.summary.find("std_error") != nullptr);
    REQUIRE(model_out.summary.find("t_stat") != nullptr);
    REQUIRE(model_out.summary.find("p_value") != nullptr);

    // Fitted values
    REQUIRE(model_out.fitted_values.rows() == 5);
    const auto& fitted = std::get<Column<double>>(*model_out.fitted_values.find("fitted"));
    REQUIRE(fitted[0] == Catch::Approx(3.0));   // 1 + 2*1
    REQUIRE(fitted[4] == Catch::Approx(11.0));  // 1 + 2*5

    // Residuals (should be ~0 for perfect fit)
    REQUIRE(model_out.residuals.rows() == 5);
    const auto& resid = std::get<Column<double>>(*model_out.residuals.find("residual"));
    REQUIRE(resid[0] == Catch::Approx(0.0).margin(1e-10));
    REQUIRE(resid[4] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("model: integer columns widened", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    t.add_column("y", Column<std::int64_t>{3, 5, 7, 9, 11});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));
}

TEST_CASE("model: dummy encoding for string columns", "[model]") {
    runtime::Table t;
    t.add_column("region", Column<std::string>{"East", "West", "North", "East", "West", "North"});
    t.add_column("y", Column<double>{10.0, 20.0, 30.0, 12.0, 22.0, 32.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ region, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // With intercept: reference level (East) absorbed -> intercept + region_West + region_North
    REQUIRE(result->rows() == 3);

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    REQUIRE(std::string(terms[0]) == "(intercept)");
    REQUIRE(std::string(terms[1]) == "region_West");
    REQUIRE(std::string(terms[2]) == "region_North");
}

TEST_CASE("model: error on missing predictor column", "[model]") {
    runtime::Table t;
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("column not found") != std::string::npos);
}

TEST_CASE("model: error on unknown method", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = unknown_method }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("unknown method") != std::string::npos);
}

TEST_CASE("model: error on missing response column", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("response column not found") != std::string::npos);
}

TEST_CASE("model: error on non-numeric response column", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<std::string>{"a", "b", "c", "d", "e"});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("must be numeric") != std::string::npos);
}

TEST_CASE("model: interaction term rejects non-numeric columns", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("region", Column<std::string>{"East", "West", "North", "East", "West"});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x:region, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("interaction term requires numeric columns") != std::string::npos);
}

TEST_CASE("model: error when observations do not exceed parameters", "[model]") {
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0});
    t.add_column("x2", Column<double>{4.0, 5.0, 6.0});
    t.add_column("x3", Column<double>{7.0, 8.0, 9.0});
    t.add_column("y", Column<double>{1.0, 2.0, 3.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2 + x3, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("need more observations") != std::string::npos);
}

TEST_CASE("model: error on rank-deficient design matrix", "[model]") {
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("x2", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("rank-deficient") != std::string::npos);
}

TEST_CASE("model: WLS requires weights parameter", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = wls }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires weights parameter") != std::string::npos);
}

TEST_CASE("model: WLS errors on missing weights column", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = wls, weights = w }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("wls:") != std::string::npos);
    REQUIRE(result.error().find("not found") != std::string::npos);
}

// ── Sorted streaming aggregate (ChunkedSortedAggregateOperator) ─────────────
//
// When the aggregate's input arrives sorted on the group-by keys, the
// interpreter streams group-at-a-time and emits groups in key order. These
// cover correctness across agg funcs/types, multi-key, cross-chunk group
// spanning, nulls, the unsorted fallback, and the advertised output ordering.

TEST_CASE("Sorted aggregate streams groups in key order with correct values") {
    runtime::Table t;
    t.add_column("sym", Column<std::string>{"A", "A", "B", "B", "B", "C"});
    t.add_column("v", Column<std::int64_t>{1, 3, 2, 4, 6, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[order sym asc][by sym, select { sym, s = sum(v), n = count(), mn = min(v), "
        "mx = max(v), av = mean(v) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* sym = std::get_if<Column<std::string>>(result->find("sym"));
    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    const auto* n = std::get_if<Column<std::int64_t>>(result->find("n"));
    const auto* mn = std::get_if<Column<std::int64_t>>(result->find("mn"));
    const auto* mx = std::get_if<Column<std::int64_t>>(result->find("mx"));
    const auto* av = std::get_if<Column<double>>(result->find("av"));
    REQUIRE(sym != nullptr);
    REQUIRE(s != nullptr);
    REQUIRE(n != nullptr);
    REQUIRE(mn != nullptr);
    REQUIRE(mx != nullptr);
    REQUIRE(av != nullptr);

    REQUIRE((*sym)[0] == "A");
    REQUIRE((*s)[0] == 4);
    REQUIRE((*n)[0] == 2);
    REQUIRE((*mn)[0] == 1);
    REQUIRE((*mx)[0] == 3);
    REQUIRE((*av)[0] == Catch::Approx(2.0));

    REQUIRE((*sym)[1] == "B");
    REQUIRE((*s)[1] == 12);
    REQUIRE((*n)[1] == 3);
    REQUIRE((*mn)[1] == 2);
    REQUIRE((*mx)[1] == 6);
    REQUIRE((*av)[1] == Catch::Approx(4.0));

    REQUIRE((*sym)[2] == "C");
    REQUIRE((*s)[2] == 5);
    REQUIRE((*n)[2] == 1);
    REQUIRE((*mn)[2] == 5);
    REQUIRE((*mx)[2] == 5);
    REQUIRE((*av)[2] == Catch::Approx(5.0));

    // The streamed output is sorted by the group key, and says so.
    REQUIRE(result->ordering.has_value());
    REQUIRE(result->ordering->size() == 1);
    REQUIRE((*result->ordering)[0].name == "sym");
}

TEST_CASE("Sorted aggregate matches hash aggregate on the same data") {
    // Same rows, two queries: one sorts first (streaming sorted path), one does
    // not (hash path, first-seen order). After sorting the hash result by key,
    // the two must be identical — a broad guard on the streaming accumulators.
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{3, 1, 2, 1, 3, 2, 1, 2, 3, 2});
    t.add_column("v", Column<double>{1.5, 2.0, 3.0, 4.0, 5.5, 6.0, 7.0, 8.0, 9.0, 10.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto streamed = runtime::interpret(
        *require_ir("t[order k asc][by k, select { k, s = sum(v), n = count(), mn = min(v), "
                    "mx = max(v), av = mean(v) }];"),
        registry);
    auto hashed =
        runtime::interpret(*require_ir("t[by k, select { k, s = sum(v), n = count(), mn = min(v), "
                                       "mx = max(v), av = mean(v) }][order k asc];"),
                           registry);
    REQUIRE(streamed.has_value());
    REQUIRE(hashed.has_value());
    REQUIRE(streamed->rows() == 3);
    REQUIRE(hashed->rows() == 3);

    for (const char* name : {"s", "mn", "mx", "av"}) {
        const auto* a = std::get_if<Column<double>>(streamed->find(name));
        const auto* b = std::get_if<Column<double>>(hashed->find(name));
        REQUIRE(a != nullptr);
        REQUIRE(b != nullptr);
        for (std::size_t i = 0; i < a->size(); ++i) {
            REQUIRE((*a)[i] == Catch::Approx((*b)[i]));
        }
    }
    const auto* ka = std::get_if<Column<std::int64_t>>(streamed->find("k"));
    const auto* na = std::get_if<Column<std::int64_t>>(streamed->find("n"));
    REQUIRE(ka != nullptr);
    REQUIRE(na != nullptr);
    REQUIRE((*ka)[0] == 1);
    REQUIRE((*ka)[1] == 2);
    REQUIRE((*ka)[2] == 3);
    REQUIRE((*na)[0] == 3);  // k=1 appears 3×
    REQUIRE((*na)[1] == 4);  // k=2 appears 4×
    REQUIRE((*na)[2] == 3);  // k=3 appears 3×
}

TEST_CASE("Sorted aggregate merges a group spanning a chunk boundary") {
    // Chunked extern source: group "B" straddles the chunk boundary, so the
    // streaming operator must carry the open group across chunks.
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("sorted_src", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        auto c0 = make_str_int_chunk("sym", {"A", "A", "B"}, "v", {1, 2, 10});
        auto c1 = make_str_int_chunk("sym", {"B", "B", "C"}, "v", {20, 30, 4});
        const std::vector<ir::OrderKey> ord{ir::OrderKey{.name = "sym", .ascending = true}};
        c0.ordering = ord;
        c1.ordering = ord;
        chunks.push_back(std::move(c0));
        chunks.push_back(std::move(c1));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn sorted_src() -> DataFrame from \"x.hpp\"; "
        "sorted_src()[by sym, select { sym, s = sum(v), n = count() }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* sym = std::get_if<Column<std::string>>(result->find("sym"));
    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    const auto* n = std::get_if<Column<std::int64_t>>(result->find("n"));
    REQUIRE(sym != nullptr);
    REQUIRE(s != nullptr);
    REQUIRE(n != nullptr);
    REQUIRE((*sym)[0] == "A");
    REQUIRE((*s)[0] == 3);
    REQUIRE((*n)[0] == 2);
    REQUIRE((*sym)[1] == "B");
    REQUIRE((*s)[1] == 60);  // 10 + 20 + 30, spanning the boundary
    REQUIRE((*n)[1] == 3);
    REQUIRE((*sym)[2] == "C");
    REQUIRE((*s)[2] == 4);
    REQUIRE((*n)[2] == 1);
}

TEST_CASE("Sorted aggregate supports multi-key group prefixes") {
    // Sorted by (a, b); grouping by {a, b} is a covering prefix.
    runtime::Table t;
    t.add_column("a", Column<std::int64_t>{1, 1, 1, 2, 2});
    t.add_column("b", Column<std::int64_t>{1, 1, 2, 1, 1});
    t.add_column("v", Column<std::int64_t>{10, 20, 30, 40, 50});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[order { a asc, b asc }][by { a, b }, select { a, b, s = sum(v) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* a = std::get_if<Column<std::int64_t>>(result->find("a"));
    const auto* b = std::get_if<Column<std::int64_t>>(result->find("b"));
    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE(s != nullptr);
    REQUIRE((*a)[0] == 1);
    REQUIRE((*b)[0] == 1);
    REQUIRE((*s)[0] == 30);  // (1,1): 10 + 20
    REQUIRE((*a)[1] == 1);
    REQUIRE((*b)[1] == 2);
    REQUIRE((*s)[1] == 30);  // (1,2): 30
    REQUIRE((*a)[2] == 2);
    REQUIRE((*b)[2] == 1);
    REQUIRE((*s)[2] == 90);  // (2,1): 40 + 50
}

TEST_CASE("Sorted aggregate skips nulls and emits null for an all-null group") {
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 2, 2});
    Column<std::int64_t> v{0, 5, 0, 0};
    runtime::ValidityBitmap valid;
    valid.push_back(false);  // k=1, null
    valid.push_back(true);   // k=1, 5
    valid.push_back(false);  // k=2, null
    valid.push_back(false);  // k=2, null
    t.add_column("v", std::move(v), std::move(valid));

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[order k asc][by k, select { k, s = sum(v), mn = min(v) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    REQUIRE(s != nullptr);
    REQUIRE((*s)[0] == 5);  // k=1: only the non-null 5 contributes

    // k=2 is all-null → result is null.
    const auto* s_entry = result->find_entry("s");
    REQUIRE(s_entry != nullptr);
    REQUIRE(s_entry->validity.has_value());
    REQUIRE_FALSE((*s_entry->validity)[1]);
}

TEST_CASE("Aggregate over unsorted input falls back to the hash path") {
    // No `order`, so the input is not group-contiguous: the sorted operator
    // must transparently fall back and still aggregate correctly.
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{2, 1, 2, 1, 3});
    t.add_column("v", Column<std::int64_t>{10, 20, 30, 40, 50});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[by k, select { k, s = sum(v), n = count() }][order k asc];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* k = std::get_if<Column<std::int64_t>>(result->find("k"));
    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    const auto* n = std::get_if<Column<std::int64_t>>(result->find("n"));
    REQUIRE(k != nullptr);
    REQUIRE(s != nullptr);
    REQUIRE(n != nullptr);
    REQUIRE((*k)[0] == 1);
    REQUIRE((*s)[0] == 60);  // 20 + 40
    REQUIRE((*n)[0] == 2);
    REQUIRE((*k)[1] == 2);
    REQUIRE((*s)[1] == 40);  // 10 + 30
    REQUIRE((*n)[1] == 2);
    REQUIRE((*k)[2] == 3);
    REQUIRE((*s)[2] == 50);
    REQUIRE((*n)[2] == 1);
}

TEST_CASE("Sorted aggregate handles empty input") {
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{});
    t.add_column("v", Column<std::int64_t>{});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[order k asc][by k, select { k, s = sum(v) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

// ── Widened streaming aggregates: Stddev / Skew / Kurtosis ──────────────────
//
// These stream via online central moments (Welford/Pébay). The strongest check
// is parity with the materializing path, whose skew/kurtosis use independent
// two-pass code: adding a non-streamable agg (median) to the query forces the
// whole aggregate to materialize, so comparing the two isolates the streaming
// moment math.

TEST_CASE("Streaming stddev/skew/kurtosis match hand-computed values") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 1, 1, 1});
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[order g asc][by g, select { g, sd = std(x), sk = skew(x), ku = kurtosis(x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* sd = std::get_if<Column<double>>(result->find("sd"));
    const auto* sk = std::get_if<Column<double>>(result->find("sk"));
    const auto* ku = std::get_if<Column<double>>(result->find("ku"));
    REQUIRE(sd != nullptr);
    REQUIRE(sk != nullptr);
    REQUIRE(ku != nullptr);
    REQUIRE((*sd)[0] == Catch::Approx(1.5811388301));  // sqrt(2.5)
    REQUIRE((*sk)[0] == Catch::Approx(0.0));           // symmetric
    REQUIRE((*ku)[0] == Catch::Approx(-1.2));          // matches scipy/pandas default
}

TEST_CASE("Streaming moments match the materializing path (parity)") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 1, 1, 1, 2, 2, 2, 2});
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 1.0, 2.0, 10.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    // Streamed: all aggs are streamable, input sorted on g.
    auto streamed = runtime::interpret(
        *require_ir("t[order g asc][by g, select { g, sd = std(x), sk = skew(x), "
                    "ku = kurtosis(x) }];"),
        registry);
    // Materialized: median is not streamable, forcing the whole aggregate onto
    // the materializing path (which computes skew/kurtosis via two passes).
    auto materialized = runtime::interpret(
        *require_ir("t[by g, select { g, sd = std(x), sk = skew(x), ku = kurtosis(x), "
                    "md = median(x) }][order g asc];"),
        registry);
    REQUIRE(streamed.has_value());
    REQUIRE(materialized.has_value());
    REQUIRE(streamed->rows() == 2);
    REQUIRE(materialized->rows() == 2);

    for (const char* name : {"sd", "sk", "ku"}) {
        const auto* a = std::get_if<Column<double>>(streamed->find(name));
        const auto* b = std::get_if<Column<double>>(materialized->find(name));
        REQUIRE(a != nullptr);
        REQUIRE(b != nullptr);
        for (std::size_t i = 0; i < a->size(); ++i) {
            REQUIRE((*a)[i] == Catch::Approx((*b)[i]));
        }
    }
}

TEST_CASE("Streaming moments also work on the unsorted hash path") {
    // No `order` → input has no ordering → the hash ChunkedAggregateOperator
    // computes the moments. Same expected values as the sorted path.
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{2, 1, 2, 1, 1, 2});
    t.add_column("x", Column<double>{10.0, 1.0, 20.0, 2.0, 3.0, 30.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[by g, select { g, sd = std(x) }][order g asc];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* g = std::get_if<Column<std::int64_t>>(result->find("g"));
    const auto* sd = std::get_if<Column<double>>(result->find("sd"));
    REQUIRE(g != nullptr);
    REQUIRE(sd != nullptr);
    REQUIRE((*g)[0] == 1);
    REQUIRE((*sd)[0] == Catch::Approx(1.0));  // std([1,2,3]) = 1
    REQUIRE((*g)[1] == 2);
    REQUIRE((*sd)[1] == Catch::Approx(10.0));  // std([10,20,30]) = 10
}

TEST_CASE("Streaming moments emit null below the per-function row threshold") {
    // Group sizes 1/2/3/4 exercise the null edges: stddev needs ≥2, skew ≥3,
    // kurtosis ≥4 valid observations.
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 2, 2, 3, 3, 3, 4, 4, 4, 4});
    t.add_column("x", Column<double>{5.0, 1.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[order g asc][by g, select { g, sd = std(x), sk = skew(x), ku = kurtosis(x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    const auto* sd = result->find_entry("sd");
    const auto* sk = result->find_entry("sk");
    const auto* ku = result->find_entry("ku");
    REQUIRE(sd != nullptr);
    REQUIRE(sk != nullptr);
    REQUIRE(ku != nullptr);
    REQUIRE(sd->validity.has_value());
    REQUIRE(sk->validity.has_value());
    REQUIRE(ku->validity.has_value());

    // Row order is g = 1,2,3,4.
    REQUIRE_FALSE((*sd->validity)[0]);  // n=1: stddev null
    REQUIRE((*sd->validity)[1]);        // n=2: stddev valid
    REQUIRE((*sd->validity)[2]);        // n=3
    REQUIRE((*sd->validity)[3]);        // n=4

    REQUIRE_FALSE((*sk->validity)[0]);  // n=1: skew null
    REQUIRE_FALSE((*sk->validity)[1]);  // n=2: skew null
    REQUIRE((*sk->validity)[2]);        // n=3: skew valid
    REQUIRE((*sk->validity)[3]);        // n=4

    REQUIRE_FALSE((*ku->validity)[0]);  // n=1: kurtosis null
    REQUIRE_FALSE((*ku->validity)[1]);  // n=2
    REQUIRE_FALSE((*ku->validity)[2]);  // n=3: kurtosis null
    REQUIRE((*ku->validity)[3]);        // n=4: kurtosis valid
}

TEST_CASE("Streaming stddev skips nulls") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 1, 1});
    Column<double> x{1.0, 0.0, 2.0, 3.0};
    runtime::ValidityBitmap valid;
    valid.push_back(true);
    valid.push_back(false);  // null — must be skipped
    valid.push_back(true);
    valid.push_back(true);
    t.add_column("x", std::move(x), std::move(valid));
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[order g asc][by g, select { g, sd = std(x) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* sd = std::get_if<Column<double>>(result->find("sd"));
    REQUIRE(sd != nullptr);
    REQUIRE((*sd)[0] == Catch::Approx(1.0));  // std([1,2,3]) = 1, null ignored
}

TEST_CASE("Streaming stddev accumulates moments across chunk boundaries") {
    // One group whose rows span two chunks; the running mean/M2 must carry over.
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    auto make_g_x_chunk = [](std::vector<std::int64_t> gs,
                             std::vector<std::int64_t> xs) -> runtime::Chunk {
        runtime::Chunk chunk;
        runtime::ColumnEntry ge;
        ge.name = "g";
        ge.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
        auto& gc = std::get<Column<std::int64_t>>(*ge.column);
        for (auto v : gs) {
            gc.push_back(v);
        }
        chunk.columns.push_back(std::move(ge));
        runtime::ColumnEntry xe;
        xe.name = "x";
        xe.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
        auto& xc = std::get<Column<std::int64_t>>(*xe.column);
        for (auto v : xs) {
            xc.push_back(v);
        }
        chunk.columns.push_back(std::move(xe));
        chunk.ordering = std::vector<ir::OrderKey>{ir::OrderKey{.name = "g", .ascending = true}};
        return chunk;
    };

    externs.register_chunked_table("moments_src", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_g_x_chunk({1, 1, 1}, {1, 2, 3}));
        chunks.push_back(make_g_x_chunk({1, 1}, {4, 5}));  // group 1 continues
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn moments_src() -> DataFrame from \"x.hpp\"; "
        "moments_src()[by g, select { g, sd = std(x) }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* sd = std::get_if<Column<double>>(result->find("sd"));
    REQUIRE(sd != nullptr);
    REQUIRE((*sd)[0] == Catch::Approx(1.5811388301));  // std([1,2,3,4,5]) = sqrt(2.5)
}

// ── Widened streaming aggregates: First / Last ───────────────────────────────
//
// Numeric First/Last stream natively in both ChunkedSortedAggregateOperator
// (group-at-a-time, via accumulate_typed's int_value/double_value slots) and
// ChunkedAggregateOperator (hash, same slots). String/Categorical First/Last
// only stream on the hash operator (via AggSlot::first_value/last_value); the
// sorted operator detects that case and falls back to the hash operator
// instead of erroring, since it has no group-at-a-time string accumulator.

TEST_CASE("Sorted aggregate streams first/last on numeric input") {
    runtime::Table t;
    t.add_column("sym", Column<std::string>{"A", "A", "A", "B", "B", "C"});
    t.add_column("v", Column<std::int64_t>{10, 20, 30, 5, 6, 100});
    t.add_column("d", Column<double>{1.5, 2.5, 3.5, 9.0, 8.0, 4.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir(
        "t[order sym asc][by sym, select { sym, fi = first(v), la = last(v), fd = first(d), "
        "ld = last(d) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* sym = std::get_if<Column<std::string>>(result->find("sym"));
    const auto* fi = std::get_if<Column<std::int64_t>>(result->find("fi"));
    const auto* la = std::get_if<Column<std::int64_t>>(result->find("la"));
    const auto* fd = std::get_if<Column<double>>(result->find("fd"));
    const auto* ld = std::get_if<Column<double>>(result->find("ld"));
    REQUIRE(sym != nullptr);
    REQUIRE(fi != nullptr);
    REQUIRE(la != nullptr);
    REQUIRE(fd != nullptr);
    REQUIRE(ld != nullptr);

    REQUIRE((*sym)[0] == "A");
    REQUIRE((*fi)[0] == 10);
    REQUIRE((*la)[0] == 30);
    REQUIRE((*fd)[0] == Catch::Approx(1.5));
    REQUIRE((*ld)[0] == Catch::Approx(3.5));

    REQUIRE((*sym)[1] == "B");
    REQUIRE((*fi)[1] == 5);
    REQUIRE((*la)[1] == 6);
    REQUIRE((*fd)[1] == Catch::Approx(9.0));
    REQUIRE((*ld)[1] == Catch::Approx(8.0));

    REQUIRE((*sym)[2] == "C");
    REQUIRE((*fi)[2] == 100);
    REQUIRE((*la)[2] == 100);
}

TEST_CASE("Sorted first/last carries state across a chunk boundary") {
    // Group "B" straddles the chunk boundary: first(v) must keep the value
    // seen in chunk 0 (not be overwritten by chunk 1), while last(v) must
    // advance to the value seen in chunk 1.
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    externs.register_chunked_table("sorted_fl_src", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        auto c0 = make_str_int_chunk("sym", {"A", "A", "B"}, "v", {1, 2, 10});
        auto c1 = make_str_int_chunk("sym", {"B", "B", "C"}, "v", {20, 30, 4});
        const std::vector<ir::OrderKey> ord{ir::OrderKey{.name = "sym", .ascending = true}};
        c0.ordering = ord;
        c1.ordering = ord;
        chunks.push_back(std::move(c0));
        chunks.push_back(std::move(c1));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn sorted_fl_src() -> DataFrame from \"x.hpp\"; "
        "sorted_fl_src()[by sym, select { sym, fi = first(v), la = last(v) }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* sym = std::get_if<Column<std::string>>(result->find("sym"));
    const auto* fi = std::get_if<Column<std::int64_t>>(result->find("fi"));
    const auto* la = std::get_if<Column<std::int64_t>>(result->find("la"));
    REQUIRE(sym != nullptr);
    REQUIRE(fi != nullptr);
    REQUIRE(la != nullptr);
    REQUIRE((*sym)[0] == "A");
    REQUIRE((*fi)[0] == 1);
    REQUIRE((*la)[0] == 2);
    REQUIRE((*sym)[1] == "B");
    REQUIRE((*fi)[1] == 10);  // seen in chunk 0, must not be overwritten
    REQUIRE((*la)[1] == 30);  // seen in chunk 1
    REQUIRE((*sym)[2] == "C");
    REQUIRE((*fi)[2] == 4);
    REQUIRE((*la)[2] == 4);
}

TEST_CASE("Sorted first/last skips nulls") {
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 1, 1});
    Column<std::int64_t> v{0, 5, 6, 0};
    runtime::ValidityBitmap valid;
    valid.push_back(false);  // leading null — first must skip it
    valid.push_back(true);   // 5
    valid.push_back(true);   // 6
    valid.push_back(false);  // trailing null — last must skip it
    t.add_column("v", std::move(v), std::move(valid));

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[order k asc][by k, select { k, fi = first(v), la = last(v) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* fi = std::get_if<Column<std::int64_t>>(result->find("fi"));
    const auto* la = std::get_if<Column<std::int64_t>>(result->find("la"));
    REQUIRE(fi != nullptr);
    REQUIRE(la != nullptr);
    REQUIRE((*fi)[0] == 5);  // skips the leading null
    REQUIRE((*la)[0] == 6);  // skips the trailing null
}

TEST_CASE("Non-numeric first/last on sorted input falls back to the hash operator") {
    // Input IS sorted on the group key, but the First/Last column is a string:
    // ChunkedSortedAggregateOperator has no string accumulator, so
    // needs_hash_fallback() must route this to ChunkedAggregateOperator
    // instead of erroring out.
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 2, 2, 2});
    t.add_column("who", Column<std::string>{"alice", "bob", "carol", "dan", "eve"});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[order k asc][by k, select { k, fi = first(who), la = last(who) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* k = std::get_if<Column<std::int64_t>>(result->find("k"));
    const auto* fi = std::get_if<Column<std::string>>(result->find("fi"));
    const auto* la = std::get_if<Column<std::string>>(result->find("la"));
    REQUIRE(k != nullptr);
    REQUIRE(fi != nullptr);
    REQUIRE(la != nullptr);
    REQUIRE((*k)[0] == 1);
    REQUIRE((*fi)[0] == "alice");
    REQUIRE((*la)[0] == "bob");
    REQUIRE((*k)[1] == 2);
    REQUIRE((*fi)[1] == "carol");
    REQUIRE((*la)[1] == "eve");
}

TEST_CASE("Categorical first/last streams via the hash aggregate operator") {
    Column<Categorical> grade;
    grade.push_back("gold");
    grade.push_back("silver");
    grade.push_back("bronze");
    grade.push_back("gold");

    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 2, 2});
    t.add_column("grade", std::move(grade));

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[by k, select { k, fi = first(grade), la = last(grade) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* fi = std::get_if<Column<Categorical>>(result->find("fi"));
    const auto* la = std::get_if<Column<Categorical>>(result->find("la"));
    REQUIRE(fi != nullptr);
    REQUIRE(la != nullptr);
    REQUIRE((*fi)[0] == "gold");
    REQUIRE((*la)[0] == "silver");
    REQUIRE((*fi)[1] == "bronze");
    REQUIRE((*la)[1] == "gold");
}

TEST_CASE("String first/last accumulates across chunk boundaries on the hash path") {
    // No ordering advertised → the hash ChunkedAggregateOperator handles it;
    // group 2 spans both chunks, exercising cross-chunk carryover of
    // flat_slots_'s first_value/last_value.
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    auto make_k_who_chunk = [](std::vector<std::int64_t> ks,
                               std::vector<std::string> whos) -> runtime::Chunk {
        runtime::Chunk chunk;
        runtime::ColumnEntry ke;
        ke.name = "k";
        ke.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
        auto& kc = std::get<Column<std::int64_t>>(*ke.column);
        for (auto v : ks) {
            kc.push_back(v);
        }
        chunk.columns.push_back(std::move(ke));

        runtime::ColumnEntry we;
        we.name = "who";
        we.column = std::make_shared<runtime::ColumnValue>(Column<std::string>{});
        auto& wc = std::get<Column<std::string>>(*we.column);
        for (const auto& v : whos) {
            wc.push_back(v);
        }
        chunk.columns.push_back(std::move(we));
        return chunk;
    };

    externs.register_chunked_table("fl_str_src", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_k_who_chunk({1, 2}, {"x1", "y1"}));
        chunks.push_back(make_k_who_chunk({2, 1}, {"y2", "x2"}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn fl_str_src() -> DataFrame from \"x.hpp\"; "
        "fl_str_src()[by k, select { k, fi = first(who), la = last(who) }][order k asc];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* k = std::get_if<Column<std::int64_t>>(result->find("k"));
    const auto* fi = std::get_if<Column<std::string>>(result->find("fi"));
    const auto* la = std::get_if<Column<std::string>>(result->find("la"));
    REQUIRE(k != nullptr);
    REQUIRE(fi != nullptr);
    REQUIRE(la != nullptr);
    REQUIRE((*k)[0] == 1);
    REQUIRE((*fi)[0] == "x1");  // seen in chunk 0
    REQUIRE((*la)[0] == "x2");  // seen in chunk 1, must overwrite
    REQUIRE((*k)[1] == 2);
    REQUIRE((*fi)[1] == "y1");
    REQUIRE((*la)[1] == "y2");
}

TEST_CASE("Streaming first/last match the materializing path (parity)") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{2, 1, 2, 1, 1, 2});
    t.add_column("x", Column<std::int64_t>{20, 1, 21, 2, 3, 22});
    t.add_column("who", Column<std::string>{"p", "a", "q", "b", "c", "r"});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    // Streamed: by-itself, first/last is streamable (routes to the hash
    // operator since there is no `order`).
    auto streamed = runtime::interpret(
        *require_ir("t[by g, select { g, fi = first(x), la = last(x), fw = first(who), "
                    "lw = last(who) }][order g asc];"),
        registry);
    // Materialized: adding median forces the whole aggregate node onto the
    // materializing path, which computes first/last via its own independent
    // fast-path code (aggregate.cpp).
    auto materialized = runtime::interpret(
        *require_ir("t[by g, select { g, fi = first(x), la = last(x), fw = first(who), "
                    "lw = last(who), md = median(x) }][order g asc];"),
        registry);
    REQUIRE(streamed.has_value());
    REQUIRE(materialized.has_value());
    REQUIRE(streamed->rows() == 2);
    REQUIRE(materialized->rows() == 2);

    const auto* fi_s = std::get_if<Column<std::int64_t>>(streamed->find("fi"));
    const auto* fi_m = std::get_if<Column<std::int64_t>>(materialized->find("fi"));
    const auto* la_s = std::get_if<Column<std::int64_t>>(streamed->find("la"));
    const auto* la_m = std::get_if<Column<std::int64_t>>(materialized->find("la"));
    const auto* fw_s = std::get_if<Column<std::string>>(streamed->find("fw"));
    const auto* fw_m = std::get_if<Column<std::string>>(materialized->find("fw"));
    const auto* lw_s = std::get_if<Column<std::string>>(streamed->find("lw"));
    const auto* lw_m = std::get_if<Column<std::string>>(materialized->find("lw"));
    REQUIRE(fi_s != nullptr);
    REQUIRE(fi_m != nullptr);
    REQUIRE(la_s != nullptr);
    REQUIRE(la_m != nullptr);
    REQUIRE(fw_s != nullptr);
    REQUIRE(fw_m != nullptr);
    REQUIRE(lw_s != nullptr);
    REQUIRE(lw_m != nullptr);
    for (std::size_t i = 0; i < 2; ++i) {
        REQUIRE((*fi_s)[i] == (*fi_m)[i]);
        REQUIRE((*la_s)[i] == (*la_m)[i]);
        REQUIRE((*fw_s)[i] == (*fw_m)[i]);
        REQUIRE((*lw_s)[i] == (*lw_m)[i]);
    }
}

// ── Multi-key group-by (the generic grouping path) ────────────────────────────
//
// A group-by whose keys are neither all-categorical nor a single string column
// lands in ChunkedAggregateOperator::process_rows_generic, which hashes each
// row's key columns in place and compares a candidate group's stored key
// against the row. Nothing exercised it before: making key comparison always
// answer "equal" — which merges unrelated groups — left the whole suite green.

namespace {

// Chunk with an Int64 key, a String key and an Int64 value: mixed key types are
// what force the generic path (a single string key or all-categorical keys have
// their own fast paths).
auto make_mixed_key_chunk(std::vector<std::int64_t> ids, std::vector<std::string> names,
                          std::vector<std::int64_t> values) -> runtime::Chunk {
    runtime::Chunk chunk;

    runtime::ColumnEntry id_entry;
    id_entry.name = "id";
    id_entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{std::move(ids)});
    chunk.columns.push_back(std::move(id_entry));

    runtime::ColumnEntry name_entry;
    name_entry.name = "name";
    name_entry.column = std::make_shared<runtime::ColumnValue>(Column<std::string>{});
    auto& name_col = std::get<Column<std::string>>(*name_entry.column);
    name_col.reserve(names.size());
    for (const auto& value : names) {
        name_col.push_back(value);
    }
    chunk.columns.push_back(std::move(name_entry));

    runtime::ColumnEntry value_entry;
    value_entry.name = "v";
    value_entry.column =
        std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{std::move(values)});
    chunk.columns.push_back(std::move(value_entry));

    return chunk;
}

}  // namespace

TEST_CASE("Interpret multi-key grouped aggregation distinguishes keys that share a column") {
    runtime::TableRegistry registry;
    runtime::Table table;
    // (1,"a") and (1,"b") share an id; (2,"a") shares the name with (1,"a").
    // A grouping that compares only part of the key would merge them.
    table.add_column("id", Column<std::int64_t>{1, 1, 2, 1, 2});
    table.add_column("name", Column<std::string>{});
    auto& names = std::get<Column<std::string>>(*table.columns[1].column);
    for (const auto* value : {"a", "b", "a", "a", "a"}) {
        names.push_back(value);
    }
    table.add_column("v", Column<std::int64_t>{10, 20, 30, 5, 7});
    registry.emplace("t", std::move(table));

    auto ir = require_ir("t[select { total = sum(v) }, by { id, name }];");
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE(result.has_value());

    const auto* id = std::get_if<Column<std::int64_t>>(result->find("id"));
    const auto* name = std::get_if<Column<std::string>>(result->find("name"));
    const auto* total = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(id != nullptr);
    REQUIRE(name != nullptr);
    REQUIRE(total != nullptr);
    REQUIRE(id->size() == 3);

    CHECK((*id)[0] == 1);
    CHECK((*name)[0] == "a");
    CHECK((*total)[0] == 15);  // 10 + 5
    CHECK((*id)[1] == 1);
    CHECK((*name)[1] == "b");
    CHECK((*total)[1] == 20);
    CHECK((*id)[2] == 2);
    CHECK((*name)[2] == "a");
    CHECK((*total)[2] == 37);  // 30 + 7
}

TEST_CASE("Interpret multi-key grouped aggregation keeps a null key distinct from zero") {
    runtime::TableRegistry registry;
    runtime::Table table;
    // A null cell still holds the type's zero value, so a grouping that ignores
    // validity would fold the null row into the genuine id=0 group.
    table.add_column("id", Column<std::int64_t>{0, 0, 0},
                     runtime::ValidityBitmap{true, false, true});
    table.add_column("name", Column<std::string>{});
    auto& names = std::get<Column<std::string>>(*table.columns[1].column);
    for (const auto* value : {"a", "a", "a"}) {
        names.push_back(value);
    }
    table.add_column("v", Column<std::int64_t>{1, 2, 4});
    registry.emplace("t", std::move(table));

    auto ir = require_ir("t[select { total = sum(v) }, by { id, name }];");
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE(result.has_value());

    const auto* total = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(total != nullptr);
    REQUIRE(total->size() == 2);
    CHECK((*total)[0] == 5);  // rows 0 and 2: id = 0
    CHECK((*total)[1] == 2);  // row 1: id is null

    const auto* id_entry = result->find_entry("id");
    REQUIRE(id_entry != nullptr);
    REQUIRE(id_entry->validity.has_value());
    CHECK((*id_entry->validity)[0] == true);
    CHECK((*id_entry->validity)[1] == false);
}

TEST_CASE("Interpret multi-key grouped aggregation tracks groups across chunks") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    // Group state (the key table, each group's stored key and its hash) has to
    // survive between chunks: (1,"a") appears in both, and must accumulate into
    // one group rather than open a second.
    externs.register_chunked_table("stream_mixed", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;
        chunks.push_back(make_mixed_key_chunk({1, 2}, {"a", "b"}, {10, 20}));
        chunks.push_back(make_mixed_key_chunk({1, 3}, {"a", "c"}, {5, 30}));
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    auto ir = require_ir(
        "extern fn stream_mixed() -> DataFrame from \"x.hpp\"; "
        "stream_mixed()[select { total = sum(v) }, by { id, name }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto* id = std::get_if<Column<std::int64_t>>(result->find("id"));
    const auto* total = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(id != nullptr);
    REQUIRE(total != nullptr);
    REQUIRE(id->size() == 3);

    CHECK((*id)[0] == 1);
    CHECK((*total)[0] == 15);  // 10 from chunk 1 + 5 from chunk 2
    CHECK((*id)[1] == 2);
    CHECK((*total)[1] == 20);
    CHECK((*id)[2] == 3);
    CHECK((*total)[2] == 30);
}

TEST_CASE("Interpret multi-key grouped aggregation survives growing past its key table") {
    // More groups than the key table's initial capacity, so it has to grow and
    // rehash every group it already holds without losing or merging any.
    constexpr std::int64_t kGroups = 5000;
    std::vector<std::int64_t> ids;
    std::vector<std::string> names;
    std::vector<std::int64_t> values;
    ids.reserve(kGroups * 2);
    names.reserve(kGroups * 2);
    values.reserve(kGroups * 2);
    for (std::int64_t pass = 0; pass < 2; ++pass) {
        for (std::int64_t i = 0; i < kGroups; ++i) {
            ids.push_back(i);
            names.push_back("name-" + std::to_string(i));
            values.push_back(i);
        }
    }

    runtime::TableRegistry registry;
    runtime::Table table;
    table.add_column("id", Column<std::int64_t>{std::move(ids)});
    table.add_column("name", Column<std::string>{});
    auto& name_col = std::get<Column<std::string>>(*table.columns[1].column);
    name_col.reserve(names.size());
    for (const auto& value : names) {
        name_col.push_back(value);
    }
    table.add_column("v", Column<std::int64_t>{std::move(values)});
    registry.emplace("t", std::move(table));

    auto ir = require_ir("t[select { total = sum(v) }, by { id, name }];");
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE(result.has_value());

    const auto* id = std::get_if<Column<std::int64_t>>(result->find("id"));
    const auto* total = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(id != nullptr);
    REQUIRE(total != nullptr);
    REQUIRE(id->size() == static_cast<std::size_t>(kGroups));
    for (std::size_t g = 0; g < static_cast<std::size_t>(kGroups); ++g) {
        REQUIRE((*id)[g] == static_cast<std::int64_t>(g));
        REQUIRE((*total)[g] == static_cast<std::int64_t>(g) * 2);  // each id appears twice
    }
}

// ── Cartesian cell overflow ──────────────────────────────────────────────────
//
// Multi-key grouping encodes a row's key tuple as a single Cartesian cell,
// `Σ code_i * stride_i`, where the strides are products of the per-column
// distinct counts. That cell identifies a tuple only while the product fits in
// 64 bits. Sixteen key columns of sixteen distinct values each multiply out to
// exactly 2^64, so the product wraps to 0: distinct tuples collide on the same
// cell, and the "how many cells are there" bound wraps with them, letting a
// zero-length dense array be indexed by an arbitrary cell. Both grouping
// implementations have to notice and fall back to identifying groups by their
// codes.

TEST_CASE("Interpret grouped aggregation survives a Cartesian cell overflow (categorical keys)") {
    // 16 categorical key columns x 16 distinct values = 2^64 cells exactly.
    constexpr std::size_t kKeys = 16;
    constexpr std::size_t kRows = 16;

    std::vector<std::string> dict;
    dict.reserve(kRows);
    for (std::size_t i = 0; i < kRows; ++i) {
        dict.push_back("v" + std::to_string(i));
    }

    runtime::Table table;
    for (std::size_t k = 0; k < kKeys; ++k) {
        Column<Categorical> col(dict);
        for (std::size_t row = 0; row < kRows; ++row) {
            col.push_code(static_cast<Column<Categorical>::code_type>(row));
        }
        table.add_column("k" + std::to_string(k), std::move(col));
    }
    std::vector<std::int64_t> values;
    values.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        values.push_back(static_cast<std::int64_t>(row) + 1);
    }
    table.add_column("v", Column<std::int64_t>{std::move(values)});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    std::string keys;
    for (std::size_t k = 0; k < kKeys; ++k) {
        keys += (k == 0 ? "k" : ", k") + std::to_string(k);
    }
    const std::string source = "t[select { total = sum(v) }, by { " + keys + " }];";
    auto ir = require_ir(source.c_str());
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE(result.has_value());

    // Every row is a distinct key tuple, so nothing may merge.
    const auto* total = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(total != nullptr);
    REQUIRE(total->size() == kRows);
    for (std::size_t g = 0; g < kRows; ++g) {
        REQUIRE((*total)[g] == static_cast<std::int64_t>(g) + 1);
    }
}

TEST_CASE("Interpret resampled aggregation survives a Cartesian cell overflow") {
    // resample routes through aggregate_table, which is where the *other*
    // Cartesian encoding lives — it prepends the time bucket to the group keys.
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    constexpr std::size_t kKeys = 16;
    constexpr std::size_t kRows = 16;

    runtime::Table table;
    std::vector<Timestamp> stamps;
    stamps.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        stamps.push_back(ts_from_nanos(static_cast<std::int64_t>(row) * min_ns));
    }
    table.add_column("ts", Column<Timestamp>{std::move(stamps)});
    for (std::size_t k = 0; k < kKeys; ++k) {
        std::vector<std::int64_t> codes;
        codes.reserve(kRows);
        for (std::size_t row = 0; row < kRows; ++row) {
            codes.push_back(static_cast<std::int64_t>(row));
        }
        table.add_column("k" + std::to_string(k), Column<std::int64_t>{std::move(codes)});
    }
    std::vector<double> values;
    values.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        values.push_back(static_cast<double>(row) + 1.0);
    }
    table.add_column("v", Column<double>{std::move(values)});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("tf", std::move(table));

    std::string keys;
    for (std::size_t k = 0; k < kKeys; ++k) {
        keys += (k == 0 ? "k" : ", k") + std::to_string(k);
    }
    const std::string source = "tf[resample 1m, select { total = sum(v) }, by { " + keys + " }];";
    auto ir = require_ir(source.c_str());
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* total = std::get_if<Column<double>>(result->find("total"));
    REQUIRE(total != nullptr);
    REQUIRE(total->size() == kRows);  // one bucket per row, each a distinct key tuple
    for (std::size_t g = 0; g < kRows; ++g) {
        REQUIRE((*total)[g] == static_cast<double>(g) + 1.0);
    }
}

// ── IN-lists over in-memory data ─────────────────────────────────────────────
//
// `col == a || col == b` on a categorical column is a membership test, and
// compute_mask evaluates it in one pass against a byte per dictionary code
// rather than building a full-width mask per OR arm. In-memory tables (CSV,
// constructed frames, a filter sitting above a join) all take that path.

TEST_CASE("Interpret an IN-list over a categorical column") {
    Column<Categorical> mode(std::vector<std::string>{"AIR", "AIR REG", "SHIP", "RAIL"});
    for (auto code : {0, 1, 2, 3, 0, 2}) {
        mode.push_code(static_cast<Column<Categorical>::code_type>(code));
    }

    runtime::Table table;
    table.add_column("mode", std::move(mode));
    table.add_column("qty", Column<std::int64_t>{10, 20, 30, 40, 50, 60});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    auto ir = require_ir(R"(t[filter mode == "AIR" || mode == "AIR REG"];)");
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE(result.has_value());

    const auto* qty = std::get_if<Column<std::int64_t>>(result->find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE(qty->size() == 3);
    CHECK((*qty)[0] == 10);
    CHECK((*qty)[1] == 20);
    CHECK((*qty)[2] == 50);

    // A value the column never takes matches nothing rather than everything.
    auto none_ir = require_ir(R"(t[filter mode == "TRUCK" || mode == "BARGE"];)");
    auto none = runtime::interpret(*none_ir, registry, nullptr, nullptr);
    REQUIRE(none.has_value());
    CHECK(none->rows() == 0);

    // Still a disjunction when the arms name different columns.
    auto mixed_ir = require_ir(R"(t[filter mode == "SHIP" || qty == 10];)");
    auto mixed = runtime::interpret(*mixed_ir, registry, nullptr, nullptr);
    REQUIRE(mixed.has_value());
    CHECK(mixed->rows() == 3);  // rows 0, 2, 5

    // An IN-list ANDed with a range: the q19 shape, in memory.
    auto both_ir = require_ir(R"(t[filter (mode == "AIR" || mode == "SHIP") && qty >= 30];)");
    auto both = runtime::interpret(*both_ir, registry, nullptr, nullptr);
    REQUIRE(both.has_value());
    const auto* both_qty = std::get_if<Column<std::int64_t>>(both->find("qty"));
    REQUIRE(both_qty != nullptr);
    REQUIRE(both_qty->size() == 3);
    CHECK((*both_qty)[0] == 30);
    CHECK((*both_qty)[1] == 50);
    CHECK((*both_qty)[2] == 60);
}

TEST_CASE("Interpret an IN-list over a categorical column with nulls") {
    Column<Categorical> mode(std::vector<std::string>{"AIR", "SHIP"});
    for (auto code : {0, 0, 1}) {
        mode.push_code(static_cast<Column<Categorical>::code_type>(code));
    }
    runtime::ValidityBitmap validity(3, true);
    validity.set(1, false);  // holds AIR's code, but is null

    runtime::Table table;
    table.add_column("mode", std::move(mode), std::move(validity));
    table.add_column("qty", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    auto ir = require_ir(R"(t[filter mode == "AIR" || mode == "SHIP"];)");
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE(result.has_value());

    const auto* qty = std::get_if<Column<std::int64_t>>(result->find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE(qty->size() == 2);  // the null matches no literal
    CHECK((*qty)[0] == 1);
    CHECK((*qty)[1] == 3);
}

// ── like(value, pattern) ─────────────────────────────────────────────────────
// SQL-LIKE matching: the pattern must cover the whole value, `%` matches zero
// or more code points, `_` exactly one, `\` escapes the next character.

namespace {

// Run `like(v, <pattern>)` over a dense String column. The pattern is written
// as it would be in Ibex source, so it also passes through the lexer's own
// string escapes (a literal `%` is `\\%` here, as it is in a .ibex file).
auto like_over(const std::vector<std::string>& values, const std::string& pattern)
    -> std::vector<bool> {
    Column<std::string> col;
    for (const auto& v : values) {
        col.push_back(v);
    }
    runtime::Table t;
    t.add_column("v", std::move(col));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    const std::string source = "t[update { m = like(v, \"" + pattern + "\") }];";
    auto ir = require_ir(source.c_str());
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* m = std::get_if<Column<bool>>(result->find("m"));
    REQUIRE(m != nullptr);

    std::vector<bool> out;
    out.reserve(m->size());
    for (std::size_t i = 0; i < m->size(); ++i) {
        out.push_back((*m)[i]);
    }
    return out;
}

}  // namespace

TEST_CASE("like matches the whole value, not a substring", "[interpreter][like]") {
    const std::vector<std::string> values{"green", "forest green", "green plate", "GREEN", ""};
    CHECK(like_over(values, "green") == std::vector<bool>{true, false, false, false, false});
    CHECK(like_over(values, "green%") == std::vector<bool>{true, false, true, false, false});
    CHECK(like_over(values, "%green") == std::vector<bool>{true, true, false, false, false});
    CHECK(like_over(values, "%green%") == std::vector<bool>{true, true, true, false, false});
}

TEST_CASE("like: % matches zero or more code points", "[interpreter][like]") {
    const std::vector<std::string> values{"", "a", "ab", "ba"};
    CHECK(like_over(values, "%") == std::vector<bool>{true, true, true, true});
    CHECK(like_over(values, "%%") == std::vector<bool>{true, true, true, true});
    CHECK(like_over(values, "a%") == std::vector<bool>{false, true, true, false});
    CHECK(like_over(values, "%a") == std::vector<bool>{false, true, false, true});
    // The empty pattern matches only the empty value.
    CHECK(like_over(values, "") == std::vector<bool>{true, false, false, false});
}

TEST_CASE("like: ordered fragments must appear in order", "[interpreter][like]") {
    const std::vector<std::string> values{
        "no special requests here",  // both, in order
        "special requests",          // adjacent, still in order
        "requests are special",      // present but out of order
        "special",                   // only the first fragment
    };
    CHECK(like_over(values, "%special%requests%") == std::vector<bool>{true, true, false, false});
    // Anchored fragments: the tail must end the value.
    CHECK(like_over(values, "special%requests") == std::vector<bool>{false, true, false, false});
}

TEST_CASE("like: _ matches exactly one code point, including multibyte", "[interpreter][like]") {
    CHECK(like_over({"cat", "cot", "coat", "ct"}, "c_t") ==
          std::vector<bool>{true, true, false, false});
    // é is two UTF-8 bytes but one code point, so a single `_` covers it.
    CHECK(like_over({"café", "cafe", "caf"}, "caf_") == std::vector<bool>{true, true, false});
    CHECK(like_over({"héllo"}, "h_llo") == std::vector<bool>{true});
    CHECK(like_over({"日本語"}, "_本_") == std::vector<bool>{true});
    CHECK(like_over({"日本語"}, "___") == std::vector<bool>{true});
    CHECK(like_over({"日本語"}, "____") == std::vector<bool>{false});
}

TEST_CASE("like: escaped wildcards match literally", "[interpreter][like]") {
    // Ibex string literals process escapes too, so a literal `%` is `\\%` in
    // source and reaches the matcher as `\%`.
    CHECK(like_over({"100%", "1000", "100"}, "100\\\\%") == std::vector<bool>{true, false, false});
    CHECK(like_over({"a_b", "axb"}, "a\\\\_b") == std::vector<bool>{true, false});
    CHECK(like_over({"a\\b", "axb"}, "a\\\\\\\\b") == std::vector<bool>{true, false});
    // An escape is only special before the character it escapes.
    CHECK(like_over({"50%off", "50XXoff"}, "50\\\\%%") == std::vector<bool>{true, false});
}

TEST_CASE("like: matching is case-sensitive", "[interpreter][like]") {
    CHECK(like_over({"BRASS", "brass", "Brass"}, "%BRASS") ==
          std::vector<bool>{true, false, false});
}

TEST_CASE("like: a trailing escape is an invalid pattern", "[interpreter][like]") {
    runtime::Table t;
    t.add_column("v", Column<std::string>{"a"});
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto ir = require_ir(R"(t[filter like(v, "abc\\")];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("trailing escape") != std::string::npos);
}

TEST_CASE("like propagates nulls from either argument", "[interpreter][like][null]") {
    // Standard scalar propagation: null in -> null out. A null predicate is not
    // true, so `filter` drops the row (and `!like(...)` drops it too).
    runtime::Table t;
    Column<std::string> names{"alpha", "beta", ""};
    t.add_column("v", std::move(names), std::vector<bool>{true, true, false});
    Column<std::string> pats{"%a%", "", "%a%"};
    t.add_column("p", std::move(pats), std::vector<bool>{true, false, true});
    t.add_column("id", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    SECTION("literal pattern: the null value yields a null result") {
        auto ir = require_ir(R"(t[update { m = like(v, "%a%") }];)");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* entry = result->find_entry("m");
        REQUIRE(entry != nullptr);
        const auto& m = std::get<Column<bool>>(*entry->column);
        REQUIRE(entry->validity.has_value());
        CHECK((*entry->validity)[0] == true);
        CHECK((*entry->validity)[1] == true);
        CHECK((*entry->validity)[2] == false);
        CHECK(m[0] == true);
        CHECK(m[1] == true);  // "beta" contains an 'a'
    }

    SECTION("pattern column: a null pattern yields a null result") {
        auto ir = require_ir("t[update { m = like(v, p) }];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* entry = result->find_entry("m");
        REQUIRE(entry != nullptr);
        REQUIRE(entry->validity.has_value());
        CHECK((*entry->validity)[0] == true);
        CHECK((*entry->validity)[1] == false);  // null pattern
        CHECK((*entry->validity)[2] == false);  // null value
        CHECK(std::get<Column<bool>>(*entry->column)[0] == true);
    }

    SECTION("filter drops null predicates, and so does its negation") {
        auto keep = require_ir(R"(t[filter like(v, "%a%")];)");
        auto kept = runtime::interpret(*keep, registry);
        REQUIRE(kept.has_value());
        CHECK(kept->rows() == 2);  // alpha, beta — the null row is not kept

        auto drop = require_ir(R"(t[filter !like(v, "%a%")];)");
        auto dropped = runtime::interpret(*drop, registry);
        REQUIRE(dropped.has_value());
        CHECK(dropped->rows() == 0);  // !null is null, which is not true either
    }
}

TEST_CASE("like matches a categorical column through its dictionary", "[interpreter][like]") {
    // Parquet hands back dictionary-encoded strings; the kernel matches each
    // distinct value once and maps the answer through the codes.
    std::vector<std::string> dict{"SMALL BRASS", "LARGE PLATED BRASS", "MEDIUM COPPER"};
    Column<Categorical> types(dict);
    for (const auto code : {0, 1, 2, 0}) {
        types.push_code(static_cast<Column<Categorical>::code_type>(code));
    }
    runtime::Table t;
    t.add_column("p_type", std::move(types));
    t.add_column("id", Column<std::int64_t>{1, 2, 3, 4});

    runtime::TableRegistry registry;
    registry.emplace("part", std::move(t));

    auto ir = require_ir(R"(part[filter like(p_type, "%BRASS")];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* id = std::get_if<Column<std::int64_t>>(result->find("id"));
    REQUIRE(id != nullptr);
    REQUIRE(id->size() == 3);
    CHECK((*id)[0] == 1);
    CHECK((*id)[1] == 2);
    CHECK((*id)[2] == 4);

    auto negated = require_ir(R"(part[filter !like(p_type, "SMALL%")];)");
    auto rest = runtime::interpret(*negated, registry);
    REQUIRE(rest.has_value());
    CHECK(rest->rows() == 2);
}

TEST_CASE("like composes with the boolean operators", "[interpreter][like]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"forest green", "green", "midnight blue"});
    t.add_column("size", Column<std::int64_t>{1, 9, 1});

    runtime::TableRegistry registry;
    registry.emplace("part", std::move(t));

    auto conjunction = require_ir(R"(part[filter like(name, "%green%") && size < 5];)");
    auto both = runtime::interpret(*conjunction, registry);
    REQUIRE(both.has_value());
    CHECK(both->rows() == 1);

    auto disjunction = require_ir(R"(part[filter like(name, "%blue%") || size == 9];)");
    auto either = runtime::interpret(*disjunction, registry);
    REQUIRE(either.has_value());
    CHECK(either->rows() == 2);
}

TEST_CASE("like evaluates a computed pattern per row", "[interpreter][like]") {
    // A pattern that is neither a bare column nor a literal (here: string
    // interpolation) bypasses the column kernel and runs through the registry's
    // per-row evaluator — the two must agree.
    runtime::Table t;
    t.add_column("v", Column<std::string>{"forest green", "midnight blue", "green"});
    t.add_column("needle", Column<std::string>{"green", "green", "blue"});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto ir = require_ir("t[update { m = like(v, `%${needle}%`) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* m = std::get_if<Column<bool>>(result->find("m"));
    REQUIRE(m != nullptr);
    CHECK((*m)[0] == true);
    CHECK((*m)[1] == false);
    CHECK((*m)[2] == false);
}

TEST_CASE("like rejects wrong arity and non-String arguments", "[interpreter][like]") {
    runtime::Table t;
    t.add_column("v", Column<std::string>{"a"});
    t.add_column("n", Column<std::int64_t>{1});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto one_arg = require_ir(R"(t[filter like(v)];)");
    auto arity = runtime::interpret(*one_arg, registry);
    REQUIRE_FALSE(arity.has_value());
    CHECK(arity.error().find("like") != std::string::npos);

    auto int_value = require_ir(R"(t[filter like(n, "a%")];)");
    auto wrong_value = runtime::interpret(*int_value, registry);
    REQUIRE_FALSE(wrong_value.has_value());
    CHECK(wrong_value.error().find("like") != std::string::npos);

    auto int_pattern = require_ir("t[filter like(v, 1)];");
    auto wrong_pattern = runtime::interpret(*int_pattern, registry);
    REQUIRE_FALSE(wrong_pattern.has_value());
    CHECK(wrong_pattern.error().find("like") != std::string::npos);
}

// ── count(col): non-null count ───────────────────────────────────────────────
// count() counts rows; count(col) counts the non-null values of col. Lowered to
// sum() over a derived 0/1 column, so every grouping path shares one semantics.

TEST_CASE("count(col) counts non-null values, count() counts rows",
          "[interpreter][aggregate][count][null]") {
    // The shape that motivates the form: a left join leaves unmatched rows with
    // a null on the right, and SQL's count(o_orderkey) must report 0 for them
    // where count() reports 1.
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 2, 3});
    t.add_column("v", Column<std::int64_t>{10, 0, 30, 0},
                 std::vector<bool>{true, false, true, false});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    SECTION("grouped select") {
        auto ir = require_ir("t[select { rows = count(), vals = count(v) }, by g];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* rows = std::get_if<Column<std::int64_t>>(result->find("rows"));
        const auto* vals = std::get_if<Column<std::int64_t>>(result->find("vals"));
        REQUIRE(rows != nullptr);
        REQUIRE(vals != nullptr);
        REQUIRE(rows->size() == 3);
        CHECK((*rows)[0] == 2);
        CHECK((*vals)[0] == 1);  // group 1: two rows, one non-null
        CHECK((*rows)[1] == 1);
        CHECK((*vals)[1] == 1);
        CHECK((*rows)[2] == 1);
        CHECK((*vals)[2] == 0);  // group 3: all null -> 0, not null
    }

    SECTION("ungrouped select") {
        auto ir = require_ir("t[select { rows = count(), vals = count(v) }];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        CHECK((*std::get_if<Column<std::int64_t>>(result->find("rows")))[0] == 4);
        CHECK((*std::get_if<Column<std::int64_t>>(result->find("vals")))[0] == 2);
    }

    SECTION("grouped update broadcasts the per-group count") {
        // `update + by` parses its aggregates at run time, not at lowering — a
        // separate code path from the two above.
        auto ir = require_ir("t[update { n = count(v) }, by g];");
        auto result = runtime::interpret(*ir, registry);
        REQUIRE(result.has_value());
        const auto* n = std::get_if<Column<std::int64_t>>(result->find("n"));
        REQUIRE(n != nullptr);
        REQUIRE(n->size() == 4);
        CHECK((*n)[0] == 1);
        CHECK((*n)[1] == 1);
        CHECK((*n)[2] == 1);
        CHECK((*n)[3] == 0);
    }
}

TEST_CASE("count(col) works over a String column", "[interpreter][aggregate][count][null]") {
    runtime::Table t;
    t.add_column("g", Column<std::int64_t>{1, 1, 2});
    Column<std::string> names{"alpha", "", "gamma"};
    t.add_column("name", std::move(names), std::vector<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto ir = require_ir("t[select { n = count(name) }, by g];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* n = std::get_if<Column<std::int64_t>>(result->find("n"));
    REQUIRE(n != nullptr);
    CHECK((*n)[0] == 1);
    CHECK((*n)[1] == 1);
}

TEST_CASE("count rejects more than one argument", "[interpreter][aggregate][count]") {
    // Rejected at lowering, before any table is touched.
    auto program = require_program("t[select { n = count(a, b) }];");
    auto lowered = parser::lower(program);
    REQUIRE_FALSE(lowered.has_value());
    CHECK(lowered.error().message.find("count()") != std::string::npos);
}

TEST_CASE("count(expr) requires a column name", "[interpreter][aggregate][count]") {
    // count(a + b) has no spelling: the 0/1 flag column it would need cannot
    // reference another column materialized in the same update pass.
    auto program = require_program("t[select { n = count(a + b) }];");
    auto lowered = parser::lower(program);
    REQUIRE_FALSE(lowered.has_value());
    CHECK(lowered.error().message.find("column name") != std::string::npos);
}

TEST_CASE("Int64 casts Bool to 0/1", "[interpreter][cast]") {
    // The cast count(col) leans on: it also makes `sum(Int64(pred))` spellable.
    runtime::Table t;
    t.add_column("x", Column<std::int64_t>{5, 15, 25});

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto ir = require_ir("t[update { big = Int64(x > 10) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* big = std::get_if<Column<std::int64_t>>(result->find("big"));
    REQUIRE(big != nullptr);
    CHECK((*big)[0] == 0);
    CHECK((*big)[1] == 1);
    CHECK((*big)[2] == 1);
}
