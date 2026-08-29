// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "data_gen.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::data_gen {

namespace {

auto split_symbols(const std::string& csv) -> std::vector<std::string> {
    std::vector<std::string> out;
    std::stringstream ss(csv);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
            out.push_back(std::move(item));
        }
    }
    if (out.empty()) {
        out.emplace_back("SYM");
    }
    return out;
}

struct SymbolInfo {
    std::string name;
    std::string sector;
};

// Known tickers get a real name and sector so demo joins read naturally;
// anything else is derived deterministically from the symbol string.
auto lookup_symbol(const std::string& symbol) -> SymbolInfo {
    static const std::map<std::string, SymbolInfo> known = {
        {"AAPL", {"Apple Inc.", "Technology"}},
        {"MSFT", {"Microsoft Corp.", "Technology"}},
        {"GOOG", {"Alphabet Inc.", "Communication Services"}},
        {"AMZN", {"Amazon.com Inc.", "Consumer Discretionary"}},
        {"NVDA", {"NVIDIA Corp.", "Technology"}},
        {"META", {"Meta Platforms Inc.", "Communication Services"}},
        {"TSLA", {"Tesla Inc.", "Consumer Discretionary"}},
        {"JPM", {"JPMorgan Chase & Co.", "Financials"}},
        {"XOM", {"Exxon Mobil Corp.", "Energy"}},
    };
    if (const auto it = known.find(symbol); it != known.end()) {
        return it->second;
    }
    static constexpr std::array<std::string_view, 6> sectors = {
        "Technology", "Financials", "Healthcare", "Energy", "Industrials", "Utilities"};
    const auto hash = std::hash<std::string>{}(symbol);
    return {symbol + " Corp.", std::string(sectors[hash % sectors.size()])};
}

auto now_ms() -> std::int64_t {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
auto gen_ticks(const runtime::RngBridge& rng, std::int64_t n, const std::string& symbols,
               double start_price, double volatility, double interval_ms, std::int64_t start_ts_ms)
    -> runtime::Table {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    n = std::max<std::int64_t>(n, 0);
    const auto rows = static_cast<std::size_t>(n);
    const auto names = split_symbols(symbols);
    const auto base_ts_ms = start_ts_ms != 0 ? start_ts_ms : now_ms();

    std::vector<std::int64_t> symbol_idx(rows);
    if (!symbol_idx.empty()) {
        rng.fill_int(symbol_idx.data(), rows, 0, static_cast<std::uint64_t>(names.size()));
    }
    std::vector<double> price_steps(rows);
    if (!price_steps.empty()) {
        rng.fill_normal(price_steps.data(), rows, 0.0, volatility);
    }
    // Each symbol gets its own base price so a group-by by symbol shows
    // distinct levels rather than five samples of one shared walk.
    std::vector<double> symbol_base(names.size(), start_price);
    if (!symbol_base.empty()) {
        rng.fill_uniform(symbol_base.data(), names.size(), start_price * 0.6, start_price * 2.4);
    }
    std::vector<std::int64_t> volume(rows);
    if (!volume.empty()) {
        rng.fill_int(volume.data(), rows, 1, 10'000);
    }
    // Poisson-process arrivals: gaps are Exponential(mean = interval_ms), not
    // evenly spaced, so bursts and lulls both occur like real tick data.
    std::vector<double> gaps_ms(rows);
    if (!gaps_ms.empty()) {
        const double rate = interval_ms > 0.0 ? 1.0 / interval_ms : 1.0;
        rng.fill_exponential(gaps_ms.data(), rows, rate);
    }

    Column<Timestamp> ts_col;
    Column<double> price_col;
    Column<std::int64_t> volume_col;
    ts_col.reserve(rows);
    price_col.reserve(rows);
    volume_col.reserve(rows);

    // `symbol` is a handful of distinct values over up to millions of rows: the
    // textbook case for a dictionary-encoded column. Emitting it as Categorical
    // (the row->dictionary codes are exactly `symbol_idx`) lets a group-by or a
    // join on `symbol` resolve each code once instead of hashing a string per
    // row — several times faster on the large tables this generator produces.
    using Code = Column<Categorical>::code_type;
    std::vector<Code> symbol_codes(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        symbol_codes[i] = static_cast<Code>(symbol_idx[i]);  // 0..names.size()-1
    }
    Column<Categorical> symbol_col(names, std::move(symbol_codes));

    // Per-symbol mean-reverting walk. A pure additive walk's variance grows with
    // the row count, so over the millions of rows this generator targets every
    // symbol drifts arbitrarily far from its base and the levels reconverge into
    // noise. The reversion term (pull toward `symbol_base`) keeps each series
    // fluctuating around its own price.
    constexpr double kReversion = 0.005;
    std::vector<double> symbol_price = symbol_base;
    auto ts_ms = static_cast<double>(base_ts_ms);
    for (std::size_t i = 0; i < rows; ++i) {
        ts_ms += gaps_ms[i];
        ts_col.push_back(Timestamp{static_cast<std::int64_t>(ts_ms * 1'000'000.0)});
        const auto sym = static_cast<std::size_t>(symbol_idx[i]);
        double price = symbol_price[sym] + price_steps[i] +
                       kReversion * (symbol_base[sym] - symbol_price[sym]);
        price = std::max(price, 0.01);
        symbol_price[sym] = price;
        price_col.push_back(price);
        volume_col.push_back(volume[i]);
    }

    runtime::Table out;
    out.add_column("timestamp", std::move(ts_col));
    out.add_column("symbol", std::move(symbol_col));
    out.add_column("price", std::move(price_col));
    out.add_column("volume", std::move(volume_col));
    return out;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto gen_walk(const runtime::RngBridge& rng, std::int64_t n, double start, double step_std)
    -> runtime::Table {
    n = std::max<std::int64_t>(n, 0);
    const auto rows = static_cast<std::size_t>(n);
    std::vector<double> steps(rows);
    if (!steps.empty()) {
        rng.fill_normal(steps.data(), rows, 0.0, step_std);
    }

    Column<double> value_col;
    value_col.reserve(rows);
    double value = start;
    for (std::size_t i = 0; i < rows; ++i) {
        value += steps[i];
        value_col.push_back(value);
    }

    runtime::Table out;
    out.add_column("value", std::move(value_col));
    return out;
}

auto gen_normal(const runtime::RngBridge& rng, std::int64_t n, double mean, double stddev)
    -> runtime::Table {
    n = std::max<std::int64_t>(n, 0);
    const auto rows = static_cast<std::size_t>(n);
    std::vector<double> values(rows);
    if (!values.empty()) {
        rng.fill_normal(values.data(), rows, mean, stddev);
    }
    Column<double> value_col;
    value_col.reserve(rows);
    for (double v : values) {
        value_col.push_back(v);
    }

    runtime::Table out;
    out.add_column("value", std::move(value_col));
    return out;
}

auto gen_uniform(const runtime::RngBridge& rng, std::int64_t n, double low, double high)
    -> runtime::Table {
    n = std::max<std::int64_t>(n, 0);
    const auto rows = static_cast<std::size_t>(n);
    std::vector<double> values(rows);
    if (!values.empty()) {
        rng.fill_uniform(values.data(), rows, low, high);
    }
    Column<double> value_col;
    value_col.reserve(rows);
    for (double v : values) {
        value_col.push_back(v);
    }

    runtime::Table out;
    out.add_column("value", std::move(value_col));
    return out;
}

auto gen_ids(std::int64_t n, const std::string& prefix) -> runtime::Table {
    n = std::max<std::int64_t>(n, 0);
    const auto rows = static_cast<std::size_t>(n);
    Column<std::string> id_col;
    id_col.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        id_col.push_back(prefix + std::to_string(i));
    }

    runtime::Table out;
    out.add_column("id", std::move(id_col));
    return out;
}

auto gen_reference(const std::string& symbols) -> runtime::Table {
    std::vector<std::string> distinct;
    for (auto& symbol : split_symbols(symbols)) {
        if (std::ranges::find(distinct, symbol) == distinct.end()) {
            distinct.push_back(std::move(symbol));
        }
    }

    // Dimension-table string columns are Categorical: `symbol` so its type
    // matches `gen_ticks`'s join key, and the rest so that gathering them across
    // a join to a large fact table copies dictionary codes rather than strings.
    Column<Categorical> symbol_col;
    Column<Categorical> name_col;
    Column<Categorical> sector_col;
    Column<Categorical> currency_col;
    Column<std::int64_t> lot_size_col;
    Column<double> tick_size_col;
    for (const auto& symbol : distinct) {
        const auto info = lookup_symbol(symbol);
        symbol_col.push_back(symbol);
        name_col.push_back(info.name);
        sector_col.push_back(info.sector);
        currency_col.push_back("USD");
        lot_size_col.push_back(100);
        tick_size_col.push_back(0.01);
    }

    runtime::Table out;
    out.add_column("symbol", std::move(symbol_col));
    out.add_column("name", std::move(name_col));
    out.add_column("sector", std::move(sector_col));
    out.add_column("currency", std::move(currency_col));
    out.add_column("lot_size", std::move(lot_size_col));
    out.add_column("tick_size", std::move(tick_size_col));
    return out;
}

}  // namespace ibex::data_gen

namespace {

using ibex::runtime::ExternArgs;
using ibex::runtime::ExternValue;

// The interpreter type-checks extern call arguments against the declared
// signature (data_gen.ibex) before invoking these lambdas, so get_if here
// never actually returns null; it's a defensive check, not a real code path.
auto arg_int(const ExternArgs& args, std::size_t i) -> std::int64_t {
    const auto* v = std::get_if<std::int64_t>(&args[i]);
    return v != nullptr ? *v : 0;
}
auto arg_double(const ExternArgs& args, std::size_t i) -> double {
    const auto* v = std::get_if<double>(&args[i]);
    return v != nullptr ? *v : 0.0;
}
auto arg_string(const ExternArgs& args, std::size_t i) -> std::string {
    const auto* v = std::get_if<std::string>(&args[i]);
    return v != nullptr ? *v : std::string{};
}

}  // namespace

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    const ibex::runtime::RngBridge rng = registry->rng();

    registry->register_table(
        "gen_ticks", [rng](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 6) {
                return std::unexpected("gen_ticks() expects 6 arguments");
            }
            return ExternValue{ibex::data_gen::gen_ticks(rng, arg_int(args, 0), arg_string(args, 1),
                                                         arg_double(args, 2), arg_double(args, 3),
                                                         arg_double(args, 4), arg_int(args, 5))};
        });

    registry->register_table(
        "gen_walk", [rng](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 3) {
                return std::unexpected("gen_walk() expects 3 arguments");
            }
            return ExternValue{ibex::data_gen::gen_walk(rng, arg_int(args, 0), arg_double(args, 1),
                                                        arg_double(args, 2))};
        });

    registry->register_table(
        "gen_normal", [rng](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 3) {
                return std::unexpected("gen_normal() expects 3 arguments");
            }
            return ExternValue{ibex::data_gen::gen_normal(
                rng, arg_int(args, 0), arg_double(args, 1), arg_double(args, 2))};
        });

    registry->register_table(
        "gen_uniform", [rng](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 3) {
                return std::unexpected("gen_uniform() expects 3 arguments");
            }
            return ExternValue{ibex::data_gen::gen_uniform(
                rng, arg_int(args, 0), arg_double(args, 1), arg_double(args, 2))};
        });

    registry->register_table(
        "gen_ids", [](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 2) {
                return std::unexpected("gen_ids() expects 2 arguments");
            }
            return ExternValue{ibex::data_gen::gen_ids(arg_int(args, 0), arg_string(args, 1))};
        });

    registry->register_table(
        "gen_reference", [](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("gen_reference() expects 1 argument");
            }
            return ExternValue{ibex::data_gen::gen_reference(arg_string(args, 0))};
        });
}
