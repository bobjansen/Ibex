// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
// Calls production filter stages. Setup, checking and destruction are untimed.
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <bit>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

#include "csv.hpp"
#include "interpreter_internal.hpp"

// Match ibex_bench: retain freed pages between resident-query iterations.
extern "C" {
const char* malloc_conf = "dirty_decay_ms:-1,muzzy_decay_ms:-1";
}

using Clock = std::chrono::steady_clock;
namespace rt = ibex::runtime;

int main(int argc, char** argv) {
    if (argc != 2)
        return 2;
    rt::TableRegistry tables;
    tables.emplace("trades", ibex::ext::csv::read(argv[1]));
    const auto& input = tables.at("trades");
    const rt::RowRange range{0, input.rows()};
    auto parsed = ibex::parser::parse("trades[filter price * qty > 50000.0];");
    if (!parsed)
        return 3;
    auto plan = ibex::parser::lower(*parsed);
    if (!plan || (*plan)->kind() != ibex::ir::NodeKind::Filter)
        return 3;
    const auto& predicate = static_cast<const ibex::ir::FilterNode&>(**plan).predicate();
    auto selection = rt::compute_filter_selection(input, predicate, nullptr, range, 0);
    if (!selection)
        return 4;
    const auto& price = std::get<ibex::Column<double>>(std::as_const(*input.find("price")));
    const auto& qty = std::get<ibex::Column<std::int64_t>>(std::as_const(*input.find("qty")));
    std::vector<std::size_t> indices;
    std::vector<std::uint8_t> mask(input.rows());
    for (std::size_t i = 0; i < input.rows(); ++i) {
        mask[i] = price[i] * static_cast<double>(qty[i]) > 50000.0;
        if (mask[i])
            indices.push_back(i);
    }
    if (selection->kept != indices.size())
        return 5;
    for (std::size_t i = 0; i < input.rows(); ++i)
        if (((selection->keep_words[i / 64] >> (i % 64)) & 1U) != mask[i])
            return 5;
    auto verify = [&](const rt::Table& output) {
        if (output.rows() != indices.size())
            throw std::runtime_error("row count");
        for (const auto& entry : output.columns) {
            std::visit(
                [&](const auto& column) {
                    using C = std::decay_t<decltype(column)>;
                    const auto& source = std::get<C>(std::as_const(*input.find(entry.name)));
                    for (std::size_t j = 0; j < indices.size(); ++j) {
                        if constexpr (std::is_same_v<C, ibex::Column<ibex::Categorical>>) {
                            if (column.code_at(j) != source.code_at(indices[j]))
                                throw std::runtime_error("categorical value");
                        } else {
                            if (column[j] != source[indices[j]])
                                throw std::runtime_error("value");
                        }
                    }
                },
                std::as_const(*entry.column));
        }
    };
    std::cout << "ready " << input.rows() << ' ' << indices.size() << '\n' << std::flush;
    std::string mode;
    std::size_t iterations;
    while (std::cin >> mode >> iterations) {
        const auto underscore = mode.find('_');
        const auto phase = mode.substr(0, underscore);
        std::vector<ibex::ir::ColumnRef> project;
        if (underscore != std::string::npos)
            project.push_back({.name = mode.substr(underscore + 1)});
        const auto* projection = project.empty() ? nullptr : &project;
        std::vector<double> times;
        std::expected<rt::Table, std::string> result;
        for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
            result = rt::Table{};
            std::optional<rt::FilterOutputLayout> layout;
            std::vector<std::size_t> chars;
            if (phase == "gather" || phase == "presize") {
                auto prepared = rt::build_filter_output_layout(input, projection);
                if (!prepared)
                    return 6;
                layout = std::move(*prepared);
                chars.assign(layout->output.columns.size(), 0);
                // Gather-only uses fresh storage, as in actual materialization.
                if (phase == "gather")
                    rt::presize_filter_output(layout->output, input, layout->src_of_dst,
                                              indices.size(), chars);
            }
            const auto start = Clock::now();
            if (phase == "query") {
                result = rt::interpret(**plan, tables);
            } else if (phase == "filter") {
                result = rt::filter_table_range(input, predicate, range, nullptr);
            } else if (phase == "mask") {
                auto actual = rt::compute_mask(predicate, input, nullptr, range);
                const auto end = Clock::now();
                if (!actual || actual->value.size() != mask.size() ||
                    !std::equal(actual->value.begin(), actual->value.end(), mask.begin()))
                    return 7;
                times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
                continue;
            } else if (phase == "selection") {
                auto actual = rt::compute_filter_selection(input, predicate, nullptr, range, 0);
                const auto end = Clock::now();
                if (!actual || actual->keep_words != selection->keep_words ||
                    actual->kept != indices.size())
                    return 7;
                times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
                continue;
            } else if (phase == "materialize") {
                result = rt::filter_table_selection(input, *selection, projection, range);
            } else if (phase == "presize") {
                rt::presize_filter_output(layout->output, input, layout->src_of_dst, indices.size(),
                                          chars);
            } else if (phase == "gather") {
                rt::gather_selection_into(layout->output, input, layout->src_of_dst, *selection,
                                          range, {});
            } else
                return 8;
            const auto end = Clock::now();
            times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
            if (phase == "gather")
                result = std::move(layout->output);
            if (phase == "presize" && layout->output.rows() != indices.size())
                return 9;
            if (!result)
                return 10;
        }
        if (phase != "presize" && phase != "mask" && phase != "selection")
            verify(*result);
        std::sort(times.begin(), times.end());
        std::cout << std::setprecision(10) << times[times.size() / 2] << '\n' << std::flush;
    }
}
