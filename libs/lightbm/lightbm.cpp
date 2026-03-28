#include "lightbm.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <cmath>
#include <cstddef>
#include <vector>

namespace ibex::lightbm {

auto fit(const runtime::Table& design_matrix, const std::string& response_col,
         std::int64_t iterations, double learning_rate)
    -> std::expected<runtime::Table, std::string> {
    if (iterations <= 0) {
        return std::unexpected("iterations must be > 0");
    }
    if (!(learning_rate > 0.0 && learning_rate <= 1.0)) {
        return std::unexpected("learning_rate must be in (0, 1]");
    }

    const auto* y_any = design_matrix.find(response_col);
    if (y_any == nullptr) {
        return std::unexpected("response column not found: " + response_col);
    }
    const auto* y = std::get_if<Column<double>>(y_any);
    if (y == nullptr) {
        return std::unexpected("response column must be Float64");
    }

    std::vector<std::string> terms;
    std::vector<const Column<double>*> x_cols;
    terms.reserve(design_matrix.columns.size());
    x_cols.reserve(design_matrix.columns.size());
    for (const auto& col : design_matrix.columns) {
        if (col.name == response_col) {
            continue;
        }
        const auto* x = std::get_if<Column<double>>(col.column);
        if (x == nullptr) {
            return std::unexpected("feature column '" + col.name + "' must be Float64");
        }
        terms.push_back(col.name);
        x_cols.push_back(x);
    }

    const std::size_t n = y->size();
    const std::size_t p = x_cols.size();
    if (p == 0) {
        return std::unexpected("no features provided");
    }

    std::vector<double> beta(p, 0.0);
    std::vector<double> pred(n, 0.0);

    // "LightBM": lightweight boosting-style stagewise additive fitting.
    // Each round picks the feature with highest |corr(residual, x_j)| and
    // updates only that coefficient.
    for (std::int64_t iter = 0; iter < iterations; ++iter) {
        std::size_t best_j = 0;
        double best_score = -1.0;
        double best_step = 0.0;

        for (std::size_t j = 0; j < p; ++j) {
            double num = 0.0;
            double den = 0.0;
            for (std::size_t i = 0; i < n; ++i) {
                const double r = (*y)[i] - pred[i];
                const double x = (*(x_cols[j]))[i];
                num += x * r;
                den += x * x;
            }
            if (den <= 0.0) {
                continue;
            }
            const double step = learning_rate * (num / den);
            const double score = std::abs(num);
            if (score > best_score) {
                best_score = score;
                best_j = j;
                best_step = step;
            }
        }

        beta[best_j] += best_step;
        for (std::size_t i = 0; i < n; ++i) {
            pred[i] += best_step * (*(x_cols[best_j]))[i];
        }
    }

    runtime::Table out;
    out.add_column("term", Column<std::string>(terms));
    out.add_column("estimate", Column<double>(beta));
    return out;
}

}  // namespace ibex::lightbm

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("lightbm_version",
                             [](const ibex::runtime::ExternArgs&)
                                 -> std::expected<ibex::runtime::ExternValue, std::string> {
                                 runtime::Table out;
                                 out.add_column("name", Column<std::string>{"lightbm"});
                                 out.add_column("version", Column<std::string>{"0.1"});
                                 return ibex::runtime::ExternValue{std::move(out)};
                             });

    registry->register_scalar_table_consumer(
        "model_lightbm", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& design_matrix, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.empty()) {
                return std::unexpected(
                    "model_lightbm expects first scalar arg as response column name");
            }

            const auto* response = std::get_if<std::string>(&args[0]);
            if (response == nullptr) {
                return std::unexpected("model_lightbm: response column name must be a string");
            }

            if ((args.size() - 1) % 2 != 0) {
                return std::unexpected(
                    "model_lightbm: expected named parameter pairs after response (name, value, "
                    "...)");
            }

            std::int64_t iterations = 200;
            double learning_rate = 0.05;
            for (std::size_t i = 1; i < args.size(); i += 2) {
                const auto* key = std::get_if<std::string>(&args[i]);
                if (key == nullptr) {
                    return std::unexpected("model_lightbm: parameter names must be strings");
                }
                const auto& value = args[i + 1];
                if (*key == "iterations") {
                    if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                        iterations = *iv;
                    } else if (const auto* dv = std::get_if<double>(&value)) {
                        iterations = static_cast<std::int64_t>(*dv);
                    } else {
                        return std::unexpected("model_lightbm: iterations must be Int or Float64");
                    }
                } else if (*key == "learning_rate") {
                    if (const auto* dv = std::get_if<double>(&value)) {
                        learning_rate = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                        learning_rate = static_cast<double>(*iv);
                    } else {
                        return std::unexpected(
                            "model_lightbm: learning_rate must be Float64 or Int");
                    }
                }
            }

            auto table = ibex::lightbm::fit(design_matrix, *response, iterations, learning_rate);
            if (!table) {
                return std::unexpected(table.error());
            }
            return ibex::runtime::ExternValue{std::move(table.value())};
        });
}
