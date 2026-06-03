#include "lightgbm.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <cstddef>
#include <cstdint>
#include <LightGBM/c_api.h>
#include <string>
#include <vector>

namespace ibex::lightgbm {

namespace {

auto last_error(const char* context) -> std::string {
    std::string message = context;
    message += ": ";
    const char* err = LGBM_GetLastError();
    message += (err != nullptr) ? err : "unknown LightGBM error";
    return message;
}

}  // namespace

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
        const auto* x = std::get_if<Column<double>>(col.column.get());
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
    if (n == 0) {
        return std::unexpected("no rows to fit");
    }

    // Row-major design matrix for LGBM_DatasetCreateFromMat.
    std::vector<double> mat(n * p);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = 0; j < p; ++j) {
            mat[(i * p) + j] = (*x_cols[j])[i];
        }
    }

    // LightGBM label/weight fields are float32.
    std::vector<float> labels(n);
    for (std::size_t i = 0; i < n; ++i) {
        labels[i] = static_cast<float>((*y)[i]);
    }

    const std::string dataset_params = "max_bin=255";
    // Single-threaded and quiet to match Ibex's execution model. min_data_*=1
    // keeps small inputs splittable; learning_rate / iterations are user-driven.
    std::string booster_params =
        "objective=regression metric=l2 verbosity=-1 num_threads=1 "
        "min_data_in_leaf=1 min_data_in_bin=1 learning_rate=";
    booster_params += std::to_string(learning_rate);

    DatasetHandle dataset = nullptr;
    if (LGBM_DatasetCreateFromMat(mat.data(), C_API_DTYPE_FLOAT64, static_cast<std::int32_t>(n),
                                  static_cast<std::int32_t>(p), /*is_row_major=*/1,
                                  dataset_params.c_str(), /*reference=*/nullptr, &dataset) != 0) {
        return std::unexpected(last_error("LightGBM dataset"));
    }
    if (LGBM_DatasetSetField(dataset, "label", labels.data(), static_cast<int>(n),
                             C_API_DTYPE_FLOAT32) != 0) {
        LGBM_DatasetFree(dataset);
        return std::unexpected(last_error("LightGBM label"));
    }

    BoosterHandle booster = nullptr;
    if (LGBM_BoosterCreate(dataset, booster_params.c_str(), &booster) != 0) {
        LGBM_DatasetFree(dataset);
        return std::unexpected(last_error("LightGBM booster"));
    }

    for (std::int64_t it = 0; it < iterations; ++it) {
        int is_finished = 0;
        if (LGBM_BoosterUpdateOneIter(booster, &is_finished) != 0) {
            LGBM_BoosterFree(booster);
            LGBM_DatasetFree(dataset);
            return std::unexpected(last_error("LightGBM train"));
        }
        if (is_finished != 0) {
            break;  // no further splits possible
        }
    }

    std::vector<double> preds(n, 0.0);
    std::int64_t out_len = 0;
    if (LGBM_BoosterGetPredict(booster, /*data_idx=*/0, &out_len, preds.data()) != 0) {
        LGBM_BoosterFree(booster);
        LGBM_DatasetFree(dataset);
        return std::unexpected(last_error("LightGBM predict"));
    }

    std::vector<double> importance(p, 0.0);
    if (LGBM_BoosterFeatureImportance(booster, /*num_iteration=*/0, C_API_FEATURE_IMPORTANCE_GAIN,
                                      importance.data()) != 0) {
        LGBM_BoosterFree(booster);
        LGBM_DatasetFree(dataset);
        return std::unexpected(last_error("LightGBM importance"));
    }

    LGBM_BoosterFree(booster);
    LGBM_DatasetFree(dataset);

    // Long-format wire table: n fitted rows followed by p importance rows.
    std::vector<std::string> kind;
    std::vector<std::string> term;
    std::vector<double> value;
    kind.reserve(n + p);
    term.reserve(n + p);
    value.reserve(n + p);
    for (std::size_t i = 0; i < n; ++i) {
        kind.emplace_back("fitted");
        term.emplace_back("");
        value.push_back(preds[i]);
    }
    for (std::size_t j = 0; j < p; ++j) {
        kind.emplace_back("importance");
        term.push_back(terms[j]);
        value.push_back(importance[j]);
    }

    runtime::Table out;
    out.add_column("kind", Column<std::string>(kind));
    out.add_column("term", Column<std::string>(term));
    out.add_column("value", Column<double>(value));
    return out;
}

}  // namespace ibex::lightgbm

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("lightgbm_version",
                             [](const ibex::runtime::ExternArgs&)
                                 -> std::expected<ibex::runtime::ExternValue, std::string> {
                                 ibex::runtime::Table out;
                                 out.add_column("name", ibex::Column<std::string>{"lightgbm"});
                                 out.add_column("version", ibex::Column<std::string>{"4.5.0"});
                                 return ibex::runtime::ExternValue{std::move(out)};
                             });

    registry->register_scalar_table_consumer(
        "model_lightgbm", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& design_matrix, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.empty()) {
                return std::unexpected(
                    "model_lightgbm expects first scalar arg as response column name");
            }

            const auto* response = std::get_if<std::string>(&args[0]);
            if (response == nullptr) {
                return std::unexpected("model_lightgbm: response column name must be a string");
            }

            if ((args.size() - 1) % 2 != 0) {
                return std::unexpected(
                    "model_lightgbm: expected named parameter pairs after response (name, value, "
                    "...)");
            }

            std::int64_t iterations = 200;
            double learning_rate = 0.05;
            for (std::size_t i = 1; i < args.size(); i += 2) {
                const auto* key = std::get_if<std::string>(&args[i]);
                if (key == nullptr) {
                    return std::unexpected("model_lightgbm: parameter names must be strings");
                }
                const auto& value = args[i + 1];
                if (*key == "iterations") {
                    if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                        iterations = *iv;
                    } else if (const auto* dv = std::get_if<double>(&value)) {
                        iterations = static_cast<std::int64_t>(*dv);
                    } else {
                        return std::unexpected("model_lightgbm: iterations must be Int or Float64");
                    }
                } else if (*key == "learning_rate") {
                    if (const auto* dv = std::get_if<double>(&value)) {
                        learning_rate = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                        learning_rate = static_cast<double>(*iv);
                    } else {
                        return std::unexpected(
                            "model_lightgbm: learning_rate must be Float64 or Int");
                    }
                }
            }

            auto table = ibex::lightgbm::fit(design_matrix, *response, iterations, learning_rate);
            if (!table) {
                return std::unexpected(table.error());
            }
            return ibex::runtime::ExternValue{std::move(table.value())};
        });
}
