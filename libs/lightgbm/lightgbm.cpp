#include "lightgbm.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <LightGBM/c_api.h>
#include <memory>
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

// Collect the Float64 feature columns (every column except `response_col`,
// preserving order) into a row-major matrix for the C API.
struct DesignMatrix {
    std::vector<std::string> terms;
    std::vector<double> data;  // row-major, n * p
    std::size_t n = 0;
    std::size_t p = 0;
};

auto build_design(const runtime::Table& design_matrix, const std::string* response_col)
    -> std::expected<DesignMatrix, std::string> {
    DesignMatrix out;
    std::vector<const Column<double>*> x_cols;
    out.terms.reserve(design_matrix.columns.size());
    x_cols.reserve(design_matrix.columns.size());
    for (const auto& col : design_matrix.columns) {
        if (response_col != nullptr && col.name == *response_col) {
            continue;
        }
        const auto* x = std::get_if<Column<double>>(col.column.get());
        if (x == nullptr) {
            return std::unexpected("feature column '" + col.name + "' must be Float64");
        }
        out.terms.push_back(col.name);
        x_cols.push_back(x);
    }

    out.p = x_cols.size();
    if (out.p == 0) {
        return std::unexpected("no feature columns");
    }
    out.n = x_cols.front()->size();
    out.data.resize(out.n * out.p);
    for (std::size_t i = 0; i < out.n; ++i) {
        for (std::size_t j = 0; j < out.p; ++j) {
            out.data[(i * out.p) + j] = (*x_cols[j])[i];
        }
    }
    return out;
}

auto fit_lightgbm(const runtime::Table& design_matrix, const std::string& response_col,
                  const runtime::ModelParams& params)
    -> std::expected<runtime::FittedModel, std::string> {
    std::int64_t iterations = 200;
    double learning_rate = 0.05;
    for (const auto& [name, value] : params) {
        if (name == "iterations") {
            if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                iterations = *iv;
            } else if (const auto* dv = std::get_if<double>(&value)) {
                iterations = static_cast<std::int64_t>(*dv);
            } else {
                return std::unexpected("iterations must be Int or Float64");
            }
        } else if (name == "learning_rate") {
            if (const auto* dv = std::get_if<double>(&value)) {
                learning_rate = *dv;
            } else if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                learning_rate = static_cast<double>(*iv);
            } else {
                return std::unexpected("learning_rate must be Float64 or Int");
            }
        }
    }
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

    auto design = build_design(design_matrix, &response_col);
    if (!design) {
        return std::unexpected(design.error());
    }
    const std::size_t n = design->n;
    const std::size_t p = design->p;
    if (n == 0) {
        return std::unexpected("no rows to fit");
    }
    if (y->size() != n) {
        return std::unexpected("response/feature row count mismatch");
    }

    // LightGBM label fields are float32.
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
    if (LGBM_DatasetCreateFromMat(design->data.data(), C_API_DTYPE_FLOAT64,
                                  static_cast<std::int32_t>(n), static_cast<std::int32_t>(p),
                                  /*is_row_major=*/1, dataset_params.c_str(),
                                  /*reference=*/nullptr, &dataset) != 0) {
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

    // The training dataset is no longer needed; prediction on new data uses
    // LGBM_BoosterPredictForMat, which does not reference it. Keep the booster
    // alive in a self-freeing handle owned by the runtime's ModelResult.
    LGBM_DatasetFree(dataset);
    std::shared_ptr<void> native(booster, [](void* handle) {
        if (handle != nullptr) {
            LGBM_BoosterFree(handle);
        }
    });

    runtime::Table fitted;
    fitted.add_column("fitted", Column<double>(preds));
    runtime::Table importance_table;
    importance_table.add_column("term", Column<std::string>(design->terms));
    importance_table.add_column("gain", Column<double>(importance));

    return runtime::FittedModel{
        .native = std::move(native),
        .fitted = std::move(fitted),
        .importance = std::move(importance_table),
        .summary = {},  // gains already carry the per-feature summary
    };
}

auto predict_lightgbm(const void* native, const runtime::Table& design_matrix)
    -> std::expected<runtime::Table, std::string> {
    if (native == nullptr) {
        return std::unexpected("null model handle");
    }
    auto design = build_design(design_matrix, /*response_col=*/nullptr);
    if (!design) {
        return std::unexpected(design.error());
    }
    const std::size_t n = design->n;
    const std::size_t p = design->p;

    std::vector<double> preds(n, 0.0);
    std::int64_t out_len = 0;
    // The booster is logically const for prediction, but the C API takes a
    // non-const handle.
    auto* booster = const_cast<void*>(native);  // NOLINT(cppcoreguidelines-pro-type-const-cast)
    if (LGBM_BoosterPredictForMat(booster, design->data.data(), C_API_DTYPE_FLOAT64,
                                  static_cast<std::int32_t>(n), static_cast<std::int32_t>(p),
                                  /*is_row_major=*/1, C_API_PREDICT_NORMAL, /*start_iteration=*/0,
                                  /*num_iteration=*/-1, "num_threads=1", &out_len,
                                  preds.data()) != 0) {
        return std::unexpected(last_error("LightGBM predict"));
    }
    if (static_cast<std::size_t>(out_len) != n) {
        return std::unexpected("LightGBM predict: unexpected output length");
    }

    runtime::Table out;
    out.add_column("prediction", Column<double>(preds));
    return out;
}

}  // namespace

}  // namespace ibex::lightgbm

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("lightgbm_version",
                             [](const ibex::runtime::ExternArgs&)
                                 -> std::expected<ibex::runtime::ExternValue, std::string> {
                                 ibex::runtime::Table out;
                                 out.add_column("name", ibex::Column<std::string>{"lightgbm"});
                                 out.add_column("version", ibex::Column<std::string>{"4.5.0"});
                                 return ibex::runtime::ExternValue{std::move(out)};
                             });

    registry->register_model("lightgbm", ibex::runtime::ModelOps{
                                             .fit = ibex::lightgbm::fit_lightgbm,
                                             .predict = ibex::lightgbm::predict_lightgbm,
                                         });
}
