#include "kmeans.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace ibex::kmeans {

namespace {

// Trained model: k centroids in p-dimensional feature space (row-major).
struct KMeansModel {
    std::size_t k = 0;
    std::size_t p = 0;
    std::vector<double> centroids;  // k * p
};

struct Design {
    std::vector<std::string> terms;  // feature names, length p
    std::vector<double> data;        // n * p, row-major
    std::size_t n = 0;
    std::size_t p = 0;
};

auto build_design(const runtime::Table& table) -> std::expected<Design, std::string> {
    std::vector<const Column<double>*> cols;
    cols.reserve(table.columns.size());
    Design out;
    out.terms.reserve(table.columns.size());
    for (const auto& col : table.columns) {
        const auto* x = std::get_if<Column<double>>(col.column.get());
        if (x == nullptr) {
            return std::unexpected("feature column '" + col.name + "' must be Float64");
        }
        out.terms.push_back(col.name);
        cols.push_back(x);
    }
    out.p = cols.size();
    if (out.p == 0) {
        return std::unexpected("no feature columns");
    }
    out.n = cols.front()->size();
    out.data.resize(out.n * out.p);
    for (std::size_t i = 0; i < out.n; ++i) {
        for (std::size_t j = 0; j < out.p; ++j) {
            out.data[(i * out.p) + j] = (*cols[j])[i];
        }
    }
    return out;
}

// Index of the nearest centroid to `row` by squared Euclidean distance.
auto nearest(const double* row, const std::vector<double>& centroids, std::size_t k, std::size_t p)
    -> std::size_t {
    std::size_t best = 0;
    double best_dist = std::numeric_limits<double>::max();
    for (std::size_t c = 0; c < k; ++c) {
        double dist = 0.0;
        for (std::size_t j = 0; j < p; ++j) {
            const double diff = row[j] - centroids[(c * p) + j];
            dist += diff * diff;
        }
        if (dist < best_dist) {
            best_dist = dist;
            best = c;
        }
    }
    return best;
}

auto fit_kmeans(const runtime::Table& design_matrix, const std::string& /*response_col*/,
                const runtime::ModelParams& params)
    -> std::expected<runtime::FittedModel, std::string> {
    std::int64_t k = 3;
    std::int64_t max_iter = 20;
    for (const auto& [name, value] : params) {
        if (name == "k") {
            if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                k = *iv;
            } else if (const auto* dv = std::get_if<double>(&value)) {
                k = static_cast<std::int64_t>(*dv);
            } else {
                return std::unexpected("k must be Int or Float64");
            }
        } else if (name == "max_iter" || name == "iterations") {
            if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                max_iter = *iv;
            } else if (const auto* dv = std::get_if<double>(&value)) {
                max_iter = static_cast<std::int64_t>(*dv);
            } else {
                return std::unexpected("max_iter must be Int or Float64");
            }
        }
    }
    if (k <= 0) {
        return std::unexpected("k must be > 0");
    }
    if (max_iter <= 0) {
        return std::unexpected("max_iter must be > 0");
    }

    auto design = build_design(design_matrix);
    if (!design) {
        return std::unexpected(design.error());
    }
    const std::size_t n = design->n;
    const std::size_t p = design->p;
    if (n == 0) {
        return std::unexpected("no rows to cluster");
    }
    if (static_cast<std::size_t>(k) > n) {
        return std::unexpected("k must be <= number of rows");
    }
    const std::size_t kk = static_cast<std::size_t>(k);

    // Deterministic initialization: evenly spaced rows as initial centroids.
    std::vector<double> centroids(kk * p, 0.0);
    for (std::size_t c = 0; c < kk; ++c) {
        const std::size_t idx = (c * n) / kk;
        for (std::size_t j = 0; j < p; ++j) {
            centroids[(c * p) + j] = design->data[(idx * p) + j];
        }
    }

    // Lloyd's algorithm.
    std::vector<std::size_t> assignment(n, 0);
    for (std::int64_t it = 0; it < max_iter; ++it) {
        bool changed = false;
        for (std::size_t i = 0; i < n; ++i) {
            const std::size_t a = nearest(&design->data[i * p], centroids, kk, p);
            if (a != assignment[i]) {
                assignment[i] = a;
                changed = true;
            }
        }

        std::vector<double> sums(kk * p, 0.0);
        std::vector<std::size_t> counts(kk, 0);
        for (std::size_t i = 0; i < n; ++i) {
            const std::size_t a = assignment[i];
            counts[a]++;
            for (std::size_t j = 0; j < p; ++j) {
                sums[(a * p) + j] += design->data[(i * p) + j];
            }
        }
        for (std::size_t c = 0; c < kk; ++c) {
            if (counts[c] == 0) {
                continue;  // keep the previous centroid for an empty cluster
            }
            for (std::size_t j = 0; j < p; ++j) {
                centroids[(c * p) + j] = sums[(c * p) + j] / static_cast<double>(counts[c]);
            }
        }

        if (!changed && it > 0) {
            break;  // converged
        }
    }

    auto model = std::make_shared<KMeansModel>();
    model->k = kk;
    model->p = p;
    model->centroids = std::move(centroids);

    Column<double> clusters;
    clusters.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        clusters.push_back(static_cast<double>(assignment[i]));
    }
    runtime::Table fitted;
    fitted.add_column("fitted", std::move(clusters));

    // Summary: the centroids — one row per cluster, a column per feature.
    runtime::Table summary;
    Column<std::int64_t> cluster_ids;
    cluster_ids.reserve(kk);
    for (std::size_t c = 0; c < kk; ++c) {
        cluster_ids.push_back(static_cast<std::int64_t>(c));
    }
    summary.add_column("cluster", std::move(cluster_ids));
    for (std::size_t j = 0; j < p; ++j) {
        Column<double> coord;
        coord.reserve(kk);
        for (std::size_t c = 0; c < kk; ++c) {
            coord.push_back(model->centroids[(c * p) + j]);
        }
        summary.add_column(design->terms[j], std::move(coord));
    }

    return runtime::FittedModel{
        .native = std::shared_ptr<void>(std::move(model)),
        .fitted = std::move(fitted),
        .importance = {},  // unsupervised: no feature importance
        .summary = std::move(summary),
    };
}

auto predict_kmeans(const void* native, const runtime::Table& design_matrix)
    -> std::expected<runtime::Table, std::string> {
    if (native == nullptr) {
        return std::unexpected("null model handle");
    }
    const auto* model = static_cast<const KMeansModel*>(native);

    auto design = build_design(design_matrix);
    if (!design) {
        return std::unexpected(design.error());
    }
    if (design->p != model->p) {
        return std::unexpected("kmeans predict: feature count does not match the trained model");
    }

    Column<double> clusters;
    clusters.reserve(design->n);
    for (std::size_t i = 0; i < design->n; ++i) {
        clusters.push_back(static_cast<double>(
            nearest(&design->data[i * design->p], model->centroids, model->k, model->p)));
    }
    runtime::Table out;
    out.add_column("prediction", std::move(clusters));
    return out;
}

}  // namespace

}  // namespace ibex::kmeans

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_model("kmeans", ibex::runtime::ModelOps{
                                           .fit = ibex::kmeans::fit_kmeans,
                                           .predict = ibex::kmeans::predict_kmeans,
                                       });
}
