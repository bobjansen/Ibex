#include "pca.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

namespace ibex::pca {

namespace {

// Trained model: project a centered p-vector onto k principal components.
struct PCAModel {
    std::size_t k = 0;
    std::size_t p = 0;
    std::vector<double> means;       // length p
    std::vector<double> components;  // k * p, row-major (row j = j-th component)
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

// Cyclic Jacobi eigendecomposition of a symmetric p x p matrix (row-major).
// On return evals[j] / evecs column j are the eigenpairs (evecs row-major).
void jacobi_eigen(std::vector<double> a, std::size_t p, std::vector<double>& evals,
                  std::vector<double>& evecs) {
    evecs.assign(p * p, 0.0);
    for (std::size_t i = 0; i < p; ++i) {
        evecs[(i * p) + i] = 1.0;
    }
    for (int sweep = 0; sweep < 100; ++sweep) {
        double off = 0.0;
        for (std::size_t q = 0; q < p; ++q) {
            for (std::size_t r = q + 1; r < p; ++r) {
                off += a[(q * p) + r] * a[(q * p) + r];
            }
        }
        if (off < 1e-24) {
            break;
        }
        for (std::size_t q = 0; q < p; ++q) {
            for (std::size_t r = q + 1; r < p; ++r) {
                const double apq = a[(q * p) + r];
                if (std::abs(apq) < 1e-300) {
                    continue;
                }
                const double theta = 0.5 * std::atan2(2.0 * apq, a[(q * p) + q] - a[(r * p) + r]);
                const double c = std::cos(theta);
                const double s = std::sin(theta);
                // a = J^T a J, rotating in the (q, r) plane.
                for (std::size_t i = 0; i < p; ++i) {
                    const double aiq = a[(i * p) + q];
                    const double air = a[(i * p) + r];
                    a[(i * p) + q] = (c * aiq) + (s * air);
                    a[(i * p) + r] = (-s * aiq) + (c * air);
                }
                for (std::size_t i = 0; i < p; ++i) {
                    const double aqi = a[(q * p) + i];
                    const double ari = a[(r * p) + i];
                    a[(q * p) + i] = (c * aqi) + (s * ari);
                    a[(r * p) + i] = (-s * aqi) + (c * ari);
                }
                for (std::size_t i = 0; i < p; ++i) {
                    const double viq = evecs[(i * p) + q];
                    const double vir = evecs[(i * p) + r];
                    evecs[(i * p) + q] = (c * viq) + (s * vir);
                    evecs[(i * p) + r] = (-s * viq) + (c * vir);
                }
            }
        }
    }
    evals.resize(p);
    for (std::size_t i = 0; i < p; ++i) {
        evals[i] = a[(i * p) + i];
    }
}

// Project the n x p centered data onto the k components into an n x k score
// table with columns pc1..pck.
auto score_table(const std::vector<double>& centered, std::size_t n, std::size_t p,
                 const std::vector<double>& components, std::size_t k) -> runtime::Table {
    runtime::Table out;
    for (std::size_t j = 0; j < k; ++j) {
        Column<double> pc;
        pc.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            double s = 0.0;
            for (std::size_t d = 0; d < p; ++d) {
                s += centered[(i * p) + d] * components[(j * p) + d];
            }
            pc.push_back(s);
        }
        out.add_column("pc" + std::to_string(j + 1), std::move(pc));
    }
    return out;
}

auto fit_pca(const runtime::Table& design_matrix, const std::string& /*response_col*/,
             const runtime::ModelParams& params)
    -> std::expected<runtime::FittedModel, std::string> {
    std::int64_t k = 2;
    for (const auto& [name, value] : params) {
        if (name == "k" || name == "components") {
            if (const auto* iv = std::get_if<std::int64_t>(&value)) {
                k = *iv;
            } else if (const auto* dv = std::get_if<double>(&value)) {
                k = static_cast<std::int64_t>(*dv);
            } else {
                return std::unexpected("k must be Int or Float64");
            }
        }
    }
    if (k <= 0) {
        return std::unexpected("k must be > 0");
    }

    auto design = build_design(design_matrix);
    if (!design) {
        return std::unexpected(design.error());
    }
    const std::size_t n = design->n;
    const std::size_t p = design->p;
    if (n < 2) {
        return std::unexpected("need at least 2 rows for PCA");
    }
    if (static_cast<std::size_t>(k) > p) {
        return std::unexpected("k must be <= number of features");
    }
    const std::size_t kk = static_cast<std::size_t>(k);

    // Center the columns; remember the means for prediction.
    std::vector<double> means(p, 0.0);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = 0; j < p; ++j) {
            means[j] += design->data[(i * p) + j];
        }
    }
    for (std::size_t j = 0; j < p; ++j) {
        means[j] /= static_cast<double>(n);
    }
    std::vector<double> centered(n * p);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = 0; j < p; ++j) {
            centered[(i * p) + j] = design->data[(i * p) + j] - means[j];
        }
    }

    // Covariance matrix (p x p), then its eigendecomposition.
    std::vector<double> cov(p * p, 0.0);
    for (std::size_t a = 0; a < p; ++a) {
        for (std::size_t b = a; b < p; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < n; ++i) {
                s += centered[(i * p) + a] * centered[(i * p) + b];
            }
            s /= static_cast<double>(n - 1);
            cov[(a * p) + b] = s;
            cov[(b * p) + a] = s;
        }
    }
    std::vector<double> evals;
    std::vector<double> evecs;  // p x p, column j = eigenvector j
    jacobi_eigen(cov, p, evals, evecs);

    // Order components by descending eigenvalue.
    std::vector<std::size_t> order(p);
    std::iota(order.begin(), order.end(), std::size_t{0});
    std::sort(order.begin(), order.end(),
              [&](std::size_t lhs, std::size_t rhs) { return evals[lhs] > evals[rhs]; });

    // Pack the top-k components row-major (row j = j-th component over features),
    // with a deterministic sign: the largest-magnitude loading is made positive.
    std::vector<double> components(kk * p, 0.0);
    for (std::size_t j = 0; j < kk; ++j) {
        const std::size_t e = order[j];
        std::size_t arg_max = 0;
        double max_abs = -1.0;
        for (std::size_t d = 0; d < p; ++d) {
            const double v = evecs[(d * p) + e];
            if (std::abs(v) > max_abs) {
                max_abs = std::abs(v);
                arg_max = d;
            }
        }
        const double sign = (evecs[(arg_max * p) + e] < 0.0) ? -1.0 : 1.0;
        for (std::size_t d = 0; d < p; ++d) {
            components[(j * p) + d] = sign * evecs[(d * p) + e];
        }
    }

    auto model = std::make_shared<PCAModel>();
    model->k = kk;
    model->p = p;
    model->means = means;
    model->components = components;

    // In-sample scores (multi-column: pc1..pck).
    runtime::Table fitted = score_table(centered, n, p, components, kk);

    // Summary: loadings — one row per component, a column per feature, plus the
    // eigenvalue (variance explained by that component).
    runtime::Table summary;
    Column<std::int64_t> comp_ids;
    Column<double> variances;
    comp_ids.reserve(kk);
    variances.reserve(kk);
    for (std::size_t j = 0; j < kk; ++j) {
        comp_ids.push_back(static_cast<std::int64_t>(j + 1));
        variances.push_back(evals[order[j]]);
    }
    summary.add_column("component", std::move(comp_ids));
    summary.add_column("variance", std::move(variances));
    for (std::size_t d = 0; d < p; ++d) {
        Column<double> loading;
        loading.reserve(kk);
        for (std::size_t j = 0; j < kk; ++j) {
            loading.push_back(components[(j * p) + d]);
        }
        summary.add_column(design->terms[d], std::move(loading));
    }

    return runtime::FittedModel{
        .native = std::shared_ptr<void>(std::move(model)),
        .fitted = std::move(fitted),
        .importance = {},  // PCA: no per-feature importance (loadings are in summary)
        .summary = std::move(summary),
    };
}

auto predict_pca(const void* native, const runtime::Table& design_matrix)
    -> std::expected<runtime::Table, std::string> {
    if (native == nullptr) {
        return std::unexpected("null model handle");
    }
    const auto* model = static_cast<const PCAModel*>(native);

    auto design = build_design(design_matrix);
    if (!design) {
        return std::unexpected(design.error());
    }
    if (design->p != model->p) {
        return std::unexpected("pca predict: feature count does not match the trained model");
    }

    std::vector<double> centered(design->n * design->p);
    for (std::size_t i = 0; i < design->n; ++i) {
        for (std::size_t j = 0; j < design->p; ++j) {
            centered[(i * design->p) + j] = design->data[(i * design->p) + j] - model->means[j];
        }
    }
    return score_table(centered, design->n, design->p, model->components, model->k);
}

}  // namespace

}  // namespace ibex::pca

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_model("pca", ibex::runtime::ModelOps{
                                        .fit = ibex::pca::fit_pca,
                                        .predict = ibex::pca::predict_pca,
                                    });
}
