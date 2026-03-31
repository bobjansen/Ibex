#include <cmath>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "model_internal.hpp"

namespace ibex::runtime {

namespace {

/// Build a design matrix from a DataFrame using a model formula.
/// Returns column names and a row-major matrix (vector of column vectors).
/// Categorical/String columns are dummy-encoded (treatment coding: first level dropped when
/// intercept is present).
auto build_model_matrix(const Table& input, const ir::ModelFormula& formula)
    -> std::expected<std::pair<std::vector<std::string>, std::vector<std::vector<double>>>,
                     std::string> {
    const std::size_t n = input.rows();
    if (n == 0) {
        return std::unexpected("model: input table has no rows");
    }

    // Resolve which columns to include from the formula terms.
    std::vector<std::string> requested_columns;
    bool has_dot = false;
    for (const auto& term : formula.terms) {
        if (term.is_dot) {
            has_dot = true;
        } else {
            for (const auto& col : term.columns) {
                requested_columns.push_back(col);
            }
        }
    }

    // If `.` is used, add all columns except the response.
    if (has_dot) {
        for (const auto& entry : input.columns) {
            if (entry.name != formula.response) {
                bool already = false;
                for (const auto& r : requested_columns) {
                    if (r == entry.name) {
                        already = true;
                        break;
                    }
                }
                if (!already) {
                    requested_columns.push_back(entry.name);
                }
            }
        }
    }

    std::vector<std::string> col_names;
    std::vector<std::vector<double>> columns;

    if (formula.has_intercept) {
        col_names.push_back("(intercept)");
        columns.push_back(std::vector<double>(n, 1.0));
    }

    for (const auto& term : formula.terms) {
        if (term.is_dot) {
            for (const auto& entry : input.columns) {
                if (entry.name == formula.response) {
                    continue;
                }
                bool from_explicit = false;
                for (const auto& explicit_term : formula.terms) {
                    if (!explicit_term.is_dot && explicit_term.columns.size() == 1 &&
                        explicit_term.columns[0] == entry.name) {
                        from_explicit = true;
                        break;
                    }
                }
                if (from_explicit) {
                    continue;
                }

                auto ok = std::visit(
                    [&](const auto& c) -> bool {
                        using T = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<T, Column<double>>) {
                            std::vector<double> v(n);
                            for (std::size_t i = 0; i < n; ++i) {
                                v[i] = c[i];
                            }
                            col_names.push_back(entry.name);
                            columns.push_back(std::move(v));
                            return true;
                        } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                            std::vector<double> v(n);
                            for (std::size_t i = 0; i < n; ++i) {
                                v[i] = static_cast<double>(c[i]);
                            }
                            col_names.push_back(entry.name);
                            columns.push_back(std::move(v));
                            return true;
                        } else if constexpr (std::is_same_v<T, Column<std::string>>) {
                            std::vector<std::string> levels;
                            for (std::size_t i = 0; i < n; ++i) {
                                bool found = false;
                                for (const auto& lv : levels) {
                                    if (lv == c[i]) {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    levels.push_back(std::string(c[i]));
                                }
                            }
                            const std::size_t start = formula.has_intercept ? 1 : 0;
                            for (std::size_t li = start; li < levels.size(); ++li) {
                                std::vector<double> dummy(n, 0.0);
                                for (std::size_t i = 0; i < n; ++i) {
                                    if (c[i] == levels[li]) {
                                        dummy[i] = 1.0;
                                    }
                                }
                                col_names.push_back(entry.name + "_" + levels[li]);
                                columns.push_back(std::move(dummy));
                            }
                            return true;
                        } else {
                            return false;
                        }
                    },
                    *entry.column);
                if (!ok) {
                    return std::unexpected("model: column '" + entry.name +
                                           "' has unsupported type for model matrix");
                }
            }
            continue;
        }

        if (term.columns.size() == 1) {
            const auto& name = term.columns[0];
            const auto* entry = input.find_entry(name);
            if (entry == nullptr) {
                return std::unexpected("model: column not found: " + name);
            }
            auto ok = std::visit(
                [&](const auto& c) -> bool {
                    using T = std::decay_t<decltype(c)>;
                    if constexpr (std::is_same_v<T, Column<double>>) {
                        std::vector<double> v(n);
                        for (std::size_t i = 0; i < n; ++i) {
                            v[i] = c[i];
                        }
                        col_names.push_back(name);
                        columns.push_back(std::move(v));
                        return true;
                    } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                        std::vector<double> v(n);
                        for (std::size_t i = 0; i < n; ++i) {
                            v[i] = static_cast<double>(c[i]);
                        }
                        col_names.push_back(name);
                        columns.push_back(std::move(v));
                        return true;
                    } else if constexpr (std::is_same_v<T, Column<std::string>>) {
                        std::vector<std::string> levels;
                        for (std::size_t i = 0; i < n; ++i) {
                            bool found = false;
                            for (const auto& lv : levels) {
                                if (lv == c[i]) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                levels.push_back(std::string(c[i]));
                            }
                        }
                        const std::size_t start = formula.has_intercept ? 1 : 0;
                        for (std::size_t li = start; li < levels.size(); ++li) {
                            std::vector<double> dummy(n, 0.0);
                            for (std::size_t i = 0; i < n; ++i) {
                                if (std::string_view(c[i]) == levels[li]) {
                                    dummy[i] = 1.0;
                                }
                            }
                            col_names.push_back(name + "_" + levels[li]);
                            columns.push_back(std::move(dummy));
                        }
                        return true;
                    } else {
                        return false;
                    }
                },
                *entry->column);
            if (!ok) {
                return std::unexpected("model: column '" + name +
                                       "' has unsupported type for model matrix");
            }
        } else {
            std::vector<std::vector<double>> factors;
            std::string interaction_name;
            for (std::size_t fi = 0; fi < term.columns.size(); ++fi) {
                const auto& name = term.columns[fi];
                if (fi > 0) {
                    interaction_name += ":";
                }
                interaction_name += name;
                const auto* entry = input.find_entry(name);
                if (entry == nullptr) {
                    return std::unexpected("model: column not found: " + name);
                }
                std::vector<double> v(n);
                bool ok = std::visit(
                    [&](const auto& c) -> bool {
                        using T = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<T, Column<double>>) {
                            for (std::size_t i = 0; i < n; ++i) {
                                v[i] = c[i];
                            }
                            return true;
                        } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                            for (std::size_t i = 0; i < n; ++i) {
                                v[i] = static_cast<double>(c[i]);
                            }
                            return true;
                        } else {
                            return false;
                        }
                    },
                    *entry->column);
                if (!ok) {
                    return std::unexpected("model: interaction term requires numeric columns, '" +
                                           name + "' is not numeric");
                }
                factors.push_back(std::move(v));
            }
            std::vector<double> product(n, 1.0);
            for (const auto& factor : factors) {
                for (std::size_t i = 0; i < n; ++i) {
                    product[i] *= factor[i];
                }
            }
            col_names.push_back(interaction_name);
            columns.push_back(std::move(product));
        }
    }

    if (columns.empty()) {
        return std::unexpected("model: design matrix has no columns");
    }
    return std::make_pair(std::move(col_names), std::move(columns));
}

auto extract_response(const Table& input, const std::string& name)
    -> std::expected<std::vector<double>, std::string> {
    const auto* entry = input.find_entry(name);
    if (entry == nullptr) {
        return std::unexpected("model: response column not found: " + name);
    }
    const std::size_t n = input.rows();
    std::vector<double> y(n);
    bool ok = std::visit(
        [&](const auto& c) -> bool {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, Column<double>>) {
                for (std::size_t i = 0; i < n; ++i) {
                    y[i] = c[i];
                }
                return true;
            } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                for (std::size_t i = 0; i < n; ++i) {
                    y[i] = static_cast<double>(c[i]);
                }
                return true;
            } else {
                return false;
            }
        },
        *entry->column);
    if (!ok) {
        return std::unexpected("model: response column '" + name + "' must be numeric");
    }
    return y;
}

auto solve_spd(const std::vector<std::vector<double>>& a, const std::vector<double>& b)
    -> std::expected<std::vector<double>, std::string> {
    const std::size_t p = a.size();

    std::vector<std::vector<double>> l(p, std::vector<double>(p, 0.0));
    for (std::size_t j = 0; j < p; ++j) {
        double sum = 0.0;
        for (std::size_t k = 0; k < j; ++k) {
            sum += l[j][k] * l[j][k];
        }
        const double diag = a[j][j] - sum;
        if (diag <= 0.0) {
            return std::unexpected(
                "model: design matrix is rank-deficient (not positive definite)");
        }
        l[j][j] = std::sqrt(diag);
        for (std::size_t i = j + 1; i < p; ++i) {
            double s = 0.0;
            for (std::size_t k = 0; k < j; ++k) {
                s += l[i][k] * l[j][k];
            }
            l[i][j] = (a[j][i] - s) / l[j][j];
        }
    }

    std::vector<double> z(p);
    for (std::size_t i = 0; i < p; ++i) {
        double s = 0.0;
        for (std::size_t k = 0; k < i; ++k) {
            s += l[i][k] * z[k];
        }
        z[i] = (b[i] - s) / l[i][i];
    }

    std::vector<double> x(p);
    for (std::size_t i = p; i-- > 0;) {
        double s = 0.0;
        for (std::size_t k = i + 1; k < p; ++k) {
            s += l[k][i] * x[k];
        }
        x[i] = (z[i] - s) / l[i][i];
    }
    return x;
}

auto compute_xtx_xty(const std::vector<std::vector<double>>& x, const std::vector<double>& y,
                     std::size_t n, std::size_t p)
    -> std::pair<std::vector<std::vector<double>>, std::vector<double>> {
    std::vector<std::vector<double>> xtx(p, std::vector<double>(p, 0.0));
    std::vector<double> xty(p, 0.0);
    for (std::size_t a = 0; a < p; ++a) {
        for (std::size_t b = a; b < p; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < n; ++i) {
                s += x[a][i] * x[b][i];
            }
            xtx[a][b] = s;
            xtx[b][a] = s;
        }
        for (std::size_t i = 0; i < n; ++i) {
            xty[a] += x[a][i] * y[i];
        }
    }
    return {std::move(xtx), std::move(xty)};
}

auto build_model_result(const std::vector<std::string>& col_names,
                        const std::vector<std::vector<double>>& x, const std::vector<double>& y,
                        const std::vector<double>& beta, const ir::ModelFormula& formula,
                        const std::string& method) -> ModelResult {
    const std::size_t n = y.size();
    const std::size_t p = beta.size();

    std::vector<double> fitted(n, 0.0);
    std::vector<double> resid(n, 0.0);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = 0; j < p; ++j) {
            fitted[i] += x[j][i] * beta[j];
        }
        resid[i] = y[i] - fitted[i];
    }

    double y_mean = 0.0;
    for (double v : y) {
        y_mean += v;
    }
    y_mean /= static_cast<double>(n);
    double ss_tot = 0.0;
    double ss_res = 0.0;
    for (std::size_t i = 0; i < n; ++i) {
        ss_tot += (y[i] - y_mean) * (y[i] - y_mean);
        ss_res += resid[i] * resid[i];
    }
    const double r2 = (ss_tot > 0.0) ? 1.0 - (ss_res / ss_tot) : 0.0;
    const double adj_r2 =
        (n > p && ss_tot > 0.0)
            ? 1.0 - ((ss_res / static_cast<double>(n - p)) / (ss_tot / static_cast<double>(n - 1)))
            : 0.0;

    const double sigma2 = (n > p) ? ss_res / static_cast<double>(n - p) : 0.0;

    auto [xtx, unused_xty] = compute_xtx_xty(x, y, n, p);
    static_cast<void>(unused_xty);
    std::vector<double> std_errors(p, 0.0);
    for (std::size_t j = 0; j < p; ++j) {
        std::vector<double> unit(p, 0.0);
        unit[j] = 1.0;
        auto inv_col = solve_spd(xtx, unit);
        if (inv_col.has_value()) {
            std_errors[j] = std::sqrt(sigma2 * (*inv_col)[j]);
        }
    }

    Table coef_table;
    Column<std::string> term_col;
    Column<double> est_col;
    for (std::size_t j = 0; j < p; ++j) {
        term_col.push_back(col_names[j]);
        est_col.push_back(beta[j]);
    }
    coef_table.add_column("term", std::move(term_col));
    coef_table.add_column("estimate", std::move(est_col));

    Table summary_table;
    Column<std::string> summary_term;
    Column<double> summary_estimate;
    Column<double> summary_std_error;
    Column<double> summary_t_stat;
    Column<double> summary_p_value;
    for (std::size_t j = 0; j < p; ++j) {
        summary_term.push_back(col_names[j]);
        summary_estimate.push_back(beta[j]);
        summary_std_error.push_back(std_errors[j]);
        const double t = (std_errors[j] > 0.0) ? beta[j] / std_errors[j] : 0.0;
        summary_t_stat.push_back(t);
        summary_p_value.push_back(2.0 * std::erfc(std::abs(t) / std::sqrt(2.0)));
    }
    summary_table.add_column("term", std::move(summary_term));
    summary_table.add_column("estimate", std::move(summary_estimate));
    summary_table.add_column("std_error", std::move(summary_std_error));
    summary_table.add_column("t_stat", std::move(summary_t_stat));
    summary_table.add_column("p_value", std::move(summary_p_value));

    Table fitted_table;
    fitted_table.add_column("fitted", Column<double>(fitted));

    Table resid_table;
    resid_table.add_column("residual", Column<double>(resid));

    return ModelResult{
        .coefficients = std::move(coef_table),
        .summary = std::move(summary_table),
        .fitted_values = std::move(fitted_table),
        .residuals = std::move(resid_table),
        .formula = formula,
        .method = method,
        .r_squared = r2,
        .adj_r_squared = adj_r2,
        .n_obs = n,
        .n_params = p,
    };
}

}  // namespace

auto fit_model(const Table& input, const ir::ModelFormula& formula, const std::string& method,
               const std::vector<ir::ModelParamSpec>& params, const ScalarRegistry* /*scalars*/,
               const ExternRegistry* externs) -> std::expected<ModelResult, std::string> {
    auto matrix = build_model_matrix(input, formula);
    if (!matrix) {
        return std::unexpected(matrix.error());
    }
    auto& [col_names, x] = matrix.value();
    const std::size_t n = input.rows();
    const std::size_t p = col_names.size();

    auto y = extract_response(input, formula.response);
    if (!y) {
        return std::unexpected(y.error());
    }

    if (n <= p) {
        return std::unexpected("model: need more observations (" + std::to_string(n) +
                               ") than parameters (" + std::to_string(p) + ")");
    }

    auto [xtx, xty] = compute_xtx_xty(x, y.value(), n, p);

    if (method == "ols") {
        auto beta = solve_spd(xtx, xty);
        if (!beta) {
            return std::unexpected(beta.error());
        }
        return build_model_result(col_names, x, y.value(), beta.value(), formula, method);
    }

    if (method == "ridge") {
        double lambda = 1.0;
        for (const auto& param : params) {
            if (param.name == "lambda") {
                if (const auto* lit = std::get_if<ir::Literal>(&param.value.node)) {
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        lambda = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        lambda = static_cast<double>(*iv);
                    }
                }
            }
        }
        for (std::size_t j = 0; j < p; ++j) {
            if (formula.has_intercept && j == 0) {
                continue;
            }
            xtx[j][j] += lambda;
        }
        auto beta = solve_spd(xtx, xty);
        if (!beta) {
            return std::unexpected(beta.error());
        }
        return build_model_result(col_names, x, y.value(), beta.value(), formula, method);
    }

    if (method == "wls") {
        std::string weights_col;
        for (const auto& param : params) {
            if (param.name == "weights") {
                if (const auto* ref = std::get_if<ir::ColumnRef>(&param.value.node)) {
                    weights_col = ref->name;
                }
            }
        }
        if (weights_col.empty()) {
            return std::unexpected("wls: requires weights parameter (e.g. weights = w)");
        }
        auto w_result = extract_response(input, weights_col);
        if (!w_result) {
            return std::unexpected("wls: " + w_result.error());
        }
        const auto& w = w_result.value();

        std::vector<std::vector<double>> xtwx(p, std::vector<double>(p, 0.0));
        std::vector<double> xtwy(p, 0.0);
        for (std::size_t a = 0; a < p; ++a) {
            for (std::size_t b = a; b < p; ++b) {
                double s = 0.0;
                for (std::size_t i = 0; i < n; ++i) {
                    s += w[i] * x[a][i] * x[b][i];
                }
                xtwx[a][b] = s;
                xtwx[b][a] = s;
            }
            for (std::size_t i = 0; i < n; ++i) {
                xtwy[a] += w[i] * x[a][i] * y.value()[i];
            }
        }
        auto beta = solve_spd(xtwx, xtwy);
        if (!beta) {
            return std::unexpected(beta.error());
        }
        return build_model_result(col_names, x, y.value(), beta.value(), formula, method);
    }

    if (externs != nullptr) {
        const std::string fn_name = "model_" + method;
        const auto* fn = externs->find(fn_name);
        if (fn != nullptr) {
            if (!fn->first_arg_is_table || !fn->table_consumer_func) {
                return std::unexpected("model: plugin method '" + fn_name +
                                       "' must be registered as a table consumer");
            }

            Table design_table;
            for (std::size_t j = 0; j < p; ++j) {
                design_table.add_column(col_names[j], Column<double>(x[j]));
            }
            design_table.add_column("__response", Column<double>(y.value()));

            ExternArgs plugin_args;
            plugin_args.emplace_back(ScalarValue{std::string("__response")});
            for (const auto& param : params) {
                if (param.name == "method") {
                    continue;
                }
                plugin_args.emplace_back(ScalarValue{param.name});
                if (const auto* lit = std::get_if<ir::Literal>(&param.value.node)) {
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*dv});
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*iv});
                    } else if (const auto* sv = std::get_if<std::string>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*sv});
                    } else if (const auto* date = std::get_if<Date>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*date});
                    } else if (const auto* ts = std::get_if<Timestamp>(&lit->value)) {
                        plugin_args.emplace_back(ScalarValue{*ts});
                    } else {
                        return std::unexpected("model: unsupported literal parameter type for '" +
                                               param.name + "'");
                    }
                } else if (const auto* ref = std::get_if<ir::ColumnRef>(&param.value.node)) {
                    plugin_args.emplace_back(ScalarValue{std::string("column:") + ref->name});
                } else {
                    return std::unexpected("model: plugin parameter '" + param.name +
                                           "' must be a literal or column reference");
                }
            }

            auto plugin_result = fn->table_consumer_func(design_table, plugin_args);
            if (!plugin_result) {
                return std::unexpected("model (" + method + "): " + plugin_result.error());
            }

            const auto* coef_table = std::get_if<Table>(&plugin_result.value());
            if (coef_table == nullptr) {
                return std::unexpected("model (" + method +
                                       "): plugin must return a coefficients table");
            }
            const auto* term_any = coef_table->find("term");
            const auto* estimate_any = coef_table->find("estimate");
            if (term_any == nullptr || estimate_any == nullptr) {
                return std::unexpected("model (" + method +
                                       "): coefficients table must include 'term' and 'estimate'");
            }
            const auto* term_col = std::get_if<Column<std::string>>(term_any);
            const auto* estimate_col = std::get_if<Column<double>>(estimate_any);
            if (term_col == nullptr || estimate_col == nullptr) {
                return std::unexpected(
                    "model (" + method +
                    "): coefficient columns must be term:String and estimate:Float64");
            }
            if (term_col->size() != estimate_col->size()) {
                return std::unexpected("model (" + method + "): coefficients length mismatch");
            }

            std::unordered_map<std::string, double> beta_by_term;
            beta_by_term.reserve(term_col->size());
            for (std::size_t i = 0; i < term_col->size(); ++i) {
                beta_by_term.insert_or_assign(std::string((*term_col)[i]), (*estimate_col)[i]);
            }

            std::vector<double> beta;
            beta.reserve(col_names.size());
            for (const auto& name : col_names) {
                auto it = beta_by_term.find(name);
                if (it == beta_by_term.end()) {
                    return std::unexpected("model (" + method +
                                           "): missing coefficient for term '" + name + "'");
                }
                beta.push_back(it->second);
            }
            return build_model_result(col_names, x, y.value(), beta, formula, method);
        }
    }

    return std::unexpected("model: unknown method '" + method +
                           "' (supported built-ins: ols, ridge, wls; plugins: model_<method>)");
}

}  // namespace ibex::runtime
