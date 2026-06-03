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
/// Returns column names and a column-major matrix (vector of column vectors).
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
        col_names.emplace_back("(intercept)");
        columns.emplace_back(n, 1.0);
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
                                std::string dummy_name = entry.name;
                                dummy_name += "_";
                                dummy_name += levels[li];
                                col_names.push_back(std::move(dummy_name));
                                columns.push_back(std::move(dummy));
                            }
                            return true;
                        } else {
                            return false;
                        }
                    },
                    *entry.column);
                if (!ok) {
                    std::string message = "model: column '";
                    message += entry.name;
                    message += "' has unsupported type for model matrix";
                    return std::unexpected(std::move(message));
                }
            }
            continue;
        }

        if (term.columns.size() == 1) {
            const auto& name = term.columns[0];
            const auto* entry = input.find_entry(name);
            if (entry == nullptr) {
                std::string message = "model: column not found: ";
                message += name;
                return std::unexpected(std::move(message));
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
                            std::string dummy_name = name;
                            dummy_name += "_";
                            dummy_name += levels[li];
                            col_names.push_back(std::move(dummy_name));
                            columns.push_back(std::move(dummy));
                        }
                        return true;
                    } else {
                        return false;
                    }
                },
                *entry->column);
            if (!ok) {
                std::string message = "model: column '";
                message += name;
                message += "' has unsupported type for model matrix";
                return std::unexpected(std::move(message));
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
                    std::string message = "model: column not found: ";
                    message += name;
                    return std::unexpected(std::move(message));
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
                    std::string message = "model: interaction term requires numeric columns, '";
                    message += name;
                    message += "' is not numeric";
                    return std::unexpected(std::move(message));
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
        std::string message = "model: response column not found: ";
        message += name;
        return std::unexpected(std::move(message));
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
        std::string message = "model: response column '";
        message += name;
        message += "' must be numeric";
        return std::unexpected(std::move(message));
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

auto compute_xtx_xty(const std::vector<std::vector<double>>& x, const std::vector<double>& y)
    -> std::pair<std::vector<std::vector<double>>, std::vector<double>> {
    auto p = x.size();
    auto n = y.size();
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

    auto [xtx, unused_xty] = compute_xtx_xty(x, y);
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
        .importance = {},  // linear model: no tree feature importances
        .native = {},      // linear model: no plugin-owned native handle
        .formula = formula,
        .method = method,
        .r_squared = r2,
        .adj_r_squared = adj_r2,
        .n_obs = n,
        .n_params = p,
    };
}

// Assembles a ModelResult from a model plugin's FittedModel. The plugin returns
// in-sample predictions, feature importances, and an opaque native handle; the
// runtime derives residuals and R² from the predictions and the response. Unlike
// build_model_result there is no linear-coefficient reconstruction — a tree model
// has none, and predictions come straight from the trained model.
auto assemble_plugin_model_result(FittedModel fitted, const std::vector<std::string>& col_names,
                                  const std::vector<double>& y, bool has_response,
                                  const ir::ModelFormula& formula, const std::string& method)
    -> std::expected<ModelResult, std::string> {
    const auto* fitted_col = std::get_if<Column<double>>(fitted.fitted.find("fitted"));
    if (fitted_col == nullptr) {
        return std::unexpected("model (" + method +
                               "): plugin fit() must return a 'fitted' Float64 column");
    }
    const std::size_t n = fitted_col->size();

    // Residuals and R² only make sense for supervised methods. Unsupervised
    // methods (e.g. kmeans) leave them empty/zero.
    Table resid_table;
    double r2 = 0.0;
    if (has_response) {
        if (y.size() != n) {
            return std::unexpected("model (" + method +
                                   "): plugin returned wrong number of fitted values");
        }
        std::vector<double> resid(n, 0.0);
        double y_mean = 0.0;
        for (double v : y) {
            y_mean += v;
        }
        y_mean /= (n > 0) ? static_cast<double>(n) : 1.0;
        double ss_tot = 0.0;
        double ss_res = 0.0;
        for (std::size_t i = 0; i < n; ++i) {
            resid[i] = y[i] - (*fitted_col)[i];
            ss_tot += (y[i] - y_mean) * (y[i] - y_mean);
            ss_res += resid[i] * resid[i];
        }
        r2 = (ss_tot > 0.0) ? 1.0 - (ss_res / ss_tot) : 0.0;
        resid_table.add_column("residual", Column<double>(resid));
    }

    return ModelResult{
        .coefficients = {},  // plugin model: no linear coefficients
        .summary = {},
        .fitted_values = std::move(fitted.fitted),
        .residuals = std::move(resid_table),
        .importance = std::move(fitted.importance),
        .native = std::move(fitted.native),
        .formula = formula,
        .method = method,
        .r_squared = r2,
        .adj_r_squared = 0.0,
        .n_obs = n,
        .n_params = col_names.size(),
    };
}

auto promote_numeric_scalar(const ScalarValue& value) -> std::optional<double> {
    if (const auto* dv = std::get_if<double>(&value)) {
        return *dv;
    }
    if (const auto* iv = std::get_if<std::int64_t>(&value)) {
        return static_cast<double>(*iv);
    }
    return std::nullopt;
}

auto eval_model_param_scalar(const ir::Expr& expr, const ScalarRegistry* scalars)
    -> std::expected<ScalarValue, std::string> {
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        return std::visit([](const auto& value) -> ScalarValue { return ScalarValue{value}; },
                          lit->value);
    }

    if (const auto* ref = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (scalars == nullptr) {
            std::string message = "unknown scalar binding '";
            message += ref->name;
            message += "'";
            return std::unexpected(std::move(message));
        }
        auto it = scalars->find(ref->name);
        if (it == scalars->end()) {
            std::string message = "unknown scalar binding '";
            message += ref->name;
            message += "'";
            return std::unexpected(std::move(message));
        }
        return it->second;
    }

    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = eval_model_param_scalar(*bin->left, scalars);
        if (!left.has_value()) {
            return std::unexpected(left.error());
        }
        auto right = eval_model_param_scalar(*bin->right, scalars);
        if (!right.has_value()) {
            return std::unexpected(right.error());
        }
        auto lhs = promote_numeric_scalar(*left);
        auto rhs = promote_numeric_scalar(*right);
        if (!lhs.has_value() || !rhs.has_value()) {
            return std::unexpected("model parameter arithmetic expects numeric scalar values");
        }

        const bool both_int = std::holds_alternative<std::int64_t>(*left) &&
                              std::holds_alternative<std::int64_t>(*right);
        switch (bin->op) {
            case ir::ArithmeticOp::Add:
                if (both_int) {
                    return ScalarValue{std::get<std::int64_t>(*left) +
                                       std::get<std::int64_t>(*right)};
                }
                return ScalarValue{*lhs + *rhs};
            case ir::ArithmeticOp::Sub:
                if (both_int) {
                    return ScalarValue{std::get<std::int64_t>(*left) -
                                       std::get<std::int64_t>(*right)};
                }
                return ScalarValue{*lhs - *rhs};
            case ir::ArithmeticOp::Mul:
                if (both_int) {
                    return ScalarValue{std::get<std::int64_t>(*left) *
                                       std::get<std::int64_t>(*right)};
                }
                return ScalarValue{*lhs * *rhs};
            case ir::ArithmeticOp::Div:
                return ScalarValue{*lhs / *rhs};
            case ir::ArithmeticOp::Mod:
                if (!both_int) {
                    return std::unexpected("model parameter modulo expects Int scalar values");
                }
                return ScalarValue{std::get<std::int64_t>(*left) % std::get<std::int64_t>(*right)};
        }
    }

    return std::unexpected(
        "model parameter must be a literal, scalar binding, or numeric scalar expression");
}

}  // namespace

auto fit_model(const Table& input, const ir::ModelFormula& formula, const std::string& method,
               const std::vector<ir::ModelParamSpec>& params, const ScalarRegistry* scalars,
               const ExternRegistry* externs) -> std::expected<ModelResult, std::string> {
    auto matrix = build_model_matrix(input, formula);
    if (!matrix) {
        return std::unexpected(matrix.error());
    }
    auto& [col_names, x] = matrix.value();
    const std::size_t n = input.rows();
    const std::size_t p = col_names.size();

    // The response is optional: unsupervised methods (e.g. kmeans) are written
    // `~ x1 + x2` with no response. Linear built-ins require one (checked below).
    const bool has_response = !formula.response.empty();
    std::optional<std::vector<double>> y;
    if (has_response) {
        auto response = extract_response(input, formula.response);
        if (!response) {
            return std::unexpected(response.error());
        }
        y = std::move(response.value());
    }

    // Model plugins (e.g. lightgbm, kmeans) own their fitted model and predict on
    // new data. They are dispatched here, before the linear-only observation/param
    // check and normal-equation setup below, which do not apply to them.
    if (externs != nullptr) {
        if (const auto* ops = externs->find_model(method)) {
            if (!ops->fit) {
                return std::unexpected("model: plugin method '" + method +
                                       "' has no fit implementation");
            }
            Table design;
            for (std::size_t j = 0; j < p; ++j) {
                design.add_column(col_names[j], Column<double>(x[j]));
            }
            if (has_response) {
                design.add_column("__response", Column<double>(*y));
            }

            ModelParams model_params;
            model_params.reserve(params.size());
            for (const auto& param : params) {
                if (param.name == "method") {
                    continue;
                }
                auto scalar = eval_model_param_scalar(param.value, scalars);
                if (!scalar.has_value()) {
                    return std::unexpected("model (" + method + "): parameter '" + param.name +
                                           "': " + scalar.error());
                }
                model_params.emplace_back(param.name, std::move(*scalar));
            }

            auto fitted = ops->fit(design, has_response ? std::string("__response") : std::string(),
                                   model_params);
            if (!fitted.has_value()) {
                return std::unexpected("model (" + method + "): " + fitted.error());
            }
            static const std::vector<double> kNoResponse;
            return assemble_plugin_model_result(std::move(*fitted), col_names,
                                                has_response ? *y : kNoResponse, has_response,
                                                formula, method);
        }
    }

    if (!has_response) {
        return std::unexpected("model: method '" + method +
                               "' requires a response (write `y ~ ...`); only plugin methods "
                               "support the response-less `~ ...` form");
    }

    if (n <= p) {
        std::string message = "model: need more observations (";
        message += std::to_string(n);
        message += ") than parameters (";
        message += std::to_string(p);
        message += ")";
        return std::unexpected(std::move(message));
    }

    auto [xtx, xty] = compute_xtx_xty(x, y.value());

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
                auto scalar = eval_model_param_scalar(param.value, scalars);
                if (!scalar.has_value()) {
                    std::string message = "ridge: ";
                    message += scalar.error();
                    return std::unexpected(std::move(message));
                }
                if (const auto numeric = promote_numeric_scalar(*scalar); numeric.has_value()) {
                    lambda = *numeric;
                } else {
                    return std::unexpected("ridge: lambda must be a numeric scalar");
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
            std::string message = "wls: ";
            message += w_result.error();
            return std::unexpected(std::move(message));
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

    std::string message = "model: unknown method '";
    message += method;
    message +=
        "' (built-ins: ols, ridge, wls; plugin methods are registered via register_model, "
        "e.g. import \"lightgbm\")";
    return std::unexpected(std::move(message));
}

auto predict_model(const ModelResult& model, const Table& newdata, const ExternRegistry& externs)
    -> std::expected<Table, std::string> {
    const auto* ops = externs.find_model(model.method);
    if (ops == nullptr || !ops->predict) {
        return std::unexpected("model_predict: method '" + model.method +
                               "' does not support prediction (not a plugin model)");
    }
    if (model.native == nullptr) {
        return std::unexpected("model_predict: model has no trained handle to predict with");
    }

    // Rebuild the design matrix from new data using the SAME formula, so the
    // feature columns line up with training (same order, intercept, encodings).
    auto matrix = build_model_matrix(newdata, model.formula);
    if (!matrix) {
        return std::unexpected(matrix.error());
    }
    auto& [col_names, x] = matrix.value();

    Table design;
    for (std::size_t j = 0; j < col_names.size(); ++j) {
        design.add_column(col_names[j], Column<double>(x[j]));
    }
    return ops->predict(model.native.get(), design);
}

}  // namespace ibex::runtime
