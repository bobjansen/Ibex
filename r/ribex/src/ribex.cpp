#define R_NO_REMAP

#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cmath>
#include <dlfcn.h>
#include <expected>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <R_ext/Arith.h>
#include <R_ext/Error.h>
#include <Rinternals.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

auto make_error(std::string_view stage, const std::string& message) -> std::string {
    return "ribex " + std::string(stage) + ": " + message;
}

auto plugin_stem(const std::string& source_path) -> std::string {
    return std::filesystem::path(source_path).stem().string();
}

enum class PluginLoadStatus : std::uint8_t { Loaded, NotFound, LoadError };

struct PluginLoadResult {
    PluginLoadStatus status;
    std::string message;
};

struct SessionState {
    ibex::runtime::TableRegistry tables;
    std::unordered_set<std::string> table_externs;
    std::unordered_set<std::string> sink_externs;
    ibex::runtime::ExternRegistry externs;
    std::unordered_set<std::string> loaded_plugins;
    std::vector<std::string> plugin_paths;
};

auto try_load_plugin(const std::string& stem, const std::vector<std::string>& search_paths,
                     std::unordered_set<std::string>& loaded_plugins,
                     ibex::runtime::ExternRegistry& externs) -> PluginLoadResult {
    if (loaded_plugins.contains(stem)) {
        return {PluginLoadStatus::Loaded, ""};
    }

    const std::string filename = stem + ".so";
    std::string last_error;
    std::string last_candidate;
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
        void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            if (std::filesystem::exists(full_path)) {
                if (const char* err = dlerror()) {
                    last_error = err;
                }
                last_candidate = full_path.string();
            }
            continue;
        }

        using RegisterFn = void (*)(ibex::runtime::ExternRegistry*);
        auto* fn = reinterpret_cast<RegisterFn>(dlsym(handle, "ibex_register"));
        if (fn == nullptr) {
            dlclose(handle);
            last_candidate = full_path.string();
            last_error = "missing ibex_register symbol";
            continue;
        }

        fn(&externs);
        loaded_plugins.insert(stem);
        return {PluginLoadStatus::Loaded, ""};
    }

    if (!last_candidate.empty()) {
        return {PluginLoadStatus::LoadError,
                "failed to load '" + last_candidate +
                    "': " + (last_error.empty() ? "unknown error" : last_error)};
    }

    return {PluginLoadStatus::NotFound, ""};
}

auto load_source_plugins(const ibex::parser::Program& program,
                         const std::vector<std::string>& plugin_search_paths,
                         ibex::runtime::ExternRegistry& externs)
    -> std::expected<void, std::string> {
    std::unordered_set<std::string> loaded_plugins;
    for (const auto& stmt : program.statements) {
        if (auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            if (decl->source_path.empty()) {
                continue;
            }
            auto stem = plugin_stem(decl->source_path);
            auto result = try_load_plugin(stem, plugin_search_paths, loaded_plugins, externs);
            if (result.status == PluginLoadStatus::NotFound) {
                return std::unexpected("could not find plugin '" + stem + ".so' in search path");
            }
            if (result.status == PluginLoadStatus::LoadError) {
                return std::unexpected(result.message);
            }
            continue;
        }

        if (std::holds_alternative<ibex::parser::ImportDecl>(stmt)) {
            return std::unexpected(
                "ribex does not yet support import declarations; use explicit extern fn "
                "declarations with plugin_paths");
        }
    }

    return {};
}

auto read_text_file(const std::string& path) -> std::expected<std::string, std::string> {
    std::ifstream in(path);
    if (!in.is_open()) {
        return std::unexpected(make_error("file error", "failed to open Ibex file '" + path + "'"));
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    if (!in.good() && !in.eof()) {
        return std::unexpected(make_error("file error", "failed to read Ibex file '" + path + "'"));
    }
    return buffer.str();
}

auto parse_plugin_paths(SEXP plugin_paths_sexp)
    -> std::expected<std::vector<std::string>, std::string> {
    std::vector<std::string> paths;
    if (plugin_paths_sexp == R_NilValue) {
        return paths;
    }
    if (TYPEOF(plugin_paths_sexp) != STRSXP) {
        return std::unexpected("'plugin_paths' must be a character vector");
    }

    const auto n = Rf_length(plugin_paths_sexp);
    paths.reserve(static_cast<std::size_t>(n));
    for (R_xlen_t i = 0; i < n; ++i) {
        SEXP elt = STRING_ELT(plugin_paths_sexp, i);
        if (elt == NA_STRING) {
            return std::unexpected("'plugin_paths' must not contain NA");
        }
        paths.emplace_back(CHAR(elt));
    }
    return paths;
}

auto named_list_name(SEXP names, R_xlen_t i, const char* what)
    -> std::expected<std::string, std::string> {
    if (names == R_NilValue || TYPEOF(names) != STRSXP || i >= Rf_length(names)) {
        return std::unexpected(std::string(what) + " must be a named list");
    }
    SEXP name = STRING_ELT(names, i);
    if (name == NA_STRING || std::string_view(CHAR(name)).empty()) {
        return std::unexpected(std::string(what) + " must not contain empty or NA names");
    }
    return std::string(CHAR(name));
}

auto parse_date_days(double days, const std::string& context)
    -> std::expected<ibex::Date, std::string> {
    if (ISNA(days)) {
        return std::unexpected(context + " must not be NA");
    }
    if (ISNAN(days) || !std::isfinite(days)) {
        return std::unexpected(context + " must be finite");
    }
    if (days < static_cast<double>(std::numeric_limits<std::int32_t>::min()) ||
        days > static_cast<double>(std::numeric_limits<std::int32_t>::max())) {
        return std::unexpected(context + " is out of range for Ibex Date");
    }
    return ibex::Date{static_cast<std::int32_t>(std::llround(days))};
}

auto parse_timestamp_seconds(double seconds, const std::string& context)
    -> std::expected<ibex::Timestamp, std::string> {
    if (ISNA(seconds)) {
        return std::unexpected(context + " must not be NA");
    }
    if (ISNAN(seconds) || !std::isfinite(seconds)) {
        return std::unexpected(context + " must be finite");
    }
    const long double nanos =
        static_cast<long double>(seconds) * static_cast<long double>(1000000000.0L);
    if (nanos < static_cast<long double>(std::numeric_limits<std::int64_t>::min()) ||
        nanos > static_cast<long double>(std::numeric_limits<std::int64_t>::max())) {
        return std::unexpected(context + " is out of range for Ibex Timestamp");
    }
    return ibex::Timestamp{static_cast<std::int64_t>(std::llround(nanos))};
}

auto build_scalar_from_r(SEXP value, const std::string& name)
    -> std::expected<ibex::runtime::ScalarValue, std::string> {
    if (value == R_NilValue || Rf_length(value) != 1) {
        return std::unexpected("scalar '" + name + "' must be a length-1 R value");
    }

    if (Rf_inherits(value, "POSIXct")) {
        if (TYPEOF(value) != REALSXP) {
            return std::unexpected("scalar '" + name + "' POSIXct value must be numeric");
        }
        auto ts = parse_timestamp_seconds(REAL(value)[0], "scalar '" + name + "'");
        if (!ts.has_value()) {
            return std::unexpected(ts.error());
        }
        return ibex::runtime::ScalarValue{*ts};
    }

    if (Rf_inherits(value, "Date")) {
        double raw = 0.0;
        if (TYPEOF(value) == REALSXP) {
            raw = REAL(value)[0];
        } else if (TYPEOF(value) == INTSXP) {
            if (INTEGER(value)[0] == NA_INTEGER) {
                return std::unexpected("scalar '" + name + "' must not be NA");
            }
            raw = static_cast<double>(INTEGER(value)[0]);
        } else {
            return std::unexpected("scalar '" + name + "' Date value must be numeric");
        }
        auto date = parse_date_days(raw, "scalar '" + name + "'");
        if (!date.has_value()) {
            return std::unexpected(date.error());
        }
        return ibex::runtime::ScalarValue{*date};
    }

    switch (TYPEOF(value)) {
        case LGLSXP:
            if (LOGICAL(value)[0] == NA_LOGICAL) {
                return std::unexpected("scalar '" + name + "' must not be NA");
            }
            return ibex::runtime::ScalarValue{LOGICAL(value)[0] != 0};
        case INTSXP:
            if (Rf_inherits(value, "factor")) {
                return std::unexpected("scalar '" + name +
                                       "': factor is not supported; convert to character");
            }
            if (INTEGER(value)[0] == NA_INTEGER) {
                return std::unexpected("scalar '" + name + "' must not be NA");
            }
            return ibex::runtime::ScalarValue{static_cast<std::int64_t>(INTEGER(value)[0])};
        case REALSXP:
            if (ISNA(REAL(value)[0])) {
                return std::unexpected("scalar '" + name + "' must not be NA");
            }
            return ibex::runtime::ScalarValue{REAL(value)[0]};
        case STRSXP:
            if (STRING_ELT(value, 0) == NA_STRING) {
                return std::unexpected("scalar '" + name + "' must not be NA");
            }
            return ibex::runtime::ScalarValue{std::string(CHAR(STRING_ELT(value, 0)))};
        default:
            return std::unexpected("scalar '" + name + "' has unsupported R type");
    }
}

auto build_scalar_registry_from_r(SEXP scalars_sexp)
    -> std::expected<ibex::runtime::ScalarRegistry, std::string> {
    ibex::runtime::ScalarRegistry registry;
    if (scalars_sexp == R_NilValue) {
        return registry;
    }
    if (TYPEOF(scalars_sexp) != VECSXP) {
        return std::unexpected("'scalars' must be a named list");
    }

    SEXP names = Rf_getAttrib(scalars_sexp, R_NamesSymbol);
    for (R_xlen_t i = 0; i < XLENGTH(scalars_sexp); ++i) {
        auto name = named_list_name(names, i, "'scalars'");
        if (!name.has_value()) {
            return std::unexpected(name.error());
        }
        auto scalar = build_scalar_from_r(VECTOR_ELT(scalars_sexp, i), *name);
        if (!scalar.has_value()) {
            return std::unexpected(scalar.error());
        }
        registry.insert_or_assign(*name, std::move(*scalar));
    }
    return registry;
}

template <typename T, typename PushFn>
auto build_column_with_validity(R_xlen_t size, PushFn&& push)
    -> std::pair<ibex::runtime::ColumnValue, std::optional<ibex::runtime::ValidityBitmap>> {
    ibex::Column<T> column;
    column.reserve(static_cast<std::size_t>(size));
    bool has_nulls = false;
    ibex::runtime::ValidityBitmap validity;

    auto mark_validity = [&](R_xlen_t i, bool valid) {
        if (!has_nulls && !valid) {
            has_nulls = true;
            validity.assign(static_cast<std::size_t>(size), true);
        }
        if (has_nulls) {
            validity.set(static_cast<std::size_t>(i), valid);
        }
    };

    for (R_xlen_t i = 0; i < size; ++i) {
        auto [value, valid] = push(i);
        column.push_back(std::move(value));
        mark_validity(i, valid);
    }

    return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                     has_nulls ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                               : std::nullopt};
}

auto build_column_from_r_vector(const std::string& name, SEXP column_sexp) -> std::expected<
    std::pair<ibex::runtime::ColumnValue, std::optional<ibex::runtime::ValidityBitmap>>,
    std::string> {
    const auto size = XLENGTH(column_sexp);
    try {
        if (Rf_inherits(column_sexp, "POSIXct")) {
            if (TYPEOF(column_sexp) != REALSXP) {
                return std::unexpected("column '" + name + "' POSIXct data must be numeric");
            }
            return build_column_with_validity<ibex::Timestamp>(size, [&](R_xlen_t i) {
                const double value = REAL(column_sexp)[i];
                if (ISNA(value)) {
                    return std::pair{ibex::Timestamp{}, false};
                }
                auto ts = parse_timestamp_seconds(value, "column '" + name + "'");
                if (!ts.has_value()) {
                    throw std::runtime_error(ts.error());
                }
                return std::pair{*ts, true};
            });
        }

        if (Rf_inherits(column_sexp, "Date")) {
            if (TYPEOF(column_sexp) != REALSXP && TYPEOF(column_sexp) != INTSXP) {
                return std::unexpected("column '" + name + "' Date data must be numeric");
            }
            return build_column_with_validity<ibex::Date>(size, [&](R_xlen_t i) {
                double value = 0.0;
                if (TYPEOF(column_sexp) == REALSXP) {
                    value = REAL(column_sexp)[i];
                    if (ISNA(value)) {
                        return std::pair{ibex::Date{}, false};
                    }
                } else {
                    if (INTEGER(column_sexp)[i] == NA_INTEGER) {
                        return std::pair{ibex::Date{}, false};
                    }
                    value = static_cast<double>(INTEGER(column_sexp)[i]);
                }
                auto date = parse_date_days(value, "column '" + name + "'");
                if (!date.has_value()) {
                    throw std::runtime_error(date.error());
                }
                return std::pair{*date, true};
            });
        }

        if (Rf_inherits(column_sexp, "factor")) {
            return std::unexpected("column '" + name +
                                   "': factor is not supported; convert to character");
        }

        switch (TYPEOF(column_sexp)) {
            case LGLSXP:
                return build_column_with_validity<bool>(size, [&](R_xlen_t i) {
                    const int value = LOGICAL(column_sexp)[i];
                    if (value == NA_LOGICAL) {
                        return std::pair{false, false};
                    }
                    return std::pair{value != 0, true};
                });
            case INTSXP:
                return build_column_with_validity<std::int64_t>(size, [&](R_xlen_t i) {
                    const int value = INTEGER(column_sexp)[i];
                    if (value == NA_INTEGER) {
                        return std::pair{std::int64_t{0}, false};
                    }
                    return std::pair{static_cast<std::int64_t>(value), true};
                });
            case REALSXP:
                return build_column_with_validity<double>(size, [&](R_xlen_t i) {
                    const double value = REAL(column_sexp)[i];
                    if (ISNA(value)) {
                        return std::pair{0.0, false};
                    }
                    return std::pair{value, true};
                });
            case STRSXP:
                return build_column_with_validity<std::string>(size, [&](R_xlen_t i) {
                    SEXP value = STRING_ELT(column_sexp, i);
                    if (value == NA_STRING) {
                        return std::pair{std::string{}, false};
                    }
                    return std::pair{std::string(CHAR(value)), true};
                });
            default:
                return std::unexpected("column '" + name + "' has unsupported R type");
        }
    } catch (const std::runtime_error& err) {
        return std::unexpected(err.what());
    }
}

auto build_runtime_table_from_r(SEXP table_obj)
    -> std::expected<ibex::runtime::Table, std::string> {
    if (!Rf_inherits(table_obj, "data.frame")) {
        return std::unexpected("unsupported table binding object; expected data.frame");
    }

    ibex::runtime::Table table;
    SEXP names = Rf_getAttrib(table_obj, R_NamesSymbol);
    std::optional<R_xlen_t> row_count;
    for (R_xlen_t i = 0; i < XLENGTH(table_obj); ++i) {
        auto name = named_list_name(names, i, "data.frame columns");
        if (!name.has_value()) {
            return std::unexpected(name.error());
        }
        SEXP column = VECTOR_ELT(table_obj, i);
        const auto size = XLENGTH(column);
        if (!row_count.has_value()) {
            row_count = size;
        } else if (*row_count != size) {
            return std::unexpected("data.frame columns must all have the same length");
        }

        auto built = build_column_from_r_vector(*name, column);
        if (!built.has_value()) {
            return std::unexpected(built.error());
        }
        auto& [column_value, validity] = *built;
        if (validity.has_value()) {
            table.add_column(*name, std::move(column_value), std::move(*validity));
        } else {
            table.add_column(*name, std::move(column_value));
        }
    }
    return table;
}

auto build_table_registry_from_r(SEXP tables_sexp)
    -> std::expected<ibex::runtime::TableRegistry, std::string> {
    ibex::runtime::TableRegistry registry;
    if (tables_sexp == R_NilValue) {
        return registry;
    }
    if (TYPEOF(tables_sexp) != VECSXP) {
        return std::unexpected("'tables' must be a named list from name to data.frame");
    }

    SEXP names = Rf_getAttrib(tables_sexp, R_NamesSymbol);
    for (R_xlen_t i = 0; i < XLENGTH(tables_sexp); ++i) {
        auto name = named_list_name(names, i, "'tables'");
        if (!name.has_value()) {
            return std::unexpected(name.error());
        }
        auto table = build_runtime_table_from_r(VECTOR_ELT(tables_sexp, i));
        if (!table.has_value()) {
            return std::unexpected("while importing table '" + *name + "': " + table.error());
        }
        registry.insert_or_assign(*name, std::move(*table));
    }
    return registry;
}

auto merge_registries(const ibex::runtime::TableRegistry& base,
                      const ibex::runtime::TableRegistry& extra) -> ibex::runtime::TableRegistry {
    ibex::runtime::TableRegistry merged = base;
    for (const auto& [name, table] : extra) {
        merged.insert_or_assign(name, table);
    }
    return merged;
}

auto merge_scalars(const ibex::runtime::ScalarRegistry& base,
                   const ibex::runtime::ScalarRegistry& extra) -> ibex::runtime::ScalarRegistry {
    ibex::runtime::ScalarRegistry merged = base;
    for (const auto& [name, value] : extra) {
        merged.insert_or_assign(name, value);
    }
    return merged;
}

auto eval_table_impl(const std::string& source, const ibex::runtime::TableRegistry& registry,
                     const ibex::runtime::ScalarRegistry& scalars,
                     const std::vector<std::string>& plugin_search_paths)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(make_error("parse error", parsed.error().format()));
    }

    ibex::runtime::ExternRegistry externs;
    if (!plugin_search_paths.empty()) {
        auto loaded = load_source_plugins(*parsed, plugin_search_paths, externs);
        if (!loaded.has_value()) {
            return std::unexpected(make_error("plugin load error", loaded.error()));
        }
    }

    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered.has_value()) {
        return std::unexpected(make_error("lowering error", lowered.error().message));
    }

    auto evaluated = ibex::runtime::interpret(*lowered.value(), registry, &scalars,
                                              plugin_search_paths.empty() ? nullptr : &externs);
    if (!evaluated.has_value()) {
        return std::unexpected(make_error("runtime error", evaluated.error()));
    }

    return std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
}

auto register_extern_decl(const ibex::parser::ExternDecl& decl, SessionState& session)
    -> std::expected<void, std::string> {
    if (decl.return_type.kind == ibex::parser::Type::Kind::DataFrame ||
        decl.return_type.kind == ibex::parser::Type::Kind::TimeFrame) {
        session.table_externs.insert(decl.name);
    }
    if (!decl.params.empty() && decl.params[0].type.kind == ibex::parser::Type::Kind::DataFrame) {
        session.sink_externs.insert(decl.name);
    }
    if (decl.source_path.empty()) {
        return {};
    }

    auto stem = plugin_stem(decl.source_path);
    auto result =
        try_load_plugin(stem, session.plugin_paths, session.loaded_plugins, session.externs);
    if (result.status == PluginLoadStatus::NotFound) {
        return std::unexpected("could not find plugin '" + stem + ".so' in search path");
    }
    if (result.status == PluginLoadStatus::LoadError) {
        return std::unexpected(result.message);
    }
    return {};
}

auto eval_table_in_session(SessionState& session, const std::string& source,
                           const ibex::runtime::TableRegistry& extra_tables,
                           const ibex::runtime::ScalarRegistry& extra_scalars)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(make_error("parse error", parsed.error().format()));
    }

    std::shared_ptr<const ibex::runtime::Table> last_table;
    for (const auto& stmt : parsed->statements) {
        if (const auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            auto registered = register_extern_decl(*decl, session);
            if (!registered.has_value()) {
                return std::unexpected(make_error("plugin load error", registered.error()));
            }
            continue;
        }
        if (std::holds_alternative<ibex::parser::ImportDecl>(stmt)) {
            return std::unexpected(
                make_error("session error",
                           "import declarations are not supported in ribex sessions; use "
                           "explicit extern fn declarations"));
        }
        if (std::holds_alternative<ibex::parser::FunctionDecl>(stmt)) {
            return std::unexpected(make_error(
                "session error", "function declarations are not supported in ribex sessions"));
        }
        if (std::holds_alternative<ibex::parser::TupleLetStmt>(stmt)) {
            return std::unexpected(make_error(
                "session error", "tuple let bindings are not supported in ribex sessions"));
        }

        ibex::parser::LowerContext context;
        context.table_externs = session.table_externs;
        context.sink_externs = session.sink_externs;
        auto runtime_registry = merge_registries(session.tables, extra_tables);
        auto runtime_scalars = merge_scalars({}, extra_scalars);

        if (const auto* let_stmt = std::get_if<ibex::parser::LetStmt>(&stmt)) {
            auto lowered = ibex::parser::lower_expr(*let_stmt->value, context);
            if (!lowered.has_value()) {
                return std::unexpected(
                    make_error("lowering error",
                               "ribex sessions currently support only table-valued let bindings: " +
                                   lowered.error().message));
            }
            auto evaluated = ibex::runtime::interpret(*lowered.value(), runtime_registry,
                                                      &runtime_scalars, &session.externs);
            if (!evaluated.has_value()) {
                return std::unexpected(make_error("runtime error", evaluated.error()));
            }
            session.tables.insert_or_assign(let_stmt->name, std::move(*evaluated));
            continue;
        }

        const auto& expr_stmt = std::get<ibex::parser::ExprStmt>(stmt);
        auto lowered = ibex::parser::lower_expr(*expr_stmt.expr, context);
        if (!lowered.has_value()) {
            return std::unexpected(
                make_error("lowering error",
                           "ribex sessions currently support only table-valued expressions: " +
                               lowered.error().message));
        }
        auto evaluated = ibex::runtime::interpret(*lowered.value(), runtime_registry,
                                                  &runtime_scalars, &session.externs);
        if (!evaluated.has_value()) {
            return std::unexpected(make_error("runtime error", evaluated.error()));
        }
        last_table = std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
    }

    return last_table;
}

void schema_finalizer(SEXP ext) {
    auto* schema = static_cast<ArrowSchema*>(R_ExternalPtrAddr(ext));
    if (schema == nullptr) {
        return;
    }
    ibex::interop::release_arrow_schema(schema);
    delete schema;
    R_ClearExternalPtr(ext);
}

void array_finalizer(SEXP ext) {
    auto* array = static_cast<ArrowArray*>(R_ExternalPtrAddr(ext));
    if (array == nullptr) {
        return;
    }
    ibex::interop::release_arrow_array(array);
    delete array;
    R_ClearExternalPtr(ext);
}

void session_finalizer(SEXP ext) {
    auto* session = static_cast<SessionState*>(R_ExternalPtrAddr(ext));
    if (session == nullptr) {
        return;
    }
    delete session;
    R_ClearExternalPtr(ext);
}

auto make_nanoarrow_xptr(void* ptr, SEXP tag, R_CFinalizer_t finalizer, const char* class_name)
    -> SEXP {
    SEXP ext = PROTECT(R_MakeExternalPtr(ptr, tag, R_NilValue));
    R_RegisterCFinalizerEx(ext, finalizer, TRUE);
    SEXP cls = PROTECT(Rf_mkString(class_name));
    Rf_classgets(ext, cls);
    UNPROTECT(2);
    return ext;
}

auto export_table_payload(std::shared_ptr<const ibex::runtime::Table> table)
    -> std::expected<SEXP, std::string> {
    auto schema = std::make_unique<ArrowSchema>();
    auto array = std::make_unique<ArrowArray>();
    auto exported =
        ibex::interop::export_table_to_arrow(std::move(table), array.get(), schema.get());
    if (!exported.has_value()) {
        return std::unexpected(exported.error());
    }

    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));

    SET_STRING_ELT(names, 0, Rf_mkChar("array"));
    SET_STRING_ELT(names, 1, Rf_mkChar("schema"));
    Rf_setAttrib(out, R_NamesSymbol, names);

    SET_VECTOR_ELT(out, 0,
                   make_nanoarrow_xptr(array.release(), Rf_install("nanoarrow_array"),
                                       array_finalizer, "nanoarrow_array"));
    SET_VECTOR_ELT(out, 1,
                   make_nanoarrow_xptr(schema.release(), Rf_install("nanoarrow_schema"),
                                       schema_finalizer, "nanoarrow_schema"));

    UNPROTECT(2);
    return out;
}

auto scalar_string(SEXP value, const char* what) -> std::expected<std::string, std::string> {
    if (TYPEOF(value) != STRSXP || Rf_length(value) != 1 || STRING_ELT(value, 0) == NA_STRING) {
        return std::unexpected(std::string(what) + " must be a length-1 character value");
    }
    return std::string(CHAR(STRING_ELT(value, 0)));
}

auto session_from_sexp(SEXP session_sexp) -> std::expected<SessionState*, std::string> {
    if (TYPEOF(session_sexp) != EXTPTRSXP) {
        return std::unexpected("'session' must be a ribex session");
    }
    auto* session = static_cast<SessionState*>(R_ExternalPtrAddr(session_sexp));
    if (session == nullptr) {
        return std::unexpected("invalid ribex session");
    }
    return session;
}

}  // namespace

extern "C" SEXP ribex_c_eval_ibex(SEXP query_sexp, SEXP plugin_paths_sexp, SEXP tables_sexp,
                                  SEXP scalars_sexp) {
    auto query = scalar_string(query_sexp, "'query'");
    if (!query.has_value()) {
        Rf_error("%s", query.error().c_str());
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto tables = build_table_registry_from_r(tables_sexp);
    if (!tables.has_value()) {
        Rf_error("%s", make_error("table import error", tables.error()).c_str());
    }

    auto scalars = build_scalar_registry_from_r(scalars_sexp);
    if (!scalars.has_value()) {
        Rf_error("%s", make_error("scalar import error", scalars.error()).c_str());
    }

    auto evaluated = eval_table_impl(*query, *tables, *scalars, *plugin_paths);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }

    return *payload;
}

extern "C" SEXP ribex_c_eval_file(SEXP path_sexp, SEXP plugin_paths_sexp, SEXP tables_sexp,
                                  SEXP scalars_sexp) {
    auto path = scalar_string(path_sexp, "'path'");
    if (!path.has_value()) {
        Rf_error("%s", path.error().c_str());
    }

    auto source = read_text_file(*path);
    if (!source.has_value()) {
        Rf_error("%s", source.error().c_str());
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto tables = build_table_registry_from_r(tables_sexp);
    if (!tables.has_value()) {
        Rf_error("%s", make_error("table import error", tables.error()).c_str());
    }

    auto scalars = build_scalar_registry_from_r(scalars_sexp);
    if (!scalars.has_value()) {
        Rf_error("%s", make_error("scalar import error", scalars.error()).c_str());
    }

    auto evaluated = eval_table_impl(*source, *tables, *scalars, *plugin_paths);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }

    return *payload;
}

extern "C" SEXP ribex_c_create_session(SEXP plugin_paths_sexp) {
    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto* session = new SessionState();
    session->plugin_paths = std::move(*plugin_paths);

    SEXP ext = PROTECT(R_MakeExternalPtr(session, Rf_install("ribex_session"), R_NilValue));
    R_RegisterCFinalizerEx(ext, session_finalizer, TRUE);
    SEXP cls = PROTECT(Rf_mkString("ribex_session"));
    Rf_classgets(ext, cls);
    UNPROTECT(2);
    return ext;
}

extern "C" SEXP ribex_c_reset_session(SEXP session_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto plugin_paths = (*session)->plugin_paths;
    delete *session;
    auto* fresh = new SessionState();
    fresh->plugin_paths = std::move(plugin_paths);
    R_SetExternalPtrAddr(session_sexp, fresh);
    return session_sexp;
}

extern "C" SEXP ribex_c_session_eval_ibex(SEXP session_sexp, SEXP query_sexp, SEXP tables_sexp,
                                          SEXP scalars_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto query = scalar_string(query_sexp, "'query'");
    if (!query.has_value()) {
        Rf_error("%s", query.error().c_str());
    }

    auto tables = build_table_registry_from_r(tables_sexp);
    if (!tables.has_value()) {
        Rf_error("%s", make_error("table import error", tables.error()).c_str());
    }

    auto scalars = build_scalar_registry_from_r(scalars_sexp);
    if (!scalars.has_value()) {
        Rf_error("%s", make_error("scalar import error", scalars.error()).c_str());
    }

    auto evaluated = eval_table_in_session(**session, *query, *tables, *scalars);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }
    if (!*evaluated) {
        return R_NilValue;
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }
    return *payload;
}

extern "C" SEXP ribex_c_session_eval_file(SEXP session_sexp, SEXP path_sexp, SEXP tables_sexp,
                                          SEXP scalars_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto path = scalar_string(path_sexp, "'path'");
    if (!path.has_value()) {
        Rf_error("%s", path.error().c_str());
    }

    auto source = read_text_file(*path);
    if (!source.has_value()) {
        Rf_error("%s", source.error().c_str());
    }

    auto tables = build_table_registry_from_r(tables_sexp);
    if (!tables.has_value()) {
        Rf_error("%s", make_error("table import error", tables.error()).c_str());
    }

    auto scalars = build_scalar_registry_from_r(scalars_sexp);
    if (!scalars.has_value()) {
        Rf_error("%s", make_error("scalar import error", scalars.error()).c_str());
    }

    auto evaluated = eval_table_in_session(**session, *source, *tables, *scalars);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }
    if (!*evaluated) {
        return R_NilValue;
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }
    return *payload;
}
