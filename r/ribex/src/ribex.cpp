// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#define R_NO_REMAP

#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <optional>
#include <R_ext/Arith.h>
#include <R_ext/Error.h>
#include <Rinternals.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
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
    robin_hood::unordered_map<std::string, std::vector<std::string>> compile_time_lists;
    robin_hood::unordered_set<std::string> table_externs;
    robin_hood::unordered_set<std::string> sink_externs;
    ibex::runtime::ExternRegistry externs;
    std::unordered_set<std::string> loaded_plugins;
    std::vector<std::string> plugin_paths;
    std::uint64_t generation = 0;
};

std::atomic<std::uint64_t> next_session_generation{1};

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

        if (Rf_inherits(column_sexp, "integer64")) {
            // bit64 stores 64-bit integers in the payload of a double vector:
            // the SEXP is a REALSXP and only the class attribute says the bits
            // are an int64. Without this branch the switch below reaches
            // `case REALSXP` and reads each element as the double those bits
            // happen to spell -- so 9007199254740993 arrives as 4.45e-308.
            // Silent, and worst for exactly the values a caller reaches for
            // bit64 to hold.
            if (TYPEOF(column_sexp) != REALSXP) {
                return std::unexpected("column '" + name + "' integer64 data must be numeric");
            }
            return build_column_with_validity<std::int64_t>(size, [&](R_xlen_t i) {
                std::int64_t value = 0;
                std::memcpy(&value, &REAL(column_sexp)[i], sizeof(value));
                // bit64 spells NA as the smallest int64 rather than R's NA
                // real, so ISNA() would not see it.
                if (value == std::numeric_limits<std::int64_t>::min()) {
                    return std::pair{std::int64_t{0}, false};
                }
                return std::pair{value, true};
            });
        }

        if (Rf_inherits(column_sexp, "factor")) {
            if (TYPEOF(column_sexp) != INTSXP) {
                return std::unexpected("column '" + name + "' factor data must be integer");
            }
            SEXP levels = Rf_getAttrib(column_sexp, R_LevelsSymbol);
            if (TYPEOF(levels) != STRSXP) {
                return std::unexpected("column '" + name + "' factor must have character levels");
            }

            std::vector<std::string> dictionary;
            dictionary.reserve(static_cast<std::size_t>(XLENGTH(levels)));
            for (R_xlen_t i = 0; i < XLENGTH(levels); ++i) {
                SEXP level = STRING_ELT(levels, i);
                if (level == NA_STRING) {
                    return std::unexpected("column '" + name + "' factor must not have NA levels");
                }
                dictionary.emplace_back(CHAR(level));
            }

            ibex::Column<ibex::Categorical> column(std::move(dictionary));
            column.reserve(static_cast<std::size_t>(size));
            bool has_nulls = false;
            ibex::runtime::ValidityBitmap validity;
            for (R_xlen_t i = 0; i < size; ++i) {
                const int value = INTEGER(column_sexp)[i];
                const bool valid = value != NA_INTEGER;
                if (valid && (value < 1 || value > XLENGTH(levels))) {
                    return std::unexpected("column '" + name + "' factor has an out-of-range code");
                }
                if (!valid && !has_nulls) {
                    has_nulls = true;
                    validity.assign(static_cast<std::size_t>(size), true);
                }
                if (has_nulls) {
                    validity.set(static_cast<std::size_t>(i), valid);
                }
                column.push_code(
                    valid ? static_cast<ibex::Column<ibex::Categorical>::code_type>(value - 1) : 0);
            }
            return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                             has_nulls
                                 ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                                 : std::nullopt};
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
    if (TYPEOF(table_obj) == EXTPTRSXP && Rf_inherits(table_obj, "nanoarrow_array")) {
        SEXP schema_obj = Rf_getAttrib(table_obj, Rf_install("schema_xptr"));
        if (schema_obj == R_NilValue) {
            schema_obj = Rf_getAttrib(table_obj, Rf_install("schema"));
        }
        if (TYPEOF(schema_obj) != EXTPTRSXP) {
            return std::unexpected(
                "unsupported nanoarrow table binding; expected a schema_xptr attribute");
        }

        auto* array = static_cast<ArrowArray*>(R_ExternalPtrAddr(table_obj));
        auto* schema = static_cast<ArrowSchema*>(R_ExternalPtrAddr(schema_obj));
        if (array == nullptr || schema == nullptr) {
            return std::unexpected("invalid nanoarrow table binding");
        }

        return ibex::interop::import_table_from_arrow(*array, *schema);
    }

    if (TYPEOF(table_obj) == VECSXP) {
        SEXP names = Rf_getAttrib(table_obj, R_NamesSymbol);
        if (TYPEOF(names) == STRSXP && XLENGTH(table_obj) == 2) {
            int array_idx = -1;
            int schema_idx = -1;
            for (R_xlen_t i = 0; i < XLENGTH(table_obj); ++i) {
                const auto* name = CHAR(STRING_ELT(names, i));
                if (std::strcmp(name, "array") == 0) {
                    array_idx = static_cast<int>(i);
                } else if (std::strcmp(name, "schema") == 0) {
                    schema_idx = static_cast<int>(i);
                }
            }
            if (array_idx >= 0 && schema_idx >= 0) {
                SEXP array_obj = VECTOR_ELT(table_obj, array_idx);
                SEXP schema_obj = VECTOR_ELT(table_obj, schema_idx);
                if (TYPEOF(array_obj) != EXTPTRSXP || TYPEOF(schema_obj) != EXTPTRSXP) {
                    return std::unexpected(
                        "Arrow payload table binding requires externalptr array and schema");
                }
                auto* array = static_cast<ArrowArray*>(R_ExternalPtrAddr(array_obj));
                auto* schema = static_cast<ArrowSchema*>(R_ExternalPtrAddr(schema_obj));
                if (array == nullptr || schema == nullptr) {
                    return std::unexpected("invalid Arrow payload table binding");
                }
                if (Rf_inherits(table_obj, "ribex_arrow_export")) {
                    return ibex::interop::adopt_table_from_arrow(array, *schema);
                }
                return ibex::interop::import_table_from_arrow(*array, *schema);
            }
        }
    }

    if (!Rf_inherits(table_obj, "data.frame")) {
        return std::unexpected(
            "unsupported table binding object; expected data.frame or nanoarrow_array");
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
        return std::unexpected(
            "'tables' must be a named list from name to data.frame or nanoarrow_array");
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

auto extract_compile_time_string_list(const ibex::parser::Expr& expr)
    -> std::optional<std::vector<std::string>> {
    const auto* array = std::get_if<ibex::parser::ArrayLiteralExpr>(&expr.node);
    if (array == nullptr) {
        return std::nullopt;
    }

    std::vector<std::string> values;
    values.reserve(array->elements.size());
    for (const auto& elem : array->elements) {
        const auto* lit = std::get_if<ibex::parser::LiteralExpr>(&elem->node);
        if (lit == nullptr) {
            return std::nullopt;
        }
        const auto* text = std::get_if<std::string>(&lit->value);
        if (text == nullptr) {
            return std::nullopt;
        }
        values.push_back(*text);
    }
    return values;
}

auto extract_compile_time_string_list(const ibex::runtime::Table& table)
    -> std::optional<std::vector<std::string>> {
    if (table.columns.size() != 1 || table.columns.front().name != "name") {
        return std::nullopt;
    }

    const auto& entry = table.columns.front();
    const auto* names = std::get_if<ibex::Column<std::string>>(entry.column.get());
    if (names == nullptr) {
        return std::nullopt;
    }
    if (entry.validity.has_value()) {
        for (std::size_t row = 0; row < names->size(); ++row) {
            if (!(*entry.validity)[row]) {
                return std::nullopt;
            }
        }
    }

    std::vector<std::string> values;
    values.reserve(names->size());
    for (const auto& value : *names) {
        values.push_back(std::string(value));
    }
    return values;
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
        context.compile_time_lists = session.compile_time_lists;
        context.table_externs = session.table_externs;
        context.sink_externs = session.sink_externs;
        auto runtime_registry = merge_registries(session.tables, extra_tables);
        auto runtime_scalars = merge_scalars({}, extra_scalars);

        // Carry every in-scope name into the lowerer, exactly as the REPL does.
        // The static column-ref check treats a bare name in a filter or update
        // expression as a column unless it is listed here, so without this a
        // bound scalar is rejected as a missing column the moment the input
        // schema is statically known -- which is precisely what happens when
        // clauses are chained instead of split across intermediate `let`s.
        for (const auto& entry : runtime_registry) {
            context.lexical_names.insert(entry.first);
        }
        for (const auto& entry : runtime_scalars) {
            context.lexical_names.insert(entry.first);
        }
        for (const auto& entry : session.compile_time_lists) {
            context.lexical_names.insert(entry.first);
        }
        context.lexical_names.insert(session.table_externs.begin(), session.table_externs.end());
        context.lexical_names.insert(session.sink_externs.begin(), session.sink_externs.end());

        if (const auto* let_stmt = std::get_if<ibex::parser::LetStmt>(&stmt)) {
            if (auto string_list = extract_compile_time_string_list(*let_stmt->value);
                string_list.has_value()) {
                session.compile_time_lists.insert_or_assign(let_stmt->name,
                                                            std::move(*string_list));
                continue;
            }
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
            if (auto compile_time_list = extract_compile_time_string_list(*evaluated);
                compile_time_list.has_value()) {
                session.compile_time_lists.insert_or_assign(let_stmt->name,
                                                            std::move(*compile_time_list));
            } else {
                session.compile_time_lists.erase(let_stmt->name);
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

auto column_type_name(const ibex::runtime::ColumnValue& value) -> const char* {
    return std::visit(
        [](const auto& column) -> const char* {
            using Column = std::decay_t<decltype(column)>;
            if constexpr (std::is_same_v<Column, ibex::Column<std::int64_t>>) {
                return "Int64";
            } else if constexpr (std::is_same_v<Column, ibex::Column<double>>) {
                return "Float64";
            } else if constexpr (std::is_same_v<Column, ibex::Column<bool>>) {
                return "Bool";
            } else if constexpr (std::is_same_v<Column, ibex::Column<ibex::Date>>) {
                return "Date";
            } else if constexpr (std::is_same_v<Column, ibex::Column<ibex::Timestamp>>) {
                return "Timestamp";
            } else if constexpr (std::is_same_v<Column, ibex::Column<ibex::Categorical>>) {
                return "Categorical";
            } else {
                return "String";
            }
        },
        value);
}

auto ir_column_type(const ibex::runtime::ColumnValue& value)
    -> std::optional<ibex::ir::ColumnType> {
    return std::visit(
        [](const auto& column) -> std::optional<ibex::ir::ColumnType> {
            using Column = std::decay_t<decltype(column)>;
            if constexpr (std::is_same_v<Column, ibex::Column<std::int64_t>>) {
                return ibex::ir::ColumnType::Int64;
            } else if constexpr (std::is_same_v<Column, ibex::Column<double>>) {
                return ibex::ir::ColumnType::Float64;
            } else if constexpr (std::is_same_v<Column, ibex::Column<bool>>) {
                return ibex::ir::ColumnType::Bool;
            } else if constexpr (std::is_same_v<Column, ibex::Column<ibex::Date>>) {
                return ibex::ir::ColumnType::Date;
            } else if constexpr (std::is_same_v<Column, ibex::Column<ibex::Timestamp>>) {
                return ibex::ir::ColumnType::Timestamp;
            } else if constexpr (std::is_same_v<Column, ibex::Column<ibex::Categorical>>) {
                // The IR has no Categorical: it is a String with a dictionary,
                // and every rule that reads the type treats it as one.
                return ibex::ir::ColumnType::String;
            } else {
                return ibex::ir::ColumnType::String;
            }
        },
        value);
}

/// Describe every table bound in the session so `infer_schema` can resolve the
/// scans a rendered dplyr plan makes.
///
/// The nullability here is the strongest evidence there is, and it is evidence
/// rather than a declaration: these are materialized columns, and a column with
/// no validity bitmap holds no nulls. That is what seeds the core's propagation
/// -- every proof downstream of a `filter` or a join is ultimately grounded in
/// one of these.
auto session_source_schemas(const SessionState& session) -> ibex::ir::SourceSchemas {
    ibex::ir::SourceSchemas sources;
    for (const auto& [name, table] : session.tables) {
        std::vector<ibex::ir::SchemaField> fields;
        fields.reserve(table.columns.size());
        for (const auto& entry : table.columns) {
            fields.push_back(ibex::ir::SchemaField{.name = entry.name,
                                                   .type = ir_column_type(*entry.column),
                                                   .nulls = entry.validity.has_value()
                                                                ? ibex::ir::Nullability::Maybe
                                                                : ibex::ir::Nullability::Never});
        }
        sources.emplace(name, ibex::ir::SchemaInfo::known(std::move(fields)));
    }
    return sources;
}

/// Infer the schema of a rendered lazy-plan query without executing it.
///
/// Deliberately total: every way this can fail to reach a Known schema returns
/// `nullopt` rather than an error, because the caller's fallback -- assume
/// every column nullable -- is sound for all of them. A plan shape this cannot
/// lower is a plan the adapter should still be able to describe conservatively.
auto infer_plan_schema(const SessionState& session, const std::string& source,
                       const std::vector<std::string>& extra_lexical_names)
    -> std::optional<ibex::ir::SchemaInfo> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value() || parsed->statements.size() != 1) {
        return std::nullopt;
    }
    const auto* expr_stmt = std::get_if<ibex::parser::ExprStmt>(&parsed->statements.front());
    if (expr_stmt == nullptr) {
        return std::nullopt;
    }

    ibex::parser::LowerContext context;
    context.compile_time_lists = session.compile_time_lists;
    context.table_externs = session.table_externs;
    context.sink_externs = session.sink_externs;
    // Same reason as `eval_table_in_session`: without the in-scope names, a
    // captured scalar in a filter reads as a missing column and lowering fails.
    for (const auto& entry : session.tables) {
        context.lexical_names.insert(entry.first);
    }
    for (const auto& entry : session.compile_time_lists) {
        context.lexical_names.insert(entry.first);
    }
    context.lexical_names.insert(session.table_externs.begin(), session.table_externs.end());
    context.lexical_names.insert(session.sink_externs.begin(), session.sink_externs.end());
    // Scalars captured from the R environment (`.env$cutoff`) are supplied at
    // eval time, not bound in the session, so the caller has to name them. A
    // plan carrying one is ordinary, and without this every such plan would
    // fail to lower and fall back to "everything nullable".
    context.lexical_names.insert(extra_lexical_names.begin(), extra_lexical_names.end());

    auto lowered = ibex::parser::lower_expr(*expr_stmt->expr, context);
    if (!lowered.has_value() || lowered.value() == nullptr) {
        return std::nullopt;
    }
    auto schema = ibex::ir::infer_schema(*lowered.value(), session_source_schemas(session));
    if (!schema.is_known()) {
        return std::nullopt;
    }
    return schema;
}

auto export_table_info(const SessionState& session, const std::string& name)
    -> std::expected<SEXP, std::string> {
    const auto it = session.tables.find(name);
    if (it == session.tables.end()) {
        return std::unexpected("session has no table binding named '" + name + "'");
    }
    const auto& table = it->second;
    const auto column_count = static_cast<R_xlen_t>(table.columns.size());

    SEXP out = PROTECT(Rf_allocVector(VECSXP, 11));
    SEXP out_names = PROTECT(Rf_allocVector(STRSXP, 11));
    constexpr const char* field_names[] = {"names",      "types",      "nullable",  "categorical",
                                           "timezone",   "rows",       "ordering",  "descending",
                                           "time_index", "generation", "grouped_by"};
    for (R_xlen_t i = 0; i < 11; ++i) {
        SET_STRING_ELT(out_names, i, Rf_mkChar(field_names[i]));
    }
    Rf_setAttrib(out, R_NamesSymbol, out_names);

    SEXP names = PROTECT(Rf_allocVector(STRSXP, column_count));
    SEXP types = PROTECT(Rf_allocVector(STRSXP, column_count));
    SEXP nullable = PROTECT(Rf_allocVector(LGLSXP, column_count));
    SEXP categorical = PROTECT(Rf_allocVector(LGLSXP, column_count));
    SEXP timezone = PROTECT(Rf_allocVector(STRSXP, column_count));
    for (R_xlen_t i = 0; i < column_count; ++i) {
        const auto& entry = table.columns[static_cast<std::size_t>(i)];
        SET_STRING_ELT(names, i, Rf_mkCharCE(entry.name.c_str(), CE_UTF8));
        SET_STRING_ELT(types, i, Rf_mkChar(column_type_name(*entry.column)));
        LOGICAL(nullable)[i] = entry.validity.has_value() ? TRUE : FALSE;
        const auto* categorical_column =
            std::get_if<ibex::Column<ibex::Categorical>>(entry.column.get());
        LOGICAL(categorical)[i] = categorical_column != nullptr ? TRUE : FALSE;
        const auto* timestamp = std::get_if<ibex::Column<ibex::Timestamp>>(entry.column.get());
        if (timestamp != nullptr && timestamp->meta().zone.has_value()) {
            const auto& zone = ibex::zone_name(*timestamp->meta().zone);
            SET_STRING_ELT(timezone, i, Rf_mkCharCE(zone.c_str(), CE_UTF8));
        } else {
            SET_STRING_ELT(timezone, i, NA_STRING);
        }
    }
    SET_VECTOR_ELT(out, 0, names);
    SET_VECTOR_ELT(out, 1, types);
    SET_VECTOR_ELT(out, 2, nullable);
    SET_VECTOR_ELT(out, 3, categorical);
    SET_VECTOR_ELT(out, 4, timezone);

    SET_VECTOR_ELT(out, 5, Rf_ScalarReal(static_cast<double>(table.rows())));
    const auto& ordering = table.ordering();
    const auto order_count = ordering.has_value() ? static_cast<R_xlen_t>(ordering->size()) : 0;
    SEXP order_names = PROTECT(Rf_allocVector(STRSXP, order_count));
    SEXP descending = PROTECT(Rf_allocVector(LGLSXP, order_count));
    if (ordering.has_value()) {
        for (R_xlen_t i = 0; i < order_count; ++i) {
            const auto& key = (*ordering)[static_cast<std::size_t>(i)];
            SET_STRING_ELT(order_names, i, Rf_mkCharCE(key.name.c_str(), CE_UTF8));
            LOGICAL(descending)[i] = key.ascending ? FALSE : TRUE;
        }
    }
    SET_VECTOR_ELT(out, 6, order_names);
    SET_VECTOR_ELT(out, 7, descending);
    if (table.time_index().has_value()) {
        SET_VECTOR_ELT(out, 8, Rf_mkString(table.time_index()->c_str()));
    } else {
        SET_VECTOR_ELT(out, 8, R_NilValue);
    }
    SET_VECTOR_ELT(out, 9, Rf_ScalarReal(static_cast<double>(session.generation)));

    const auto group_count = static_cast<R_xlen_t>(table.grouped_by().size());
    SEXP grouped_by = PROTECT(Rf_allocVector(STRSXP, group_count));
    for (R_xlen_t i = 0; i < group_count; ++i) {
        const auto& group = table.grouped_by()[static_cast<std::size_t>(i)];
        SET_STRING_ELT(grouped_by, i, Rf_mkCharCE(group.c_str(), CE_UTF8));
    }
    SET_VECTOR_ELT(out, 10, grouped_by);

    UNPROTECT(10);
    return out;
}

void collect_buffer_addresses(const ArrowArray& array, const std::string& path,
                              std::vector<std::pair<std::string, std::string>>& out) {
    for (std::int64_t i = 0; i < array.n_buffers; ++i) {
        std::ostringstream address;
        address << "0x" << std::hex << reinterpret_cast<std::uintptr_t>(array.buffers[i]);
        out.emplace_back(path + ".buffer" + std::to_string(i), address.str());
    }
    for (std::int64_t i = 0; i < array.n_children; ++i) {
        if (array.children[i] != nullptr) {
            collect_buffer_addresses(*array.children[i], path + ".child" + std::to_string(i), out);
        }
    }
    if (array.dictionary != nullptr) {
        collect_buffer_addresses(*array.dictionary, path + ".dictionary", out);
    }
}

}  // namespace

extern "C" SEXP ribex_c_arrow_buffer_addresses(SEXP array_sexp) {
    if (TYPEOF(array_sexp) != EXTPTRSXP || !Rf_inherits(array_sexp, "nanoarrow_array")) {
        Rf_error("'array' must be a nanoarrow_array");
    }
    auto* array = static_cast<ArrowArray*>(R_ExternalPtrAddr(array_sexp));
    if (array == nullptr || array->release == nullptr) {
        Rf_error("'array' must point to a live ArrowArray");
    }

    std::vector<std::pair<std::string, std::string>> addresses;
    collect_buffer_addresses(*array, "root", addresses);

    SEXP result = PROTECT(Rf_allocVector(STRSXP, static_cast<R_xlen_t>(addresses.size())));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, static_cast<R_xlen_t>(addresses.size())));
    for (R_xlen_t i = 0; i < static_cast<R_xlen_t>(addresses.size()); ++i) {
        SET_STRING_ELT(result, i, Rf_mkChar(addresses[static_cast<std::size_t>(i)].second.c_str()));
        SET_STRING_ELT(names, i, Rf_mkChar(addresses[static_cast<std::size_t>(i)].first.c_str()));
    }
    Rf_setAttrib(result, R_NamesSymbol, names);
    UNPROTECT(2);
    return result;
}

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
    session->generation = next_session_generation.fetch_add(1, std::memory_order_relaxed);

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
    fresh->generation = next_session_generation.fetch_add(1, std::memory_order_relaxed);
    R_SetExternalPtrAddr(session_sexp, fresh);
    return session_sexp;
}

extern "C" SEXP ribex_c_session_table_info(SEXP session_sexp, SEXP name_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }
    auto name = scalar_string(name_sexp, "'name'");
    if (!name.has_value()) {
        Rf_error("%s", name.error().c_str());
    }
    auto info = export_table_info(**session, *name);
    if (!info.has_value()) {
        Rf_error("%s", make_error("session error", info.error()).c_str());
    }
    return *info;
}

extern "C" SEXP ribex_c_session_infer_schema(SEXP session_sexp, SEXP query_sexp,
                                             SEXP lexical_names_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }
    auto query = scalar_string(query_sexp, "'query'");
    if (!query.has_value()) {
        Rf_error("%s", query.error().c_str());
    }

    std::vector<std::string> lexical_names;
    if (TYPEOF(lexical_names_sexp) == STRSXP) {
        const R_xlen_t count = Rf_xlength(lexical_names_sexp);
        lexical_names.reserve(static_cast<std::size_t>(count));
        for (R_xlen_t i = 0; i < count; ++i) {
            SEXP element = STRING_ELT(lexical_names_sexp, i);
            if (element != NA_STRING) {
                lexical_names.emplace_back(Rf_translateCharUTF8(element));
            }
        }
    }

    const auto schema = infer_plan_schema(**session, *query, lexical_names);
    if (!schema.has_value()) {
        return R_NilValue;
    }

    const auto column_count = static_cast<R_xlen_t>(schema->fields().size());
    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP out_names = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(out_names, 0, Rf_mkChar("names"));
    SET_STRING_ELT(out_names, 1, Rf_mkChar("nullable"));
    Rf_setAttrib(out, R_NamesSymbol, out_names);

    SEXP names = PROTECT(Rf_allocVector(STRSXP, column_count));
    SEXP nullable = PROTECT(Rf_allocVector(LGLSXP, column_count));
    for (R_xlen_t i = 0; i < column_count; ++i) {
        const auto& field = schema->fields()[static_cast<std::size_t>(i)];
        SET_STRING_ELT(names, i, Rf_mkCharCE(field.name.c_str(), CE_UTF8));
        LOGICAL(nullable)[i] = field.non_null() ? FALSE : TRUE;
    }
    SET_VECTOR_ELT(out, 0, names);
    SET_VECTOR_ELT(out, 1, nullable);
    UNPROTECT(4);
    return out;
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
