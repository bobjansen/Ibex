// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#define R_NO_REMAP

// The R bindings. Every evaluation runs on `repl::ReplSession` -- the same
// evaluator as the `ibex` REPL and `ibex_eval` -- so a script that runs there
// runs here: imports, functions, scalar bindings, resources. This file only
// converts between R values and Ibex tables/scalars and exports results as
// Arrow C data.

#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <expected>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <R_ext/Arith.h>
#include <R_ext/Error.h>
#include <R_ext/Print.h>
#include <Rinternals.h>
#include <robin_hood.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

auto make_error(std::string_view stage, const std::string& message) -> std::string {
    return "ibex " + std::string(stage) + ": " + message;
}

/// One R `ibex_session`. The extern registry outlives the session that holds a
/// reference to it, hence the declaration order. `generation` changes on every
/// reset, so the dplyr backend can tell a reset session from the one it bound
/// its tables in.
struct RSession {
    ibex::runtime::ExternRegistry registry;
    std::vector<std::string> plugin_paths;
    std::unique_ptr<ibex::repl::ReplSession> repl;
    std::uint64_t generation = 0;
};

std::atomic<std::uint64_t> next_session_generation{1};

auto make_repl_config(const std::vector<std::string>& plugin_paths) -> ibex::repl::ReplConfig {
    ibex::repl::ReplConfig config;
    config.plugin_search_paths = plugin_paths;  // also where `import` finds its stubs
    config.persistent_history = false;
    return config;
}

auto make_session(std::vector<std::string> plugin_paths) -> std::unique_ptr<RSession> {
    auto session = std::make_unique<RSession>();
    session->plugin_paths = std::move(plugin_paths);
    session->repl = std::make_unique<ibex::repl::ReplSession>(
        make_repl_config(session->plugin_paths), session->registry);
    session->generation = next_session_generation.fetch_add(1, std::memory_order_relaxed);
    return session;
}

/// Runs `source` in `session` with this call's own tables and scalars. What
/// the script prints reaches the R console; the result is the last table it
/// produced, or null when it produced none.
auto run_in_session(RSession& session, const std::string& source,
                    ibex::runtime::TableRegistry tables, ibex::runtime::ScalarRegistry scalars)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    ibex::repl::ReplSession::ExecuteOptions options;
    options.tables = std::move(tables);
    options.scalars = std::move(scalars);
    options.render = false;
    auto result = session.repl->execute(source, std::move(options));
    if (!result.output.empty()) {
        Rprintf("%s", result.output.c_str());
    }
    if (!result.ok) {
        return std::unexpected(make_error("error", result.error));
    }
    if (!result.table.has_value()) {
        return std::shared_ptr<const ibex::runtime::Table>{};
    }
    return std::make_shared<const ibex::runtime::Table>(std::move(*result.table));
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
    const long double nanos = static_cast<long double>(seconds) * 1000000000.0L;
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

/// Bind an R character vector as a Categorical column.
///
/// Every other Ibex frontend already hands string columns over dictionary
/// encoded -- `csv::read` and the Parquet reader both produce Categorical -- so
/// binding R's character vector as a String column made the R path the only
/// one whose group-by, distinct and sort hashed text per row. On 8M rows and
/// 252 distinct symbols that was 100ms against 3ms for the same query over the
/// same values as codes.
///
/// The encoding is cheap here in a way it would not be for an arbitrary string
/// column: R interns strings in a global CHARSXP cache, so equal strings in a
/// character vector are usually the same SEXP, and identity alone decides the
/// code. That makes the common row a pointer hash rather than a string hash
/// plus a string copy.
///
/// "Usually" is not "always" -- the cache is keyed by encoding as well as by
/// text, so the same characters can arrive as two distinct CHARSXPs. Two
/// dictionary entries holding equal text would split one group in two, so a
/// pointer miss falls through to a lookup by text and only a text miss appends
/// to the dictionary. The by-text map owns its keys: a `string_view` into
/// `dictionary` would dangle the moment a short (SSO) entry moved under a
/// reallocation.
auto build_categorical_from_strings(SEXP column_sexp, R_xlen_t size)
    -> std::pair<ibex::runtime::ColumnValue, std::optional<ibex::runtime::ValidityBitmap>> {
    using code_type = ibex::Column<ibex::Categorical>::code_type;

    std::vector<std::string> dictionary;
    ibex::Column<ibex::Categorical>::codes_storage codes;
    codes.reserve(static_cast<std::size_t>(size));
    // Keyed by the pointer's integer value, not by `const void*`: the pointer
    // specialization hashes by identity, and CHARSXPs are aligned, so every
    // key landed in a handful of buckets. That is not a slow hash, it is a
    // quadratic scan -- 8M lookups against 252 colliding entries took 7.8s,
    // against 25ms once the value is mixed.
    robin_hood::unordered_map<std::uintptr_t, code_type> by_pointer;
    robin_hood::unordered_map<std::string, code_type> by_text;

    bool has_nulls = false;
    ibex::runtime::ValidityBitmap validity;

    for (R_xlen_t i = 0; i < size; ++i) {
        SEXP value = STRING_ELT(column_sexp, i);
        if (value == NA_STRING) {
            if (!has_nulls) {
                has_nulls = true;
                validity.assign(static_cast<std::size_t>(size), true);
            }
            validity.set(static_cast<std::size_t>(i), false);
            codes.push_back(0);
            continue;
        }

        const auto [slot, inserted] =
            by_pointer.try_emplace(reinterpret_cast<std::uintptr_t>(value), 0);
        if (inserted) {
            std::string text(CHAR(value));
            if (const auto seen = by_text.find(text); seen != by_text.end()) {
                slot->second = seen->second;
            } else {
                const auto code = static_cast<code_type>(dictionary.size());
                by_text.emplace(text, code);
                dictionary.push_back(std::move(text));
                slot->second = code;
            }
        }
        codes.push_back(slot->second);
    }

    ibex::Column<ibex::Categorical> column(std::move(dictionary), std::move(codes));
    return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                     has_nulls ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                               : std::nullopt};
}

auto build_column_from_r_vector(const std::string& name, SEXP column_sexp, bool encode_strings)
    -> std::expected<
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
                if (encode_strings) {
                    return build_categorical_from_strings(column_sexp, size);
                }
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
                if (Rf_inherits(table_obj, "ibex_arrow_export")) {
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
    // Opt-in dictionary encoding for character columns, set by `ibex_tbl()`.
    // It is an attribute rather than an argument because the binder is reached
    // through three registered entry points that all take `tables` as a plain
    // named list, and a per-binding switch does not belong in any of their
    // signatures.
    SEXP encode_flag = Rf_getAttrib(table_obj, Rf_install("ibex_categorical_strings"));
    const bool encode_strings = TYPEOF(encode_flag) == LGLSXP && XLENGTH(encode_flag) == 1 &&
                                LOGICAL(encode_flag)[0] == TRUE;
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

        auto built = build_column_from_r_vector(*name, column, encode_strings);
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

// The finalizers below are where R hands ownership back. Between the export and
// this call the object is owned by R's collector, through a raw `void*` in the
// external pointer -- a smart pointer cannot live in that slot, which is the
// contract `R_RegisterCFinalizerEx` is built on. What *can* be expressed is the
// hand-back: adopt the pointer into a `unique_ptr` so the object is destroyed by
// a destructor rather than a bare `delete`, matching the `make_unique(...)
// .release()` (and `new`) that produced it.

void schema_finalizer(SEXP ext) {
    const std::unique_ptr<ArrowSchema> schema(static_cast<ArrowSchema*>(R_ExternalPtrAddr(ext)));
    if (!schema) {
        return;
    }
    ibex::interop::release_arrow_schema(schema.get());
    R_ClearExternalPtr(ext);
}

void array_finalizer(SEXP ext) {
    const std::unique_ptr<ArrowArray> array(static_cast<ArrowArray*>(R_ExternalPtrAddr(ext)));
    if (!array) {
        return;
    }
    ibex::interop::release_arrow_array(array.get());
    R_ClearExternalPtr(ext);
}

void session_finalizer(SEXP ext) {
    const std::unique_ptr<RSession> session(static_cast<RSession*>(R_ExternalPtrAddr(ext)));
    if (!session) {
        return;
    }
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

// Borrowed, not owned: `export_table_to_arrow` takes the handle by reference and
// the exported Arrow structures take their own shared owners -- one for the
// array plus one per column buffer -- so the payload keeps the table alive on
// its own and outlives this call regardless of what the caller does next.
auto export_table_payload(const std::shared_ptr<const ibex::runtime::Table>& table)
    -> std::expected<SEXP, std::string> {
    auto schema = std::make_unique<ArrowSchema>();
    auto array = std::make_unique<ArrowArray>();
    auto exported = ibex::interop::export_table_to_arrow(table, array.get(), schema.get());
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

auto session_from_sexp(SEXP session_sexp) -> std::expected<RSession*, std::string> {
    if (TYPEOF(session_sexp) != EXTPTRSXP) {
        return std::unexpected("'session' must be a ibex session");
    }
    auto* session = static_cast<RSession*>(R_ExternalPtrAddr(session_sexp));
    if (session == nullptr) {
        return std::unexpected("invalid ibex session");
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

auto export_table_info(RSession& session, const std::string& name)
    -> std::expected<SEXP, std::string> {
    auto bound = session.repl->table_binding(name);
    if (!bound.has_value()) {
        return std::unexpected(bound.error());
    }
    const auto& table = *bound;
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

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)

extern "C" SEXP ibex_c_arrow_buffer_addresses(SEXP array_sexp) {
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

namespace {

/// The tables, scalars and plugin paths every evaluation entry point takes from
/// R, converted; raises an R error on a bad argument.
struct CallInputs {
    ibex::runtime::TableRegistry tables;
    ibex::runtime::ScalarRegistry scalars;
};

auto call_inputs(SEXP tables_sexp, SEXP scalars_sexp) -> CallInputs {
    auto tables = build_table_registry_from_r(tables_sexp);
    if (!tables.has_value()) {
        Rf_error("%s", make_error("table import error", tables.error()).c_str());
    }
    auto scalars = build_scalar_registry_from_r(scalars_sexp);
    if (!scalars.has_value()) {
        Rf_error("%s", make_error("scalar import error", scalars.error()).c_str());
    }
    return CallInputs{.tables = std::move(*tables), .scalars = std::move(*scalars)};
}

auto source_text(SEXP text_sexp, bool is_path) -> std::string {
    auto text = scalar_string(text_sexp, is_path ? "'path'" : "'query'");
    if (!text.has_value()) {
        Rf_error("%s", text.error().c_str());
    }
    if (!is_path) {
        return std::move(*text);
    }
    auto source = read_text_file(*text);
    if (!source.has_value()) {
        Rf_error("%s", source.error().c_str());
    }
    return std::move(*source);
}

/// The result of one evaluation as R gets it: an Arrow payload, or NULL.
auto result_payload(
    const std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string>& evaluated)
    -> SEXP {
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }
    if (!*evaluated) {
        return R_NilValue;
    }
    auto payload = export_table_payload(*evaluated);
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }
    return *payload;
}

/// A one-off evaluation (`eval_ibex`, `eval_file`): a fresh session for the call.
auto eval_once(SEXP text_sexp, bool is_path, SEXP plugin_paths_sexp, SEXP tables_sexp,
               SEXP scalars_sexp) -> SEXP {
    auto source = source_text(text_sexp, is_path);
    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }
    auto inputs = call_inputs(tables_sexp, scalars_sexp);
    auto session = make_session(std::move(*plugin_paths));
    return result_payload(
        run_in_session(*session, source, std::move(inputs.tables), std::move(inputs.scalars)));
}

auto eval_in_session(SEXP session_sexp, SEXP text_sexp, bool is_path, SEXP tables_sexp,
                     SEXP scalars_sexp) -> SEXP {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }
    auto source = source_text(text_sexp, is_path);
    auto inputs = call_inputs(tables_sexp, scalars_sexp);
    return result_payload(
        run_in_session(**session, source, std::move(inputs.tables), std::move(inputs.scalars)));
}

}  // namespace

extern "C" SEXP ibex_c_eval_ibex(SEXP query_sexp, SEXP plugin_paths_sexp, SEXP tables_sexp,
                                 SEXP scalars_sexp) {
    return eval_once(query_sexp, false, plugin_paths_sexp, tables_sexp, scalars_sexp);
}

extern "C" SEXP ibex_c_eval_file(SEXP path_sexp, SEXP plugin_paths_sexp, SEXP tables_sexp,
                                 SEXP scalars_sexp) {
    return eval_once(path_sexp, true, plugin_paths_sexp, tables_sexp, scalars_sexp);
}

extern "C" SEXP ibex_c_create_session(SEXP plugin_paths_sexp) {
    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto* session = make_session(std::move(*plugin_paths)).release();
    SEXP ext = PROTECT(R_MakeExternalPtr(session, Rf_install("ibex_session"), R_NilValue));
    R_RegisterCFinalizerEx(ext, session_finalizer, TRUE);
    SEXP cls = PROTECT(Rf_mkString("ibex_session"));
    Rf_classgets(ext, cls);
    UNPROTECT(2);
    return ext;
}

extern "C" SEXP ibex_c_shutdown_runtime() {
    // R can unload a namespace while its host process remains alive (notably
    // during RStudio restart/reload flows).  The pool executes functions from
    // this DLL, so it must be joined before R removes the DLL mapping.
    ibex::runtime::shutdown_process_worker_pool();
    return R_NilValue;
}

extern "C" void ibex_shutdown_runtime_for_unload() {
    ibex::runtime::shutdown_process_worker_pool();
}

extern "C" SEXP ibex_c_reset_session(SEXP session_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto fresh = make_session((*session)->plugin_paths);
    const std::unique_ptr<RSession> old(*session);
    R_SetExternalPtrAddr(session_sexp, fresh.release());
    return session_sexp;
}

extern "C" SEXP ibex_c_session_table_info(SEXP session_sexp, SEXP name_sexp) {
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

/// Infer the schema of a rendered lazy-plan query without executing it.
///
/// Deliberately total: every way this can fail to reach a Known schema returns
/// NULL rather than an error, because the caller's fallback -- assume every
/// column nullable -- is sound for all of them.
extern "C" SEXP ibex_c_session_infer_schema(SEXP session_sexp, SEXP query_sexp,
                                            SEXP lexical_names_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }
    auto query = scalar_string(query_sexp, "'query'");
    if (!query.has_value()) {
        Rf_error("%s", query.error().c_str());
    }

    // Scalars captured from the R environment (`.env$cutoff`) are supplied at
    // eval time, not bound in the session, so the caller names them: they are
    // not columns.
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

    const auto schema = (*session)->repl->infer_schema(*query, lexical_names);
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

extern "C" SEXP ibex_c_session_eval_ibex(SEXP session_sexp, SEXP query_sexp, SEXP tables_sexp,
                                         SEXP scalars_sexp) {
    return eval_in_session(session_sexp, query_sexp, false, tables_sexp, scalars_sexp);
}

extern "C" SEXP ibex_c_session_eval_file(SEXP session_sexp, SEXP path_sexp, SEXP tables_sexp,
                                         SEXP scalars_sexp) {
    return eval_in_session(session_sexp, path_sexp, true, tables_sexp, scalars_sexp);
}
// NOLINTEND(cppcoreguidelines-pro-type-vararg)
// NOLINTEND(bugprone-easily-swappable-parameters)
