#pragma once
// Ibex SQLite library — reading and writing SQLite database tables.
//
// Reading:
//   extern fn read_sqlite(path: String, table: String) -> DataFrame from "sqlite.hpp";
//   let df = read_sqlite("mydb.sqlite", "users");
//
// Writing:
//   extern fn write_sqlite(df: DataFrame, path: String, table: String) -> Int from "sqlite.hpp";
//   let rows = write_sqlite(df, "mydb.sqlite", "users");
//
// read_sqlite reads all rows from the named table and infers column types:
//   - Columns whose every non-NULL value is INTEGER  → int64
//   - Columns whose every non-NULL value is INTEGER or REAL → double
//   - Otherwise → string (with automatic categorical encoding for low-cardinality data)
//
// write_sqlite drops and recreates the named table, then inserts all rows.
// The entire insert is wrapped in a single transaction for performance.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cstdint>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

namespace {

/// RAII wrapper for a sqlite3 database handle.
struct SqliteDb {
    sqlite3* handle = nullptr;

    explicit SqliteDb(const std::string& path, int flags = SQLITE_OPEN_READONLY) {
        if (sqlite3_open_v2(path.c_str(), &handle, flags, nullptr) != SQLITE_OK) {
            std::string msg = "read_sqlite: cannot open database '";
            msg += path;
            msg += "'";
            if (handle) {
                msg += ": ";
                msg += sqlite3_errmsg(handle);
                sqlite3_close(handle);
                handle = nullptr;
            }
            throw std::runtime_error(msg);
        }
    }

    ~SqliteDb() {
        if (handle) {
            sqlite3_close(handle);
        }
    }

    SqliteDb(const SqliteDb&) = delete;
    SqliteDb& operator=(const SqliteDb&) = delete;
};

/// RAII wrapper for a sqlite3_stmt.
struct SqliteStmt {
    sqlite3_stmt* stmt = nullptr;

    SqliteStmt(sqlite3* db, const std::string& sql) {
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error(std::string("sqlite: prepare failed: ") +
                                     sqlite3_errmsg(db));
        }
    }

    ~SqliteStmt() {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
    }

    SqliteStmt(const SqliteStmt&) = delete;
    SqliteStmt& operator=(const SqliteStmt&) = delete;
};

/// Return a double-quoted SQL identifier, with internal double-quotes escaped.
inline auto sqlite_quote_ident(std::string_view name) -> std::string {
    std::string q;
    q.reserve(name.size() + 2);
    q += '"';
    for (char c : name) {
        if (c == '"') {
            q += '"';  // double the quote
        }
        q += c;
    }
    q += '"';
    return q;
}

/// Execute a SQL statement that returns no rows, throwing on error.
inline void sqlite_exec(sqlite3* db, const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = "sqlite: exec failed: ";
        msg += err ? err : "(unknown error)";
        sqlite3_free(err);
        throw std::runtime_error(msg);
    }
}

}  // namespace

/// Read all rows from `table_name` in the SQLite database at `path`.
///
/// Type inference (per column, ignoring NULLs):
///   - All INTEGER  → Column<int64_t>
///   - All INTEGER or REAL → Column<double>
///   - Otherwise → Column<string> (categorical-encoded when cardinality is low)
inline auto read_sqlite(std::string_view path, std::string_view table_name)
    -> ibex::runtime::Table {
    SqliteDb db(std::string(path));

    const std::string sql = "SELECT * FROM " + sqlite_quote_ident(table_name);
    SqliteStmt stmt(db.handle, sql);

    const int col_count = sqlite3_column_count(stmt.stmt);
    if (col_count == 0) {
        return ibex::runtime::Table{};
    }

    // Collect column names.
    std::vector<std::string> col_names;
    col_names.reserve(col_count);
    for (int c = 0; c < col_count; ++c) {
        const char* name = sqlite3_column_name(stmt.stmt, c);
        col_names.emplace_back(name ? name : "");
    }

    // Raw cell storage: each cell is NULL, int64, double, or string.
    struct NullTag {};
    using CellValue = std::variant<NullTag, std::int64_t, double, std::string>;

    std::vector<std::vector<CellValue>> raw(col_count);

    int rc;
    while ((rc = sqlite3_step(stmt.stmt)) == SQLITE_ROW) {
        for (int c = 0; c < col_count; ++c) {
            switch (sqlite3_column_type(stmt.stmt, c)) {
                case SQLITE_INTEGER:
                    raw[c].emplace_back(sqlite3_column_int64(stmt.stmt, c));
                    break;
                case SQLITE_FLOAT:
                    raw[c].emplace_back(sqlite3_column_double(stmt.stmt, c));
                    break;
                case SQLITE_NULL:
                    raw[c].emplace_back(NullTag{});
                    break;
                default:  // SQLITE_TEXT, SQLITE_BLOB
                {
                    const auto* text = sqlite3_column_text(stmt.stmt, c);
                    raw[c].emplace_back(text ? reinterpret_cast<const char*>(text) : "");
                }
            }
        }
    }

    if (rc != SQLITE_DONE) {
        throw std::runtime_error(std::string("sqlite: query error: ") +
                                 sqlite3_errmsg(db.handle));
    }

    constexpr std::size_t kMaxCategoricalUniques = 4096;
    constexpr double kMaxCategoricalRatio = 0.05;

    ibex::runtime::Table result;

    for (int c = 0; c < col_count; ++c) {
        const auto& cells = raw[c];
        const std::size_t n = cells.size();

        // Build validity bitmap.
        std::vector<bool> validity(n, true);
        bool has_nulls = false;
        for (std::size_t i = 0; i < n; ++i) {
            if (std::holds_alternative<NullTag>(cells[i])) {
                validity[i] = false;
                has_nulls = true;
            }
        }

        // Try int64: every non-null cell must be SQLITE_INTEGER.
        bool all_int = true;
        bool any_valid = false;
        for (std::size_t i = 0; i < n; ++i) {
            if (!validity[i]) {
                continue;
            }
            any_valid = true;
            if (!std::holds_alternative<std::int64_t>(cells[i])) {
                all_int = false;
                break;
            }
        }
        if (all_int && any_valid) {
            ibex::Column<std::int64_t> col;
            col.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                col.push_back(validity[i] ? std::get<std::int64_t>(cells[i]) : 0LL);
            }
            if (has_nulls) {
                result.add_column(col_names[c], std::move(col), std::move(validity));
            } else {
                result.add_column(col_names[c], std::move(col));
            }
            continue;
        }

        // Try double: every non-null cell must be INTEGER or REAL.
        bool all_double = true;
        any_valid = false;
        for (std::size_t i = 0; i < n; ++i) {
            if (!validity[i]) {
                continue;
            }
            any_valid = true;
            if (!std::holds_alternative<std::int64_t>(cells[i]) &&
                !std::holds_alternative<double>(cells[i])) {
                all_double = false;
                break;
            }
        }
        if (all_double && any_valid) {
            ibex::Column<double> col;
            col.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                if (!validity[i]) {
                    col.push_back(0.0);
                } else if (const auto* iv = std::get_if<std::int64_t>(&cells[i])) {
                    col.push_back(static_cast<double>(*iv));
                } else {
                    col.push_back(std::get<double>(cells[i]));
                }
            }
            if (has_nulls) {
                result.add_column(col_names[c], std::move(col), std::move(validity));
            } else {
                result.add_column(col_names[c], std::move(col));
            }
            continue;
        }

        // String fallback: convert every cell to a string representation.
        // When nulls are present, skip categorical encoding.
        auto cell_to_string = [](const CellValue& cell) -> std::string {
            if (const auto* sv = std::get_if<std::string>(&cell)) {
                return *sv;
            }
            if (const auto* iv = std::get_if<std::int64_t>(&cell)) {
                return std::to_string(*iv);
            }
            if (const auto* dv = std::get_if<double>(&cell)) {
                return std::to_string(*dv);
            }
            return {};  // NullTag — should not be reached for valid rows
        };

        if (has_nulls) {
            ibex::Column<std::string> col;
            col.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                col.push_back(validity[i] ? cell_to_string(cells[i]) : std::string_view{});
            }
            result.add_column(col_names[c], std::move(col), std::move(validity));
            continue;
        }

        // Attempt categorical encoding for low-cardinality string columns.
        if (n > 0) {
            const std::size_t ratio_limit = std::max<std::size_t>(
                1, static_cast<std::size_t>(static_cast<double>(n) * kMaxCategoricalRatio));
            const std::size_t max_uniques = std::min(kMaxCategoricalUniques, ratio_limit);

            std::vector<ibex::Column<ibex::Categorical>::code_type> codes;
            codes.reserve(n);
            std::vector<std::string> dict;
            dict.reserve(std::min(n, max_uniques));
            std::unordered_map<std::string_view, ibex::Column<ibex::Categorical>::code_type> index;
            index.reserve(std::min(n, max_uniques));

            // Pre-convert to strings so we can take stable string_view keys.
            std::vector<std::string> str_vals;
            str_vals.reserve(n);
            for (const auto& cell : cells) {
                str_vals.push_back(cell_to_string(cell));
            }

            bool categorical_ok = true;
            for (const auto& v : str_vals) {
                std::string_view key{v};
                auto it = index.find(key);
                if (it != index.end()) {
                    codes.push_back(it->second);
                    continue;
                }
                if (index.size() + 1 > max_uniques) {
                    categorical_ok = false;
                    break;
                }
                auto code =
                    static_cast<ibex::Column<ibex::Categorical>::code_type>(dict.size());
                dict.push_back(v);
                index.emplace(dict.back(), code);
                codes.push_back(code);
            }
            if (categorical_ok) {
                result.add_column(col_names[c], ibex::Column<ibex::Categorical>(
                                                    std::move(dict), std::move(codes)));
                continue;
            }
            result.add_column(col_names[c], ibex::Column<std::string>(std::move(str_vals)));
            continue;
        }

        result.add_column(col_names[c], ibex::Column<std::string>{});
    }

    return result;
}

/// Write `table` to the SQLite database at `path`, storing data in `table_name`.
///
/// The target table is dropped and recreated to match the current DataFrame schema.
/// All inserts are batched inside a single transaction.
/// Returns the number of rows inserted.
inline auto write_sqlite(const ibex::runtime::Table& table, std::string_view path,
                         std::string_view table_name) -> std::int64_t {
    SqliteDb db(std::string(path), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);

    const std::string qtable = sqlite_quote_ident(table_name);
    const auto& cols = table.columns;
    const std::size_t n_cols = cols.size();
    const std::size_t n_rows = table.rows();

    // Drop and recreate the table.
    sqlite_exec(db.handle, "DROP TABLE IF EXISTS " + qtable);

    if (n_cols > 0) {
        std::string create_sql = "CREATE TABLE " + qtable + " (";
        for (std::size_t c = 0; c < n_cols; ++c) {
            if (c > 0) {
                create_sql += ", ";
            }
            create_sql += sqlite_quote_ident(cols[c].name);
            create_sql += ' ';
            std::visit(
                [&](const auto& col_val) {
                    using ColT = std::decay_t<decltype(col_val)>;
                    if constexpr (std::is_same_v<ColT, ibex::Column<std::int64_t>> ||
                                  std::is_same_v<ColT, ibex::Column<ibex::Date>> ||
                                  std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>) {
                        create_sql += "INTEGER";
                    } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                        create_sql += "REAL";
                    } else {
                        create_sql += "TEXT";
                    }
                },
                *cols[c].column);
        }
        create_sql += ')';
        sqlite_exec(db.handle, create_sql);
    }

    if (n_rows == 0) {
        return 0;
    }

    // Prepare the INSERT statement.
    std::string insert_sql = "INSERT INTO " + qtable + " VALUES (";
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (c > 0) {
            insert_sql += ", ";
        }
        insert_sql += '?';
    }
    insert_sql += ')';
    SqliteStmt stmt(db.handle, insert_sql);

    sqlite_exec(db.handle, "BEGIN TRANSACTION");
    try {
        for (std::size_t r = 0; r < n_rows; ++r) {
            sqlite3_reset(stmt.stmt);
            sqlite3_clear_bindings(stmt.stmt);

            for (std::size_t c = 0; c < n_cols; ++c) {
                const int param = static_cast<int>(c + 1);

                if (ibex::runtime::is_null(cols[c], r)) {
                    sqlite3_bind_null(stmt.stmt, param);
                    continue;
                }

                std::visit(
                    [&](const auto& col_val) {
                        using ColT = std::decay_t<decltype(col_val)>;
                        if constexpr (std::is_same_v<ColT, ibex::Column<std::int64_t>>) {
                            sqlite3_bind_int64(stmt.stmt, param, col_val[r]);
                        } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                            sqlite3_bind_double(stmt.stmt, param, col_val[r]);
                        } else if constexpr (std::is_same_v<ColT, ibex::Column<std::string>>) {
                            auto sv = col_val[r];
                            sqlite3_bind_text(stmt.stmt, param, sv.data(),
                                             static_cast<int>(sv.size()), SQLITE_TRANSIENT);
                        } else if constexpr (std::is_same_v<ColT,
                                                             ibex::Column<ibex::Categorical>>) {
                            auto sv = col_val[r];
                            sqlite3_bind_text(stmt.stmt, param, sv.data(),
                                             static_cast<int>(sv.size()), SQLITE_TRANSIENT);
                        } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                            sqlite3_bind_int64(stmt.stmt, param,
                                              static_cast<std::int64_t>(col_val[r].days));
                        } else if constexpr (std::is_same_v<ColT,
                                                             ibex::Column<ibex::Timestamp>>) {
                            sqlite3_bind_int64(stmt.stmt, param, col_val[r].nanos);
                        }
                    },
                    *cols[c].column);
            }

            if (sqlite3_step(stmt.stmt) != SQLITE_DONE) {
                throw std::runtime_error(std::string("sqlite: insert failed: ") +
                                         sqlite3_errmsg(db.handle));
            }
        }
        sqlite_exec(db.handle, "COMMIT");
    } catch (...) {
        sqlite_exec(db.handle, "ROLLBACK");
        throw;
    }

    return static_cast<std::int64_t>(n_rows);
}
