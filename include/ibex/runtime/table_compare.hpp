#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <optional>
#include <string>

namespace ibex::runtime {

/// First structural divergence between two runtime tables.
struct TableMismatch {
    std::string location;
    std::string expected;
    std::string actual;

    [[nodiscard]] auto message() const -> std::string;
};

/// Compare runtime tables without rendering them. This is the authoritative
/// parity predicate for schema, metadata, values, validity, and categorical
/// code/dictionary backing.
[[nodiscard]] auto compare_tables(const Table& expected, const Table& actual)
    -> std::optional<TableMismatch>;

}  // namespace ibex::runtime
