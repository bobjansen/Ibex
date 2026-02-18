#pragma once

#include <ibex/core/column.hpp>

#include <cassert>
#include <concepts>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>

namespace ibex {

/// A named column entry: associates a compile-time name tag with a Column<T>.
template <typename Tag, ColumnElement T>
struct NamedColumn {
    using tag_type = Tag;
    using value_type = T;

    Column<T> column;
};

/// Schema descriptor: a pack of (Tag, Type) pairs.
///
/// Usage:
///   struct Price {};
///   struct Volume {};
///   using MySchema = Schema<Price, double, Volume, int64_t>;
template <typename... Pairs>
struct Schema;

// Base case
template <>
struct Schema<> {
    static constexpr std::size_t num_columns = 0;
};

// Recursive: Tag, Type, Rest...
template <typename Tag, ColumnElement T, typename... Rest>
struct Schema<Tag, T, Rest...> {
    static constexpr std::size_t num_columns = 1 + Schema<Rest...>::num_columns;
};

/// A statically typed, schema-aware DataFrame.
///
/// Each column is identified by a tag type and holds a Column<T>
/// whose element type is determined by the schema.
///
/// TODO: Full implementation — currently a structural placeholder
/// demonstrating the typed schema approach.
template <typename SchemaT>
class DataFrame {
public:
    DataFrame() = default;

    /// Number of rows (all columns must have equal length).
    [[nodiscard]] auto rows() const noexcept -> std::size_t { return rows_; }

    /// Number of columns defined by the schema.
    [[nodiscard]] static constexpr auto cols() noexcept -> std::size_t {
        return SchemaT::num_columns;
    }

    /// Validate that all columns have consistent row counts.
    /// TODO: implement once column storage is wired up.
    [[nodiscard]] auto is_valid() const noexcept -> bool { return true; }

private:
    std::size_t rows_ = 0;
};

}  // namespace ibex
