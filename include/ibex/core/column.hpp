#pragma once

#include <algorithm>
#include <concepts>
#include <span>
#include <vector>

namespace ibex {

/// Concept constraining valid column element types.
template <typename T>
concept ColumnElement = std::regular<T> && std::totally_ordered<T>;

/// A typed, owning columnar storage container.
///
/// Column<T> owns a contiguous vector of homogeneously typed values
/// and exposes span-based access for zero-copy interop.
template <ColumnElement T>
class Column {
   public:
    using value_type = T;
    using size_type = std::size_t;

    Column() = default;

    explicit Column(std::vector<T> data) : data_(std::move(data)) {}

    Column(std::initializer_list<T> init) : data_(init) {}

    /// Number of elements.
    [[nodiscard]] auto size() const noexcept -> size_type { return data_.size(); }

    /// Whether the column is empty.
    [[nodiscard]] auto empty() const noexcept -> bool { return data_.empty(); }

    /// Immutable element access (bounds-checked).
    [[nodiscard]] auto at(size_type idx) const -> const T& { return data_.at(idx); }

    /// Mutable element access (bounds-checked).
    [[nodiscard]] auto at(size_type idx) -> T& { return data_.at(idx); }

    /// Unchecked element access.
    [[nodiscard]] auto operator[](size_type idx) const noexcept -> const T& { return data_[idx]; }

    /// Unchecked mutable element access.
    [[nodiscard]] auto operator[](size_type idx) noexcept -> T& { return data_[idx]; }

    /// Zero-copy immutable view of the underlying data.
    [[nodiscard]] auto span() const noexcept -> std::span<const T> { return data_; }

    /// Zero-copy mutable view of the underlying data.
    [[nodiscard]] auto span() noexcept -> std::span<T> { return data_; }

    /// Append a value.
    void push_back(const T& value) { data_.push_back(value); }
    void push_back(T&& value) { data_.push_back(std::move(value)); }

    /// Reserve capacity.
    void reserve(size_type capacity) { data_.reserve(capacity); }

    /// Apply a predicate and return a filtered column.
    template <std::predicate<const T&> Pred>
    [[nodiscard]] auto filter(Pred pred) const -> Column<T> {
        std::vector<T> result;
        std::ranges::copy_if(data_, std::back_inserter(result), pred);
        return Column<T>{std::move(result)};
    }

    /// Apply a transform and return a new column.
    template <typename F>
        requires std::invocable<F, const T&>
    [[nodiscard]] auto transform(F func) const -> Column<std::invoke_result_t<F, const T&>> {
        using U = std::invoke_result_t<F, const T&>;
        std::vector<U> result;
        result.reserve(data_.size());
        std::ranges::transform(data_, std::back_inserter(result), func);
        return Column<U>{std::move(result)};
    }

    // Iterator support
    [[nodiscard]] auto begin() noexcept { return data_.begin(); }
    [[nodiscard]] auto end() noexcept { return data_.end(); }
    [[nodiscard]] auto begin() const noexcept { return data_.cbegin(); }
    [[nodiscard]] auto end() const noexcept { return data_.cend(); }

   private:
    std::vector<T> data_;
};

}  // namespace ibex
