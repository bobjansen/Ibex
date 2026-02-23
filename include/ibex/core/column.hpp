#pragma once

#include <algorithm>
#include <concepts>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ibex {

/// Concept constraining valid column element types.
template <typename T>
concept ColumnElement = std::regular<T> && std::totally_ordered<T>;

/// Tag type for dictionary-encoded categorical columns.
struct Categorical {};

/// A typed, owning columnar storage container.
///
/// Column<T> owns a contiguous vector of homogeneously typed values
/// and exposes span-based access for zero-copy interop.
template <typename T>
class Column {
   public:
    using value_type = T;
    using size_type = std::size_t;

    static_assert(ColumnElement<T>,
                  "Column<T> requires T to satisfy ColumnElement (regular + totally ordered).");

    Column() = default;

    explicit Column(std::vector<T> data) : data_(std::move(data)) {}

    Column(std::initializer_list<T> init) : data_(init) {}

    /// Number of elements.
    [[nodiscard]] auto size() const noexcept -> size_type { return data_.size(); }

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

    /// Construct a value in-place.
    template <typename... Args>
        requires std::constructible_from<T, Args...>
    auto emplace_back(Args&&... args) -> T& {
        return data_.emplace_back(std::forward<Args>(args)...);
    }

    /// Assign from count and value.
    void assign(size_type count, const T& value) { data_.assign(count, value); }

    /// Assign from range.
    template <typename InputIt>
        requires std::input_iterator<InputIt>
    void assign(InputIt first, InputIt last) {
        data_.assign(first, last);
    }
    /// Assign from initializer list.
    void assign(std::initializer_list<T> init) { data_.assign(init); }

    /// Insert value before position.
    [[nodiscard]] auto insert(typename std::vector<T>::const_iterator pos, const T& value) ->
        typename std::vector<T>::iterator {
        return data_.insert(pos, value);
    }
    [[nodiscard]] auto insert(typename std::vector<T>::const_iterator pos, T&& value) ->
        typename std::vector<T>::iterator {
        return data_.insert(pos, std::move(value));
    }

    /// Insert count copies of value.
    [[nodiscard]] auto insert(typename std::vector<T>::const_iterator pos, size_type count,
                              const T& value) -> typename std::vector<T>::iterator {
        return data_.insert(pos, count, value);
    }

    /// Insert range.
    template <typename InputIt>
        requires std::input_iterator<InputIt>
    [[nodiscard]] auto insert(typename std::vector<T>::const_iterator pos, InputIt first,
                              InputIt last) -> typename std::vector<T>::iterator {
        return data_.insert(pos, first, last);
    }

    /// Insert initializer list.
    [[nodiscard]] auto insert(typename std::vector<T>::const_iterator pos,
                              std::initializer_list<T> init) -> typename std::vector<T>::iterator {
        return data_.insert(pos, init);
    }

    /// Emplace value before position.
    template <typename... Args>
        requires std::constructible_from<T, Args...>
    [[nodiscard]] auto emplace(typename std::vector<T>::const_iterator pos, Args&&... args) ->
        typename std::vector<T>::iterator {
        return data_.emplace(pos, std::forward<Args>(args)...);
    }

    /// Erase element at position.
    [[nodiscard]] auto erase(typename std::vector<T>::const_iterator pos) ->
        typename std::vector<T>::iterator {
        return data_.erase(pos);
    }
    /// Erase range.
    [[nodiscard]] auto erase(typename std::vector<T>::const_iterator first,
                             typename std::vector<T>::const_iterator last) ->
        typename std::vector<T>::iterator {
        return data_.erase(first, last);
    }

    /// Reserve capacity.
    void reserve(size_type capacity) { data_.reserve(capacity); }

    /// Current capacity.
    [[nodiscard]] auto capacity() const noexcept -> size_type { return data_.capacity(); }

    /// Remove all elements.
    void clear() noexcept { data_.clear(); }

    /// Resize the column (value-initialize new elements).
    void resize(size_type count) { data_.resize(count); }

    /// Resize the column (copy-initialize new elements with value).
    void resize(size_type count, const T& value) { data_.resize(count, value); }

    /// Reduce capacity to fit size.
    void shrink_to_fit() { data_.shrink_to_fit(); }

    /// Remove the last element.
    void pop_back() { data_.pop_back(); }

    /// Whether the column is empty.
    [[nodiscard]] auto empty() const noexcept -> bool { return data_.empty(); }

    /// Max size supported by the allocator.
    [[nodiscard]] auto max_size() const noexcept -> size_type { return data_.max_size(); }

    /// Raw data access.
    [[nodiscard]] auto data() noexcept -> T* { return data_.data(); }
    [[nodiscard]] auto data() const noexcept -> const T* { return data_.data(); }

    /// First and last elements.
    [[nodiscard]] auto front() const -> const T& { return data_.front(); }
    [[nodiscard]] auto front() -> T& { return data_.front(); }
    [[nodiscard]] auto back() const -> const T& { return data_.back(); }
    [[nodiscard]] auto back() -> T& { return data_.back(); }

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
    [[nodiscard]] auto cbegin() const noexcept { return data_.cbegin(); }
    [[nodiscard]] auto cend() const noexcept { return data_.cend(); }
    [[nodiscard]] auto rbegin() noexcept { return data_.rbegin(); }
    [[nodiscard]] auto rend() noexcept { return data_.rend(); }
    [[nodiscard]] auto rbegin() const noexcept { return data_.crbegin(); }
    [[nodiscard]] auto rend() const noexcept { return data_.crend(); }
    [[nodiscard]] auto crbegin() const noexcept { return data_.crbegin(); }
    [[nodiscard]] auto crend() const noexcept { return data_.crend(); }

   private:
    std::vector<T> data_;
};

/// Specialization for categorical columns (dictionary-encoded strings).
template <>
class Column<Categorical> {
   public:
    using value_type = std::string_view;
    using size_type = std::size_t;
    using code_type = std::int32_t;

    Column()
        : dict_(std::make_shared<std::vector<std::string>>()),
          index_(std::make_shared<std::unordered_map<std::string, code_type>>()) {}

    explicit Column(std::vector<std::string> dict)
        : dict_(std::make_shared<std::vector<std::string>>(std::move(dict))),
          index_(std::make_shared<std::unordered_map<std::string, code_type>>()) {
        rebuild_index();
    }

    Column(std::vector<std::string> dict, std::vector<code_type> codes)
        : dict_(std::make_shared<std::vector<std::string>>(std::move(dict))),
          index_(std::make_shared<std::unordered_map<std::string, code_type>>()),
          codes_(std::move(codes)) {
        rebuild_index();
    }

    Column(std::shared_ptr<std::vector<std::string>> dict,
           std::shared_ptr<std::unordered_map<std::string, code_type>> index,
           std::vector<code_type> codes = {})
        : dict_(std::move(dict)), index_(std::move(index)), codes_(std::move(codes)) {}

    [[nodiscard]] auto size() const noexcept -> size_type { return codes_.size(); }
    [[nodiscard]] auto empty() const noexcept -> bool { return codes_.empty(); }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> value_type {
        if (dict_ == nullptr || dict_->empty()) {
            return std::string_view{};
        }
        return (*dict_)[static_cast<std::size_t>(codes_[idx])];
    }

    [[nodiscard]] auto code_at(size_type idx) const noexcept -> code_type { return codes_[idx]; }

    void push_code(code_type code) { codes_.push_back(code); }

    void push_back(value_type value) {
        auto code = find_or_insert(value);
        codes_.push_back(code);
    }

    void reserve(size_type capacity) { codes_.reserve(capacity); }
    void clear() noexcept { codes_.clear(); }

    void resize(size_type count) { codes_.resize(count, 0); }

    [[nodiscard]] auto dictionary() const noexcept -> const std::vector<std::string>& {
        return *dict_;
    }

    [[nodiscard]] auto dictionary_ptr() const noexcept
        -> const std::shared_ptr<std::vector<std::string>>& {
        return dict_;
    }

    [[nodiscard]] auto index_ptr() const noexcept
        -> const std::shared_ptr<std::unordered_map<std::string, code_type>>& {
        return index_;
    }

    [[nodiscard]] auto codes() const noexcept -> const std::vector<code_type>& { return codes_; }

    [[nodiscard]] auto codes_data() noexcept -> code_type* { return codes_.data(); }
    [[nodiscard]] auto codes_data() const noexcept -> const code_type* { return codes_.data(); }

    [[nodiscard]] auto find_code(value_type value) const -> std::optional<code_type> {
        if (index_ == nullptr) {
            return std::nullopt;
        }
        auto it = index_->find(std::string(value));
        if (it == index_->end()) {
            return std::nullopt;
        }
        return it->second;
    }

   private:
    void rebuild_index() {
        index_->clear();
        index_->reserve(dict_->size());
        for (std::size_t i = 0; i < dict_->size(); ++i) {
            index_->emplace((*dict_)[i], static_cast<code_type>(i));
        }
    }

    auto find_or_insert(value_type value) -> code_type {
        if (index_ == nullptr) {
            index_ = std::make_shared<std::unordered_map<std::string, code_type>>();
        }
        auto it = index_->find(std::string(value));
        if (it != index_->end()) {
            return it->second;
        }
        code_type code = static_cast<code_type>(dict_->size());
        dict_->emplace_back(value);
        index_->emplace(dict_->back(), code);
        return code;
    }

    std::shared_ptr<std::vector<std::string>> dict_;
    std::shared_ptr<std::unordered_map<std::string, code_type>> index_;
    std::vector<code_type> codes_;
};

}  // namespace ibex
