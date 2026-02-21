#pragma once

#include <any>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace ibex::runtime {

/// Type-erased external function wrapper.
///
/// Stores C++ callables for interop with Ibex queries.
/// Functions are registered by name and can be looked up at runtime.
class ExternRegistry {
   public:
    /// Callable signature: type-erased via std::any.
    /// In practice, callers must know the expected signature.
    using ErasedFunc = std::any;

    ExternRegistry() = default;

    /// Register an external function by name.
    /// Overwrites any previously registered function with the same name.
    template <typename F>
    void register_func(std::string name, F&& func) {
        registry_.insert_or_assign(std::move(name), ErasedFunc{std::forward<F>(func)});
    }

    /// Look up a registered function by name.
    [[nodiscard]] auto find(const std::string& name) const -> std::optional<ErasedFunc> {
        if (auto it = registry_.find(name); it != registry_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    /// Retrieve a function with a specific type, or throw.
    template <typename F>
    [[nodiscard]] auto get(const std::string& name) const -> F {
        auto it = registry_.find(name);
        if (it == registry_.end()) {
            throw std::runtime_error("ExternRegistry: function not found: " + name);
        }
        return std::any_cast<F>(it->second);
    }

    /// Check whether a function is registered.
    [[nodiscard]] auto contains(const std::string& name) const -> bool {
        return registry_.contains(name);
    }

    /// Number of registered functions.
    [[nodiscard]] auto size() const noexcept -> std::size_t { return registry_.size(); }

   private:
    std::unordered_map<std::string, ErasedFunc> registry_;
};

}  // namespace ibex::runtime
