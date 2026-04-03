#include <catch2/catch_tostring.hpp>

#include <string>
#include <string_view>

// Catch2 v3 is compiled as C++14, so CATCH_CONFIG_CPP17_STRING_VIEW is not
// defined during the library build and StringMaker<std::string_view>::convert
// has no definition in the Catch2 static library. Our tests compile as C++23
// where the declaration IS visible, causing an unresolved symbol at link time.
// Provide the missing definition for toolchains where this mismatch occurs.
#if (defined(__APPLE__) && defined(__clang__) && defined(CATCH_CONFIG_CPP17_STRING_VIEW)) || \
    defined(_MSC_VER)
namespace Catch {

auto StringMaker<std::string_view>::convert(std::string_view str) -> std::string {
    return std::string(str);
}

}  // namespace Catch
#endif
