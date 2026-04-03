#include <catch2/catch_tostring.hpp>

#include <string>
#include <string_view>

#if defined(__APPLE__) && defined(__clang__) && defined(CATCH_CONFIG_CPP17_STRING_VIEW)
namespace Catch {

auto StringMaker<std::string_view>::convert(std::string_view str) -> std::string {
    return std::string(str);
}

}  // namespace Catch
#endif
