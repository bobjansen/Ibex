// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// The libcurl entry points the plugins' HTTPS readers use (parquet::read and
// csv::read on https:// paths), resolved once.
//
// Where the build defines IBEX_LAZY_CURL_SONAME (ELF platforms, see
// cmake/LazyCurl.cmake), libcurl is not linked at all: it is dlopen'ed the
// first time an https:// path is read.
// Linking it put libcurl and its ~30-library dependency chain (TLS, Kerberos,
// LDAP, SSH, ...) into every process start, ~2.5 ms of an empty script's ~6 ms,
// for a feature most runs never touch. Elsewhere the table simply points at the
// linked functions, so the reader has one code path either way.

#include <curl/curl.h>
#include <stdexcept>
#include <string>

#if defined(IBEX_LAZY_CURL_SONAME)
#include <dlfcn.h>
#endif

namespace ibex::lazy_curl {

struct CurlApi {
    decltype(&curl_global_init) global_init = nullptr;
    decltype(&curl_easy_init) easy_init = nullptr;
    decltype(&curl_easy_setopt) easy_setopt = nullptr;
    decltype(&curl_easy_perform) easy_perform = nullptr;
    decltype(&curl_easy_getinfo) easy_getinfo = nullptr;
    decltype(&curl_easy_cleanup) easy_cleanup = nullptr;
    decltype(&curl_easy_strerror) easy_strerror = nullptr;
};

#if defined(IBEX_LAZY_CURL_SONAME)
namespace detail {

template <typename Fn>
auto resolve(void* handle, const char* name, std::string& missing) -> Fn {
    void* symbol = dlsym(handle, name);
    if (symbol == nullptr) {
        missing += missing.empty() ? name : std::string(", ") + name;
    }
    return reinterpret_cast<Fn>(symbol);
}

}  // namespace detail
#endif

/// The resolved entry points. Throws (every time) if libcurl cannot be loaded,
/// with the reason prefixed by `caller` (e.g. "parquet::read"), so a missing
/// libcurl fails only the https:// read that needed it.
inline auto curl_api([[maybe_unused]] const char* caller) -> const CurlApi& {
#if defined(IBEX_LAZY_CURL_SONAME)
    struct Loaded {
        CurlApi api;
        std::string error;
    };
    static const Loaded loaded = [] {
        Loaded out;
        // Never closed: libcurl's global state must outlive every handle, and
        // the process keeps it until exit as it did when linked.
        void* handle = dlopen(IBEX_LAZY_CURL_SONAME, RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            const char* reason = dlerror();
            out.error = std::string("cannot load ") + IBEX_LAZY_CURL_SONAME + " (" +
                        (reason != nullptr ? reason : "unknown error") + ")";
            return out;
        }
        std::string missing;
        using detail::resolve;
        out.api.global_init =
            resolve<decltype(&curl_global_init)>(handle, "curl_global_init", missing);
        out.api.easy_init = resolve<decltype(&curl_easy_init)>(handle, "curl_easy_init", missing);
        out.api.easy_setopt =
            resolve<decltype(&curl_easy_setopt)>(handle, "curl_easy_setopt", missing);
        out.api.easy_perform =
            resolve<decltype(&curl_easy_perform)>(handle, "curl_easy_perform", missing);
        out.api.easy_getinfo =
            resolve<decltype(&curl_easy_getinfo)>(handle, "curl_easy_getinfo", missing);
        out.api.easy_cleanup =
            resolve<decltype(&curl_easy_cleanup)>(handle, "curl_easy_cleanup", missing);
        out.api.easy_strerror =
            resolve<decltype(&curl_easy_strerror)>(handle, "curl_easy_strerror", missing);
        if (!missing.empty()) {
            out.error = std::string(IBEX_LAZY_CURL_SONAME) + " lacks " + missing;
        }
        return out;
    }();
    if (!loaded.error.empty()) {
        throw std::runtime_error(std::string(caller) +
                                 ": HTTPS support unavailable: " + loaded.error);
    }
    return loaded.api;
#else
    static const CurlApi linked{
        .global_init = &curl_global_init,
        .easy_init = &curl_easy_init,
        .easy_setopt = &curl_easy_setopt,
        .easy_perform = &curl_easy_perform,
        .easy_getinfo = &curl_easy_getinfo,
        .easy_cleanup = &curl_easy_cleanup,
        .easy_strerror = &curl_easy_strerror,
    };
    return linked;
#endif
}

}  // namespace ibex::lazy_curl
