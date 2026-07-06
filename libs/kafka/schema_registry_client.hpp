#pragma once

#include <algorithm>
#include <cctype>
#include <expected>
#include <functional>
#include <mutex>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>

#include "schema_registry.hpp"

#if defined(IBEX_KAFKA_ENABLE_CURL)
#include <curl/curl.h>
#endif

namespace ibex_kafka {

struct SchemaRegistryClientOptions {
    long connect_timeout_ms = 2000;
    long request_timeout_ms = 5000;
};

inline constexpr std::string_view transient_schema_registry_error_prefix =
    "transient schema registry error: ";

inline auto make_transient_schema_registry_error(std::string message) -> std::string {
    return std::string(transient_schema_registry_error_prefix) + std::move(message);
}

inline auto is_transient_schema_registry_error(std::string_view message) -> bool {
    return message.rfind(transient_schema_registry_error_prefix, 0) == 0;
}

inline auto is_transient_schema_registry_http_status(long status) -> bool {
    return status == 408 || status == 425 || status == 429 || (status >= 500 && status <= 599);
}

inline auto trim_registry_url(std::string_view text) -> std::string_view {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0) {
        text.remove_suffix(1);
    }
    return text;
}

inline auto normalize_schema_registry_url(std::string_view base_url)
    -> std::expected<std::string, std::string> {
    base_url = trim_registry_url(base_url);
    while (!base_url.empty() && base_url.back() == '/') {
        base_url.remove_suffix(1);
    }
    if (base_url.empty()) {
        return std::unexpected("schema registry URL must not be empty");
    }
    return std::string(base_url);
}

inline auto schema_registry_schema_by_id_url(std::string_view base_url, std::int32_t schema_id)
    -> std::expected<std::string, std::string> {
    if (schema_id < 0) {
        return std::unexpected("schema registry id must be non-negative");
    }
    auto normalized = normalize_schema_registry_url(base_url);
    if (!normalized) {
        return std::unexpected(normalized.error());
    }
    return *normalized + "/schemas/ids/" + std::to_string(schema_id);
}

inline auto schema_registry_supported_types_url(std::string_view base_url)
    -> std::expected<std::string, std::string> {
    auto normalized = normalize_schema_registry_url(base_url);
    if (!normalized) {
        return std::unexpected(normalized.error());
    }
    return *normalized + "/schemas/types";
}

#if defined(IBEX_KAFKA_ENABLE_CURL)
inline auto is_transient_schema_registry_curl_error(CURLcode rc) -> bool {
    switch (rc) {
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_COULDNT_CONNECT:
        case CURLE_OPERATION_TIMEDOUT:
        case CURLE_SEND_ERROR:
        case CURLE_RECV_ERROR:
        case CURLE_GOT_NOTHING:
            return true;
        default:
            return false;
    }
}

inline auto curl_schema_registry_write(char* ptr, std::size_t size, std::size_t nmemb,
                                       void* userdata) -> std::size_t {
    auto* out = static_cast<std::string*>(userdata);
    const std::size_t bytes = size * nmemb;
    out->append(ptr, bytes);
    return bytes;
}

inline auto curl_schema_registry_get(std::string_view url,
                                     const SchemaRegistryClientOptions& options)
    -> std::expected<std::string, std::string> {
    static const bool curl_initialized = []() {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        return true;
    }();
    (void)curl_initialized;

    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        return std::unexpected("schema registry HTTP init failed");
    }

    std::string body;
    curl_easy_setopt(curl, CURLOPT_URL, std::string(url).c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_schema_registry_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, options.connect_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, options.request_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "ibex-kafka-schema-registry/1");

    const CURLcode rc = curl_easy_perform(curl);
    if (rc != CURLE_OK) {
        long status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
        std::string error = "schema registry GET failed";
        if (status > 0) {
            error += " (HTTP " + std::to_string(status) + ")";
        }
        error += ": ";
        error += curl_easy_strerror(rc);
        curl_easy_cleanup(curl);
        if (is_transient_schema_registry_http_status(status) ||
            is_transient_schema_registry_curl_error(rc)) {
            return std::unexpected(make_transient_schema_registry_error(std::move(error)));
        }
        return std::unexpected(error);
    }

    curl_easy_cleanup(curl);
    return body;
}
#endif

class SchemaRegistryClient {
   public:
    using Getter = std::function<std::expected<std::string, std::string>(std::string_view url)>;

    explicit SchemaRegistryClient(std::string base_url, SchemaRegistryClientOptions options = {},
                                  Getter getter = {})
        : base_url_(std::move(base_url)), options_(options), getter_(std::move(getter)) {}

    auto normalized_base_url() const -> std::expected<std::string, std::string> {
        return normalize_schema_registry_url(base_url_);
    }

    auto fetch_schema_by_id(std::int32_t schema_id)
        -> std::expected<RegistrySchemaEntry, std::string> {
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = schema_cache_.find(schema_id);
            if (it != schema_cache_.end()) {
                return it->second;
            }
        }

        auto url = schema_registry_schema_by_id_url(base_url_, schema_id);
        if (!url) {
            return std::unexpected(url.error());
        }
        auto body = get(*url);
        if (!body) {
            return std::unexpected(body.error());
        }
        auto parsed = parse_schema_registry_entry(*body);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        if (parsed->id == 0) {
            parsed->id = schema_id;
        } else if (parsed->id != schema_id) {
            return std::unexpected("schema registry returned schema id " +
                                   std::to_string(parsed->id) + " for request id " +
                                   std::to_string(schema_id));
        }

        std::lock_guard<std::mutex> lock(mu_);
        schema_cache_[schema_id] = *parsed;
        return *parsed;
    }

    auto fetch_supported_types() -> std::expected<std::vector<RegistrySchemaType>, std::string> {
        auto url = schema_registry_supported_types_url(base_url_);
        if (!url) {
            return std::unexpected(url.error());
        }
        auto body = get(*url);
        if (!body) {
            return std::unexpected(body.error());
        }
        return parse_schema_registry_supported_types(*body);
    }

   private:
    auto get(std::string_view url) -> std::expected<std::string, std::string> {
        if (getter_) {
            return getter_(url);
        }
#if defined(IBEX_KAFKA_ENABLE_CURL)
        return curl_schema_registry_get(url, options_);
#else
        (void)options_;
        return std::unexpected("schema registry HTTP support is not enabled in this build");
#endif
    }

    std::string base_url_;
    SchemaRegistryClientOptions options_;
    Getter getter_;
    std::mutex mu_;
    robin_hood::unordered_map<std::int32_t, RegistrySchemaEntry> schema_cache_;
};

}  // namespace ibex_kafka
