// This is a singleton because caches are meant to be global state for sharing
// across requests.

#pragma once

#include <memory>

#include "execution/stages/cache.h"
#include "execution/stages/counters.h"
#include "singletons/singleton.h"

namespace delivery {

class CacheSingleton : public Singleton<CacheSingleton> {
 public:
  void initializeFeaturesCaches(size_t feature_store_content_cache_size) {
    content_features_cache_ =
        std::make_unique<FeaturesCache>(feature_store_content_cache_size);
    non_content_features_cache_ =
        std::make_unique<FeaturesCache>(/*maxSize=*/10'000);
  }

  void addCountersCaches(const std::string& name, int64_t global_rates_size,
                         int64_t item_counts_size, int64_t user_counts_size,
                         int64_t query_counts_size,
                         int64_t item_query_counts_size) {
    counters::Caches cache;

    if (global_rates_size == 0) {
      global_rates_size = default_global_rate_cache_size_;
    }
    cache.global_counts_cache =
        std::make_unique<counters::Cache>(global_rates_size);
    if (item_counts_size == 0) {
      item_counts_size = default_cache_size_;
    }
    cache.item_counts_cache =
        std::make_unique<counters::Cache>(item_counts_size);
    if (user_counts_size == 0) {
      user_counts_size = default_cache_size_;
    }
    cache.user_counts_cache =
        std::make_unique<counters::Cache>(user_counts_size);
    if (query_counts_size == 0) {
      query_counts_size = default_cache_size_;
    }
    cache.query_counts_cache =
        std::make_unique<counters::Cache>(query_counts_size);
    if (item_query_counts_size == 0) {
      item_query_counts_size = default_cache_size_;
    }
    cache.item_query_counts_cache =
        std::make_unique<counters::Cache>(item_query_counts_size);

    name_to_counters_caches_[name] = std::move(cache);
  }

  FeaturesCache& contentFeaturesCache() { return *content_features_cache_; }

  FeaturesCache& nonContentFeaturesCache() {
    return *non_content_features_cache_;
  }

  counters::Caches& countersCaches(const std::string& name) {
    return name_to_counters_caches_[name];
  }

 private:
  friend class Singleton;

  CacheSingleton() = default;

  // This is the default maximum size of cache holding global rates.
  const int64_t default_global_rate_cache_size_ = 100;
  // This is the default size of the item, query, and user count caches.
  const int64_t default_cache_size_ = 100'000;

  std::unique_ptr<FeaturesCache> content_features_cache_;
  std::unique_ptr<FeaturesCache> non_content_features_cache_;

  absl::flat_hash_map<std::string, counters::Caches> name_to_counters_caches_;
};
}  // namespace delivery
