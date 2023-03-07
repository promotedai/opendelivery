// This is the top-level config for the delivery service.

#pragma once

#include <stdint.h>

#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>

#include "config/counters_config.h"
#include "config/execution_config.h"
#include "config/feature_config.h"
#include "config/feature_store_config.h"
#include "config/json.h"
#include "config/paging_config.h"
#include "config/personalize_config.h"

namespace delivery {
struct PlatformConfig {
  uint64_t platform_id = 0;
  std::string region;
  std::string name;

  // Might need to make this optional.
  PagingConfig paging_config;

  std::vector<FeatureStoreConfig> feature_store_configs;
  // This is the number of items to be cached from content feature store.
  // Specified at this level of the config because said cache is global state.
  uint64_t feature_store_content_cache_size = 100'000;
  std::string feature_store_timeout;

  std::unordered_map<std::string, CountersConfig> counters_configs;

  std::vector<PersonalizeConfig> personalize_configs;

  // Various configs related to feature handling. Down the line we should
  // combine some of these.
  SparseFeaturesConfig sparse_features_config;
  // Presence indicates if exclusion is enabled.
  std::optional<ExcludeUserFeaturesConfig> exclude_user_features_config;
  TimeFeaturesConfig time_features_config;

  // This isn't found in any actual configs yet. This is experimental and
  // specific to delivery-cpp for the time being.
  ExecutionConfig execution_config = defaultExecutionConfig();

  constexpr static auto properties = std::make_tuple(
      property(&PlatformConfig::platform_id, "platformId"),
      property(&PlatformConfig::region, "region"),
      property(&PlatformConfig::name, "name"),
      property(&PlatformConfig::paging_config, "pagingConfig"),
      property(&PlatformConfig::feature_store_configs, "featureStores"),
      property(&PlatformConfig::feature_store_content_cache_size,
               "featureStoreLocalCacheSize"),
      property(&PlatformConfig::feature_store_timeout, "featureStoreTimeoutCpp"),
      property(&PlatformConfig::counters_configs, "countersConfigs"),
      property(&PlatformConfig::personalize_configs, "personalizes"),
      property(&PlatformConfig::sparse_features_config, "sparseFeaturesConfig"),
      property(&PlatformConfig::exclude_user_features_config,
               "excludePersonalFeaturesConfig"),
      property(&PlatformConfig::time_features_config, "derivedFeaturesConfig"),
      property(&PlatformConfig::execution_config, "executionConfig"));

 private:
  ExecutionConfig defaultExecutionConfig();
};
}  // namespace delivery
