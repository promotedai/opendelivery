// Home to counters-specific config options.

#pragma once

#include <string>
#include <vector>

#include "config/json.h"

namespace delivery {
// This is to simplify feature ID specification in configs.
struct SplitFeatureID {
  bool is_unattributed = false;
  std::string window;
  std::string agg_value;
  std::string type;

  constexpr static auto properties = std::make_tuple(
      property(&SplitFeatureID::is_unattributed, "isUnattributed"),
      property(&SplitFeatureID::window, "window"),
      property(&SplitFeatureID::agg_value, "aggValue"),
      property(&SplitFeatureID::type, "type"));
};

struct CountersCacheConfig {
  int64_t global_rates_size = 0;
  int64_t item_counts_size = 0;
  int64_t user_counts_size = 0;
  int64_t query_counts_size = 0;
  int64_t item_query_counts_size = 0;

  constexpr static auto properties = std::make_tuple(
      property(&CountersCacheConfig::global_rates_size, "globalRatesSize"),
      property(&CountersCacheConfig::item_counts_size, "itemCountsSize"),
      property(&CountersCacheConfig::user_counts_size, "userCountsSize"),
      property(&CountersCacheConfig::query_counts_size, "queryCountsSize"),
      property(&CountersCacheConfig::item_query_counts_size,
               "itemQueryCountsSize"));
};

struct CountersConfig {
  // This is the full Redis connection string.
  std::string url;

  // Unfortunately our configs represent this as a string for the time being.
  // This is in milliseconds.
  std::string timeout;

  CountersCacheConfig cache_config;

  // Which feature IDs will be extracted from tables. If empty/unset, all
  // features will be enabled. Feature IDs which don't appear in a particular
  // table are skipped for that table. Rate feature IDs are not specified
  // themselves, but are deduced from specification of the numerator and
  // denominator. The "split" representation is for easier specification.
  std::vector<SplitFeatureID> enabled_model_features;

  constexpr static auto properties =
      std::make_tuple(property(&CountersConfig::url, "url"),
                      property(&CountersConfig::timeout, "timeout"),
                      property(&CountersConfig::cache_config, "cache"),
                      property(&CountersConfig::enabled_model_features,
                               "enabledModelFeatures"));
};
}  // namespace delivery
