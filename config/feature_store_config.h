// These are options particular to a single feature store.

#pragma once

#include <string>
#include <vector>

#include "config/json.h"

namespace delivery {
struct FeatureStoreConfig {
  std::string table;
  std::string primary_key;
  uint64_t type = 0;
  std::vector<std::string> feature_columns = {"features"};

  constexpr static auto properties = std::make_tuple(
      property(&FeatureStoreConfig::table, "table"),
      property(&FeatureStoreConfig::primary_key, "pk"),
      property(&FeatureStoreConfig::type, "type"),
      property(&FeatureStoreConfig::feature_columns, "featureColumns"));
};
}  // namespace delivery
