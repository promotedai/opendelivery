// These configs all control feature processing in some way.

#pragma once

#include <string>
#include <vector>

#include "config/json.h"

namespace delivery {
struct StrangerFeatureQueueConfig {
  std::string queue_name;

  constexpr static auto properties = std::make_tuple(property(
      &StrangerFeatureQueueConfig::queue_name, "destinationQueueName"));
};

struct SparseFeaturesConfig {
  // The max number of properties we consider for the respective scope.
  uint64_t max_request_properties = 50;
  uint64_t max_insertion_properties = 50;

  StrangerFeatureQueueConfig stranger_feature_queue_config;
  // The proportion [0, 1] of requests that will have their stranger features
  // recorded.
  double stranger_feature_sampling_rate = 0;

  // This is a list of paths to features we want to compute distribution stat
  // features around.
  std::vector<std::string> distribution_feature_paths;

  constexpr static auto properties = std::make_tuple(
      property(&SparseFeaturesConfig::max_request_properties,
               "maxRequestPropertiesSparseFeatures"),
      property(&SparseFeaturesConfig::max_insertion_properties,
               "maxInsertionPropertiesSparseFeatures"),
      property(&SparseFeaturesConfig::stranger_feature_queue_config,
               "featureIDQueueConfig"),
      property(&SparseFeaturesConfig::stranger_feature_sampling_rate,
               "featureIDLogSamplingRate"),
      property(&SparseFeaturesConfig::distribution_feature_paths,
               "distributionFeaturePaths"));
};

struct ExcludeUserFeaturesConfig {
  // This specifies the user feature store field that is used to indicate if
  // user-specific features should be disabled.
  std::string user_property;

  constexpr static auto properties = std::make_tuple(
      property(&ExcludeUserFeaturesConfig::user_property, "userProperty"));
};

struct TimeFeaturesConfig {
  // This is a list of paths to features we know to be "time"s.
  std::vector<std::string> time_feature_paths;

  // This is the timezone we assume the user is in for time calculations (e.g.
  // "America/New_York").
  std::string default_user_timezone;

  constexpr static auto properties = std::make_tuple(
      property(&TimeFeaturesConfig::time_feature_paths, "timeFeaturePaths"),
      property(&TimeFeaturesConfig::default_user_timezone,
               "defaultUserTimezone"));
};
}  // namespace delivery
