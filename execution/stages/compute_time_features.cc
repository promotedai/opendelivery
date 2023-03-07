#include "execution/stages/compute_time_features.h"

#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
#include "config/feature_config.h"
#include "date/date.h"
#include "date/tz.h"
#include "execution/feature_context.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
// The smallest timestamp we consider is 2000-01-01 00:00:00.
const int64_t min_timestamp_secs = 946684800;
const int64_t min_timestamp_millis = min_timestamp_secs * 1000;

// The largest timestamp we consider is 2050-02-11 00:00:00.
const int64_t max_timestamp_secs = 2528150400;
const int64_t max_timestamp_millis = 2528150400 * 1000;

// Helper class to turn a timestamp into the various bits of date/time info we
// care about.
struct ProcessedTimestamp {
  ProcessedTimestamp(const std::string& timezone, uint64_t timestamp) {
    // All Unix timestamps are in reference to UTC. This gets a time point for
    // the given timezone when the UTC timezone was at the given timestamp.
    auto local_time =
        date::make_zoned(timezone,
                         std::chrono::time_point<std::chrono::system_clock>(
                             std::chrono::milliseconds(timestamp)))
            .get_local_time();
    // This is a time point for midnight.
    auto midnight = date::floor<date::days>(local_time);
    // This is just a tuple - not a time point.
    auto ymd = date::year_month_day{midnight};

    millis_since_midnight =
        std::chrono::duration_cast<std::chrono::milliseconds>(local_time -
                                                              midnight)
            .count();
    // 0-based.
    hour_of_day =
        std::chrono::duration_cast<std::chrono::hours>(local_time - midnight)
            .count();
    // 0-based.
    day_of_week = date::weekday(midnight).c_encoding();
    // 1-based, so we need to subtract 1 when using this as a vector index.
    day_of_month = static_cast<unsigned>(ymd.day());
    // 1-based, so we need to subtract 1 when using this as a vector index.
    month_of_year = static_cast<unsigned>(ymd.month());
  }

  int64_t millis_since_midnight;
  int64_t hour_of_day;
  int64_t day_of_week;
  int64_t day_of_month;
  int64_t month_of_year;
};

std::string getTimezone(const std::string& default_user_timezone,
                        const std::string& region) {
  if (!default_user_timezone.empty()) {
    return default_user_timezone;
  }
  if (region == "us-east-1") {
    return "America/New_York";
  }
  if (region == "us-east-2") {
    return "America/Chicago";
  }
  // Keep this to the bottom.
  return "UTC";
}

bool isProbableTimestampSecs(int64_t val) {
  return val >= min_timestamp_secs && val <= max_timestamp_secs;
}

bool isProbableTimestampMillis(int64_t val) {
  return val >= min_timestamp_millis && val <= max_timestamp_millis;
}

void processWellKnownTimeFeatures(
    const std::string& timezone, uint64_t timestamp,
    const PeriodicTimeValues& periodic_time_values, FeatureScope& scope) {
  ProcessedTimestamp processed(timezone, timestamp);

  scope.int_features[delivery_private_features::TIME_MILLIS_SINCE_MIDNIGHT] =
      processed.millis_since_midnight;

  scope.features[delivery_private_features::TIME_HOUR_OF_DAY] =
      static_cast<float>(processed.hour_of_day);
  scope.features[delivery_private_features::TIME_HOUR_OF_DAY_PERIODIC_SIN] =
      periodic_time_values.hour_of_day_sin_values[processed.hour_of_day];
  scope.features[delivery_private_features::TIME_HOUR_OF_DAY_PERIODIC_COS] =
      periodic_time_values.hour_of_day_cos_values[processed.hour_of_day];

  scope.features[delivery_private_features::TIME_DAY_OF_WEEK] =
      static_cast<float>(processed.day_of_week);
  scope.features[delivery_private_features::TIME_DAY_OF_WEEK_PERIODIC_SIN] =
      periodic_time_values.day_of_week_sin_values[processed.day_of_week];
  scope.features[delivery_private_features::TIME_DAY_OF_WEEK_PERIODIC_COS] =
      periodic_time_values.day_of_week_cos_values[processed.day_of_week];

  scope.features[delivery_private_features::TIME_DAY_OF_MONTH] =
      static_cast<float>(processed.day_of_month);
  scope.features[delivery_private_features::TIME_DAY_OF_MONTH_PERIODIC_SIN] =
      periodic_time_values.day_of_month_sin_values[processed.day_of_month - 1];
  scope.features[delivery_private_features::TIME_DAY_OF_MONTH_PERIODIC_COS] =
      periodic_time_values.day_of_month_cos_values[processed.day_of_month - 1];

  scope.features[delivery_private_features::TIME_MONTH_OF_YEAR] =
      static_cast<float>(processed.month_of_year);
  scope.features[delivery_private_features::TIME_MONTH_OF_YEAR_PERIODIC_SIN] =
      periodic_time_values
          .month_of_year_sin_values[processed.month_of_year - 1];
  scope.features[delivery_private_features::TIME_MONTH_OF_YEAR_PERIODIC_COS] =
      periodic_time_values
          .month_of_year_cos_values[processed.month_of_year - 1];
}

absl::flat_hash_map<uint64_t, TimeFeatureMetadata>
initializeConfiguredTimeFeatures(
    const std::vector<std::string>& time_feature_paths) {
  absl::flat_hash_map<uint64_t, TimeFeatureMetadata> ret;
  ret.reserve(time_feature_paths.size());
  for (const auto& path : time_feature_paths) {
    uint64_t base_id = hashlib::makeHash(path);
    TimeFeatureMetadata metadata;

    metadata.millis_since_midnight_path =
        absl::StrCat(millis_since_midnight_prefix, path);
    metadata.millis_since_midnight_id =
        hashlib::makeHash(metadata.millis_since_midnight_path);

    metadata.hour_of_day_path = absl::StrCat(hour_of_day_prefix, path);
    metadata.hour_of_day_id = hashlib::makeHash(metadata.hour_of_day_path);
    metadata.hour_of_day_sin_path = absl::StrCat(hour_of_day_sin_prefix, path);
    metadata.hour_of_day_sin_id =
        hashlib::makeHash(metadata.hour_of_day_sin_path);
    metadata.hour_of_day_cos_path = absl::StrCat(hour_of_day_cos_prefix, path);
    metadata.hour_of_day_cos_id =
        hashlib::makeHash(metadata.hour_of_day_cos_path);

    metadata.day_of_week_path = absl::StrCat(day_of_week_prefix, path);
    metadata.day_of_week_id = hashlib::makeHash(metadata.day_of_week_path);
    metadata.day_of_week_sin_path = absl::StrCat(day_of_week_sin_prefix, path);
    metadata.day_of_week_sin_id =
        hashlib::makeHash(metadata.day_of_week_sin_path);
    metadata.day_of_week_cos_path = absl::StrCat(day_of_week_cos_prefix, path);
    metadata.day_of_week_cos_id =
        hashlib::makeHash(metadata.day_of_week_cos_path);

    metadata.day_of_month_path = absl::StrCat(day_of_month_prefix, path);
    metadata.day_of_month_id = hashlib::makeHash(metadata.day_of_month_path);
    metadata.day_of_month_sin_path =
        absl::StrCat(day_of_month_sin_prefix, path);
    metadata.day_of_month_sin_id =
        hashlib::makeHash(metadata.day_of_month_sin_path);
    metadata.day_of_month_cos_path =
        absl::StrCat(day_of_month_cos_prefix, path);
    metadata.day_of_month_cos_id =
        hashlib::makeHash(metadata.day_of_month_cos_path);

    ret[base_id] = std::move(metadata);
  }
  return ret;
}

void processConfiguredTimeFeatures(
    uint64_t start_time, const std::string& timezone,
    const absl::flat_hash_map<uint64_t, TimeFeatureMetadata>&
        configured_time_features,
    const PeriodicTimeValues& periodic_time_values, FeatureScope& scope) {
  // These are just probable, rather than configured.
  for (const auto& [k, v] : scope.int_features) {
    // Grab the difference between a likely timestamp and the request time, then
    // write it back in in seconds. We don't need to add metadata since that
    // will have been done when `int_features` was populated.
    if (isProbableTimestampMillis(v)) {
      scope.features[k] = static_cast<float>(v - start_time) / 1000;
    } else if (isProbableTimestampSecs(v)) {
      // The unnecessary multiplication is done to preserve precision.
      scope.features[k] = static_cast<float>(v * 1000 - start_time) / 1000;
    }
  }

  for (const auto& [id, metadata] : configured_time_features) {
    auto it = scope.int_features.find(id);
    if (it == scope.int_features.end()) {
      continue;
    }

    ProcessedTimestamp processed(timezone, it->second);

    scope.int_features[metadata.millis_since_midnight_id] =
        processed.millis_since_midnight;
    scope.stranger_feature_paths[metadata.millis_since_midnight_path] =
        metadata.millis_since_midnight_id;

    scope.features[metadata.hour_of_day_id] =
        static_cast<float>(processed.hour_of_day);
    scope.stranger_feature_paths[metadata.hour_of_day_path] =
        metadata.hour_of_day_id;
    scope.features[metadata.hour_of_day_sin_id] =
        periodic_time_values.hour_of_day_sin_values[processed.hour_of_day];
    scope.stranger_feature_paths[metadata.hour_of_day_sin_path] =
        metadata.hour_of_day_sin_id;
    scope.features[metadata.hour_of_day_cos_id] =
        periodic_time_values.hour_of_day_cos_values[processed.hour_of_day];
    scope.stranger_feature_paths[metadata.hour_of_day_cos_path] =
        metadata.hour_of_day_cos_id;

    scope.features[metadata.day_of_week_id] =
        static_cast<float>(processed.day_of_week);
    scope.stranger_feature_paths[metadata.day_of_week_path] =
        metadata.day_of_week_id;
    scope.features[metadata.day_of_week_sin_id] =
        periodic_time_values.day_of_week_sin_values[processed.day_of_week];
    scope.stranger_feature_paths[metadata.day_of_week_sin_path] =
        metadata.day_of_week_sin_id;
    scope.features[metadata.day_of_week_cos_id] =
        periodic_time_values.day_of_week_cos_values[processed.day_of_week];
    scope.stranger_feature_paths[metadata.day_of_week_cos_path] =
        metadata.day_of_week_cos_id;

    scope.features[metadata.day_of_month_id] =
        static_cast<float>(processed.day_of_month);
    scope.stranger_feature_paths[metadata.day_of_month_path] =
        metadata.day_of_month_id;
    scope.features[metadata.day_of_month_sin_id] =
        periodic_time_values
            .day_of_month_sin_values[processed.day_of_month - 1];
    scope.stranger_feature_paths[metadata.day_of_month_sin_path] =
        metadata.day_of_month_sin_id;
    scope.features[metadata.day_of_month_cos_id] =
        periodic_time_values
            .day_of_month_cos_values[processed.day_of_month - 1];
    scope.stranger_feature_paths[metadata.day_of_month_cos_path] =
        metadata.day_of_month_cos_id;
  }
}

void ComputeTimeFeaturesStage::runSync() {
  std::string timezone = getTimezone(config_.default_user_timezone, region_);

  // Well-known features are processed regardless of any configuration.
  feature_context_.processRequestFeatures(
      [this, &timezone](FeatureScope& scope) {
        processWellKnownTimeFeatures(timezone, start_time_,
                                     periodic_time_values_, scope);
      });

  // If there's no relevant configuration, bail out.
  if (config_.time_feature_paths.empty()) {
    return;
  }

  // Because we intend to eventually do live config reloading, we don't leave
  // this to global state. But we can cache this based on the config if this
  // ends up being expensive.
  auto configured_time_features =
      initializeConfiguredTimeFeatures(config_.time_feature_paths);

  // Process all scopes.
  feature_context_.processUserFeatures(
      [this, &timezone, &configured_time_features](FeatureScope& scope) {
        processConfiguredTimeFeatures(start_time_, timezone,
                                      configured_time_features,
                                      periodic_time_values_, scope);
      });
  feature_context_.processRequestFeatures(
      [this, &timezone, &configured_time_features](FeatureScope& scope) {
        processConfiguredTimeFeatures(start_time_, timezone,
                                      configured_time_features,
                                      periodic_time_values_, scope);
      });
  for (const auto& insertion : insertions_) {
    feature_context_.processInsertionFeatures(
        insertion.content_id(),
        [this, &timezone, &configured_time_features](
            FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
          processConfiguredTimeFeatures(start_time_, timezone,
                                        configured_time_features,
                                        periodic_time_values_, scope);
        });
  }
}
}  // namespace delivery
