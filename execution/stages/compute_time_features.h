// This stage is responsible for computing all time-based features.
// (Timestamp-deriving logic in hashlib notwithstanding.)

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/stages/stage.h"

namespace delivery {
class Insertion;
class FeatureContext;
struct FeatureScope;
struct TimeFeaturesConfig;
}  // namespace delivery

namespace delivery {
const std::string day_of_week_prefix = "DayOfWeek=";
const std::string hour_of_day_prefix = "HourOfDay=";
const std::string day_of_month_prefix = "DayOfMonth=";
const std::string millis_since_midnight_prefix = "TimeOfDay=";
const std::string hour_of_day_sin_prefix = "HourOfDaySin=";
const std::string hour_of_day_cos_prefix = "HourOfDayCos=";
const std::string day_of_week_sin_prefix = "DayOfWeekSin=";
const std::string day_of_week_cos_prefix = "DayOfWeekCos=";
const std::string day_of_month_sin_prefix = "DayOfMonthSin=";
const std::string day_of_month_cos_prefix = "DayOfMonthCos=";

// See https://ianlondon.github.io/blog/encoding-cyclical-features-24hour-time/.
// This is populated as global state, and just defined here for convenience.
struct PeriodicTimeValues {
  std::vector<float> hour_of_day_sin_values;
  std::vector<float> hour_of_day_cos_values;

  std::vector<float> day_of_week_sin_values;
  std::vector<float> day_of_week_cos_values;

  std::vector<float> day_of_month_sin_values;
  std::vector<float> day_of_month_cos_values;

  std::vector<float> month_of_year_sin_values;
  std::vector<float> month_of_year_cos_values;
};

class ComputeTimeFeaturesStage : public Stage {
 public:
  ComputeTimeFeaturesStage(size_t id,
                           const PeriodicTimeValues& periodic_time_values,
                           const TimeFeaturesConfig& config,
                           const std::vector<delivery::Insertion>& insertions,
                           uint64_t start_time, const std::string& region,
                           FeatureContext& feature_context)
      : Stage(id),
        periodic_time_values_(periodic_time_values),
        config_(config),
        insertions_(insertions),
        start_time_(start_time),
        region_(region),
        feature_context_(feature_context) {}

  std::string name() const override { return "ComputeTimeFeatures"; }

  void runSync() override;

 private:
  const PeriodicTimeValues& periodic_time_values_;
  const TimeFeaturesConfig& config_;
  const std::vector<delivery::Insertion>& insertions_;
  uint64_t start_time_;
  std::string region_;
  FeatureContext& feature_context_;
};

// Implementation details. Declared here for testing.
struct TimeFeatureMetadata {
  std::string millis_since_midnight_path;
  uint64_t millis_since_midnight_id = 0;

  std::string hour_of_day_path;
  uint64_t hour_of_day_id = 0;
  std::string hour_of_day_sin_path;
  uint64_t hour_of_day_sin_id = 0;
  std::string hour_of_day_cos_path;
  uint64_t hour_of_day_cos_id = 0;

  std::string day_of_week_path;
  uint64_t day_of_week_id = 0;
  std::string day_of_week_sin_path;
  uint64_t day_of_week_sin_id = 0;
  std::string day_of_week_cos_path;
  uint64_t day_of_week_cos_id = 0;

  std::string day_of_month_path;
  uint64_t day_of_month_id = 0;
  std::string day_of_month_sin_path;
  uint64_t day_of_month_sin_id = 0;
  std::string day_of_month_cos_path;
  uint64_t day_of_month_cos_id = 0;
};

std::string getTimezone(const std::string& default_user_timezone,
                        const std::string& region);
void processWellKnownTimeFeatures(
    const std::string& timezone, uint64_t timestamp,
    const PeriodicTimeValues& periodic_time_values, FeatureScope& scope);
absl::flat_hash_map<uint64_t, TimeFeatureMetadata>
initializeConfiguredTimeFeatures(
    const std::vector<std::string>& time_feature_paths);
void processConfiguredTimeFeatures(
    uint64_t start_time, const std::string& timezone,
    const absl::flat_hash_map<uint64_t, TimeFeatureMetadata>&
        configured_time_features,
    const PeriodicTimeValues& periodic_time_values, FeatureScope& scope);
}  // namespace delivery
