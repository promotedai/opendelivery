#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "config/feature_config.h"
#include "execution/feature_context.h"
#include "execution/stages/compute_time_features.h"
#include "gtest/gtest.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

using ::delivery_private_features::TIME_DAY_OF_MONTH;
using ::delivery_private_features::TIME_DAY_OF_MONTH_PERIODIC_COS;
using ::delivery_private_features::TIME_DAY_OF_MONTH_PERIODIC_SIN;
using ::delivery_private_features::TIME_DAY_OF_WEEK;
using ::delivery_private_features::TIME_DAY_OF_WEEK_PERIODIC_COS;
using ::delivery_private_features::TIME_DAY_OF_WEEK_PERIODIC_SIN;
using ::delivery_private_features::TIME_HOUR_OF_DAY;
using ::delivery_private_features::TIME_HOUR_OF_DAY_PERIODIC_COS;
using ::delivery_private_features::TIME_HOUR_OF_DAY_PERIODIC_SIN;
using ::delivery_private_features::TIME_MILLIS_SINCE_MIDNIGHT;
using ::delivery_private_features::TIME_MONTH_OF_YEAR;
using ::delivery_private_features::TIME_MONTH_OF_YEAR_PERIODIC_COS;
using ::delivery_private_features::TIME_MONTH_OF_YEAR_PERIODIC_SIN;

namespace delivery {
// These obviously aren't real trig values.
PeriodicTimeValues SomePeriodicTimeValues() {
  PeriodicTimeValues ret;
  ret.hour_of_day_sin_values = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
  ret.hour_of_day_cos_values = {-0,  -1,  -2,  -3,  -4,  -5,  -6,  -7,
                                -8,  -9,  -10, -11, -12, -13, -14, -15,
                                -16, -17, -18, -19, -20, -21, -22, -23};
  ret.day_of_week_sin_values = {0, 1, 2, 3, 4, 5, 6};
  ret.day_of_week_cos_values = {-0, -1, -2, -3, -4, -5, -6};
  // These correspond to 1-based types.
  ret.day_of_month_sin_values = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                 23, 24, 25, 26, 27, 28, 29, 30, 31};
  ret.day_of_month_cos_values = {-1,  -2,  -3,  -4,  -5,  -6,  -7,  -8,
                                 -9,  -10, -11, -12, -13, -14, -15, -16,
                                 -17, -18, -19, -20, -21, -22, -23, -24,
                                 -25, -26, -27, -28, -29, -30, -31};
  ret.month_of_year_sin_values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  ret.month_of_year_cos_values = {-1, -2, -3, -4,  -5,  -6,
                                  -7, -8, -9, -10, -11, -12};
  return ret;
}

TEST(ComputeTimeFeaturesTest, GetTimezone) {
  EXPECT_EQ(getTimezone("America/Los_Angeles", "us-east-2"),
            "America/Los_Angeles");
  EXPECT_EQ(getTimezone("", "us-east-2"), "America/Chicago");
  EXPECT_EQ(getTimezone("", "us-east-3"), "UTC");
}

TEST(ComputeTimeFeaturesTest, ProcessWellKnownTimeFeatures) {
  std::string timezone = "America/Los_Angeles";
  uint64_t timestamp = 1672425916000;
  FeatureScope scope;
  processWellKnownTimeFeatures(timezone, timestamp, SomePeriodicTimeValues(),
                               scope);

  EXPECT_EQ(scope.int_features.size(), 1);
  EXPECT_EQ(scope.int_features[TIME_MILLIS_SINCE_MIDNIGHT], 38716000);
  EXPECT_EQ(scope.features.size(), 12);
  EXPECT_FLOAT_EQ(scope.features[TIME_HOUR_OF_DAY], 10);
  EXPECT_FLOAT_EQ(scope.features[TIME_HOUR_OF_DAY_PERIODIC_SIN], 10);
  EXPECT_FLOAT_EQ(scope.features[TIME_HOUR_OF_DAY_PERIODIC_COS], -10);
  EXPECT_FLOAT_EQ(scope.features[TIME_DAY_OF_WEEK], 5);
  EXPECT_FLOAT_EQ(scope.features[TIME_DAY_OF_WEEK_PERIODIC_SIN], 5);
  EXPECT_FLOAT_EQ(scope.features[TIME_DAY_OF_WEEK_PERIODIC_COS], -5);
  EXPECT_FLOAT_EQ(scope.features[TIME_DAY_OF_MONTH], 30);
  EXPECT_FLOAT_EQ(scope.features[TIME_DAY_OF_MONTH_PERIODIC_SIN], 30);
  EXPECT_FLOAT_EQ(scope.features[TIME_DAY_OF_MONTH_PERIODIC_COS], -30);
  EXPECT_FLOAT_EQ(scope.features[TIME_MONTH_OF_YEAR], 12);
  EXPECT_FLOAT_EQ(scope.features[TIME_MONTH_OF_YEAR_PERIODIC_SIN], 12);
  EXPECT_FLOAT_EQ(scope.features[TIME_MONTH_OF_YEAR_PERIODIC_COS], -12);
}

TEST(ComputeTimeFeaturesTest, InitializeConfiguredTimeFeatures) {
  auto id_to_metadata = initializeConfiguredTimeFeatures({"birthday", "cake"});
  uint64_t birthday_hash = hashlib::makeHash(std::string("birthday"));
  uint64_t cake_hash = hashlib::makeHash(std::string("cake"));

  EXPECT_EQ(id_to_metadata.size(), 2);
  EXPECT_TRUE(id_to_metadata.contains(birthday_hash));
  auto& metadata = id_to_metadata[birthday_hash];
  EXPECT_EQ(metadata.millis_since_midnight_path,
            millis_since_midnight_prefix + "birthday");
  EXPECT_EQ(metadata.millis_since_midnight_id,
            hashlib::makeHash(millis_since_midnight_prefix + "birthday"));
  EXPECT_EQ(metadata.hour_of_day_path, hour_of_day_prefix + "birthday");
  EXPECT_EQ(metadata.hour_of_day_id,
            hashlib::makeHash(hour_of_day_prefix + "birthday"));
  EXPECT_EQ(metadata.hour_of_day_sin_path, hour_of_day_sin_prefix + "birthday");
  EXPECT_EQ(metadata.hour_of_day_sin_id,
            hashlib::makeHash(hour_of_day_sin_prefix + "birthday"));
  EXPECT_EQ(metadata.hour_of_day_cos_path, hour_of_day_cos_prefix + "birthday");
  EXPECT_EQ(metadata.hour_of_day_cos_id,
            hashlib::makeHash(hour_of_day_cos_prefix + "birthday"));
  EXPECT_EQ(metadata.day_of_week_path, day_of_week_prefix + "birthday");
  EXPECT_EQ(metadata.day_of_week_id,
            hashlib::makeHash(day_of_week_prefix + "birthday"));
  EXPECT_EQ(metadata.day_of_week_sin_path, day_of_week_sin_prefix + "birthday");
  EXPECT_EQ(metadata.day_of_week_sin_id,
            hashlib::makeHash(day_of_week_sin_prefix + "birthday"));
  EXPECT_EQ(metadata.day_of_week_cos_path, day_of_week_cos_prefix + "birthday");
  EXPECT_EQ(metadata.day_of_week_cos_id,
            hashlib::makeHash(day_of_week_cos_prefix + "birthday"));
  EXPECT_EQ(metadata.day_of_month_path, day_of_month_prefix + "birthday");
  EXPECT_EQ(metadata.day_of_month_id,
            hashlib::makeHash(day_of_month_prefix + "birthday"));
  EXPECT_EQ(metadata.day_of_month_sin_path,
            day_of_month_sin_prefix + "birthday");
  EXPECT_EQ(metadata.day_of_month_sin_id,
            hashlib::makeHash(day_of_month_sin_prefix + "birthday"));
  EXPECT_EQ(metadata.day_of_month_cos_path,
            day_of_month_cos_prefix + "birthday");
  EXPECT_EQ(metadata.day_of_month_cos_id,
            hashlib::makeHash(day_of_month_cos_prefix + "birthday"));
  // Settle for checking presence of the other path.
  EXPECT_TRUE(id_to_metadata.contains(cake_hash));
}

TEST(ComputeTimeFeaturesTest, ProcessConfiguredTimeFeatures) {
  uint64_t start_time = 1644965283000;
  std::string timezone = "America/Los_Angeles";
  auto configured_time_features = initializeConfiguredTimeFeatures({"pizza"});
  uint64_t pizza_hash = hashlib::makeHash(std::string("pizza"));
  auto metadata = configured_time_features[pizza_hash];
  FeatureScope scope;
  scope.int_features[pizza_hash] = 1644965783000;
  // This is not configured but we still want to recognize values in this range.
  scope.int_features[hashlib::makeHash(std::string("pizza_secs"))] = 1644965783;
  processConfiguredTimeFeatures(start_time, timezone, configured_time_features,
                                SomePeriodicTimeValues(), scope);

  EXPECT_EQ(scope.features.size(), 11);
  EXPECT_EQ(scope.features[metadata.hour_of_day_id], 14);
  EXPECT_EQ(scope.features[metadata.hour_of_day_sin_id], 14);
  EXPECT_EQ(scope.features[metadata.hour_of_day_cos_id], -14);
  EXPECT_EQ(scope.features[metadata.day_of_week_id], 2);
  EXPECT_EQ(scope.features[metadata.day_of_week_sin_id], 2);
  EXPECT_EQ(scope.features[metadata.day_of_week_cos_id], -2);
  EXPECT_EQ(scope.features[metadata.day_of_month_id], 15);
  EXPECT_EQ(scope.features[metadata.day_of_month_sin_id], 15);
  EXPECT_EQ(scope.features[metadata.day_of_month_cos_id], -15);
  // Time-difference feature for "pizza".
  EXPECT_EQ(scope.features[pizza_hash], 500);
  // Time-difference feature for "pizza_secs".
  EXPECT_EQ(scope.features[hashlib::makeHash(std::string("pizza_secs"))], 500);

  // The 2 we added plus the millis since midnight.
  EXPECT_EQ(scope.int_features.size(), 3);
  EXPECT_EQ(scope.int_features[metadata.millis_since_midnight_id], 53783000);

  // The 11 float features minus the "pizza_secs" time-difference.
  EXPECT_EQ(scope.stranger_feature_paths.size(), 10);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.millis_since_midnight_path],
            metadata.millis_since_midnight_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.hour_of_day_path],
            metadata.hour_of_day_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.hour_of_day_sin_path],
            metadata.hour_of_day_sin_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.hour_of_day_cos_path],
            metadata.hour_of_day_cos_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.day_of_week_path],
            metadata.day_of_week_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.day_of_week_sin_path],
            metadata.day_of_week_sin_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.day_of_week_cos_path],
            metadata.day_of_week_cos_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.day_of_month_path],
            metadata.day_of_month_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.day_of_month_sin_path],
            metadata.day_of_month_sin_id);
  EXPECT_EQ(scope.stranger_feature_paths[metadata.day_of_month_cos_path],
            metadata.day_of_month_cos_id);
}

TEST(ComputeTimeFeaturesTest, RunSync) {
  PeriodicTimeValues periodic = SomePeriodicTimeValues();
  TimeFeaturesConfig config;
  config.time_feature_paths = {"banana"};
  uint64_t banana_hash = hashlib::makeHash(std::string("banana"));
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  FeatureContext context;
  context.initialize(insertions);
  context.processInsertionFeatures(
      "a", [banana_hash](FeatureScope& scope, const FeatureScope&,
                         const FeatureScope&) {
        scope.int_features[banana_hash] = 1644965783000;
      });
  context.processRequestFeatures([banana_hash](FeatureScope& scope) {
    scope.int_features[banana_hash] = 1644965783000;
  });
  context.processUserFeatures([banana_hash](FeatureScope& scope) {
    scope.int_features[banana_hash] = 1644965783000;
  });
  ComputeTimeFeaturesStage stage(0, periodic, config, insertions, 0, "",
                                 context);
  stage.runSync();

  // Just make sure that every scope was affected.
  EXPECT_FALSE(context.getInsertionFeatures("a").features.empty());
  EXPECT_FALSE(
      context.getInsertionFeatures("a").stranger_feature_paths.empty());
  EXPECT_FALSE(context.getRequestFeatures().features.empty());
  EXPECT_FALSE(context.getRequestFeatures().stranger_feature_paths.empty());
  EXPECT_FALSE(context.getUserFeatures().features.empty());
  EXPECT_FALSE(context.getUserFeatures().stranger_feature_paths.empty());
}
}  // namespace delivery
