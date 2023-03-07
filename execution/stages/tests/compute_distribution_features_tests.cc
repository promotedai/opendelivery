#include <algorithm>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/feature_context.h"
#include "execution/stages/compute_distribution_features.h"
#include "gtest/gtest.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
TEST(ComputeDistributionFeaturesTest, InitializeDistributionFeatureMetadata) {
  std::vector<std::string> distribution_feature_paths = {"a", "b"};
  std::vector<DistributionFeatureMetadata> metadata =
      initializeDistributionFeatureMetadata(distribution_feature_paths);

  // The four default ones + two specified.
  EXPECT_EQ(metadata.size(), 6);
  // Examine just one of the defaults.
  EXPECT_EQ(
      metadata[0].base_id,
      delivery_private_features::DAY_30 +
          delivery_private_features::COUNT_NAVIGATE +
          delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_IMPRESSION);
  EXPECT_EQ(metadata[0].set_value_path,
            "DistPropSet=ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].set_value_id,
            hashlib::makeHash(metadata[0].set_value_path));
  EXPECT_EQ(
      metadata[0].non_zero_value_path,
      "DistPropNonZero=ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].non_zero_value_id,
            hashlib::makeHash(metadata[0].non_zero_value_path));
  EXPECT_EQ(metadata[0].percentile_all_path,
            "DistributionPercentileAll=ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_"
            "IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].percentile_all_id,
            hashlib::makeHash(metadata[0].percentile_all_path));
  EXPECT_EQ(metadata[0].percentile_non_zero_path,
            "DistributionPercentileNonZero=ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_"
            "IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].percentile_non_zero_id,
            hashlib::makeHash(metadata[0].percentile_non_zero_path));
  EXPECT_EQ(metadata[0].fraction_median_all_path,
            "DistributionFractionMedianAll=ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_"
            "IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].fraction_median_all_id,
            hashlib::makeHash(metadata[0].fraction_median_all_path));
  EXPECT_EQ(metadata[0].fraction_median_non_zero_path,
            "DistributionFractionMedianNonZero=ITEM_DEVICE_RATE_SMOOTH_"
            "NAVIGATE_IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].fraction_median_non_zero_id,
            hashlib::makeHash(metadata[0].fraction_median_non_zero_path));
  EXPECT_EQ(metadata[0].feature_value_is_zero_path,
            "DistributionFeatureValueIsZero=ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_"
            "IMPRESSION_30DAY");
  EXPECT_EQ(metadata[0].feature_value_is_zero_id,
            hashlib::makeHash(metadata[0].feature_value_is_zero_path));
  // Examine just one of the specified.
  EXPECT_EQ(metadata[4].base_id, hashlib::makeHash(std::string("a")));
  EXPECT_EQ(metadata[4].set_value_path, "DistPropSet=a");
  EXPECT_EQ(metadata[4].set_value_id,
            hashlib::makeHash(metadata[4].set_value_path));
  EXPECT_EQ(metadata[4].non_zero_value_path, "DistPropNonZero=a");
  EXPECT_EQ(metadata[4].non_zero_value_id,
            hashlib::makeHash(metadata[4].non_zero_value_path));
  EXPECT_EQ(metadata[4].percentile_all_path, "DistributionPercentileAll=a");
  EXPECT_EQ(metadata[4].percentile_all_id,
            hashlib::makeHash(metadata[4].percentile_all_path));
  EXPECT_EQ(metadata[4].percentile_non_zero_path,
            "DistributionPercentileNonZero=a");
  EXPECT_EQ(metadata[4].percentile_non_zero_id,
            hashlib::makeHash(metadata[4].percentile_non_zero_path));
  EXPECT_EQ(metadata[4].fraction_median_all_path,
            "DistributionFractionMedianAll=a");
  EXPECT_EQ(metadata[4].fraction_median_all_id,
            hashlib::makeHash(metadata[4].fraction_median_all_path));
  EXPECT_EQ(metadata[4].fraction_median_non_zero_path,
            "DistributionFractionMedianNonZero=a");
  EXPECT_EQ(metadata[4].fraction_median_non_zero_id,
            hashlib::makeHash(metadata[4].fraction_median_non_zero_path));
  EXPECT_EQ(metadata[4].feature_value_is_zero_path,
            "DistributionFeatureValueIsZero=a");
  EXPECT_EQ(metadata[4].feature_value_is_zero_id,
            hashlib::makeHash(metadata[4].feature_value_is_zero_path));
}

TEST(ComputeDistributionFeaturesTest, InitializeInsertionFeatureMetadata) {
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  context.initialize(insertions);
  context.addInsertionFeatures("a", {{100, 1}, {200, 2}});
  context.addInsertionFeatures("b", {{100, 0}});
  std::vector<DistributionFeatureMetadata> metadata = {
      DistributionFeatureMetadata{.base_id = 100},
      DistributionFeatureMetadata{.base_id = 200}};
  initializeInsertionFeatureMetadata(insertions, context, metadata);

  EXPECT_EQ(metadata[0].insertion_features.size(), 2);
  // Insertion b is ordered first because of the zero value.
  EXPECT_EQ(metadata[0].insertion_features[0].insertion_id, "b");
  EXPECT_EQ(metadata[0].insertion_features[0].value, 0);
  EXPECT_EQ(metadata[0].insertion_features[1].insertion_id, "a");
  EXPECT_EQ(metadata[0].insertion_features[1].value, 1);
  EXPECT_EQ(metadata[0].set_count, 2);
  EXPECT_EQ(metadata[0].non_zero_count, 1);

  EXPECT_EQ(metadata[1].insertion_features.size(), 2);
  EXPECT_EQ(metadata[1].insertion_features[0].insertion_id, "b");
  EXPECT_EQ(metadata[1].insertion_features[0].value, 0);
  EXPECT_EQ(metadata[1].insertion_features[1].insertion_id, "a");
  EXPECT_EQ(metadata[1].insertion_features[1].value, 2);
  EXPECT_EQ(metadata[1].set_count, 1);
  EXPECT_EQ(metadata[1].non_zero_count, 1);
}

TEST(ComputeDistributionFeaturesTest, ApplyDistributionFeaturesToRequest) {
  FeatureContext context;
  std::vector<DistributionFeatureMetadata> metadata = {
      DistributionFeatureMetadata{.set_value_path = "a",
                                  .set_value_id = 100,
                                  .non_zero_value_path = "b",
                                  .non_zero_value_id = 101,
                                  .percentile_all_path = "c",
                                  .percentile_all_id = 102,
                                  .percentile_non_zero_path = "d",
                                  .percentile_non_zero_id = 103,
                                  .fraction_median_all_path = "e",
                                  .fraction_median_all_id = 104,
                                  .fraction_median_non_zero_path = "f",
                                  .fraction_median_non_zero_id = 105,
                                  .feature_value_is_zero_path = "g",
                                  .feature_value_is_zero_id = 106,
                                  .set_count = 100,
                                  .non_zero_count = 101}};
  applyDistributionFeaturesToRequest(context, metadata);

  const auto& scope = context.getRequestFeatures();
  EXPECT_EQ(scope.features.size(), 2);
  EXPECT_EQ(scope.features.at(100), 100);
  EXPECT_EQ(scope.features.at(101), 101);
  EXPECT_EQ(scope.stranger_feature_paths.size(), 7);
  EXPECT_EQ(scope.stranger_feature_paths.at("a"), 100);
  EXPECT_EQ(scope.stranger_feature_paths.at("b"), 101);
  EXPECT_EQ(scope.stranger_feature_paths.at("c"), 102);
  EXPECT_EQ(scope.stranger_feature_paths.at("d"), 103);
  EXPECT_EQ(scope.stranger_feature_paths.at("e"), 104);
  EXPECT_EQ(scope.stranger_feature_paths.at("f"), 105);
  EXPECT_EQ(scope.stranger_feature_paths.at("g"), 106);
}

TEST(ComputeDistributionFeaturesTest, CalculateInsertionFeatureStats) {
  std::vector<std::string> id_storage = {"0", "1", "2", "3", "4",
                                         "5", "6", "7", "8", "9"};
  std::vector<DistributionFeatureMetadata> metadata;
  metadata.emplace_back(DistributionFeatureMetadata{
      .set_count = 9,
      .non_zero_count = 8,
      .insertion_features =
          {
              {.insertion_id = id_storage[0], .value = 0},
              {.insertion_id = id_storage[1], .value = 0},
              {.insertion_id = id_storage[2], .value = 1},
              {.insertion_id = id_storage[3], .value = 2},
              {.insertion_id = id_storage[4], .value = 2},
              {.insertion_id = id_storage[5], .value = 2},
              {.insertion_id = id_storage[6], .value = 3},
              {.insertion_id = id_storage[7], .value = 4},
              {.insertion_id = id_storage[8], .value = 5},
              {.insertion_id = id_storage[9], .value = 5},
          },
  });
  calculateInsertionFeatureStats(metadata);

  EXPECT_EQ(metadata[0].all_feature_percentiles.size(), 10);
  EXPECT_EQ(metadata[0].all_feature_percentiles["0"], 0);
  EXPECT_EQ(metadata[0].all_feature_percentiles["1"], 0);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["2"], 2.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["3"], 3.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["4"], 3.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["5"], 3.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["6"], 6.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["7"], 7.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["8"], 8.0 / 9);
  EXPECT_FLOAT_EQ(metadata[0].all_feature_percentiles["9"], 8.0 / 9);
  EXPECT_EQ(metadata[0].non_zero_feature_percentiles.size(), 8);
  EXPECT_EQ(metadata[0].non_zero_feature_percentiles["2"], 0);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["3"], 1.0 / 7);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["4"], 1.0 / 7);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["5"], 1.0 / 7);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["6"], 4.0 / 7);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["7"], 5.0 / 7);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["8"], 6.0 / 7);
  EXPECT_FLOAT_EQ(metadata[0].non_zero_feature_percentiles["9"], 6.0 / 7);
  EXPECT_EQ(metadata[0].median_value_all, 2);
  EXPECT_EQ(metadata[0].median_value_non_zero, 3);
}

TEST(ComputeDistributionFeaturesTest, ApplyDistributionFeaturesToInsertions) {
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  context.initialize(insertions);
  context.addInsertionFeatures("a", {{100, 1}});
  context.addInsertionFeatures("b", {{100, 0}});
  std::vector<DistributionFeatureMetadata> metadata = {
      DistributionFeatureMetadata{
          .base_id = 100,
          .percentile_all_id = 2,
          .percentile_non_zero_id = 3,
          .fraction_median_all_id = 4,
          .fraction_median_non_zero_id = 5,
          .feature_value_is_zero_id = 6,
          .median_value_all = 7,
          .median_value_non_zero = 0,
          .all_feature_percentiles = {{"a", 10}, {"b", 11}},
          .non_zero_feature_percentiles = {{"a", 12}, {"b", 13}},
      }};
  applyDistributionFeaturesToInsertions(insertions, context, metadata);

  {
    const auto& scope = context.getInsertionFeatures("a");
    EXPECT_EQ(scope.features.at(2), 10);
    EXPECT_EQ(scope.features.at(3), 12);
    EXPECT_FLOAT_EQ(scope.features.at(4), 1.0 / 7);
    EXPECT_EQ(scope.features.at(5), 0);
  }
  {
    const auto& scope = context.getInsertionFeatures("b");
    EXPECT_EQ(scope.features.at(2), 11);
    EXPECT_EQ(scope.features.at(3), 13);
    EXPECT_EQ(scope.features.at(6), 1);
  }
}

TEST(ComputeDistributionFeaturesTest, RunSync) {
  std::vector<std::string> distribution_feature_paths = {"a"};
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  context.initialize(insertions);
  ComputeDistributionFeaturesStage stage(0, distribution_feature_paths,
                                         insertions, context);
  stage.runSync();

  // Just confirm work is done at the appropriate scopes.
  EXPECT_FALSE(context.getInsertionFeatures("b").features.empty());
  EXPECT_FALSE(context.getRequestFeatures().features.empty());
  EXPECT_FALSE(context.getRequestFeatures().stranger_feature_paths.empty());
}

TEST(ComputeDistributionFeaturesTest, RunSyncEmptyInsertions) {
  std::vector<std::string> distribution_feature_paths = {"a"};
  std::vector<delivery::Insertion> insertions;
  FeatureContext context;
  ComputeDistributionFeaturesStage stage(0, distribution_feature_paths,
                                         insertions, context);
  stage.runSync();

  EXPECT_TRUE(context.getRequestFeatures().features.empty());
  EXPECT_TRUE(context.getRequestFeatures().stranger_feature_paths.empty());
}
}  // namespace delivery
