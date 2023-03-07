#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "config/feature_config.h"
#include "execution/feature_context.h"
#include "execution/stages/exclude_user_features.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
const uint64_t user_feature = 100;  // Not really.
const uint64_t user_tainted_request_feature =
    delivery_private_features::CountWindow::NONE +
    delivery_private_features::AggMetric::COUNT_IMPRESSION +
    delivery_private_features::CountType::USER_QUERY_HOURS_AGO;
const uint64_t user_tainted_insertion_feature =
    delivery_private_features::CountWindow::NONE +
    delivery_private_features::AggMetric::COUNT_IMPRESSION +
    delivery_private_features::CountType::USER_ITEM_HOURS_AGO;

TEST(ExcludeUserFeaturesStageTest, DontExclude) {
  bool ignore_usage = false;
  std::optional<ExcludeUserFeaturesConfig> config;
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  FeatureContext context;
  context.initialize(insertions);
  context.addUserFeatures({{user_feature, 7}});
  context.addRequestFeatures({{user_tainted_request_feature, 8}});
  context.addInsertionFeatures("a", {{user_tainted_insertion_feature, 9}});
  ExcludeUserFeaturesStage stage(0, ignore_usage, config, context, insertions);
  stage.runSync();

  ASSERT_TRUE(context.getUserFeatures().features.contains(user_feature));
  EXPECT_EQ(context.getUserFeatures().features.at(user_feature), 7);
  ASSERT_TRUE(context.getRequestFeatures().features.contains(
      user_tainted_request_feature));
  EXPECT_EQ(
      context.getRequestFeatures().features.at(user_tainted_request_feature),
      8);
  ASSERT_TRUE(context.getInsertionFeatures("a").features.contains(
      user_tainted_insertion_feature));
  EXPECT_EQ(context.getInsertionFeatures("a").features.at(
                user_tainted_insertion_feature),
            9);
}

TEST(ExcludeUserFeaturesStageTest, IgnoreUsage) {
  bool ignore_usage = true;
  std::optional<ExcludeUserFeaturesConfig> config;
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  FeatureContext context;
  context.initialize(insertions);
  context.addUserFeatures({{user_feature, 7}});
  context.addRequestFeatures({{user_tainted_request_feature, 8}});
  context.addInsertionFeatures("a", {{user_tainted_insertion_feature, 9}});
  ExcludeUserFeaturesStage stage(0, ignore_usage, config, context, insertions);
  stage.runSync();

  EXPECT_FALSE(context.getUserFeatures().features.contains(user_feature));
  ASSERT_TRUE(context.getRequestFeatures().features.contains(
      user_tainted_request_feature));
  EXPECT_EQ(
      context.getRequestFeatures().features.at(user_tainted_request_feature),
      0);
  ASSERT_TRUE(context.getInsertionFeatures("a").features.contains(
      user_tainted_insertion_feature));
  EXPECT_EQ(context.getInsertionFeatures("a").features.at(
                user_tainted_insertion_feature),
            0);
}

TEST(ExcludeUserFeaturesStageTest, UserProperty) {
  bool ignore_usage = false;
  std::optional<ExcludeUserFeaturesConfig> config = {
      {.user_property = "isSeller"}};
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  FeatureContext context;
  context.initialize(insertions);
  // Second entry corresponds to "isSeller".
  context.addUserFeatures({{user_feature, 7}, {6619843120710314000, 1}});
  context.addRequestFeatures({{user_tainted_request_feature, 8}});
  context.addInsertionFeatures("a", {{user_tainted_insertion_feature, 9}});
  ExcludeUserFeaturesStage stage(0, ignore_usage, config, context, insertions);
  stage.runSync();

  EXPECT_FALSE(context.getUserFeatures().features.contains(user_feature));
  ASSERT_TRUE(context.getRequestFeatures().features.contains(
      user_tainted_request_feature));
  EXPECT_EQ(
      context.getRequestFeatures().features.at(user_tainted_request_feature),
      0);
  ASSERT_TRUE(context.getInsertionFeatures("a").features.contains(
      user_tainted_insertion_feature));
  EXPECT_EQ(context.getInsertionFeatures("a").features.at(
                user_tainted_insertion_feature),
            0);
}

TEST(ExcludeUserFeaturesStageTest, OnlyCertainFeaturesGetZeroed) {
  bool ignore_usage = false;
  std::optional<ExcludeUserFeaturesConfig> config = {
      {.user_property = "isSeller"}};
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  FeatureContext context;
  context.initialize(insertions);
  // Second entry corresponds to "isSeller".
  context.addUserFeatures({{user_feature, 7}, {6619843120710314000, 1}});
  context.addRequestFeatures({{user_tainted_request_feature, 8}, {10, 11}});
  context.addInsertionFeatures("a",
                               {{user_tainted_insertion_feature, 9}, {12, 13}});
  ExcludeUserFeaturesStage stage(0, ignore_usage, config, context, insertions);
  stage.runSync();

  EXPECT_FALSE(context.getUserFeatures().features.contains(user_feature));
  ASSERT_TRUE(context.getRequestFeatures().features.contains(
      user_tainted_request_feature));
  EXPECT_EQ(
      context.getRequestFeatures().features.at(user_tainted_request_feature),
      0);
  ASSERT_TRUE(context.getRequestFeatures().features.contains(10));
  EXPECT_EQ(context.getRequestFeatures().features.at(10), 11);
  ASSERT_TRUE(context.getInsertionFeatures("a").features.contains(
      user_tainted_insertion_feature));
  EXPECT_EQ(context.getInsertionFeatures("a").features.at(
                user_tainted_insertion_feature),
            0);
  ASSERT_TRUE(context.getInsertionFeatures("a").features.contains(12));
  EXPECT_EQ(context.getInsertionFeatures("a").features.at(12), 13);
}
}  // namespace delivery
