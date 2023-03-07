#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/feature_context.h"
#include "execution/stages/compute_ratio_features.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
namespace {
namespace dpf = delivery_private_features;
}  // namespace

TEST(ComputeRatioFeaturesTest, AllScopes) {
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  context.initialize(insertions);
  context.addInsertionFeatures(
      "a",
      {{dpf::ITEM_RATE_RAW_OVER_IMPRESSION + dpf::COUNT_NAVIGATE + dpf::DAY_7,
        100},
       {dpf::ITEM_RATE_RAW_OVER_IMPRESSION + dpf::COUNT_NAVIGATE + dpf::DAY_30,
        200}});
  context.addInsertionFeatures(
      "b", {{dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::HOUR, 20},
            {dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY, 80}});
  context.addRequestFeatures({{dpf::QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                                   dpf::COUNT_NAVIGATE + dpf::DAY_7,
                               33},
                              {dpf::QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                                   dpf::COUNT_NAVIGATE + dpf::DAY_30,
                               99}});
  context.addUserFeatures(
      {{dpf::LOG_USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY, 8},
       {dpf::LOG_USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30, 16}});
  ComputeRatioFeaturesStage stage(0, context, insertions);
  stage.runSync();

  EXPECT_EQ(context.getInsertionFeatures("a").features.at(
                dpf::RAW_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO),
            0.5);
  EXPECT_EQ(context.getInsertionFeatures("b").features.at(
                dpf::ITEMXQUERY_IMPRESSION_1_TO_24_HOUR_COUNT_RATE_RATIO),
            6);
  EXPECT_FLOAT_EQ(context.getRequestFeatures().features.at(
                      dpf::SMOOTH_QUERY_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO),
                  1.0 / 3);
  EXPECT_FLOAT_EQ(context.getUserFeatures().features.at(
                      dpf::LOG_USER_IMPRESSION_1_TO_30_DAY_COUNT_RATE_RATIO),
                  15);
}

TEST(ComputeRatioFeaturesTest, CrossScope) {
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  FeatureContext context;
  context.initialize(insertions);
  context.addInsertionFeatures(
      "a", {{dpf::ITEM_QUERY_RATE_SMOOTH_OVER_IMPRESSION + dpf::COUNT_NAVIGATE +
                 dpf::DAY_30,
             100}});
  context.addRequestFeatures({{dpf::QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                                   dpf::COUNT_NAVIGATE + dpf::DAY_30,
                               50}});
  ComputeRatioFeaturesStage stage(0, context, insertions);
  stage.runSync();

  EXPECT_EQ(context.getInsertionFeatures("a").features.at(
                dpf::SMOOTH_CTR_30_DAY_ITEMXQUERY_TO_QUERY_COUNTER_RATE_RATIO),
            2);
}
}  // namespace delivery
