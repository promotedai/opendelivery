#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/feature_context.h"
#include "execution/stages/sqs_client.h"
#include "execution/stages/tests/mock_clients.h"
#include "execution/stages/write_out_stranger_features.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
TEST(WriteOutStrangerFeaturesTest, ZeroSample) {
  double sample_rate = 0;
  uint64_t start_time = 100000;
  FeatureContext context;
  // Just want the context to be non-empty.
  context.processRequestFeatures([](FeatureScope& scope) {
    scope.stranger_feature_paths = {{"a", 1}};
  });
  std::vector<delivery::Insertion> insertions;
  auto mock_client = std::make_unique<MockSqsClient>();
  EXPECT_CALL(*mock_client, sendMessage).Times(0);
  WriteOutStrangerFeaturesStage stage(0, sample_rate, start_time, context,
                                      insertions, std::move(mock_client));
  stage.runSync();
}

TEST(WriteOutStrangerFeaturesTest, NotInSample) {
  double sample_rate = 0.1;
  uint64_t start_time = 100015;
  FeatureContext context;
  context.processRequestFeatures([](FeatureScope& scope) {
    scope.stranger_feature_paths = {{"a", 1}};
  });
  std::vector<delivery::Insertion> insertions;
  auto mock_client = std::make_unique<MockSqsClient>();
  EXPECT_CALL(*mock_client, sendMessage).Times(0);
  WriteOutStrangerFeaturesStage stage(0, sample_rate, start_time, context,
                                      insertions, std::move(mock_client));
  stage.runSync();
}

TEST(WriteOutStrangerFeaturesTest, InSample) {
  double sample_rate = 0.2;
  uint64_t start_time = 100015;
  FeatureContext context;
  context.processRequestFeatures([](FeatureScope& scope) {
    scope.stranger_feature_paths = {{"a", 1}};
  });
  std::vector<delivery::Insertion> insertions;
  auto mock_client = std::make_unique<MockSqsClient>();
  EXPECT_CALL(*mock_client, sendMessage).Times(1);
  WriteOutStrangerFeaturesStage stage(0, sample_rate, start_time, context,
                                      insertions, std::move(mock_client));
  stage.runSync();
}

TEST(WriteOutStrangerFeaturesTest, SendMessage) {
  double sample_rate = 0.2;
  uint64_t start_time = 100015;
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("c");
  insertions.emplace_back().set_content_id("d");
  FeatureContext context;
  context.initialize(insertions);
  context.processUserFeatures([](FeatureScope& scope) {
    scope.stranger_feature_paths = {{"a", 1}};
  });
  context.processRequestFeatures([](FeatureScope& scope) {
    scope.stranger_feature_paths = {{"b", 2}};
  });
  context.processInsertionFeatures(
      "c", [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
        scope.stranger_feature_paths = {{"c", 3}};
      });
  context.processInsertionFeatures(
      "d", [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
        scope.stranger_feature_paths = {{"d", 4}};
      });
  auto mock_client = std::make_unique<MockSqsClient>();
  std::string message;
  EXPECT_CALL(*mock_client, sendMessage)
      .WillOnce(testing::SaveArg<0>(&message));
  WriteOutStrangerFeaturesStage stage(0, sample_rate, start_time, context,
                                      insertions, std::move(mock_client));
  stage.runSync();

  EXPECT_EQ(message, R"({"a":1,"b":2,"c":3,"d":4})");
}
}  // namespace delivery
