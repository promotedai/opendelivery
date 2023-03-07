#include <google/protobuf/stubs/port.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "execution/feature_context.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
class FeatureContextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<delivery::Insertion> insertions;
    insertions.emplace_back().set_content_id(id_1_);
    insertions.emplace_back().set_content_id(id_2_);
    context_.initialize(insertions);
  }

  FeatureContext context_;
  std::string id_1_ = "1";
  std::string id_2_ = "2";
};

TEST_F(FeatureContextTest, AddInsertionFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  context_.addInsertionFeatures(id_1_, std::move(features));

  {
    auto& scope = context_.getInsertionFeatures(id_1_);
    EXPECT_EQ(scope.features.size(), 2);
    ASSERT_TRUE(scope.features.contains(0));
    EXPECT_EQ(scope.features.at(0), 10);
    ASSERT_TRUE(scope.features.contains(1));
    EXPECT_EQ(scope.features.at(1), 11);
  }
  {
    auto& scope = context_.getInsertionFeatures(id_2_);
    EXPECT_TRUE(scope.features.empty());
  }
}

TEST_F(FeatureContextTest, AddRequestFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  context_.addRequestFeatures(std::move(features));

  auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.size(), 2);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);
}

TEST_F(FeatureContextTest, AddUserFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  context_.addUserFeatures(std::move(features));

  auto& scope = context_.getUserFeatures();
  EXPECT_EQ(scope.features.size(), 2);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);
}

TEST_F(FeatureContextTest, AddInsertionFeaturesProto) {
  delivery_private_features::Features features;
  (*features.mutable_sparse())[0] = 10;
  (*features.mutable_sparse())[1] = 11;
  (*features.mutable_sparse_id())[2] = 12;
  (*features.mutable_sparse_id())[3] = 13;
  delivery_private_features::Int64Sequence sequence;
  sequence.add_ids(14);
  sequence.add_ids(114);
  (*features.mutable_sparse_id_list())[4] = sequence;
  sequence.clear_ids();
  sequence.add_ids(15);
  sequence.add_ids(115);
  (*features.mutable_sparse_id_list())[5] = sequence;
  context_.addInsertionFeatures(id_1_, std::move(features));

  {
    auto& scope = context_.getInsertionFeatures(id_1_);

    EXPECT_EQ(scope.features.size(), 2);
    ASSERT_TRUE(scope.features.contains(0));
    EXPECT_EQ(scope.features.at(0), 10);
    ASSERT_TRUE(scope.features.contains(1));
    EXPECT_EQ(scope.features.at(1), 11);

    EXPECT_EQ(scope.int_features.size(), 2);
    ASSERT_TRUE(scope.int_features.contains(2));
    EXPECT_EQ(scope.int_features.at(2), 12);
    ASSERT_TRUE(scope.int_features.contains(3));
    EXPECT_EQ(scope.int_features.at(3), 13);

    EXPECT_EQ(scope.int_list_features.size(), 2);
    ASSERT_TRUE(scope.int_list_features.contains(4));
    EXPECT_THAT(scope.int_list_features.at(4), testing::ElementsAre(14, 114));
    ASSERT_TRUE(scope.int_list_features.contains(5));
    EXPECT_THAT(scope.int_list_features.at(5), testing::ElementsAre(15, 115));
  }
  {
    auto& scope = context_.getInsertionFeatures(id_2_);
    EXPECT_TRUE(scope.features.empty());
  }
}

TEST_F(FeatureContextTest, AddUserFeaturesProto) {
  delivery_private_features::Features features;
  (*features.mutable_sparse())[0] = 10;
  (*features.mutable_sparse())[1] = 11;
  (*features.mutable_sparse_id())[2] = 12;
  (*features.mutable_sparse_id())[3] = 13;
  delivery_private_features::Int64Sequence sequence;
  sequence.add_ids(14);
  sequence.add_ids(114);
  (*features.mutable_sparse_id_list())[4] = sequence;
  sequence.clear_ids();
  sequence.add_ids(15);
  sequence.add_ids(115);
  (*features.mutable_sparse_id_list())[5] = sequence;
  context_.addUserFeatures(std::move(features));

  auto& scope = context_.getUserFeatures();

  EXPECT_EQ(scope.features.size(), 2);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);

  EXPECT_EQ(scope.int_features.size(), 2);
  ASSERT_TRUE(scope.int_features.contains(2));
  EXPECT_EQ(scope.int_features.at(2), 12);
  ASSERT_TRUE(scope.int_features.contains(3));
  EXPECT_EQ(scope.int_features.at(3), 13);

  EXPECT_EQ(scope.int_list_features.size(), 2);
  ASSERT_TRUE(scope.int_list_features.contains(4));
  EXPECT_THAT(scope.int_list_features.at(4), testing::ElementsAre(14, 114));
  ASSERT_TRUE(scope.int_list_features.contains(5));
  EXPECT_THAT(scope.int_list_features.at(5), testing::ElementsAre(15, 115));
}

TEST_F(FeatureContextTest, AddStrangerInsertionFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  absl::flat_hash_map<std::string, uint64_t> feature_paths;
  feature_paths["10"] = 0;
  feature_paths["11"] = 1;
  context_.addStrangerInsertionFeatures(id_2_, std::move(features),
                                        std::move(feature_paths));

  {
    auto& scope = context_.getInsertionFeatures(id_1_);
    EXPECT_TRUE(scope.features.empty());
    EXPECT_TRUE(scope.stranger_feature_paths.empty());
  }
  {
    auto& scope = context_.getInsertionFeatures(id_2_);

    EXPECT_EQ(scope.features.size(), 2);
    ASSERT_TRUE(scope.features.contains(0));
    EXPECT_EQ(scope.features.at(0), 10);
    ASSERT_TRUE(scope.features.contains(1));
    EXPECT_EQ(scope.features.at(1), 11);

    EXPECT_EQ(scope.stranger_feature_paths.size(), 2);
    ASSERT_TRUE(scope.stranger_feature_paths.contains("10"));
    EXPECT_EQ(scope.stranger_feature_paths.at("10"), 0);
    ASSERT_TRUE(scope.stranger_feature_paths.contains("11"));
    EXPECT_EQ(scope.stranger_feature_paths.at("11"), 1);
  }
}

TEST_F(FeatureContextTest, AddStrangerRequestFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  absl::flat_hash_map<std::string, uint64_t> feature_paths;
  feature_paths["10"] = 0;
  feature_paths["11"] = 1;
  context_.addStrangerRequestFeatures(std::move(features),
                                      std::move(feature_paths));

  auto& scope = context_.getRequestFeatures();

  EXPECT_EQ(scope.features.size(), 2);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);

  EXPECT_EQ(scope.stranger_feature_paths.size(), 2);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("10"));
  EXPECT_EQ(scope.stranger_feature_paths.at("10"), 0);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("11"));
  EXPECT_EQ(scope.stranger_feature_paths.at("11"), 1);
}

TEST_F(FeatureContextTest, AddStrangerUserFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  absl::flat_hash_map<std::string, uint64_t> feature_paths;
  feature_paths["10"] = 0;
  feature_paths["11"] = 1;
  context_.addStrangerUserFeatures(std::move(features),
                                   std::move(feature_paths));

  auto& scope = context_.getUserFeatures();

  EXPECT_EQ(scope.features.size(), 2);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);

  EXPECT_EQ(scope.stranger_feature_paths.size(), 2);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("10"));
  EXPECT_EQ(scope.stranger_feature_paths.at("10"), 0);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("11"));
  EXPECT_EQ(scope.stranger_feature_paths.at("11"), 1);
}

// Assume the add*() functions use the same underlying map merging logic.
TEST_F(FeatureContextTest, MultipleAdds) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  context_.addInsertionFeatures(id_1_, std::move(features));
  // Add a smaller amount and then a larger amount to test the side-swapping
  // optimization.
  features.clear();
  features[2] = 12;
  context_.addInsertionFeatures(id_1_, std::move(features));
  features.clear();
  features[3] = 13;
  features[4] = 14;
  features[5] = 15;
  features[6] = 16;
  context_.addInsertionFeatures(id_1_, std::move(features));

  {
    auto& scope = context_.getInsertionFeatures(id_1_);
    EXPECT_EQ(scope.features.size(), 7);
    ASSERT_TRUE(scope.features.contains(0));
    EXPECT_EQ(scope.features.at(0), 10);
    ASSERT_TRUE(scope.features.contains(1));
    EXPECT_EQ(scope.features.at(1), 11);
    ASSERT_TRUE(scope.features.contains(2));
    EXPECT_EQ(scope.features.at(2), 12);
    ASSERT_TRUE(scope.features.contains(3));
    EXPECT_EQ(scope.features.at(3), 13);
    ASSERT_TRUE(scope.features.contains(4));
    EXPECT_EQ(scope.features.at(4), 14);
    ASSERT_TRUE(scope.features.contains(5));
    EXPECT_EQ(scope.features.at(5), 15);
    ASSERT_TRUE(scope.features.contains(6));
    EXPECT_EQ(scope.features.at(6), 16);
  }
  {
    auto& scope = context_.getInsertionFeatures(id_2_);
    EXPECT_TRUE(scope.features.empty());
  }
}

TEST_F(FeatureContextTest, ProcessInsertionFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  context_.addInsertionFeatures(id_1_, std::move(features));
  context_.processInsertionFeatures(
      id_1_, [](FeatureScope& insertion, const FeatureScope& request,
                const FeatureScope& user) { insertion.features[2] = 12; });

  {
    auto& scope = context_.getInsertionFeatures(id_1_);
    EXPECT_EQ(scope.features.size(), 3);
    ASSERT_TRUE(scope.features.contains(0));
    EXPECT_EQ(scope.features.at(0), 10);
    ASSERT_TRUE(scope.features.contains(1));
    EXPECT_EQ(scope.features.at(1), 11);
    ASSERT_TRUE(scope.features.contains(2));
    EXPECT_EQ(scope.features.at(2), 12);
  }
  {
    auto& scope = context_.getInsertionFeatures(id_2_);
    EXPECT_TRUE(scope.features.empty());
  }
}

TEST_F(FeatureContextTest, ProcessRequestFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  absl::flat_hash_map<std::string, uint64_t> feature_paths;
  feature_paths["10"] = 0;
  feature_paths["11"] = 1;
  context_.addStrangerRequestFeatures(std::move(features),
                                      std::move(feature_paths));
  context_.processRequestFeatures([](FeatureScope& request) {
    request.features[2] = 12;
    request.stranger_feature_paths["12"] = 2;
  });

  auto& scope = context_.getRequestFeatures();

  EXPECT_EQ(scope.features.size(), 3);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);
  ASSERT_TRUE(scope.features.contains(2));
  EXPECT_EQ(scope.features.at(2), 12);

  EXPECT_EQ(scope.stranger_feature_paths.size(), 3);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("10"));
  EXPECT_EQ(scope.stranger_feature_paths.at("10"), 0);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("11"));
  EXPECT_EQ(scope.stranger_feature_paths.at("11"), 1);
  ASSERT_TRUE(scope.stranger_feature_paths.contains("12"));
  EXPECT_EQ(scope.stranger_feature_paths.at("12"), 2);
}

TEST_F(FeatureContextTest, ProcessUserFeatures) {
  absl::flat_hash_map<uint64_t, float> features;
  features[0] = 10;
  features[1] = 11;
  context_.addUserFeatures(std::move(features));
  context_.processUserFeatures(
      [](FeatureScope& user) { user.features[2] = 12; });

  auto& scope = context_.getUserFeatures();

  EXPECT_EQ(scope.features.size(), 3);
  ASSERT_TRUE(scope.features.contains(0));
  EXPECT_EQ(scope.features.at(0), 10);
  ASSERT_TRUE(scope.features.contains(1));
  EXPECT_EQ(scope.features.at(1), 11);
  ASSERT_TRUE(scope.features.contains(2));
  EXPECT_EQ(scope.features.at(2), 12);
}
}  // namespace delivery
