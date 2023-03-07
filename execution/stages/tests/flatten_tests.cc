#include <google/protobuf/struct.pb.h>
#include <google/protobuf/stubs/common.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "execution/feature_context.h"
#include "execution/stages/flatten.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
namespace {
// Arbitrary properties to get hashlib to populate all output fields.
common::Properties exhaustiveProperties() {
  common::Properties props;

  google::protobuf::Value number_value;
  number_value.set_number_value(16777217);
  (*props.mutable_struct_()->mutable_fields())["number"] = number_value;

  google::protobuf::ListValue base_list_value;
  *base_list_value.add_values() = number_value;
  google::protobuf::Value list_value;
  *list_value.mutable_list_value() = base_list_value;
  (*props.mutable_struct_()->mutable_fields())["list"] = list_value;

  return props;
}

// Arbitrary properties to get hashlib to populate a lot of entries, and only
// floats.
common::Properties longProperties() {
  common::Properties props;

  for (int i = 0; i < 100; ++i) {
    google::protobuf::Value number_value;
    number_value.set_number_value(0.5);
    (*props.mutable_struct_()->mutable_fields())[absl::StrCat(i)] =
        number_value;
  }

  return props;
}
}  // namespace

TEST(FlattenStageTest, RequestUnlimited) {
  delivery::Request req;
  *req.mutable_properties() = exhaustiveProperties();
  std::vector<delivery::Insertion> insertions;
  uint64_t max_request_properties = 100;
  uint64_t max_insertion_properties = 100;
  FeatureContext context;
  FlattenStage stage(0, req, insertions, max_request_properties,
                     max_insertion_properties, context);
  stage.runSync();

  const auto& scope = context.getRequestFeatures();
  EXPECT_FALSE(scope.features.empty());
  EXPECT_FALSE(scope.int_features.empty());
  EXPECT_FALSE(scope.int_list_features.empty());
  EXPECT_FALSE(scope.stranger_feature_paths.empty());
}

TEST(FlattenStageTest, RequestLimited) {
  delivery::Request req;
  *req.mutable_properties() = longProperties();
  std::vector<delivery::Insertion> insertions;
  uint64_t max_request_properties = 8;
  uint64_t max_insertion_properties = 100;
  FeatureContext context;
  FlattenStage stage(0, req, insertions, max_request_properties,
                     max_insertion_properties, context);
  stage.runSync();

  const auto& scope = context.getRequestFeatures();
  EXPECT_EQ(scope.features.size(), 8);
  EXPECT_TRUE(scope.int_features.empty());
  EXPECT_TRUE(scope.int_list_features.empty());
  EXPECT_FALSE(scope.stranger_feature_paths.empty());
}

TEST(FlattenStageTest, InsertionUnlimited) {
  delivery::Request req;
  std::vector<delivery::Insertion> insertions;
  auto& insertion_a = insertions.emplace_back();
  insertion_a.set_content_id("a");
  (*insertion_a.mutable_properties()) = exhaustiveProperties();
  auto& insertion_b = insertions.emplace_back();
  insertion_b.set_content_id("b");
  (*insertion_b.mutable_properties()) = exhaustiveProperties();
  uint64_t max_request_properties = 100;
  uint64_t max_insertion_properties = 100;
  FeatureContext context;
  context.initialize(insertions);
  FlattenStage stage(0, req, insertions, max_request_properties,
                     max_insertion_properties, context);
  stage.runSync();

  {
    const auto& scope = context.getInsertionFeatures("a");
    EXPECT_FALSE(scope.features.empty());
    EXPECT_FALSE(scope.int_features.empty());
    EXPECT_FALSE(scope.int_list_features.empty());
    EXPECT_FALSE(scope.stranger_feature_paths.empty());
  }
  {
    const auto& scope = context.getInsertionFeatures("b");
    EXPECT_FALSE(scope.features.empty());
    EXPECT_FALSE(scope.int_features.empty());
    EXPECT_FALSE(scope.int_list_features.empty());
    EXPECT_FALSE(scope.stranger_feature_paths.empty());
  }
}

TEST(FlattenStageTest, InsertionLimited) {
  delivery::Request req;
  std::vector<delivery::Insertion> insertions;
  auto& insertion = insertions.emplace_back();
  insertion.set_content_id("a");
  (*insertion.mutable_properties()) = longProperties();
  uint64_t max_request_properties = 100;
  uint64_t max_insertion_properties = 16;
  FeatureContext context;
  context.initialize(insertions);
  FlattenStage stage(0, req, insertions, max_request_properties,
                     max_insertion_properties, context);
  stage.runSync();

  const auto& scope = context.getInsertionFeatures("a");
  EXPECT_EQ(scope.features.size(), 16);
  EXPECT_TRUE(scope.int_features.empty());
  EXPECT_TRUE(scope.int_list_features.empty());
  EXPECT_FALSE(scope.stranger_feature_paths.empty());
}
}  // namespace delivery
