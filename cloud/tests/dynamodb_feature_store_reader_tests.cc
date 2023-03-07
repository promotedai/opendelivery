#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/dynamodb/model/AttributeValue.h>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cloud/dynamodb_feature_store_reader.h"
#include "execution/stages/feature_store_client.h"
#include "gtest/gtest.h"

namespace delivery {
TEST(DynamoDBFeatureStoreClientTest, ProcessAttributesEmpty) {
  std::string key = "key";
  Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> map;

  FeatureStoreResult result = processAttributes(key, map);
  EXPECT_TRUE(result.key.empty());
  EXPECT_TRUE(result.columns_bytes.empty());
}

TEST(DynamoDBFeatureStoreClientTest, ProcessAttributes) {
  std::string key = "key";
  Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> map;
  {
    Aws::DynamoDB::Model::AttributeValue key_value;
    key_value.SetS("key_value");
    map[key] = key_value;
  }
  {
    Aws::DynamoDB::Model::AttributeValue some_value;
    std::string b = "some_bytes";
    Aws::Utils::ByteBuffer bytes(reinterpret_cast<unsigned char*>(b.data()),
                                 b.size());
    some_value.SetB(bytes);
    map["some_projection"] = some_value;
  }

  FeatureStoreResult result = processAttributes(key, map);
  EXPECT_EQ(result.key, "key_value");
  ASSERT_EQ(result.columns_bytes.size(), 1);
  EXPECT_EQ(result.columns_bytes[0], "some_bytes");
}

TEST(DynamoDBFeatureStoreClientTest, NumBatches) {
  EXPECT_EQ(numBatches(static_cast<size_t>(dynamodb_batch_limit) * 3 - 1), 3);
  EXPECT_EQ(numBatches(static_cast<size_t>(dynamodb_batch_limit) * 3), 3);
  EXPECT_EQ(numBatches(static_cast<size_t>(dynamodb_batch_limit) * 3 + 1), 4);
}

TEST(DynamoDBFeatureStoreClientTest, RemoveEmptyKeys) {
  std::vector<FeatureStoreResult> results;
  results.emplace_back().key = "a";
  results.emplace_back().key = "";
  results.emplace_back().key = "c";
  results.emplace_back().key = "";

  auto processed = removeEmptyKeys(results);
  ASSERT_EQ(processed.size(), 2);
  EXPECT_EQ(processed[0].key, "a");
  EXPECT_EQ(processed[1].key, "c");
}
}  // namespace delivery
