// This thing doesn't perform well for a variety of reasons, but it doesn't
// really matter because of how long the calls to DynamoDB take. We can assume
// any request waiting on a read through this is going to be too slow anyway.

#pragma once

#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <stddef.h>

#include <functional>
#include <string>
#include <vector>

#include "execution/stages/feature_store_client.h"

namespace Aws {
namespace DynamoDB {
class DynamoDBClient;
namespace Model {
class AttributeValue;
}
}  // namespace DynamoDB
}  // namespace Aws

namespace delivery {
// DynamoDB has a hard limit of 100 items per batch request:
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
const int dynamodb_batch_limit = 100;

class DynamoDBFeatureStoreClient : public FeatureStoreClient {
 public:
  explicit DynamoDBFeatureStoreClient(
      const Aws::DynamoDB::DynamoDBClient& dynamodb_client)
      : dynamodb_client_(dynamodb_client) {}

  // DynamoDB is NoSQL so it doesn't really have columns. It has "attributes",
  // but we can think of them like columns.
  void read(
      const std::string& table, const std::string& key_column,
      const std::string& key, const std::string& columns,
      std::function<void(std::vector<FeatureStoreResult>)>&& cb) const override;
  void readBatch(
      const std::string& table, const std::string& key_column,
      const std::vector<std::string>& keys, const std::string& columns,
      std::function<void(std::vector<FeatureStoreResult>)>&& cb) const override;

 private:
  const Aws::DynamoDB::DynamoDBClient& dynamodb_client_;
};

// Declared here for testing.
FeatureStoreResult processAttributes(
    const std::string& key,
    const Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>& map);
size_t numBatches(size_t num_keys);
std::vector<FeatureStoreResult> removeEmptyKeys(
    std::vector<FeatureStoreResult> results);
}  // namespace delivery
