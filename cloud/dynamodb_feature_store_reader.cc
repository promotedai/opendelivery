#include "cloud/dynamodb_feature_store_reader.h"

#include <aws/core/utils/Array.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/DynamoDBErrors.h>
#include <aws/dynamodb/DynamoDBServiceClientModel.h>
#include <aws/dynamodb/model/AttributeValue.h>
#include <aws/dynamodb/model/BatchGetItemResult.h>
#include <aws/dynamodb/model/GetItemResult.h>
#include <aws/dynamodb/model/KeysAndAttributes.h>

#include <algorithm>
#include <atomic>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#include "aws/dynamodb/model/BatchGetItemRequest.h"
#include "aws/dynamodb/model/GetItemRequest.h"
#include "execution/stages/feature_store_client.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace Aws {
namespace Client {
class AsyncCallerContext;
}
}  // namespace Aws

namespace delivery {
FeatureStoreResult processAttributes(
    const std::string& key,
    const Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>& map) {
  FeatureStoreResult res;

  if (map.empty()) {
    return res;
  }

  // -1 because one of the map entries corresponds to the key.
  res.columns_bytes.reserve(map.size() - 1);
  for (const auto& [k, v] : map) {
    if (k == key) {
      res.key = v.GetS();
    } else {
      // Assume all projected columns are bytes.
      const auto& b = v.GetB();
      res.columns_bytes.emplace_back(
          reinterpret_cast<char*>(b.GetUnderlyingData()), b.GetLength());
    }
  }
  return res;
}

void DynamoDBFeatureStoreClient::read(
    const std::string& table, const std::string& key_column,
    const std::string& key, const std::string& columns,
    std::function<void(std::vector<FeatureStoreResult>)>&& cb) const {
  Aws::DynamoDB::Model::GetItemRequest req;
  req.SetTableName(table);
  req.SetProjectionExpression(columns);
  Aws::DynamoDB::Model::AttributeValue value;
  value.SetS(key);
  req.AddKey(key_column, value);

  dynamodb_client_.GetItemAsync(
      req, [key_column, cb](
               const Aws::DynamoDB::DynamoDBClient*,
               const Aws::DynamoDB::Model::GetItemRequest&,
               const Aws::DynamoDB::Model::GetItemOutcome& outcome,
               const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        FeatureStoreResult result;
        if (outcome.IsSuccess()) {
          result = processAttributes(key_column, outcome.GetResult().GetItem());
        } else {
          LOG_ERROR << "Response error from DynamoDB: "
                    << outcome.GetError().GetMessage();
        }
        // This request can be marked successful even if the single key being
        // requested is not found.
        std::vector<FeatureStoreResult> results;
        if (!result.key.empty()) {
          results.emplace_back(std::move(result));
        }
        cb(std::move(results));
      });
}

size_t numBatches(size_t num_keys) {
  size_t num_batches = (num_keys / dynamodb_batch_limit);
  // Add a batch for remainder.
  if (num_keys % dynamodb_batch_limit != 0) {
    ++num_batches;
  }
  return num_batches;
}

std::vector<FeatureStoreResult> removeEmptyKeys(
    std::vector<FeatureStoreResult> results) {
  std::vector<FeatureStoreResult> res;
  res.reserve(results.size());
  for (auto& result : results) {
    if (!result.key.empty()) {
      res.emplace_back(std::move(result));
    }
  }
  return res;
}

void DynamoDBFeatureStoreClient::readBatch(
    const std::string& table, const std::string& key_column,
    const std::vector<std::string>& keys, const std::string& columns,
    std::function<void(std::vector<FeatureStoreResult>)>&& cb) const {
  // For many keys, we will have to send multiple requests because of DynamoDB
  // limitations.
  size_t num_batches = numBatches(keys.size());

  // We make a counter to share among all the requests so that the callback only
  // happens once.
  auto num_callbacks = std::make_shared<std::atomic<size_t>>(num_batches);
  auto results = std::make_shared<std::vector<FeatureStoreResult>>();
  // Can't just reserve here because the callbacks need to access by index.
  results->resize(keys.size());

  for (size_t i = 0; i < num_batches; ++i) {
    // [start, stop)
    size_t start_index = i * dynamodb_batch_limit;
    size_t stop_index =
        std::min(start_index + dynamodb_batch_limit, keys.size());

    Aws::DynamoDB::Model::BatchGetItemRequest req;
    Aws::DynamoDB::Model::KeysAndAttributes maps;
    maps.SetProjectionExpression(columns);
    for (size_t j = start_index; j < stop_index; ++j) {
      Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> map;
      Aws::DynamoDB::Model::AttributeValue value;
      value.SetS(keys[j]);
      map.emplace(key_column, value);
      maps.AddKeys(map);
    }
    req.AddRequestItems(table, maps);

    dynamodb_client_.BatchGetItemAsync(
        req,
        [table, key_column, results, start_index, num_callbacks, cb](
            const Aws::DynamoDB::DynamoDBClient*,
            const Aws::DynamoDB::Model::BatchGetItemRequest&,
            const Aws::DynamoDB::Model::BatchGetItemOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
          if (outcome.IsSuccess()) {
            // We can receive less than we requested if any of the IDs did not
            // exist.
            const Aws::Vector<
                Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>>&
                responses = outcome.GetResult().GetResponses().at(table);
            for (size_t k = 0; k < responses.size(); ++k) {
              (*results)[start_index + k] =
                  processAttributes(key_column, responses[k]);
            }
          } else {
            LOG_ERROR << "Response error from DynamoDB: "
                      << outcome.GetError().GetMessage();
          }
          if (--(*num_callbacks) == 0) {
            // Since we allocated enough results for all of the expected keys
            // but some might not have existed, we remove any such results to
            // simplify downstream processing.
            cb(removeEmptyKeys(std::move(*results)));
          }
        });
  }
}
}  // namespace delivery
