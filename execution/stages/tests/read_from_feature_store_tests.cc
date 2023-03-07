#include <google/protobuf/stubs/port.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "config/feature_store_config.h"
#include "execution/stages/cache.h"
#include "execution/stages/feature_store_client.h"
#include "execution/stages/read_from_feature_store.h"
#include "execution/stages/tests/mock_clients.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/delivery/private/features/features.pb.h"
#include "utils/time.h"

namespace delivery {
TEST(ReadFromFeatureStoreTest, DeserializeAndCache) {
  std::string some_key = "some_key";
  std::string another_key = "another_key";
  std::vector<std::string> keys_to_fetch = {some_key, another_key};
  // Create a result for "some_key", but not "another_key".
  std::vector<FeatureStoreResult> results;
  auto& result = results.emplace_back();
  result.key = some_key;
  delivery_private_features::FeaturesList features_list;
  (*features_list.add_features()->mutable_sparse())[8] = 9;
  features_list.SerializeToString(&result.columns_bytes.emplace_back());

  uint64_t start_time = 2001;
  FeaturesCache cache(1'000);
  absl::flat_hash_map<std::string, delivery_private_features::Features>
      id_to_features;
  std::function<void(std::string_view, delivery_private_features::Features)>
      feature_adder =
          [&id_to_features](std::string_view id,
                            delivery_private_features::Features features) {
            id_to_features[id] = std::move(features);
          };
  std::vector<std::string> errors;

  deserializeAndCache(results, keys_to_fetch, start_time, cache, feature_adder,
                      errors);
  FeaturesCache::ConstAccessor accessor;
  {
    std::string timed_key = makeTimedKey(some_key, start_time);
    EXPECT_TRUE(cache.find(accessor, {timed_key.data(), timed_key.size()}));
    ASSERT_TRUE(id_to_features.contains(some_key));
    ASSERT_TRUE(id_to_features[some_key].sparse().contains(8));
    EXPECT_EQ(id_to_features[some_key].sparse().at(8), 9);
    EXPECT_TRUE(errors.empty());
  }
  {
    std::string timed_key = makeTimedKey(another_key, start_time);
    EXPECT_TRUE(cache.find(accessor, {timed_key.data(), timed_key.size()}));
    EXPECT_FALSE(id_to_features.contains(another_key));
    EXPECT_TRUE(errors.empty());
  }
}

TEST(ReadFromFeatureStoreTest, ProcessCachedKeys) {
  std::vector<std::string> keys{"a", "b"};
  FeaturesCache cache(1'000);
  uint64_t start_time = 2002;
  // Cache the first key. Don't care about the value.
  std::string timed_key = makeTimedKey(keys[0], start_time);
  CacheKey cache_key(timed_key.data(), timed_key.size());
  cache.insert(cache_key, {});
  absl::flat_hash_map<std::string, delivery_private_features::Features>
      id_to_features;
  std::function<void(std::string_view, delivery_private_features::Features)>
      feature_adder =
          [&id_to_features](std::string_view id,
                            delivery_private_features::Features features) {
            id_to_features[id] = std::move(features);
          };
  std::vector<std::string> keys_to_fetch;

  processCachedKeys(keys, start_time, cache, feature_adder, keys_to_fetch);
  EXPECT_EQ(id_to_features.size(), 1);
  EXPECT_TRUE(id_to_features.contains("a"));
  ASSERT_EQ(keys_to_fetch.size(), 1);
  EXPECT_EQ(keys_to_fetch[0], "b");
}

TEST(ReadFromFeatureStoreTest, Read) {
  bool ran = false;
  bool timed_out = false;
  FeaturesCache cache(1'000);
  auto client_ptr = std::make_unique<MockFeatureStoreClient>();
  auto& client = *client_ptr;
  FeatureStoreConfig config;
  std::string timeout = "10ms";
  // Uncached key to force read.
  auto key_generator = []() -> std::vector<std::string> {
    return {"some_key"};
  };
  absl::flat_hash_map<std::string, delivery_private_features::Features>
      id_to_features;
  auto feature_adder = [&id_to_features](
                           std::string_view id,
                           delivery_private_features::Features features) {
    id_to_features[id] = std::move(features);
  };
  ReadFromFeatureStoreStage stage(0, cache, std::move(client_ptr), config,
                                  timeout, 2001, key_generator, feature_adder);
  std::vector<FeatureStoreResult> results;
  EXPECT_CALL(client, read).WillOnce(testing::InvokeArgument<4>(results));
  stage.run(
      [ran = &ran]() { *ran = true; },
      [timed_out = &timed_out](const std::chrono::duration<double>&,
                               std::function<void()>&&) { *timed_out = true; });
  EXPECT_TRUE(ran);
  EXPECT_TRUE(timed_out);
}

TEST(ReadFromFeatureStoreTest, ReadBatch) {
  bool ran = false;
  bool timed_out = false;
  FeaturesCache cache(1'000);
  auto client_ptr = std::make_unique<MockFeatureStoreClient>();
  auto& client = *client_ptr;
  FeatureStoreConfig config;
  std::string timeout = "10ms";
  // Multiple uncached keys to force batch read.
  auto key_generator = []() -> std::vector<std::string> {
    return {"some_key", "some_other_key"};
  };
  absl::flat_hash_map<std::string, delivery_private_features::Features>
      id_to_features;
  auto feature_adder = [&id_to_features](
                           std::string_view id,
                           delivery_private_features::Features features) {
    id_to_features[id] = std::move(features);
  };
  ReadFromFeatureStoreStage stage(0, cache, std::move(client_ptr), config,
                                  timeout, 2001, key_generator, feature_adder);
  std::vector<FeatureStoreResult> results;
  EXPECT_CALL(client, readBatch).WillOnce(testing::InvokeArgument<4>(results));
  stage.run(
      [ran = &ran]() { *ran = true; },
      [timed_out = &timed_out](const std::chrono::duration<double>&,
                               std::function<void()>&&) { *timed_out = true; });
  EXPECT_TRUE(ran);
  EXPECT_TRUE(timed_out);
}
}  // namespace delivery
