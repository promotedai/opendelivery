// The metadata about the structure of counters Redis is also contained in
// Redis. This is a singleton to act as a global cache for that structure.

#pragma once

#include <gtest/gtest_prod.h>
#include <stddef.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "execution/stages/counters.h"
#include "singletons/singleton.h"

namespace delivery {
class RedisClient;
class RedisClientArray;
namespace counters {
struct DatabaseInfo;
struct RateInfo;
struct TableInfo;
}  // namespace counters
struct SplitFeatureID;
}  // namespace delivery

namespace delivery {
namespace counters {
class CountersSingleton : public Singleton<CountersSingleton> {
 public:
  const DatabaseInfo* getDatabaseInfo(uint64_t platform_id,
                                      const std::string& name) const {
    auto platform_it = platform_to_name_to_database_.find(platform_id);
    if (platform_it == platform_to_name_to_database_.end()) {
      return nullptr;
    }
    const auto& name_to_database = platform_it->second;
    auto name_it = name_to_database.find(name);
    if (name_it == name_to_database.end()) {
      return nullptr;
    }
    return name_it->second.get();
  }

  std::unique_ptr<RedisClient> getCountersClient(const std::string& name,
                                                 size_t index);

 private:
  friend class Singleton;
  FRIEND_TEST(CountersSingletonTest, CombineSplitFeatureIds);
  FRIEND_TEST(CountersSingletonTest, ParseEnabledFeatureIds);
  FRIEND_TEST(CountersSingletonTest, DeriveRateFeatureIds);
  FRIEND_TEST(CountersSingletonTest, CreateTableInfo);

  CountersSingleton();

  absl::flat_hash_map<
      uint64_t, absl::flat_hash_map<std::string, std::unique_ptr<DatabaseInfo>>>
      platform_to_name_to_database_;

  absl::flat_hash_map<std::string, RedisClientArray> name_to_clients_;

  // If there's an error, this aborts.
  void createClients(const std::string& url, const std::string& timeout,
                     const std::string& name);

  // If there's an error, the empty set is returned.
  static absl::flat_hash_set<uint64_t> combineSplitFeatureIds(
      const std::vector<SplitFeatureID>& split_feature_ids);

  // If there's an error, the empty set is returned.
  static absl::flat_hash_set<uint64_t> parseEnabledFeatureIds(
      const absl::flat_hash_set<uint64_t>& enabled_feature_ids,
      std::string_view table_feature_ids);

  static std::vector<RateInfo> deriveRateFeatureIds(
      const absl::flat_hash_set<uint64_t>& feature_ids);

  // If there's an error, nullptr is returned.
  static std::unique_ptr<TableInfo> createTableInfo(
      const std::string& name, const std::string& row_format,
      const std::string& table_feature_ids,
      const absl::flat_hash_set<uint64_t>& config_feature_ids);
};
}  // namespace counters
}  // namespace delivery
