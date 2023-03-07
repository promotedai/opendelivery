// These stages are responsible for:
// - Reading counters data from Redis
// - Processing that data into structured features

#pragma once

#include <stddef.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
#include "execution/feature_context.h"
#include "execution/stages/cache.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/stage.h"
#include "execution/user_agent.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
class Insertion;
class Request;
struct CountersContext;
}  // namespace delivery

namespace delivery {
namespace counters {
// This delimits different parts of a key.
const std::string key_separator = "\x1f";
// These signals a particular meaning for part of a key.
const std::string user_separator = absl::StrCat("\x1d", "u");
const std::string query_separator = absl::StrCat("\x1d", "q");

// Expected labels in table metadata strings.
const std::string os_key_label = "os";
const std::string app_key_label = "user_agent";
const std::string fid_key_label = "fid";

// Device-specific -> combined across all devices.
const absl::flat_hash_map<uint64_t, uint64_t> segmented_id_to_aggregate = {
    {delivery_private_features::ITEM_DEVICE_COUNT,
     delivery_private_features::ITEM_COUNT},
    {delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_IMPRESSION,
     delivery_private_features::ITEM_RATE_RAW_OVER_IMPRESSION},
    {delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_NAVIGATE,
     delivery_private_features::ITEM_RATE_RAW_OVER_NAVIGATE},
    {delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_ADD_TO_CART,
     delivery_private_features::ITEM_RATE_RAW_OVER_ADD_TO_CART},
    {delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_CHECKOUT,
     delivery_private_features::ITEM_RATE_RAW_OVER_CHECKOUT},
    {delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_PURCHASE,
     delivery_private_features::ITEM_RATE_RAW_OVER_PURCHASE},
    {delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_IMPRESSION,
     delivery_private_features::ITEM_RATE_SMOOTH_OVER_IMPRESSION},
    {delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_NAVIGATE,
     delivery_private_features::ITEM_RATE_SMOOTH_OVER_NAVIGATE},
    {delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_ADD_TO_CART,
     delivery_private_features::ITEM_RATE_SMOOTH_OVER_ADD_TO_CART},
    {delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_CHECKOUT,
     delivery_private_features::ITEM_RATE_SMOOTH_OVER_CHECKOUT},
    {delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_PURCHASE,
     delivery_private_features::ITEM_RATE_SMOOTH_OVER_PURCHASE}};

struct Caches {
  std::unique_ptr<Cache> global_counts_cache;
  std::unique_ptr<Cache> item_counts_cache;
  std::unique_ptr<Cache> user_counts_cache;
  std::unique_ptr<Cache> query_counts_cache;
  std::unique_ptr<Cache> item_query_counts_cache;
};

struct RateInfo {
  uint64_t numerator = 0;
  uint64_t denominator = 0;
  uint64_t raw = 0;
  uint64_t smooth = 0;
  uint64_t global = 0;
};

struct TableInfo {
  std::string name;
  // Key labels -> their positions in the Redis strings.
  absl::flat_hash_map<std::string, int> key_label_map;
  // The set of feature IDs which will be extracted for this table.
  absl::flat_hash_set<uint64_t> feature_ids;
  // Metadata for the set of rate feature IDs which will be computed for this
  // table. This is based on the above.
  std::vector<RateInfo> rate_feature_ids;
};

struct DatabaseInfo {
  std::unique_ptr<TableInfo> global;
  std::unique_ptr<TableInfo> content;
  std::unique_ptr<TableInfo> content_query;
  std::unique_ptr<TableInfo> user;
  std::unique_ptr<TableInfo> log_user;
  std::unique_ptr<TableInfo> query;
  std::unique_ptr<TableInfo> last_user_event;
  std::unique_ptr<TableInfo> last_log_user_event;
  std::unique_ptr<TableInfo> last_user_query;
  std::unique_ptr<TableInfo> last_log_user_query;
};

// Replaces the masked bits in `original` with the ones in `other`.
uint64_t replaceMaskedBits(uint64_t original, uint64_t other, uint64_t mask);

// Returns the ID of the aggregated feature corresponding to the given segmented
// feature. Returns 0 if the given feature is not segmented.
uint64_t getAggregateFeatureId(uint64_t feature_id);

class ReadFromCountersStage : public Stage {
 public:
  explicit ReadFromCountersStage(
      size_t id, std::unique_ptr<RedisClient> client, Caches& caches,
      const DatabaseInfo& database, uint64_t platform_id,
      const delivery::Request& req,
      const std::vector<delivery::Insertion>& insertions, uint64_t start_time,
      const UserAgent& user_agent, CountersContext& counters_context)
      : Stage(id),
        client_(std::move(client)),
        caches_(caches),
        database_(database),
        platform_id_(platform_id),
        req_(req),
        insertions_(insertions),
        start_time_(start_time),
        user_agent_(user_agent),
        counters_context_(counters_context) {}
  std::string name() const override { return "ReadFromCounters"; }

  void runSync() override {}

  void run(std::function<void()>&& done_cb,
           std::function<void(const std::chrono::duration<double>&,
                              std::function<void()>&&)>&&) override;

  // Public class functions for testing.
  void read(const TableInfo& table, const std::string& key,
            absl::flat_hash_map<uint64_t, uint64_t>& counts,
            std::shared_ptr<std::function<void()>> finish);
  void cacheAsideRead(std::unique_ptr<Cache>& cache, const TableInfo& table,
                      const std::string& key, uint64_t start_time,
                      absl::flat_hash_map<uint64_t, uint64_t>& counts,
                      std::shared_ptr<std::function<void()>> finish,
                      std::string_view segment = {});
  absl::flat_hash_map<uint64_t, uint64_t> parseCounts(
      const std::vector<std::string>& data, const TableInfo& table);
  absl::flat_hash_map<uint64_t, uint64_t> parseLastUser(
      const std::vector<std::string>& data, const TableInfo& table);

 private:
  std::unique_ptr<RedisClient> client_;
  Caches& caches_;
  const DatabaseInfo& database_;
  uint64_t platform_id_;
  const delivery::Request& req_;
  const std::vector<delivery::Insertion>& insertions_;
  uint64_t start_time_;
  const UserAgent& user_agent_;
  CountersContext& counters_context_;
};

class ProcessCountersStage : public Stage {
 public:
  explicit ProcessCountersStage(
      size_t id, const DatabaseInfo& database,
      const std::vector<delivery::Insertion>& insertions,
      FeatureContext& feature_context, CountersContext& counters_context)
      : Stage(id),
        database_(database),
        insertions_(insertions),
        feature_context_(feature_context),
        counters_context_(counters_context) {}
  std::string name() const override { return "ProcessCounters"; }

  void runSync() override;

 private:
  const DatabaseInfo& database_;
  const std::vector<delivery::Insertion>& insertions_;
  FeatureContext& feature_context_;
  CountersContext& counters_context_;
};

// Declared here for testing.
struct GlobalInfo {
  absl::flat_hash_map<uint64_t, float> rates;
  absl::flat_hash_map<uint64_t, float> smoothing_parameters;
};
GlobalInfo makeGlobalInfo(
    const std::vector<RateInfo>& rate_infos,
    const absl::flat_hash_map<uint64_t, uint64_t>& global_counts);
absl::flat_hash_map<uint64_t, float> makeSparse(
    const GlobalInfo& global_info,
    const absl::flat_hash_map<uint64_t, uint64_t>& counts,
    const std::vector<RateInfo>& rate_infos);
void mergeCountsIntoSparse(
    const absl::flat_hash_map<uint64_t, uint64_t>& counts,
    absl::flat_hash_map<uint64_t, float>& sparse,
    std::vector<std::string>& errors);
void mergeSparseIntoSparse(const absl::flat_hash_map<uint64_t, float>& src,
                           absl::flat_hash_map<uint64_t, float>& dst,
                           std::vector<std::string>& errors);
}  // namespace counters
}  // namespace delivery
