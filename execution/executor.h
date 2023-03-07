// This is meant to abstract execution details from everybody else:
// - Event loops are (kinda) global state
// - Stages can assume they're only being run if all their inputs are ready
// - Stages can not think about cancellations + request-level timeouts
// - Arbitrary runtime stats
// - Text logging

#pragma once

#include <stddef.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// For cache types.
#include "execution/stages/cache.h"
#include "execution/stages/stage.h"
#include "proto/delivery/INTERNAL_execution.pb.h"

namespace delivery {
class DeliveryLogWriter;
class FeatureStoreClient;
class MonitoringClient;
class PersonalizeClient;
class RedisClient;
class SqsClient;
struct PeriodicTimeValues;
namespace counters {
class Caches;
class DatabaseInfo;
}  // namespace counters

// To abstract global state from executor building.
struct ConfigurationOptions {
  // "Client" getters.
  std::function<std::unique_ptr<RedisClient>()> paging_read_redis_client_getter;
  std::function<std::unique_ptr<RedisClient>()>
      paging_write_redis_client_getter;
  std::function<std::unique_ptr<RedisClient>()> counters_redis_client_getter;
  std::function<std::unique_ptr<FeatureStoreClient>()>
      feature_store_client_getter;
  std::function<std::unique_ptr<PersonalizeClient>()> personalize_client_getter;
  std::function<std::unique_ptr<DeliveryLogWriter>()>
      delivery_log_writer_getter;
  std::function<std::unique_ptr<SqsClient>()> sqs_client_getter;
  std::function<std::unique_ptr<MonitoringClient>()> monitoring_client_getter;

  // Cache getters.
  std::function<FeaturesCache&()> content_features_cache_getter;
  std::function<FeaturesCache&()> non_content_features_cache_getter;
  std::function<counters::Caches&()> counters_caches_getter;

  // Misc.
  const counters::DatabaseInfo* counters_database = nullptr;
  const PeriodicTimeValues* periodic_time_values = nullptr;
};

// Just representing the execution graph as an adjacency list for now.
struct ExecutorNode {
  // This is an atomic counter to not assume execution happens on a single
  // thread. std::atomic has very specific semantics that make it unusable in
  // the standard containers, so we wrap it in a more friendly type.
  std::unique_ptr<std::atomic<size_t>> remaining_inputs =
      std::make_unique<std::atomic<size_t>>(0);
  std::unique_ptr<Stage> stage;
  std::vector<size_t> output_ids;
  delivery::DeliveryLatency latency;
  uint64_t duration_start = 0;
};

class Executor {
 public:
  virtual ~Executor() {}

  virtual void execute() = 0;

  // Just needed for exporting latencies. This version is bad because it's
  // leaking an assumed graph structure for all implementations. (ExecutorNode
  // shouldn't even be defined here.) Can refactor once our observability story
  // is clearer to me.
  virtual const std::vector<ExecutorNode>& nodes() const = 0;
};
}  // namespace delivery
