// This stage is responsible for reading from feature store and structuring that
// data for downstream processing.

#pragma once

#include <stddef.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/stages/cache.h"
#include "execution/stages/feature_store_client.h"
#include "execution/stages/stage.h"

namespace delivery {
struct FeatureStoreConfig;
}
namespace delivery_private_features {
class Features;
}

namespace delivery {
class ReadFromFeatureStoreStage : public Stage {
 public:
  ReadFromFeatureStoreStage(
      size_t id, FeaturesCache& cache,
      std::unique_ptr<const FeatureStoreClient> client,
      const FeatureStoreConfig& config, const std::string& timeout,
      uint64_t start_time,
      std::function<std::vector<std::string>()>&& key_generator,
      std::function<void(std::string_view,
                         delivery_private_features::Features)>&& feature_adder)
      : Stage(id),
        cache_(cache),
        client_(std::move(client)),
        config_(config),
        timeout_(timeout),
        start_time_(start_time),
        key_generator_(key_generator),
        feature_adder_(feature_adder) {}
  std::string name() const override { return "ReadFromFeatureStore"; }

  void runSync() override;

  void run(std::function<void()>&& done_cb,
           std::function<void(const std::chrono::duration<double>& delay,
                              std::function<void()>&& cb)>&&) override;

 private:
  FeaturesCache& cache_;
  std::unique_ptr<const FeatureStoreClient> client_;
  const FeatureStoreConfig& config_;
  const std::string& timeout_;
  uint64_t start_time_;
  std::function<std::vector<std::string>()> key_generator_;
  std::vector<std::string> keys_to_fetch_;
  std::function<void(std::string_view, delivery_private_features::Features)>
      feature_adder_;
  // This is the one from run(), which calls back to the executor.
  std::function<void()> done_cb_;
  std::vector<FeatureStoreResult> results_;
};

// Declared here for testing.
void deserializeAndCache(
    const std::vector<FeatureStoreResult>& results,
    const std::vector<std::string>& keys_to_fetch, uint64_t start_time,
    FeaturesCache& cache,
    std::function<void(std::string_view, delivery_private_features::Features)>&
        feature_adder,
    std::vector<std::string>& errors);
void processCachedKeys(
    const std::vector<std::string>& keys, uint64_t start_time,
    FeaturesCache& cache,
    std::function<void(std::string_view, delivery_private_features::Features)>&
        feature_adder,
    std::vector<std::string>& keys_to_fetch);
}  // namespace delivery
