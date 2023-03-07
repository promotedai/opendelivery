#include "execution/stages/read_from_feature_store.h"

#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "cache.h"
#include "config/feature_store_config.h"
#include "execution/stages/cache.h"
#include "feature_store_client.h"
#include "proto/delivery/private/features/features.pb.h"
#include "utils/time.h"

namespace delivery {
void deserializeAndCache(
    const std::vector<FeatureStoreResult>& results,
    const std::vector<std::string>& keys_to_fetch, uint64_t start_time,
    FeaturesCache& cache,
    std::function<void(std::string_view, delivery_private_features::Features)>&
        feature_adder,
    std::vector<std::string>& errors) {
  // Make a set for faster intersection.
  absl::flat_hash_set<std::string_view> keys_without_results;
  keys_without_results.reserve(keys_to_fetch.size());
  for (const auto& key : keys_to_fetch) {
    keys_without_results.emplace(key);
  }

  for (const auto& result : results) {
    delivery_private_features::Features features;
    for (const auto& column_bytes : result.columns_bytes) {
      // The values in feature store are actually FeaturesLists instead of
      // Features.
      delivery_private_features::FeaturesList features_list;
      if (!features_list.MergeFromString(column_bytes)) {
        errors.emplace_back(absl::StrCat(
            "Unable to deserialize feature list for ID ", result.key));
      }
      for (const auto& element : features_list.features()) {
        features.MergeFrom(element);
      }
    }
    std::string timed_key = makeTimedKey(result.key, start_time);
    CacheKey cache_key(timed_key.data(), timed_key.size());
    cache.insert(cache_key, features);
    feature_adder(result.key, std::move(features));
    keys_without_results.erase(result.key);
  }

  // Cache empty results for the keys we didn't receive.
  for (std::string_view key : keys_without_results) {
    std::string timed_key = makeTimedKey(key, start_time);
    CacheKey cache_key(timed_key.data(), timed_key.size());
    cache.insert(cache_key, {});
  }
}

void ReadFromFeatureStoreStage::runSync() {
  deserializeAndCache(results_, keys_to_fetch_, start_time_, cache_,
                      feature_adder_, errors_);

  done_cb_();
}

// Cached keys end up populating `id_to_features`. Keys missing from the cache
// end up populating `keys_to_fetch`.
void processCachedKeys(
    const std::vector<std::string>& keys, uint64_t start_time,
    FeaturesCache& cache,
    std::function<void(std::string_view, delivery_private_features::Features)>&
        feature_adder,
    std::vector<std::string>& keys_to_fetch) {
  for (const auto& key : keys) {
    std::string timed_key = makeTimedKey(key, start_time);
    CacheKey cache_key(timed_key.data(), timed_key.size());
    FeaturesCache::ConstAccessor accessor;
    if (cache.find(accessor, cache_key)) {
      feature_adder(key, *accessor.get());
    } else {
      keys_to_fetch.emplace_back(key);
    }
  }
}

// Just used to avoid races between the client async call and it timing out.
struct CoordinationState {
  std::mutex mutex;
  bool already_finished = false;
};

void ReadFromFeatureStoreStage::run(
    std::function<void()>&& cb,
    std::function<void(const std::chrono::duration<double>& delay,
                       std::function<void()>&& cb)>&& timeout_cb) {
  done_cb_ = cb;

  // Keys which are not present in feature store will just be missing from the
  // response. We stash these to later recognize which keys were not in the
  // response.
  processCachedKeys(key_generator_(), start_time_, cache_, feature_adder_,
                    keys_to_fetch_);
  // If everything was cached, then skip feature store.
  if (keys_to_fetch_.empty()) {
    done_cb_();
    return;
  }

  auto state = std::make_shared<CoordinationState>();

  // If we made it this far, we have to wait on feature store. Might be
  // worthwhile to make these callbacks just update the cache asynchronously
  // while this request just goes on without it.
  const std::string columns = absl::StrCat(
      config_.primary_key, ",", absl::StrJoin(config_.feature_columns, ","));
  if (keys_to_fetch_.size() == 1) {
    client_->read(config_.table, config_.primary_key, keys_to_fetch_[0],
                  columns,
                  [this, state](std::vector<FeatureStoreResult> results) {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    // If we already timed out, do nothing. We have to gate
                    // access to stage fields because this instance could not
                    // exist any more by the time this functor happens.
                    if (state->already_finished) {
                      return;
                    }
                    state->already_finished = true;
                    this->results_ = std::move(results);
                    runSync();
                  });
  } else {
    client_->readBatch(config_.table, config_.primary_key, keys_to_fetch_,
                       columns,
                       [this, state](std::vector<FeatureStoreResult> results) {
                         std::lock_guard<std::mutex> lock(state->mutex);
                         // If we already timed out, do nothing.
                         if (state->already_finished) {
                           return;
                         }
                         state->already_finished = true;
                         this->results_ = std::move(results);
                         runSync();
                       });
  }
  int timeout;
  try {
    timeout = std::stoi(timeout_);
  } catch (const std::exception&) {
    errors_.emplace_back(
        absl::StrCat("Invalid feature store timeout specified: ", timeout_,
                     ". Defaulting to 500ms."));
    timeout = 500;
  }
  timeout_cb(std::chrono::milliseconds(timeout), [this, state]() {
    std::lock_guard<std::mutex> lock(state->mutex);
    // Check if we didn't time out.
    if (state->already_finished) {
      return;
    }
    state->already_finished = true;
    // We call this instead of runSync() to not cache every key with empty
    // results.
    done_cb_();
  });
}
}  // namespace delivery
