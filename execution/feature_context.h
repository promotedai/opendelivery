// The work of delivery is largely to amass different representations of
// features and to make them available to our ML models. This class is intended
// to simplify things for stages, whether they produce or consume features.
//
// In a sense, this stash of features is the heart of processing. In the long
// term our representation of feature processing should be fundamentally tied to
// the execution structure itself for arbitrary complexity.

#pragma once

#include <stddef.h>

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
class Insertion;
}

namespace delivery {
// Features have three scopes:
// 1. Insertion
// 2. Request
// 3. User
// Request- and user-scoped features are not *explicitly* in the scope of each
// insertion.
//
// Practically, "stranger" means that hashlib isn't reversible and that, for
// feature IDs produced by it, we must also record metadata to recognize these
// features later. Formally, "stranger" means a feature is outside of the
// well-known range *and* that it was computed by delivery. Features from
// feature store can be outside of the well-known range, but not be computed by
// delivery. We assume the feature store system is responsible for identifying
// them. Features based on counters can be computed by delivery, but be within
// the well-known range.
struct FeatureScope {
  // Hard to say at this point how bad conversion and merging of maps is, but
  // avoiding these inside of a given scope significantly complicates these
  // interfaces. Deciding to accept the cost for now. Using flat_hash_map as
  // the destination type since it should generally perform better.
  absl::flat_hash_map<uint64_t, float> features;  // All strangers end up here.
  absl::flat_hash_map<uint64_t, int64_t> int_features;
  absl::flat_hash_map<uint64_t, std::vector<int64_t>> int_list_features;

  absl::flat_hash_map<std::string, uint64_t> stranger_feature_paths;

  // Don't expect much contention here, so keeping it simple.
  std::mutex mutex;
};

// This API has four jobs:
// 1. Allow for adding batches of features with specified scopes.
// 2. Allow for removal of user-scoped features.
// 3. Allow for retrieval of particular feature scopes.
// 4. Allow for retrieval of mappings from stranger features to their original
// paths.
class FeatureContext {
 public:
  // Must be called before anything else. All other functions are thread-safe.
  void initialize(const std::vector<delivery::Insertion>& insertions);

  void addInsertionFeatures(std::string_view insertion_id,
                            absl::flat_hash_map<uint64_t, float> features);
  void addRequestFeatures(absl::flat_hash_map<uint64_t, float> features);
  void addUserFeatures(absl::flat_hash_map<uint64_t, float> features);

  // Convenience overloads.
  void addInsertionFeatures(std::string_view insertion_id,
                            delivery_private_features::Features features);
  void addUserFeatures(delivery_private_features::Features features);

  void addStrangerInsertionFeatures(
      std::string_view insertion_id,
      absl::flat_hash_map<uint64_t, float> features,
      absl::flat_hash_map<std::string, uint64_t> feature_paths);
  void addStrangerRequestFeatures(
      absl::flat_hash_map<uint64_t, float> features,
      absl::flat_hash_map<std::string, uint64_t> feature_paths);
  void addStrangerUserFeatures(
      absl::flat_hash_map<uint64_t, float> features,
      absl::flat_hash_map<std::string, uint64_t> feature_paths);

  // For processing based on already-added features. Note:
  // - Processors are *not* allowed to do async work.
  // - Calling the add*() functions from a processor will deadlock. Use the
  // passed-in scope directly.
  // - Remember to properly handle strangers.
  void processInsertionFeatures(
      std::string_view insertion_id,
      std::function<void(FeatureScope& insertion, const FeatureScope& request,
                         const FeatureScope& user)>&& processor);
  void processRequestFeatures(
      std::function<void(FeatureScope& request)>&& processor);
  void processUserFeatures(std::function<void(FeatureScope& user)>&& processor);

  // No more additions or processing is allowed once these functions are used.
  // Although these functions are thread-safe, they leak references. This is
  // unlikely to be an issue since they are only needed for prediction and
  // stages that run after responding. If necessary, we can add a simple state
  // machine to enforce this.
  const FeatureScope& getInsertionFeatures(std::string_view insertion_id) const;
  const FeatureScope& getUserFeatures() const;
  const FeatureScope& getRequestFeatures() const;

 private:
  absl::flat_hash_map<std::string, size_t> insertion_id_to_idx_;
  std::vector<FeatureScope> insertion_features_;
  FeatureScope user_features_;
  FeatureScope request_features_;
};
}  // namespace delivery
