#include "execution/feature_context.h"

#include <utility>

#include "execution/merge_maps.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
void FeatureContext::initialize(
    const std::vector<delivery::Insertion>& insertions) {
  insertion_features_ = std::vector<FeatureScope>(insertions.size());
  for (const auto& insertion : insertions) {
    size_t idx = insertion_id_to_idx_.size();
    insertion_id_to_idx_[insertion.content_id()] = idx;
  }
  // May be worth giving each map a large reservation.
}

void FeatureContext::addInsertionFeatures(
    std::string_view insertion_id,
    absl::flat_hash_map<uint64_t, float> features) {
  size_t idx = insertion_id_to_idx_.at(insertion_id);
  FeatureScope& scope = insertion_features_[idx];
  std::lock_guard<std::mutex> lock(scope.mutex);
  mergeMaps(scope.features, features);
}

void FeatureContext::addRequestFeatures(
    absl::flat_hash_map<uint64_t, float> features) {
  std::lock_guard<std::mutex> lock(request_features_.mutex);
  mergeMaps(request_features_.features, features);
}

void FeatureContext::addUserFeatures(
    absl::flat_hash_map<uint64_t, float> features) {
  std::lock_guard<std::mutex> lock(user_features_.mutex);
  mergeMaps(user_features_.features, features);
}

void FeatureContext::addInsertionFeatures(
    std::string_view insertion_id,
    delivery_private_features::Features features) {
  size_t idx = insertion_id_to_idx_.at(insertion_id);
  FeatureScope& scope = insertion_features_[idx];
  std::lock_guard<std::mutex> lock(scope.mutex);
  mergeMaps(scope.features, *features.mutable_sparse());
  mergeMaps(scope.int_features, *features.mutable_sparse_id());
  mergeMaps(scope.int_list_features, *features.mutable_sparse_id_list());
}

void FeatureContext::addUserFeatures(
    delivery_private_features::Features features) {
  std::lock_guard<std::mutex> lock(user_features_.mutex);
  mergeMaps(user_features_.features, *features.mutable_sparse());
  mergeMaps(user_features_.int_features, *features.mutable_sparse_id());
  mergeMaps(user_features_.int_list_features,
            *features.mutable_sparse_id_list());
}

void FeatureContext::addStrangerInsertionFeatures(
    std::string_view insertion_id,
    absl::flat_hash_map<uint64_t, float> features,
    absl::flat_hash_map<std::string, uint64_t> feature_paths) {
  size_t idx = insertion_id_to_idx_.at(insertion_id);
  FeatureScope& scope = insertion_features_[idx];
  std::lock_guard<std::mutex> lock(scope.mutex);
  mergeMaps(scope.features, features);
  mergeMaps(scope.stranger_feature_paths, feature_paths);
}

void FeatureContext::addStrangerRequestFeatures(
    absl::flat_hash_map<uint64_t, float> features,
    absl::flat_hash_map<std::string, uint64_t> feature_paths) {
  std::lock_guard<std::mutex> lock(request_features_.mutex);
  mergeMaps(request_features_.features, features);
  mergeMaps(request_features_.stranger_feature_paths, feature_paths);
}

void FeatureContext::addStrangerUserFeatures(
    absl::flat_hash_map<uint64_t, float> features,
    absl::flat_hash_map<std::string, uint64_t> feature_paths) {
  std::lock_guard<std::mutex> lock(user_features_.mutex);
  mergeMaps(user_features_.features, features);
  mergeMaps(user_features_.stranger_feature_paths, feature_paths);
}

void FeatureContext::processInsertionFeatures(
    std::string_view insertion_id,
    std::function<void(FeatureScope& insertion, const FeatureScope& request,
                       const FeatureScope& user)>&& processor) {
  size_t idx = insertion_id_to_idx_.at(insertion_id);
  FeatureScope& insertion_features = insertion_features_[idx];
  std::lock_guard<std::mutex> insertion_lock(insertion_features.mutex);
  std::lock_guard<std::mutex> request_lock(request_features_.mutex);
  std::lock_guard<std::mutex> user_lock(user_features_.mutex);
  processor(insertion_features, request_features_, user_features_);
}

void FeatureContext::processRequestFeatures(
    std::function<void(FeatureScope& request)>&& processor) {
  std::lock_guard<std::mutex> lock(request_features_.mutex);
  processor(request_features_);
}

void FeatureContext::processUserFeatures(
    std::function<void(FeatureScope& user)>&& processor) {
  std::lock_guard<std::mutex> lock(user_features_.mutex);
  processor(user_features_);
}

const FeatureScope& FeatureContext::getInsertionFeatures(
    std::string_view insertion_id) const {
  return insertion_features_[insertion_id_to_idx_.at(insertion_id)];
}

const FeatureScope& FeatureContext::getUserFeatures() const {
  return user_features_;
}

const FeatureScope& FeatureContext::getRequestFeatures() const {
  return request_features_;
}
}  // namespace delivery
