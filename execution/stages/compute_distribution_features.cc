#include "execution/stages/compute_distribution_features.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/str_cat.h"
#include "execution/feature_context.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
// Convenience tuple to simplify default dist features.
struct DistFeature {
  std::string path;
  uint64_t id = 0;
};

// These are manually selected. In the future our config setup should be changed
// so it can specify well-known features and then this can be moved to there.
const std::vector<DistFeature> default_dist_features = {
    DistFeature{
        .path = "ITEM_DEVICE_RATE_SMOOTH_NAVIGATE_IMPRESSION_30DAY",
        .id =
            delivery_private_features::DAY_30 +
            delivery_private_features::COUNT_NAVIGATE +
            delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_IMPRESSION},
    DistFeature{
        .path = "ITEM_RATE_SMOOTH_NAVIGATE_IMPRESSION_30DAY",
        .id = delivery_private_features::DAY_30 +
              delivery_private_features::COUNT_NAVIGATE +
              delivery_private_features::ITEM_RATE_SMOOTH_OVER_IMPRESSION},
    DistFeature{
        .path = "ITEM_RATE_SMOOTH_CHECKOUT_NAVIGATE_30DAY",
        .id = delivery_private_features::DAY_30 +
              delivery_private_features::COUNT_CHECKOUT +
              delivery_private_features::ITEM_RATE_SMOOTH_OVER_NAVIGATE},
    DistFeature{
        .path = "ITEM_RATE_SMOOTH_PURCHASE_NAVIGATE_30DAY",
        .id = delivery_private_features::DAY_30 +
              delivery_private_features::COUNT_PURCHASE +
              delivery_private_features::ITEM_RATE_SMOOTH_OVER_NAVIGATE}};

std::vector<DistributionFeatureMetadata> initializeDistributionFeatureMetadata(
    const std::vector<std::string>& distribution_feature_paths) {
  std::vector<DistributionFeatureMetadata> ret;

  // Concatenate default features and specified (sparse) ones.
  std::vector<DistFeature> dist_features(default_dist_features.begin(),
                                         default_dist_features.end());
  dist_features.reserve(dist_features.size() +
                        distribution_feature_paths.size());
  for (const auto& path : distribution_feature_paths) {
    dist_features.emplace_back(
        DistFeature{.path = path, .id = hashlib::makeHash(path)});
  }

  ret.reserve(dist_features.size());
  for (const auto& feature : dist_features) {
    DistributionFeatureMetadata metadata;
    metadata.base_id = feature.id;

    metadata.set_value_path =
        absl::StrCat(distribution_set_value_prefix, feature.path);
    metadata.set_value_id = hashlib::makeHash(metadata.set_value_path);
    metadata.non_zero_value_path =
        absl::StrCat(distribution_non_zero_value_prefix, feature.path);
    metadata.non_zero_value_id =
        hashlib::makeHash(metadata.non_zero_value_path);
    metadata.percentile_all_path =
        absl::StrCat(distribution_percentile_all_prefix, feature.path);
    metadata.percentile_all_id =
        hashlib::makeHash(metadata.percentile_all_path);
    metadata.percentile_non_zero_path =
        absl::StrCat(distribution_percentile_non_zero_prefix, feature.path);
    metadata.percentile_non_zero_id =
        hashlib::makeHash(metadata.percentile_non_zero_path);
    metadata.fraction_median_all_path =
        absl::StrCat(distribution_fraction_median_all_prefix, feature.path);
    metadata.fraction_median_all_id =
        hashlib::makeHash(metadata.fraction_median_all_path);
    metadata.fraction_median_non_zero_path = absl::StrCat(
        distribution_fraction_median_non_zero_prefix, feature.path);
    metadata.fraction_median_non_zero_id =
        hashlib::makeHash(metadata.fraction_median_non_zero_path);
    metadata.feature_value_is_zero_path =
        absl::StrCat(distribution_feature_value_is_zero_prefix, feature.path);
    metadata.feature_value_is_zero_id =
        hashlib::makeHash(metadata.feature_value_is_zero_path);

    ret.emplace_back(std::move(metadata));
  }

  return ret;
}

void initializeInsertionFeatureMetadata(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context,
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata) {
  // Avoid resizes in the subsequent loop.
  for (auto& metadata : configured_feature_metadata) {
    metadata.insertion_features.reserve(insertions.size());
  }

  for (const auto& insertion : insertions) {
    feature_context.processInsertionFeatures(
        insertion.content_id(),
        [&configured_feature_metadata, &insertion](
            FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
          for (auto& metadata : configured_feature_metadata) {
            auto it = scope.features.find(metadata.base_id);
            if (it != scope.features.end()) {
              metadata.insertion_features.emplace_back(InsertionFeatureMetadata{
                  .insertion_id = insertion.content_id(), .value = it->second});
              ++metadata.set_count;
              metadata.non_zero_count += (it->second != 0);
            } else {
              metadata.insertion_features.emplace_back(InsertionFeatureMetadata{
                  .insertion_id = insertion.content_id()});
            }
          }
        });
  }

  // We do an ascending sort to simplify skipping of zero-values and calculation
  // of percentiles. Stability isn't important because ties are handled
  // downstream.
  for (auto& metadata : configured_feature_metadata) {
    sort(metadata.insertion_features.begin(), metadata.insertion_features.end(),
         [](const InsertionFeatureMetadata& a,
            const InsertionFeatureMetadata& b) { return a.value < b.value; });
  }
}

void applyDistributionFeaturesToRequest(
    FeatureContext& feature_context,
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata) {
  feature_context.processRequestFeatures(
      [&configured_feature_metadata](FeatureScope& scope) {
        for (const auto& metadata : configured_feature_metadata) {
          scope.features[metadata.set_value_id] =
              static_cast<float>(metadata.set_count);
          scope.features[metadata.non_zero_value_id] =
              static_cast<float>(metadata.non_zero_count);

          // Just report strangers at request scope instead of for each
          // insertion.
          scope.stranger_feature_paths[metadata.set_value_path] =
              metadata.set_value_id;
          scope.stranger_feature_paths[metadata.non_zero_value_path] =
              metadata.non_zero_value_id;
          scope.stranger_feature_paths[metadata.percentile_all_path] =
              metadata.percentile_all_id;
          scope.stranger_feature_paths[metadata.percentile_non_zero_path] =
              metadata.percentile_non_zero_id;
          scope.stranger_feature_paths[metadata.fraction_median_all_path] =
              metadata.fraction_median_all_id;
          scope.stranger_feature_paths[metadata.fraction_median_non_zero_path] =
              metadata.fraction_median_non_zero_id;
          scope.stranger_feature_paths[metadata.feature_value_is_zero_path] =
              metadata.feature_value_is_zero_id;
        }
      });
}

// Calculates normalized rank percentiles in the range [0, 1].
absl::flat_hash_map<std::string_view, float>
calculateInsertionFeaturePercentiles(
    std::vector<InsertionFeatureMetadata>& metadata, bool only_non_zero) {
  absl::flat_hash_map<std::string_view, float> ret;

  size_t first_index = 0;
  // This is sorted so all of the zeroes are at the front.
  if (only_non_zero) {
    for (; first_index < metadata.size() && metadata[first_index].value == 0;
         ++first_index) {
    }
  }
  ret.reserve(metadata.size() - first_index);
  size_t last_index = metadata.size() - 1;
  for (size_t i = first_index; i <= last_index; ++i) {
    size_t loop_iteration_first_index = i;
    float percentile =
        static_cast<float>(loop_iteration_first_index - first_index);
    if (last_index != first_index) {  // Avoid division by 0.
      percentile = percentile / static_cast<float>(last_index - first_index);
    }

    // Make sure ties all get the same value.
    size_t tie_count = 0;
    while (i < last_index && metadata[i].value == metadata[i + 1].value) {
      ++tie_count;
      ++i;
    }

    // <= because there's always a 0th "tie" and it's the insertion we started
    // on.
    for (size_t j = 0; j <= tie_count; ++j) {
      ret[metadata[loop_iteration_first_index + j].insertion_id] = percentile;
    }
  }

  return ret;
}

void calculateInsertionFeatureStats(
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata) {
  for (auto& metadata : configured_feature_metadata) {
    metadata.all_feature_percentiles =
        calculateInsertionFeaturePercentiles(metadata.insertion_features,
                                             /*only_non_zero=*/false);
    metadata.non_zero_feature_percentiles =
        calculateInsertionFeaturePercentiles(metadata.insertion_features,
                                             /*only_non_zero=*/true);

    if (metadata.set_count > 0) {
      metadata.median_value_all =
          metadata.insertion_features[metadata.insertion_features.size() / 2]
              .value;
    }
    if (metadata.non_zero_count > 0) {
      // We can assume non-zero-values are sorted to the end.
      size_t median_non_zero_pos =
          metadata.insertion_features.size() - metadata.non_zero_count / 2;
      metadata.median_value_non_zero =
          metadata.insertion_features[median_non_zero_pos].value;
    }
  }
}

void applyDistributionFeaturesToInsertions(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context,
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata) {
  for (const auto& insertion : insertions) {
    feature_context.processInsertionFeatures(
        insertion.content_id(),
        [&configured_feature_metadata, &insertion](
            FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
          auto& features = scope.features;
          for (auto& metadata : configured_feature_metadata) {
            features[metadata.percentile_all_id] =
                metadata.all_feature_percentiles[insertion.content_id()];
            features[metadata.percentile_non_zero_id] =
                metadata.non_zero_feature_percentiles[insertion.content_id()];

            auto it = features.find(metadata.base_id);
            if (it != features.end()) {
              float base_id_value = it->second;

              if (base_id_value == 0) {
                features[metadata.feature_value_is_zero_id] = 1;
              } else {
                features[metadata.fraction_median_all_id] =
                    metadata.median_value_all == 0
                        ? 0
                        : base_id_value / metadata.median_value_all;
                features[metadata.fraction_median_non_zero_id] =
                    metadata.median_value_non_zero == 0
                        ? 0
                        : base_id_value / metadata.median_value_non_zero;
              }
            }
          }
        });
  }
}

void ComputeDistributionFeaturesStage::runSync() {
  // If there are no insertions, there are no distributions.
  if (insertions_.empty()) {
    return;
  }

  // We can cache this based on the config if this ends up being expensive.
  auto configured_feature_metadata =
      initializeDistributionFeatureMetadata(distribution_feature_paths_);

  initializeInsertionFeatureMetadata(insertions_, feature_context_,
                                     configured_feature_metadata);

  applyDistributionFeaturesToRequest(feature_context_,
                                     configured_feature_metadata);

  calculateInsertionFeatureStats(configured_feature_metadata);

  applyDistributionFeaturesToInsertions(insertions_, feature_context_,
                                        configured_feature_metadata);
}
}  // namespace delivery
