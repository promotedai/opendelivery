// This stage is responsible for computing features that are based upon
// distributions across insertions.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
}  // namespace delivery

namespace delivery {
const std::string distribution_set_value_prefix = "DistPropSet=";
const std::string distribution_non_zero_value_prefix = "DistPropNonZero=";
const std::string distribution_percentile_all_prefix =
    "DistributionPercentileAll=";
const std::string distribution_percentile_non_zero_prefix =
    "DistributionPercentileNonZero=";
const std::string distribution_fraction_median_all_prefix =
    "DistributionFractionMedianAll=";
const std::string distribution_fraction_median_non_zero_prefix =
    "DistributionFractionMedianNonZero=";
const std::string distribution_feature_value_is_zero_prefix =
    "DistributionFeatureValueIsZero=";

class ComputeDistributionFeaturesStage : public Stage {
 public:
  ComputeDistributionFeaturesStage(
      size_t id, const std::vector<std::string>& distribution_feature_paths,
      const std::vector<delivery::Insertion>& insertions,
      FeatureContext& feature_context)
      : Stage(id),
        distribution_feature_paths_(distribution_feature_paths),
        insertions_(insertions),
        feature_context_(feature_context) {}

  std::string name() const override { return "ComputeDistributionFeatures"; }

  void runSync() override;

 private:
  const std::vector<std::string>& distribution_feature_paths_;
  const std::vector<delivery::Insertion>& insertions_;
  FeatureContext& feature_context_;
};

// Declared here for testing.
struct InsertionFeatureMetadata {
  std::string_view insertion_id;
  float value = 0;
};
struct DistributionFeatureMetadata {
  uint64_t base_id = 0;

  std::string set_value_path;
  uint64_t set_value_id = 0;
  std::string non_zero_value_path;
  uint64_t non_zero_value_id = 0;
  std::string percentile_all_path;
  uint64_t percentile_all_id = 0;
  std::string percentile_non_zero_path;
  uint64_t percentile_non_zero_id = 0;
  std::string fraction_median_all_path;
  uint64_t fraction_median_all_id = 0;
  std::string fraction_median_non_zero_path;
  uint64_t fraction_median_non_zero_id = 0;
  std::string feature_value_is_zero_path;
  uint64_t feature_value_is_zero_id = 0;

  int set_count = 0;
  int non_zero_count = 0;
  float median_value_all = 0;
  float median_value_non_zero = 0;

  // Associates an insertion ID and a particular feature value.
  std::vector<InsertionFeatureMetadata> insertion_features;
  // Maps an insertion id to its calculated feature value percentile across all
  // values.
  absl::flat_hash_map<std::string_view, float> all_feature_percentiles;
  // Maps an insertion id to its calculated feature value percentile across
  // those with non-zero-values.
  absl::flat_hash_map<std::string_view, float> non_zero_feature_percentiles;
};
std::vector<DistributionFeatureMetadata> initializeDistributionFeatureMetadata(
    const std::vector<std::string>& distribution_feature_paths);
void initializeInsertionFeatureMetadata(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context,
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata);
void applyDistributionFeaturesToRequest(
    FeatureContext& feature_context,
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata);
void calculateInsertionFeatureStats(
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata);
void applyDistributionFeaturesToInsertions(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context,
    std::vector<DistributionFeatureMetadata>& configured_feature_metadata);
}  // namespace delivery
