// This stage is responsible for computing ratios of other feature values.

#pragma once

#include <stddef.h>

#include <string>
#include <vector>

#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
}  // namespace delivery

namespace delivery {
class ComputeRatioFeaturesStage : public Stage {
 public:
  ComputeRatioFeaturesStage(size_t id, FeatureContext& feature_context,
                            const std::vector<delivery::Insertion>& insertions)
      : Stage(id), feature_context_(feature_context), insertions_(insertions) {}

  std::string name() const override { return "ComputeRatioFeatures"; }

  void runSync() override;

 private:
  FeatureContext& feature_context_;
  const std::vector<delivery::Insertion>& insertions_;
};
}  // namespace delivery
