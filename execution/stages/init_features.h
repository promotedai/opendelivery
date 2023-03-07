// This stage is responsible for initializing the feature context used by other
// stages. This is split from InitStage because other stages (e.g. paging) can
// change our insertion set.

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
class InitFeaturesStage : public Stage {
 public:
  InitFeaturesStage(size_t id,
                    const std::vector<delivery::Insertion>& insertions,
                    FeatureContext& feature_context)
      : Stage(id), insertions_(insertions), feature_context_(feature_context) {}

  std::string name() const override { return "InitFeatures"; }

  void runSync() override;

 private:
  const std::vector<delivery::Insertion>& insertions_;
  FeatureContext& feature_context_;
};
}  // namespace delivery
