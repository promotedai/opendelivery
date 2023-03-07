// This stage is responsible for stripping out all features which could be
// related to the specific user. This is to allow sellers and internal users to
// receive non-personalized results.

#pragma once

#include <stddef.h>

#include <optional>  
#include <string>  
#include <vector>  

#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
struct ExcludeUserFeaturesConfig;
}

namespace delivery {
class ExcludeUserFeaturesStage : public Stage {
 public:
  ExcludeUserFeaturesStage(
      size_t id, bool ignore_usage,
      const std::optional<ExcludeUserFeaturesConfig>& config,
      FeatureContext& feature_context,
      const std::vector<delivery::Insertion>& insertions)
      : Stage(id),
        ignore_usage_(ignore_usage),
        config_(config),
        feature_context_(feature_context),
        insertions_(insertions) {}

  std::string name() const override { return "ExcludeUserFeatures"; }

  void runSync() override;

 private:
  bool ignore_usage_;
  const std::optional<ExcludeUserFeaturesConfig>& config_;
  FeatureContext& feature_context_;
  const std::vector<delivery::Insertion>& insertions_;
};
}  // namespace delivery
