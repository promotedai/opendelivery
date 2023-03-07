// This stage is responsible for using hashlib to flatten stranger features.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
class Request;
}  // namespace delivery

namespace delivery {
class FlattenStage : public Stage {
 public:
  FlattenStage(size_t id, const delivery::Request& req,
               const std::vector<delivery::Insertion>& insertions,
               uint64_t max_request_properties,
               uint64_t max_insertion_properties,
               FeatureContext& feature_context)
      : Stage(id),
        req_(req),
        insertions_(insertions),
        max_request_properties_(max_request_properties),
        max_insertion_properties_(max_insertion_properties),
        feature_context_(feature_context) {}

  std::string name() const override { return "Flatten"; }

  void runSync() override;

 private:
  const delivery::Request& req_;
  const std::vector<delivery::Insertion>& insertions_;
  uint64_t max_request_properties_;
  uint64_t max_insertion_properties_;
  FeatureContext& feature_context_;
};
}  // namespace delivery
