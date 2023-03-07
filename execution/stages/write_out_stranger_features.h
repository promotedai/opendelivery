// This stage is responsible for sending stranger feature metadata to SQS for
// downstream reverse-mapping of hashlib outputs.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/stages/sqs_client.h"
#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
}  // namespace delivery

namespace delivery {
class WriteOutStrangerFeaturesStage : public Stage {
 public:
  WriteOutStrangerFeaturesStage(
      size_t id, double sample_rate, uint64_t start_time,
      const FeatureContext& feature_context,
      const std::vector<delivery::Insertion>& insertions,
      std::unique_ptr<SqsClient> sqs_client)
      : Stage(id),
        sample_rate_(sample_rate),
        start_time_(start_time),
        feature_context_(feature_context),
        insertions_(insertions),
        sqs_client_(std::move(sqs_client)) {}
  std::string name() const override { return "WriteOutStrangerFeatures"; }

  void runSync() override;

 private:
  double sample_rate_;
  uint64_t start_time_;
  const FeatureContext& feature_context_;
  const std::vector<delivery::Insertion>& insertions_;
  std::unique_ptr<SqsClient> sqs_client_;
};
}  // namespace delivery
