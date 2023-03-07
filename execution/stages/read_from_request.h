// This stage is responsible for features that can be trivially processed based
// on the request. Not all these features are at request scope. Technically this
// includes features for paged insertions.

#pragma once

#include <stddef.h>

#include <string>

#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
class Request;
}  // namespace delivery

namespace delivery {
const std::string user_agent_prefix = "Request.device.browser.user_agent=";
const std::string log_user_id_prefix = "Request.user_info.log_user_id=";
const std::string referrer_prefix = "Request.device.browser.referrer=";
const std::string content_id_prefix = "ContentId=";

class ReadFromRequestStage : public Stage {
 public:
  ReadFromRequestStage(size_t id, const delivery::Request& req,
                       const std::vector<delivery::Insertion>& insertions,
                       FeatureContext& feature_context)
      : Stage(id),
        req_(req),
        insertions_(insertions),
        feature_context_(feature_context) {}

  std::string name() const override { return "ReadFromRequest"; }

  void runSync() override;

 private:
  const delivery::Request& req_;
  const std::vector<delivery::Insertion>& insertions_;
  FeatureContext& feature_context_;
};
}  // namespace delivery
