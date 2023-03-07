// This interface is just for mocking purposes. "Personalize" is specific to
// AWS.

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "execution/user_agent.h"

namespace delivery {
struct PersonalizeResult {
  std::string id;
  float score = 0;
};

class PersonalizeClient {
 public:
  virtual ~PersonalizeClient() = default;

  // If there's an error, feeds an empty vector into the callback.
  virtual void getPersonalizedRanking(
      const std::string& campaign_arn, const UserAgent& user_agent,
      const std::vector<std::string>& input_list, const std::string& user_id,
      std::function<void(std::vector<PersonalizeResult>)>&& cb) const = 0;
};
}  // namespace delivery
