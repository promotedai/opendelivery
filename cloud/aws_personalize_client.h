// Responsible for interacting with AWS's Personalize client and returning
// standard C++ types.

#pragma once

#include <aws/personalize-runtime/PersonalizeRuntimeClient.h>

#include <functional>
#include <string>
#include <vector>

#include "execution/stages/personalize_client.h"

namespace delivery {
struct UserAgent;
}

namespace delivery {
class AwsPersonalizeClient : public PersonalizeClient {
 public:
  explicit AwsPersonalizeClient(
      const Aws::PersonalizeRuntime::PersonalizeRuntimeClient&
          personalize_client)
      : personalize_client_(personalize_client) {}

  virtual void getPersonalizedRanking(
      const std::string& campaign_arn, const UserAgent& user_agent,
      const std::vector<std::string>& input_list, const std::string& user_id,
      std::function<void(std::vector<PersonalizeResult>)>&& cb) const override;

 private:
  const Aws::PersonalizeRuntime::PersonalizeRuntimeClient& personalize_client_;
};
}  // namespace delivery
