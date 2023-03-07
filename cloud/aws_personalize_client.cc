#include "cloud/aws_personalize_client.h"

#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/personalize-runtime/PersonalizeRuntimeErrors.h>
#include <aws/personalize-runtime/PersonalizeRuntimeServiceClientModel.h>
#include <aws/personalize-runtime/model/GetPersonalizedRankingResult.h>
#include <aws/personalize-runtime/model/PredictedItem.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "aws/personalize-runtime/model/GetPersonalizedRankingRequest.h"
#include "execution/stages/personalize_client.h"
#include "execution/user_agent.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace Aws {
namespace Client {
class AsyncCallerContext;
}
}  // namespace Aws

namespace delivery {
void AwsPersonalizeClient::getPersonalizedRanking(
    const std::string& campaign_arn, const UserAgent& user_agent,
    const std::vector<std::string>& input_list, const std::string& user_id,
    std::function<void(std::vector<PersonalizeResult>)>&& cb) const {
  Aws::PersonalizeRuntime::Model::GetPersonalizedRankingRequest req;
  req.SetCampaignArn(campaign_arn);
  Aws::Map<Aws::String, Aws::String> context;
  context.emplace("OS", user_agent.os);
  context.emplace("APP", user_agent.app);
  req.SetContext(context);
  req.SetInputList(input_list);
  req.SetUserId(user_id);

  personalize_client_.GetPersonalizedRankingAsync(
      req,
      [cb](const Aws::PersonalizeRuntime::PersonalizeRuntimeClient*,
           const Aws::PersonalizeRuntime::Model::GetPersonalizedRankingRequest&,
           const Aws::PersonalizeRuntime::Model::GetPersonalizedRankingOutcome&
               outcome,
           const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        std::vector<PersonalizeResult> results;
        if (outcome.IsSuccess()) {
          const auto& rankings = outcome.GetResult().GetPersonalizedRanking();
          results.reserve(rankings.size());
          for (const auto& ranking : rankings) {
            results.emplace_back(PersonalizeResult{
                .id = ranking.GetItemId(),
                .score = static_cast<float>(ranking.GetScore())});
          }
        } else {
          LOG_ERROR << "Response error from Personalize: "
                    << outcome.GetError().GetMessage();
        }
        cb(std::move(results));
      });
}
}  // namespace delivery
