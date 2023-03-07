#include "cloud/aws_sqs_client.h"

#include <aws/core/utils/Outcome.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/SQSErrors.h>
#include <aws/sqs/SQSServiceClientModel.h>

#include <memory>

#include "aws/sqs/model/SendMessageRequest.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace Aws {
namespace Client {
class AsyncCallerContext;
}
}  // namespace Aws

namespace delivery {
void AwsSqsClient::sendMessage(const std::string& message_body) const {
  Aws::SQS::Model::SendMessageRequest req;
  req.SetMessageBody(message_body);
  req.SetQueueUrl(url_);

  client_.SendMessageAsync(
      req,
      [](const Aws::SQS::SQSClient*, const Aws::SQS::Model::SendMessageRequest&,
         const Aws::SQS::Model::SendMessageOutcome& outcome,
         const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        if (!outcome.IsSuccess()) {
          LOG_ERROR << "Response error from SQS: "
                    << outcome.GetError().GetMessage();
        }
      });
}
}  // namespace delivery
