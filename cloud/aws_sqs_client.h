// Responsible for interacting with AWS's SQS client.

#pragma once

#include <string>
#include <utility>

#include "execution/stages/sqs_client.h"

namespace Aws {
namespace SQS {
class SQSClient;
}
}  // namespace Aws

namespace delivery {
class AwsSqsClient : public SqsClient {
 public:
  explicit AwsSqsClient(
      std::pair<const Aws::SQS::SQSClient&, const std::string&> client_and_url)
      : client_(client_and_url.first), url_(client_and_url.second) {}

  void sendMessage(const std::string& message_body) const override;

 private:
  const Aws::SQS::SQSClient& client_;
  const std::string& url_;
};
}  // namespace delivery
