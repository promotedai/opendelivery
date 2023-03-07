// This interface is just for mocking purposes. "SQS" is specific to
// AWS.

#pragma once

#include <string>

namespace delivery {
class SqsClient {
 public:
  virtual ~SqsClient() = default;

  // No callback because this isn't intended to be followed by anything.
  virtual void sendMessage(const std::string& message_body) const = 0;
};
}  // namespace delivery
