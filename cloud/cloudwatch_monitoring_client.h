// This is a Cloudwatch-specific implementation of allowing delivery to write
// out arbitrary data for monitoring.

#pragma once

#include <string>

#include "execution/stages/monitoring_client.h"

namespace Aws {
namespace CloudWatch {
class CloudWatchClient;
}
}  // namespace Aws

namespace delivery {
class CloudwatchMonitoringClient : public MonitoringClient {
 public:
  explicit CloudwatchMonitoringClient(Aws::CloudWatch::CloudWatchClient& client,
                                      const std::string& platform)
      : client_(client), platform_(platform) {}

  void write(const MonitoringData& data) override;

 private:
  Aws::CloudWatch::CloudWatchClient& client_;
  const std::string& platform_;
};
}  // namespace delivery
