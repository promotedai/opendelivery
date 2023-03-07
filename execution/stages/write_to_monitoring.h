// This stage is responsible for gathering and exporting any data we want to
// monitor.

#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <utility>

#include "execution/stages/monitoring_client.h"
#include "execution/stages/stage.h"

namespace event {
class LogRequest;
}

namespace delivery {
class WriteToMonitoringStage : public Stage {
 public:
  WriteToMonitoringStage(size_t id, const event::LogRequest& log_req,
                         std::unique_ptr<MonitoringClient> monitoring_client)
      : Stage(id),
        log_req_(log_req),
        monitoring_client_(std::move(monitoring_client)) {}
  std::string name() const override { return "WriteToMonitoring"; }

  void runSync() override;

 private:
  const event::LogRequest& log_req_;
  std::unique_ptr<MonitoringClient> monitoring_client_;
};
}  // namespace delivery
