#include "write_to_monitoring.h"

#include <vector>

#include "monitoring_client.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/execution.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "proto/event/event.pb.h"

namespace delivery {
void WriteToMonitoringStage::runSync() {
  if (log_req_.delivery_log().empty()) {
    errors_.emplace_back(
        "Trying to create monitoring data with no delivery log");
    return;
  }
  const auto& delivery_log = log_req_.delivery_log(0);

  MonitoringData data;
  data.request_insertion_count = delivery_log.request().insertion_size();
  for (const auto& insertion : delivery_log.execution().execution_insertion()) {
    data.feature_count += insertion.feature_stage().features().sparse_size();
  }

  monitoring_client_->write(data);
}
}  // namespace delivery
