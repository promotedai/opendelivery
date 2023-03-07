// This is the interface for delivery to export monitoring-specific data.

#pragma once

#include <functional>
#include <string>
#include <vector>

namespace delivery {
struct MonitoringData {
  int request_insertion_count = 0;
  int feature_count = 0;
};

class MonitoringClient {
 public:
  virtual ~MonitoringClient() = default;
  virtual void write(const MonitoringData& data) = 0;
};
}  // namespace delivery
