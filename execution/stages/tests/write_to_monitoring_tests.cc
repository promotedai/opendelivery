#include <google/protobuf/stubs/port.h>

#include <memory>
#include <utility>

#include "execution/stages/monitoring_client.h"
#include "execution/stages/tests/mock_clients.h"
#include "execution/stages/write_to_monitoring.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/execution.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "proto/event/event.pb.h"

namespace delivery {
TEST(WriteToMonitoringTest, AllDataFields) {
  auto mock_client = std::make_unique<MockMonitoringClient>();
  MonitoringData data;
  EXPECT_CALL(*mock_client, write).WillOnce(testing::SaveArg<0>(&data));
  event::LogRequest log_req;
  auto& delivery_log = *log_req.add_delivery_log();
  // Arbitrarily add one request insertion and two execution insertions.
  delivery_log.mutable_request()->add_insertion();
  auto& execution = *delivery_log.mutable_execution();
  (*execution.add_execution_insertion()
        ->mutable_feature_stage()
        ->mutable_features()
        ->mutable_sparse())[100] = 101;
  (*execution.add_execution_insertion()
        ->mutable_feature_stage()
        ->mutable_features()
        ->mutable_sparse())[200] = 201;
  WriteToMonitoringStage stage(0, log_req, std::move(mock_client));
  stage.runSync();

  EXPECT_EQ(data.request_insertion_count, 1);
  EXPECT_EQ(data.feature_count, 2);
}
}  // namespace delivery
