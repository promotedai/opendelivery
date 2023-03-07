#include <google/protobuf/stubs/port.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/context.h"
#include "execution/executor.h"
#include "execution/feature_context.h"
#include "execution/paging_context.h"
#include "execution/stages/tests/mock_clients.h"
#include "execution/stages/write_to_delivery_log.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/INTERNAL_execution.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/execution.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "proto/event/event.pb.h"

namespace delivery {
class MockExecutor : public Executor {
 public:
  MOCK_METHOD(void, execute, (), (override));
  MOCK_METHOD(const std::vector<ExecutorNode>&, nodes, (), (const override));
};

TEST(WriteToDeliveryLogTest, ConstructAndWrite) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  Context context({});
  context.executor = std::move(executor);
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  ASSERT_EQ(log_req.delivery_log_size(), 1);
  EXPECT_FALSE(log_req.delivery_log(0).execution().server_version().empty());
}

TEST(WriteToDeliveryLogTest, DontWriteEcho) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  EXPECT_CALL(*mock_writer, write).Times(0);
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  Context context({});
  context.executor = std::move(executor);
  context.is_echo = true;
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();
}

TEST(WriteToDeliveryLogTest, Latency) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  ExecutorNode node;
  node.latency.set_method(
      delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__CALC_EMBEDDINGS);
  node.latency.set_start_millis(100);
  std::vector<ExecutorNode> nodes;
  nodes.emplace_back(std::move(node));
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  Context context({});
  context.executor = std::move(executor);
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  ASSERT_EQ(log_req.delivery_log_size(), 1);
  ASSERT_EQ(log_req.delivery_log(0).execution().latency_size(), 1);
  EXPECT_EQ(log_req.delivery_log(0).execution().latency(0).start_millis(), 100);
}

TEST(WriteToDeliveryLogTest, LogRequestFields) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  delivery::Request req;
  req.mutable_user_info()->set_user_id("a");
  req.mutable_timing()->set_client_log_timestamp(100);
  req.mutable_client_info()->set_client_type(
      common::ClientInfo_ClientType_PROMOTED_REPLAYER);
  req.mutable_device()->set_ip_address("b");
  Context context(req);
  context.executor = std::move(executor);
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  EXPECT_EQ(log_req.user_info().user_id(), "a");
  EXPECT_EQ(log_req.timing().client_log_timestamp(), 100);
  EXPECT_EQ(log_req.client_info().client_type(),
            common::ClientInfo_ClientType_PROMOTED_REPLAYER);
  EXPECT_EQ(log_req.device().ip_address(), "b");
}

TEST(WriteToDeliveryLogTest, TopLevelFeatures) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  Context context({});
  context.executor = std::move(executor);
  context.feature_context.addRequestFeatures({{100, 101}});
  delivery_private_features::Features features;
  // This should be skipped.
  (*features.mutable_sparse())[102'000'000] = 0;
  (*features.mutable_sparse_id())[102] = 103;
  std::vector<int64_t> seq = {105, 106};
  *(*features.mutable_sparse_id_list())[104].mutable_ids() = {seq.begin(),
                                                              seq.end()};
  context.feature_context.addUserFeatures(features);
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  ASSERT_EQ(log_req.delivery_log_size(), 1);
  const auto& req_features =
      log_req.delivery_log(0).execution().request_feature_stage().features();
  EXPECT_EQ(req_features.sparse_size(), 1);
  EXPECT_EQ(req_features.sparse().at(100), 101);
  const auto& user_features =
      log_req.delivery_log(0).execution().user_feature_stage().features();
  EXPECT_EQ(user_features.sparse_size(), 0);
  EXPECT_EQ(user_features.sparse_id_size(), 1);
  EXPECT_EQ(user_features.sparse_id().at(102), 103);
  EXPECT_EQ(user_features.sparse_id_list_size(), 1);
  EXPECT_THAT(user_features.sparse_id_list().at(104).ids(),
              testing::ElementsAre(105, 106));
}

TEST(WriteToDeliveryLogTest, ExecutionInsertions) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  Context context({});
  context.executor = std::move(executor);
  auto& insertion = *context.resp.add_insertion();
  insertion.set_position(2);
  insertion.set_content_id("a");
  insertion.set_insertion_id("ai");
  context.feature_context.initialize({insertion});
  context.feature_context.addInsertionFeatures("a", {{100, 101}});
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  ASSERT_EQ(log_req.delivery_log_size(), 1);
  ASSERT_EQ(log_req.delivery_log(0).execution().execution_insertion_size(), 1);
  const auto& execution_insertion =
      log_req.delivery_log(0).execution().execution_insertion(0);
  EXPECT_EQ(execution_insertion.position(), 2);
  EXPECT_EQ(execution_insertion.content_id(), "a");
  EXPECT_EQ(execution_insertion.insertion_id(), "ai");
  EXPECT_EQ(execution_insertion.feature_stage().features().sparse_size(), 1);
  EXPECT_EQ(execution_insertion.feature_stage().features().sparse().at(100),
            101);
}

TEST(WriteToDeliveryLogTest, ExecutionInsertionsShadow) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  delivery::Request req;
  req.mutable_client_info()->set_traffic_type(
      common::ClientInfo_TrafficType_SHADOW);
  req.add_insertion()->set_content_id("a");
  req.add_insertion()->set_content_id("b");
  req.add_insertion()->set_content_id("c");
  Context context(req);
  context.executor = std::move(executor);
  context.paging_context.min_position = 0;
  context.paging_context.max_position = 2;
  auto& insertion = *context.resp.add_insertion();
  insertion.set_position(2);
  insertion.set_content_id("a");
  context.feature_context.initialize({insertion, req.insertion(1)});
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  ASSERT_EQ(log_req.delivery_log_size(), 1);
  ASSERT_EQ(log_req.delivery_log(0).execution().execution_insertion_size(), 3);
  const auto& execution = log_req.delivery_log(0).execution();
  EXPECT_EQ(execution.execution_insertion(0).position(), 2);
  EXPECT_EQ(execution.execution_insertion(0).content_id(), "a");
  // In the shadow set, insertion "a" gets position 0 and insertion "b" gets
  // position 1, but "a" is skipped due to redundancy.
  EXPECT_EQ(execution.execution_insertion(1).position(), 1);
  EXPECT_EQ(execution.execution_insertion(1).content_id(), "b");
  // We don't care about the third insertion "c", except that this test doesn't
  // throw an error.
}

TEST(WriteToDeliveryLogTest, ExecutionInsertionsInternal) {
  auto mock_writer = std::make_unique<MockDeliveryLogWriter>();
  event::LogRequest log_req;
  EXPECT_CALL(*mock_writer, write).WillOnce(testing::SaveArg<0>(&log_req));
  auto executor = std::make_unique<MockExecutor>();
  std::vector<ExecutorNode> nodes;
  EXPECT_CALL(*executor, nodes).WillOnce(testing::ReturnRef(nodes));
  delivery::Request req;
  req.mutable_user_info()->set_is_internal_user(true);
  req.add_insertion()->set_content_id("a");
  req.add_insertion()->set_content_id("b");
  req.add_insertion()->set_content_id("c");
  Context context(req);
  context.executor = std::move(executor);
  auto& insertion = *context.resp.add_insertion();
  insertion.set_position(2);
  insertion.set_content_id("a");
  context.feature_context.initialize({insertion, req.insertion(1)});
  WriteToDeliveryLogStage stage(0, context, std::move(mock_writer));
  stage.runSync();

  ASSERT_EQ(log_req.delivery_log_size(), 1);
  ASSERT_EQ(log_req.delivery_log(0).execution().execution_insertion_size(), 3);
  const auto& execution = log_req.delivery_log(0).execution();
  EXPECT_EQ(execution.execution_insertion(0).position(), 2);
  EXPECT_EQ(execution.execution_insertion(0).content_id(), "a");
  // Insertion "a" is skipped due to redundancy. No position is assigned to
  // insertion "b".
  EXPECT_EQ(execution.execution_insertion(1).position(), 0);
  EXPECT_EQ(execution.execution_insertion(1).content_id(), "b");
}
}  // namespace delivery
