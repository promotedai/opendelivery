// These tests do not test SimpleExecutor itself. These tests are primarily for
// making sure that different config setups don't cause a crash.

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "config/execution_config.h"
#include "config/platform_config.h"
#include "execution/context.h"
#include "execution/executor.h"
#include "execution/simple_executor.h"
#include "execution/stages/cache.h"
#include "execution/stages/compute_time_features.h"
#include "execution/stages/counters.h"
#include "execution/stages/feature_store_client.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/tests/mock_clients.h"
#include "execution/stages/write_to_delivery_log.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
class ConfigureSimpleExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    delivery::Request req;
    context_ = std::make_unique<Context>(std::move(req));
    raw_context_ = context_.get();
    // Don't want to use the default execution config, which is for manual test.
    context_->platform_config.execution_config = {};
  }

  // Normally, the Executor (and the Context containing it) are deallocated as
  // part of the async execution. But we don't want to actually do execution
  // here. Instead we manually destroy the Executor.
  void TearDown() override { auto _ = std::move(raw_context_->executor); }

  std::unique_ptr<Context> context_;
  Context* raw_context_;
  ConfigurationOptions options_;
};

TEST_F(ConfigureSimpleExecutorTest, NoStages) {
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, Init) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "Init";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromPaging) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromPaging";
  options_.paging_read_redis_client_getter = []() {
    return std::make_unique<MockRedisClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, InitFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "InitFeatures";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromItemFeatureStore) {
  context_->platform_config.feature_store_configs.emplace_back().type =
      item_feature_store_type;
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromItemFeatureStore";
  FeaturesCache cache(1);
  options_.content_features_cache_getter = [&]() -> FeaturesCache& {
    return cache;
  };
  options_.feature_store_client_getter = []() {
    return std::make_unique<MockFeatureStoreClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromItemFeatureStoreWithoutConfig) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromItemFeatureStore";
  FeaturesCache cache(1);
  options_.content_features_cache_getter = [&]() -> FeaturesCache& {
    return cache;
  };
  options_.feature_store_client_getter = []() {
    return std::make_unique<MockFeatureStoreClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromUserFeatureStore) {
  context_->platform_config.feature_store_configs.emplace_back().type =
      user_feature_store_type;
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromUserFeatureStore";
  FeaturesCache cache(1);
  options_.non_content_features_cache_getter = [&]() -> FeaturesCache& {
    return cache;
  };
  options_.feature_store_client_getter = []() {
    return std::make_unique<MockFeatureStoreClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromUserFeatureStoreWithoutConfig) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromUserFeatureStore";
  FeaturesCache cache(1);
  options_.non_content_features_cache_getter = [&]() -> FeaturesCache& {
    return cache;
  };
  options_.feature_store_client_getter = []() {
    return std::make_unique<MockFeatureStoreClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromCounters) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromCounters";
  options_.counters_redis_client_getter = []() {
    return std::make_unique<MockRedisClient>();
  };
  counters::Caches caches;
  options_.counters_caches_getter = [&]() -> counters::Caches& {
    return caches;
  };
  counters::DatabaseInfo database;
  options_.counters_database = &database;
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromCountersWithoutDatabase) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromCounters";
  options_.counters_redis_client_getter = []() {
    return std::make_unique<MockRedisClient>();
  };
  counters::Caches caches;
  options_.counters_caches_getter = [&]() -> counters::Caches& {
    return caches;
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ProcessCounters) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ProcessCounters";
  counters::DatabaseInfo database;
  options_.counters_database = &database;
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ProcessCountersWithoutDatabase) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ProcessCounters";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromPersonalize) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromPersonalize";
  options_.personalize_client_getter = []() {
    return std::make_unique<MockPersonalizeClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ReadFromRequest) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ReadFromRequest";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, Flatten) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "Flatten";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ExcludeUserFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ExcludeUserFeatures";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ComputeQueryFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ComputeQueryFeatures";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ComputeRatioFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ComputeRatioFeatures";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ComputeDistributionFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ComputeDistributionFeatures";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ComputeTimeFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ComputeTimeFeatures";
  PeriodicTimeValues periodic;
  options_.periodic_time_values = &periodic;
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, ComputeTimeFeaturesWithoutPeriodic) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "ComputeTimeFeatures";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, Respond) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "Respond";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, WriteToPaging) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "WriteToPaging";
  options_.paging_write_redis_client_getter = []() {
    return std::make_unique<MockRedisClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, WriteToDeliveryLog) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "WriteToDeliveryLog";
  options_.delivery_log_writer_getter = []() {
    return std::make_unique<MockDeliveryLogWriter>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, WriteOutStrangerFeatures) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "WriteOutStrangerFeatures";
  options_.sqs_client_getter = []() {
    return std::make_unique<MockSqsClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, WriteToMonitoring) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "WriteToMonitoring";
  options_.monitoring_client_getter = []() {
    return std::make_unique<MockMonitoringClient>();
  };
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}

TEST_F(ConfigureSimpleExecutorTest, Unrecognized) {
  auto& stage =
      context_->platform_config.execution_config.stages.emplace_back();
  stage.type = "garbo";
  auto& executor = configureSimpleExecutor(std::move(context_), options_);
  EXPECT_NE(executor, nullptr);
}
}  // namespace delivery
