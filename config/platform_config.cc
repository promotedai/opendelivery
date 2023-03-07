#include "config/platform_config.h"

#include <cstdint>
#include <string>
#include <vector>

#include "execution_config.h"

namespace delivery {
// This is a convenience default for manual testing of all currently
// implemented stages.
ExecutionConfig PlatformConfig::defaultExecutionConfig() {
  ExecutionConfig config;
  config.stages.reserve(50);  // Arbitrary number.
  // Init.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "Init";
    stage.id = 0;
    stage.input_ids = {};
  }
  // Read from paging.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ReadFromPaging";
    stage.id = 5;
    stage.input_ids = {0};
  }
  // Initialize features.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "InitFeatures";
    stage.id = 10;
    stage.input_ids = {5};
  }
  // Feature store.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ReadFromItemFeatureStore";
    stage.id = 11;
    stage.input_ids = {10};
  }
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ReadFromUserFeatureStore";
    stage.id = 12;
    stage.input_ids = {10};
  }
  // Counters.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ReadFromCounters";
    stage.id = 13;
    stage.input_ids = {10};
  }
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ProcessCounters";
    stage.id = 14;
    stage.input_ids = {13};
  }
  // Personalize.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ReadFromPersonalize";
    stage.id = 15;
    stage.input_ids = {10};
  }
  // Flatten.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "Flatten";
    stage.id = 16;
    stage.input_ids = {11};
  }
  // Exclude user features.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ExcludeUserFeatures";
    stage.id = 17;
    stage.input_ids = {12, 14};
  }
  // Compute derived features.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ComputeQueryFeatures";
    stage.id = 18;
    stage.input_ids = {11};
  }
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ReadFromRequest";
    stage.id = 19;
    stage.input_ids = {10};
  }
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ComputeDistributionFeatures";
    stage.id = 21;
    stage.input_ids = {15, 16, 17, 18, 19};
  }
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ComputeTimeFeatures";
    stage.id = 22;
    stage.input_ids = {21};
  }
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "ComputeRatioFeatures";
    stage.id = 23;
    stage.input_ids = {22};
  }
  // Respond.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "Respond";
    stage.id = 30;
    stage.input_ids = {23};
  }
  // Write to paging.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "WriteToPaging";
    stage.id = 35;
    stage.input_ids = {30};
  }
  // Write to delivery log.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "WriteToDeliveryLog";
    stage.id = 40;
    stage.input_ids = {30};
  }
  // Write out stranger features.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "WriteOutStrangerFeatures";
    stage.id = 45;
    stage.input_ids = {30};
  }
  // Write to monitoring.
  {
    auto& stage = config.stages.emplace_back();
    stage.type = "WriteToMonitoring";
    stage.id = 50;
    stage.input_ids = {40};
  }
  return config;
}
}  // namespace delivery
