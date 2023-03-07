#include "execution/simple_executor.h"

#include <atomic>
#include <chrono>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "config/execution_config.h"
#include "config/feature_config.h"
#include "config/platform_config.h"
#include "context.h"
#include "execution/stages/compute_distribution_features.h"
#include "execution/stages/compute_query_features.h"
#include "execution/stages/compute_ratio_features.h"
#include "execution/stages/compute_time_features.h"
#include "execution/stages/counters.h"
#include "execution/stages/exclude_user_features.h"
#include "execution/stages/feature_store_client.h"
#include "execution/stages/flatten.h"
#include "execution/stages/init.h"
#include "execution/stages/init_features.h"
#include "execution/stages/paging.h"
#include "execution/stages/personalize_client.h"
#include "execution/stages/read_from_feature_store.h"
#include "execution/stages/read_from_personalize.h"
#include "execution/stages/read_from_request.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/respond.h"
#include "execution/stages/sqs_client.h"
#include "execution/stages/stage.h"
#include "execution/stages/write_out_stranger_features.h"
#include "execution/stages/write_to_delivery_log.h"
#include "execution/stages/write_to_monitoring.h"
#include "executor.h"
#include "feature_context.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/INTERNAL_execution.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "trantor/net/EventLoop.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"
#include "utils/time.h"

namespace delivery {
void startLatency(ExecutorNode& node) {
  node.latency.set_start_millis(millisSinceEpoch());
  node.duration_start = millisForDuration();
}

// This assumes startLatency() was already called.
void finishLatency(ExecutorNode& node) {
  node.latency.set_duration_millis(millisForDuration() - node.duration_start);
}

void SimpleExecutor::execute() {
  // If stages make async calls, responses can be handled by threads without
  // loops we know of (e.g. in the AWS SDK). This means we have to stash the
  // loop we'll use now for queueing successive stages instead of always getting
  // it on the fly.
  loop_ = trantor::EventLoop::getEventLoopOfCurrentThread();
  for (auto& curr_node : nodes_) {
    // Immediately queue all stages which aren't waiting on other stages.
    if (curr_node.stage != nullptr && *curr_node.remaining_inputs == 0) {
      loop_->queueInLoop([this, &curr_node] {
        startLatency(curr_node);
        curr_node.stage->run(
            /*done_cb=*/[this, &curr_node] { this->afterRun(curr_node); },
            /*timeout_cb=*/
            [this](const std::chrono::duration<double>& delay,
                   std::function<void()>&& cb) {
              this->scheduleTimeout(delay, std::move(cb));
            });
      });
    }
  }
}

void SimpleExecutor::afterRun(ExecutorNode& curr_node) {
  // By construction, this should be the only final stage. Queue cleanup.
  if (curr_node.output_ids.empty()) {
    loop_->queueInLoop(std::move(clean_up_cb_));
    return;
  }
  for (const auto& error : curr_node.stage->errors()) {
    LOG_ERROR << error;
  }
  // Stages that define their own async behavior will still be attributed with
  // that duration even if the event loop was actually free to do other work.
  finishLatency(curr_node);
  // Note that nothing happens for terminal nodes.
  for (size_t output_id : curr_node.output_ids) {
    auto& next_node = nodes_.at(output_id);
    // If this is the last stage being waited on by another, queue that stage
    // now.
    if (--(*next_node.remaining_inputs) == 0) {
      loop_->queueInLoop([this, &next_node] {
        startLatency(next_node);
        next_node.stage->run(
            /*done_cb=*/[this, &next_node] { this->afterRun(next_node); },
            /*timeout_cb=*/
            [this](const std::chrono::duration<double>& delay,
                   std::function<void()>&& cb) {
              this->scheduleTimeout(delay, std::move(cb));
            });
      });
    }
  }
}

void SimpleExecutor::scheduleTimeout(const std::chrono::duration<double>& delay,
                                     std::function<void()>&& cb) {
  // Presumably this can be additionally delayed if the loop is busy at that
  // point in time. Shouldn't be an issue because the stage that scheduled the
  // timeout can't resume if the loop is busy anyway.
  loop_->runAfter(delay, std::move(cb));
}

std::string SimpleExecutor::dotString() const {
  std::vector<std::string> lines;
  lines.reserve(nodes_.size() + 2);  // Approximation for linear graphs.
  lines.emplace_back("digraph {");
  for (const auto& node : nodes_) {
    if (node.stage != nullptr) {
      const auto& in = *node.stage;
      // "Declare" each node in case it has no connected nodes.
      lines.emplace_back(absl::StrCat("\"", in.name(), " (", in.id(), ")\""));
      for (size_t output_id : node.output_ids) {
        const auto& out = *nodes_.at(output_id).stage;
        lines.emplace_back(absl::StrCat("\"", in.name(), " (", in.id(),
                                        ")\" -> \"", out.name(), " (", out.id(),
                                        ")\""));
      }
    }
  }
  lines.emplace_back("}");
  return absl::StrJoin(lines, "\n");
}

void SimpleExecutorBuilder::addStage(
    std::unique_ptr<Stage> stage, const std::vector<size_t>& input_ids,
    delivery::DeliveryLatency_DeliveryMethod latency_tag) {
  size_t stage_id = stage->id();
  if (nodes_.size() <= stage_id) {
    nodes_.resize(stage_id + 1);
  }
  *nodes_[stage_id].remaining_inputs = input_ids.size();
  nodes_[stage_id].stage = std::move(stage);
  // Update output indexes for the inputs rather than for this stage.
  for (size_t input_id : input_ids) {
    if (nodes_.size() <= input_id) {
      nodes_.resize(input_id + 1);
    }
    nodes_[input_id].output_ids.push_back(stage_id);
  }
  nodes_[stage_id].latency.set_method(latency_tag);
}

class NoOpStage : public Stage {
 public:
  explicit NoOpStage(size_t id) : Stage(id) {}

  std::string name() const override { return "NoOp"; }

  // Do nothing.
  void runSync() override {}
};

std::unique_ptr<SimpleExecutor> SimpleExecutorBuilder::build(
    std::function<void()>&& clean_up_cb) {
  std::vector<size_t> final_ids;
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (nodes_[i].stage != nullptr && nodes_[i].output_ids.empty()) {
      final_ids.push_back(i);
    }
  }
  // Always ensure a single, final stage for clear deallocation
  // responsibility.
  if (final_ids.size() != 1) {
    addStage(std::make_unique<NoOpStage>(nodes_.size()), final_ids);
  }
  return std::make_unique<SimpleExecutor>(std::move(clean_up_cb),
                                          std::move(nodes_));
}

std::unique_ptr<SimpleExecutor> SimpleExecutorBuilder::build(
    std::unique_ptr<Context> context) {
  // Moving a unique_ptr into the lambda should be safe, but std::function isn't
  // happy about it. Instead, just degrade it into a shared_ptr and move that
  // instead.
  std::shared_ptr<Context> shared_context = std::move(context);
  return build([shared_context]() {
    // Implicit deletion of shared_context.
  });
}

// If a stage cannot be built, it is replaced by a stage which does no
// processing. The topology of the graph remains the same.
//
// Eventual improvements:
// - Move string types to a (Protobuf-based?) enum
// - Come up with better default behavior (e.g. InitStage and then RespondStage)
std::unique_ptr<Executor>& configureSimpleExecutor(
    std::unique_ptr<Context> context, const ConfigurationOptions& options) {
  // Construction should be cheap, but if it gets expensive we can cache them
  // and add a virtual clone() function.
  SimpleExecutorBuilder builder;

  for (const auto& stage : context->platform_config.execution_config.stages) {
    if (stage.type == "Init") {
      builder.addStage(std::make_unique<InitStage>(stage.id, *context),
                       stage.input_ids);
    } else if (stage.type == "ReadFromPaging") {
      builder.addStage(
          std::make_unique<ReadFromPagingStage>(
              stage.id, options.paging_read_redis_client_getter(),
              context->platform_config.paging_config, context->req(),
              context->execution_insertions, context->paging_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_PAGING__GET_ALLOCATED);
    } else if (stage.type == "InitFeatures") {
      builder.addStage(std::make_unique<InitFeaturesStage>(
                           stage.id, context->execution_insertions,
                           context->feature_context),
                       stage.input_ids);
    } else if (stage.type == "ReadFromItemFeatureStore") {
      const auto& feature_store_configs =
          context->platform_config.feature_store_configs;
      int config_idx = -1;
      for (int i = 0; i < feature_store_configs.size(); ++i) {
        if (feature_store_configs[i].type == item_feature_store_type) {
          config_idx = i;
          break;
        }
      }
      if (config_idx == -1) {
        LOG_ERROR << "Trying to build a ReadFromItemFeatureStore stage with no "
                     "appropriately typed config";
        builder.addStage(std::make_unique<NoOpStage>(stage.id),
                         stage.input_ids);
        continue;
      }
      auto key_generator = [&execution_insertions =
                                context->execution_insertions]() {
        std::vector<std::string> keys;
        keys.reserve(execution_insertions.size());
        for (const auto& insertion : execution_insertions) {
          keys.emplace_back(insertion.content_id());
        }
        return keys;
      };
      auto feature_adder = [&feature_context = context->feature_context](
                               std::string_view insertion_id,
                               delivery_private_features::Features features) {
        feature_context.addInsertionFeatures(insertion_id, std::move(features));
      };
      builder.addStage(
          std::make_unique<ReadFromFeatureStoreStage>(
              stage.id, options.content_features_cache_getter(),
              options.feature_store_client_getter(),
              context->platform_config.feature_store_configs[config_idx],
              context->platform_config.feature_store_timeout,
              context->start_time, std::move(key_generator),
              std::move(feature_adder)),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__GET_FEATURES);
    } else if (stage.type == "ReadFromUserFeatureStore") {
      const auto& feature_store_configs =
          context->platform_config.feature_store_configs;
      int config_idx = -1;
      for (int i = 0; i < feature_store_configs.size(); ++i) {
        if (feature_store_configs[i].type == user_feature_store_type) {
          config_idx = i;
          break;
        }
      }
      if (config_idx == -1) {
        LOG_ERROR << "Trying to build a ReadFromUserFeatureStore stage with no "
                     "appropriately typed config";
        builder.addStage(std::make_unique<NoOpStage>(stage.id),
                         stage.input_ids);
        continue;
      }
      auto key_generator =
          [&user_info =
               context->req().user_info()]() -> std::vector<std::string> {
        if (user_info.user_id().empty()) {
          return {};
        }
        return {user_info.user_id()};
      };
      auto feature_adder = [&feature_context = context->feature_context](
                               std::string_view _,
                               delivery_private_features::Features features) {
        feature_context.addUserFeatures(std::move(features));
      };
      builder.addStage(
          std::make_unique<ReadFromFeatureStoreStage>(
              stage.id, options.non_content_features_cache_getter(),
              options.feature_store_client_getter(),
              context->platform_config.feature_store_configs[config_idx],
              context->platform_config.feature_store_timeout,
              context->start_time, std::move(key_generator),
              std::move(feature_adder)),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__GET_FEATURES);
    } else if (stage.type == "ReadFromCounters") {
      if (options.counters_database == nullptr) {
        LOG_ERROR << "Trying to build a ReadFromCounters stage with no "
                     "counters database";
        builder.addStage(std::make_unique<NoOpStage>(stage.id),
                         stage.input_ids);
        continue;
      }
      builder.addStage(
          std::make_unique<counters::ReadFromCountersStage>(
              stage.id, options.counters_redis_client_getter(),
              options.counters_caches_getter(), *options.counters_database,
              context->platform_config.platform_id, context->req(),
              context->execution_insertions, context->start_time,
              context->user_agent, context->counters_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__GET_COUNTS);
    } else if (stage.type == "ProcessCounters") {
      if (options.counters_database == nullptr) {
        LOG_ERROR << "Trying to build a ProcessCounters stage with no "
                     "counters database";
        builder.addStage(std::make_unique<NoOpStage>(stage.id),
                         stage.input_ids);
        continue;
      }
      builder.addStage(
          std::make_unique<counters::ProcessCountersStage>(
              stage.id, *options.counters_database,
              context->execution_insertions, context->feature_context,
              context->counters_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__GET_COUNTS);
    } else if (stage.type == "ReadFromPersonalize") {
      builder.addStage(
          std::make_unique<ReadFromPersonalizeStage>(
              stage.id, options.personalize_client_getter(),
              context->platform_config.personalize_configs, context->req(),
              context->execution_insertions, context->user_agent,
              context->personalize_campaign_to_scores_and_ranks),
          stage.input_ids,
          delivery::
              DeliveryLatency_DeliveryMethod_AGGREGATOR__GET_PERSONALIZE_SCORES);
    } else if (stage.type == "ReadFromRequest") {
      builder.addStage(
          std::make_unique<ReadFromRequestStage>(stage.id, context->req(),
                                                 context->execution_insertions,
                                                 context->feature_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__MERGE_FEATURES);
    } else if (stage.type == "Flatten") {
      builder.addStage(
          std::make_unique<FlattenStage>(
              stage.id, context->req(), context->execution_insertions,
              context->platform_config.sparse_features_config
                  .max_request_properties,
              context->platform_config.sparse_features_config
                  .max_insertion_properties,
              context->feature_context),
          stage.input_ids,
          // This isn't really an accurate tag, but we definitely want to see
          // how much time is spent here.
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__GET_FEATURES);
    } else if (stage.type == "ExcludeUserFeatures") {
      builder.addStage(
          std::make_unique<ExcludeUserFeaturesStage>(
              stage.id, context->req().user_info().ignore_usage(),
              context->platform_config.exclude_user_features_config,
              context->feature_context, context->execution_insertions),
          stage.input_ids);
    } else if (stage.type == "ComputeDistributionFeatures") {
      builder.addStage(
          std::make_unique<ComputeDistributionFeaturesStage>(
              stage.id,
              context->platform_config.sparse_features_config
                  .distribution_feature_paths,
              context->execution_insertions, context->feature_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__MERGE_FEATURES);
    } else if (stage.type == "ComputeTimeFeatures") {
      if (options.periodic_time_values == nullptr) {
        LOG_ERROR << "Periodic time values missing";
        builder.addStage(std::make_unique<NoOpStage>(stage.id),
                         stage.input_ids);
        continue;
      }
      builder.addStage(
          std::make_unique<ComputeTimeFeaturesStage>(
              stage.id, *options.periodic_time_values,
              context->platform_config.time_features_config,
              context->execution_insertions, context->start_time,
              context->platform_config.region, context->feature_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__MERGE_FEATURES);
    } else if (stage.type == "ComputeQueryFeatures") {
      builder.addStage(
          std::make_unique<ComputeQueryFeaturesStage>(
              stage.id, context->req().search_query(),
              context->execution_insertions, context->feature_context),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__MERGE_FEATURES);
    } else if (stage.type == "ComputeRatioFeatures") {
      builder.addStage(
          std::make_unique<ComputeRatioFeaturesStage>(
              stage.id, context->feature_context,
              context->execution_insertions),
          stage.input_ids,
          delivery::DeliveryLatency_DeliveryMethod_AGGREGATOR__MERGE_FEATURES);
    } else if (stage.type == "Respond") {
      builder.addStage(std::make_unique<RespondStage>(
                           stage.id, context->req(), context->paging_context,
                           context->execution_insertions, context->resp,
                           std::move(context->respond_cb)),
                       stage.input_ids);
    } else if (stage.type == "WriteToPaging") {
      builder.addStage(std::make_unique<WriteToPagingStage>(
                           stage.id, options.paging_write_redis_client_getter(),
                           context->platform_config.paging_config,
                           context->resp, context->paging_context),
                       stage.input_ids);
    } else if (stage.type == "WriteToDeliveryLog") {
      // Make an exception and give this stage visibility of the entire context
      // because it needs most of the information.
      builder.addStage(
          std::make_unique<WriteToDeliveryLogStage>(
              stage.id, *context, options.delivery_log_writer_getter()),
          stage.input_ids);
    } else if (stage.type == "WriteOutStrangerFeatures") {
      builder.addStage(
          std::make_unique<WriteOutStrangerFeaturesStage>(
              stage.id,
              context->platform_config.sparse_features_config
                  .stranger_feature_sampling_rate,
              context->start_time, context->feature_context,
              context->execution_insertions, options.sqs_client_getter()),
          stage.input_ids);
    } else if (stage.type == "WriteToMonitoring") {
      builder.addStage(
          std::make_unique<WriteToMonitoringStage>(
              stage.id, context->log_req, options.monitoring_client_getter()),
          stage.input_ids);
    } else {
      LOG_ERROR << "Unrecognized stage type: " << stage.type;
      builder.addStage(std::make_unique<NoOpStage>(stage.id), stage.input_ids);
    }
  }

  // It was a goal to not give each stage shared ownership of the context for
  // clearer APIs. The context contains everything needed for processing the
  // request, including the executor, but the executor defines the lifetime of
  // the processing, and thus must be the owner of the context.
  auto& raw_context = *context;
  std::unique_ptr<SimpleExecutor> simple_executor =
      builder.build(std::move(context));
  raw_context.executor = std::move(simple_executor);
  return raw_context.executor;
}
}  // namespace delivery
