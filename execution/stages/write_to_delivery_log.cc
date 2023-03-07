#include "write_to_delivery_log.h"

#include <google/protobuf/stubs/port.h>
#include <stdint.h>

#include <algorithm>
#include <string_view>
#include <type_traits>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "config/platform_config.h"
#include "execution/context.h"
#include "execution/executor.h"
#include "execution/feature_context.h"
#include "execution/paging_context.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/INTERNAL_execution.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/execution.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "proto/event/event.pb.h"
#include "utils/time.h"

const std::string git_commit_hash = GIT_COMMIT_HASH;
// Truncate the commit hash to not waste space.
const std::string server_version = git_commit_hash.size() > 8
                                       ? git_commit_hash.substr(0, 8)
                                       : git_commit_hash;

namespace delivery {
delivery_private_features::Features makeExecutionFeatures(
    const FeatureScope& scope) {
  delivery_private_features::Features ret;

  auto& sparse = *ret.mutable_sparse();
  for (const auto& [k, v] : scope.features) {
    // Clear out zero-valued features that are unlikely to be well-known. This
    // is to save log space.
    if (k <= 600'000 || v != 0) {
      sparse[k] = v;
    }
  }
  auto& sparse_id = *ret.mutable_sparse_id();
  for (const auto& [k, v] : scope.int_features) {
    sparse_id[k] = v;
  }
  auto& sparse_id_list = *ret.mutable_sparse_id_list();
  for (const auto& [k, v] : scope.int_list_features) {
    *sparse_id_list[k].mutable_ids() = {v.begin(), v.end()};
  }

  return ret;
}

delivery::Insertion makeExecutionInsertion(
    const Context& context, const delivery::Insertion& insertion) {
  delivery::Insertion ret;

  ret.set_position(insertion.position());
  ret.set_insertion_id(insertion.insertion_id());
  ret.set_content_id(insertion.content_id());
  try {
    auto execution_features = makeExecutionFeatures(
        context.feature_context.getInsertionFeatures(insertion.content_id()));
    *ret.mutable_feature_stage()->mutable_features() =
        std::move(execution_features);
  } catch (const std::exception&) {
    // All insertions which are processed have their feature sets initialized.
    // In special cases here we can try to look up features for insertions which
    // were on the request but not processed (e.g. if we initially recognized
    // that an insertion was recently seen on another page). Nothing to do here
    // but leave the feature stage empty.
  }

  return ret;
}

// This creates a union of the following insertion sets:
// - The response insertions
// - In the case of shadow traffic, the page of insertions based on the ranks
// implied by the request
//
// The second set is important for downstream processing to be still possible
// even though the SDK doesn't do feature loading, etc.
void addExecutionInsertions(const Context& context,
                            delivery::DeliveryExecution* execution) {
  absl::flat_hash_set<std::string_view> seen_ids;

  for (const auto& insertion : context.resp.insertion()) {
    *execution->add_execution_insertion() =
        makeExecutionInsertion(context, insertion);
    seen_ids.emplace(insertion.content_id());
  }

  if (context.req().client_info().traffic_type() ==
      common::ClientInfo_TrafficType_SHADOW) {
    const auto& req_insertions = context.req().insertion();
    int64_t page_size = context.paging_context.max_position -
                        context.paging_context.min_position + 1;
    for (int64_t i = 0; i < page_size; ++i) {
      const auto& req_insertion = req_insertions[static_cast<int>(i)];
      // We don't want duplicate insertions. Insertions that we would have
      // responded with take priority.
      if (!seen_ids.contains(req_insertion.content_id())) {
        auto& execution_insertion = *execution->add_execution_insertion();
        execution_insertion = makeExecutionInsertion(context, req_insertion);
        execution_insertion.set_position(i +
                                         context.paging_context.min_position);
        seen_ids.emplace(req_insertion.content_id());
      }
    }
  }

  // For internal users, we log all request insertions for investigation
  // purposes.
  if (context.req().user_info().is_internal_user()) {
    for (const auto& req_insertion : context.req().insertion()) {
      if (!seen_ids.contains(req_insertion.content_id())) {
        *execution->add_execution_insertion() =
            makeExecutionInsertion(context, req_insertion);
        seen_ids.emplace(req_insertion.content_id());
      }
    }
  }
}

void WriteToDeliveryLogStage::runSync() {
  event::LogRequest& log_req = context_.log_req;
  log_req.set_platform_id(context_.platform_config.platform_id);
  *log_req.mutable_user_info() = context_.req().user_info();
  // Event API time set below.
  *log_req.mutable_timing() = context_.req().timing();
  *log_req.mutable_client_info() = context_.req().client_info();
  *log_req.mutable_device() = context_.req().device();
  // TODO(james): Flesh this out when experiments are ported.
  *log_req.add_cohort_membership() = event::CohortMembership();

  auto& delivery_log = *log_req.add_delivery_log();
  delivery_log.set_platform_id(log_req.platform_id());
  *delivery_log.mutable_request() = context_.req();
  *delivery_log.mutable_response() = context_.resp;
  auto* execution = delivery_log.mutable_execution();
  execution->set_execution_server(ExecutionServer::API);
  execution->set_server_version(server_version);
  *execution->mutable_user_feature_stage()->mutable_features() =
      makeExecutionFeatures(context_.feature_context.getUserFeatures());
  *execution->mutable_request_feature_stage()->mutable_features() =
      makeExecutionFeatures(context_.feature_context.getRequestFeatures());
  // TODO(vlad): Flesh this out when predictors are ported.
  *execution->mutable_predictor_stage() = delivery::PredictorStage();
  execution->mutable_after_response_stage()
      ->set_removed_execution_insertion_count(0);
  addExecutionInsertions(context_, execution);
  for (auto& node : context_.executor->nodes()) {
    // Skip stages with unspecified latencies for now.
    if (node.latency.method() !=
        delivery::DeliveryLatency_DeliveryMethod_UNKNOWN_DELIVERY_METHOD) {
      // Intentional copy since execution still ongoing.
      *execution->add_latency() = node.latency;
    }
  }

  // Set this at the last moment.
  log_req.mutable_timing()->set_event_api_timestamp(millisSinceEpoch());

  // We're trying to support multiple situations here while we migrate:
  // 1. Traffic being "echoed" from Golang - This is being done to test C++
  // under load. We want to echo all traffic, but don't want C++ to produce a
  // ton of useless logs.
  if (!context_.is_echo) {
    // 2. Traffic being "shadowed" from Golang - This is being done to compare
    // the quality of C++ results. This traffic could be already shadowed to
    // Golang, or legitimately served by Golang. In either event, we must
    // produce logs.
    // 3. Traffic being sent directly to C++ - We want to handle this
    // identically to the previous case.
    delivery_log_writer_->write(log_req);
  }
  // 4. Though we don't want to produce a ton of useless logs, james@ still
  // wants some logging on outliers to investigate performance. The start time
  // on the context isn't meant to measure durations, but this is just for rough
  // debugging anyway.
  if (context_.is_echo && millisSinceEpoch() - context_.start_time > 200) {
    log_req.mutable_client_info()->set_traffic_type(
        common::ClientInfo_TrafficType_INTERNAL);
    delivery_log_writer_->write(log_req);
  }
}
}  // namespace delivery
