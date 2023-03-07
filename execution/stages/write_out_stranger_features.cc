#include "write_out_stranger_features.h"

#include <json/value.h>
#include <json/writer.h>

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "execution/feature_context.h"
#include "proto/delivery/delivery.pb.h"
#include "sqs_client.h"

namespace delivery {
void WriteOutStrangerFeaturesStage::runSync() {
  // Assume the lowest bits of the request's starting time (in milliseconds) is
  // fair enough for sampling.
  uint64_t max_remainder = static_cast<uint64_t>(sample_rate_ * 100);
  if (max_remainder <= start_time_ % 100) {
    return;
  }

  // Build a simple JSON object from the stranger features recorded at all
  // scopes.
  Json::Value value;
  const auto& user_scope = feature_context_.getUserFeatures();
  for (const auto& [k, v] : user_scope.stranger_feature_paths) {
    value[k] = v;
  }
  const auto& request_scope = feature_context_.getRequestFeatures();
  for (const auto& [k, v] : request_scope.stranger_feature_paths) {
    value[k] = v;
  }
  for (const auto& insertion : insertions_) {
    const auto& insertion_scope =
        feature_context_.getInsertionFeatures(insertion.content_id());
    for (const auto& [k, v] : insertion_scope.stranger_feature_paths) {
      value[k] = v;
    }
  }

  // Unlikely, but empty JSON serializes to "null" which we don't want to write
  // out.
  if (value.empty()) {
    return;
  }

  Json::StreamWriterBuilder builder;
  // This causes no whitespace to be preoduced.
  builder["indentation"] = "";
  sqs_client_->sendMessage(Json::writeString(builder, value));
}
}  // namespace delivery
