// A Context instance is specific to a particular request. It does not belong to
// a particular thread or event loop.

#pragma once

#include <chrono>
#include <deque>

#include "config/platform_config.h"
#include "execution/counters_context.h"
#include "execution/executor.h"
#include "execution/feature_context.h"
#include "execution/paging_context.h"
#include "execution/user_agent.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "proto/event/event.pb.h"

namespace delivery {
class Context {
 public:
  explicit Context(delivery::Request req) : req_(std::move(req)) {}

  // This is only available as a const& because we want it to remain unmodified
  // for logging.
  const delivery::Request& req() const { return req_; }

  // This should not be used to measure durations.
  uint64_t start_time = millisSinceEpoch();

  // User agent should be populated outside of any stages.
  UserAgent user_agent;

  // These are mutable and can be modified freely by processing (assuming no
  // stages are racing). May include insertions not specified on the request
  // (e.g. due to paging).
  std::vector<delivery::Insertion> execution_insertions;

  // This is the callback for responding to the user. This must be populated and
  // passed into `RespondStage`.
  std::function<void(const delivery::Response&)> respond_cb;
  // Saved here to be logged after responding to the client.
  delivery::Response resp;

  // The top-level config for this request.
  PlatformConfig platform_config;

  // Information about previous insertion allocations to respect and new ones to
  // store.
  PagingContext paging_context;
  // Mainly for passing state between multiple counters-related stages.
  CountersContext counters_context;

  // Scores from Personalize and ranks deduced from them. Outer keys are
  // campaign names. Inner keys are content IDs.
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<std::string, std::pair<float, int>>>
      personalize_campaign_to_scores_and_ranks;

  // This is where features are stashed by any stage which produces them.
  FeatureContext feature_context;

  // This is the request which is used to write to the delivery log.
  event::LogRequest log_req;

  // This drives all execution for this context once the /deliver controller
  // returns.
  std::unique_ptr<Executor> executor;

  // This is a hack while we migrate to prevent the writing of delivery logs.
  bool is_echo = false;

 private:
  // `InitStage` is a friend to do any modifications we actually do want to make
  // to the request (e.g. assigning our own ID).
  delivery::Request req_;
  friend class InitStage;
};
}  // namespace delivery
