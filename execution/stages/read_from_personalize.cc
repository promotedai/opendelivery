#include "execution/stages/read_from_personalize.h"

#include <atomic>
#include <ext/alloc_traits.h>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "config/personalize_config.h"
#include "personalize_client.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
const std::string default_user = "default_user";

void convertResults(
    const std::vector<PersonalizeConfig>& configs,
    std::vector<std::vector<PersonalizeResult>>& all_results,
    absl::flat_hash_map<
        std::string, absl::flat_hash_map<std::string, std::pair<float, int>>>&
        campaign_to_scores_and_ranks) {
  campaign_to_scores_and_ranks.reserve(configs.size());
  for (size_t i = 0; i < configs.size(); ++i) {
    absl::flat_hash_map<std::string, std::pair<float, int>> scores_and_ranks;
    auto& results = all_results[i];
    scores_and_ranks.reserve(results.size());
    for (int j = 0; j < results.size(); ++j) {
      scores_and_ranks[std::move(results[j].id)] = {results[j].score, j + 1};
    }

    campaign_to_scores_and_ranks[configs[i].campaign_name] =
        std::move(scores_and_ranks);
  }
}

void ReadFromPersonalizeStage::runSync() {
  convertResults(enabled_configs_, results_, campaign_to_scores_and_ranks_);
  cb_();
}

// Just used to avoid races between the client async calls and them timing out.
struct CoordinationState {
  std::mutex mutex;
  bool timed_out = false;
  size_t remaining_reads = 0;
};

void ReadFromPersonalizeStage::run(
    std::function<void()>&& cb,
    std::function<void(const std::chrono::duration<double>& delay,
                       std::function<void()>&& cb)>&& timeout_cb) {
  // If there are no insertions then there's nothing for Personalize to rank.
  // Personalize technically considers an empty list an erroneous input, so this
  // also keeps our logs cleaner.
  if (insertions_.empty()) {
    cb();
    return;
  }

  // For shadow traffic, we generally don't want to waste time (or money)
  // calling Personalize.
  if (req_.client_info().traffic_type() ==
      common::ClientInfo_TrafficType_SHADOW) {
    enabled_configs_.reserve(configs_.size());
    for (const auto& config : configs_) {
      if (config.enable_for_shadow_traffic) {
        enabled_configs_.emplace_back(config);
      }
    }
  } else {
    enabled_configs_ = configs_;
  }

  // No configs means nothing to do.
  if (enabled_configs_.empty()) {
    cb();
    return;
  }

  cb_ = cb;

  std::vector<std::string> ids;
  // Our configs currently allow for per-config size specification, but the
  // types required by the AWS SDK make this either expensive or painful. In
  // practice we always use the same size, so just use the one from the first
  // config for all configs.
  ids.reserve(enabled_configs_[0].max_input_size);
  for (size_t i = 0;
       i < insertions_.size() && i < enabled_configs_[0].max_input_size; ++i) {
    ids.emplace_back(insertions_[i].content_id());
  }

  // Pre-size to access by index in case some time out.
  results_.resize(enabled_configs_.size());
  auto state = std::make_shared<CoordinationState>();
  // + 1 to prevent finishing before this function is done.
  state->remaining_reads = enabled_configs_.size() + 1;

  for (size_t i = 0; i < enabled_configs_.size(); ++i) {
    const std::string* user_id = &default_user;
    if (!req_.user_info().log_user_id().empty() &&
        absl::StrContains(enabled_configs_[i].campaign_name, "loguserid")) {
      user_id = &req_.user_info().log_user_id();
    } else if (!req_.user_info().user_id().empty()) {
      user_id = &req_.user_info().user_id();
    }

    client_->getPersonalizedRanking(
        enabled_configs_[i].campaign_arn, user_agent_, ids, *user_id,
        [this, i, state](std::vector<PersonalizeResult> results) {
          std::lock_guard<std::mutex> lock(state->mutex);
          if (state->timed_out) {
            return;
          }
          results_[i] = std::move(results);
          if (--state->remaining_reads == 0) {
            runSync();
          }
        });
  }
  // Here we use the same timeout regardless of how many client calls we have.
  // We just use the timeout from the first config for all configs.
  int timeout;
  try {
    timeout = std::stoi(enabled_configs_[0].timeout);
  } catch (const std::exception&) {
    errors_.emplace_back(absl::StrCat(
        "Invalid Personalize timeout specified: ", enabled_configs_[0].timeout,
        ". Defaulting to 100ms."));
    timeout = 100;
  }
  timeout_cb(std::chrono::milliseconds(timeout), [this, state]() {
    std::lock_guard<std::mutex> lock(state->mutex);
    // Check if we didn't time out.
    if (state->remaining_reads == 0) {
      return;
    }
    state->timed_out = true;
    runSync();
  });

  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->timed_out) {
    return;
  }
  if (--state->remaining_reads == 0) {
    runSync();
  }
}
}  // namespace delivery
