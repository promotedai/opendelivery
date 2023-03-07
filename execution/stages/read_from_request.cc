#include "execution/stages/read_from_request.h"

#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "execution/context.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"
#include "utils/uuid.h"

namespace delivery {
void processUserAgent(const std::string& user_agent,
                      absl::flat_hash_map<uint64_t, float>& features,
                      absl::flat_hash_map<std::string, uint64_t>& strangers) {
  if (user_agent.empty()) {
    features[delivery_private_features::FEATURE_USER_AGENT_MISSING] = 1;
    return;
  }
  features[delivery_private_features::FEATURE_USER_AGENT_MISSING] = 0;

  hashlib::HashState state;
  state.updateState(user_agent_prefix);
  state.updateState(user_agent);
  uint64_t id = state.digestState();
  features[id] = 1;
  strangers[absl::StrCat(user_agent_prefix, user_agent)] = id;

  // This isn't unicode-safe.
  std::string lower_user_agent = absl::AsciiStrToLower(user_agent);
  float ios = 0, android = 0, web = 0, bot = 0, ios_app = 0, android_app = 0,
        ios_web = 0, android_web = 0, chrome_web = 0, linux_web = 0,
        mac_web = 0, windows_web = 0;

  // Order is important.
  if (absl::StartsWith(lower_user_agent, "okhttp")) {
    android = 1;
    android_app = 1;
  } else if (absl::StrContains(lower_user_agent, "android")) {
    android = 1;
    web = 1;
    android_web = 1;
  } else if (absl::StrContains(lower_user_agent, "darwin")) {
    ios = 1;
    ios_app = 1;
  } else if (absl::StrContains(lower_user_agent, "iphone") ||
             absl::StrContains(lower_user_agent, "ipad")) {
    ios = 1;
    web = 1;
    ios_web = 1;
  } else if (absl::StrContains(lower_user_agent, "macintosh")) {
    web = 1;
    mac_web = 1;
  } else if (absl::StrContains(lower_user_agent, "windows")) {
    windows_web = 1;
    web = 1;
  } else if (absl::StrContains(lower_user_agent, "cros")) {
    chrome_web = 1;
    web = 1;
  } else if (absl::StrContains(lower_user_agent, "x11")) {
    linux_web = 1;
    web = 1;
  } else if (absl::StrContains(lower_user_agent, "safari")) {
    web = 1;
  } else {
    bot = 1;
  }
  features[delivery_private_features::FEATURE_USER_AGENT_IS_ANDROID] = android;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_BOT] = bot;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_IOS] = ios;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_WEB] = web;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_IOS_APP] = ios_app;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_ANDROID_APP] =
      android_app;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_IOS_WEB] = ios_web;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_ANDROID_WEB] =
      android_web;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_CHROME_WEB] =
      chrome_web;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_LINUX_WEB] =
      linux_web;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_MAC_WEB] = mac_web;
  features[delivery_private_features::FEATURE_USER_AGENT_IS_WINDOWS_WEB] =
      windows_web;
}

void processPlacementFeatures(const delivery::Request& req,
                              absl::flat_hash_map<uint64_t, float>& features) {
  // For the time being we treat all insertions as though they are in the first
  // position.
  features[delivery_private_features::FEATURE_RESPONSE_INSERTION_POSITION] = 0;
  features[delivery_private_features::FEATURE_USE_CASE] = req.use_case();
  const auto& paging = req.paging();
  float offset;
  // We expect only one of the two fields to be populated.
  if (!paging.cursor().empty()) {
    offset = static_cast<float>(std::atoi(paging.cursor().c_str()));
  } else {
    offset = static_cast<float>(paging.offset());
  }
  features[delivery_private_features::FEATURE_RESPONSE_PAGING_OFFSET] = offset;
  features[delivery_private_features::FEATURE_RESPONSE_PAGING_SIZE] =
      static_cast<float>(paging.size());
  features[delivery_private_features::FEATURE_DEVICE_TYPE] =
      req.device().device_type();
}

void ReadFromRequestStage::runSync() {
  absl::flat_hash_map<uint64_t, float> request_features;
  absl::flat_hash_map<std::string, uint64_t> request_strangers;

  processUserAgent(req_.device().browser().user_agent(), request_features,
                   request_strangers);

  // These features are based on user info, but do not belong to user scope.
  const auto& log_user_id = req_.user_info().log_user_id();
  if (!log_user_id.empty()) {
    hashlib::HashState state;
    state.updateState(log_user_id_prefix);
    state.updateState(log_user_id);
    uint64_t id = state.digestState();
    request_features[id] = 1;
    request_strangers[absl::StrCat(log_user_id_prefix, log_user_id)] = id;
  }
  request_features[delivery_private_features::FEATURE_HAS_USER_ID] =
      !req_.user_info().user_id().empty();

  const auto& referrer = req_.device().browser().referrer();
  if (!referrer.empty()) {
    hashlib::HashState state;
    state.updateState(referrer_prefix);
    state.updateState(referrer);
    uint64_t id = state.digestState();
    request_features[id] = 1;
    request_strangers[absl::StrCat(referrer_prefix, referrer)] = id;
  }

  processPlacementFeatures(req_, request_features);

  feature_context_.addStrangerRequestFeatures(std::move(request_features),
                                              std::move(request_strangers));

  for (const auto& insertion : insertions_) {
    feature_context_.processInsertionFeatures(
        insertion.content_id(),
        [&insertion](FeatureScope& scope, const FeatureScope&,
                     const FeatureScope&) {
          uint64_t id = hashlib::makeHash(insertion.content_id());
          scope.features[id] = 1;
          scope.stranger_feature_paths[absl::StrCat(
              content_id_prefix, insertion.content_id())] = id;

          if (insertion.has_retrieval_score()) {
            scope.features[delivery_private_features::RETRIEVAL_SCORE] =
                insertion.retrieval_score();
          } else {
            scope.features[delivery_private_features::RETRIEVAL_SCORE_MISSING] =
                1;
          }
          if (insertion.has_retrieval_rank()) {
            scope.features[delivery_private_features::RETRIEVAL_RANK] =
                static_cast<float>(insertion.retrieval_rank());
          } else {
            scope.features[delivery_private_features::RETRIEVAL_RANK_MISSING] =
                1;
          }
        });
  }
}
}  // namespace delivery
