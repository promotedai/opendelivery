// Home to personalize-specific config options.

#pragma once

#include <string>

#include "config/json.h"

namespace delivery {
struct PersonalizeConfig {
  std::string campaign_name;
  std::string campaign_arn;
  // 500 is the most rankings Personalize will respond with.
  uint64_t max_input_size = 500;
  std::string timeout;
  // We default to not calling Personalize for shadow traffic. This can be
  // enabled during initial training.
  bool enable_for_shadow_traffic = false;

  constexpr static auto properties = std::make_tuple(
      property(&PersonalizeConfig::campaign_name, "campaignName"),
      property(&PersonalizeConfig::campaign_arn, "campaignArn"),
      property(&PersonalizeConfig::max_input_size, "maxInputSize"),
      property(&PersonalizeConfig::timeout, "timeout"),
      property(&PersonalizeConfig::enable_for_shadow_traffic,
               "enableForShadowTraffic"));
};
}  // namespace delivery
