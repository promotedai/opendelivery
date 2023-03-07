#include "execution/stages/exclude_user_features.h"

#include <cstdint>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "config/feature_config.h"
#include "execution/feature_context.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
void ExcludeUserFeaturesStage::runSync() {
  // There are currently two ways to indicate that user features should be
  // ignored:
  // 1. The request indicates that usage should be ignored
  // 2. A feature we read from user feature store indicates it
  bool exclude = ignore_usage_;

  feature_context_.processUserFeatures([this, &exclude](FeatureScope& scope) {
    if (!exclude && config_.has_value()) {
      auto it = scope.features.find(hashlib::makeHash(config_->user_property));
      if (it != scope.features.end() && it->second > 0) {
        exclude = true;
      }
    }
    if (!exclude) {
      return;
    }

    // Everything gets cleared at user-scope.
    scope.features.clear();
    scope.int_features.clear();
    scope.int_list_features.clear();
    scope.stranger_feature_paths.clear();
  });
  if (!exclude) {
    return;
  }

  // Request- and insertion-scoped features can have features tainted by this
  // user. We set values to 0 to avoid complexity around erasing while
  // iterating.
  feature_context_.processRequestFeatures([](FeatureScope& scope) {
    for (auto& [k, v] : scope.features) {
      uint64_t masked_key =
          k & delivery_private_features::CountFeatureMask::TYPE;
      bool a =
          masked_key == delivery_private_features::CountType::USER_QUERY_COUNT;
      bool b = masked_key ==
               delivery_private_features::CountType::USER_QUERY_HOURS_AGO;
      bool c = masked_key ==
               delivery_private_features::CountType::LOG_USER_QUERY_COUNT;
      bool d = masked_key ==
               delivery_private_features::CountType::LOG_USER_QUERY_HOURS_AGO;
      if (a + b + c + d) {
        v = 0;
      }
    }
  });
  for (const auto& insertion : insertions_) {
    feature_context_.processInsertionFeatures(
        insertion.content_id(),
        [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
          for (auto& [k, v] : scope.features) {
            uint64_t masked_key =
                k & delivery_private_features::CountFeatureMask::TYPE;
            bool a = masked_key ==
                     delivery_private_features::CountType::USER_ITEM_COUNT;
            bool b = masked_key ==
                     delivery_private_features::CountType::USER_ITEM_HOURS_AGO;
            bool c = masked_key ==
                     delivery_private_features::CountType::LOG_USER_ITEM_COUNT;
            bool d =
                masked_key ==
                delivery_private_features::CountType::LOG_USER_ITEM_HOURS_AGO;
            if (a + b + c + d) {
              v = 0;
            }
          }
        });
  }
}
}  // namespace delivery
