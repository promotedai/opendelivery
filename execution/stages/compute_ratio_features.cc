#include "execution/stages/compute_ratio_features.h"

#include <cstdint>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "execution/feature_context.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
namespace {
// We generally try to avoid this, but it really helps with readability of this
// file.
namespace dpf = delivery_private_features;
}  // namespace

struct RatioFeatureMetadata {
  uint64_t numerator_id = 0;
  uint64_t denominator_id = 0;
  // This is defined as the time units of the numerator / the time units of the
  // denominator. The result ratio is divided by this. This is intended to be
  // used when the numerator and denominator aren't already rates themselves.
  float time_ratio = 1;
  uint64_t result_id = 0;
};

// These should eventually be specified by config.
const std::vector<RatioFeatureMetadata> insertion_ratio_features = {
    {.numerator_id =
         dpf::ITEM_RATE_RAW_OVER_IMPRESSION + dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id =
         dpf::ITEM_RATE_RAW_OVER_IMPRESSION + dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::RAW_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::SMOOTH_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_DEVICE_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id = dpf::ITEM_DEVICE_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::SMOOTH_DEVICE_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id =
         dpf::ITEM_RATE_SMOOTH_OVER_IMPRESSION + dpf::COUNT_NAVIGATE + dpf::DAY,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .result_id = dpf::SMOOTH_CTR_1_DAY_TO_7_DAY_COUNTER_RATE_RATIO},
    {.numerator_id =
         dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE + dpf::COUNT_PURCHASE + dpf::DAY_7,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_PURCHASE + dpf::DAY_30,
     .result_id =
         dpf::SMOOTH_POST_NAVIGATE_PURCHASE_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id =
         dpf::ITEM_RATE_RAW_OVER_NAVIGATE + dpf::COUNT_PURCHASE + dpf::DAY_7,
     .denominator_id =
         dpf::ITEM_RATE_RAW_OVER_NAVIGATE + dpf::COUNT_PURCHASE + dpf::DAY_30,
     .result_id =
         dpf::RAW_POST_NAVIGATE_PURCHASE_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE +
                     dpf::COUNT_ADD_TO_CART + dpf::DAY_7,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_ADD_TO_CART + dpf::DAY_30,
     .result_id = dpf::
         SMOOTH_POST_NAVIGATE_ADD_TO_CART_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id =
         dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE + dpf::COUNT_CHECKOUT + dpf::DAY_7,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_CHECKOUT + dpf::DAY_30,
     .result_id =
         dpf::SMOOTH_POST_NAVIGATE_CHECKOUT_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::SMOOTH_CTR_30_DAY_ITEMXQUERY_TO_ITEM_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .denominator_id = dpf::QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id =
         dpf::SMOOTH_CTR_30_DAY_ITEMXQUERY_TO_QUERY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_RATE_SMOOTH_OVER_NAVIGATE +
                     dpf::COUNT_PURCHASE + dpf::DAY_30,
     .denominator_id = dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_PURCHASE + dpf::DAY_30,
     .result_id = dpf::
         POST_NAVIGATE_PURCHASE_30_DAY_ITEMXQUERY_TO_ITEM_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_RATE_SMOOTH_OVER_NAVIGATE +
                     dpf::COUNT_PURCHASE + dpf::DAY_30,
     .denominator_id = dpf::QUERY_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_PURCHASE + dpf::DAY_30,
     .result_id = dpf::
         POST_NAVIGATE_PURCHASE_30_DAY_ITEMXQUERY_TO_QUERY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .denominator_id = dpf::USER_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::SMOOTH_CTR_30_DAY_ITEM_TO_USER_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_RATE_SMOOTH_OVER_NAVIGATE + dpf::COUNT_PURCHASE +
                     dpf::DAY_30,
     .denominator_id = dpf::USER_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_PURCHASE + dpf::DAY_30,
     .result_id = dpf::
         SMOOTH_POST_NAVIGATE_PURCHASE_30_DAY_ITEM_TO_USER_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_IMPRESSION + dpf::HOUR,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::ITEM_IMPRESSION_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_7,
     .time_ratio = 1.0 / 7,
     .result_id = dpf::ITEM_IMPRESSION_1_TO_7_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_7,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::ITEM_IMPRESSION_7_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::HOUR,
     .denominator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::ITEMXQUERY_IMPRESSION_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .denominator_id =
         dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_7,
     .time_ratio = 1.0 / 7,
     .result_id = dpf::ITEMXQUERY_IMPRESSION_1_TO_7_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_7,
     .denominator_id =
         dpf::ITEM_QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::ITEMXQUERY_IMPRESSION_7_TO_30_DAY_COUNT_RATE_RATIO},

    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_NAVIGATE + dpf::HOUR,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::ITEM_NAVIGATE_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .time_ratio = 1.0 / 7,
     .result_id = dpf::ITEM_NAVIGATE_1_TO_7_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::ITEM_NAVIGATE_7_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_PURCHASE + dpf::DAY,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_7,
     .time_ratio = 1.0 / 7,
     .result_id = dpf::ITEM_PURCHASE_1_TO_7_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_7,
     .denominator_id = dpf::ITEM_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::ITEM_PURCHASE_7_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .denominator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .time_ratio = 1.0 / 7,
     .result_id = dpf::ITEMXQUERY_NAVIGATE_1_TO_7_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id =
         dpf::ITEM_QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::ITEMXQUERY_NAVIGATE_7_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_PURCHASE + dpf::DAY,
     .denominator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_7,
     .time_ratio = 1.0 / 7,
     .result_id = dpf::ITEMXQUERY_PURCHASE_1_TO_7_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::ITEM_QUERY_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_7,
     .denominator_id =
         dpf::ITEM_QUERY_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::ITEMXQUERY_PURCHASE_7_TO_30_DAY_COUNT_RATE_RATIO},
};

const std::vector<RatioFeatureMetadata> request_ratio_features = {
    {.numerator_id = dpf::QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id = dpf::QUERY_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::SMOOTH_QUERY_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::QUERY_RATE_SMOOTH_OVER_NAVIGATE +
                     dpf::COUNT_PURCHASE + dpf::DAY_7,
     .denominator_id = dpf::QUERY_RATE_SMOOTH_OVER_NAVIGATE +
                       dpf::COUNT_PURCHASE + dpf::DAY_30,
     .result_id = dpf::
         SMOOTH_QUERY_POST_NAVIGATE_PURCHASE_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::HOUR,
     .denominator_id = dpf::QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::QUERY_IMPRESSION_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .denominator_id = dpf::QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::QUERY_IMPRESSION_1_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::HOUR,
     .denominator_id = dpf::QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::QUERY_NAVIGATE_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .denominator_id = dpf::QUERY_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::QUERY_NAVIGATE_1_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::QUERY_COUNT + dpf::COUNT_PURCHASE + dpf::DAY,
     .denominator_id = dpf::QUERY_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::QUERY_PURCHASE_1_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_7,
     .denominator_id = dpf::QUERY_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30,
     .time_ratio = 7.0 / 30,
     .result_id = dpf::QUERY_IMPRESSION_7_TO_30_DAY_COUNT_RATE_RATIO},
};

const std::vector<RatioFeatureMetadata> user_ratio_features = {
    {.numerator_id = dpf::USER_RATE_SMOOTH_OVER_IMPRESSION +
                     dpf::COUNT_NAVIGATE + dpf::DAY_7,
     .denominator_id = dpf::USER_RATE_SMOOTH_OVER_IMPRESSION +
                       dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .result_id = dpf::SMOOTH_USER_CTR_7_DAY_TO_30_DAY_COUNTER_RATE_RATIO},
    {.numerator_id = dpf::USER_COUNT + dpf::COUNT_IMPRESSION + dpf::HOUR,
     .denominator_id = dpf::USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::USER_IMPRESSION_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .denominator_id = dpf::USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::USER_IMPRESSION_1_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::LOG_USER_COUNT + dpf::COUNT_IMPRESSION + dpf::HOUR,
     .denominator_id = dpf::LOG_USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::LOG_USER_IMPRESSION_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::LOG_USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY,
     .denominator_id =
         dpf::LOG_USER_COUNT + dpf::COUNT_IMPRESSION + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::LOG_USER_IMPRESSION_1_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::USER_COUNT + dpf::COUNT_NAVIGATE + dpf::HOUR,
     .denominator_id = dpf::USER_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .time_ratio = 1.0 / 24,
     .result_id = dpf::USER_NAVIGATE_1_TO_24_HOUR_COUNT_RATE_RATIO},
    {.numerator_id = dpf::USER_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY,
     .denominator_id = dpf::USER_COUNT + dpf::COUNT_NAVIGATE + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::USER_NAVIGATE_1_TO_30_DAY_COUNT_RATE_RATIO},
    {.numerator_id = dpf::USER_COUNT + dpf::COUNT_PURCHASE + dpf::DAY,
     .denominator_id = dpf::USER_COUNT + dpf::COUNT_PURCHASE + dpf::DAY_30,
     .time_ratio = 1.0 / 30,
     .result_id = dpf::USER_PURCHASE_1_TO_30_DAY_COUNT_RATE_RATIO},
};

void calculateScopeRatios(FeatureScope& scope,
                          const std::vector<RatioFeatureMetadata>& metadata) {
  // We iterate through the metadata instead of the sparse, so save ourselves
  // the effort if a scope is empty.
  if (scope.features.empty()) {
    return;
  }

  for (const auto& ratio : metadata) {
    auto numerator_it = scope.features.find(ratio.numerator_id);
    if (numerator_it != scope.features.end()) {
      auto denominator_it = scope.features.find(ratio.denominator_id);
      if (denominator_it != scope.features.end() &&
          denominator_it->second != 0) {
        scope.features[ratio.result_id] =
            numerator_it->second / denominator_it->second / ratio.time_ratio;
      }
    }
  }
}

// Insertion ratios can have their denominators in other scopes. This checks
// those if the denominator is not in the insertion scope.
void calculateInsertionScopeRatios(
    FeatureScope& insertion_scope, const FeatureScope& request_scope,
    const FeatureScope& user_scope,
    const std::vector<RatioFeatureMetadata>& metadata) {
  if (insertion_scope.features.empty()) {
    return;
  }

  for (const auto& ratio : metadata) {
    auto numerator_it = insertion_scope.features.find(ratio.numerator_id);
    if (numerator_it != insertion_scope.features.end()) {
      auto denominator_it = insertion_scope.features.find(ratio.denominator_id);
      if (denominator_it != insertion_scope.features.end() &&
          denominator_it->second != 0) {
        insertion_scope.features[ratio.result_id] =
            numerator_it->second / denominator_it->second / ratio.time_ratio;
        continue;
      }
      auto denominator_const_it =
          request_scope.features.find(ratio.denominator_id);
      if (denominator_const_it != request_scope.features.end() &&
          denominator_const_it->second != 0) {
        insertion_scope.features[ratio.result_id] =
            numerator_it->second / denominator_const_it->second /
            ratio.time_ratio;
        continue;
      }
      denominator_const_it = user_scope.features.find(ratio.denominator_id);
      if (denominator_const_it != user_scope.features.end() &&
          denominator_const_it->second != 0) {
        insertion_scope.features[ratio.result_id] =
            numerator_it->second / denominator_const_it->second /
            ratio.time_ratio;
      }
    }
  }
}

void ComputeRatioFeaturesStage::runSync() {
  // Go over every scope.
  feature_context_.processUserFeatures([](FeatureScope& scope) {
    calculateScopeRatios(scope, user_ratio_features);
  });
  feature_context_.processRequestFeatures([](FeatureScope& scope) {
    calculateScopeRatios(scope, request_ratio_features);
  });
  for (const auto& insertion : insertions_) {
    feature_context_.processInsertionFeatures(
        insertion.content_id(),
        [](FeatureScope& insertion_scope, const FeatureScope& request_scope,
           const FeatureScope& user_scope) {
          calculateInsertionScopeRatios(insertion_scope, request_scope,
                                        user_scope, insertion_ratio_features);
        });
  }
}
}  // namespace delivery
