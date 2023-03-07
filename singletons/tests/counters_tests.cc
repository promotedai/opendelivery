#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "config/counters_config.h"
#include "execution/stages/counters.h"
#include "gtest/gtest.h"
#include "proto/delivery/private/features/features.pb.h"
#include "singletons/counters.h"

namespace delivery {
namespace counters {
// Fixture just for friending.
class CountersSingletonTest : public ::testing::Test {};

TEST_F(CountersSingletonTest, CombineSplitFeatureIds) {
  {
    std::vector<SplitFeatureID> split = {
        {false, "DAY_7", "COUNT_IMPRESSION", "ITEM_DEVICE_COUNT"},
        {false, "DAY_7", "COUNT_NAVIGATE", "ITEM_DEVICE_COUNT"}};
    auto non_split = CountersSingleton::combineSplitFeatureIds(split);
    EXPECT_EQ(non_split.size(), 2);
    EXPECT_TRUE(non_split.contains(1056806));
    EXPECT_TRUE(non_split.contains(1056838));
  }
  // Bad agg.
  {
    std::vector<SplitFeatureID> split = {
        {false, "DAY_7", "COUNT_IMPRESSIO", "ITEM_DEVICE_COUNT"}};
    auto non_split = CountersSingleton::combineSplitFeatureIds(split);
    EXPECT_TRUE(non_split.empty());
  }
  // Bad type.
  {
    std::vector<SplitFeatureID> split = {
        {false, "DAY_7", "COUNT_IMPRESSION", "ITEM_DEVICE_COUN"}};
    auto non_split = CountersSingleton::combineSplitFeatureIds(split);
    EXPECT_TRUE(non_split.empty());
  }
}

TEST_F(CountersSingletonTest, ParseEnabledFeatureIds) {
  {
    absl::flat_hash_set<uint64_t> enabled_feature_ids = {1, 2, 3};
    std::string table_feature_ids = "1,3,4";
    auto intersection = CountersSingleton::parseEnabledFeatureIds(
        enabled_feature_ids, table_feature_ids);
    EXPECT_EQ(intersection.size(), 2);
    EXPECT_TRUE(intersection.contains(1));
    EXPECT_TRUE(intersection.contains(3));
  }
  // All enabled.
  {
    absl::flat_hash_set<uint64_t> enabled_feature_ids;
    ;
    std::string table_feature_ids = "1,3,4";
    auto intersection = CountersSingleton::parseEnabledFeatureIds(
        enabled_feature_ids, table_feature_ids);
    EXPECT_EQ(intersection.size(), 3);
    EXPECT_TRUE(intersection.contains(1));
    EXPECT_TRUE(intersection.contains(3));
    EXPECT_TRUE(intersection.contains(4));
  }
  // Bad string.
  {
    absl::flat_hash_set<uint64_t> enabled_feature_ids;
    ;
    std::string table_feature_ids = "1,a,4";
    auto intersection = CountersSingleton::parseEnabledFeatureIds(
        enabled_feature_ids, table_feature_ids);
    EXPECT_TRUE(intersection.empty());
  }
}

TEST_F(CountersSingletonTest, DeriveRateFeatureIds) {
  // Ordering of result elements is based on iteration over an unordered map.
  // Custom comparator to just sort before settings expectations.
  auto rate_info_comparator = [](const RateInfo& a, const RateInfo& b) {
    return a.numerator < b.numerator && a.denominator < b.denominator &&
           a.raw < b.raw && a.smooth < b.smooth && a.global < b.global;
  };
  {
    // f1 produces no rates because of an ineligible type.
    uint64_t f1 = delivery_private_features::DAY_7 +
                  delivery_private_features::COUNT_IMPRESSION +
                  delivery_private_features::LOG_USER_QUERY_HOURS_AGO;
    // f2 produces no rates because of no eligible denominator.
    uint64_t f2 = delivery_private_features::DAY_7 +
                  delivery_private_features::COUNT_IMPRESSION +
                  delivery_private_features::ITEM_DEVICE_COUNT;
    // f3 produces no rates because of no eligible denominator.
    uint64_t f3 = delivery_private_features::DAY_30 +
                  delivery_private_features::COUNT_IMPRESSION +
                  delivery_private_features::ITEM_DEVICE_COUNT;
    // f4 produces two raw rates and two smoothed rates with fTwo as the
    // denominator.  The additional rate is the aggregated metric.
    uint64_t f4 = delivery_private_features::DAY_7 +
                  delivery_private_features::COUNT_NAVIGATE +
                  delivery_private_features::ITEM_DEVICE_COUNT;
    // These are the aggregated features corresponding to f2 and f4.
    uint64_t f2_agg = delivery_private_features::DAY_7 +
                      delivery_private_features::COUNT_IMPRESSION +
                      delivery_private_features::ITEM_COUNT;
    uint64_t f4_agg = delivery_private_features::DAY_7 +
                      delivery_private_features::COUNT_NAVIGATE +
                      delivery_private_features::ITEM_COUNT;

    absl::flat_hash_set<uint64_t> feature_ids = {f1, f2, f3, f4};
    auto derived = CountersSingleton::deriveRateFeatureIds(feature_ids);
    std::sort(derived.begin(), derived.end(), rate_info_comparator);
    ASSERT_EQ(derived.size(), 2);
    EXPECT_EQ(derived[0].numerator, f4);
    EXPECT_EQ(derived[0].denominator, f2);
    EXPECT_EQ(
        derived[0].raw,
        replaceMaskedBits(
            f4, delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_IMPRESSION,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[0].smooth,
        replaceMaskedBits(
            f4,
            delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_IMPRESSION,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[0].global,
        replaceMaskedBits(
            f4, delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_IMPRESSION,
            delivery_private_features::TYPE));
    EXPECT_EQ(derived[1].numerator, f4_agg);
    EXPECT_EQ(derived[1].denominator, f2_agg);
    EXPECT_EQ(
        derived[1].raw,
        replaceMaskedBits(
            f4_agg, delivery_private_features::ITEM_RATE_RAW_OVER_IMPRESSION,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[1].smooth,
        replaceMaskedBits(
            f4_agg, delivery_private_features::ITEM_RATE_SMOOTH_OVER_IMPRESSION,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[1].global,
        replaceMaskedBits(
            f4_agg, delivery_private_features::ITEM_RATE_RAW_OVER_IMPRESSION,
            delivery_private_features::TYPE));
  }
  // Checkout because of inconsistent ordering of actions in Protobuf enums.
  {
    uint64_t f1 = delivery_private_features::DAY_30 +
                  delivery_private_features::COUNT_CHECKOUT +
                  delivery_private_features::ITEM_DEVICE_COUNT;
    uint64_t f2 = delivery_private_features::DAY_30 +
                  delivery_private_features::COUNT_PURCHASE +
                  delivery_private_features::ITEM_DEVICE_COUNT;
    uint64_t f1_agg = delivery_private_features::DAY_30 +
                      delivery_private_features::COUNT_CHECKOUT +
                      delivery_private_features::ITEM_COUNT;
    uint64_t f2_agg = delivery_private_features::DAY_30 +
                      delivery_private_features::COUNT_PURCHASE +
                      delivery_private_features::ITEM_COUNT;

    absl::flat_hash_set<uint64_t> feature_ids = {f1, f2};
    auto derived = CountersSingleton::deriveRateFeatureIds(feature_ids);
    std::sort(derived.begin(), derived.end(), rate_info_comparator);
    ASSERT_EQ(derived.size(), 2);
    EXPECT_EQ(derived[0].numerator, f2);
    EXPECT_EQ(derived[0].denominator, f1);
    EXPECT_EQ(
        derived[0].raw,
        replaceMaskedBits(
            f2, delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_CHECKOUT,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[0].smooth,
        replaceMaskedBits(
            f2,
            delivery_private_features::ITEM_DEVICE_RATE_SMOOTH_OVER_CHECKOUT,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[0].global,
        replaceMaskedBits(
            f2, delivery_private_features::ITEM_DEVICE_RATE_RAW_OVER_CHECKOUT,
            delivery_private_features::TYPE));
    EXPECT_EQ(derived[1].numerator, f2_agg);
    EXPECT_EQ(derived[1].denominator, f1_agg);
    EXPECT_EQ(
        derived[1].raw,
        replaceMaskedBits(
            f2_agg, delivery_private_features::ITEM_RATE_RAW_OVER_CHECKOUT,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[1].smooth,
        replaceMaskedBits(
            f2_agg, delivery_private_features::ITEM_RATE_SMOOTH_OVER_CHECKOUT,
            delivery_private_features::TYPE));
    EXPECT_EQ(
        derived[1].global,
        replaceMaskedBits(
            f2_agg, delivery_private_features::ITEM_RATE_RAW_OVER_CHECKOUT,
            delivery_private_features::TYPE));
  }
}

TEST_F(CountersSingletonTest, CreateTableInfo) {
  {
    std::string name = "some_table";
    std::string row_format = "fid:value";
    std::string table_feature_ids = "100,101";
    absl::flat_hash_set<uint64_t> config_feature_ids = {101, 102};

    auto table_info = CountersSingleton::createTableInfo(
        name, row_format, table_feature_ids, config_feature_ids);
    EXPECT_EQ(table_info->name, name);
    EXPECT_EQ(table_info->key_label_map.size(), 1);
    ASSERT_TRUE(table_info->key_label_map.contains("fid"));
    EXPECT_EQ(table_info->key_label_map["fid"], 0);
    EXPECT_EQ(table_info->feature_ids.size(), 1);
    EXPECT_TRUE(table_info->feature_ids.contains(101));
    // Don't bother checking rate feature IDs.
  }
  // Invalid row format.
  {
    std::string name = "some_table";
    std::string row_format = "garbo";
    std::string table_feature_ids = "100,101";
    absl::flat_hash_set<uint64_t> config_feature_ids = {101, 102};

    auto table_info = CountersSingleton::createTableInfo(
        name, row_format, table_feature_ids, config_feature_ids);
    EXPECT_EQ(table_info, nullptr);
  }
}
}  // namespace counters
}  // namespace delivery
