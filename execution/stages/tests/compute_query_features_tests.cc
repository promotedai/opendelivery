#include <stddef.h>

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "execution/stages/compute_query_features.h"
#include "execution/feature_context.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "hash_utils/make_hash.h"
#include "hash_utils/text.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
const int64_t cat_hash =
    static_cast<int64_t>(hashlib::makeHash(std::string("cat")));
const int64_t in_hash =
    static_cast<int64_t>(hashlib::makeHash(std::string("in")));
const int64_t the_hash =
    static_cast<int64_t>(hashlib::makeHash(std::string("the")));
const int64_t hat_hash =
    static_cast<int64_t>(hashlib::makeHash(std::string("hat")));
const int64_t on_hash =
    static_cast<int64_t>(hashlib::makeHash(std::string("on")));
const int64_t mat_hash =
    static_cast<int64_t>(hashlib::makeHash(std::string("mat")));

TEST(ComputeQueryFeaturesTest, CalculateInverseDocumentFrequencies) {
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  insertions.emplace_back().set_content_id("c");
  FeatureContext context;
  context.initialize(insertions);
  context.processInsertionFeatures("a", [](FeatureScope& scope,
                                           const FeatureScope&,
                                           const FeatureScope&) {
    scope.int_list_features[delivery_private_features::CLEAN_TITLE_WORDS] = {
        cat_hash, in_hash, the_hash, hat_hash};
  });
  context.processInsertionFeatures("b", [](FeatureScope& scope,
                                           const FeatureScope&,
                                           const FeatureScope&) {
    scope.int_list_features[delivery_private_features::CLEAN_TITLE_WORDS] = {
        hat_hash, on_hash, the_hash, mat_hash};
  });
  context.processInsertionFeatures("c", [](FeatureScope& scope,
                                           const FeatureScope&,
                                           const FeatureScope&) {
    scope.int_list_features[delivery_private_features::CLEAN_TITLE_WORDS] = {
        the_hash};
  });
  auto frequencies = calculateInverseDocumentFrequencies(insertions, context);

  EXPECT_EQ(frequencies.size(), 6);
  EXPECT_FLOAT_EQ(frequencies[cat_hash], std::sqrt(3.0) - 1);
  EXPECT_FLOAT_EQ(frequencies[hat_hash], std::sqrt(1.5) - 1);
  EXPECT_FLOAT_EQ(frequencies[the_hash], std::sqrt(1.0) - 1);
}

TEST(ComputeQueryFeaturesTest, ProcessRequestQueryFeatures) {
  FeatureContext context;
  std::string query = R"(" CAT hat:  Mat!")";
  size_t num_unique_words = 10;
  auto metadata = processRequestQueryFeatures(context, query, num_unique_words);

  const auto& scope = context.getRequestFeatures();
  EXPECT_EQ(scope.features.size(), 3);
  EXPECT_EQ(scope.features.at(delivery_private_features::QUERY_HAS_QUOTES), 1);
  EXPECT_EQ(scope.features.at(delivery_private_features::CLEAN_QUERY_NUM_WORDS),
            3);
  EXPECT_EQ(scope.features.at(
                delivery_private_features::NUM_UNIQUE_TITLE_WORDS_REQUEST),
            10);

  EXPECT_EQ(metadata.hashed_query,
            hashlib::makeHash(std::string(" CAT hat:  Mat!")));
  EXPECT_EQ(metadata.hashed_clean_query,
            hashlib::makeHash(std::string("cat hat: mat!")));
  EXPECT_THAT(metadata.unique_words,
              ::testing::UnorderedElementsAre(static_cast<uint64_t>(cat_hash),
                                              static_cast<uint64_t>(hat_hash),
                                              static_cast<uint64_t>(mat_hash)));
}

TEST(ComputeQueryFeaturesTest, ProcessInsertionQueryFeatures) {
  std::string query = " cat in THE HAT ";
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  insertions.emplace_back().set_content_id("c");
  FeatureContext context;
  context.initialize(insertions);
  context.processInsertionFeatures("a", [&](FeatureScope& scope,
                                            const FeatureScope&,
                                            const FeatureScope&) {
    std::string title = "cat in the hat";
    scope.int_features[delivery_private_features::EXACT_TITLE] =
        static_cast<int64_t>(hashlib::makeHash(title));
    scope.int_features[delivery_private_features::CLEAN_TITLE] =
        static_cast<int64_t>(hashlib::makeHash(hashlib::cleanTitle(title)));
    scope.int_list_features[delivery_private_features::CLEAN_TITLE_WORDS] = {
        cat_hash, in_hash, the_hash, hat_hash};
  });
  context.processInsertionFeatures("b", [&](FeatureScope& scope,
                                            const FeatureScope&,
                                            const FeatureScope&) {
    std::string title = "hat on the HAt:mat";
    scope.int_features[delivery_private_features::EXACT_TITLE] =
        static_cast<int64_t>(hashlib::makeHash(title));
    scope.int_features[delivery_private_features::CLEAN_TITLE] =
        static_cast<int64_t>(hashlib::makeHash(hashlib::cleanTitle(title)));
    scope.int_list_features[delivery_private_features::CLEAN_TITLE_WORDS] = {
        the_hash, hat_hash, on_hash, mat_hash};
  });
  context.processInsertionFeatures("c", [&](FeatureScope& scope,
                                            const FeatureScope&,
                                            const FeatureScope&) {
    std::string title = "  THE!";
    scope.int_features[delivery_private_features::EXACT_TITLE] =
        static_cast<int64_t>(hashlib::makeHash(title));
    scope.int_features[delivery_private_features::CLEAN_TITLE] =
        static_cast<int64_t>(hashlib::makeHash(hashlib::cleanTitle(title)));
    scope.int_list_features[delivery_private_features::CLEAN_TITLE_WORDS] = {
        the_hash};
  });
  QueryMetadata metadata;
  metadata.hashed_query = hashlib::makeHash(query);
  metadata.hashed_clean_query = hashlib::makeHash(hashlib::cleanTitle(query));
  metadata.unique_words = {
      static_cast<uint64_t>(cat_hash), static_cast<uint64_t>(in_hash),
      static_cast<uint64_t>(the_hash), static_cast<uint64_t>(hat_hash)};
  absl::flat_hash_map<int64_t, double> frequencies = {
      {cat_hash, 10}, {in_hash, 20}, {the_hash, 30},
      {hat_hash, 40}, {on_hash, 50}, {mat_hash, 60}};
  processInsertionQueryFeatures(insertions, context, metadata, frequencies,
                                query);

  const auto& scope_a = context.getInsertionFeatures("a");
  EXPECT_EQ(scope_a.features.size(), 5);
  EXPECT_FALSE(scope_a.features.contains(
      delivery_private_features::EXACT_QUERY_TITLE_MATCH));
  EXPECT_TRUE(scope_a.features.contains(
      delivery_private_features::CLEAN_QUERY_TITLE_MATCH));
  EXPECT_EQ(scope_a.features.at(
                delivery_private_features::NUM_WORDS_QUERY_TITLE_MATCH),
            4);
  EXPECT_EQ(scope_a.features.at(
                delivery_private_features::PCT_QUERY_WORDS_QUERY_TITLE_MATCH),
            1);
  EXPECT_EQ(scope_a.features.at(
                delivery_private_features::PCT_ITEM_WORDS_QUERY_TITLE_MATCH),
            1);
  EXPECT_EQ(
      scope_a.features.at(
          delivery_private_features::REQUEST_TFIDF_WORDS_QUERY_TITLE_MATCH),
      100);

  const auto& scope_b = context.getInsertionFeatures("b");
  EXPECT_EQ(scope_b.features.size(), 4);
  EXPECT_FALSE(scope_b.features.contains(
      delivery_private_features::EXACT_QUERY_TITLE_MATCH));
  EXPECT_FALSE(scope_b.features.contains(
      delivery_private_features::CLEAN_QUERY_TITLE_MATCH));
  EXPECT_EQ(scope_b.features.at(
                delivery_private_features::NUM_WORDS_QUERY_TITLE_MATCH),
            2);
  EXPECT_EQ(scope_b.features.at(
                delivery_private_features::PCT_QUERY_WORDS_QUERY_TITLE_MATCH),
            0.5);
  EXPECT_EQ(scope_b.features.at(
                delivery_private_features::PCT_ITEM_WORDS_QUERY_TITLE_MATCH),
            0.5);
  EXPECT_EQ(
      scope_b.features.at(
          delivery_private_features::REQUEST_TFIDF_WORDS_QUERY_TITLE_MATCH),
      70);

  const auto& scope_c = context.getInsertionFeatures("c");
  EXPECT_EQ(scope_c.features.size(), 4);
  EXPECT_FALSE(scope_c.features.contains(
      delivery_private_features::EXACT_QUERY_TITLE_MATCH));
  EXPECT_FALSE(scope_c.features.contains(
      delivery_private_features::CLEAN_QUERY_TITLE_MATCH));
  EXPECT_EQ(scope_c.features.at(
                delivery_private_features::NUM_WORDS_QUERY_TITLE_MATCH),
            1);
  EXPECT_EQ(scope_c.features.at(
                delivery_private_features::PCT_QUERY_WORDS_QUERY_TITLE_MATCH),
            0.25);
  EXPECT_EQ(scope_c.features.at(
                delivery_private_features::PCT_ITEM_WORDS_QUERY_TITLE_MATCH),
            1);
  EXPECT_EQ(
      scope_c.features.at(
          delivery_private_features::REQUEST_TFIDF_WORDS_QUERY_TITLE_MATCH),
      30);
}

TEST(ComputeQueryFeaturesTest, RunSyncEmptyQuery) {
  std::string query = "";
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  context.initialize(insertions);
  // Set titles to not short-circuit query<>title processing.
  context.processInsertionFeatures(
      "a", [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
        scope.int_features[delivery_private_features::EXACT_TITLE] = 100;
      });
  context.processInsertionFeatures(
      "b", [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
        scope.int_features[delivery_private_features::EXACT_TITLE] = 100;
      });
  ComputeQueryFeaturesStage stage(0, query, insertions, context);
  stage.runSync();

  EXPECT_TRUE(context.getRequestFeatures().features.empty());
  EXPECT_TRUE(context.getInsertionFeatures("a").features.empty());
  EXPECT_TRUE(context.getInsertionFeatures("b").features.empty());
}

TEST(ComputeQueryFeaturesTest, RunSync) {
  std::string query = "not empty";
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  context.initialize(insertions);
  // Set titles to not short-circuit query<>title processing.
  context.processInsertionFeatures(
      "a", [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
        scope.int_features[delivery_private_features::EXACT_TITLE] = 100;
      });
  context.processInsertionFeatures(
      "b", [](FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
        scope.int_features[delivery_private_features::EXACT_TITLE] = 100;
      });
  ComputeQueryFeaturesStage stage(0, query, insertions, context);
  stage.runSync();

  EXPECT_FALSE(context.getRequestFeatures().features.empty());
  EXPECT_FALSE(context.getInsertionFeatures("a").features.empty());
  EXPECT_FALSE(context.getInsertionFeatures("b").features.empty());
}
}  // namespace delivery
