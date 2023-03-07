#include "execution/stages/compute_query_features.h"

#include <cmath>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/match.h"
#include "execution/feature_context.h"
#include "hash_utils/make_hash.h"
#include "hash_utils/process_value.h"
#include "hash_utils/text.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
// https://nlp.stanford.edu/IR-book/html/htmledition/inverse-document-frequency-1.html
// The documents in our case are the insertion titles.
absl::flat_hash_map<int64_t, double> calculateInverseDocumentFrequencies(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context) {
  absl::flat_hash_map<int64_t, double> ret;

  absl::flat_hash_map<int64_t, int> word_to_num_titles_with_word;
  for (const auto& insertion : insertions) {
    feature_context.processInsertionFeatures(
        insertion.content_id(),
        [&word_to_num_titles_with_word](
            FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
          auto it = scope.int_list_features.find(
              delivery_private_features::CLEAN_TITLE_WORDS);
          if (it != scope.int_list_features.end()) {
            const auto& words = it->second;
            // We use a set to ignore duplicate words.
            absl::flat_hash_set<int64_t> unique_words;
            auto num_words = std::min(words.size(), hashlib::title_words_limit);
            for (int i = 0; i < num_words; ++i) {
              unique_words.emplace(words[i]);
            }
            for (int64_t word : unique_words) {
              word_to_num_titles_with_word[word]++;
            }
          }
        });
  }
  for (const auto [word, count] : word_to_num_titles_with_word) {
    // Use sqrt to give more weight to rare word matches in bigger sets.
    ret[word] = std::sqrt(static_cast<double>(insertions.size()) /
                          static_cast<double>(count)) -
                1;
  }

  return ret;
}

QueryMetadata processRequestQueryFeatures(FeatureContext& feature_context,
                                          const std::string& query,
                                          size_t num_unique_words) {
  QueryMetadata metadata;

  feature_context.processRequestFeatures([&query, &metadata, num_unique_words](
                                             FeatureScope& scope) {
    // Strip surrounding quotation marks if present.
    std::string unquoted_query;
    if (absl::StartsWith(query, "\"") && absl::EndsWith(query, "\"")) {
      scope.features[delivery_private_features::QUERY_HAS_QUOTES] = 1;
      unquoted_query = query.substr(1, query.size() - 2);
    } else {
      unquoted_query = query;
    }
    metadata.hashed_query = hashlib::makeHash(unquoted_query);
    std::string clean_query = hashlib::cleanTitle(unquoted_query);
    metadata.hashed_clean_query = hashlib::makeHash(clean_query);

    auto words = hashlib::cleanWords(clean_query);
    scope.features[delivery_private_features::CLEAN_QUERY_NUM_WORDS] =
        static_cast<float>(words.size());
    auto num_words = std::min(words.size(), hashlib::title_words_limit);
    for (int i = 0; i < num_words; ++i) {
      metadata.unique_words.emplace(hashlib::makeHash(words[i]));
    }

    scope.features[delivery_private_features::NUM_UNIQUE_TITLE_WORDS_REQUEST] =
        static_cast<float>(num_unique_words);
  });

  return metadata;
}

void processInsertionQueryFeatures(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context, const QueryMetadata& metadata,
    absl::flat_hash_map<int64_t, double>& frequencies,
    const std::string& query) {
  for (const auto& insertion : insertions) {
    feature_context.processInsertionFeatures(
        insertion.content_id(),
        [&metadata, &frequencies, &query, &insertion](
            FeatureScope& scope, const FeatureScope&, const FeatureScope&) {
          auto it =
              scope.int_features.find(delivery_private_features::EXACT_TITLE);
          if (it == scope.int_features.end()) {
            // If the item has no title, then no query<>title features can be
            // computed.
            return;
          }

          // Handle exact matches.
          if (it->second == metadata.hashed_query) {
            scope.features[delivery_private_features::EXACT_QUERY_TITLE_MATCH] =
                1;
          }
          if (scope.int_features[delivery_private_features::CLEAN_TITLE] ==
              metadata.hashed_clean_query) {
            scope.features[delivery_private_features::CLEAN_QUERY_TITLE_MATCH] =
                1;
          }
          // Exact matching of item IDs is supported.
          if (query == insertion.content_id()) {
            scope.features[delivery_private_features::CONTENT_ID_QUERY_MATCH] =
                1;
          }

          // Handle word matches.
          int num_matches = 0;
          double sum_frequency_matches = 0;

          absl::flat_hash_set<int64_t> unique_words;
          auto& words = scope.int_list_features
                            [delivery_private_features::CLEAN_TITLE_WORDS];
          auto num_words = std::min(words.size(), hashlib::title_words_limit);
          for (int i = 0; i < num_words; ++i) {
            unique_words.emplace(words[i]);
          }
          // Intersect words between the query and this item's title.
          for (int64_t word : unique_words) {
            if (metadata.unique_words.contains(word)) {
              ++num_matches;
              sum_frequency_matches += frequencies[word];
            }
          }

          scope.features
              [delivery_private_features::NUM_WORDS_QUERY_TITLE_MATCH] =
              static_cast<float>(num_matches);
          if (!metadata.unique_words.empty()) {
            scope.features
                [delivery_private_features::PCT_QUERY_WORDS_QUERY_TITLE_MATCH] =
                static_cast<float>(num_matches) /
                static_cast<float>(metadata.unique_words.size());
          }
          if (!unique_words.empty()) {
            scope.features
                [delivery_private_features::PCT_ITEM_WORDS_QUERY_TITLE_MATCH] =
                static_cast<float>(num_matches) /
                static_cast<float>(unique_words.size());
          }

          scope.features[delivery_private_features::
                             REQUEST_TFIDF_WORDS_QUERY_TITLE_MATCH] =
              static_cast<float>(sum_frequency_matches);
        });
  }
}

void ComputeQueryFeaturesStage::runSync() {
  // Empty queries are valid, but there's nothing we can compute .
  if (query_.empty()) {
    return;
  }

  absl::flat_hash_map<int64_t, double> frequencies =
      calculateInverseDocumentFrequencies(insertions_, feature_context_);

  QueryMetadata metadata =
      processRequestQueryFeatures(feature_context_, query_, frequencies.size());

  processInsertionQueryFeatures(insertions_, feature_context_, metadata,
                                frequencies, query_);
}
}  // namespace delivery
