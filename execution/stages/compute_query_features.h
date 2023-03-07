// This stage is responsible for computing features based on the request's
// search query.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "execution/stages/stage.h"

namespace delivery {
class FeatureContext;
class Insertion;
}  // namespace delivery

namespace delivery {
class ComputeQueryFeaturesStage : public Stage {
 public:
  ComputeQueryFeaturesStage(size_t id, const std::string& query,
                            const std::vector<delivery::Insertion>& insertions,
                            FeatureContext& feature_context)
      : Stage(id),
        query_(query),
        insertions_(insertions),
        feature_context_(feature_context) {}

  std::string name() const override { return "ComputeQueryFeatures"; }

  void runSync() override;

 private:
  const std::string& query_;
  const std::vector<delivery::Insertion>& insertions_;
  FeatureContext& feature_context_;
};

// Declared here for testing.
// For stashing things from request-scope processing which will also be needed
// by insertion-scope processing.
struct QueryMetadata {
  // Surrounding quotation marks are removed before hashing.
  uint64_t hashed_query = 0;

  // Surrounding quotation marks are removed, white space is collapsed, and
  // characters are lower-cased before hashing.
  uint64_t hashed_clean_query = 0;

  // Deduped words split from the clean query.
  absl::flat_hash_set<uint64_t> unique_words;
};
absl::flat_hash_map<int64_t, double> calculateInverseDocumentFrequencies(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context);
QueryMetadata processRequestQueryFeatures(FeatureContext& feature_context,
                                          const std::string& query,
                                          size_t num_unique_words);
void processInsertionQueryFeatures(
    const std::vector<delivery::Insertion>& insertions,
    FeatureContext& feature_context, const QueryMetadata& metadata,
    absl::flat_hash_map<int64_t, double>& frequencies,
    const std::string& query);
}  // namespace delivery
