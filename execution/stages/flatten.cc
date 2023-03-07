#include "execution/stages/flatten.h"

#include "absl/container/flat_hash_map.h"
#include "execution/feature_context.h"
#include "execution/merge_maps.h"
#include "hash_utils/flatten.h"
#include "proto/delivery/delivery.pb.h"

namespace common {
class Properties;
}

namespace delivery {
void flattenScope(uint64_t key_limit, const common::Properties& props,
                  FeatureScope& scope) {
  hashlib::FlattenOptions options{.key_limit = key_limit};
  hashlib::FlattenOutput output = hashlib::flatten(options, props);

  mergeMaps(scope.features, output.sparse_floats);
  mergeMaps(scope.int_features, output.sparse_ints);
  mergeMaps(scope.int_list_features, output.sparse_int_lists);
  mergeMaps(scope.stranger_feature_paths, output.metadata);
}

void FlattenStage::runSync() {
  // Request.
  feature_context_.processRequestFeatures([this](FeatureScope& scope) {
    flattenScope(max_request_properties_, req_.properties(), scope);
  });

  // Insertions.
  for (const auto& insertion : insertions_) {
    feature_context_.processInsertionFeatures(
        insertion.content_id(),
        [this, insertion](FeatureScope& scope, const FeatureScope&,
                          const FeatureScope&) {
          flattenScope(max_insertion_properties_, insertion.properties(),
                       scope);
        });
  }
}
}  // namespace delivery
