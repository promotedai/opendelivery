#include <memory>
#include <vector>

#include "execution/feature_context.h"
#include "execution/stages/init_features.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
TEST(InitFeaturesStageTest, Initialize) {
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  FeatureContext context;
  InitFeaturesStage stage(0, insertions, context);
  stage.runSync();

  context.getInsertionFeatures("a");
  context.getInsertionFeatures("b");
}
}  // namespace delivery
