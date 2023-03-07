#include "execution/stages/init_features.h"

#include "execution/feature_context.h"

namespace delivery {
void InitFeaturesStage::runSync() { feature_context_.initialize(insertions_); }
}  // namespace delivery
