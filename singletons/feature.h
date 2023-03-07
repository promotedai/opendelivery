// This is a singleton to act as a global cache for any feature-related work
// that is required for every request.

#pragma once

#include <gtest/gtest_prod.h>

#include "execution/stages/compute_time_features.h"
#include "singletons/singleton.h"

namespace delivery {
class FeatureSingleton : public Singleton<FeatureSingleton> {
 public:
  const PeriodicTimeValues& getPeriodicTimeValues() {
    return periodic_time_values_;
  }

 private:
  friend class Singleton;
  FRIEND_TEST(FeatureSingletonTest, CreatePeriodicTimeValues);

  FeatureSingleton();

  static PeriodicTimeValues createPeriodicTimeValues();

  PeriodicTimeValues periodic_time_values_;
};
}  // namespace delivery
