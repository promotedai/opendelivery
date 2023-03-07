#include <vector>

#include "execution/stages/compute_time_features.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "singletons/feature.h"

namespace delivery {
// Fixture just for friending.
class FeatureSingletonTest : public ::testing::Test {
 protected:
  void expectFloatVectorNear(const std::vector<float>& a,
                             const std::vector<float>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (int i = 0; i < a.size(); ++i) {
      EXPECT_NEAR(a[i], b[i], .000001);
    }
  }
};

TEST_F(FeatureSingletonTest, CreatePeriodicTimeValues) {
  PeriodicTimeValues values = FeatureSingleton::createPeriodicTimeValues();

  ASSERT_EQ(values.hour_of_day_sin_values.size(), 24);
  expectFloatVectorNear(
      values.hour_of_day_sin_values,
      {0,  0.25881904,  0.5,         0.70710677,  0.86602539,  0.96592581,
       1,  0.96592581,  0.86602539,  0.70710677,  0.5,         0.25881904,
       0,  -0.25881904, -0.5,        -0.70710677, -0.86602539, -0.96592581,
       -1, -0.96592581, -0.86602539, -0.70710677, -0.5,        -0.25881904});
  ASSERT_EQ(values.hour_of_day_cos_values.size(), 24);
  expectFloatVectorNear(
      values.hour_of_day_cos_values,
      {1,  0.96592581,  0.86602539,  0.70710677,  0.5,         0.25881904,
       0,  -0.25881904, -0.5,        -0.70710677, -0.86602539, -0.96592581,
       -1, -0.96592581, -0.86602539, -0.70710677, -0.5,        -0.25881904,
       0,  0.25881904,  0.5,         0.70710677,  0.86602539,  0.96592581});

  ASSERT_EQ(values.day_of_week_sin_values.size(), 7);
  expectFloatVectorNear(values.day_of_week_sin_values,
                        {0, 0.78183150, 0.97492790, 0.43388372, -0.43388372,
                         -0.97492790, -0.78183150});
  ASSERT_EQ(values.day_of_week_cos_values.size(), 7);
  expectFloatVectorNear(values.day_of_week_cos_values,
                        {1, 0.62348979, -0.22252093, -0.90096884, -0.90096884,
                         -0.22252093, 0.62348979});

  ASSERT_EQ(values.day_of_month_sin_values.size(), 31);
  expectFloatVectorNear(
      values.day_of_month_sin_values,
      {0,           0.20129851,  0.39435586,  0.57126820,  0.72479277,
       0.84864425,  0.93775212,  0.98846834,  0.99871653,  0.96807712,
       0.89780455,  0.79077571,  0.65137249,  0.48530197,  0.29936313,
       0.10116831,  -0.10116831, -0.29936313, -0.48530197, -0.65137249,
       -0.79077571, -0.89780455, -0.96807712, -0.99871653, -0.98846834,
       -0.93775212, -0.84864425, -0.72479277, -0.57126820, -0.39435586,
       -0.20129851});
  ASSERT_EQ(values.day_of_month_cos_values.size(), 31);
  expectFloatVectorNear(
      values.day_of_month_cos_values,
      {1,           0.97952991,  0.91895782,  0.82076346,  0.68896692,
       0.52896398,  0.34730523,  0.15142777,  -0.05064916, -0.25065252,
       -0.44039416, -0.61210596, -0.75875812, -0.87434661, -0.95413923,
       -0.99486935, -0.99486935, -0.95413923, -0.87434661, -0.75875812,
       -0.61210596, -0.44039416, -0.25065252, -0.05064916, 0.15142777,
       0.34730523,  0.52896398,  0.68896692,  0.82076346,  0.91895782,
       0.97952991});

  ASSERT_EQ(values.month_of_year_sin_values.size(), 12);
  expectFloatVectorNear(values.month_of_year_sin_values,
                        {0, 0.5, 0.86602539, 1, 0.86602539, 0.5, 0, -0.5,
                         -0.86602539, -1, -0.86602539, -0.5});
  ASSERT_EQ(values.month_of_year_cos_values.size(), 12);
  expectFloatVectorNear(values.month_of_year_cos_values,
                        {1, 0.86602539, 0.5, 0, -0.5, -0.86602539, -1,
                         -0.86602539, -0.5, 0, 0.5, 0.86602539});
}
}  // namespace delivery
