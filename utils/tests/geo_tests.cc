#include <memory>

#include "gtest/gtest.h"
#include "utils/geo.h"

namespace delivery {
TEST(GeoTest, GetHaversineDistanceInMilesBig) {
  double lat_a = 1.359167;
  double lng_a = 103.989441;
  double lat_b = 10.818889;
  double lng_b = 106.65195;
  EXPECT_NEAR(getHaversineDistanceInMiles(lat_a, lng_a, lat_b, lng_b), 679.4207,
              0.01);
}

TEST(GeoTest, GetHaversineDistanceInMilesSmall) {
  double lat_a = 47.6038;
  double lng_a = -122.3301;
  double lat_b = 47.445175;
  double lng_b = -122.453075;
  EXPECT_NEAR(getHaversineDistanceInMiles(lat_a, lng_a, lat_b, lng_b), 12.3848,
              0.01);
}
}  // namespace delivery
