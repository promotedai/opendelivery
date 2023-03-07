#include "utils/geo.h"

#include <cmath>

#include "utils/math.h"

namespace delivery {
const double earth_radius_in_miles = 3963.19059;
const double to_radians = pi / 180;

double getHaversineDistanceInMiles(double lat_a, double lng_a, double lat_b,
                                   double lng_b) {
  double lat_diff = (lat_a - lat_b) * to_radians;
  double lng_diff = (lng_a - lng_b) * to_radians;

  lat_a *= to_radians;
  lat_b *= to_radians;

  double a = std::sin(lat_diff / 2) * std::sin(lat_diff / 2) +
             std::sin(lng_diff / 2) * std::sin(lng_diff / 2) * std::cos(lat_a) *
                 std::cos(lat_b);
  double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
  return earth_radius_in_miles * c;
}
}  // namespace delivery
