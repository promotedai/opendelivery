// In the long term it may make sense to just have particular stages which own
// geography logic. But until we have a more mature handling of geography
// features, this will do.

#pragma once

namespace delivery {
// https://en.wikipedia.org/wiki/Haversine_formula
double getHaversineDistanceInMiles(double lat_a, double lng_a, double lat_b,
                                   double lng_b);
}  // namespace delivery
