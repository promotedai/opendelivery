#include "singletons/feature.h"

#include <algorithm>
#include <cmath>
#include <vector>

#include "execution/stages/compute_time_features.h"
#include "utils/math.h"

namespace delivery {
const double hour_of_day_periodic_factor = 2 * pi / 24;
const double day_of_week_periodic_factor = 2 * pi / 7;
const double day_of_month_periodic_factor = 2 * pi / 31;
const double month_of_year_periodic_factor = 2 * pi / 12;

FeatureSingleton::FeatureSingleton() {
  periodic_time_values_ = createPeriodicTimeValues();
}

PeriodicTimeValues FeatureSingleton::createPeriodicTimeValues() {
  PeriodicTimeValues ret;

  ret.hour_of_day_sin_values.resize(24);
  ret.hour_of_day_cos_values.resize(24);
  for (int i = 0; i < 24; ++i) {
    ret.hour_of_day_sin_values[i] =
        static_cast<float>(std::sin(hour_of_day_periodic_factor * i));
    ret.hour_of_day_cos_values[i] =
        static_cast<float>(std::cos(hour_of_day_periodic_factor * i));
  }

  ret.day_of_week_sin_values.resize(7);
  ret.day_of_week_cos_values.resize(7);
  for (int i = 0; i < 7; ++i) {
    ret.day_of_week_sin_values[i] =
        static_cast<float>(std::sin(day_of_week_periodic_factor * i));
    ret.day_of_week_cos_values[i] =
        static_cast<float>(std::cos(day_of_week_periodic_factor * i));
  }

  ret.day_of_month_sin_values.resize(31);
  ret.day_of_month_cos_values.resize(31);
  for (int i = 0; i < 31; ++i) {
    ret.day_of_month_sin_values[i] =
        static_cast<float>(std::sin(day_of_month_periodic_factor * i));
    ret.day_of_month_cos_values[i] =
        static_cast<float>(std::cos(day_of_month_periodic_factor * i));
  }

  ret.month_of_year_sin_values.resize(12);
  ret.month_of_year_cos_values.resize(12);
  for (int i = 0; i < 12; ++i) {
    ret.month_of_year_sin_values[i] =
        static_cast<float>(std::sin(month_of_year_periodic_factor * i));
    ret.month_of_year_cos_values[i] =
        static_cast<float>(std::cos(month_of_year_periodic_factor * i));
  }

  return ret;
}
}  // namespace delivery
