#include "utils/time.h"

#include <chrono>
#include <type_traits>

#include "absl/strings/str_cat.h"

namespace delivery {
uint64_t millisSinceEpoch() {
  return std::chrono::time_point_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now())
      .time_since_epoch()
      .count();
}

uint64_t millisForDuration() {
  return std::chrono::time_point_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now())
      .time_since_epoch()
      .count();
}

std::string makeTimedKey(std::string_view key, uint64_t millis) {
  return absl::StrCat(key, millis / millis_in_15_min);
}
}  // namespace delivery
