// Primarily shorthand for common chrono recipes. The chrono library is verbose
// because of its type system, but it doesn't really help us because we just
// store these time values in Protobuf uints anyway.

#pragma once

#include <stdint.h>

#include <string>
#include <string_view>

namespace delivery {
const int millis_in_15_min = 1'000 * 60 * 15;

uint64_t millisSinceEpoch();
uint64_t millisForDuration();

std::string makeTimedKey(std::string_view key, uint64_t millis);
}  // namespace delivery
