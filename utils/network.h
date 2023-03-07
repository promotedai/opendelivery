// The fewer network utils we need to roll, the better. We should replace this
// with a third-party networking lib if it gets larger than a couple functions.

#pragma once

#include <string>

namespace delivery {
struct StructuredRedisUrl {
  bool successful_parse = false;
  std::string hostname;
  std::string port;
  std::string database_number;
};

// Returns an empty string if an error occurred.
std::string getIp(const std::string& hostname);

// Just a rudimentary parser for full Redis connection strings.
StructuredRedisUrl parseRedisUrl(const std::string& url);
}  // namespace delivery
