// This interface is only as featured as we need it to be. Limitations
// documented inline.

#pragma once

#include <stddef.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <vector>

namespace delivery {
class RedisClient {
 public:
  virtual ~RedisClient() = default;

  // Readers.

  // If there's an error, feeds an empty vector into the callback (as if nothing
  // was previously pushed).
  virtual void lRange(const std::string& key, int64_t start, int64_t stop,
                      std::function<void(std::vector<std::string>)>&& cb) = 0;

  // If there's an error, feeds an empty vector into the callback (as if nothing
  // was previously hashed).
  virtual void hGetAll(const std::string& key,
                       std::function<void(std::vector<std::string>)>&& cb) = 0;

  // Writers.

  // If there's an error, feeds 0 into the callback (as compared to the
  // resulting list size).
  virtual void rPush(const std::string& key,
                     const std::vector<std::string>& values,
                     std::function<void(int64_t)>&& cb) = 0;

  // No callback because this isn't intended to be followed by anything.
  virtual void expire(const std::string& key, int64_t ttl) = 0;

  // No callback because this isn't intended to be followed by anything.
  virtual void lTrim(const std::string& key, int64_t start, int64_t stop) = 0;
};
}  // namespace delivery
