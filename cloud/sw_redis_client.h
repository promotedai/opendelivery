// Responsible for interacting with Sw's Redis client impl and returning
// standard C++ types.
//
// Sw is just the abbreviated name of the person who started developing it.

#pragma once

#include <stdint.h>

#include <functional>
#include <string>
#include <vector>

#include "execution/stages/redis_client.h"

namespace sw {
namespace redis {
class AsyncRedis;
}
}  // namespace sw

namespace delivery {
class SwRedisClient : public RedisClient {
 public:
  explicit SwRedisClient(sw::redis::AsyncRedis& client) : client_(client) {}

  void lRange(const std::string& key, int64_t start, int64_t stop,
              std::function<void(std::vector<std::string>)>&& cb) override;
  void hGetAll(const std::string& key,
               std::function<void(std::vector<std::string>)>&& cb) override;
  void rPush(const std::string& key, const std::vector<std::string>& values,
             std::function<void(int64_t)>&& cb) override;
  void expire(const std::string& key, int64_t ttl) override;
  void lTrim(const std::string& key, int64_t start, int64_t stop) override;

 private:
  sw::redis::AsyncRedis& client_;
};
}  // namespace delivery
