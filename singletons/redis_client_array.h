// Sw's async Redis client is thread-safe. Its underlying TCP connections are
// not thread-safe (and probably shouldn't be), but it does have the option to
// scale the number of connections. Where it falls short for us is that it only
// has one backing event loop and there is no option to scale the number of
// these. Given that some of our execution stages are currently implemented thus
// that a lot of work can happen in client threads, a single Redis client here
// could easily bottleneck. (That is to say, N Drogon event loops could be
// throwing work onto 1 Sw event loop.) To deal with this, we just make more
// clients (with one connection each).

#pragma once

#include <stddef.h>

#include <memory>
#include <string_view>
#include <vector>

#include "async_redis.h"

namespace delivery {
class RedisClientArray {
 public:
  RedisClientArray(std::string_view host, int port, int database_number,
                   int timeout_millis);

  sw::redis::AsyncRedis& getClient(size_t index);

 private:
  std::vector<std::unique_ptr<sw::redis::AsyncRedis>> clients_;
};
}  // namespace delivery
