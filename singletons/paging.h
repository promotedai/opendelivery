// This owns the logic around creating and getting the appropriate clients for
// paging. This is a singleton because those clients are inherently global
// state.

#pragma once

#include <stddef.h>

#include <memory>

#include "singletons/redis_client_array.h"
#include "singletons/singleton.h"

namespace delivery {
class RedisClient;
}

namespace delivery {
class PagingSingleton : public Singleton<PagingSingleton> {
 public:
  std::unique_ptr<RedisClient> getPagingClient(size_t index);
  std::unique_ptr<RedisClient> getPagingReadClient(size_t index);

 private:
  friend class Singleton;

  PagingSingleton();

  std::unique_ptr<RedisClientArray> clients_;
  std::unique_ptr<RedisClientArray> read_clients_;
};
}  // namespace delivery
