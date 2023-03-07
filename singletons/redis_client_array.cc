#include "singletons/redis_client_array.h"

#include <stdlib.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "async_utils.h"
#include "connection.h"
#include "errors.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace delivery {
RedisClientArray::RedisClientArray(std::string_view host, int port,
                                   int database_number, int timeout_millis) {
  sw::redis::ConnectionOptions connection_opts;
  connection_opts.host = host;
  connection_opts.port = port;
  connection_opts.db = database_number;
  // Arbitrary connection timeout to ensure an attempt can't hang forever.
  connection_opts.connect_timeout = std::chrono::milliseconds(10'000);
  connection_opts.socket_timeout = std::chrono::milliseconds(timeout_millis);

  // This has to remain in sync with the number of Drogon threads/loops we
  // create in main.cc. It's unlikely that we'll ever want to change either
  // location so not factoring it out for now.
  clients_.resize(std::thread::hardware_concurrency());

  // Simple setup to block this initialization on async pings of all clients.
  std::mutex m;
  size_t remaining_clients = clients_.size();
  auto cv = std::make_shared<std::condition_variable>();
  std::unique_lock<std::mutex> lock(m);

  for (auto &client : clients_) {
    client = std::make_unique<sw::redis::AsyncRedis>(connection_opts);
    client->command<std::string>(
        "ping",
        [&m, &remaining_clients, cv](sw::redis::Future<std::string> &&fut) {
          try {
            // If successful, this will return "pong" (though we don't care).
            // If unsuccessful, this will throw.
            fut.get();
          } catch (const sw::redis::TimeoutError &err) {
            /// Timeouts aren't necessarily problematic.
            LOG_INFO << "Timed out during ping: " << err.what();
          } catch (const sw::redis::Error &err) {
            // Other errors are problematic.
            LOG_FATAL << "Failed to ping: " << err.what();
            abort();
          }
          {
            std::lock_guard<std::mutex> lock(m);
            --remaining_clients;
          }
          cv->notify_all();
        });
  }

  cv->wait(lock, [&remaining_clients] { return remaining_clients == 0; });
  LOG_INFO << "Redis client array initialized";
}

sw::redis::AsyncRedis &RedisClientArray::getClient(size_t index) {
  return *clients_[index];
}
}  // namespace delivery
