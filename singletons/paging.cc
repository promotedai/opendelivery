#include "singletons/paging.h"

#include <cstdlib>
#include <exception>
#include <string>

#include "cloud/sw_redis_client.h"
#include "config/paging_config.h"
#include "config/platform_config.h"
#include "redis_client_array.h"
#include "singletons/config.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"
#include "utils/network.h"

namespace delivery {
void createClients(const std::string& url, const std::string& timeout,
                   std::unique_ptr<RedisClientArray>& client_array) {
  auto structured_url = parseRedisUrl(url);
  if (!structured_url.successful_parse) {
    LOG_FATAL << "Invalid paging URL: " << url;
    abort();
  }
  const int port = std::atoi(structured_url.port.c_str());
  if (port == 0) {
    LOG_FATAL << "Invalid paging port: " << structured_url.port;
    abort();
  }
  const int database_number = std::atoi(structured_url.database_number.c_str());
  if (database_number == 0 && structured_url.database_number != "0") {
    LOG_FATAL << "Invalid paging database number: " << structured_url.port;
    abort();
  }
  int timeout_millis = -1.0;
  try {
    timeout_millis = std::stoi(timeout);
  } catch (const std::exception&) {
    LOG_FATAL << "Invalid timeout: " << timeout;
    abort();
  }

  client_array = std::make_unique<RedisClientArray>(
      structured_url.hostname, port, database_number, timeout_millis);
};

PagingSingleton::PagingSingleton() {
  auto platform_config =
      delivery::ConfigSingleton::getInstance().getPlatformConfig();
  const auto& paging_config = platform_config.paging_config;

  // Paging is currently required.
  if (paging_config.url.empty()) {
    LOG_FATAL << "No paging URL specified";
    abort();
  }
  createClients(paging_config.url, paging_config.timeout, clients_);

  // Read replicas are not required. We will just fall back to using the other
  // client.
  if (paging_config.read_url.empty()) {
    return;
  }
  createClients(paging_config.read_url, paging_config.timeout, read_clients_);
}

std::unique_ptr<RedisClient> PagingSingleton::getPagingClient(size_t index) {
  return std::make_unique<SwRedisClient>(clients_->getClient(index));
}

std::unique_ptr<RedisClient> PagingSingleton::getPagingReadClient(
    size_t index) {
  if (read_clients_ != nullptr) {
    return std::make_unique<SwRedisClient>(read_clients_->getClient(index));
  } else {
    // If there is no read replica, fall back to using the other client.
    return std::make_unique<SwRedisClient>(clients_->getClient(index));
  }
}
}  // namespace delivery
