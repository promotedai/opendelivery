#include "cloud/sw_redis_client.h"

#include <utility>

#include "async_redis.h"
#include "async_utils.h"
#include "errors.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace delivery {
void SwRedisClient::lRange(const std::string &key, int64_t start, int64_t stop,
                           std::function<void(std::vector<std::string>)> &&cb) {
  client_.command<std::vector<std::string>>(
      "lrange", key, start, stop,
      [cb](sw::redis::Future<std::vector<std::string>> &&fut) {
        try {
          cb(fut.get());
        } catch (const sw::redis::TimeoutError &err) {
          LOG_INFO << "Timed out during LRANGE: " << err.what();
          cb({});
        } catch (const sw::redis::Error &err) {
          LOG_ERROR << "Failed to LRANGE: " << err.what();
          cb({});
        }
      });
}

void SwRedisClient::hGetAll(
    const std::string &key,
    std::function<void(std::vector<std::string>)> &&cb) {
  client_.command<std::vector<std::string>>(
      "hgetall", key, [cb](sw::redis::Future<std::vector<std::string>> &&fut) {
        try {
          cb(fut.get());
        } catch (const sw::redis::TimeoutError &err) {
          LOG_INFO << "Timed out during HGETALL: " << err.what();
          cb({});
        } catch (const sw::redis::Error &err) {
          LOG_ERROR << "Failed to HGETALL: " << err.what();
          cb({});
        }
      });
}

void SwRedisClient::rPush(const std::string &key,
                          const std::vector<std::string> &values,
                          std::function<void(int64_t)> &&cb) {
  // The command interface for a variable number of args requires us to form a
  // container including the command itself.
  std::vector<std::string> command_terms;
  command_terms.reserve(2 + values.size());
  command_terms.emplace_back("rpush");
  command_terms.emplace_back(key);
  command_terms.insert(command_terms.end(), values.begin(), values.end());
  // The underlying Redis library requires us to use long long. This is
  // generally advised against nowadays
  // (https://google.github.io/styleguide/cppguide.html#Integer_Types) but our
  // Redis list sizes shouldn't be exceeding the 16-bit range anyway.
  client_.command<long long>(  // NOLINT(google-runtime-int)
      command_terms.begin(), command_terms.end(),
      [cb](sw::redis::Future<long long> &&fut) {  // NOLINT(google-runtime-int)
        try {
          cb(fut.get());
        } catch (const sw::redis::TimeoutError &err) {
          LOG_INFO << "Timed out during RPUSH: " << err.what();
          cb(0);
        } catch (const sw::redis::Error &err) {
          LOG_ERROR << "Failed to RPUSH: " << err.what();
          cb(0);
        }
      });
}

void SwRedisClient::expire(const std::string &key, int64_t ttl) {
  client_.command<bool>("expire", key, ttl, [](sw::redis::Future<bool> &&fut) {
    try {
      fut.get();
    } catch (const sw::redis::TimeoutError &err) {
      LOG_INFO << "Timed out during EXPIRE: " << err.what();
    } catch (const sw::redis::Error &err) {
      LOG_ERROR << "Failed to EXPIRE: " << err.what();
    }
  });
}

void SwRedisClient::lTrim(const std::string &key, int64_t start, int64_t stop) {
  client_.command<void>(
      "ltrim", key, start, stop, [](sw::redis::Future<void> &&fut) {
        try {
          fut.get();
        } catch (const sw::redis::TimeoutError &err) {
          LOG_INFO << "Timed out during LTRIM: " << err.what();
        } catch (const sw::redis::Error &err) {
          LOG_ERROR << "Failed to LTRIM: " << err.what();
        }
      });
}
}  // namespace delivery
