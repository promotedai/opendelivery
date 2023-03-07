#include <stdint.h>
#include <stdlib.h>

#include <string>
#include <thread>

#include "absl/container/flat_hash_set.h"
#include "config/platform_config.h"
#include "drogon/HttpAppFramework.h"
#include "drogon/HttpResponse.h"
#include "singletons/cache.h"
#include "singletons/config.h"
#include "singletons/counters.h"
#include "singletons/env.h"
#include "singletons/paging.h"
#include "singletons/user_agent.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

int main() {
  const uint16_t port = 9090;
  drogon::app().addListener("0.0.0.0", port);
  drogon::app().setThreadNum(std::thread::hardware_concurrency());
  // If we get near ~1,000 connections then they'll start getting refused
  // because of file handler limits on Linux. Fail more explicitly here. There
  // are ways we can deal with it but I don't want to prematurely optimize.
  // Setting our limit substantially lower to give us emergency buffer.
  drogon::app().setMaxConnectionNum(512);
  // Drogon's default 404 response is some pre-compiled HTML we don't want.
  drogon::app().setCustom404Page(drogon::HttpResponse::newHttpResponse(),
                                 /*set404=*/true);

  // Do this here because we don't ever want to abort once we start listening.
  if (delivery::EnvSingleton::getInstance().getApiKeys().empty()) {
    LOG_FATAL << "No API key specified";
    abort();
  }
  if (delivery::EnvSingleton::getInstance().getKafkaBrokers().empty()) {
    LOG_FATAL << "No Kafka brokers specified";
    abort();
  }
  // The ConfigSingleton constructor will abort if it can't initialize.
  auto platform_config =
      delivery::ConfigSingleton::getInstance().getPlatformConfig();
  // We consider caches a requirement because of how slow these stages may be
  // otherwise.
  delivery::CacheSingleton::getInstance().initializeFeaturesCaches(
      platform_config.feature_store_content_cache_size);
  // The CountersSingleton constructor will abort if it can't initialize.
  delivery::counters::CountersSingleton::getInstance();
  // The PagingSingleton constructor will abort if it can't initialize.
  delivery::PagingSingleton::getInstance();
  // This can take several seconds so just do it now instead of on the first
  // request.
  delivery::UserAgentSingleton::getInstance();

  LOG_INFO << "Starting to listen on port " << port;
  drogon::app().run();
  LOG_INFO << "Stopping listening";

  return 0;
}
