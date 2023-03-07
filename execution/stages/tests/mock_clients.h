// Mock impls for all "client" interfaces. Homed here to avoid updating multiple
// places as those interfaces evolve.

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "execution/stages/feature_store_client.h"
#include "execution/stages/monitoring_client.h"
#include "execution/stages/personalize_client.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/sqs_client.h"
#include "execution/stages/write_to_delivery_log.h"
#include "gmock/gmock.h"

namespace delivery {
class MockRedisClient : public RedisClient {
 public:
  MOCK_METHOD(void, lRange,
              (const std::string&, int64_t, int64_t,
               std::function<void(std::vector<std::string>)>&&),
              (override));
  MOCK_METHOD(void, hGetAll,
              (const std::string&,
               std::function<void(std::vector<std::string>)>&&),
              (override));
  MOCK_METHOD(void, rPush,
              (const std::string&, const std::vector<std::string>&,
               std::function<void(int64_t)>&&),
              (override));
  MOCK_METHOD(void, expire, (const std::string&, int64_t), (override));
  MOCK_METHOD(void, lTrim, (const std::string&, int64_t, int64_t), (override));
};

class MockFeatureStoreClient : public FeatureStoreClient {
 public:
  MOCK_METHOD(void, read,
              (const std::string&, const std::string&, const std::string&,
               const std::string&,
               std::function<void(std::vector<FeatureStoreResult>)>&&),
              (const, override));
  MOCK_METHOD(void, readBatch,
              (const std::string&, const std::string&,
               const std::vector<std::string>&, const std::string&,
               std::function<void(std::vector<FeatureStoreResult>)>&&),
              (const, override));
};

class MockPersonalizeClient : public PersonalizeClient {
 public:
  MOCK_METHOD(void, getPersonalizedRanking,
              (const std::string&, const UserAgent&,
               const std::vector<std::string>&, const std::string&,
               std::function<void(std::vector<PersonalizeResult>)>&&),
              (const, override));
};

class MockDeliveryLogWriter : public DeliveryLogWriter {
 public:
  MOCK_METHOD(void, write, (const event::LogRequest&), (override));
};

class MockSqsClient : public SqsClient {
 public:
  MOCK_METHOD(void, sendMessage, (const std::string&), (const, override));
};

class MockMonitoringClient : public MonitoringClient {
 public:
  MOCK_METHOD(void, write, (const MonitoringData&), (override));
};
}  // namespace delivery
