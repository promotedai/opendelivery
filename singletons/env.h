// The environment is effectively a singleton. Environment variables are
// accessed in C++ as char*s so this provides structured accessors.

#pragma once

#include <gtest/gtest_prod.h>
#include <unistd.h>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_split.h"
#include "singletons/singleton.h"

// This isn't portable but we don't care right now and there doesn't seem to be
// an alternative which is, other than explicitly listing all of the ones we
// might care about somewhere.
extern char** environ;

namespace delivery {
class EnvSingleton : public Singleton<EnvSingleton> {
 public:
  const absl::flat_hash_set<std::string>& getApiKeys() { return api_keys_; }

  const std::vector<std::string>& getConfigPaths() { return config_paths_; }

  const std::string& getKafkaBrokers() { return kafka_brokers_; }

  const absl::flat_hash_map<std::string, std::string>& getAllVars() {
    return all_vars_;
  };

 private:
  friend class Singleton;
  FRIEND_TEST(EnvSingletonTest, ParseApiKeys);
  FRIEND_TEST(EnvSingletonTest, ParseAllVars);

  EnvSingleton()
      : api_keys_(parseApiKeys(std::getenv("API_KEY"))),
        all_vars_(parseAllVars(environ)) {
    if (const char* config_paths = std::getenv("CONFIG_PATHS")) {
      config_paths_ = absl::StrSplit(config_paths, ',');
    }
    if (const char* kafka_brokers = std::getenv("EVENT_KAFKA_BROKERS")) {
      kafka_brokers_ = kafka_brokers;
    }
  }

  // Multiple keys are separated by commas. Empty keys are ignored.
  static absl::flat_hash_set<std::string> parseApiKeys(const char* api_keys) {
    absl::flat_hash_set<std::string> ret;
    if (api_keys != nullptr) {
      for (const auto& api_key : absl::StrSplit(api_keys, ',')) {
        if (!api_key.empty()) {
          ret.emplace(api_key);
        }
      }
    }
    return ret;
  }

  static absl::flat_hash_map<std::string, std::string> parseAllVars(
      char** kvs) {
    absl::flat_hash_map<std::string, std::string> ret;
    for (size_t i = 0; kvs[i] != nullptr; ++i) {
      // environ promises no extra =s.
      ret.insert(absl::StrSplit(kvs[i], '='));
    }
    return ret;
  }

  absl::flat_hash_set<std::string> api_keys_;
  absl::flat_hash_map<std::string, std::string> all_vars_;
  // Order matters because later configs can override earlier ones.
  std::vector<std::string> config_paths_;
  // We don't want to split this because librdkafka takes a CSV list.
  std::string kafka_brokers_;
};
}  // namespace delivery
