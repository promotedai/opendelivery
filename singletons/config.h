// Responsible for abstracting config "creation" details from everyone else.
//
// This is a singleton to eventually act as the owner for additional loading on
// the fly.

#pragma once

#include <gtest/gtest_prod.h>
#include <json/value.h>

#include <memory>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "config/platform_config.h"
#include "singletons/singleton.h"

namespace delivery {
class ConfigSingleton : public Singleton<ConfigSingleton> {
 public:
  // This returns a mutable, deep copy of the mother config. If this turns out
  // too expensive we can do an immutable, shallow "copy".
  PlatformConfig getPlatformConfig() { return mother_; }

 private:
  friend class Singleton;
  FRIEND_TEST(ConfigSingletonTest, ConfigLoader);
  FRIEND_TEST(ConfigSingletonTest, Load);
  FRIEND_TEST(ConfigSingletonTest, ReplaceEnvVar);
  FRIEND_TEST(ConfigSingletonTest, ToJson);

  ConfigSingleton();

  PlatformConfig mother_;

  struct ConfigLoader {
    static std::unique_ptr<ConfigLoader> create(std::string_view path);
    virtual ~ConfigLoader() {}

    virtual std::string load() = 0;
  };

  struct S3ConfigLoader : public ConfigLoader {
    std::string region;
    std::string bucket;
    std::string object_key;

    std::string load() override;
  };

  struct FileConfigLoader : public ConfigLoader {
    std::string name;

    std::string load() override;
  };

  static std::string replaceEnvVar(
      std::string_view config,
      const absl::flat_hash_map<std::string, std::string>& env_vars);

  static Json::Value toJson(const std::string& config);
};
}  // namespace delivery
