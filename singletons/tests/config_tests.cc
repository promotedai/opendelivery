#include <json/value.h>

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "singletons/config.h"

using ::testing::HasSubstr;

namespace delivery {
// Fixture just for friending.
class ConfigSingletonTest : public ::testing::Test {};

TEST_F(ConfigSingletonTest, ConfigLoader) {
  // s3.
  {
    auto config_loader = ConfigSingleton::ConfigLoader::create(
        "s3:us-east-1:promoted-configs/configs/mymarket/dev.json");
    auto* s3_loader =
        dynamic_cast<ConfigSingleton::S3ConfigLoader*>(config_loader.get());
    ASSERT_NE(s3_loader, nullptr);
    EXPECT_EQ(s3_loader->region, "us-east-1");
    EXPECT_EQ(s3_loader->bucket, "promoted-configs");
    EXPECT_EQ(s3_loader->object_key, "configs/mymarket/dev.json");
  }
  // file.
  {
    auto config_loader =
        ConfigSingleton::ConfigLoader::create("file:good.json");
    auto* file_loader =
        dynamic_cast<ConfigSingleton::FileConfigLoader*>(config_loader.get());
    ASSERT_NE(file_loader, nullptr);
    EXPECT_EQ(file_loader->name, "good.json");
  }
}

TEST_F(ConfigSingletonTest, Load) {
  // file.
  {
    ConfigSingleton::FileConfigLoader loader;
    loader.name = std::string(TEST_DATA_DIR) + "/test.json";
    auto config = loader.load();
    EXPECT_THAT(config, HasSubstr(R"("platformId": 2)"));
    EXPECT_THAT(config, HasSubstr(R"("region": "a")"));
    EXPECT_THAT(config, HasSubstr(R"("name": "b")"));
  }
  {
    ConfigSingleton::FileConfigLoader loader;
    loader.name = "nonexistent";
    auto config = loader.load();
    EXPECT_EQ(config, "");
  }
}

TEST_F(ConfigSingletonTest, ReplaceEnvVar) {
  auto config = R"(
      {
        "platformId": "{{.PLATFORM_ID}}",
        "region": "{{.REGION}}? yes, {{.REGION}}",
        "name": "b"
      }
    )";
  absl::flat_hash_map<std::string, std::string> env_vars{{"PLATFORM_ID", "2"},
                                                         {"REGION", "a"}};
  EXPECT_EQ(ConfigSingleton::replaceEnvVar(config, env_vars), R"(
      {
        "platformId": "2",
        "region": "a? yes, a",
        "name": "b"
      }
    )");
}

TEST_F(ConfigSingletonTest, ToJson) {
  {
    auto config = R"(
      {
        "platformId": 2,
        "region": "a",
        "name": "b"
      }
    )";
    auto json = ConfigSingleton::toJson(config);
    ASSERT_NE(json, Json::Value::nullSingleton());
    EXPECT_EQ(json["platformId"], 2);
    EXPECT_EQ(json["region"], "a");
    EXPECT_EQ(json["name"], "b");
  }
  {
    auto config = R"(nonesense)";
    auto json = ConfigSingleton::toJson(config);
    EXPECT_EQ(json, Json::Value::nullSingleton());
  }
}
}  // namespace delivery
