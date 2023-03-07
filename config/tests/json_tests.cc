#include <optional>
#include <string>

#include "config/json.h"
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"

namespace delivery {
struct TestInnerConfig {
  struct TestInnerInnerConfig {
    uint64_t test_leaf;

    constexpr static auto properties =
        std::make_tuple(property(&TestInnerInnerConfig::test_leaf, "testLeaf"));
  };
  TestInnerInnerConfig test_inner_inner_config;
  std::optional<uint64_t> test_inner_null;

  constexpr static auto properties = std::make_tuple(
      property(&TestInnerConfig::test_inner_inner_config,
               "testInnerInnerConfig"),
      property(&TestInnerConfig::test_inner_null, "testInnerNull"));
};

struct TestEntryConfig {
  uint64_t test_entry_field;

  constexpr static auto properties = std::make_tuple(
      property(&TestEntryConfig::test_entry_field, "testEntryField"));
};

struct TestInnerMapConfig {
  std::unordered_map<std::string, TestEntryConfig> test_inner_map;

  constexpr static auto properties = std::make_tuple(
      property(&TestInnerMapConfig::test_inner_map, "testInnerMap"));
};

struct TestConfig {
  bool test_bool;
  uint64_t test_uint;
  int64_t test_int;
  double test_float;
  std::string test_string;
  TestInnerConfig test_inner_config;
  std::vector<uint64_t> test_array;
  std::optional<std::string> test_null;
  uint64_t test_default = 100;
  std::unordered_map<std::string, TestInnerMapConfig> test_map;

  constexpr static auto properties = std::make_tuple(
      property(&TestConfig::test_bool, "testBool"),
      property(&TestConfig::test_uint, "testUInt"),
      property(&TestConfig::test_int, "testInt"),
      property(&TestConfig::test_float, "testFloat"),
      property(&TestConfig::test_string, "testString"),
      property(&TestConfig::test_inner_config, "testInnerConfig"),
      property(&TestConfig::test_array, "testArray"),
      property(&TestConfig::test_null, "testNull"),
      property(&TestConfig::test_default, "testDefault"),
      property(&TestConfig::test_map, "testMap"));
};

TEST(JsonTest, Bool) {
  Json::Value value;
  value["testBool"] = true;

  TestConfig config;
  applyJson(config, value);
  EXPECT_EQ(config.test_bool, true);
}

TEST(JsonTest, Number) {
  Json::Value value;
  value["testUInt"] = 123;
  value["testInt"] = -123;
  value["testFloat"] = 8.75;

  TestConfig config;
  applyJson(config, value);
  EXPECT_EQ(config.test_uint, 123);
  EXPECT_EQ(config.test_int, -123);
  EXPECT_EQ(config.test_float, 8.75);
}

TEST(JsonTest, String) {
  Json::Value value;
  value["testString"] = "abc";

  TestConfig config;
  applyJson(config, value);
  EXPECT_EQ(config.test_string, "abc");
}

TEST(JsonTest, Object) {
  Json::Value inner_inner;
  inner_inner["testLeaf"] = 8;
  Json::Value inner;
  inner["testInnerInnerConfig"] = inner_inner;
  Json::Value value;
  value["testInnerConfig"] = inner;

  TestConfig config;
  applyJson(config, value);
  EXPECT_EQ(config.test_inner_config.test_inner_inner_config.test_leaf, 8);
}

TEST(JsonTest, Array) {
  Json::Value value;
  value["testArray"][0] = 10;
  value["testArray"][1] = 11;
  value["testArray"][2] = 12;

  TestConfig config;
  config.test_array.push_back(14);  // Assuming a default value.
  applyJson(config, value);
  EXPECT_THAT(config.test_array, testing::ElementsAre(10, 11, 12));
}

TEST(JsonTest, Null) {
  {
    Json::Value value;
    value["testNull"] = Json::Value(Json::nullValue);

    TestConfig config;
    applyJson(config, value);
    EXPECT_FALSE(config.test_null.has_value());
  }
  {
    Json::Value value;
    value["testNull"] = "abc";

    TestConfig config;
    applyJson(config, value);
    ASSERT_TRUE(config.test_null.has_value());
    EXPECT_EQ(config.test_null, "abc");
  }
}

TEST(JsonTest, Default) {
  {
    Json::Value value;

    TestConfig config;
    applyJson(config, value);
    EXPECT_EQ(config.test_default, 100);
  }
  {
    Json::Value value;
    value["testDefault"] = 101;

    TestConfig config;
    applyJson(config, value);
    EXPECT_EQ(config.test_default, 101);
  }
}

TEST(JsonTest, Map) {
  Json::Value entry;
  entry["testEntryField"] = 7;
  Json::Value inner_map;
  inner_map["inner_key"] = entry;
  Json::Value inner_config;
  inner_config["testInnerMap"] = inner_map;
  Json::Value map;
  map["key"] = inner_config;
  Json::Value value;
  value["testMap"] = map;

  TestConfig config;
  applyJson(config, value);
  EXPECT_EQ(config.test_map["key"].test_inner_map["inner_key"].test_entry_field,
            7);
}

TEST(JsonTest, UnimplementedField) {
  Json::Value value;
  value["unimplementedField"] = 9;

  // Assume things are fine if this doesn't blow up.
  TestConfig config;
  applyJson(config, value);
}

TEST(JsonTest, Iteration) {
  TestConfig config;
  {
    Json::Value value;
    value["testUInt"] = 1;
    value["testNull"] = "abc";

    applyJson(config, value);
    EXPECT_EQ(config.test_uint, 1);
    ASSERT_TRUE(config.test_null.has_value());
    EXPECT_EQ(config.test_null, "abc");
  }
  {
    Json::Value value;
    value["testUInt"] = 2;

    applyJson(config, value);
    EXPECT_EQ(config.test_uint, 2);
    ASSERT_TRUE(config.test_null.has_value());
    EXPECT_EQ(config.test_null, "abc");
  }
  {
    Json::Value value;
    value["testNull"] = Json::Value(Json::nullValue);

    applyJson(config, value);
    EXPECT_EQ(config.test_uint, 2);
    EXPECT_FALSE(config.test_null.has_value());
  }
}

TEST(JsonTest, ParseHelper) {
  Json::Value value;
  value["testBool"] = "TRUE";
  value["testUInt"] = "123";
  value["testInt"] = "-123";
  value["testFloat"] = "8.75";

  TestConfig config;
  applyJson(config, value);
  EXPECT_EQ(config.test_bool, true);
  EXPECT_EQ(config.test_uint, 123);
  EXPECT_EQ(config.test_int, -123);
  EXPECT_EQ(config.test_float, 8.75);
}
}  // namespace delivery
