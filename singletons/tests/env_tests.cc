#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "singletons/env.h"

namespace delivery {
// Fixture just for friending.
class EnvSingletonTest : public ::testing::Test {};

TEST_F(EnvSingletonTest, ParseApiKeys) {
  {
    auto keys = delivery::EnvSingleton::parseApiKeys("");
    EXPECT_EQ(keys.size(), 0);
  }
  {
    auto keys = delivery::EnvSingleton::parseApiKeys("apple");
    EXPECT_EQ(keys.size(), 1);
    EXPECT_TRUE(keys.contains("apple"));
  }
  {
    auto keys = delivery::EnvSingleton::parseApiKeys("apple,banana");
    EXPECT_EQ(keys.size(), 2);
    EXPECT_TRUE(keys.contains("apple"));
    EXPECT_TRUE(keys.contains("banana"));
  }
}

TEST_F(EnvSingletonTest, ParseAllVars) {
  std::vector<std::string> vars{"A=B", "C=D"};
  char* test_environ[vars.size() + 1];
  for (size_t i = 0; i < vars.size(); ++i) {
    test_environ[i] = vars[i].data();
  }
  test_environ[vars.size()] = nullptr;
  auto parsed_vars = delivery::EnvSingleton::parseAllVars(test_environ);
  ASSERT_EQ(parsed_vars.size(), 2);
  EXPECT_EQ(parsed_vars["A"], "B");
  EXPECT_EQ(parsed_vars["C"], "D");
}
}  // namespace delivery
