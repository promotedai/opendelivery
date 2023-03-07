// Generally don't want these tests depending on the actual singleton instances,
// but the current initialization is annoying enough that I don't want to
// reproduce a test version of it here. All functions for the singleton are
// const, at least.

#include <string>

#include "gtest/gtest.h"
#include "singletons/user_agent.h"

namespace delivery {
TEST(UserAgentTest, Empty) {
  auto user_agent = UserAgentSingleton::getInstance().parse("");
  EXPECT_EQ(user_agent.os, "Other");
  EXPECT_EQ(user_agent.app, "Other");
}

TEST(UserAgentTest, Browser) {
  auto user_agent = UserAgentSingleton::getInstance().parse(
      "Mozilla/5.0 (iPhone; CPU iPhone OS 14_8 like Mac OS X) "
      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 "
      "Safari/604.1");
  EXPECT_EQ(user_agent.os, "iOS");
  EXPECT_EQ(user_agent.app, "Mobile Safari");
}

TEST(UserAgentTest, MobileApp) {
  auto user_agent = UserAgentSingleton::getInstance().parse(
      "Promoted/980 CFNetwork/1240.0.4 Darwin/20.6.0");
  EXPECT_EQ(user_agent.os, "iOS");
  EXPECT_EQ(user_agent.app, "Promoted");
}
}  // namespace delivery
