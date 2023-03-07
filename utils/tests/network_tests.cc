#include <string>

#include "gtest/gtest.h"
#include "utils/network.h"

namespace delivery {
TEST(NetworkTest, GetIp) { EXPECT_EQ(getIp("localhost"), "127.0.0.1"); }

TEST(NetworkTest, GetIpError) { EXPECT_TRUE(getIp("localhostt").empty()); }

TEST(NetworkTest, ParseRedisUrl) {
  {
    auto structured_url = parseRedisUrl(
        "redis://"
        "prm-prod-metrics-counters-group-ro.x1v9xw.ng.0001.use1.cache."
        "amazonaws.com:6399/0");
    EXPECT_TRUE(structured_url.successful_parse);
    EXPECT_EQ(structured_url.hostname,
              "prm-prod-metrics-counters-group-ro.x1v9xw.ng.0001.use1."
              "cache.amazonaws.com");
    EXPECT_EQ(structured_url.port, "6399");
    EXPECT_EQ(structured_url.database_number, "0");
  }
  // No scheme.
  {
    auto structured_url = parseRedisUrl(
        "prm-prod-metrics-counters-group-ro.x1v9xw.ng.0001.use1.cache."
        "amazonaws.com:6399/8");
    EXPECT_TRUE(structured_url.successful_parse);
    EXPECT_EQ(structured_url.hostname,
              "prm-prod-metrics-counters-group-ro.x1v9xw.ng.0001.use1."
              "cache.amazonaws.com");
    EXPECT_EQ(structured_url.port, "6399");
    EXPECT_EQ(structured_url.database_number, "8");
  }
  // Implicit database number.
  {
    auto structured_url = parseRedisUrl(
        "redis://"
        "prm-prod-metrics-counters-group-ro.x1v9xw.ng.0001.use1.cache."
        "amazonaws.com:6399");
    EXPECT_TRUE(structured_url.successful_parse);
    EXPECT_EQ(structured_url.hostname,
              "prm-prod-metrics-counters-group-ro.x1v9xw.ng.0001.use1."
              "cache.amazonaws.com");
    EXPECT_EQ(structured_url.port, "6399");
    EXPECT_EQ(structured_url.database_number, "0");
  }
  // Bad URL.
  {
    auto structured_url = parseRedisUrl("garbo");
    EXPECT_FALSE(structured_url.successful_parse);
  }
}
}  // namespace delivery
