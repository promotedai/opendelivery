#include <string>

#include "gtest/gtest.h"
#include "utils/uuid.h"

namespace delivery {
TEST(UuidTest, Define) {
  auto id = uuid();
  EXPECT_EQ(id.length(), 32);
  for (const char& c : id) {
    const bool digit = '0' <= c && c <= '9';
    const bool letter = 'a' <= c && c <= 'f';
    EXPECT_TRUE(digit != letter);
  }
}
}  // namespace delivery
