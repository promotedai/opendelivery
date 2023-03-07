#include <stdint.h>

#include <memory>

#include "gtest/gtest.h"
#include "utils/time.h"

namespace delivery {
TEST(TimeTest, MillisSinceEpoch) {
  uint64_t ms = millisSinceEpoch();
  // This should verify that we aren't using steady_clock and that the returned
  // units are millis. Dates are from Nov 2022 to Nov 2072.
  EXPECT_GT(ms, 1669529611000);
  EXPECT_LT(ms, 3247452811000);
}

TEST(TimeTest, MillisForDuration) {
  uint64_t ms = millisForDuration();
  // This should verify that we aren't using system_clock. steady_clock's epoch
  // is not 1970 so its range of values shouldn't intersect with system_clock's
  // modern values.
  EXPECT_LT(ms, 1669529611000);
}

TEST(TimeTest, MakeTimedKey) {
  EXPECT_EQ(makeTimedKey("base", 0),
            makeTimedKey("base", millis_in_15_min - 1));
  EXPECT_NE(makeTimedKey("base", 0), makeTimedKey("base", millis_in_15_min));
}
}  // namespace delivery
