#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>

#include "cloud/kafka_delivery_log_writer.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/event/event.pb.h"

namespace delivery {
TEST(KafkaDeliveryLogWriter, MakeKey) {
  event::LogRequest log_req;
  log_req.set_platform_id(100);
  log_req.mutable_user_info()->set_log_user_id("jimmy");
  auto key = makeKey(log_req);

  ASSERT_EQ(key->size(), 13);
  EXPECT_EQ(*reinterpret_cast<uint64_t*>(key->data()), 100);
  EXPECT_EQ(std::string_view(key->data() + 8, key->size() - 8), "jimmy");
}

TEST(KafkaDeliveryLogWriter, MakeValue) {
  event::LogRequest log_req;
  // Set an arbitrary field to make the Proto non-empty.
  log_req.set_platform_id(1000);

  EXPECT_NE(makeValue(log_req, 100000000000), nullptr);
  EXPECT_EQ(makeValue(log_req, 1), nullptr);
}
}  // namespace delivery
