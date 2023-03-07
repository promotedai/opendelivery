#include "cloud/kafka_delivery_log_writer.h"

#include <stdint.h>

#include <cstring>

#include "absl/strings/str_cat.h"
#include "kafka/Error.h"
#include "kafka/KafkaProducer.h"
#include "kafka/ProducerRecord.h"
#include "kafka/Types.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/execution.pb.h"
#include "proto/event/event.pb.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace kafka {
namespace clients {
namespace producer {
class RecordMetadata;
}
}  // namespace clients
}  // namespace kafka

const std::string topic = "tracking.event.log-request";

namespace delivery {
std::shared_ptr<std::string> makeKey(const event::LogRequest& log_req) {
  // Arbitrary 8 first characters to then overwrite.
  auto key = std::make_shared<std::string>(
      absl::StrCat("01234567", log_req.user_info().log_user_id()));
  uint64_t platform_id = log_req.platform_id();
  // Assume x86 and thus memory is little-endian.
  std::memcpy(key->data(), &platform_id, sizeof(platform_id));
  return key;
}

std::shared_ptr<std::string> makeValue(const event::LogRequest& log_req,
                                       size_t message_max_bytes) {
  auto value = std::make_shared<std::string>(log_req.SerializeAsString());
  if (value->size() > message_max_bytes) {
    return nullptr;
  }
  return value;
}

KafkaDeliveryLogWriter::KafkaDeliveryLogWriter(
    kafka::clients::KafkaProducer& producer, size_t message_max_bytes)
    : producer_(producer), message_max_bytes_(message_max_bytes) {}

void KafkaDeliveryLogWriter::write(const event::LogRequest& log_req) {
  std::shared_ptr<std::string> key = makeKey(log_req);
  std::shared_ptr<std::string> value = makeValue(log_req, message_max_bytes_);
  if (value == nullptr) {
    LOG_ERROR << "Not writing to delivery log for request "
              << log_req.delivery_log(0).request().request_id()
              << " because too large";
    return;
  }

  auto record = kafka::clients::producer::ProducerRecord(
      topic, kafka::Key(key->c_str(), key->size()),
      kafka::Value(value->c_str(), value->size()));
  producer_.send(record,
                 // Capture key and value so they live through retries.
                 [key, value](const kafka::clients::producer::RecordMetadata&,
                              const kafka::Error& error) {
                   if (error) {
                     LOG_ERROR << "Writing to delivery log failed: "
                               << error.message();
                   }
                 });
}
}  // namespace delivery
