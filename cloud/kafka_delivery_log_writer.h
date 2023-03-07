// Asynchronously produces a Kafka message for a given delivery log request.

#pragma once

#include <stddef.h>

#include <memory>
#include <string>

#include "execution/stages/write_to_delivery_log.h"

namespace event {
class LogRequest;
}

namespace kafka {
namespace clients {
class KafkaProducer;
}
}  // namespace kafka

namespace delivery {
// Available here for testing.
std::shared_ptr<std::string> makeKey(const event::LogRequest& log_req);
std::shared_ptr<std::string> makeValue(const event::LogRequest& log_req,
                                       size_t message_max_bytes);

class KafkaDeliveryLogWriter : public DeliveryLogWriter {
 public:
  // message_max_bytes is technically available via the producer, but it's in
  // string form and stored inside a map.
  explicit KafkaDeliveryLogWriter(kafka::clients::KafkaProducer& producer,
                                  size_t message_max_bytes);
  void write(const event::LogRequest& log_req) override;

 private:
  kafka::clients::KafkaProducer& producer_;
  size_t message_max_bytes_;
};
}  // namespace delivery
