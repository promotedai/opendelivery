// This creates a delivery log record out of an execution context and passes it
// to a writer for IO handling.

#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <utility>

#include "execution/stages/stage.h"

namespace event {
class LogRequest;
}

namespace delivery {
struct Context;

class DeliveryLogWriter {
 public:
  virtual ~DeliveryLogWriter() = default;
  virtual void write(const event::LogRequest& log_req) = 0;
};

class WriteToDeliveryLogStage : public Stage {
 public:
  WriteToDeliveryLogStage(
      size_t id, Context& context,
      std::unique_ptr<DeliveryLogWriter> delivery_log_writer)
      : Stage(id),
        context_(context),
        delivery_log_writer_(std::move(delivery_log_writer)) {}
  std::string name() const override { return "WriteToDeliveryLog"; }

  void runSync() override;

 private:
  Context& context_;
  std::unique_ptr<DeliveryLogWriter> delivery_log_writer_;
};
}  // namespace delivery
