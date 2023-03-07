// This stage is responsible for all initial processing of a request before
// other stages can run.

#pragma once

#include <stddef.h>

#include <string>

#include "execution/stages/stage.h"

namespace delivery {
class Context;

class InitStage : public Stage {
 public:
  InitStage(size_t id, Context& context) : Stage(id), context_(context) {}

  std::string name() const override { return "Init"; }

  void runSync() override;

 private:
  Context& context_;
};
}  // namespace delivery
