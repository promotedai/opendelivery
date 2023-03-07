// This defines the shape of all executions corresponding to this config.

#pragma once

#include <string>
#include <vector>

#include "config/json.h"

namespace delivery {
struct StageSpec {
  std::string type;
  uint64_t id = 0;
  std::vector<uint64_t> input_ids;

  constexpr static auto properties = std::make_tuple(
      property(&StageSpec::type, "type"), property(&StageSpec::id, "id"),
      property(&StageSpec::input_ids, "inputIds"));
};

struct ExecutionConfig {
  std::vector<StageSpec> stages;

  constexpr static auto properties =
      std::make_tuple(property(&ExecutionConfig::stages, "stages"));
};
}  // namespace delivery
