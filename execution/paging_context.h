// A PagingContext instance is specific to a particular Context (and thus a
// particular request).

#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
struct SeenInfo {
  delivery::Insertion insertion;
  // If false, the above insertion must be ignored on requests.
  // If true, the above insertion must be replaced on requests.
  bool on_curr_page = false;
};

struct PagingContext {
  // The Redis key to use.
  std::string key;

  // Positions are absolute, zero-based, and inclusive.
  int64_t min_position = 0;
  int64_t max_position = 0;

  // This request must allocate insertions to these positions.
  std::vector<int64_t> open_positions;

  // Each entry corresponds to a past allocation.
  absl::flat_hash_map<std::string, SeenInfo> seen_infos;
};
}  // namespace delivery
