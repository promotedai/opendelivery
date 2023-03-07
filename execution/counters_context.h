// A CountersContext instance is specific to a particular Context (and thus a
// particular request).

#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "proto/delivery/delivery.pb.h"
#include "utils/time.h"

namespace delivery {
struct CountersContext {
  // Intermediate count values for passing data between stages. Keys are feature
  // IDs.
  absl::flat_hash_map<uint64_t, uint64_t> global_counts;
  absl::flat_hash_map<uint64_t, uint64_t> user_counts;
  absl::flat_hash_map<uint64_t, uint64_t> log_user_counts;
  absl::flat_hash_map<uint64_t, uint64_t> last_user_query;
  absl::flat_hash_map<uint64_t, uint64_t> last_log_user_query;
  absl::flat_hash_map<uint64_t, uint64_t> query_counts;
  // Outer keys are content IDs. Inner keys are feature IDs.
  absl::flat_hash_map<std::string, absl::flat_hash_map<uint64_t, uint64_t>>
      content_counts;
  absl::flat_hash_map<std::string, absl::flat_hash_map<uint64_t, uint64_t>>
      content_query_counts;
  absl::flat_hash_map<std::string, absl::flat_hash_map<uint64_t, uint64_t>>
      last_user_event;
  absl::flat_hash_map<std::string, absl::flat_hash_map<uint64_t, uint64_t>>
      last_log_user_event;
};
}  // namespace delivery
