#include "execution/stages/respond.h"

#include <algorithm>

#include "proto/delivery/delivery.pb.h"
#include "utils/uuid.h"

namespace delivery {
void RespondStage::runSync() {
  resp_.set_request_id(req_.request_id());
  resp_.mutable_paging_info()->set_cursor(
      absl::StrCat(paging_context_.min_position + insertions_.size()));
  for (const auto &insertion : insertions_) {
    auto *resp_insertion = resp_.add_insertion();
    resp_insertion->set_content_id(insertion.content_id());
    resp_insertion->set_position(insertion.position());
    // Truncate the UUID from 32 characters to 20 to save space.
    resp_insertion->set_insertion_id(uuid().substr(0, 20));
  }
  // Clients expect insertions sorted by position (ascending).
  std::sort(resp_.mutable_insertion()->begin(),
            resp_.mutable_insertion()->end(),
            [](const delivery::Insertion &a, const delivery::Insertion &b) {
              return a.position() < b.position();
            });
  respond_cb_(resp_);
}
}  // namespace delivery
