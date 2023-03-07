// This stage is responsible for any necessary processing of a response, and for
// passing that response into the ordained callback.

#pragma once

#include <stddef.h>

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "execution/paging_context.h"
#include "execution/stages/stage.h"

namespace delivery {
class Insertion;
class Request;
class Response;
}  // namespace delivery

namespace delivery {
class RespondStage : public Stage {
 public:
  RespondStage(size_t id, const delivery::Request &req,
               const PagingContext &paging_context,
               const std::vector<delivery::Insertion> &insertions,
               delivery::Response &resp,
               std::function<void(const delivery::Response &)> &&respond_cb)
      : Stage(id),
        req_(req),
        paging_context_(paging_context),
        insertions_(insertions),
        resp_(resp),
        respond_cb_(std::move(respond_cb)) {}

  std::string name() const override { return "Respond"; }

  void runSync() override;

 private:
  const delivery::Request &req_;
  const PagingContext &paging_context_;
  const std::vector<delivery::Insertion> &insertions_;
  delivery::Response &resp_;
  std::function<void(const delivery::Response &)> respond_cb_;
};
}  // namespace delivery
