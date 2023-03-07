// This implements the "/healthz" route handler, which is used for health checks.

#pragma once

#include <functional>
#include <memory>

#include "drogon/HttpController.h"
#include "drogon/HttpResponse.h"
#include "drogon/HttpTypes.h"
#include "drogon/drogon_callbacks.h"

namespace delivery {
class Healthz : public drogon::HttpController<Healthz> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(Healthz::healthz, "/healthz", drogon::Get);
  METHOD_LIST_END

  void healthz(
      const drogon::HttpRequestPtr &http_req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback) const;
};
}  // namespace delivery
