// This implements the "/deliver" route handler, which is the primary endpoint
// for the delivery service.

#pragma once

#include <functional>
#include <string>

#include "drogon/HttpController.h"
#include "drogon/HttpFilter.h"
#include "drogon/HttpResponse.h"
#include "drogon/HttpTypes.h"
#include "drogon/drogon_callbacks.h"

namespace delivery {
class ApiKeyFilter : public drogon::HttpFilter<ApiKeyFilter> {
 public:
  void doFilter(const drogon::HttpRequestPtr &req,
                drogon::FilterCallback &&queue_response,
                drogon::FilterChainCallback &&queue_handler) override;
};

class Deliver : public drogon::HttpController<Deliver> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(Deliver::deliver, "/deliver", drogon::Post,
                "delivery::ApiKeyFilter");
  // This is the same as the /deliver handler, except traffic is marked as
  // to not produce delivery logs. This is to be removed post-migration.
  ADD_METHOD_TO(Deliver::echo, "/echo", drogon::Post, "delivery::ApiKeyFilter");
  METHOD_LIST_END

  void deliver(
      const drogon::HttpRequestPtr &http_req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback) const;
  void echo(
      const drogon::HttpRequestPtr &http_req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback) const;
};
}  // namespace delivery
