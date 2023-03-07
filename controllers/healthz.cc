#include "controllers/healthz.h"

namespace delivery {
void Healthz::healthz(
    const drogon::HttpRequestPtr &http_req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) const {
  auto http_resp = drogon::HttpResponse::newHttpResponse();
  http_resp->setContentTypeCode(drogon::CT_TEXT_PLAIN);
  http_resp->setStatusCode(drogon::k200OK);
  http_resp->setBody("ok");

  callback(http_resp);
}
}  // namespace delivery
