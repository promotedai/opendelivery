#define DROGON_TEST_MAIN

#include <stdlib.h>

#include <future>
#include <memory>
#include <string>
#include <thread>

#include "drogon/HttpAppFramework.h"
#include "drogon/HttpClient.h"
#include "drogon/HttpRequest.h"
#include "drogon/HttpResponse.h"
#include "drogon/HttpTypes.h"
#include "drogon/drogon_test.h"
#include "trantor/net/EventLoop.h"

using ::drogon::app;
using ::drogon::Get;
using ::drogon::HttpClient;
using ::drogon::HttpRequest;
using ::drogon::HttpResponsePtr;
using ::drogon::k200OK;
using ::drogon::k401Unauthorized;
using ::drogon::k404NotFound;
using ::drogon::Post;
using ::drogon::ReqResult;

const std::string api_key = "abc";
const std::string config_path =
    "file:" + std::string(TEST_DATA_DIR) + "/test.json";

DROGON_TEST(ControllersTest) {
  auto client = HttpClient::newHttpClient("http://127.0.0.1:8018");

  {
    {
      auto req = HttpRequest::newHttpRequest();
      req->setMethod(Get);
      req->setPath("/healthz");
      client->sendRequest(
          req, [TEST_CTX](ReqResult res, const HttpResponsePtr& resp) {
            REQUIRE(res == ReqResult::Ok);
            REQUIRE(resp != nullptr);
            CHECK(resp->getStatusCode() == k200OK);
          });
    }

    {
      auto req = HttpRequest::newHttpRequest();
      req->setMethod(Post);
      req->setPath("/deliver");
      req->addHeader("x-api-key", api_key);
      client->sendRequest(
          req, [TEST_CTX](ReqResult res, const HttpResponsePtr& resp) {
            REQUIRE(res == ReqResult::Ok);
            REQUIRE(resp != nullptr);
            CHECK(resp->getStatusCode() == k200OK);
          });
    }

    {
      auto req = HttpRequest::newHttpRequest();
      req->setMethod(Post);
      req->setPath("/echo");
      req->addHeader("x-api-key", api_key);
      client->sendRequest(
          req, [TEST_CTX](ReqResult res, const HttpResponsePtr& resp) {
            REQUIRE(res == ReqResult::Ok);
            REQUIRE(resp != nullptr);
            CHECK(resp->getStatusCode() == k200OK);
          });
    }

    {
      auto req = HttpRequest::newHttpRequest();
      req->setMethod(Post);
      req->setPath("/deliver");
      req->addHeader("x-api-key", "abd");
      client->sendRequest(
          req, [TEST_CTX](ReqResult res, const HttpResponsePtr& resp) {
            REQUIRE(res == ReqResult::Ok);
            REQUIRE(resp != nullptr);
            CHECK(resp->getStatusCode() == k401Unauthorized);
          });
    }

    {
      auto req = HttpRequest::newHttpRequest();
      req->setMethod(Post);
      req->setPath("/deeliver");
      req->addHeader("x-api-key", api_key);
      client->sendRequest(
          req, [TEST_CTX](ReqResult res, const HttpResponsePtr& resp) {
            REQUIRE(res == ReqResult::Ok);
            REQUIRE(resp != nullptr);
            CHECK(resp->getStatusCode() == k404NotFound);
          });
    }
  }
}

int main(int argc, char** argv) {
  setenv("API_KEY", api_key.data(), true);
  setenv("CONFIG_PATHS", config_path.data(), true);

  app().addListener("0.0.0.0", 8018);

  // Boilerplate to run the server in another thread.
  std::promise<void> p1;
  std::future<void> f1 = p1.get_future();

  std::thread thr([&]() {
    // Queues the promise to be fulfilled after starting the loop.
    app().getLoop()->queueInLoop([&p1]() { p1.set_value(); });
    app().run();
  });

  // The future is only satisfied after the event loop started.
  f1.get();
  int status = drogon::test::run(argc, argv);

  // Ask the event loop to shutdown and wait.
  app().getLoop()->queueInLoop([]() { app().quit(); });
  thr.join();

  unsetenv("CONFIG_PATHS");
  unsetenv("API_KEY");
  return status;
}
