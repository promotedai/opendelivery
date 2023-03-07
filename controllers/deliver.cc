#include "controllers/deliver.h"

#include <google/protobuf/stubs/stringpiece.h>
#include <google/protobuf/util/json_util.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "cloud/aws_personalize_client.h"
#include "cloud/aws_sqs_client.h"
#include "cloud/cloudwatch_monitoring_client.h"
#include "cloud/dynamodb_feature_store_reader.h"
#include "cloud/kafka_delivery_log_writer.h"
#include "config/feature_config.h"
#include "config/platform_config.h"
#include "drogon/HttpAppFramework.h"
#include "drogon/HttpRequest.h"
#include "execution/context.h"
#include "execution/executor.h"
#include "execution/simple_executor.h"
#include "execution/stages/feature_store_client.h"
#include "execution/stages/monitoring_client.h"
#include "execution/stages/personalize_client.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/sqs_client.h"
#include "execution/stages/write_to_delivery_log.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "singletons/aws.h"
#include "singletons/cache.h"
#include "singletons/config.h"
#include "singletons/counters.h"
#include "singletons/env.h"
#include "singletons/feature.h"
#include "singletons/paging.h"
#include "singletons/user_agent.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace delivery {
namespace counters {
struct Caches;
}
}  // namespace delivery

namespace delivery {
void ApiKeyFilter::doFilter(const drogon::HttpRequestPtr &req,
                            drogon::FilterCallback &&queue_response,
                            drogon::FilterChainCallback &&queue_handler) {
  if (delivery::EnvSingleton::getInstance().getApiKeys().contains(
          req->getHeader("x-api-key"))) {
    queue_handler();
  } else {
    LOG_INFO << "Rejecting unauthorized request";
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setStatusCode(drogon::HttpStatusCode::k401Unauthorized);
    queue_response(resp);
  }
}

// This impl should be as minimal as possible. Just keeping Drogon dependencies
// and global state outside of other classes.
void deliverBase(
    std::chrono::steady_clock::time_point begin,
    std::unique_ptr<Context> context,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  // Prepare async response processing.
  context->respond_cb = [callback, begin](const delivery::Response &resp) {
    std::string body;
    google::protobuf::util::MessageToJsonString(resp, &body);
    auto http_resp = drogon::HttpResponse::newHttpResponse();
    http_resp->setBody(std::move(body));
    callback(http_resp);

    LOG_INFO << "Request " << resp.request_id() << " processed in "
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - begin)
                    .count()
             << " ms";
  };
  context->user_agent = UserAgentSingleton::getInstance().parse(
      context->req().device().browser().user_agent());
  // Get necessary configs.
  context->platform_config = ConfigSingleton::getInstance().getPlatformConfig();

  ConfigurationOptions options = {
      .paging_read_redis_client_getter =
          []() {
            return PagingSingleton::getInstance().getPagingReadClient(
                drogon::app().getCurrentThreadIndex());
          },
      .paging_write_redis_client_getter =
          []() {
            return PagingSingleton::getInstance().getPagingClient(
                drogon::app().getCurrentThreadIndex());
          },
      // Hardcode "default" until we have experiments set up.
      .counters_redis_client_getter =
          []() {
            return counters::CountersSingleton::getInstance().getCountersClient(
                "default", drogon::app().getCurrentThreadIndex());
          },
      .feature_store_client_getter =
          [&region = context->platform_config.region]() {
            return std::make_unique<DynamoDBFeatureStoreClient>(
                AwsSingleton::getInstance().getDynamoDBClient(region));
          },
      .personalize_client_getter =
          [&region = context->platform_config.region]() {
            return std::make_unique<AwsPersonalizeClient>(
                AwsSingleton::getInstance().getPersonalizeClient(region));
          },
      .delivery_log_writer_getter =
          []() {
            return std::make_unique<KafkaDeliveryLogWriter>(
                AwsSingleton::getInstance().getKafkaProducer(),
                AwsSingleton::kafka_message_max_bytes);
          },
      .sqs_client_getter =
          [&region = context->platform_config.region,
           &name = context->platform_config.sparse_features_config
                       .stranger_feature_queue_config.queue_name]() {
            return std::make_unique<AwsSqsClient>(
                AwsSingleton::getInstance().getSqsClientAndUrl(region, name));
          },
      .monitoring_client_getter =
          [&region = context->platform_config.region,
           &platform = context->platform_config.name]() {
            return std::make_unique<CloudwatchMonitoringClient>(
                AwsSingleton::getInstance().getCloudwatchClient(region),
                platform);
          },
      .content_features_cache_getter = []() -> FeaturesCache & {
        return CacheSingleton::getInstance().contentFeaturesCache();
      },
      .non_content_features_cache_getter =
          [&req = context->req()]() -> FeaturesCache & {
        return CacheSingleton::getInstance().nonContentFeaturesCache();
      },
      .counters_caches_getter = []() -> counters::Caches & {
        return CacheSingleton::getInstance().countersCaches("default");
      },
      .counters_database =
          counters::CountersSingleton::getInstance().getDatabaseInfo(
              context->platform_config.platform_id, "default"),
      .periodic_time_values =
          &FeatureSingleton::getInstance().getPeriodicTimeValues()};

  std::unique_ptr<Executor> &executor =
      configureSimpleExecutor(std::move(context), options);
  executor->execute();
}

void Deliver::deliver(
    const drogon::HttpRequestPtr &http_req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) const {
  // Keep this to the front.
  auto begin = std::chrono::steady_clock::now();

  // Request processing.
  delivery::Request req;
  google::protobuf::util::JsonStringToMessage(
      google::protobuf::StringPiece(
          http_req->body().data(),
          static_cast<google::protobuf::stringpiece_ssize_type>(
              http_req->body().size())),
      &req);

  auto context = std::make_unique<Context>(std::move(req));
  deliverBase(begin, std::move(context), std::move(callback));
}

void Deliver::echo(
    const drogon::HttpRequestPtr &http_req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) const {
  auto begin = std::chrono::steady_clock::now();

  delivery::Request req;
  google::protobuf::util::JsonStringToMessage(
      google::protobuf::StringPiece(
          http_req->body().data(),
          static_cast<google::protobuf::stringpiece_ssize_type>(
              http_req->body().size())),
      &req);

  auto context = std::make_unique<Context>(std::move(req));
  context->is_echo = true;
  deliverBase(begin, std::move(context), std::move(callback));
}
}  // namespace delivery
