// The AWS SDK is designed to be global state, so we don't have much of a choice
// except to make it a singleton.
//
// Kafka isn't part of AWS, but we use it via MSK so it can live here for now.

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/monitoring/CloudWatchClient.h>
#include <aws/personalize-runtime/PersonalizeRuntimeClient.h>
#include <aws/s3/S3Client.h>
#include <aws/sqs/SQSClient.h>

#include "absl/container/flat_hash_map.h"
#include "aws/sqs/model/GetQueueUrlRequest.h"
#include "kafka/KafkaProducer.h"
#include "singletons/env.h"
#include "singletons/singleton.h"

namespace delivery {
class AwsSingleton : public Singleton<AwsSingleton> {
 public:
  Aws::S3::S3Client& getS3Client(std::string_view region) {
    std::unique_lock<std::mutex> guard(s3_mutex_);
    auto& s3_client = region_to_s3_client_[region];
    if (s3_client == nullptr) {
      Aws::Client::ClientConfiguration client_config;
      client_config.region = region;
      client_config.executor = thread_pool_;
      region_to_s3_client_[region] =
          std::make_unique<Aws::S3::S3Client>(client_config);
    }
    return *s3_client;
  }

  const Aws::DynamoDB::DynamoDBClient& getDynamoDBClient(
      std::string_view region) {
    std::unique_lock<std::mutex> guard(dynamodb_mutex_);
    auto& dynamodb_client = region_to_dynamodb_client_[region];
    if (dynamodb_client == nullptr) {
      Aws::Client::ClientConfiguration client_config;
      client_config.region = region;
      client_config.executor = thread_pool_;
      region_to_dynamodb_client_[region] =
          std::make_unique<Aws::DynamoDB::DynamoDBClient>(client_config);
    }
    return *dynamodb_client;
  }

  const Aws::PersonalizeRuntime::PersonalizeRuntimeClient& getPersonalizeClient(
      std::string_view region) {
    std::unique_lock<std::mutex> guard(personalize_mutex_);
    auto& personalize_client = region_to_personalize_client_[region];
    if (personalize_client == nullptr) {
      Aws::Client::ClientConfiguration client_config;
      client_config.region = region;
      client_config.executor = thread_pool_;
      region_to_personalize_client_[region] =
          std::make_unique<Aws::PersonalizeRuntime::PersonalizeRuntimeClient>(
              client_config);
    }
    return *personalize_client;
  }

  // Our infra currently supplies an SQS queue name instead of a URL. To avoid
  // the latency of this lookup, we cache the names and treat them similarly to
  // the client.
  std::pair<const Aws::SQS::SQSClient&, const std::string&> getSqsClientAndUrl(
      std::string_view region, const std::string& name) {
    std::unique_lock<std::mutex> guard(sqs_mutex_);
    auto& sqs_client = region_to_sqs_client_[region];
    if (sqs_client == nullptr) {
      Aws::Client::ClientConfiguration client_config;
      client_config.region = region;
      client_config.executor = thread_pool_;
      region_to_sqs_client_[region] =
          std::make_unique<Aws::SQS::SQSClient>(client_config);
    }
    auto& url = sqs_name_to_url_[name];
    if (url.empty()) {
      Aws::SQS::Model::GetQueueUrlRequest req;
      req.SetQueueName(name);
      // This could fail, though only really will when our configs are messed
      // up. Need to make this more resilient to support live config loading.
      url = sqs_client->GetQueueUrl(req).GetResult().GetQueueUrl();
    }
    return {*sqs_client, url};
  }

  Aws::CloudWatch::CloudWatchClient& getCloudwatchClient(
      std::string_view region) {
    std::unique_lock<std::mutex> guard(cloudwatch_mutex_);
    auto& cloudwatch_client = region_to_cloudwatch_client_[region];
    if (cloudwatch_client == nullptr) {
      Aws::Client::ClientConfiguration client_config;
      client_config.region = region;
      client_config.executor = thread_pool_;
      region_to_cloudwatch_client_[region] =
          std::make_unique<Aws::CloudWatch::CloudWatchClient>(client_config);
    }
    return *cloudwatch_client;
  }

  inline static const size_t kafka_message_max_bytes = 1'048'588;

  kafka::clients::KafkaProducer& getKafkaProducer() { return *kafka_producer_; }

 private:
  friend class Singleton;

  AwsSingleton() {
    Aws::InitAPI(options_);

    // The AWS SDK has a memory leak and data race each time it creates a
    // thread. The default behavior of the SDK is to spin up a new thread for
    // every request but the aforementioned issues make that unacceptable. We
    // set up a thread pool for the SDK to avoid those issues rather than for
    // performance reasons.
    thread_pool_ =
        std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(
            std::thread::hardware_concurrency());

    // This is about as default of a working setup as I could get. Some notes
    // for future development:
    // - modern-cpp-kafka doesn't know about our event loops, and instead has
    // its own thread pool to support async. There will probably be enough load
    // that this is justifiable, but it could make sense to decrease our number
    // of event loops instead of using hardware concurrency. If we're really
    // desperate, we can also throw it away and make a client on top of
    // librdkafka using our event loops.
    // - This defaults to acks=all. Could set acks=1 or even acks=0 if we're
    // willing to accept some message loss.
    // - I don't know if any buffering is going on. We could buffer messages in
    // our producer-user even if modern-cpp-kafka can't do it.
    const kafka::Properties props({
        {"bootstrap.servers", EnvSingleton::getInstance().getKafkaBrokers()},
        {"compression.codec", "gzip"},
        {"message.max.bytes", absl::StrCat(kafka_message_max_bytes)},
        {"message.timeout.ms", "3000"},
        // We require TLS encryption, but we don't require authentication
        // currently.
        {"security.protocol", "ssl"},
        {"enable.ssl.certificate.verification", "false"},
    });
    kafka_producer_ = std::make_unique<kafka::clients::KafkaProducer>(props);
  }

  ~AwsSingleton() { Aws::ShutdownAPI(options_); }

  Aws::SDKOptions options_;
  std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> thread_pool_;
  std::mutex s3_mutex_;
  absl::flat_hash_map<std::string, std::unique_ptr<Aws::S3::S3Client>>
      region_to_s3_client_;
  std::mutex dynamodb_mutex_;
  absl::flat_hash_map<std::string,
                      std::unique_ptr<Aws::DynamoDB::DynamoDBClient>>
      region_to_dynamodb_client_;
  std::mutex personalize_mutex_;
  absl::flat_hash_map<
      std::string,
      std::unique_ptr<Aws::PersonalizeRuntime::PersonalizeRuntimeClient>>
      region_to_personalize_client_;
  std::mutex sqs_mutex_;
  absl::flat_hash_map<std::string, std::unique_ptr<Aws::SQS::SQSClient>>
      region_to_sqs_client_;
  absl::flat_hash_map<std::string, std::string> sqs_name_to_url_;
  std::mutex cloudwatch_mutex_;
  absl::flat_hash_map<std::string,
                      std::unique_ptr<Aws::CloudWatch::CloudWatchClient>>
      region_to_cloudwatch_client_;

  std::unique_ptr<kafka::clients::KafkaProducer> kafka_producer_;
};
}  // namespace delivery
