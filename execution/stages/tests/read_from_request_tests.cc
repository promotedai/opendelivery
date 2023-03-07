#include <string>
#include <vector>

#include "execution/context.h"
#include "execution/stages/read_from_request.h"
#include "gtest/gtest.h"
#include "hash_utils/make_hash.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
namespace {
namespace dpf = delivery_private_features;
}

// This is just to reduce boilerplate for many tests about user agent.
class ReadFromRequestTest : public ::testing::Test {
 protected:
  ReadFromRequestStage getStage() {
    return ReadFromRequestStage(0, req_, insertions_, context_);
  }

  delivery::Request req_;
  std::vector<delivery::Insertion> insertions_;
  FeatureContext context_;
};

TEST_F(ReadFromRequestTest, UserAgentMissing) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() = "";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 1);
}

TEST_F(ReadFromRequestTest, UserAgentIosWeb) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) "
      "AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 "
      "Safari/604.1";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_IOS), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WEB), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_IOS_WEB), 1);
}

TEST_F(ReadFromRequestTest, UserAgentIosNative) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "MyApp/1 CFNetwork/808.3 Darwin/16.3.0";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_IOS), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_IOS_APP), 1);
}

TEST_F(ReadFromRequestTest, UserAgentBot) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Mozilla/5.0 (compatible; Googlebot/2.1; "
      "+http://www.google.com/bot.html)";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_BOT), 1);
}

TEST_F(ReadFromRequestTest, UserAgentMacWeb) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
      "(KHTML, like Gecko) Version/14.1 Safari/605.1.15";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WEB), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_MAC_WEB), 1);
}

TEST_F(ReadFromRequestTest, UserAgentChromeWeb) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Mozilla/5.0 (X11; CrOS x86_64 14268.67.0) AppleWebKit/537.36 (KHTML, "
      "like Gecko) Chrome/96.0.4664.111 Safari/537.36";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WEB), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_CHROME_WEB), 1);
}

TEST_F(ReadFromRequestTest, UserAgentWindowsWeb) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WEB), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WINDOWS_WEB), 1);
}

TEST_F(ReadFromRequestTest, UserAgentLinuxWeb) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/51.0.2704.103 Safari/537.36";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WEB), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_LINUX_WEB), 1);
}

TEST_F(ReadFromRequestTest, UserAgentAndroid) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile "
      "Safari/537.36";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_WEB), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_ANDROID), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_ANDROID_WEB), 1);
}

TEST_F(ReadFromRequestTest, UserAgentOkhttp) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() =
      "okhttp/3.12.1";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_ANDROID), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_IS_ANDROID_APP), 1);
}

TEST_F(ReadFromRequestTest, UserAgentStranger) {
  *req_.mutable_device()->mutable_browser()->mutable_user_agent() = "unknown";
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USER_AGENT_MISSING), 0);
  EXPECT_TRUE(
      scope.stranger_feature_paths.contains(user_agent_prefix + "unknown"));
}

TEST_F(ReadFromRequestTest, UserInfo) {
  *req_.mutable_user_info()->mutable_log_user_id() = "a";
  *req_.mutable_user_info()->mutable_user_id() = "b";
  hashlib::HashState state;
  state.updateState(log_user_id_prefix);
  state.updateState(std::string("a"));
  uint64_t id = state.digestState();
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(id), 1);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_HAS_USER_ID), 1);
  EXPECT_TRUE(scope.stranger_feature_paths.contains(log_user_id_prefix + "a"));
}

TEST_F(ReadFromRequestTest, Referrer) {
  *req_.mutable_device()->mutable_browser()->mutable_referrer() = "a";
  hashlib::HashState state;
  state.updateState(referrer_prefix);
  state.updateState(std::string("a"));
  uint64_t id = state.digestState();
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(id), 1);
  EXPECT_TRUE(scope.stranger_feature_paths.contains(referrer_prefix + "a"));
}

TEST_F(ReadFromRequestTest, Placement) {
  req_.mutable_device()->set_device_type(common::DeviceType::MOBILE);
  req_.mutable_paging()->set_size(10);
  req_.mutable_paging()->set_cursor("100");
  req_.set_use_case(delivery::UseCase::CATEGORY_CONTENT);
  getStage().runSync();

  const auto& scope = context_.getRequestFeatures();
  EXPECT_EQ(scope.features.at(dpf::FEATURE_RESPONSE_INSERTION_POSITION), 0);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_DEVICE_TYPE), 2);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_RESPONSE_PAGING_OFFSET), 100);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_RESPONSE_PAGING_SIZE), 10);
  EXPECT_EQ(scope.features.at(dpf::FEATURE_USE_CASE), 7);
}

TEST_F(ReadFromRequestTest, InsertionScope) {
  insertions_.reserve(2);
  auto& insertion_a = insertions_.emplace_back();
  insertion_a.set_content_id("a");
  insertion_a.set_retrieval_score(100);
  insertion_a.set_retrieval_rank(101);
  uint64_t hashed_a = hashlib::makeHash(std::string("a"));
  auto& insertion_b = insertions_.emplace_back();
  insertion_b.set_content_id("b");
  uint64_t hashed_b = hashlib::makeHash(std::string("b"));
  context_.initialize(insertions_);
  getStage().runSync();

  const auto& scope_a = context_.getInsertionFeatures("a");
  EXPECT_EQ(scope_a.features.at(hashed_a), 1);
  EXPECT_EQ(scope_a.features.at(dpf::RETRIEVAL_SCORE), 100);
  EXPECT_EQ(scope_a.features.at(dpf::RETRIEVAL_RANK), 101);
  EXPECT_EQ(scope_a.stranger_feature_paths.at(content_id_prefix + "a"),
            hashed_a);

  const auto& scope_b = context_.getInsertionFeatures("b");
  EXPECT_EQ(scope_b.features.at(hashed_b), 1);
  EXPECT_FALSE(scope_b.features.contains(dpf::RETRIEVAL_SCORE));
  EXPECT_FALSE(scope_b.features.contains(dpf::RETRIEVAL_RANK));
  EXPECT_EQ(scope_b.features.at(dpf::RETRIEVAL_SCORE_MISSING), 1);
  EXPECT_EQ(scope_b.features.at(dpf::RETRIEVAL_RANK_MISSING), 1);
  EXPECT_EQ(scope_b.stranger_feature_paths.at(content_id_prefix + "b"),
            hashed_b);
}
}  // namespace delivery
