#include <ext/alloc_traits.h>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "config/personalize_config.h"
#include "execution/stages/personalize_client.h"
#include "execution/stages/read_from_personalize.h"
#include "execution/stages/tests/mock_clients.h"
#include "execution/user_agent.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"

using ::testing::SizeIs;

namespace delivery {
TEST(ReadFromPersonalizeTest, ConvertResults) {
  std::vector<PersonalizeConfig> configs;
  configs.emplace_back().campaign_name = "a";
  configs.emplace_back().campaign_name = "b";
  std::vector<std::vector<PersonalizeResult>> all_results;
  all_results.emplace_back() = {PersonalizeResult{.id = "a1", .score = 10},
                                PersonalizeResult{.id = "a2", .score = 20}};
  all_results.emplace_back() = {PersonalizeResult{.id = "b1", .score = 30}};
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<std::string, std::pair<float, int>>>
      campaign_to_scores_and_ranks;
  convertResults(configs, all_results, campaign_to_scores_and_ranks);

  EXPECT_EQ(campaign_to_scores_and_ranks.size(), 2);
  EXPECT_EQ(campaign_to_scores_and_ranks["a"].size(), 2);
  EXPECT_EQ(campaign_to_scores_and_ranks["a"]["a1"],
            (std::pair<float, int>{10, 1}));
  EXPECT_EQ(campaign_to_scores_and_ranks["a"]["a2"],
            (std::pair<float, int>{20, 2}));
  EXPECT_EQ(campaign_to_scores_and_ranks["b"].size(), 1);
  EXPECT_EQ(campaign_to_scores_and_ranks["b"]["b1"],
            (std::pair<float, int>{30, 1}));
}

TEST(ReadFromPersonalizeTest, Run) {
  bool ran = false;
  bool timed_out = false;
  auto client_ptr = std::make_unique<MockPersonalizeClient>();
  auto& client = *client_ptr;
  std::vector<PersonalizeConfig> configs;
  // This size limit will be used for all configs.
  configs.emplace_back(PersonalizeConfig{
      .campaign_name = "name_a", .campaign_arn = "arn_a", .max_input_size = 2});
  // This config will enable using log user ID.
  configs.emplace_back(PersonalizeConfig{.campaign_name = "name_b_loguserid",
                                         .campaign_arn = "arn_b",
                                         .max_input_size = 1});
  delivery::Request req;
  req.mutable_user_info()->set_user_id("user_id");
  req.mutable_user_info()->set_log_user_id("log_user_id");
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  // This will be trimmed for all configs.
  insertions.emplace_back().set_content_id("c");
  UserAgent user_agent;
  user_agent.app = "app";
  user_agent.os = "os";
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<std::string, std::pair<float, int>>>
      campaign_to_scores_and_ranks;
  ReadFromPersonalizeStage stage(0, std::move(client_ptr), configs, req,
                                 insertions, user_agent,
                                 campaign_to_scores_and_ranks);

  std::vector<PersonalizeResult> results;
  EXPECT_CALL(client, getPersonalizedRanking("arn_a", ::testing::_, SizeIs(2),
                                             "user_id", ::testing::_))
      .WillOnce(testing::InvokeArgument<4>(results));
  EXPECT_CALL(client, getPersonalizedRanking("arn_b", ::testing::_, SizeIs(2),
                                             "log_user_id", ::testing::_))
      .WillOnce(testing::InvokeArgument<4>(results));
  stage.run(
      [ran = &ran]() { *ran = true; },
      [timed_out = &timed_out](const std::chrono::duration<double>&,
                               std::function<void()>&&) { *timed_out = true; });
  EXPECT_TRUE(ran);
  EXPECT_TRUE(timed_out);
}

TEST(ReadFromPersonalizeTest, ShadowTraffic) {
  bool ran = false;
  bool timed_out = false;
  auto client_ptr = std::make_unique<MockPersonalizeClient>();
  auto& client = *client_ptr;
  std::vector<PersonalizeConfig> configs;
  // Only this config should be enabled.
  configs.emplace_back(PersonalizeConfig{.campaign_name = "name_a",
                                         .campaign_arn = "arn_a",
                                         .max_input_size = 2,
                                         .enable_for_shadow_traffic = true});
  configs.emplace_back(PersonalizeConfig{.campaign_name = "name_b_loguserid",
                                         .campaign_arn = "arn_b"});
  delivery::Request req;
  req.mutable_user_info()->set_user_id("user_id");
  req.mutable_user_info()->set_log_user_id("log_user_id");
  req.mutable_client_info()->set_traffic_type(
      common::ClientInfo_TrafficType_SHADOW);
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  UserAgent user_agent;
  user_agent.app = "app";
  user_agent.os = "os";
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<std::string, std::pair<float, int>>>
      campaign_to_scores_and_ranks;
  ReadFromPersonalizeStage stage(0, std::move(client_ptr), configs, req,
                                 insertions, user_agent,
                                 campaign_to_scores_and_ranks);

  std::vector<PersonalizeResult> results;
  EXPECT_CALL(client, getPersonalizedRanking("arn_a", ::testing::_, SizeIs(2),
                                             "user_id", ::testing::_))
      .WillOnce(testing::InvokeArgument<4>(results));
  EXPECT_CALL(client, getPersonalizedRanking("arn_b", ::testing::_, SizeIs(2),
                                             "log_user_id", ::testing::_))
      .Times(0);
  stage.run(
      [ran = &ran]() { *ran = true; },
      [timed_out = &timed_out](const std::chrono::duration<double>&,
                               std::function<void()>&&) { *timed_out = true; });
  EXPECT_TRUE(ran);
  EXPECT_TRUE(timed_out);
}
}  // namespace delivery
