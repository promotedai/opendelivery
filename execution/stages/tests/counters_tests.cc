#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
#include "execution/counters_context.h"
#include "execution/stages/cache.h"
#include "execution/stages/counters.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/tests/mock_clients.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "proto/delivery/private/features/features.pb.h"
#include "utils/time.h"

namespace delivery {
namespace counters {
// Parsing is implemented as class functions to simplify param-passing. This is
// a convenience fixture to default all of the stage fields we don't care about
// for parsing.
class CountersParsingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_.name = "test";
    table_.key_label_map = {{fid_key_label, 0}};
    user_agent_table_.name = "user_agent_test";
    user_agent_table_.key_label_map = {
        {os_key_label, 0}, {app_key_label, 1}, {fid_key_label, 2}};
  }

  ReadFromCountersStage getStageForParsing(const UserAgent& user_agent) {
    auto client = std::make_unique<MockRedisClient>();
    client_ = client.get();
    return ReadFromCountersStage(0, std::move(client), caches_, database_, 0,
                                 req_, insertions_, 2000, user_agent, context_);
  }

  TableInfo table_;
  TableInfo user_agent_table_;
  MockRedisClient* client_;
  Caches caches_;
  DatabaseInfo database_;
  delivery::Request req_;
  std::vector<delivery::Insertion> insertions_;
  CountersContext context_;
};

TEST_F(CountersParsingTest, ParseCountsEmpty) {
  std::vector<std::string> data;
  UserAgent user_agent;
  auto counts = getStageForParsing(user_agent).parseCounts(data, table_);
  EXPECT_TRUE(counts.empty());
}

TEST_F(CountersParsingTest, ParseCountsNonEmpty) {
  std::vector<std::string> data = {// KV1.
                                   "1056840", "452905",
                                   // KV2.
                                   "1056872", "2398"};
  UserAgent user_agent;
  table_.feature_ids = {1056840};
  auto counts = getStageForParsing(user_agent).parseCounts(data, table_);
  EXPECT_EQ(counts.size(), 1);
  EXPECT_EQ(counts[1056840], 452905);
}

TEST_F(CountersParsingTest, ParseCountsUserAgentMatch) {
  std::vector<std::string> data = {
      // KV1.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056802"), "970535",
      // KV2.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056804"), "16014860",
      // KV3.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056806"), "107944800",
      // KV4.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056808"), "474180062",
      // KV5.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056834"), "102700",
      // KV6.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056836"), "1747185",
      // KV7.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056838"), "10115930",
      // KV8.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056840"), "43375158"};
  UserAgent user_agent;
  user_agent.os = "Other";
  user_agent.app = "Other";
  user_agent_table_.feature_ids = {1056806, 1056808, 1056838, 1056840};
  auto counts =
      getStageForParsing(user_agent).parseCounts(data, user_agent_table_);
  EXPECT_EQ(counts.size(), 8);
  // Check one of the segmented counts.
  EXPECT_EQ(counts[1056808], 474180062);
  // Check one of the aggregated counts.
  EXPECT_EQ(counts[1572904], 474180062);
}

TEST_F(CountersParsingTest, ParseCountsUserAgentDoesntMatch) {
  std::vector<std::string> data = {
      // KV1.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056802"), "970535",
      // KV2.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056804"), "16014860",
      // KV3.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056806"), "107944800",
      // KV4.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056808"), "474180062",
      // KV5.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056834"), "102700",
      // KV6.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056836"), "1747185",
      // KV7.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056838"), "10115930",
      // KV8.
      absl::StrCat("Other", "\x1f", "Other", "\x1f", "1056840"), "43375158"};
  UserAgent user_agent;
  user_agent_table_.feature_ids = {1056806, 1056808, 1056838, 1056840};
  auto counts =
      getStageForParsing(user_agent).parseCounts(data, user_agent_table_);
  EXPECT_EQ(counts.size(), 4);
  // Should only be aggregated counts.
  EXPECT_FALSE(counts.contains(1056808));
  EXPECT_EQ(counts[1572904], 474180062);
}

TEST_F(CountersParsingTest, ParseCountsMalformedKey) {
  std::vector<std::string> data = {"z1056840", "452905"};
  UserAgent user_agent;
  table_.feature_ids = {1056840};
  auto stage = getStageForParsing(user_agent);
  auto counts = stage.parseCounts(data, table_);
  EXPECT_TRUE(counts.empty());
  EXPECT_EQ(stage.errors().size(), 1);
}

TEST_F(CountersParsingTest, ParseCountsMalformedValue) {
  std::vector<std::string> data = {"1056840", "z452905"};
  UserAgent user_agent;
  table_.feature_ids = {1056840};
  auto stage = getStageForParsing(user_agent);
  auto counts = stage.parseCounts(data, table_);
  EXPECT_TRUE(counts.empty());
  EXPECT_EQ(stage.errors().size(), 1);
}

TEST_F(CountersParsingTest, ParseLastUserEmpty) {
  std::vector<std::string> data;
  UserAgent user_agent;
  auto counts = getStageForParsing(user_agent).parseLastUser(data, table_);
  EXPECT_TRUE(counts.empty());
}

TEST_F(CountersParsingTest, ParseLastUserNonEmpty) {
  std::vector<std::string> data = {// KV1.
                                   "1335296", "500",
                                   // KV2.
                                   "1548288", "600",
                                   // KV3.
                                   "4", "30"};
  UserAgent user_agent;
  table_.feature_ids = {1335296, 1548288, 4};
  auto counts = getStageForParsing(user_agent).parseLastUser(data, table_);
  EXPECT_EQ(counts.size(), 3);
  EXPECT_EQ(counts[1335296], 1500);
  EXPECT_EQ(counts[1548288], 1400);
  EXPECT_EQ(counts[4], 30);
}

TEST_F(CountersParsingTest, ParseLastUserMalformedKey) {
  std::vector<std::string> data = {"z1056840", "452905"};
  UserAgent user_agent;
  table_.feature_ids = {1056840};
  auto stage = getStageForParsing(user_agent);
  auto counts = stage.parseLastUser(data, table_);
  EXPECT_TRUE(counts.empty());
  EXPECT_EQ(stage.errors().size(), 1);
}

TEST_F(CountersParsingTest, ParseLastUserMalformedValue) {
  std::vector<std::string> data = {"1056840", "z452905"};
  UserAgent user_agent;
  table_.feature_ids = {1056840};
  auto stage = getStageForParsing(user_agent);
  auto counts = stage.parseLastUser(data, table_);
  EXPECT_TRUE(counts.empty());
  EXPECT_EQ(stage.errors().size(), 1);
}

TEST_F(CountersParsingTest, CacheAsideReadNullCache) {
  std::unique_ptr<Cache> cache = nullptr;
  UserAgent user_agent;
  table_.feature_ids = {1};
  absl::flat_hash_map<uint64_t, uint64_t> counts;
  std::vector<std::string> data = {"1", "2"};
  auto stage = getStageForParsing(user_agent);
  bool called_finish = false;

  EXPECT_CALL(*client_, hGetAll).WillOnce(testing::InvokeArgument<1>(data));
  stage.cacheAsideRead(cache, table_, "some_key", millisSinceEpoch(), counts,
                       std::make_shared<std::function<void()>>(
                           [&called_finish]() { called_finish = true; }));
  EXPECT_EQ(counts.size(), 1);
  EXPECT_EQ(counts[1], 2);
  EXPECT_TRUE(called_finish);
}

TEST_F(CountersParsingTest, CacheAsideReadMissAndInsert) {
  auto cache = std::make_unique<Cache>(100);
  UserAgent user_agent;
  table_.feature_ids = {1};
  absl::flat_hash_map<uint64_t, uint64_t> counts;
  std::vector<std::string> data = {"1", "2"};
  auto stage = getStageForParsing(user_agent);
  std::string some_key = "some_key";
  int some_millis = 200;
  std::string timed_key = makeTimedKey(some_key, some_millis);
  bool called_finish = false;

  EXPECT_CALL(*client_, hGetAll).WillOnce(testing::InvokeArgument<1>(data));
  stage.cacheAsideRead(cache, table_, some_key, some_millis, counts,
                       std::make_shared<std::function<void()>>(
                           [&called_finish]() { called_finish = true; }));
  EXPECT_EQ(counts.size(), 1);
  EXPECT_EQ(counts[1], 2);
  // Check that counts were cached.
  Cache::ConstAccessor accessor;
  EXPECT_TRUE(cache->find(accessor, {timed_key.data(), timed_key.size()}));
  EXPECT_EQ(accessor.get()->size(), counts.size());
  EXPECT_TRUE(called_finish);
}

TEST_F(CountersParsingTest, CacheAsideReadMissAndInsertEmpty) {
  auto cache = std::make_unique<Cache>(100);
  UserAgent user_agent;
  table_.feature_ids = {1};
  absl::flat_hash_map<uint64_t, uint64_t> counts;
  std::vector<std::string> empty_data;
  auto stage = getStageForParsing(user_agent);
  std::string some_key = "some_key";
  int some_millis = 200;
  std::string timed_key = makeTimedKey(some_key, some_millis);
  bool called_finish = false;

  EXPECT_CALL(*client_, hGetAll)
      .WillOnce(testing::InvokeArgument<1>(empty_data));
  stage.cacheAsideRead(cache, table_, some_key, some_millis, counts,
                       std::make_shared<std::function<void()>>(
                           [&called_finish]() { called_finish = true; }));
  EXPECT_TRUE(counts.empty());
  // Check that counts were cached.
  Cache::ConstAccessor accessor;
  EXPECT_TRUE(cache->find(accessor, {timed_key.data(), timed_key.size()}));
  EXPECT_EQ(accessor.get()->size(), counts.size());
  EXPECT_TRUE(called_finish);
}

TEST_F(CountersParsingTest, CacheAsideReadHit) {
  std::string some_key = "some_key";
  auto cache = std::make_unique<Cache>(100);
  absl::flat_hash_map<uint64_t, uint64_t> counts = {{1, 2}};
  int some_millis = 200;
  std::string timed_key = makeTimedKey(some_key, some_millis);
  cache->insert({timed_key.data(), timed_key.size()}, counts);
  counts.clear();
  UserAgent user_agent;
  table_.feature_ids = {1};
  std::vector<std::string> data = {"1", "2"};
  auto stage = getStageForParsing(user_agent);
  bool called_finish = false;

  EXPECT_CALL(*client_, hGetAll).Times(0);
  stage.cacheAsideRead(cache, table_, some_key, some_millis, counts,
                       std::make_shared<std::function<void()>>(
                           [&called_finish]() { called_finish = true; }));
  EXPECT_EQ(counts.size(), 1);
  EXPECT_EQ(counts[1], 2);
  EXPECT_TRUE(called_finish);
}

TEST_F(CountersParsingTest, CacheAsideReadSegments) {
  std::string some_key = "some_key";
  auto cache = std::make_unique<Cache>(100);
  absl::flat_hash_map<uint64_t, uint64_t> counts = {{1, 2}};
  std::vector<std::string> empty_data;
  int some_millis = 200;
  std::string timed_key = makeTimedKey(some_key, some_millis);
  cache->insert({timed_key.data(), timed_key.size()}, counts);
  counts.clear();
  UserAgent user_agent;
  table_.feature_ids = {1};
  std::vector<std::string> data = {"1", "2"};
  auto stage = getStageForParsing(user_agent);
  bool called_finish = false;

  EXPECT_CALL(*client_, hGetAll)
      .WillOnce(testing::InvokeArgument<1>(empty_data));
  stage.cacheAsideRead(cache, table_, some_key, some_millis, counts,
                       std::make_shared<std::function<void()>>(
                           [&called_finish]() { called_finish = true; }),
                       "some_segment");
  EXPECT_EQ(counts.size(), 0);
  EXPECT_TRUE(called_finish);
}

TEST(CountersTest, ReadFromCountersRunNullInputs) {
  auto client_ptr = std::make_unique<MockRedisClient>();
  auto& client = *client_ptr;
  Caches caches;
  // All tables default to nullptr.
  DatabaseInfo database;
  // Initialize user info and at least one content ID to make sure all table are
  // considered.
  delivery::Request req;
  req.mutable_user_info()->set_user_id("hi");
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("bye");
  UserAgent user_agent;
  CountersContext context;
  auto stage =
      ReadFromCountersStage(0, std::move(client_ptr), caches, database, 0, req,
                            insertions, 2000, user_agent, context);
  bool ran = false;

  EXPECT_CALL(client, hGetAll).Times(0);
  stage.run([&ran]() { ran = true; }, [](const std::chrono::duration<double>&,
                                         std::function<void()>&&) {});
  EXPECT_TRUE(ran);
}

std::unique_ptr<TableInfo> someTable() {
  auto table = std::make_unique<TableInfo>();
  table->key_label_map = {{fid_key_label, 0}};
  return table;
}

TEST(CountersTest, ReadFromCountersRunNonNullInputs) {
  auto client_ptr = std::make_unique<MockRedisClient>();
  auto& client = *client_ptr;
  Caches caches;
  DatabaseInfo database;
  // Initialize all tables.
  database.global = someTable();
  database.content = someTable();
  database.content_query = someTable();
  database.user = someTable();
  database.log_user = someTable();
  database.query = someTable();
  database.last_user_event = someTable();
  database.last_log_user_event = someTable();
  database.last_user_query = someTable();
  database.last_log_user_query = someTable();
  // Initialize user info and at least one content ID to make sure all table are
  // considered.
  delivery::Request req;
  req.mutable_user_info()->set_user_id("hi");
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("bye");
  UserAgent user_agent;
  CountersContext context;
  auto stage =
      ReadFromCountersStage(0, std::move(client_ptr), caches, database, 0, req,
                            insertions, 2000, user_agent, context);
  bool ran = false;
  std::vector<std::string> data;

  EXPECT_CALL(client, hGetAll).WillRepeatedly(testing::InvokeArgument<1>(data));
  stage.run([&ran]() { ran = true; }, [](const std::chrono::duration<double>&,
                                         std::function<void()>&&) {});
  EXPECT_TRUE(ran);
}

TEST(CountersTest, MakeGlobalInfo) {
  std::vector<RateInfo> rate_infos;
  auto& rate_info = rate_infos.emplace_back();
  rate_info.numerator = delivery_private_features::DAY_7 +
                        delivery_private_features::COUNT_NAVIGATE +
                        delivery_private_features::ITEM_COUNT;
  rate_info.denominator = delivery_private_features::DAY_7 +
                          delivery_private_features::COUNT_IMPRESSION +
                          delivery_private_features::ITEM_COUNT;
  rate_info.raw = delivery_private_features::DAY_7 +
                  delivery_private_features::COUNT_NAVIGATE +
                  delivery_private_features::ITEM_RATE_RAW_OVER_IMPRESSION;
  absl::flat_hash_map<uint64_t, uint64_t> counts;
  counts[rate_info.numerator] = 10;
  counts[rate_info.denominator] = 100;
  auto global_info = makeGlobalInfo(rate_infos, counts);

  EXPECT_EQ(global_info.rates.size(), 1);
  EXPECT_FLOAT_EQ(global_info.rates[rate_info.raw], static_cast<float>(0.1));
  EXPECT_EQ(global_info.smoothing_parameters.size(), 1);
  EXPECT_FLOAT_EQ(global_info.smoothing_parameters[rate_info.raw], 20);
}

TEST(CountersTest, MakeSparseEmpty) {
  auto sparse = makeSparse({}, {}, {});
  EXPECT_TRUE(sparse.empty());
}

TEST(CountersTest, MakeSparse) {
  std::vector<RateInfo> rate_infos;
  auto& rate_info = rate_infos.emplace_back();
  rate_info.numerator = delivery_private_features::DAY_7 +
                        delivery_private_features::COUNT_NAVIGATE +
                        delivery_private_features::ITEM_COUNT;
  rate_info.denominator = delivery_private_features::DAY_7 +
                          delivery_private_features::COUNT_IMPRESSION +
                          delivery_private_features::ITEM_COUNT;
  rate_info.raw = delivery_private_features::DAY_7 +
                  delivery_private_features::COUNT_NAVIGATE +
                  delivery_private_features::ITEM_RATE_RAW_OVER_IMPRESSION;
  rate_info.smooth =
      delivery_private_features::DAY_7 +
      delivery_private_features::COUNT_NAVIGATE +
      delivery_private_features::ITEM_RATE_SMOOTH_OVER_IMPRESSION;
  rate_info.global = rate_info.raw;
  GlobalInfo global_info;
  global_info.rates = {{rate_info.global, 0.2}};
  global_info.smoothing_parameters = {{rate_info.global, 10}};

  {
    absl::flat_hash_map<uint64_t, uint64_t> counts;
    counts[rate_info.numerator] = 10;
    counts[rate_info.denominator] = 100;
    auto sparse = makeSparse(global_info, counts, rate_infos);

    EXPECT_EQ(sparse.size(), 4);
    EXPECT_FLOAT_EQ(sparse[rate_info.numerator], 10);
    EXPECT_FLOAT_EQ(sparse[rate_info.denominator], 100);
    EXPECT_FLOAT_EQ(sparse[rate_info.raw], static_cast<float>(0.1));
    EXPECT_FLOAT_EQ(sparse[rate_info.smooth], static_cast<float>(12) / 110);
  }
  // Zero item counts.
  {
    absl::flat_hash_map<uint64_t, uint64_t> counts;
    counts[rate_info.numerator] = 0;
    counts[rate_info.denominator] = 0;
    auto sparse = makeSparse(global_info, counts, rate_infos);

    EXPECT_EQ(sparse.size(), 4);
    EXPECT_FLOAT_EQ(sparse[rate_info.numerator], 0);
    EXPECT_FLOAT_EQ(sparse[rate_info.denominator], 0);
    EXPECT_FLOAT_EQ(sparse[rate_info.raw], 0);
    EXPECT_FLOAT_EQ(sparse[rate_info.smooth], static_cast<float>(200) / 1000);
  }
}

TEST(CountersTest, MergeCountsIntoSparse) {
  absl::flat_hash_map<uint64_t, uint64_t> counts = {
      {delivery_private_features::USER_QUERY_HOURS_AGO, 654060000}, {100, 101}};
  absl::flat_hash_map<uint64_t, float> sparse;
  std::vector<std::string> errors;
  mergeCountsIntoSparse(counts, sparse, errors);

  EXPECT_EQ(sparse.size(), 2);
  EXPECT_FLOAT_EQ(sparse[delivery_private_features::USER_QUERY_HOURS_AGO],
                  181.68335);
  EXPECT_FLOAT_EQ(sparse[100], 101);
  EXPECT_TRUE(errors.empty());
}

TEST(CountersTest, MergeCountsIntoSparseConlict) {
  absl::flat_hash_map<uint64_t, uint64_t> counts = {{100, 101}};
  absl::flat_hash_map<uint64_t, float> sparse = {{100, 101}};
  std::vector<std::string> errors;
  mergeCountsIntoSparse(counts, sparse, errors);

  EXPECT_EQ(sparse.size(), 1);
  EXPECT_FLOAT_EQ(sparse[100], 101);
  EXPECT_EQ(errors.size(), 1);
}

TEST(CountersTest, MergeSparseIntoSparse) {
  absl::flat_hash_map<uint64_t, float> sparse_src = {{100, 101}};
  absl::flat_hash_map<uint64_t, float> sparse_dst;
  std::vector<std::string> errors;
  mergeSparseIntoSparse(sparse_src, sparse_dst, errors);

  EXPECT_EQ(sparse_dst.size(), 1);
  EXPECT_FLOAT_EQ(sparse_dst[100], 101);
  EXPECT_TRUE(errors.empty());
}

TEST(CountersTest, MergeSparseIntoSparseConlict) {
  absl::flat_hash_map<uint64_t, float> sparse_src = {{100, 101}};
  absl::flat_hash_map<uint64_t, float> sparse_dst = {{100, 101}};
  std::vector<std::string> errors;
  mergeSparseIntoSparse(sparse_src, sparse_dst, errors);

  EXPECT_EQ(sparse_dst.size(), 1);
  EXPECT_FLOAT_EQ(sparse_dst[100], 101);
  EXPECT_EQ(errors.size(), 1);
}

TEST(CountersTest, ProcessCountersStageMostInputsMissing) {
  DatabaseInfo database;
  // Populate global just to test the rest of the checks.
  database.global = someTable();
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("cid_1");
  insertions.emplace_back().set_content_id("cid_2");
  FeatureContext feature_context;
  feature_context.initialize(insertions);
  CountersContext counters_context;
  auto stage = ProcessCountersStage(0, database, insertions, feature_context,
                                    counters_context);
  stage.runSync();
  EXPECT_TRUE(feature_context.getInsertionFeatures("cid_1").features.empty());
  EXPECT_TRUE(feature_context.getInsertionFeatures("cid_2").features.empty());
  EXPECT_TRUE(stage.errors().empty());
}

TEST(CountersTest, ProcessCountersStage) {
  DatabaseInfo database;
  database.global = someTable();
  database.global->rate_feature_ids = {RateInfo{.raw = 1000}};
  database.user = someTable();
  database.user->rate_feature_ids = {
      RateInfo{.raw = 1001, .smooth = 1002, .global = 1000}};
  database.log_user = someTable();
  database.log_user->rate_feature_ids = {
      RateInfo{.raw = 1003, .smooth = 1004, .global = 1000}};
  database.query = someTable();
  database.query->rate_feature_ids = {
      RateInfo{.raw = 1005, .smooth = 1006, .global = 1000}};
  database.content = someTable();
  database.content->rate_feature_ids = {
      RateInfo{.raw = 1007, .smooth = 1008, .global = 1000}};
  database.content_query = someTable();
  database.content_query->rate_feature_ids = {
      RateInfo{.raw = 1009, .smooth = 1010, .global = 1000}};
  std::string cid_1 = "cid_1";
  std::string cid_2 = "cid_2";
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id(cid_1);
  insertions.emplace_back().set_content_id(cid_2);
  FeatureContext feature_context;
  feature_context.initialize(insertions);
  CountersContext counters_context;
  counters_context.global_counts[100] = 101;
  // User-level.
  counters_context.user_counts[102] = 103;
  counters_context.log_user_counts[104] = 105;
  // Request-level.
  counters_context.last_user_query[106] = 107;
  counters_context.last_log_user_query[108] = 109;
  counters_context.query_counts[110] = 111;
  // Content ID 1.
  counters_context.content_counts[cid_1][112] = 113;
  counters_context.content_query_counts[cid_1][114] = 115;
  counters_context.last_user_event[cid_1][116] = 117;
  counters_context.last_log_user_event[cid_1][118] = 119;
  // Content ID 2.
  counters_context.content_counts[cid_2][120] = 121;
  counters_context.content_query_counts[cid_2][122] = 123;
  counters_context.last_user_event[cid_2][124] = 125;
  counters_context.last_log_user_event[cid_2][126] = 127;
  auto stage = ProcessCountersStage(0, database, insertions, feature_context,
                                    counters_context);
  stage.runSync();

  // The one missing ID is 100 (from global).
  // User-level.
  {
    const auto& features = feature_context.getUserFeatures().features;
    EXPECT_EQ(features.size(), 6);
    EXPECT_EQ(features.at(102), 103);
    EXPECT_EQ(features.at(104), 105);
    // This test doesn't care about the derived rate values; just that they're
    // present.
    EXPECT_TRUE(features.contains(1001));
    EXPECT_TRUE(features.contains(1002));
    EXPECT_TRUE(features.contains(1003));
    EXPECT_TRUE(features.contains(1004));
  }
  // Request-level.
  {
    const auto& features = feature_context.getRequestFeatures().features;
    EXPECT_EQ(features.size(), 5);
    EXPECT_EQ(features.at(106), 107);
    EXPECT_EQ(features.at(108), 109);
    EXPECT_EQ(features.at(110), 111);
    EXPECT_TRUE(features.contains(1005));
    EXPECT_TRUE(features.contains(1006));
  }
  // Content ID 1.
  {
    const auto& features = feature_context.getInsertionFeatures(cid_1).features;
    EXPECT_EQ(features.size(), 8);
    EXPECT_EQ(features.at(112), 113);
    EXPECT_EQ(features.at(114), 115);
    EXPECT_EQ(features.at(116), 117);
    EXPECT_EQ(features.at(118), 119);
    EXPECT_TRUE(features.contains(1007));
    EXPECT_TRUE(features.contains(1008));
    EXPECT_TRUE(features.contains(1009));
    EXPECT_TRUE(features.contains(1010));
  }
  // Settle for checking length of other content ID.
  EXPECT_EQ(feature_context.getInsertionFeatures(cid_2).features.size(), 8);
  EXPECT_TRUE(stage.errors().empty());
}
}  // namespace counters
}  // namespace delivery
