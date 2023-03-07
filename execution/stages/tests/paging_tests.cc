#include <google/protobuf/struct.pb.h>
#include <google/protobuf/stubs/common.h>
#include <stddef.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "config/paging_config.h"
#include "execution/paging_context.h"
#include "execution/stages/paging.h"
#include "execution/stages/redis_client.h"
#include "execution/stages/tests/mock_clients.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/blender.pb.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
TEST(PagingTest, MakePagingKeyComponents) {
  delivery::Request baseline_req;
  auto baseline = makePagingKey(PagingConfig(), baseline_req);
  // Example of a non-component field.
  {
    auto req = baseline_req;
    req.set_request_id("xyz");
    EXPECT_EQ(makePagingKey(PagingConfig(), baseline_req), baseline);
  }
  // Platform ID.
  {
    auto req = baseline_req;
    req.set_platform_id(100);
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
  // Log user ID.
  {
    auto req = baseline_req;
    req.mutable_user_info()->set_log_user_id("100");
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
  // Client info.
  {
    auto req = baseline_req;
    req.mutable_client_info()->set_client_type(
        common::ClientInfo_ClientType::ClientInfo_ClientType_PLATFORM_SERVER);
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
  // Use case.
  {
    auto req = baseline_req;
    req.set_use_case(delivery::UseCase::SEARCH);
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
  // Search query.
  {
    auto req = baseline_req;
    req.set_search_query("abc");
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
  // Blender config.
  {
    auto req = baseline_req;
    req.mutable_blender_config()->add_blender_rule();
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
  // Properties.
  {
    auto req = baseline_req;
    google::protobuf::Value value;
    value.set_number_value(10);
    (*req.mutable_properties()->mutable_struct_()->mutable_fields())["lmn"] =
        value;
    EXPECT_NE(makePagingKey(PagingConfig(), req), baseline);
  }
}

TEST(PagingTest, MakePagingKeyPropertiesSorting) {
  delivery::Request baseline_req;
  google::protobuf::Value value;
  value.set_number_value(4);
  auto& baseline_fields =
      *baseline_req.mutable_properties()->mutable_struct_()->mutable_fields();
  baseline_fields["a"] = value;
  baseline_fields["b"] = value;
  baseline_fields["c"] = value;
  baseline_fields["d"] = value;
  baseline_fields["e"] = value;
  auto baseline = makePagingKey(PagingConfig(), baseline_req);

  delivery::Request req;
  auto& fields = *req.mutable_properties()->mutable_struct_()->mutable_fields();
  fields["b"] = value;
  fields["d"] = value;
  fields["e"] = value;
  fields["a"] = value;
  fields["c"] = value;
  EXPECT_EQ(makePagingKey(PagingConfig(), req), baseline);
}

TEST(PagingTest, MakePagingKeyNonKeyProperties) {
  delivery::Request baseline_req;
  google::protobuf::Value value;
  value.set_number_value(4);
  (*baseline_req.mutable_properties()
        ->mutable_struct_()
        ->mutable_fields())["b"] = value;
  auto baseline = makePagingKey(PagingConfig(), baseline_req);

  auto modified_req = baseline_req;
  value.set_number_value(3);
  (*modified_req.mutable_properties()
        ->mutable_struct_()
        ->mutable_fields())["a"] = value;
  PagingConfig config;
  config.non_key_properties.emplace_back("a");
  EXPECT_EQ(makePagingKey(config, modified_req), baseline);
}

TEST(PagingTest, InitCurrPageRequestEmpty) {
  PagingContext context;
  std::vector<std::string> errors;
  delivery::Request req;
  std::vector<delivery::Insertion> insertions;

  initCurrPage(context, errors, req, insertions);
  EXPECT_EQ(context.min_position, 0);
  EXPECT_EQ(context.max_position, 0);
  EXPECT_TRUE(context.open_positions.empty());
  EXPECT_FALSE(errors.empty());
}

TEST(PagingTest, InitCurrPageRequestLacksPaging) {
  PagingContext context;
  std::vector<std::string> errors;
  delivery::Request req;
  std::vector<delivery::Insertion> insertions(/*n=*/1);

  initCurrPage(context, errors, req, insertions);
  EXPECT_EQ(context.min_position, 0);
  EXPECT_EQ(context.max_position, 0);
  EXPECT_THAT(context.open_positions, testing::ElementsAre(0));
  EXPECT_TRUE(errors.empty());
}

TEST(PagingTest, InitCurrPageRequestHasPaging) {
  PagingContext context;
  std::vector<std::string> errors;
  delivery::Request req;
  req.mutable_paging()->set_offset(2);
  req.mutable_paging()->set_size(4);
  std::vector<delivery::Insertion> insertions(/*n=*/10);

  initCurrPage(context, errors, req, insertions);
  EXPECT_EQ(context.min_position, 2);
  EXPECT_EQ(context.max_position, 5);
  EXPECT_THAT(context.open_positions, testing::ElementsAre(2, 3, 4, 5));
  EXPECT_TRUE(errors.empty());
}

TEST(PagingTest, ProcessPastAllocsDuplicateInsertion) {
  PagingContext context;
  context.min_position = 101;
  context.max_position = 102;
  context.open_positions = {101, 102};
  std::vector<std::string> errors;
  delivery::Request req;
  delivery::Insertion insertion_a;
  insertion_a.set_position(100);
  insertion_a.set_content_id("100");
  delivery::Insertion insertion_b;
  insertion_b.set_position(101);
  insertion_b.set_content_id("101");
  delivery::Insertion insertion_b_duplicate;
  insertion_b_duplicate.set_position(102);
  insertion_b_duplicate.set_content_id("101");
  std::vector<std::string> allocs{insertion_a.SerializeAsString(),
                                  insertion_b.SerializeAsString(),
                                  insertion_b_duplicate.SerializeAsString()};
  bool limit_to_req_insertions = false;

  processPastAllocs(context, errors, req, {}, allocs, limit_to_req_insertions);
  EXPECT_EQ(context.seen_infos.size(), 2);
  ASSERT_TRUE(context.seen_infos.contains("100"));
  EXPECT_EQ(context.seen_infos["100"].on_curr_page, false);
  ASSERT_TRUE(context.seen_infos.contains("101"));
  EXPECT_EQ(context.seen_infos["101"].on_curr_page, true);
  EXPECT_THAT(context.open_positions, testing::ElementsAre(102));
}

TEST(PagingTest, ProcessPastAllocsDuplicatePosition) {
  PagingContext context;
  context.min_position = 101;
  context.max_position = 102;
  context.open_positions = {101, 102};
  std::vector<std::string> errors;
  delivery::Request req;
  delivery::Insertion insertion_a;
  insertion_a.set_position(100);
  insertion_a.set_content_id("100");
  delivery::Insertion insertion_b;
  insertion_b.set_position(101);
  insertion_b.set_content_id("101");
  delivery::Insertion insertion_b_duplicate;
  insertion_b_duplicate.set_position(101);
  insertion_b_duplicate.set_content_id("102");
  std::vector<std::string> allocs{insertion_a.SerializeAsString(),
                                  insertion_b.SerializeAsString(),
                                  insertion_b_duplicate.SerializeAsString()};
  bool limit_to_req_insertions = false;

  processPastAllocs(context, errors, req, {}, allocs, limit_to_req_insertions);
  EXPECT_EQ(context.seen_infos.size(), 2);
  ASSERT_TRUE(context.seen_infos.contains("100"));
  EXPECT_EQ(context.seen_infos["100"].on_curr_page, false);
  ASSERT_TRUE(context.seen_infos.contains("101"));
  EXPECT_EQ(context.seen_infos["101"].on_curr_page, true);
  EXPECT_THAT(context.open_positions, testing::ElementsAre(102));
}

TEST(PagingTest, ProcessPastAllocsWithLimit) {
  PagingContext context;
  context.min_position = 100;
  context.open_positions = {100};
  std::vector<std::string> errors;
  delivery::Insertion insertion_a;
  insertion_a.set_position(100);
  insertion_a.set_content_id("100");
  delivery::Request req;
  delivery::Insertion insertion_b;
  insertion_b.set_position(101);
  insertion_b.set_content_id("101");
  std::vector<std::string> allocs{insertion_a.SerializeAsString(),
                                  insertion_b.SerializeAsString()};
  bool limit_to_req_insertions = true;

  processPastAllocs(context, errors, req, {insertion_a}, allocs,
                    limit_to_req_insertions);
  EXPECT_EQ(context.seen_infos.size(), 1);
  ASSERT_TRUE(context.seen_infos.contains("100"));
}

TEST(PagingTest, ProcessPastAllocsInvalidInsertion) {
  PagingContext context;
  std::vector<std::string> errors;
  delivery::Request req;
  std::vector<std::string> alloc_lists{"garbo"};
  bool limit_to_req_insertions = false;

  processPastAllocs(context, errors, req, {}, alloc_lists,
                    limit_to_req_insertions);
  EXPECT_TRUE(!errors.empty());
  EXPECT_TRUE(context.seen_infos.empty());
}

TEST(PagingTest, GetInsertionsWhichCanBeOnCurrPage) {
  SeenInfo info_a;
  info_a.insertion.set_content_id("a");
  info_a.on_curr_page = false;
  SeenInfo info_b;
  info_b.insertion.set_content_id("b");
  info_b.on_curr_page = true;
  PagingContext context;
  context.seen_infos = {{"a", info_a}, {"b", info_b}};
  std::vector<delivery::Insertion> insertions;
  insertions.emplace_back().set_content_id("a");
  insertions.emplace_back().set_content_id("b");
  insertions.emplace_back().set_content_id("c");
  getInsertionsWhichCanBeOnCurrPage(context, insertions);

  EXPECT_EQ(insertions.size(), 2);
  // Was seen, but on current page.
  EXPECT_EQ(insertions[0].content_id(), "b");
  // Not seen before.
  EXPECT_EQ(insertions[1].content_id(), "c");
}

// This verifies that the stage both calls `lRange` and the callback which is
// passed in.
TEST(PagingTest, ReadCalls) {
  bool ran = false;
  auto client_ptr = std::make_unique<MockRedisClient>();
  auto& client = *client_ptr;
  PagingConfig config;
  delivery::Request req;
  std::vector<delivery::Insertion> insertions;
  PagingContext context;
  ReadFromPagingStage stage(0, std::move(client_ptr), config, req, insertions,
                            context);
  std::vector<std::string> allocs;
  EXPECT_CALL(client, lRange).WillOnce(testing::InvokeArgument<3>(allocs));
  stage.run(
      [ran = &ran]() { *ran = true; },
      [](const std::chrono::duration<double>&, std::function<void()>&&) {});
  EXPECT_TRUE(ran);
}

TEST(PagingTest, MakeAllocs) {
  PagingContext context;
  context.seen_infos.emplace("c", SeenInfo());
  delivery::Response resp;
  auto* insertion_a = resp.add_insertion();
  insertion_a->set_content_id("a");
  auto* insertion_b = resp.add_insertion();
  insertion_b->set_content_id("a");
  insertion_b->set_insertion_id("b");
  auto* insertion_c = resp.add_insertion();
  insertion_c->set_content_id("c");

  auto allocs = makeAllocs(context, resp);
  ASSERT_EQ(allocs.size(), 2);
  // Both elements will equal each other because the insertion ID gets stripped
  // out from the second insertion.
  EXPECT_EQ(allocs[0], allocs[1]);
  // Insertion C gets ignored because the paging context shows it has a past
  // alloc.
}

// This verifies that the stage both calls `rPush` and the callback which is
// passed in.
TEST(PagingTest, WriteCalls) {
  auto client_ptr = std::make_unique<MockRedisClient>();
  auto& client = *client_ptr;
  PagingConfig config;
  delivery::Response resp;
  resp.add_insertion();
  PagingContext context;
  // Imply a novel insertion so writing isn't a no-op.
  context.open_positions = {0};
  WriteToPagingStage stage(0, std::move(client_ptr), config, resp, context);
  EXPECT_CALL(client, rPush)
      .WillOnce(testing::InvokeArgument<2>(/*num_values=*/0));
  EXPECT_CALL(client, expire).Times(1);
  EXPECT_CALL(client, lTrim).Times(0);
  stage.runSync();
}

// Like above but calls `lTrim` due to having many values.
TEST(PagingTest, WriteCallsWithTrim) {
  auto client_ptr = std::make_unique<MockRedisClient>();
  auto& client = *client_ptr;
  PagingConfig config;
  delivery::Response resp;
  resp.add_insertion();
  PagingContext context;
  context.open_positions = {0};
  WriteToPagingStage stage(0, std::move(client_ptr), config, resp, context);
  EXPECT_CALL(client, rPush)
      .WillOnce(testing::InvokeArgument<2>(/*num_values=*/1'000'000));
  EXPECT_CALL(client, expire).Times(1);
  EXPECT_CALL(client, lTrim).Times(1);
  stage.runSync();
}
}  // namespace delivery
