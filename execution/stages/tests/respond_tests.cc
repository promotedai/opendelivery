#include <functional>
#include <string>
#include <vector>

#include "execution/stages/respond.h"
#include "gtest/gtest.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
TEST(RespondStageTest, CallbackUsed) {
  delivery::Request req;
  PagingContext paging_context;
  std::vector<delivery::Insertion> insertions;
  delivery::Response resp;
  bool ran = false;
  RespondStage stage(0, req, paging_context, insertions, resp,
                     [ran = &ran](const delivery::Response &) { *ran = true; });
  stage.runSync();
  EXPECT_TRUE(ran);
}

TEST(RespondStageTest, ResponseFields) {
  delivery::Request req;
  req.set_request_id("a");
  PagingContext paging_context;
  paging_context.min_position = 5;
  std::vector<delivery::Insertion> insertions;
  auto &insertion = insertions.emplace_back();
  insertion.set_content_id("b");
  insertion.set_position(100);
  delivery::Response resp;
  RespondStage stage(0, req, paging_context, insertions, resp,
                     [](const delivery::Response &) {});
  stage.runSync();

  EXPECT_EQ(resp.request_id(), "a");
  EXPECT_EQ(resp.paging_info().cursor(), "6");
  ASSERT_EQ(resp.insertion_size(), 1);
  EXPECT_EQ(resp.insertion(0).content_id(), "b");
  EXPECT_EQ(resp.insertion(0).position(), 100);
  EXPECT_EQ(resp.insertion(0).insertion_id().size(), 20);
}

TEST(RespondStageTest, CursorWhenNoInsertions) {
  delivery::Request req;
  req.set_request_id("a");
  PagingContext paging_context;
  paging_context.min_position = 5;
  std::vector<delivery::Insertion> insertions;
  delivery::Response resp;
  RespondStage stage(0, req, paging_context, insertions, resp,
                     [](const delivery::Response &) {});
  stage.runSync();

  EXPECT_EQ(resp.paging_info().cursor(), "5");
}

TEST(RespondStageTest, SortByPosition) {
  delivery::Request req;
  PagingContext paging_context;
  std::vector<delivery::Insertion> insertions;
  {
    auto &insertion = insertions.emplace_back();
    insertion.set_position(101);
  }
  {
    auto &insertion = insertions.emplace_back();
    insertion.set_position(100);
  }
  delivery::Response resp;
  RespondStage stage(0, req, paging_context, insertions, resp,
                     [](const delivery::Response &) {});
  stage.runSync();

  ASSERT_EQ(resp.insertion_size(), 2);
  EXPECT_EQ(resp.insertion(0).position(), 100);
  EXPECT_EQ(resp.insertion(1).position(), 101);
}
}  // namespace delivery
