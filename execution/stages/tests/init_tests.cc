#include <google/protobuf/struct.pb.h>
#include <google/protobuf/stubs/common.h>

#include <memory>
#include <string>
#include <vector>

#include "execution/context.h"
#include "execution/stages/init.h"
#include "gtest/gtest.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"

namespace delivery {
TEST(InitStageTest, RequestId) {
  Context context({});
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_FALSE(context.req().request_id().empty());
}

TEST(InitStageTest, ExecutionInsertions) {
  delivery::Request req;
  req.add_insertion();
  Context context(req);
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_EQ(context.execution_insertions.size(),
            context.req().insertion_size());
}

TEST(InitStageTest, ConvertInsertionMatrixHeaderMismatch) {
  delivery::Request req;
  req.mutable_insertion_matrix_headers()->Add("1");
  req.mutable_insertion_matrix()->add_values();
  Context context(req);
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_FALSE(stage.errors().empty());
}

TEST(InitStageTest, ConvertInsertionMatrixReservedHeaders) {
  delivery::Request req;
  google::protobuf::ListValue base_list_value;
  {
    req.mutable_insertion_matrix_headers()->Add("contentId");
    google::protobuf::Value value;
    value.set_string_value("0");
    *base_list_value.add_values() = value;
  }
  {
    req.mutable_insertion_matrix_headers()->Add("retrievalRank");
    google::protobuf::Value value;
    value.set_number_value(1);
    *base_list_value.add_values() = value;
  }
  {
    req.mutable_insertion_matrix_headers()->Add("retrievalScore");
    google::protobuf::Value value;
    value.set_number_value(2.0);
    *base_list_value.add_values() = value;
  }
  google::protobuf::Value list_value;
  *list_value.mutable_list_value() = base_list_value;
  *req.mutable_insertion_matrix()->add_values() = list_value;
  Context context(req);
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_TRUE(stage.errors().empty());
  ASSERT_EQ(context.execution_insertions.size(), 1);
  EXPECT_EQ(context.execution_insertions[0].content_id(), "0");
  EXPECT_EQ(context.execution_insertions[0].retrieval_rank(), 1);
  EXPECT_EQ(context.execution_insertions[0].retrieval_score(), 2.0);
}

TEST(InitStageTest, ConvertInsertionMatrixConflictingHeaders) {
  delivery::Request req;
  google::protobuf::ListValue base_list_value;
  {
    req.mutable_insertion_matrix_headers()->Add("1");
    google::protobuf::Value value;
    value.set_string_value("a");
    *base_list_value.add_values() = value;
  }
  {
    req.mutable_insertion_matrix_headers()->Add("1");
    google::protobuf::Value value;
    value.set_string_value("b");
    *base_list_value.add_values() = value;
  }
  {
    req.mutable_insertion_matrix_headers()->Add("1.3");
    google::protobuf::Value value;
    value.set_string_value("c");
    *base_list_value.add_values() = value;
  }
  google::protobuf::Value list_value;
  *list_value.mutable_list_value() = base_list_value;
  *req.mutable_insertion_matrix()->add_values() = list_value;
  Context context(req);
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_TRUE(stage.errors().empty());
  ASSERT_EQ(context.execution_insertions.size(), 1);
  EXPECT_EQ(context.execution_insertions[0]
                .properties()
                .struct_()
                .fields()
                .at("1")
                .struct_value()
                .fields()
                .at("3")
                .string_value(),
            "c");
}

TEST(InitStageTest, ConvertInsertionMatrixCombinedSubStructs) {
  delivery::Request req;
  google::protobuf::ListValue base_list_value;
  {
    req.mutable_insertion_matrix_headers()->Add("1");
    google::protobuf::Value value;
    value.set_string_value("a");
    *base_list_value.add_values() = value;
  }
  {
    req.mutable_insertion_matrix_headers()->Add("2.3");
    google::protobuf::Value value;
    value.set_string_value("b");
    *base_list_value.add_values() = value;
  }
  {
    req.mutable_insertion_matrix_headers()->Add("2.4");
    google::protobuf::Value value;
    value.set_number_value(4.0);
    *base_list_value.add_values() = value;
  }
  google::protobuf::Value list_value;
  *list_value.mutable_list_value() = base_list_value;
  *req.mutable_insertion_matrix()->add_values() = list_value;
  Context context(req);
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_TRUE(stage.errors().empty());
  ASSERT_EQ(context.execution_insertions.size(), 1);
  EXPECT_EQ(context.execution_insertions[0]
                .properties()
                .struct_()
                .fields()
                .at("1")
                .string_value(),
            "a");
  const auto& fields = context.execution_insertions[0]
                           .properties()
                           .struct_()
                           .fields()
                           .at("2")
                           .struct_value()
                           .fields();
  EXPECT_EQ(fields.at("3").string_value(), "b");
  EXPECT_EQ(fields.at("4").number_value(), 4.0);
}

TEST(InitStageTest, ConvertInsertionMatrixMultipleInsertions) {
  delivery::Request req;
  req.mutable_insertion_matrix_headers()->Add("1");
  {
    google::protobuf::Value value;
    value.set_string_value("a");
    google::protobuf::ListValue base_list_value;
    *base_list_value.add_values() = value;
    google::protobuf::Value list_value;
    *list_value.mutable_list_value() = base_list_value;
    *req.mutable_insertion_matrix()->add_values() = list_value;
  }
  {
    google::protobuf::Value value;
    value.set_string_value("b");
    google::protobuf::ListValue base_list_value;
    *base_list_value.add_values() = value;
    google::protobuf::Value list_value;
    *list_value.mutable_list_value() = base_list_value;
    *req.mutable_insertion_matrix()->add_values() = list_value;
  }
  Context context(req);
  InitStage stage(0, context);
  stage.runSync();
  EXPECT_TRUE(stage.errors().empty());
  ASSERT_EQ(context.execution_insertions.size(), 2);
  EXPECT_EQ(context.execution_insertions[0]
                .properties()
                .struct_()
                .fields()
                .at("1")
                .string_value(),
            "a");
  EXPECT_EQ(context.execution_insertions[1]
                .properties()
                .struct_()
                .fields()
                .at("1")
                .string_value(),
            "b");
}
}  // namespace delivery
