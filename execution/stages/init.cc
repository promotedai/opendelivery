#include "execution/stages/init.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/stubs/common.h>
#include <stdint.h>

#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "execution/context.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "utils/uuid.h"

namespace delivery {
// Conflicting headers cause earlier ones to be overwritten.
std::vector<delivery::Insertion> convertInsertionMatrix(
    const delivery::Request& req, std::vector<std::string>& errors) {
  std::vector<delivery::Insertion> ret;

  const auto& headers = req.insertion_matrix_headers();
  ret.reserve(req.insertion_matrix().values_size());
  for (const auto& insertion : req.insertion_matrix().values()) {
    const auto& values = insertion.list_value().values();
    if (values.size() != headers.size()) {
      errors.emplace_back(absl::StrCat("Mismatched insertion matrix size (",
                                       values.size(), ") and header size (",
                                       headers.size(), ")"));
      return {};
    }

    delivery::Insertion converted_insertion;
    for (int i = 0; i < headers.size(); ++i) {
      // Fast-paths for legacy headers. Need to put these in specific locations
      // to not break feature hashing.
      if (headers[i] == "contentId") {
        converted_insertion.set_content_id(values[i].string_value());
        continue;
      }
      if (headers[i] == "retrievalRank") {
        converted_insertion.set_retrieval_rank(
            static_cast<uint64_t>(values[i].number_value()));
        continue;
      }
      if (headers[i] == "retrievalScore") {
        converted_insertion.set_retrieval_score(
            static_cast<float>(values[i].number_value()));
        continue;
      }
      // Each "." in the header scopes into a sub-struct.
      const std::vector<absl::string_view> parts =
          absl::StrSplit(headers[i], ".");
      auto* leaf = converted_insertion.mutable_properties()->mutable_struct_();
      for (size_t j = 0; j < parts.size() - 1; ++j) {
        leaf = (*leaf->mutable_fields())[parts[j]].mutable_struct_value();
      }
      (*leaf->mutable_fields())[parts[parts.size() - 1]] = values[i];
    }
    ret.emplace_back(std::move(converted_insertion));
  }

  return ret;
}

void InitStage::runSync() {
  auto& req = context_.req_;
  req.set_request_id(uuid());

  auto& execution_insertions = context_.execution_insertions;
  // Insertions specified via the matrix override insertions specified
  // otherwise.
  if (!req.insertion_matrix_headers().empty() &&
      req.insertion_matrix().values_size() > 0) {
    execution_insertions = convertInsertionMatrix(req, errors_);
  } else {
    execution_insertions.reserve(req.insertion_size());
    for (const auto& req_insertion : req.insertion()) {
      // Intentional copy to modify downstream. Can copy just particular fields
      // if the maintenace cost is worth the performance gain.
      execution_insertions.emplace_back(req_insertion);
    }
  }
}
}  // namespace delivery
