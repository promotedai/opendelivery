#include "execution/stages/paging.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/text_format.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <numeric>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/str_cat.h"
#include "config/paging_config.h"
#include "execution/paging_context.h"
#include "hash_utils/make_hash.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/blender.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "redis_client.h"

namespace delivery {
// How many values a key can have before we trim the earlier ones.
const int64_t max_values_per_key = 3000;
// Indicates what fraction of the allocs will be kept. Higher means fewer.
const int64_t alloc_trim_divisor = 2;

std::string makePagingKey(const PagingConfig& paging_config,
                          const delivery::Request& req) {
  hashlib::HashState state;
  state.updateState(req.platform_id());
  state.updateState(req.user_info().log_user_id());
  // This will have to be updated if more fields are introduced to `ClientInfo`.
  state.updateState(req.client_info().client_type());
  state.updateState(req.client_info().traffic_type());
  state.updateState(req.use_case());
  state.updateState(req.search_query());

  // C++ doesn't have a built-in way of hashing arbitrary types. Writing
  // Protobufs to a serialized string doesn't provide any hashing guarantees
  // (https://developers.google.com/protocol-buffers/docs/encoding#implications).
  // Text formatter does, however, since all fields are sorted by their tag and
  // all map values are sorted by their keys.
  std::string blender_config;
  google::protobuf::TextFormat::PrintToString(req.blender_config(),
                                              &blender_config);
  state.updateState(blender_config);

  std::string properties;
  // Ignore volatile request properties, if there are any. This currently does
  // not support nested fields.
  if (req.properties().struct_().fields_size() != 0 &&
      !paging_config.non_key_properties.empty()) {
    auto props_copy = req.properties();
    auto* fields = props_copy.mutable_struct_()->mutable_fields();
    for (const auto& non_key_prop : paging_config.non_key_properties) {
      fields->erase(non_key_prop);
    }
    google::protobuf::TextFormat::PrintToString(props_copy, &properties);
  } else {
    google::protobuf::TextFormat::PrintToString(req.properties(), &properties);
  }
  state.updateState(properties);

  return absl::StrCat(state.digestState());
}

void initCurrPage(PagingContext& paging_context,
                  std::vector<std::string>& errors,
                  const delivery::Request& req,
                  const std::vector<delivery::Insertion>& insertions) {
  int32_t offset = 0;
  int32_t size = 0;
  if (req.has_paging()) {
    const auto& paging = req.paging();
    // We expect only one of the two fields to be populated.
    if (!paging.cursor().empty()) {
      offset = std::atoi(paging.cursor().c_str());
    } else {
      offset = paging.offset();
    }
    size = std::min(static_cast<int32_t>(insertions.size()), paging.size());
  } else {
    size = static_cast<int32_t>(insertions.size());
  }

  int64_t max_position = offset + size - 1;

  if (offset < 0 || max_position < 0) {
    errors.emplace_back(absl::StrCat(
        "Empty insertions and/or paging for request ", req.request_id()));
  } else {
    paging_context.min_position = offset;
    paging_context.max_position = max_position;

    // Start by assuming all positions are open.
    paging_context.open_positions.resize(size);
    std::iota(paging_context.open_positions.begin(),
              paging_context.open_positions.end(), offset);
  }
}

template <bool limit_to_req_insertions>
void processPastAllocs(PagingContext& paging_context,
                       std::vector<std::string>& errors,
                       const delivery::Request& req,
                       const std::vector<delivery::Insertion>& insertions,
                       const std::vector<std::string>& allocs) {
  const int64_t tombstone = -1;

  std::vector<int64_t>& open_positions = paging_context.open_positions;

  // Factoring this out to make the rest of the function easier to read.
  auto handle_error = [&errors, &req, &paging_context]() {
    errors.emplace_back(absl::StrCat(
        "Unable to deserialize paging value for request ", req.request_id()));
    // If any values are malformed, ignore them all.
    paging_context.seen_infos.clear();
  };

  absl::flat_hash_set<std::string_view> insertions_on_req;
  if constexpr (limit_to_req_insertions) {
    // This early in processing, the execution insertions are expected to just
    // be the ones from the request.
    insertions_on_req.reserve(insertions.size());
    for (const auto& insertion : insertions) {
      insertions_on_req.emplace(insertion.content_id());
    }
  }

  for (const auto& alloc : allocs) {
    SeenInfo seen_info;
    const bool parsed = seen_info.insertion.ParseFromArray(
        alloc.data(), static_cast<int>(alloc.size()));
    if (!parsed) {
      return handle_error();
    }

    if constexpr (limit_to_req_insertions) {
      if (!insertions_on_req.contains(seen_info.insertion.content_id())) {
        continue;
      }
    }

    // Paging is written to asynchronously, so despite our best efforts we
    // can find redundancy in insertions and positions. To deal with them...
    //
    // For insertions, we prefer whatever we encounter first.
    //
    // For positions:
    // - If it's on the current page we prefer whatever we encounter first.
    // - If it's on a different page, we don't bother deduping.
    seen_info.on_curr_page =
        paging_context.min_position <= seen_info.insertion.position() &&
        seen_info.insertion.position() <= paging_context.max_position;
    if (seen_info.on_curr_page) {
      int64_t position_in_page =
          static_cast<int64_t>(seen_info.insertion.position()) -
          paging_context.min_position;

      if (open_positions[position_in_page] == tombstone) {
        continue;
      }

      bool is_novel_insertion =
          paging_context.seen_infos
              .emplace(seen_info.insertion.content_id(), std::move(seen_info))
              .second;
      if (is_novel_insertion) {
        open_positions[position_in_page] = tombstone;
      }
    } else {
      paging_context.seen_infos.emplace(seen_info.insertion.content_id(),
                                        std::move(seen_info));
    }
  }

  // Erase tombstones.
  open_positions.erase(
      std::remove_if(open_positions.begin(), open_positions.end(),
                     [](int64_t i) { return i == tombstone; }),
      open_positions.end());
}

// Helper to keep the templated impl out of our header.
void processPastAllocs(PagingContext& paging_context,
                       std::vector<std::string>& errors,
                       const delivery::Request& req,
                       const std::vector<delivery::Insertion>& insertions,
                       const std::vector<std::string>& allocs,
                       bool limit_to_req_insertions) {
  if (limit_to_req_insertions) {
    processPastAllocs</*limit_to_req_insertions=*/true>(
        paging_context, errors, req, insertions, allocs);
  } else {
    processPastAllocs</*limit_to_req_insertions=*/false>(
        paging_context, errors, req, insertions, allocs);
  }
}

// Previously allocated insertions are taken from the paging context.
// `insertions` refers to just ones from the request.
void getInsertionsWhichCanBeOnCurrPage(
    PagingContext& paging_context,
    std::vector<delivery::Insertion>& insertions) {
  std::vector<delivery::Insertion> res;

  res.reserve(insertions.size());
  for (auto& insertion : insertions) {
    auto it = paging_context.seen_infos.find(insertion.content_id());
    if (it == paging_context.seen_infos.end()) {
      // Insertions which weren't already seen are kept.
      res.emplace_back(std::move(insertion));
    } else {
      if (it->second.on_curr_page) {
        // Insertions which were previously seen on the current page are
        // replaced by the instance that was previously allocated.
        const auto& insertion = it->second.insertion;
        res.emplace_back(insertion);
      } else {
        // Insertions which were already seen on other pages are dropped.
      }
    }
  }

  insertions = std::move(res);
}

// This happens after Redis returns.
void ReadFromPagingStage::runSync() {
  initCurrPage(paging_context_, errors_, req_, insertions_);
  if (!allocs_.empty()) {
    processPastAllocs(paging_context_, errors_, req_, insertions_, allocs_,
                      paging_config_.limit_to_req_insertions);
    getInsertionsWhichCanBeOnCurrPage(paging_context_, insertions_);
  }

  done_cb_();
}

void ReadFromPagingStage::run(
    std::function<void()>&& done_cb,
    std::function<void(const std::chrono::duration<double>&,
                       std::function<void()>&&)>&&) {
  done_cb_ = done_cb;

  paging_context_.key = makePagingKey(paging_config_, req_);
  client_->lRange(paging_context_.key, 0, -1,
                  [this](std::vector<std::string> allocs) {
                    this->allocs_ = std::move(allocs);
                    runSync();
                  });
}

std::vector<std::string> makeAllocs(PagingContext& paging_context,
                                    const delivery::Response& resp) {
  std::vector<std::string> ret;
  ret.reserve(paging_context.open_positions.size());
  for (auto& insertion : resp.insertion()) {
    // Avoid redundant allocs.
    if (!paging_context.seen_infos.contains(insertion.content_id())) {
      // Intended copy since other stages could still be using the response.
      // Note that this copy does not include some fields which were on the
      // original request insertions (e.g. properties). This is bad - and known,
      // but accepted until those fields are more important for us.
      auto copy = insertion;
      // Strip insertion ID to reduce serialized size.
      copy.clear_insertion_id();
      ret.emplace_back(copy.SerializeAsString());
    }
  }
  return ret;
}

void WriteToPagingStage::runSync() {
  std::vector<std::string> allocs = makeAllocs(paging_context_, resp_);
  // If all insertions were past allocs, then don't bother.
  if (allocs.empty()) {
    return;
  }

  client_->rPush(paging_context_.key, allocs,
                 // These callbacks are not tied to the lifespan of the current
                 // request, so copy the necessary bits.
                 [client = client_, key = paging_context_.key,
                  ttl = paging_config_.ttl](int64_t num_values) {
                   client->expire(key, ttl);
                   // Expiration is (re)set for the entire key. If we want to
                   // remove just some of the allocs for a key, we must trim it
                   // manually.
                   if (num_values > max_values_per_key) {
                     client->lTrim(
                         key, -(max_values_per_key / alloc_trim_divisor), -1);
                   }
                 });
}
}  // namespace delivery
