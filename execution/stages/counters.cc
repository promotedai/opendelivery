#include "execution/stages/counters.h"

#include <atomic>
#include <ext/alloc_traits.h>
#include <string_view>
#include <tuple>

#include "absl/meta/type_traits.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "cache.h"
#include "execution/counters_context.h"
#include "hash_utils/text.h"
#include "proto/common/common.pb.h"
#include "proto/delivery/delivery.pb.h"
#include "redis_client.h"
#include "utils/time.h"

namespace delivery {
namespace counters {
// Higher is smoother. Full context:
// https://docs.google.com/document/d/136j_F6ZGHfUKIpYi-g3FW3yZpRNfBcIbP2zEOAc2wi4/edit
const float smoothing_coefficient = 2.0;

const absl::flat_hash_set<uint64_t> timestamp_types = {
    delivery_private_features::USER_QUERY_HOURS_AGO,
    delivery_private_features::LOG_USER_QUERY_HOURS_AGO,
    delivery_private_features::USER_ITEM_HOURS_AGO,
    delivery_private_features::LOG_USER_ITEM_HOURS_AGO};

const int millis_in_an_hour = millis_in_15_min * 4;

uint64_t replaceMaskedBits(uint64_t original, uint64_t other, uint64_t mask) {
  return original ^ ((original ^ other) & mask);
}

uint64_t getAggregateFeatureId(uint64_t feature_id) {
  uint64_t type = feature_id & delivery_private_features::TYPE;
  auto it = segmented_id_to_aggregate.find(type);
  if (it != segmented_id_to_aggregate.end()) {
    return replaceMaskedBits(feature_id, it->second,
                             delivery_private_features::TYPE);
  }
  return 0;
}

std::string makeUserIdKey(uint64_t platform_id, std::string_view user_id) {
  return absl::StrJoin(std::tuple<uint64_t, std::string_view, std::string_view>(
                           platform_id, user_separator, user_id),
                       key_separator);
}

std::string makeQueryKey(uint64_t platform_id,
                         std::string_view hashed_search_query) {
  return absl::StrJoin(std::tuple<uint64_t, std::string_view, std::string_view>(
                           platform_id, query_separator, hashed_search_query),
                       key_separator);
}

std::string makeLastUserQueryKey(uint64_t platform_id, std::string_view user_id,
                                 std::string_view hashed_search_query) {
  return absl::StrJoin(
      std::tuple<uint64_t, std::string_view, std::string_view, std::string_view,
                 std::string_view>(platform_id, user_separator, user_id,
                                   query_separator, hashed_search_query),
      key_separator);
}

std::string makeContentKey(uint64_t platform_id, std::string_view content_id) {
  return absl::StrJoin(
      std::tuple<uint64_t, std::string_view>(platform_id, content_id),
      key_separator);
}

std::string makeContentQueryKey(uint64_t platform_id,
                                std::string_view content_id,
                                std::string_view hashed_search_query) {
  return absl::StrJoin(
      std::tuple<uint64_t, std::string_view, std::string_view,
                 std::string_view>(platform_id, content_id, query_separator,
                                   hashed_search_query),
      key_separator);
}

std::string makeLastUserEventKey(uint64_t platform_id, std::string_view user_id,
                                 std::string_view content_id) {
  return absl::StrJoin(std::tuple<uint64_t, std::string_view, std::string_view,
                                  std::string_view>(platform_id, user_separator,
                                                    user_id, content_id),
                       key_separator);
}

absl::flat_hash_map<uint64_t, uint64_t> ReadFromCountersStage::parseCounts(
    const std::vector<std::string>& data, const TableInfo& table) {
  absl::flat_hash_map<uint64_t, uint64_t> counts;

  if (data.size() % 2 != 0) {
    errors_.emplace_back(
        absl::StrCat("HGETALL returned an uneven number of rows ", data.size(),
                     " from table ", table.name));
    return {};
  }

  int fid_label_pos = table.key_label_map.at(fid_key_label);
  bool data_has_user_agent = false;
  int os_label_pos = 0;
  int app_label_pos = 0;
  auto it = table.key_label_map.find(os_key_label);
  // If the os label is present, assume all user agent-related labels are.
  if (it != table.key_label_map.end()) {
    data_has_user_agent = true;
    os_label_pos = it->second;
    app_label_pos = table.key_label_map.at(app_key_label);
  }

  counts.reserve(data.size() / 2);
  for (size_t i = 0; i < data.size(); i += 2) {
    std::string_view key_str = data[i];
    std::string_view count_str = data[i + 1];

    std::vector<std::string_view> key_parts =
        absl::StrSplit(key_str, key_separator);
    uint64_t fid;
    if (!absl::SimpleAtoi(key_parts[fid_label_pos], &fid)) {
      errors_.emplace_back(absl::StrCat("Failed to parse fid ",
                                        key_parts[fid_label_pos],
                                        " from table ", table.name));
      continue;
    }

    if (table.feature_ids.contains(fid)) {
      uint64_t count;
      if (!absl::SimpleAtoi(count_str, &count)) {
        errors_.emplace_back(absl::StrCat("Failed to parse count ", count_str,
                                          " from table ", table.name));
        continue;
      }

      if (!data_has_user_agent) {
        counts[fid] += count;
        continue;
      }
      uint64_t agg_fid = getAggregateFeatureId(fid);
      if (agg_fid == 0) {
        counts[fid] += count;
        continue;
      }
      // For tables where counts are segmented across user agents, we need to
      // add counts to both:
      // - The aggregated feature ID
      // - The segmented one if the user agent for this execution matches
      counts[agg_fid] += count;
      if (key_parts[os_label_pos] == user_agent_.os &&
          key_parts[app_label_pos] == user_agent_.app) {
        counts[fid] += count;
      }
    }
  }

  return counts;
}

absl::flat_hash_map<uint64_t, uint64_t> ReadFromCountersStage::parseLastUser(
    const std::vector<std::string>& data, const TableInfo& table) {
  absl::flat_hash_map<uint64_t, uint64_t> counts;

  if (data.size() % 2 != 0) {
    errors_.emplace_back(
        absl::StrCat("HGETALL returned an uneven number of rows ", data.size(),
                     " from table ", table.name));
    return {};
  }

  int fid_label_pos = table.key_label_map.at(fid_key_label);

  counts.reserve(data.size() / 2);
  for (size_t i = 0; i < data.size(); i += 2) {
    std::string_view key_str = data[i];
    std::string_view value_str = data[i + 1];

    std::vector<std::string_view> key_parts =
        absl::StrSplit(key_str, key_separator);
    uint64_t fid;
    if (!absl::SimpleAtoi(key_parts[fid_label_pos], &fid)) {
      errors_.emplace_back(absl::StrCat("Failed to parse fid ",
                                        key_parts[fid_label_pos],
                                        " from table ", table.name));
      continue;
    }

    if (table.feature_ids.contains(fid)) {
      uint64_t value;
      if (!absl::SimpleAtoi(value_str, &value)) {
        errors_.emplace_back(absl::StrCat("Failed to parse value ", value_str,
                                          " from table ", table.name));
        continue;
      }

      if (timestamp_types.contains(fid & delivery_private_features::TYPE)) {
        counts[fid] = start_time_ - value;
      } else {
        counts[fid] = value;
      }
    }
  }

  return counts;
}

void ReadFromCountersStage::read(
    const TableInfo& table, const std::string& key,
    absl::flat_hash_map<uint64_t, uint64_t>& counts,
    std::shared_ptr<std::function<void()>> finish) {
  client_->hGetAll(key, [this, &counts, &table, finish = std::move(finish)](
                            const std::vector<std::string>& data) {
    counts = parseLastUser(data, table);
    (*finish)();
  });
}

void ReadFromCountersStage::cacheAsideRead(
    std::unique_ptr<Cache>& cache, const TableInfo& table,
    const std::string& key, uint64_t start_time,
    absl::flat_hash_map<uint64_t, uint64_t>& counts,
    std::shared_ptr<std::function<void()>> finish, std::string_view segment) {
  CacheKey cache_key;
  if (cache != nullptr) {
    Cache::ConstAccessor accessor;
    std::string timed_key = makeTimedKey(key, start_time);
    // The current implementation of counters is inefficient. The hash key does
    // not indicate the segment (i.e. user agent) so each read of a segmented
    // table produces the counts for all segments. parseCounts() is also
    // inefficient. Despite having all segments available to us, we only take
    // the count for this request's segment and the sum of all segments. We know
    // this is bad. In the meanwhile, for segmented tables we specify the
    // segment in the cache key to avoid natural collisions of the hash key.
    if (!segment.empty()) {
      timed_key = absl::StrCat(timed_key, segment);
    }
    cache_key = CacheKey(timed_key.data(), timed_key.size());
    if (cache->find(accessor, cache_key)) {
      counts = *accessor.get();
      (*finish)();
      return;
    }
  }
  client_->hGetAll(
      key, [this, &counts, &table, &cache, cache_key,
            finish = std::move(finish)](const std::vector<std::string>& data) {
        counts = parseCounts(data, table);
        if (cache != nullptr) {
          cache->insert(cache_key, counts);
        }
        (*finish)();
      });
}

void ReadFromCountersStage::run(
    std::function<void()>&& done_cb,
    std::function<void(const std::chrono::duration<double>&,
                       std::function<void()>&&)>&&) {
  std::string hashed_search_query =
      hashlib::hashSearchQuery(req_.search_query());
  std::string cat_user_agent = absl::StrCat(user_agent_.os, user_agent_.app);

  // We don't want to kick off the processing stage until all reads are done.
  // This is initialized to 1 so this function can prevent cached reads from
  // instantly decrementing before further reads can be started.
  auto remaining_reads = std::make_shared<std::atomic<size_t>>(1);
  auto finish =
      std::make_shared<std::function<void()>>([remaining_reads, done_cb]() {
        if (--(*remaining_reads) == 0) {
          done_cb();
        }
      });

  // Global. This shouldn't ever be null but let's be defensive.
  if (database_.global != nullptr) {
    ++(*remaining_reads);
    cacheAsideRead(caches_.global_counts_cache, *database_.global,
                   absl::StrCat(platform_id_), start_time_,
                   counters_context_.global_counts, finish, cat_user_agent);
  } else {
    errors_.emplace_back(
        "Trying to read from a counters database with no global table");
  }
  // User counts.
  if (req_.has_user_info()) {
    if (!req_.user_info().user_id().empty()) {
      if (database_.user != nullptr) {
        ++(*remaining_reads);
        cacheAsideRead(caches_.user_counts_cache, *database_.user,
                       makeUserIdKey(platform_id_, req_.user_info().user_id()),
                       start_time_, counters_context_.user_counts, finish);
      }
      if (database_.last_user_query != nullptr) {
        ++(*remaining_reads);
        read(*database_.last_user_query,
             makeLastUserQueryKey(platform_id_, req_.user_info().user_id(),
                                  hashed_search_query),
             counters_context_.last_user_query, finish);
      }
    }
    if (!req_.user_info().log_user_id().empty()) {
      if (database_.log_user != nullptr) {
        ++(*remaining_reads);
        cacheAsideRead(
            caches_.user_counts_cache, *database_.log_user,
            makeUserIdKey(platform_id_, req_.user_info().log_user_id()),
            start_time_, counters_context_.log_user_counts, finish);
      }
      if (database_.last_log_user_query != nullptr) {
        ++(*remaining_reads);
        read(*database_.last_log_user_query,
             makeLastUserQueryKey(platform_id_, req_.user_info().log_user_id(),
                                  hashed_search_query),
             counters_context_.last_log_user_query, finish);
      }
    }
  }
  // Query counts.
  if (database_.query != nullptr) {
    ++(*remaining_reads);
    cacheAsideRead(caches_.query_counts_cache, *database_.query,
                   makeQueryKey(platform_id_, hashed_search_query), start_time_,
                   counters_context_.query_counts, finish);
  }
  // Item counts.
  // Reserve to avoid resizes after some reads have been kicked off.
  counters_context_.content_counts.reserve(insertions_.size());
  counters_context_.content_query_counts.reserve(insertions_.size());
  counters_context_.last_user_event.reserve(insertions_.size());
  counters_context_.last_log_user_event.reserve(insertions_.size());
  for (const auto& insertion : insertions_) {
    const auto& content_id = insertion.content_id();
    if (database_.content != nullptr) {
      ++(*remaining_reads);
      cacheAsideRead(caches_.item_counts_cache, *database_.content,
                     makeContentKey(platform_id_, content_id), start_time_,
                     counters_context_.content_counts[content_id], finish,
                     cat_user_agent);
    }
    if (database_.content_query != nullptr) {
      ++(*remaining_reads);
      cacheAsideRead(
          caches_.item_query_counts_cache, *database_.content_query,
          makeContentQueryKey(platform_id_, content_id, hashed_search_query),
          start_time_, counters_context_.content_query_counts[content_id],
          finish);
    }
    if (req_.has_user_info()) {
      if (database_.last_user_event != nullptr &&
          !req_.user_info().user_id().empty()) {
        ++(*remaining_reads);
        read(*database_.last_user_event,
             makeLastUserEventKey(platform_id_, req_.user_info().user_id(),
                                  content_id),
             counters_context_.last_user_event[content_id], finish);
      }
      if (database_.last_log_user_event != nullptr &&
          !req_.user_info().log_user_id().empty()) {
        ++(*remaining_reads);
        read(*database_.last_log_user_event,
             makeLastUserEventKey(platform_id_, req_.user_info().log_user_id(),
                                  content_id),
             counters_context_.last_log_user_event[content_id], finish);
      }
    }
  }

  (*finish)();
}

float calculateSafeRate(uint64_t numerator, uint64_t denominator) {
  if (denominator == 0) {
    return 0;
  }
  return static_cast<float>(numerator) / static_cast<float>(denominator);
}

float calculateSmoothingParameter(float rate) {
  if (rate == 0) {
    return 0;
  }
  return smoothing_coefficient / rate;
}

float smooth(float global_rate, float smoothing_parameter,
             uint64_t numerator_count, uint64_t denominator_count) {
  if (smoothing_parameter + static_cast<float>(denominator_count) == 0) {
    return 0;
  }
  return (global_rate * smoothing_parameter +
          static_cast<float>(numerator_count)) /
         (smoothing_parameter + static_cast<float>(denominator_count));
}

GlobalInfo makeGlobalInfo(
    const std::vector<RateInfo>& rate_infos,
    const absl::flat_hash_map<uint64_t, uint64_t>& global_counts) {
  GlobalInfo global_info;
  global_info.rates.reserve(rate_infos.size());
  global_info.smoothing_parameters.reserve(rate_infos.size());
  for (const auto& info : rate_infos) {
    auto it = global_counts.find(info.numerator);
    uint64_t numerator = it != global_counts.end() ? it->second : 0;
    it = global_counts.find(info.denominator);
    uint64_t denominator = it != global_counts.end() ? it->second : 0;

    float rate = calculateSafeRate(numerator, denominator);
    global_info.rates[info.raw] = rate;
    global_info.smoothing_parameters[info.raw] =
        calculateSmoothingParameter(rate);
  }
  return global_info;
}

// This must be used if rates are expected to be computed from counts.
absl::flat_hash_map<uint64_t, float> makeSparse(
    const GlobalInfo& global_info,
    const absl::flat_hash_map<uint64_t, uint64_t>& counts,
    const std::vector<RateInfo>& rate_infos) {
  absl::flat_hash_map<uint64_t, float> sparse;

  if (counts.empty()) {
    return sparse;
  }
  for (const auto [k, v] : counts) {
    sparse[k] = static_cast<float>(v);
  }
  for (const auto& rate : rate_infos) {
    auto it = counts.find(rate.numerator);
    uint64_t numerator = it != counts.end() ? it->second : 0;
    it = counts.find(rate.denominator);
    uint64_t denominator = it != counts.end() ? it->second : 0;

    sparse[rate.raw] = calculateSafeRate(numerator, denominator);
    sparse[rate.smooth] =
        smooth(global_info.rates.at(rate.global),
               global_info.smoothing_parameters.at(rate.global), numerator,
               denominator);
  }

  return sparse;
}

void mergeCountsIntoSparse(
    const absl::flat_hash_map<uint64_t, uint64_t>& counts,
    absl::flat_hash_map<uint64_t, float>& sparse,
    std::vector<std::string>& errors) {
  for (const auto [k, v] : counts) {
    if (sparse.contains(k)) {
      errors.emplace_back(absl::StrCat("Sparse key ", k, " already exists"));
      continue;
    }

    if (timestamp_types.contains(k & delivery_private_features::TYPE)) {
      // Time values are stored in millis, but we ultimately want them in hours.
      sparse[k] = static_cast<float>(v) / millis_in_an_hour;
    } else {
      sparse[k] = static_cast<float>(v);
    }
  }
}

void mergeSparseIntoSparse(const absl::flat_hash_map<uint64_t, float>& src,
                           absl::flat_hash_map<uint64_t, float>& dst,
                           std::vector<std::string>& errors) {
  for (const auto [k, v] : src) {
    if (dst.contains(k)) {
      errors.emplace_back(absl::StrCat("Sparse key ", k, " already exists"));
      continue;
    }

    dst[k] = static_cast<float>(v);
  }
}

void ProcessCountersStage::runSync() {
  // This shouldn't ever be null but let's be defensive.
  if (database_.global == nullptr) {
    errors_.emplace_back("Trying to process counts with no global table");
    return;
  }

  // Compute global rates and smoothing parameters, which are used to generate
  // all later sparses.
  auto global_info = makeGlobalInfo(database_.global->rate_feature_ids,
                                    counters_context_.global_counts);

  // Stash user features.
  feature_context_.addUserFeatures(
      makeSparse(global_info, counters_context_.user_counts,
                 database_.user->rate_feature_ids));
  feature_context_.addUserFeatures(
      makeSparse(global_info, counters_context_.log_user_counts,
                 database_.log_user->rate_feature_ids));

  // Stash request features.
  absl::flat_hash_map<uint64_t, float> query_sparse;
  if (database_.query != nullptr) {
    query_sparse = makeSparse(global_info, counters_context_.query_counts,
                              database_.query->rate_feature_ids);
  }
  mergeCountsIntoSparse(counters_context_.last_user_query, query_sparse,
                        errors_);
  mergeCountsIntoSparse(counters_context_.last_log_user_query, query_sparse,
                        errors_);
  feature_context_.addRequestFeatures(std::move(query_sparse));

  // Stash insertion features.
  for (const auto& insertion : insertions_) {
    const auto& content_id = insertion.content_id();
    absl::flat_hash_map<uint64_t, float> content_sparse;
    if (database_.content != nullptr) {
      auto it = counters_context_.content_counts.find(content_id);
      if (it != counters_context_.content_counts.end()) {
        content_sparse = makeSparse(global_info, it->second,
                                    database_.content->rate_feature_ids);
      }
    }

    if (database_.content_query != nullptr) {
      auto it = counters_context_.content_query_counts.find(content_id);
      if (it != counters_context_.content_query_counts.end()) {
        auto content_query_sparse = makeSparse(
            global_info, it->second, database_.content_query->rate_feature_ids);
        mergeSparseIntoSparse(content_query_sparse, content_sparse, errors_);
      }
    }

    auto it = counters_context_.last_user_event.find(content_id);
    if (it != counters_context_.last_user_event.end()) {
      mergeCountsIntoSparse(it->second, content_sparse, errors_);
    }

    it = counters_context_.last_log_user_event.find(content_id);
    if (it != counters_context_.last_log_user_event.end()) {
      mergeCountsIntoSparse(it->second, content_sparse, errors_);
    }

    feature_context_.addInsertionFeatures(content_id,
                                          std::move(content_sparse));
  }
}
}  // namespace counters
}  // namespace delivery
