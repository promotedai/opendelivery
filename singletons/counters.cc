#include "singletons/counters.h"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <future>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "absl/hash/hash.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "async_redis.h"
#include "async_utils.h"
#include "cloud/sw_redis_client.h"
#include "config/counters_config.h"
#include "config/platform_config.h"
#include "errors.h"
#include "execution/stages/redis_client.h"
#include "proto/delivery/private/features/features.pb.h"
#include "redis_client_array.h"
#include "singletons/cache.h"
#include "singletons/config.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"
#include "utils.h"
#include "utils/network.h"

namespace delivery {
namespace counters {
// Constants to facilitate deduction of rate feature IDs.
const int counter_type_shift = 13;
const absl::flat_hash_set<uint64_t> type_has_rates = {
    delivery_private_features::ITEM_COUNT,
    delivery_private_features::ITEM_DEVICE_COUNT,
    delivery_private_features::USER_COUNT,
    delivery_private_features::LOG_USER_COUNT,
    delivery_private_features::QUERY_COUNT,
    delivery_private_features::ITEM_QUERY_COUNT};
// This specifies an ordering. An "earlier" key should not have a "later" value.
const absl::flat_hash_map<uint64_t, int> agg_indexes = {
    {delivery_private_features::COUNT_IMPRESSION, 1},
    {delivery_private_features::COUNT_NAVIGATE, 2},
    {delivery_private_features::COUNT_ADD_TO_CART, 3},
    {delivery_private_features::COUNT_CHECKOUT, 4},
    {delivery_private_features::COUNT_PURCHASE, 5}};

// Metadata keys.
const std::string row_format_key = absl::StrCat("\x1d\x1f", "row_format");
const std::string feature_ids_key = absl::StrCat("\x1d\x1f", "feature_ids");

const absl::flat_hash_set<std::string> valid_key_labels = {
    os_key_label, app_key_label, fid_key_label};

CountersSingleton::CountersSingleton() {
  auto platform_config =
      delivery::ConfigSingleton::getInstance().getPlatformConfig();
  for (const auto& [name, config] : platform_config.counters_configs) {
    createClients(config.url, config.timeout, name);
    // Assume that there's at least one client.
    auto& client = name_to_clients_.at(name).getClient(0);

    auto hgetall_fut =
        client.hgetall<absl::flat_hash_map<std::string, std::string>>(
            row_format_key);
    absl::flat_hash_map<std::string, std::string> row_formats;
    try {
      row_formats = hgetall_fut.get();
    } catch (const std::exception& err) {
      // This result is necessary so we want to explode even in the event of
      // timeout. This is fragile, and we can make it more resilient if it
      // becomes a problem.
      LOG_FATAL << "Failed to HGETALL: " << err.what();
      abort();
    }

    auto database_info = std::make_unique<DatabaseInfo>();
    for (const auto& [table, row_format] : row_formats) {
      std::unique_ptr<TableInfo>* table_info;

      if (absl::StartsWith(table, "platform")) {
        table_info = &database_info->global;
      } else if (absl::StartsWith(table, "user")) {
        table_info = &database_info->user;
      } else if (absl::StartsWith(table, "log-user")) {
        table_info = &database_info->log_user;
      } else if (table == "content" || table == "content-device") {
        table_info = &database_info->content;
      } else if (table == "content-query") {
        table_info = &database_info->content_query;
      } else if (table == "query") {
        table_info = &database_info->query;
      } else if (table == "last-time-user-event") {
        table_info = &database_info->last_user_event;
      } else if (table == "last-time-log-user-event") {
        table_info = &database_info->last_log_user_event;
      } else if (table == "last-time-user-query") {
        table_info = &database_info->last_user_query;
      } else if (table == "last-time-log-user-query") {
        table_info = &database_info->last_log_user_query;
      } else {
        LOG_INFO << "Database " << name << " has unsupported counters table "
                 << table;
        continue;
      }

      sw::redis::Future<sw::redis::OptionalString> hget_fut =
          client.hget(feature_ids_key, table);
      std::string table_feature_ids;
      try {
        table_feature_ids = hget_fut.get().value();
      } catch (const std::exception& err) {
        LOG_FATAL << "Failed to HGET: " << err.what();
        abort();
      }

      *table_info = createTableInfo(
          table, row_format, table_feature_ids,
          combineSplitFeatureIds(config.enabled_model_features));
    }

    if (database_info->global == nullptr) {
      LOG_FATAL << "Database " << name << " has no global table";
      abort();
    }

    const auto& cache_config = config.cache_config;
    CacheSingleton::getInstance().addCountersCaches(
        name, cache_config.global_rates_size, cache_config.item_counts_size,
        cache_config.user_counts_size, cache_config.query_counts_size,
        cache_config.item_query_counts_size);

    platform_to_name_to_database_[platform_config.platform_id][name] =
        std::move(database_info);
  }
}

void CountersSingleton::createClients(const std::string& url,
                                      const std::string& timeout,
                                      const std::string& name) {
  auto structured_url = parseRedisUrl(url);
  if (!structured_url.successful_parse) {
    LOG_FATAL << "Invalid counters URL: " << url;
    abort();
  }
  const int port = std::atoi(structured_url.port.c_str());
  if (port == 0) {
    LOG_FATAL << "Invalid counters port: " << structured_url.port;
    abort();
  }
  const int database_number = std::atoi(structured_url.database_number.c_str());
  if (database_number == 0 && structured_url.database_number != "0") {
    LOG_FATAL << "Invalid counters database number: " << structured_url.port;
    abort();
  }
  int timeout_millis = -1.0;
  try {
    timeout_millis = std::stoi(timeout);
  } catch (const std::exception&) {
    LOG_FATAL << "Invalid timeout: " << timeout;
    abort();
  }

  RedisClientArray client_array(structured_url.hostname, port, database_number,
                                timeout_millis);
  name_to_clients_.emplace(name, std::move(client_array));
}

std::unique_ptr<RedisClient> CountersSingleton::getCountersClient(
    const std::string& name, size_t index) {
  return std::make_unique<SwRedisClient>(
      name_to_clients_.at(name).getClient(index));
}

absl::flat_hash_set<uint64_t> CountersSingleton::combineSplitFeatureIds(
    const std::vector<SplitFeatureID>& split_feature_ids) {
  absl::flat_hash_set<uint64_t> ret;
  for (const auto& split : split_feature_ids) {
    // Cast to uint64 to add all other values to.
    uint64_t is_unattributed = split.is_unattributed;
    // Having no window is valid for timestamps.
    delivery_private_features::CountWindow window;
    delivery_private_features::CountWindow_Parse(split.window, &window);
    delivery_private_features::AggMetric agg_value;
    if (!delivery_private_features::AggMetric_Parse(split.agg_value,
                                                    &agg_value)) {
      return {};
    }
    delivery_private_features::CountType type;
    if (!delivery_private_features::CountType_Parse(split.type, &type)) {
      return {};
    }
    ret.emplace(is_unattributed + window + agg_value + type);
  }
  return ret;
}

absl::flat_hash_set<uint64_t> CountersSingleton::parseEnabledFeatureIds(
    const absl::flat_hash_set<uint64_t>& enabled_feature_ids,
    std::string_view table_feature_ids) {
  absl::flat_hash_set<uint64_t> intersection;
  const bool is_all_enabled = enabled_feature_ids.empty();
  auto feature_ids = absl::StrSplit(table_feature_ids, ',');
  for (const auto& feature_id : feature_ids) {
    uint64_t probe = std::atoi(feature_id.data());
    if (probe == 0) {
      return {};
    }
    if (enabled_feature_ids.contains(probe) || is_all_enabled) {
      intersection.emplace(probe);
    }
  }
  return intersection;
}

RateInfo getRateFeatureIds(uint64_t feature_id, uint64_t denominator_index) {
  RateInfo rates;
  // These sums rely on the ordering of the Protobuf values.
  rates.raw = feature_id + (denominator_index << counter_type_shift);
  rates.smooth = rates.raw + (agg_indexes.size() << counter_type_shift);
  // Replace 1662976 with constant proto enum when some new CountType is added.
  const bool is_item =
      (delivery_private_features::ITEM_COUNT <= feature_id &&
       feature_id < 1662976) ||
      (delivery_private_features::ITEM_DEVICE_COUNT <= feature_id &&
       feature_id < delivery_private_features::USER_COUNT);
  // Non-item count rates need to be mapped back to the (item based)-global
  // rate. Global rates are CountType'd as item counts to indicate the counting
  // source even though the count and rates are scoped to the entire platform.
  if (!is_item) {
    uint64_t item_feature_id =
        replaceMaskedBits(feature_id, delivery_private_features::ITEM_COUNT,
                          delivery_private_features::TYPE);
    rates.global = item_feature_id + (denominator_index << counter_type_shift);
  } else {
    rates.global = rates.raw;
  }
  return rates;
}

std::vector<RateInfo> CountersSingleton::deriveRateFeatureIds(
    const absl::flat_hash_set<uint64_t>& feature_ids) {
  std::vector<RateInfo> rate_feature_ids;
  for (uint64_t numerator : feature_ids) {
    // Only some types have rates.
    if (!type_has_rates.contains(numerator & delivery_private_features::TYPE)) {
      continue;
    }
    for (const auto [denominator_agg_metric, denominator_index] : agg_indexes) {
      uint64_t denominator =
          replaceMaskedBits(numerator, denominator_agg_metric,
                            delivery_private_features::AGG_METRIC);
      // Rates are only defined for some events and only where the denominator
      // is an "earlier" event.
      auto it =
          agg_indexes.find(numerator & delivery_private_features::AGG_METRIC);
      if (it == agg_indexes.end() || it->second <= denominator_index) {
        continue;
      }
      // We only calculate rates where both the numerator and denominator
      // feature IDs are enabled.
      if (feature_ids.contains(denominator)) {
        RateInfo rate = getRateFeatureIds(numerator, denominator_index);
        rate_feature_ids.emplace_back(RateInfo{numerator, denominator, rate.raw,
                                               rate.smooth, rate.global});
        // Add any dynamically aggregated counts.
        uint64_t numerator_agg = getAggregateFeatureId(numerator);
        uint64_t denominator_agg = getAggregateFeatureId(denominator);
        if (numerator_agg != 0 && denominator_agg != 0) {
          RateInfo agg_rate =
              getRateFeatureIds(numerator_agg, denominator_index);
          rate_feature_ids.emplace_back(RateInfo{numerator_agg, denominator_agg,
                                                 agg_rate.raw, agg_rate.smooth,
                                                 agg_rate.global});
        }
      }
    }
  }
  return rate_feature_ids;
}

std::unique_ptr<TableInfo> CountersSingleton::createTableInfo(
    const std::string& name, const std::string& row_format,
    const std::string& table_feature_ids,
    const absl::flat_hash_set<uint64_t>& config_feature_ids) {
  auto table_info = std::make_unique<TableInfo>();
  table_info->name = name;

  // Process the row format.
  std::vector<std::string_view> row_parts = absl::StrSplit(row_format, ":");
  if (row_parts.size() != 2) {
    return nullptr;
  }
  std::vector<std::string_view> key_labels = absl::StrSplit(row_parts[0], ",");
  for (int i = 0; i < key_labels.size(); ++i) {
    std::string_view stripped_label = absl::StripAsciiWhitespace(key_labels[i]);
    if (!valid_key_labels.contains(stripped_label)) {
      continue;
    }
    table_info->key_label_map[stripped_label] = i;
  }
  if (table_info->key_label_map.empty()) {
    return nullptr;
  }

  // Only associate this table with feature IDs which were specified.
  table_info->feature_ids =
      parseEnabledFeatureIds(config_feature_ids, table_feature_ids);
  if (table_info->feature_ids.empty()) {
    return nullptr;
  }
  table_info->rate_feature_ids = deriveRateFeatureIds(table_info->feature_ids);

  LOG_INFO << "Counters table " << name << " had the following IDs specified: "
           << absl::StrJoin(table_info->feature_ids, " ");
  std::string rates;
  for (const auto& rate : table_info->rate_feature_ids) {
    absl::StrAppend(&rates, rate.raw, " ", rate.smooth, " ");
  }
  LOG_INFO << "Counters table " << name
           << " had the following IDs derived: " << rates;

  return table_info;
}
}  // namespace counters
}  // namespace delivery
