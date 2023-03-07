// Home to paging-specific config options.

#pragma once

#include <vector>

#include "config/json.h"

namespace delivery {
struct PagingConfig {
  std::string url;
  // This URL is for the read replica corresponding to the above URL.
  std::string read_url;

  // Unfortunately our configs represent this as a string for the time being.
  // This is in milliseconds.
  std::string timeout;

  // Properties that should be ignored when making the paging key.
  std::vector<std::string> non_key_properties;

  // When true, previously allocated insertions are ignored if they're not on
  // the request too.
  bool limit_to_req_insertions = false;

  // How long paging allocs live for (after their most recent expiration
  // refresh). In seconds.
  int64_t ttl = 300;

  constexpr static auto properties = std::make_tuple(
      property(&PagingConfig::url, "url"),
      property(&PagingConfig::read_url, "readURL"),
      property(&PagingConfig::timeout, "timeout"),
      property(&PagingConfig::non_key_properties, "nonKeyProperties"),
      property(&PagingConfig::limit_to_req_insertions,
               "limitToRequestInsertions"),
      property(&PagingConfig::ttl, "ttl"));
};
}  // namespace delivery
