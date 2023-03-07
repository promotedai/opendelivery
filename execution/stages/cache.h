// These are just typedefs around the LRU cache impl we use to simplify
// potentially changing it later.

#pragma once

#include "absl/container/flat_hash_map.h"
#include "proto/delivery/private/features/features.pb.h"
#include "thread-safe-lru/scalable-cache.h"

namespace delivery {
// "ThreadSafeString" isn't a great description. This doesn't do any
// ref-counting. It's just a memoized string to avoid redundant hash cost.
typedef tstarling::ThreadSafeStringKey CacheKey;

typedef tstarling::ThreadSafeScalableCache<
    CacheKey, delivery_private_features::Features, CacheKey::HashCompare>
    FeaturesCache;

namespace counters {
typedef tstarling::ThreadSafeScalableCache<
    CacheKey, absl::flat_hash_map<uint64_t, uint64_t>, CacheKey::HashCompare>
    Cache;
}
}  // namespace delivery
