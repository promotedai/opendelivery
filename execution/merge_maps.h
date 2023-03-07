// Generic map-merging util. If you need support for additional value types,
// just add more specializations.

#pragma once

#include <cstdint>
#include <vector>

#include "proto/delivery/private/features/features.pb.h"

namespace delivery {
template <typename Out, typename In>
Out convertValue(In&& in) {
  return static_cast<Out&&>(in);
}

template <>
inline std::vector<int64_t> convertValue(
    delivery_private_features::Int64Sequence&& sequence) {
  return {sequence.ids().begin(), sequence.ids().end()};
}

template <typename DstMap, typename SrcMap>
void mergeMaps(DstMap& dst, SrcMap& src) {
  // If types are the same and sizes are different, merge the smaller one into
  // the larger one.
  if constexpr (std::is_same_v<DstMap, SrcMap>) {
    if (dst.size() < src.size()) {
      auto tmp = std::move(dst);
      dst = std::move(src);
      src = std::move(tmp);
    }
  }
  dst.reserve(dst.size() + src.size());
  for (auto& [k, v] : src) {
    dst[k] = convertValue<typename DstMap::mapped_type,
                          typename SrcMap::mapped_type>(std::move(v));
  }
}
}  // namespace delivery
