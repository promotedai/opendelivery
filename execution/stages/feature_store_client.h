// This interface is only currently defined for reading purposes.

#pragma once

#include <functional>
#include <string>
#include <vector>

namespace delivery {
struct FeatureStoreResult {
  std::string key;
  std::vector<std::string> columns_bytes;
};

class FeatureStoreClient {
 public:
  virtual ~FeatureStoreClient() = default;

  // `columns` is expected to be comma-separated and include the key column. The
  // values for those columns will be returned as bytes via the callback. 0 or 1
  // FeatureStoreResult is expected for each key passed in.
  virtual void read(
      const std::string& table, const std::string& key_column,
      const std::string& key, const std::string& columns,
      std::function<void(std::vector<FeatureStoreResult>)>&& cb) const = 0;
  virtual void readBatch(
      const std::string& table, const std::string& key_column,
      const std::vector<std::string>& keys, const std::string& columns,
      std::function<void(std::vector<FeatureStoreResult>)>&& cb) const = 0;
};
}  // namespace delivery
