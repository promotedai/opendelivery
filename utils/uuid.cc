#include "utils/uuid.h"

#include <ctype.h>

#include <algorithm>
#include <string>

#include "drogon/utils/Utilities.h"

namespace delivery {
std::string uuid() {
  std::string uuid = drogon::utils::getUuid();
  // Delivery Log UUIDs are expected to be lowercase, so we force all UUIDs to
  // be so.
  std::transform(uuid.begin(), uuid.end(), uuid.begin(),
                 [](unsigned char c) { return tolower(c); });
  return uuid;
}
}  // namespace delivery
