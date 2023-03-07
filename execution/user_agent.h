// This is an intermediate type to abstract the user agent details we care about
// away from any particular parsing library.

#pragma once

#include <string>

namespace delivery {
struct UserAgent {
  std::string os;
  std::string app;
};
}  // namespace delivery
