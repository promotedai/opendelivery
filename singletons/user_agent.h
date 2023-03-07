// User agents are parsed using a big regex which has to be constructed per
// instance of the parser. That construction takes multiple seconds. This is a
// singleton to effectively act as a global cache for a single parser instance.

#pragma once

#include <memory>
#include <string>

#include "UaParser.h"
#include "execution/user_agent.h"
#include "singletons/singleton.h"

namespace delivery {
class UserAgentSingleton : public Singleton<UserAgentSingleton> {
 public:
  UserAgent parse(const std::string& user_agent) const;

 private:
  friend class Singleton;

  UserAgentSingleton();

  std::unique_ptr<uap_cpp::UserAgentParser> parser_;
};
}  // namespace delivery
