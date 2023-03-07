// This must remain in the same directory as user_agent.yaml. This is because of
// how we specify the location of the user agent YAML file. What we should do
// long-term is:
// 1. Contribute an extra constructor to that repo which takes the
// YAML file contents as a string instead of the file path.
// 2. Inline the contents here and use that constructor instead.

#include "singletons/user_agent.h"

#include <filesystem>

namespace delivery {
UserAgentSingleton::UserAgentSingleton() {
  // This is a macro for indicating the path of this source file.
  const std::string file_path = __FILE__;
  std::filesystem::path parent_path =
      std::filesystem::path(file_path).parent_path();
  parser_ = std::make_unique<uap_cpp::UserAgentParser>(parent_path /
                                                       "user_agent.yaml");
}

UserAgent UserAgentSingleton::parse(const std::string& user_agent) const {
  UserAgent first_party_user_agent;
  uap_cpp::UserAgent third_party_user_agent = parser_->parse(user_agent);
  first_party_user_agent.os = std::move(third_party_user_agent.os.family);
  first_party_user_agent.app = std::move(third_party_user_agent.browser.family);
  return first_party_user_agent;
}
}  // namespace delivery
