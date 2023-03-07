#include "utils/network.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include "absl/strings/match.h"

namespace delivery {
// HTTP port.
const std::string service = "80";

std::string getIp(const std::string& hostname) {
  char ip[64] = "";

  struct addrinfo* info = nullptr;
  int error =
      getaddrinfo(hostname.c_str(), service.c_str(), /*hints=*/nullptr, &info);
  if (error == 0) {
    // Assume IPv4 for now.
    inet_ntop(AF_INET, &info->ai_addr->sa_data[2], ip, sizeof(ip));
  }
  if (info != nullptr) {
    freeaddrinfo(info);
  }

  return ip;
}

StructuredRedisUrl parseRedisUrl(const std::string& url) {
  StructuredRedisUrl structured_url;

  // All positions are absolute.
  size_t hostname_pos = 0;
  // Hiredis expects the scheme to not be present.
  const std::string redis_scheme = "redis://";
  if (absl::StartsWith(url, redis_scheme)) {
    hostname_pos += redis_scheme.size();
  }
  // Hostname and port have to be separated by a colon.
  size_t colon_pos = url.find(':', hostname_pos);
  if (colon_pos == std::string::npos) {
    structured_url.successful_parse = false;
    return structured_url;
  }
  size_t slash_pos = url.find('/', colon_pos + 1);

  structured_url.successful_parse = true;
  structured_url.hostname = url.substr(hostname_pos, colon_pos - hostname_pos);
  // The database number is optional and defaults to 0 when it's missing.
  if (slash_pos == std::string::npos) {
    structured_url.port = url.substr(colon_pos + 1);
    structured_url.database_number = "0";
  } else {
    structured_url.port =
        url.substr(colon_pos + 1, slash_pos - (colon_pos + 1));
    structured_url.database_number = url.substr(slash_pos + 1);
  }

  return structured_url;
}
}  // namespace delivery
