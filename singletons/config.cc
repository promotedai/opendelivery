#include "singletons/config.h"

#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/GetObjectResult.h>
#include <json/reader.h>
#include <stdlib.h>

#include <algorithm>
#include <fstream>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "aws/s3/model/GetObjectRequest.h"
#include "config/json.h"
#include "config/platform_config.h"
#include "singletons/aws.h"
#include "singletons/env.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"

namespace delivery {
ConfigSingleton::ConfigSingleton() {
  const auto& all_vars = EnvSingleton::getInstance().getAllVars();
  const auto& config_paths = EnvSingleton::getInstance().getConfigPaths();
  if (config_paths.empty()) {
    LOG_FATAL << "No configs specified";
    abort();
  }
  for (const auto& path : config_paths) {
    std::unique_ptr<ConfigLoader> loader = ConfigLoader::create(path);
    std::string raw_json = loader->load();
    Json::Value json = toJson(replaceEnvVar(raw_json, all_vars));
    if (json.isNull()) {
      LOG_FATAL << "Invalid config: " << path;
      abort();
    }
    applyJson(mother_, json);
  }
  LOG_INFO << "Initial configuration successful";
}

void parseS3Path(std::string_view path, std::string& region,
                 std::string& bucket, std::string& object_key) {
  size_t colon_location = path.find(':');
  if (colon_location != std::string::npos) {
    region = path.substr(0, colon_location);
    std::string_view remainder = path.substr(colon_location + 1);
    size_t slash_location = remainder.find('/');
    bucket = remainder.substr(0, slash_location);
    object_key = remainder.substr(slash_location + 1);
  }
}

std::unique_ptr<ConfigSingleton::ConfigLoader>
ConfigSingleton::ConfigLoader::create(std::string_view path) {
  static const std::string s3_prefix = "s3:";
  static const std::string file_prefix = "file:";

  if (absl::StartsWith(path, s3_prefix)) {
    auto loader = std::make_unique<S3ConfigLoader>();
    parseS3Path(path.substr(s3_prefix.size()), loader->region, loader->bucket,
                loader->object_key);
    if (!loader->region.empty() && !loader->bucket.empty() &&
        !loader->object_key.empty()) {
      return loader;
    }
  } else if (absl::StartsWith(path, file_prefix)) {
    auto loader = std::make_unique<FileConfigLoader>();
    loader->name = path.substr(file_prefix.size());
    if (!loader->name.empty()) {
      return loader;
    }
  }
  LOG_FATAL << "Invalid config path: " << std::string(path);
  abort();
}

std::string ConfigSingleton::S3ConfigLoader::load() {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(object_key);
  Aws::S3::Model::GetObjectOutcome outcome =
      AwsSingleton::getInstance().getS3Client(region).GetObject(request);

  if (!outcome.IsSuccess()) {
    LOG_ERROR << "Response error from S3: " << outcome.GetError().GetMessage();
    // Treat empty config as invalid.
    return "";
  }
  // No retry or checksum for now.
  std::stringstream buffer;
  buffer << outcome.GetResult().GetBody().rdbuf();
  return buffer.str();
}

std::string ConfigSingleton::FileConfigLoader::load() {
  std::ifstream input(name);
  if (input.fail()) {
    // Treat empty config as invalid.
    return "";
  }
  std::stringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string ConfigSingleton::replaceEnvVar(
    std::string_view config,
    const absl::flat_hash_map<std::string, std::string>& env_vars) {
  std::vector<std::pair<std::string, std::string>> replacements;
  replacements.reserve(env_vars.size());
  for (const auto& kv : env_vars) {
    replacements.emplace_back(absl::StrCat("{{.", kv.first, "}}"), kv.second);
  }
  return absl::StrReplaceAll(config, replacements);
}

Json::Value ConfigSingleton::toJson(const std::string& config) {
  Json::Value ret;

  Json::CharReaderBuilder builder;
  Json::CharReader* reader = builder.newCharReader();
  std::string errors;
  bool success = reader->parse(config.c_str(), config.c_str() + config.size(),
                               &ret, &errors);
  delete reader;
  if (!success) {
    return Json::Value::nullSingleton();
  }

  return ret;
}
}  // namespace delivery
