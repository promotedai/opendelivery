// This stage is responsible for getting rankings from Personalize and
// structuring that data for downstream processing.

#pragma once

#include <stddef.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "config/personalize_config.h"
#include "execution/stages/personalize_client.h"
#include "execution/stages/stage.h"

namespace delivery {
class Insertion;
class Request;
struct UserAgent;
}  // namespace delivery

namespace delivery {
class ReadFromPersonalizeStage : public Stage {
 public:
  ReadFromPersonalizeStage(
      size_t id, std::unique_ptr<PersonalizeClient> client,
      const std::vector<PersonalizeConfig>& configs,
      const delivery::Request& req,
      const std::vector<delivery::Insertion>& insertions,
      const UserAgent& user_agent,
      absl::flat_hash_map<
          std::string, absl::flat_hash_map<std::string, std::pair<float, int>>>&
          campaign_to_scores_and_ranks)
      : Stage(id),
        client_(std::move(client)),
        configs_(configs),
        req_(req),
        insertions_(insertions),
        user_agent_(user_agent),
        campaign_to_scores_and_ranks_(campaign_to_scores_and_ranks) {}

  std::string name() const override { return "ReadFromPersonalize"; }

  void runSync() override;

  void run(std::function<void()>&& cb,
           std::function<void(const std::chrono::duration<double>& delay,
                              std::function<void()>&& cb)>&&) override;

 private:
  std::unique_ptr<PersonalizeClient> client_;
  const std::vector<PersonalizeConfig>& configs_;
  std::vector<PersonalizeConfig> enabled_configs_;
  const delivery::Request& req_;
  const std::vector<delivery::Insertion>& insertions_;
  const UserAgent& user_agent_;
  std::vector<std::vector<PersonalizeResult>> results_;
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<std::string, std::pair<float, int>>>&
      campaign_to_scores_and_ranks_;
  std::function<void()> cb_;
};

// Declared here for testing.
void convertResults(
    const std::vector<PersonalizeConfig>& configs,
    std::vector<std::vector<PersonalizeResult>>& all_results,
    absl::flat_hash_map<
        std::string, absl::flat_hash_map<std::string, std::pair<float, int>>>&
        campaign_to_scores_and_ranks);
}  // namespace delivery
