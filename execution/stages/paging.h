// These stages are responsible for:
// - Reading previous insertion allocations from Redis
// - Writing new insertion allocations from Redis
// - Trimming allocations when there are too many

#pragma once

#include <stddef.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "execution/stages/redis_client.h"
#include "execution/stages/stage.h"

namespace delivery {
class Insertion;
class Request;
class Response;
struct PagingConfig;
struct PagingContext;
}  // namespace delivery

namespace delivery {
// Available here for testing.
std::string makePagingKey(const PagingConfig& paging_config,
                          const delivery::Request& req);
void initCurrPage(PagingContext& paging_context,
                  std::vector<std::string>& errors,
                  const delivery::Request& req,
                  const std::vector<delivery::Insertion>& insertions);
void processPastAllocs(PagingContext& paging_context,
                       std::vector<std::string>& errors,
                       const delivery::Request& req,
                       const std::vector<delivery::Insertion>& insertions,
                       const std::vector<std::string>& allocs,
                       bool limit_to_req_insertions);
void getInsertionsWhichCanBeOnCurrPage(
    PagingContext& paging_context,
    std::vector<delivery::Insertion>& insertions);
std::vector<std::string> makeAllocs(PagingContext& paging_context,
                                    const delivery::Response& resp);

class ReadFromPagingStage : public Stage {
 public:
  ReadFromPagingStage(size_t id, std::unique_ptr<RedisClient> client,
                      const PagingConfig& paging_config,
                      const delivery::Request& req,
                      std::vector<delivery::Insertion>& insertions,
                      PagingContext& paging_context)
      : Stage(id),
        client_(std::move(client)),
        paging_config_(paging_config),
        req_(req),
        insertions_(insertions),
        paging_context_(paging_context) {}
  std::string name() const override { return "ReadFromPaging"; }

  void runSync() override;

  void run(std::function<void()>&& done_cb,
           std::function<void(const std::chrono::duration<double>&,
                              std::function<void()>&&)>&&) override;

 private:
  std::unique_ptr<RedisClient> client_;
  const PagingConfig& paging_config_;
  const delivery::Request& req_;
  std::vector<delivery::Insertion>& insertions_;
  PagingContext& paging_context_;
  // This is the one from run(), which calls back to the executor.
  std::function<void()> done_cb_;
  std::vector<std::string> allocs_;
};

class WriteToPagingStage : public Stage {
 public:
  WriteToPagingStage(size_t id, std::unique_ptr<RedisClient> client,
                     const PagingConfig& paging_config,
                     const delivery::Response& resp,
                     PagingContext& paging_context)
      : Stage(id),
        client_(std::move(client)),
        paging_config_(paging_config),
        resp_(resp),
        paging_context_(paging_context) {}
  std::string name() const override { return "WriteToPaging"; }

  void runSync() override;

 private:
  // We degrade this to a shared_ptr to pass into chained callbacks.
  std::shared_ptr<RedisClient> client_;
  const PagingConfig& paging_config_;
  const delivery::Response& resp_;
  PagingContext& paging_context_;
};
}  // namespace delivery
