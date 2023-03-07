// "Simple" here meaning the easiest thing to write and reason about:
// + Each instance belongs to a particular request. All stages need to be known
// before calling execute().
// + Runs all stages on the same event loop where it started. This
// is hard to summarize in a comment but I think it should actually work well.
// The event loops only handle the requests they can afford to, and without
// variable, synchronous waits work stealing won't accomplish much.
// + Doesn't do any optimizations, like immediately scheduling (as opposed to
// queueing) or inlining "simple" stages.
// - This implementation must be thread-safe. The after-run callback for a
// stage, which can queue the next one, can be handled by async client threads.
// Not keeping this implementation thread-safe would mean that those threads
// instead need to queue events which would then use the thread associated with
// the event loop to queue the next stage, and this is being considered harder
// to reason about.

#pragma once

#include <stddef.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/executor.h"
#include "proto/delivery/INTERNAL_execution.pb.h"

namespace trantor {
class EventLoop;
}

namespace delivery {
class Context;
class Stage;

// Whether feature store configs are for item or user feature stores is
// indicated by a "type" integer with no Protobuf definition currently.
const int item_feature_store_type = 1;
const int user_feature_store_type = 2;

// This should be preferred to directly using SimpleExecutorBuilder.
std::unique_ptr<Executor>& configureSimpleExecutor(
    std::unique_ptr<Context> context, const ConfigurationOptions& options);

class SimpleExecutor : public Executor {
 public:
  explicit SimpleExecutor(std::function<void()>&& clean_up_cb,
                          std::vector<ExecutorNode> nodes)
      : clean_up_cb_(std::move(clean_up_cb)), nodes_(std::move(nodes)) {}

  void execute() override;

  const std::vector<ExecutorNode>& nodes() const override { return nodes_; }

  // This returns the DOT (https://graphviz.org/doc/info/lang.html)
  // representation of the execution graph for visualization. This is just a
  // debug tool. Not necessarily unique to this class but putting it here to
  // minimize interface pollution.
  std::string dotString() const;

 private:
  void afterRun(ExecutorNode& curr_node);

  void scheduleTimeout(const std::chrono::duration<double>& delay,
                       std::function<void()>&& cb);

  trantor::EventLoop* loop_;
  std::function<void()> clean_up_cb_;
  std::vector<ExecutorNode> nodes_;
};

// This currently doesn't do any any checks for sanity or that stages are
// cohesively sensible.
class SimpleExecutorBuilder {
 public:
  SimpleExecutorBuilder() = default;

  // Each stage must have an ID that is both unique and non-negative.
  void addStage(
      std::unique_ptr<Stage> stage, const std::vector<size_t>& input_ids,
      delivery::DeliveryLatency_DeliveryMethod latency_tag =
          delivery::DeliveryLatency_DeliveryMethod_UNKNOWN_DELIVERY_METHOD);

  // The callback is run after all other stages and is responsible for
  // deallocation.
  std::unique_ptr<SimpleExecutor> build(std::function<void()>&& clean_up_cb);

  // Convenience overload of the above.
  std::unique_ptr<SimpleExecutor> build(std::unique_ptr<Context> context);

 private:
  // Index in the vector is equal to the stage ID for the node. Gaps are fine.
  std::vector<ExecutorNode> nodes_;
};
}  // namespace delivery
