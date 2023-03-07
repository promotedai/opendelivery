#define DROGON_TEST_MAIN

#include <stddef.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "drogon/HttpAppFramework.h"
#include "drogon/drogon_test.h"
#include "execution/simple_executor.h"
#include "execution/stages/stage.h"
#include "trantor/net/EventLoop.h"

using ::delivery::SimpleExecutor;
using ::delivery::SimpleExecutorBuilder;
using ::drogon::app;

// Although the Drogon testing framework is still better than gtest for using
// the event loops, it doesn't seem to offer any way of preventing the main loop
// from quit()ing if your events can recursively create events. This just uses
// locking to make that guarantee. Can factor it out if we need more tests based
// on the event loops.
class TestCoordinator {
 public:
  void startTest() {
    std::lock_guard<std::mutex> lk(m_);
    ++remaining_tests_;
  }

  void finishTest() {
    {
      std::lock_guard<std::mutex> lk(m_);
      --remaining_tests_;
    }
    cv_.notify_all();
  }

  void waitForAllFinished() {
    std::unique_lock<std::mutex> lk(m_);
    if (remaining_tests_ > 0) {
      cv_.wait(lk, [this] { return remaining_tests_ == 0; });
    }
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  size_t remaining_tests_ = 0;
};
TestCoordinator tc;

struct TestContext {
  std::unique_ptr<SimpleExecutor> executor;
  std::atomic<size_t> stages_ran = 0;
};

class TestStage : public delivery::Stage {
 public:
  TestStage(size_t id, TestContext& context,
            std::function<void(TestContext&)>&& check_cb)
      : delivery::Stage(id),
        context_(context),
        check_cb_(std::move(check_cb)) {}

  std::string name() const override { return "Test"; }

  void runSync() override {
    // Just blow up if context lifetime is mishandled.
    check_cb_(context_);
    ++context_.stages_ran;
  }

 private:
  TestContext& context_;
  std::function<void(TestContext&)> check_cb_;
};

// This stage entrusts its work to another thread to emulate calling an async
// client library.
class TestOtherThreadStage : public delivery::Stage {
 public:
  TestOtherThreadStage(size_t id, TestContext& context,
                       std::function<void(TestContext&)>&& check_cb)
      : delivery::Stage(id),
        context_(context),
        check_cb_(std::move(check_cb)) {}

  std::string name() const override { return "TestOtherThread"; }

  void runSync() override {}

  void run(std::function<void()>&& done_cb,
           std::function<void(const std::chrono::duration<double>&,
                              std::function<void()>&&)>&&) override {
    std::thread thr([this, done_cb]() {
      check_cb_(context_);
      ++context_.stages_ran;
      done_cb();
    });
    thr.detach();
  }

 private:
  TestContext& context_;
  std::function<void(TestContext&)> check_cb_;
};

// This stage entrusts its work to a timeout-based callback.
class TestTimeoutStage : public delivery::Stage {
 public:
  TestTimeoutStage(size_t id, TestContext& context,
                   std::function<void(TestContext&)>&& check_cb)
      : delivery::Stage(id),
        context_(context),
        check_cb_(std::move(check_cb)) {}

  std::string name() const override { return "TestTimeout"; }

  void runSync() override {}

  void run(
      std::function<void()>&& done_cb,
      std::function<void(const std::chrono::duration<double>& delay,
                         std::function<void()>&& cb)>&& timeout_cb) override {
    timeout_cb(std::chrono::milliseconds(1), [this, done_cb]() {
      check_cb_(context_);
      ++context_.stages_ran;
      done_cb();
    });
  }

 private:
  TestContext& context_;
  std::function<void(TestContext&)> check_cb_;
};

// SimpleExecutor throws all events on the loop of the current thread, but this
// function is called by a thread without a loop. So we make our individual
// tests events on the main loop to begin with.
DROGON_TEST(SimpleExecutorTest) {
  // Linear graph.
  tc.startTest();
  app().getLoop()->queueInLoop([TEST_CTX] {
    auto context = std::make_shared<TestContext>();
    SimpleExecutorBuilder builder;
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/0, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 0);
                         }),
                     /*input_ids=*/{});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/1, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 1);
                         }),
                     /*input_ids=*/{0});
    auto& executor = context->executor;
    executor = builder.build([context]() mutable {
      // Destroy context before marking the test as finished.
      context.reset();
      tc.finishTest();
    });
    executor->execute();
  });

  // Diamond graph.
  tc.startTest();
  app().getLoop()->queueInLoop([TEST_CTX] {
    auto context = std::make_shared<TestContext>();
    SimpleExecutorBuilder builder;
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/0, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 0);
                         }),
                     /*input_ids=*/{});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/1, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran >= 1);
                         }),
                     /*input_ids=*/{0});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/2, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran >= 1);
                         }),
                     /*input_ids=*/{0});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/3, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 3);
                         }),
                     /*input_ids=*/{1, 2});
    auto& executor = context->executor;
    executor = builder.build([context]() mutable {
      context.reset();
      tc.finishTest();
    });
    executor->execute();
  });

  // Automatic final stage.
  tc.startTest();
  app().getLoop()->queueInLoop([TEST_CTX] {
    auto context = std::make_shared<TestContext>();
    SimpleExecutorBuilder builder;
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/0, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 0);
                         }),
                     /*input_ids=*/{});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/1, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran >= 1);
                         }),
                     /*input_ids=*/{0});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/2, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran >= 1);
                         }),
                     /*input_ids=*/{0});
    auto& executor = context->executor;
    executor = builder.build([context]() mutable {
      context.reset();
      tc.finishTest();
    });
    executor->execute();
  });

  // Test thread-safeness when a stage passes its after-run callback to another
  // thread.
  tc.startTest();
  app().getLoop()->queueInLoop([TEST_CTX] {
    auto context = std::make_shared<TestContext>();
    SimpleExecutorBuilder builder;
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/0, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 0);
                         }),
                     /*input_ids=*/{});
    builder.addStage(std::make_unique<TestOtherThreadStage>(
                         /*stage_id=*/1, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran >= 1);
                         }),
                     /*input_ids=*/{0});
    builder.addStage(std::make_unique<TestOtherThreadStage>(
                         /*stage_id=*/2, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran >= 1);
                         }),
                     /*input_ids=*/{0});
    builder.addStage(std::make_unique<TestStage>(
                         /*stage_id=*/3, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 3);
                         }),
                     /*input_ids=*/{1, 2});
    auto& executor = context->executor;
    executor = builder.build([context]() mutable {
      context.reset();
      tc.finishTest();
    });
    executor->execute();
  });

  // Test that a timeout can be scheduled correctly. If not, this test will
  // hang.
  tc.startTest();
  app().getLoop()->queueInLoop([TEST_CTX] {
    auto context = std::make_shared<TestContext>();
    SimpleExecutorBuilder builder;
    builder.addStage(std::make_unique<TestTimeoutStage>(
                         /*stage_id=*/0, *context,
                         [TEST_CTX](TestContext& context) {
                           CHECK(context.stages_ran == 0);
                         }),
                     /*input_ids=*/{});
    auto& executor = context->executor;
    executor = builder.build([context]() mutable {
      context.reset();
      tc.finishTest();
    });
    executor->execute();
  });
}

int main(int argc, char** argv) {
  // Boilerplate to run the server in another thread.
  std::promise<void> p1;
  std::future<void> f1 = p1.get_future();

  std::thread thr([&]() {
    // Queues the promise to be fulfilled after starting the loop.
    app().getLoop()->queueInLoop([&p1]() { p1.set_value(); });
    app().run();
  });

  // The future is only satisfied after the event loop started.
  f1.get();
  int status = drogon::test::run(argc, argv);

  // Don't quit too soon or our checks never happen.
  tc.waitForAllFinished();
  // Ask the event loop to shutdown and wait.
  app().getLoop()->queueInLoop([]() { app().quit(); });
  thr.join();

  return status;
}
