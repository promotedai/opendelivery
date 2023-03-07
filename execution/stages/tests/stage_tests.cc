#include <stddef.h>

#include <functional>
#include <string>

#include "execution/stages/stage.h"
#include "gtest/gtest.h"

namespace delivery {
class TestStage : public Stage {
 public:
  explicit TestStage(size_t id) : Stage(id) {}

  std::string name() const override { return "Test"; }

  void run(std::function<void()>&& done_cb,
           std::function<void(const std::chrono::duration<double>&,
                              std::function<void()>&&)>&&) override {
    runSync();
    done_cb();
  }

  void runSync() override { ran_sync = true; }

  bool ran = false;
  bool ran_sync = false;
};

TEST(StageTest, Run) {
  TestStage stage(0);
  stage.run(
      [&stage] { stage.ran = true; },
      [](const std::chrono::duration<double>&, std::function<void()>&&) {});
  EXPECT_TRUE(stage.ran_sync);
  EXPECT_TRUE(stage.ran);
}
}  // namespace delivery
