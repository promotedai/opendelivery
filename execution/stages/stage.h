// A stage can be thought of as a discrete unit of work. Inputs and outputs are
// specified as parameters of derived class constructors, and should be owned by
// something other than the stage. A stage can assume that its inputs are ready
// to be used by the time run() is called.
//
// It does not need to overload run() unless it starts async work which it also
// needs to handle the response of.

#pragma once

#include <stddef.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <vector>

namespace delivery {
class Stage {
 public:
  // This ID is used by executors to identify this stage. Users are responsible
  // for ensuring uniqueness.
  explicit Stage(size_t id) : id_(id) {}

  virtual ~Stage() {}

  // Probably just for debugging purposes.
  virtual std::string name() const = 0;

  // `done_cb` is called to signal downstream stages may be run and must be
  // called. `timeout_cb` can be used to interrupt other async calls and is
  // optional.
  virtual void run(
      std::function<void()>&& done_cb,
      std::function<void(const std::chrono::duration<double>& delay,
                         std::function<void()>&& cb)>&& timeout_cb) {
    runSync();
    done_cb();
  }

  virtual void runSync() = 0;

  size_t id() const { return id_; }

  const std::vector<std::string>& errors() const { return errors_; }

 protected:
  // Errors in C++ are usually represented either by throwing exceptions or
  // returning status codes, but these don't fit in an event loop world. Either
  // method will bubble up to Drogon's event loops, which can't protect us from
  // ourselves.
  //
  // Instead a stage does what it can to not blow up, stashes strings here and
  // relies on the executor (which has access to the global loggers) to log.
  std::vector<std::string> errors_;

 private:
  size_t id_;
};
}  // namespace delivery
