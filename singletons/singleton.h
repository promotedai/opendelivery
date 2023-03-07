// To correctly inherit from this class:
// 1. Make this class a friend of the derived class
// 2. Give the derived class a private, default constructor

#pragma once

namespace delivery {
template <typename T>
class Singleton {
 public:
  static T &getInstance() {
    static T instance;
    return instance;
  }

  Singleton(const Singleton &) = delete;
  Singleton &operator=(const Singleton &) = delete;
  Singleton(Singleton &&) = delete;
  Singleton &operator=(Singleton &&) = delete;

 protected:
  Singleton() = default;
};
}  // namespace delivery
