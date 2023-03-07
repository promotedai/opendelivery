// There actually are comprehensive JSON-in-C++ libraries out there (e.g.
// https://github.com/nlohmann/json), but from brief review they all have
// significant drawbacks. (e.g. https://github.com/nlohmann/json/pull/2229)
//
// We already have JsonCpp as part of Drogon, so we just need some reflection on
// top of it to transcode. C++ doesn't give reflection automatically, but it
// does give metaprogramming.
//
// To use this, given a class like:
// struct ExampleConfig {
//   uint64_t example_field;
// };
//
// Just add the following inside the class:
//   constexpr static auto properties = std::make_tuple(
//       property(&ExampleConfig::example_field, "exampleField"));
//
// Things to note:
// - Fields which are optional (nullable), and don't have a default value, must
//   have their type wrapped in a std::optional<>.
// - Fields corresponding to arrays must have the std::vector<> type. Note
//   there's a limitation where array elements are required to have the same
//   type. JSON doesn't have this restriction, but it's annoying to support and
//   I don't think we use JSON like this anyway.
// - Fields which correspond to maps must have similar type traits to the std::
//   map types.

#pragma once

#include <iostream>
#include <map>
#include <optional>
#include <tuple>

#include "json/json.h"

namespace delivery {
template <typename T, T... S, typename F>
constexpr void for_sequence(std::integer_sequence<T, S...>, F&& f) {
  (static_cast<void>(f(std::integral_constant<T, S>{})), ...);
}

template <typename Class, typename T>
struct PropertyImpl {
  constexpr PropertyImpl(T Class::*member_, const char* name_)
      : member{member_}, name{name_} {}

  using Type = T;

  T Class::*member;
  const char* name;
};

template <typename Class, typename T>
constexpr auto property(T Class::*member, const char* name) {
  return PropertyImpl<Class, T>{member, name};
}

template <typename T>
struct is_optional : std::false_type {};
template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template <class T>
struct is_leaf
    : std::integral_constant<bool, !std::is_class_v<T> ||
                                       std::is_same_v<std::string, T>> {};

template <typename T>
struct is_vector : std::false_type {};
template <typename T>
struct is_vector<std::vector<T>> : std::true_type {};

template <typename T, typename U = void>
struct is_mappish_impl : std::false_type {};
template <typename T>
struct is_mappish_impl<
    T, std::void_t<typename T::key_type, typename T::mapped_type,
                   decltype(std::declval<T&>()
                                [std::declval<const typename T::key_type&>()])>>
    : std::true_type {};
template <typename T>
struct is_mappish : is_mappish_impl<T>::type {};

// Our config files currently represent fields which are meant to be entirely
// replaced with env vars as strings - even if the underlying types aren't - to
// keep JSON happy.
template <typename T>
T parseHelper(const std::string&);

template <>
inline uint64_t parseHelper<uint64_t>(const std::string& val) {
  return std::stoi(val);
}

template <>
inline int64_t parseHelper<int64_t>(const std::string& val) {
  return std::stoi(val);
}

template <>
inline double parseHelper<double>(const std::string& val) {
  return std::stod(val);
}

template <>
inline bool parseHelper<bool>(const std::string& val) {
  return val[0] == 'T' || val[0] == 't';
}

// This just uses the type metadata of our C++ type to traverse the JSON value.
// JSON fields which are not implemented in C++ will be ignored.
template <typename T>
void applyJson(T& result, const Json::Value& data) {
  if constexpr (is_optional<T>::value) {  // Non-defaulted optional JSON fields
                                          // go through here.
    if (data.isNull()) {
      result = std::nullopt;
    } else {
      result = typename T::value_type{};
      applyJson<typename T::value_type>(result.value(), data);
    }
  } else if constexpr (is_leaf<T>::value) {  // JSON numbers, strings, and bools
                                             // end up here.
    if (!std::is_same_v<std::string, T> && data.isString()) {
      result = parseHelper<T>(data.as<std::string>());
    } else {
      result = data.as<T>();
    }
  } else if constexpr (is_vector<T>::value) {  // JSON arrays go through here.
    result.clear();
    result.reserve(data.size());
    for (int i = 0; i < data.size(); ++i) {
      applyJson<typename T::value_type>(result.emplace_back(), data[i]);
    }
  } else if constexpr (is_mappish<T>::value) {  // JSON doesn't formally have a
                                                // map type. But when you think
                                                // about it, objects can be
                                                // interpreted as maps.
    result.reserve(data.size());
    for (const auto& name : data.getMemberNames()) {
      applyJson<typename T::mapped_type>(result[name], data[name]);
    }
  } else {  // JSON objects go through here.
    constexpr auto properties = std::tuple_size<decltype(T::properties)>::value;
    for_sequence(std::make_index_sequence<properties>{}, [&](auto i) {
      constexpr auto property = std::get<i>(T::properties);
      using Type = typename decltype(property)::Type;
      if (data.isMember(property.name)) {
        applyJson<Type>(result.*(property.member), data[property.name]);
      }
    });
  }
}
}  // namespace delivery
