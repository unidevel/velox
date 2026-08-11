/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/parquet/reader/VariantToJson.h"

#include <gtest/gtest.h>

#include <cstring>

#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox::parquet {
namespace {

// Builders for the variant encoding, see
// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md. They
// use the narrowest header of every kind: version 1 metadata with one byte
// offsets, and objects and arrays with one byte sizes, ids and offsets.
std::string metadata(const std::vector<std::string>& keys) {
  std::string result;
  result += static_cast<char>(0x01);
  result += static_cast<char>(keys.size());
  size_t offset = 0;
  for (const auto& key : keys) {
    result += static_cast<char>(offset);
    offset += key.size();
  }
  result += static_cast<char>(offset);
  for (const auto& key : keys) {
    result += key;
  }
  return result;
}

// 'fields' pairs the id of a key in the metadata with the encoded field value.
std::string object(const std::vector<std::pair<uint8_t, std::string>>& fields) {
  std::string result;
  result += static_cast<char>(0x02);
  result += static_cast<char>(fields.size());
  for (const auto& field : fields) {
    result += static_cast<char>(field.first);
  }
  size_t offset = 0;
  for (const auto& field : fields) {
    result += static_cast<char>(offset);
    offset += field.second.size();
  }
  result += static_cast<char>(offset);
  for (const auto& field : fields) {
    result += field.second;
  }
  return result;
}

std::string array(const std::vector<std::string>& elements) {
  std::string result;
  result += static_cast<char>(0x03);
  result += static_cast<char>(elements.size());
  size_t offset = 0;
  for (const auto& element : elements) {
    result += static_cast<char>(offset);
    offset += element.size();
  }
  result += static_cast<char>(offset);
  for (const auto& element : elements) {
    result += element;
  }
  return result;
}

std::string primitive(uint8_t typeInfo, std::string_view content = {}) {
  std::string result;
  result += static_cast<char>(typeInfo << 2);
  result += content;
  return result;
}

template <typename T>
std::string littleEndian(T value) {
  std::string result(sizeof(T), '\0');
  std::memcpy(result.data(), &value, sizeof(T));
  return result;
}

std::string shortString(std::string_view text) {
  std::string result;
  result += static_cast<char>((text.size() << 2) | 0x01);
  result += text;
  return result;
}

std::string longString(std::string_view text) {
  return primitive(
      16, littleEndian(static_cast<int32_t>(text.size())) + std::string(text));
}

// Returns the JSON text of the variant made of 'value' and the metadata in
// 'keys'.
std::string toJson(
    const std::string& value,
    const std::string& keys = metadata({})) {
  std::string json;
  appendVariantAsJson(value, keys, json);
  return json;
}

class VariantToJsonTest : public testing::Test {};

TEST_F(VariantToJsonTest, primitives) {
  EXPECT_EQ(toJson(primitive(0)), "null");
  EXPECT_EQ(toJson(primitive(1)), "true");
  EXPECT_EQ(toJson(primitive(2)), "false");
  EXPECT_EQ(toJson(primitive(3, littleEndian<int8_t>(42))), "42");
  EXPECT_EQ(toJson(primitive(3, littleEndian<int8_t>(-1))), "-1");
  EXPECT_EQ(toJson(primitive(4, littleEndian<int16_t>(-1000))), "-1000");
  EXPECT_EQ(toJson(primitive(5, littleEndian<int32_t>(100000))), "100000");
  EXPECT_EQ(
      toJson(primitive(6, littleEndian<int64_t>(1234567890123))),
      "1234567890123");
}

TEST_F(VariantToJsonTest, floatingPoint) {
  EXPECT_EQ(toJson(primitive(14, littleEndian<float>(1.5f))), "1.5");
  EXPECT_EQ(toJson(primitive(7, littleEndian<double>(-0.25))), "-0.25");
  // A round value keeps a fraction so that it is not read back as an integer.
  EXPECT_EQ(toJson(primitive(7, littleEndian<double>(2.0))), "2.0");
}

TEST_F(VariantToJsonTest, decimals) {
  EXPECT_EQ(
      toJson(primitive(
          8,
          std::string(1, static_cast<char>(2)) + littleEndian<int32_t>(1234))),
      "12.34");
  // Trailing zeros of the fraction are dropped, as is the fraction if it is all
  // zeros.
  EXPECT_EQ(
      toJson(primitive(
          8,
          std::string(1, static_cast<char>(2)) + littleEndian<int32_t>(1200))),
      "12");
  EXPECT_EQ(
      toJson(primitive(
          9, std::string(1, static_cast<char>(3)) + littleEndian<int64_t>(-5))),
      "-0.005");
  EXPECT_EQ(
      toJson(primitive(
          10,
          std::string(1, static_cast<char>(0)) +
              littleEndian<int128_t>(static_cast<int128_t>(12345)))),
      "12345");
}

TEST_F(VariantToJsonTest, dateAndTimestamp) {
  EXPECT_EQ(
      toJson(primitive(11, littleEndian<int32_t>(19723))), "\"2024-01-01\"");
  EXPECT_EQ(
      toJson(primitive(13, littleEndian<int64_t>(1704067200123456))),
      "\"2024-01-01 00:00:00.123456\"");
  // A time zoned timestamp is rendered in UTC.
  EXPECT_EQ(
      toJson(primitive(12, littleEndian<int64_t>(1704067200123456))),
      "\"2024-01-01 00:00:00.123456+00:00\"");
}

TEST_F(VariantToJsonTest, strings) {
  EXPECT_EQ(toJson(shortString("abc")), "\"abc\"");
  EXPECT_EQ(toJson(shortString("")), "\"\"");
  EXPECT_EQ(toJson(shortString("a\"b")), "\"a\\\"b\"");
  EXPECT_EQ(toJson(shortString("a\nb")), "\"a\\nb\"");

  // Strings longer than the 63 bytes a short string can hold.
  const std::string long100(100, 'x');
  EXPECT_EQ(toJson(longString(long100)), "\"" + long100 + "\"");
}

TEST_F(VariantToJsonTest, binary) {
  EXPECT_EQ(
      toJson(primitive(15, littleEndian<int32_t>(3) + std::string("abc"))),
      "\"YWJj\"");
}

TEST_F(VariantToJsonTest, arrays) {
  EXPECT_EQ(toJson(array({})), "[]");
  EXPECT_EQ(
      toJson(array(
          {primitive(3, littleEndian<int8_t>(1)),
           shortString("two"),
           primitive(0)})),
      "[1,\"two\",null]");
  EXPECT_EQ(
      toJson(array({array({primitive(3, littleEndian<int8_t>(1))})})), "[[1]]");
}

TEST_F(VariantToJsonTest, objects) {
  EXPECT_EQ(toJson(object({})), "{}");

  const auto keys = metadata({"age", "name", "tags"});
  EXPECT_EQ(
      toJson(
          object(
              {{0, primitive(3, littleEndian<int8_t>(30))},
               {1, shortString("alice")},
               {2, array({shortString("admin")})}}),
          keys),
      "{\"age\":30,\"name\":\"alice\",\"tags\":[\"admin\"]}");

  // Objects nest.
  EXPECT_EQ(
      toJson(
          object({{1, object({{0, primitive(3, littleEndian<int8_t>(7))}})}}),
          keys),
      "{\"name\":{\"age\":7}}");
}

TEST_F(VariantToJsonTest, malformed) {
  // A value that claims a longer content than the binary holds.
  EXPECT_THROW(toJson(primitive(6, littleEndian<int32_t>(1))), VeloxUserError);
  // A field id that is not in the metadata dictionary.
  EXPECT_THROW(
      toJson(object({{5, primitive(1)}}), metadata({"age"})), VeloxUserError);
  // An unknown primitive type.
  EXPECT_THROW(toJson(primitive(30)), VeloxUserError);
  // An empty metadata binary, and an unsupported encoding version.
  EXPECT_THROW(toJson(primitive(1), ""), VeloxUserError);
  EXPECT_THROW(
      toJson(primitive(1), std::string(1, static_cast<char>(0x02))),
      VeloxUserError);
}

} // namespace
} // namespace facebook::velox::parquet
