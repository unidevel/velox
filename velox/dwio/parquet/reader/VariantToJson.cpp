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

#include <algorithm>
#include <cctype>
#include <cstring>

#include <folly/Conv.h>
#include <folly/json.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/encode/Base64.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

namespace facebook::velox::parquet {
namespace {

// A variant value is a one-byte header followed by content. The low
// 'kBasicTypeBits' bits of the header are the basic type, the remaining bits
// are the type info, whose meaning depends on the basic type.
constexpr uint8_t kBasicTypeBits = 2;
constexpr uint8_t kBasicTypeMask = 0x3;
constexpr uint8_t kTypeInfoMask = 0x3F;

enum BasicType : uint8_t {
  // The type info is one of 'PrimitiveType'.
  kPrimitive = 0,
  // A string whose byte size is the type info. The content directly follows the
  // header byte.
  kShortString = 1,
  // The content is a size, a list of field ids into the metadata dictionary, a
  // list of field offsets and the field data. The type info is 0_b4_b3b2_b1b0,
  // where b4 selects a 1 or 4 byte size, b3b2 the byte width of the ids and
  // b1b0 the byte width of the offsets.
  kObject = 2,
  // Like 'kObject' without the field ids. The type info is 000_b2_b1b0, where
  // b2 selects a 1 or 4 byte size and b1b0 the byte width of the offsets.
  kArray = 3,
};

enum PrimitiveType : uint8_t {
  kNull = 0,
  kTrue = 1,
  kFalse = 2,
  // Little-endian signed integers of 1, 2, 4 and 8 bytes.
  kInt1 = 3,
  kInt2 = 4,
  kInt4 = 5,
  kInt8 = 6,
  kDouble = 7,
  // A one-byte scale followed by a little-endian signed unscaled value of 4, 8
  // or 16 bytes.
  kDecimal4 = 8,
  kDecimal8 = 9,
  kDecimal16 = 10,
  // Days since the epoch, as a 4 byte signed integer.
  kDate = 11,
  // Microseconds since the epoch, as an 8 byte signed integer.
  kTimestamp = 12,
  // Like 'kTimestamp' but always to be interpreted as UTC.
  kTimestampNtz = 13,
  kFloat = 14,
  // A 4 byte unsigned size followed by that many bytes of content.
  kBinary = 15,
  kLongString = 16,
};

// Byte width of the 4 byte unsigned integers of the encoding.
constexpr int32_t kU32Size = 4;

// The version is in the low 4 bits of the first metadata byte.
constexpr uint8_t kVersionMask = 0x0F;
constexpr uint8_t kSupportedVersion = 1;

// The largest scale a decimal can have, and hence the largest number of
// fraction digits appendDecimal() can produce.
constexpr uint8_t kMaxDecimalScale = 38;

void checkRange(std::string_view bytes, int64_t position, int64_t size) {
  VELOX_USER_CHECK_GE(position, 0, "Malformed variant: negative offset");
  VELOX_USER_CHECK_LE(
      position + size,
      static_cast<int64_t>(bytes.size()),
      "Malformed variant: reading {} bytes at offset {} of a {} byte binary",
      size,
      position,
      bytes.size());
}

uint8_t byteAt(std::string_view bytes, int64_t position) {
  checkRange(bytes, position, 1);
  return static_cast<uint8_t>(bytes[position]);
}

// Returns the little-endian signed integer in 'numBytes' bytes at 'position'.
int64_t readLong(std::string_view bytes, int64_t position, int32_t numBytes) {
  checkRange(bytes, position, numBytes);
  uint64_t result = 0;
  for (int32_t i = 0; i < numBytes - 1; ++i) {
    result |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[position + i]))
        << (8 * i);
  }
  // The most significant byte carries the sign.
  const auto mostSignificant = static_cast<uint64_t>(static_cast<int64_t>(
      static_cast<int8_t>(bytes[position + numBytes - 1])));
  result |= mostSignificant << (8 * (numBytes - 1));
  return static_cast<int64_t>(result);
}

// Returns the little-endian unsigned integer in 'numBytes' bytes at 'position'.
int64_t
readUnsigned(std::string_view bytes, int64_t position, int32_t numBytes) {
  checkRange(bytes, position, numBytes);
  int64_t result = 0;
  for (int32_t i = 0; i < numBytes; ++i) {
    result |= static_cast<int64_t>(static_cast<uint8_t>(bytes[position + i]))
        << (8 * i);
  }
  return result;
}

// Returns the 16 byte little-endian signed integer at 'position'.
int128_t readInt128(std::string_view bytes, int64_t position) {
  checkRange(bytes, position, 16);
  __uint128_t result = 0;
  for (int32_t i = 0; i < 15; ++i) {
    result |=
        static_cast<__uint128_t>(static_cast<uint8_t>(bytes[position + i]))
        << (8 * i);
  }
  const auto mostSignificant = static_cast<__uint128_t>(
      static_cast<int128_t>(static_cast<int8_t>(bytes[position + 15])));
  result |= mostSignificant << (8 * 15);
  return static_cast<int128_t>(result);
}

// Returns the key at 'id' in the metadata dictionary.
std::string_view metadataKey(std::string_view metadata, int64_t id) {
  checkRange(metadata, 0, 1);
  // The byte width of the dictionary offsets is in the high 2 bits of the
  // header.
  const int32_t offsetSize =
      ((static_cast<uint8_t>(metadata[0]) >> 6) & 0x3) + 1;
  const auto dictionarySize = readUnsigned(metadata, 1, offsetSize);
  VELOX_USER_CHECK_LT(
      id,
      dictionarySize,
      "Malformed variant: field id out of range of the {} key metadata dictionary",
      dictionarySize);
  // The header byte, the dictionary size and 'dictionarySize' + 1 offsets come
  // before the key data.
  const int64_t keysStart = 1 + (dictionarySize + 2) * offsetSize;
  const auto offset =
      readUnsigned(metadata, 1 + (id + 1) * offsetSize, offsetSize);
  const auto nextOffset =
      readUnsigned(metadata, 1 + (id + 2) * offsetSize, offsetSize);
  VELOX_USER_CHECK_LE(
      offset, nextOffset, "Malformed variant: decreasing metadata offsets");
  checkRange(metadata, keysStart + offset, nextOffset - offset);
  return metadata.substr(keysStart + offset, nextOffset - offset);
}

// Returns the string at 'position', which is either a short or a long string.
std::string_view readString(
    std::string_view value,
    int64_t position,
    uint8_t basicType,
    uint8_t typeInfo) {
  int64_t start;
  int64_t size;
  if (basicType == kShortString) {
    start = position + 1;
    size = typeInfo;
  } else {
    start = position + 1 + kU32Size;
    size = readUnsigned(value, position + 1, kU32Size);
  }
  checkRange(value, start, size);
  return value.substr(start, size);
}

void appendQuoted(std::string_view text, std::string& json) {
  json += '"';
  json += text;
  json += '"';
}

// Appends 'unscaled' * 10^-'scale' without an exponent and without trailing
// zeros in the fraction.
void appendDecimal(int128_t unscaled, uint8_t scale, std::string& json) {
  VELOX_USER_CHECK_LE(
      scale, kMaxDecimalScale, "Malformed variant: decimal scale {}", scale);
  const bool negative = unscaled < 0;
  auto magnitude = negative ? ~static_cast<__uint128_t>(unscaled) + 1
                            : static_cast<__uint128_t>(unscaled);

  std::string digits;
  do {
    digits += static_cast<char>('0' + static_cast<uint8_t>(magnitude % 10));
    magnitude /= 10;
  } while (magnitude != 0);
  std::reverse(digits.begin(), digits.end());
  if (digits.size() <= scale) {
    digits.insert(0, scale + 1 - digits.size(), '0');
  }

  std::string_view fraction(digits.data() + digits.size() - scale, scale);
  while (!fraction.empty() && fraction.back() == '0') {
    fraction.remove_suffix(1);
  }
  if (negative) {
    json += '-';
  }
  json += std::string_view(digits.data(), digits.size() - scale);
  if (!fraction.empty()) {
    json += '.';
    json += fraction;
  }
}

// Appends a float or a double. Always emits a fraction or an exponent, so that
// the JSON text of a floating point value is not mistaken for an integer.
template <typename T>
void appendFloatingPoint(T value, std::string& json) {
  const auto text = folly::to<std::string>(value);
  json += text;
  if (text.find_first_of(".eE") == std::string::npos &&
      std::isdigit(static_cast<unsigned char>(text.back()))) {
    json += ".0";
  }
}

void appendTimestamp(int64_t micros, bool withTimeZone, std::string& json) {
  TimestampToStringOptions options;
  options.precision = TimestampToStringOptions::Precision::kMicroseconds;
  options.dateTimeSeparator = ' ';
  options.skipTrailingZeros = true;
  json += '"';
  json += Timestamp::fromMicros(micros).toString(options);
  if (withTimeZone) {
    json += "+00:00";
  }
  json += '"';
}

void appendValue(
    std::string_view value,
    std::string_view metadata,
    int64_t position,
    std::string& json);

void appendObject(
    std::string_view value,
    std::string_view metadata,
    int64_t position,
    uint8_t typeInfo,
    std::string& json) {
  const int32_t sizeBytes = ((typeInfo >> 4) & 0x1) != 0 ? kU32Size : 1;
  const auto numFields = readUnsigned(value, position + 1, sizeBytes);
  const int32_t idSize = ((typeInfo >> 2) & 0x3) + 1;
  const int32_t offsetSize = (typeInfo & 0x3) + 1;
  const int64_t idsStart = position + 1 + sizeBytes;
  const int64_t offsetsStart = idsStart + numFields * idSize;
  const int64_t dataStart = offsetsStart + (numFields + 1) * offsetSize;

  json += '{';
  for (int64_t i = 0; i < numFields; ++i) {
    if (i != 0) {
      json += ',';
    }
    const auto id = readUnsigned(value, idsStart + i * idSize, idSize);
    const auto offset =
        readUnsigned(value, offsetsStart + i * offsetSize, offsetSize);
    folly::json::escapeString(metadataKey(metadata, id), json, {});
    json += ':';
    appendValue(value, metadata, dataStart + offset, json);
  }
  json += '}';
}

void appendArray(
    std::string_view value,
    std::string_view metadata,
    int64_t position,
    uint8_t typeInfo,
    std::string& json) {
  const int32_t sizeBytes = ((typeInfo >> 2) & 0x1) != 0 ? kU32Size : 1;
  const auto numElements = readUnsigned(value, position + 1, sizeBytes);
  const int32_t offsetSize = (typeInfo & 0x3) + 1;
  const int64_t offsetsStart = position + 1 + sizeBytes;
  const int64_t dataStart = offsetsStart + (numElements + 1) * offsetSize;

  json += '[';
  for (int64_t i = 0; i < numElements; ++i) {
    if (i != 0) {
      json += ',';
    }
    const auto offset =
        readUnsigned(value, offsetsStart + i * offsetSize, offsetSize);
    appendValue(value, metadata, dataStart + offset, json);
  }
  json += ']';
}

void appendPrimitive(
    std::string_view value,
    int64_t position,
    uint8_t typeInfo,
    std::string& json) {
  switch (typeInfo) {
    case kNull:
      json += "null";
      return;
    case kTrue:
      json += "true";
      return;
    case kFalse:
      json += "false";
      return;
    case kInt1:
      json += folly::to<std::string>(readLong(value, position + 1, 1));
      return;
    case kInt2:
      json += folly::to<std::string>(readLong(value, position + 1, 2));
      return;
    case kInt4:
      json += folly::to<std::string>(readLong(value, position + 1, 4));
      return;
    case kInt8:
      json += folly::to<std::string>(readLong(value, position + 1, 8));
      return;
    case kFloat: {
      const auto bits = static_cast<int32_t>(readLong(value, position + 1, 4));
      float number;
      std::memcpy(&number, &bits, sizeof(number));
      appendFloatingPoint(number, json);
      return;
    }
    case kDouble: {
      const auto bits = readLong(value, position + 1, 8);
      double number;
      std::memcpy(&number, &bits, sizeof(number));
      appendFloatingPoint(number, json);
      return;
    }
    case kDecimal4: {
      const auto scale = byteAt(value, position + 1);
      appendDecimal(readLong(value, position + 2, 4), scale, json);
      return;
    }
    case kDecimal8: {
      const auto scale = byteAt(value, position + 1);
      appendDecimal(readLong(value, position + 2, 8), scale, json);
      return;
    }
    case kDecimal16: {
      const auto scale = byteAt(value, position + 1);
      appendDecimal(readInt128(value, position + 2), scale, json);
      return;
    }
    case kDate:
      appendQuoted(
          DATE()->toString(
              static_cast<int32_t>(readLong(value, position + 1, 4))),
          json);
      return;
    case kTimestamp:
      appendTimestamp(readLong(value, position + 1, 8), true, json);
      return;
    case kTimestampNtz:
      appendTimestamp(readLong(value, position + 1, 8), false, json);
      return;
    case kBinary: {
      const auto size = readUnsigned(value, position + 1, kU32Size);
      const int64_t start = position + 1 + kU32Size;
      checkRange(value, start, size);
      appendQuoted(encoding::Base64::encode(value.data() + start, size), json);
      return;
    }
    case kLongString:
      folly::json::escapeString(
          readString(value, position, kPrimitive, typeInfo), json, {});
      return;
    default:
      VELOX_USER_FAIL("Malformed variant: unknown primitive type {}", typeInfo);
  }
}

void appendValue(
    std::string_view value,
    std::string_view metadata,
    int64_t position,
    std::string& json) {
  checkRange(value, position, 1);
  const auto header = static_cast<uint8_t>(value[position]);
  const uint8_t basicType = header & kBasicTypeMask;
  const uint8_t typeInfo = (header >> kBasicTypeBits) & kTypeInfoMask;
  switch (basicType) {
    case kShortString:
      folly::json::escapeString(
          readString(value, position, basicType, typeInfo), json, {});
      return;
    case kObject:
      appendObject(value, metadata, position, typeInfo, json);
      return;
    case kArray:
      appendArray(value, metadata, position, typeInfo, json);
      return;
    default:
      appendPrimitive(value, position, typeInfo, json);
      return;
  }
}

} // namespace

void appendVariantAsJson(
    std::string_view value,
    std::string_view metadata,
    std::string& json) {
  checkRange(metadata, 0, 1);
  const uint8_t version = static_cast<uint8_t>(metadata[0]) & kVersionMask;
  VELOX_USER_CHECK_EQ(
      version,
      kSupportedVersion,
      "Unsupported variant encoding version, only version {} is supported",
      kSupportedVersion);
  appendValue(value, metadata, 0, json);
}

} // namespace facebook::velox::parquet
