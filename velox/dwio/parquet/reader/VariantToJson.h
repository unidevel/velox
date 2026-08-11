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

#pragma once

#include <string>
#include <string_view>

namespace facebook::velox::parquet {

/// Decodes a binary variant and appends its JSON text to 'json'. 'value' and
/// 'metadata' are the two binary fields of a variant, encoded as specified by
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md.
/// Throws a user error if either binary is malformed.
///
/// Timestamps are rendered in UTC, since a file reader has no access to the
/// session time zone.
void appendVariantAsJson(
    std::string_view value,
    std::string_view metadata,
    std::string& json);

} // namespace facebook::velox::parquet
