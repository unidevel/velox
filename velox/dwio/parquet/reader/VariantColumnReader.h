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

#include "velox/dwio/parquet/reader/StructColumnReader.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::parquet {

/// Reads a variant column as JSON text. A variant is stored as a group of two
/// binary fields, 'value' and 'metadata' (see
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md),
/// but engines that have no variant type request it as a single string column.
/// Delta Lake tables read through Presto are the case this supports. The two
/// fields are read with the inherited struct machinery and decoded into one
/// JSON string per row.
class VariantColumnReader : public StructColumnReader {
 public:
  /// The two binary fields making up a variant.
  static constexpr const char* kValueField = "value";
  static constexpr const char* kMetadataField = "metadata";

  VariantColumnReader(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec,
      memory::MemoryPool& pool);

  /// True if 'fileType' is a variant group whose JSON text is requested, i.e.
  /// 'requestedType' is a string type.
  static bool isVariantAsJson(
      const TypePtr& requestedType,
      const dwio::common::TypeWithId& fileType);

  void getValues(const RowSet& rows, VectorPtr* result) override;

  // Keeps the two fields off the top level, unlike the inherited struct
  // behavior. getValues() needs their values in place, so they must be read
  // eagerly instead of being deferred to a LazyVector of their own.
  void setIsTopLevel() override {
    isTopLevel_ = true;
  }

 private:
  // Type of the JSON text produced by 'this'. The inherited requestedType() is
  // the ROW type of the file instead, because the struct machinery builds the
  // child readers from it.
  const TypePtr jsonType_;

  dwio::common::SelectiveColumnReader* valueReader_;
  dwio::common::SelectiveColumnReader* metadataReader_;

  // Values of the two fields for the rows of the ongoing getValues(). Members
  // so that the vectors and the decoding state are reused across batches.
  VectorPtr fieldValues_;
  VectorPtr fieldMetadata_;
  DecodedVector decodedValues_;
  DecodedVector decodedMetadata_;
};

} // namespace facebook::velox::parquet
