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

#include "velox/dwio/parquet/reader/VariantColumnReader.h"

#include "velox/dwio/parquet/reader/VariantToJson.h"

namespace facebook::velox::parquet {
namespace {

// Adds the ScanSpec children for the two fields of the variant and returns the
// ROW type of the file, which the inherited constructor uses as the requested
// type to build the child readers. The fields are projected out so that their
// values are materialized; getValues() consumes them and produces JSON text in
// their place. Called from a member initializer, i.e. before the base class is
// constructed.
const TypePtr& addFieldsToScanSpec(
    common::ScanSpec& scanSpec,
    const dwio::common::TypeWithId& fileType) {
  const auto& rowType = fileType.type()->asRow();
  for (auto i = 0; i < rowType.size(); ++i) {
    auto* fieldSpec = scanSpec.getOrCreateChild(rowType.nameOf(i));
    fieldSpec->setProjectOut(true);
    fieldSpec->setChannel(i);
  }
  return fileType.type();
}

} // namespace

VariantColumnReader::VariantColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool)
    : StructColumnReader(
          columnReaderOptions,
          addFieldsToScanSpec(scanSpec, *fileType),
          fileType,
          params,
          scanSpec,
          pool),
      jsonType_(requestedType) {
  auto* valueSpec = scanSpec.childByName(kValueField);
  auto* metadataSpec = scanSpec.childByName(kMetadataField);
  VELOX_CHECK_NOT_NULL(valueSpec);
  VELOX_CHECK_NOT_NULL(metadataSpec);
  valueReader_ = children_.at(valueSpec->subscript());
  metadataReader_ = children_.at(metadataSpec->subscript());
}

// static
bool VariantColumnReader::isVariantAsJson(
    const TypePtr& requestedType,
    const dwio::common::TypeWithId& fileType) {
  if (requestedType == nullptr || requestedType->kind() != TypeKind::VARCHAR ||
      !fileType.type()->isRow()) {
    return false;
  }
  const auto& rowType = fileType.type()->asRow();
  if (rowType.size() != 2) {
    return false;
  }
  for (const auto* field : {kValueField, kMetadataField}) {
    const auto index = rowType.getChildIdxIfExists(field);
    if (!index.has_value() ||
        rowType.childAt(*index)->kind() != TypeKind::VARBINARY) {
      return false;
    }
  }
  return true;
}

void VariantColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  VELOX_CHECK_NOT_NULL(result);
  valueReader_->getValues(rows, &fieldValues_);
  metadataReader_->getValues(rows, &fieldMetadata_);

  decodedValues_.decode(*fieldValues_);
  decodedMetadata_.decode(*fieldMetadata_);

  auto json = BaseVector::create<FlatVector<StringView>>(
      jsonType_, rows.size(), memoryPool());
  std::string text;
  for (vector_size_t i = 0; i < rows.size(); ++i) {
    // Both fields are required in a variant, so a null in either one means the
    // variant is null. This is also how a null variant group arrives here: its
    // definition level makes both fields null.
    if (decodedValues_.isNullAt(i) || decodedMetadata_.isNullAt(i)) {
      json->setNull(i, true);
      continue;
    }
    const auto value = decodedValues_.valueAt<StringView>(i);
    const auto metadata = decodedMetadata_.valueAt<StringView>(i);
    text.clear();
    appendVariantAsJson(
        std::string_view(value.data(), value.size()),
        std::string_view(metadata.data(), metadata.size()),
        text);
    json->set(i, StringView(text));
  }
  *result = std::move(json);
}

} // namespace facebook::velox::parquet
