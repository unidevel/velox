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

#include "velox/experimental/cudf/CudfNoDefaults.h"
#include "velox/experimental/cudf/connectors/hive/delta/CudfDeltaSplitReader.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/expression/AstUtils.h"

#include "velox/common/base/Exceptions.h"
#include "velox/connectors/hive/ConstantFromString.h"
#include "velox/connectors/hive/FileSplitReader.h"
#include "velox/type/tz/TimeZoneMap.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/transform.hpp>

#include <algorithm>
#include <limits>
#include <tuple>

namespace facebook::velox::cudf_velox::connector::hive::delta {

CudfDeltaSplitReader::CudfDeltaSplitReader(
    std::shared_ptr<CudfHiveConnectorSplit> split,
    std::shared_ptr<const velox_delta::HiveDeltaSplit> deltaSplit,
    std::shared_ptr<const velox_hive::HiveTableHandle> tableHandle,
    const RowTypePtr& outputType,
    const std::vector<std::string>& readColumnNames,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ::facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<CudfHiveConfig>& cudfHiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStatistics,
    const std::shared_ptr<IoStats>& ioStats,
    cudf::ast::expression const* subfieldFilterExpr)
    : CudfSplitReader(
          std::move(split),
          std::move(tableHandle),
          outputType,
          readColumnNames,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          cudfHiveConfig,
          ioStatistics,
          ioStats,
          subfieldFilterExpr),
      deltaSplit_(std::move(deltaSplit)) {
  VELOX_CHECK_NOT_NULL(deltaSplit_);
}

void CudfDeltaSplitReader::resetDeltaSplit() {
  injectedColumns_.clear();
  fileColumnNames_.clear();
  splitRowCount_ = 0;
  noColumnsToRead_ = false;
  syntheticTableProduced_ = false;
  deferSubfieldFilter_ = false;
}

cudf::ast::expression const* CudfDeltaSplitReader::pushdownFilter() const {
  return deferSubfieldFilter_ ? nullptr : CudfSplitReader::pushdownFilter();
}

void CudfDeltaSplitReader::prepareSplitInternal(
    dwio::common::RuntimeStats& /*runtimeStats*/) {
  // Reading files with logically deleted rows is not yet supported. Reject
  // early rather than reading the file and returning the deleted rows.
  VELOX_USER_CHECK(
      !deltaSplit_->hasDeletionVector,
      "Reading Delta files with a deletion vector is not supported.");

  resetDeltaSplit();
  cacheSchemaFromMetadata();
  adaptColumns();
  noColumnsToRead_ = readColumnNames_.empty();
  prepareSubfieldFilter();

  if (deferSubfieldFilter_) {
    VLOG(1) << "Delta subfield filter is deferred until synthesized columns "
               "are available";
  }
  if (not noColumnsToRead_) {
    createCudfReader();
  }
}

rmm::device_async_resource_ref
CudfDeltaSplitReader::determineCudfMemoryResource() const {
  return deferSubfieldFilter_ ? get_temp_mr() : get_output_mr();
}

void CudfDeltaSplitReader::prepareSubfieldFilter() {
  if (CudfSplitReader::pushdownFilter() != nullptr and
      (noColumnsToRead_ or not injectedColumns_.empty())) {
    // The filter AST is expressed against the complete table layout. Once
    // synthesized columns are removed from the Parquet projection, its column
    // indices no longer match the file table. Apply it after rebuilding that
    // complete layout instead.
    deferSubfieldFilter_ = true;
  }
}

std::optional<std::unique_ptr<cudf::table>>
CudfDeltaSplitReader::readNextChunk() {
  std::unique_ptr<cudf::table> table;
  std::optional<cudf::size_type> rowCountOverride;

  if (noColumnsToRead_) {
    if (syntheticTableProduced_) {
      return std::nullopt;
    }
    syntheticTableProduced_ = true;
    const auto numRows = static_cast<cudf::size_type>(std::min<std::size_t>(
        splitRowCount_,
        static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max())));
    splitRowCount_ -= numRows;
    if (splitRowCount_ > 0) {
      syntheticTableProduced_ = false;
    }
    rowCountOverride = numRows;
    table = std::make_unique<cudf::table>(
        std::vector<std::unique_ptr<cudf::column>>{});
  } else {
    auto tableChunk = CudfSplitReader::readNextChunk();
    if (not tableChunk.has_value()) {
      return std::nullopt;
    }
    table = std::move(tableChunk.value());
  }

  const auto injectionMemoryResource =
      deferSubfieldFilter_ ? get_temp_mr() : get_output_mr();
  table = buildOutputTable(
      std::move(table), injectionMemoryResource, rowCountOverride);

  if (deferSubfieldFilter_) {
    auto* filter = subfieldFilterAst();
    VELOX_CHECK_NOT_NULL(filter);
    auto filterMask =
        cudf::compute_column(table->view(), *filter, stream_, get_temp_mr());
    table = cudf::apply_boolean_mask(
        table->view(), filterMask->view(), stream_, get_output_mr());
  }

  return table;
}

void CudfDeltaSplitReader::cacheSchemaFromMetadata() {
  fileMetaDatas();
  VELOX_CHECK_EQ(
      fileMetaData_.size(),
      1,
      "Expected one Parquet footer for a Delta data file");

  const auto& metadata = fileMetaData_.front();
  VELOX_CHECK(not metadata.schema.empty(), "Parquet footer schema is empty");
  VELOX_CHECK_GE(
      metadata.num_rows, 0, "Parquet footer reports a negative row count");
  std::tie(std::ignore, splitRowCount_) = computeSplitRowRange();

  const auto& root = metadata.schema.front();
  fileColumnNames_.reserve(root.children_idx.size());
  for (const auto childIndex : root.children_idx) {
    VELOX_CHECK_LT(
        childIndex,
        metadata.schema.size(),
        "Parquet schema child index is out of range");
    fileColumnNames_.insert(metadata.schema[childIndex].name);
  }
}

std::pair<std::size_t, std::size_t> CudfDeltaSplitReader::computeSplitRowRange()
    const {
  const auto rowGroupOffset = [](const auto& rowGroup) {
    if (rowGroup.file_offset.has_value()) {
      return rowGroup.file_offset.value();
    }
    if (rowGroup.columns.front().file_offset != 0) {
      return rowGroup.columns.front().file_offset;
    }
    const auto& column = rowGroup.columns.front().meta_data;
    return column.dictionary_page_offset != 0
        ? std::min(column.dictionary_page_offset, column.data_page_offset)
        : column.data_page_offset;
  };

  std::size_t startRow{0};
  std::size_t numRows{0};
  for (const auto& rowGroup : fileMetaData_.front().row_groups) {
    const auto offset = rowGroupOffset(rowGroup);
    if (offset < split_->start) {
      startRow += rowGroup.num_rows;
    } else if (offset - split_->start < split_->size()) {
      numRows += rowGroup.num_rows;
    }
  }
  return {startRow, numRows};
}

void CudfDeltaSplitReader::adaptColumns() {
  std::vector<std::string> fileReadNames;
  std::vector<TypePtr> fileReadTypes;
  fileReadNames.reserve(readColumnNames_.size());
  fileReadTypes.reserve(readColumnNames_.size());
  for (size_t index{0}; index < readColumnNames_.size(); ++index) {
    const auto& fieldName = readColumnNames_[index];
    // The base reader resolved every read column's type, including filter-only
    // partition columns, which are absent from dataColumns and are typed by
    // the table handle's filter column handles (resolveScanColumnType()).
    const TypePtr& veloxType = readColumnTypes_[index];

    if (auto info = deltaSplit_->infoColumns.find(fieldName);
        info != deltaSplit_->infoColumns.end()) {
      injectedColumns_.push_back({index, fieldName, info->second, veloxType});
    } else if (auto partition = deltaSplit_->partitionKeys.find(fieldName);
               partition != deltaSplit_->partitionKeys.end()) {
      injectedColumns_.push_back(
          {index, fieldName, partition->second, veloxType});
    } else if (not fileColumnNames_.contains(fieldName)) {
      VELOX_USER_CHECK(
          deltaSplit_->columnMappingMode !=
              velox_delta::DeltaColumnMappingMode::kId,
          "Delta id-mode column '{}' is missing from the Parquet file; field-id resolution is not supported.",
          fieldName);
      injectedColumns_.push_back({index, fieldName, std::nullopt, veloxType});
    } else {
      fileReadNames.push_back(fieldName);
      fileReadTypes.push_back(veloxType);
    }
  }

  readColumnNames_ = std::move(fileReadNames);
  readColumnTypes_ = std::move(fileReadTypes);
}

std::unique_ptr<cudf::scalar> CudfDeltaSplitReader::makeInjectedScalar(
    const InjectedColumn& column) const {
  try {
    const bool readAsLocalTime =
        hiveConfig().readTimestampPartitionValueAsLocalTime(
            connectorQueryCtx_->sessionProperties());
    // Match the CPU DeltaSplitReader: when the session adjusts timestamps to
    // its time zone, timestamp partition values are interpreted in that zone.
    const auto& sessionTimezone = connectorQueryCtx_->sessionTimezone();
    const tz::TimeZone* timezone =
        connectorQueryCtx_->adjustTimestampToTimezone() &&
            !sessionTimezone.empty()
        ? tz::locateZone(sessionTimezone)
        : nullptr;
    const VectorPtr constant = velox::connector::hive::newConstantFromString(
        column.veloxType,
        column.value,
        connectorQueryCtx_->memoryPool(),
        readAsLocalTime,
        /*isDaysSinceEpoch=*/false,
        timezone);
    return cudf_velox::makeScalarFromConstantExpr(
        std::make_shared<core::ConstantTypedExpr>(constant),
        connectorQueryCtx_->memoryPool(),
        std::nullopt,
        stream_);
  } catch (const std::exception& error) {
    VELOX_USER_FAIL(
        "Invalid Delta synthesized value. Column: {}, type: {}, value: {}, error: {}",
        column.name,
        column.veloxType->toString(),
        column.value.value_or("<null>"),
        error.what());
  }
}

std::unique_ptr<cudf::column> CudfDeltaSplitReader::makeAllNullColumn(
    const TypePtr& type,
    cudf::size_type numRows,
    rmm::device_async_resource_ref memoryResource) const {
  auto nullMask = [&]() {
    return cudf::create_null_mask(
        numRows, cudf::mask_state::ALL_NULL, stream_, memoryResource);
  };

  if (type->isRow()) {
    std::vector<std::unique_ptr<cudf::column>> children;
    children.reserve(type->size());
    for (size_t index{0}; index < type->size(); ++index) {
      children.push_back(
          makeAllNullColumn(type->childAt(index), numRows, memoryResource));
    }
    return cudf::make_structs_column(
        numRows,
        std::move(children),
        numRows,
        nullMask(),
        stream_,
        memoryResource);
  }

  if (type->isArray()) {
    cudf::numeric_scalar<cudf::size_type> zero{
        0, true, stream_, memoryResource};
    auto offsets = cudf::make_column_from_scalar(
        zero, numRows + 1, stream_, memoryResource);
    auto elements = makeAllNullColumn(type->childAt(0), 0, memoryResource);
    return cudf::make_lists_column(
        numRows, std::move(offsets), std::move(elements), numRows, nullMask());
  }

  auto scalar = cudf::make_default_constructed_scalar(
      cudf_velox::veloxToCudfDataType(type), stream_, memoryResource);
  return cudf::make_column_from_scalar(
      *scalar, numRows, stream_, memoryResource);
}

std::unique_ptr<cudf::column> CudfDeltaSplitReader::makeInjectedColumn(
    const InjectedColumn& column,
    cudf::size_type numRows,
    rmm::device_async_resource_ref memoryResource) const {
  if (not column.value.has_value() and
      (column.veloxType->isRow() or column.veloxType->isArray())) {
    return makeAllNullColumn(column.veloxType, numRows, memoryResource);
  }

  auto scalar = makeInjectedScalar(column);
  return cudf::make_column_from_scalar(
      *scalar, numRows, stream_, memoryResource);
}

std::unique_ptr<cudf::table> CudfDeltaSplitReader::buildOutputTable(
    std::unique_ptr<cudf::table>&& table,
    rmm::device_async_resource_ref memoryResource,
    std::optional<cudf::size_type> rowCountOverride) const {
  const auto numRows = rowCountOverride.value_or(table->num_rows());
  auto physicalColumns = rowCountOverride.has_value()
      ? std::vector<std::unique_ptr<cudf::column>>{}
      : table->release();

  if (physicalColumns.empty() and injectedColumns_.empty()) {
    cudf::numeric_scalar<int8_t> zero{0, true, stream_, memoryResource};
    std::vector<std::unique_ptr<cudf::column>> dummyColumns;
    dummyColumns.push_back(
        cudf::make_column_from_scalar(zero, numRows, stream_, memoryResource));
    return std::make_unique<cudf::table>(std::move(dummyColumns));
  }

  std::vector<std::unique_ptr<cudf::column>> outputColumns;
  outputColumns.reserve(physicalColumns.size() + injectedColumns_.size());
  auto physical = physicalColumns.begin();
  auto injected = injectedColumns_.begin();

  const auto numOutputColumns =
      physicalColumns.size() + injectedColumns_.size();
  for (size_t outputIndex{0}; outputIndex < numOutputColumns; ++outputIndex) {
    if (injected != injectedColumns_.end() and
        injected->outputIndex == outputIndex) {
      outputColumns.push_back(
          makeInjectedColumn(*injected++, numRows, memoryResource));
    } else {
      VELOX_CHECK(
          physical != physicalColumns.end(),
          "Physical Delta column index is out of range");
      outputColumns.push_back(std::move(*physical++));
    }
  }

  VELOX_CHECK(
      physical == physicalColumns.end(),
      "Not all physical Delta columns were consumed");
  VELOX_CHECK(
      injected == injectedColumns_.end(),
      "Not all synthesized Delta columns were consumed");
  return std::make_unique<cudf::table>(std::move(outputColumns));
}

} // namespace facebook::velox::cudf_velox::connector::hive::delta
