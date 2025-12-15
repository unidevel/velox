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

#include "velox/connectors/hive/delta/DeltaSplitReader.h"

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/delta/HiveDeltaSplit.h"

using namespace facebook::velox::dwio::common;

namespace facebook::velox::connector::hive::delta {

DeltaSplitReader::DeltaSplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
    const std::unordered_map<std::string, std::shared_ptr<const HiveColumnHandle>>* partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    FileHandleFactory* const fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : SplitReader(
          hiveSplit,
          hiveTableHandle,
          partitionKeys,
          connectorQueryCtx,
          hiveConfig,
          readerOutputType,
          ioStats,
          fsStats,
          fileHandleFactory,
          executor,
          scanSpec) {}

void DeltaSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  createReader(fileReadOps);
  if (emptySplit_) {
    return;
  }
  auto rowType = getAdaptedRowType();

  LOG(INFO) << "DeltaSplitReader::prepareSplit - adapted rowType: " << rowType->toString();
  LOG(INFO) << "  rowType children count: " << rowType->size();
  for (size_t i = 0; i < rowType->size(); ++i) {
    LOG(INFO) << "    rowType[" << i << "]: " << rowType->nameOf(i) << " : " << rowType->childAt(i)->toString();
  }

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  LOG(INFO) << "DeltaSplitReader::prepareSplit - calling createRowReader";
  createRowReader(std::move(metadataFilter), std::move(rowType), std::nullopt);
  LOG(INFO) << "DeltaSplitReader::prepareSplit - createRowReader completed";
}

uint64_t DeltaSplitReader::next(uint64_t size, VectorPtr& output) {
  Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  mutation.deletedRows = nullptr;

  const auto actualSize = baseRowReader_->nextReadSize(size);
  if (actualSize == dwio::common::RowReader::kAtEnd) {
    return 0;
  }

  auto rowsScanned = baseRowReader_->next(actualSize, output, &mutation);

  return rowsScanned;
}

std::vector<TypePtr> DeltaSplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const RowTypePtr& tableSchema) const {
  std::vector<TypePtr> columnTypes = fileType->children();
  auto& childrenSpecs = scanSpec_->children();

  LOG(INFO) << "DeltaSplitReader::adaptColumns called";
  LOG(INFO) << "  fileType: " << fileType->toString();
  LOG(INFO) << "  fileType children count: " << fileType->size();
  LOG(INFO) << "  tableSchema: " << (tableSchema ? tableSchema->toString() : "null");
  LOG(INFO) << "  scanSpec children count: " << childrenSpecs.size();
  LOG(INFO) << "  partitionKeys count: " << hiveSplit_->partitionKeys.size();

  for (const auto& childSpec : childrenSpecs) {
    const std::string& fieldName = childSpec->fieldName();
    LOG(INFO) << "  Processing column: " << fieldName
              << ", columnType: " << static_cast<int>(childSpec->columnType())
              << ", subscript: " << childSpec->subscript();

    if (auto iter = hiveSplit_->infoColumns.find(fieldName);
        iter != hiveSplit_->infoColumns.end()) {
      // Handle info columns (e.g., $path, $file_size)
      LOG(INFO) << "    -> Info column";
      auto infoColumnType = readerOutputType_->findChild(fieldName);
      auto constant = newConstantFromString(
          infoColumnType,
          iter->second,
          connectorQueryCtx_->memoryPool(),
          hiveConfig_->readTimestampPartitionValueAsLocalTime(
              connectorQueryCtx_->sessionProperties()),
          false,
          adjustTimestampToTimezone_ ? sessionTimezone_ : nullptr);
      childSpec->setConstantValue(constant);
    } else {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
      LOG(INFO) << "    -> fileTypeIdx: " << (fileTypeIdx.has_value() ? std::to_string(*fileTypeIdx) : "not found")
                << ", outputTypeIdx: " << (outputTypeIdx.has_value() ? std::to_string(*outputTypeIdx) : "not found");

      if (outputTypeIdx.has_value() && fileTypeIdx.has_value()) {
        // Column exists in both file and output - read from file
        LOG(INFO) << "    -> Column in file and output, clearing constant";
        childSpec->setConstantValue(nullptr);
        auto& outputType = readerOutputType_->childAt(*outputTypeIdx);
        columnTypes[*fileTypeIdx] = outputType;
      } else if (!fileTypeIdx.has_value()) {
        // Column missing from file - could be partition column or schema evolution
        if (auto it = hiveSplit_->partitionKeys.find(fieldName);
            it != hiveSplit_->partitionKeys.end()) {
          // Partition column - set constant value from partition metadata
          LOG(INFO) << "    -> Partition column, setting constant value";
          setPartitionValue(childSpec.get(), fieldName, it->second);
        } else {
          // Schema evolution - column added after file was written
          LOG(INFO) << "    -> Schema evolution, setting NULL constant";
          VELOX_CHECK(tableSchema, "Unable to resolve column '{}'", fieldName);
          childSpec->setConstantValue(
              BaseVector::createNullConstant(
                  tableSchema->findChild(fieldName),
                  1,
                  connectorQueryCtx_->memoryPool()));
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  LOG(INFO) << "  Returning columnTypes with size: " << columnTypes.size();
  for (size_t i = 0; i < columnTypes.size(); ++i) {
    LOG(INFO) << "    columnTypes[" << i << "]: " << columnTypes[i]->toString();
  }

  return columnTypes;
}

} // namespace facebook::velox::connector::hive::delta

