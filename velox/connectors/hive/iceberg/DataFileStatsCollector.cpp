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
#include "velox/connectors/hive/iceberg/DataFileStatsCollector.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/encode/Base64.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/parquet/writer/arrow/Metadata.h"
#include "velox/dwio/parquet/writer/arrow/Statistics.h"

namespace facebook::velox::connector::hive::iceberg {

using namespace facebook::velox::parquet;
using namespace facebook::velox::dwrf;

DataFileStatsCollector::DataFileStatsCollector(
    std::shared_ptr<
        std::vector<std::unique_ptr<dwio::common::DataFileStatsSettings>>>
        settings)
    : FileStatsCollector(std::move(settings)) {}

void DataFileStatsCollector::collectStats(
    const void* metadata,
    const std::shared_ptr<dwio::common::DataFileStatistics>& dataFileStats) {
  std::unordered_set<int32_t> skipBoundsFields;
  std::function<int32_t(IcebergDataFileStatsSettings*)> processFields =
      [&skipBoundsFields,
       &processFields](IcebergDataFileStatsSettings* field) -> int32_t {
    if (field->skipBounds) {
      skipBoundsFields.insert(field->fieldId);
    }
    if (field->children.empty()) {
      return 1;
    }
    int32_t count = 0;
    for (const auto& child : field->children) {
      count += processFields(child.get());
    }
    return count;
  };

  // numFields is not the number of columns in Iceberg table's schema,
  // e.g., schema_->size(). It also contains the sub-fields when there are
  // nested types in table's schema.
  int32_t numFields = 0;
  for (const auto& field : *statsSetting_) {
    auto* icebergField =
        static_cast<IcebergDataFileStatsSettings*>(field.get());
    numFields += processFields(icebergField);
  }

  if (const auto* parquetMetadata =
          static_cast<const std::shared_ptr<parquet::arrow::FileMetaData>*>(
              metadata);
      parquetMetadata != nullptr && *parquetMetadata != nullptr) {
    const auto& fileMetadata = *parquetMetadata;

    std::unordered_map<int32_t, std::shared_ptr<arrow::Statistics>>
        globalMinStats;
    std::unordered_map<int32_t, std::shared_ptr<arrow::Statistics>>
        globalMaxStats;

    dataFileStats->numRecords = fileMetadata->numRows();
    const auto numRowGroups = fileMetadata->numRowGroups();
    for (auto i = 0; i < numRowGroups; ++i) {
      const auto rgm = fileMetadata->rowGroup(i);
      VELOX_CHECK_EQ(numFields, rgm->numColumns());
      dataFileStats->splitOffsets.emplace_back(rgm->fileOffset());

      for (auto j = 0; j < numFields; ++j) {
        const auto columnChunkMetadata = rgm->columnChunk(j);
        const auto fieldId = columnChunkMetadata->fieldId();
        const auto numValues = columnChunkMetadata->numValues();

        if (fieldId < 0) {
          continue;
        }

        dataFileStats->valueCounts[fieldId] += numValues;
        dataFileStats->columnsSizes[fieldId] +=
            columnChunkMetadata->totalCompressedSize();

        const auto columnChunkStats = columnChunkMetadata->statistics();
        if (columnChunkStats->nanCount() > 0) {
          dataFileStats->nanValueCounts[fieldId] +=
              columnChunkStats->nanCount();
        }
        dataFileStats->nullValueCounts[fieldId] +=
            columnChunkStats->nullCount();

        if (columnChunkStats->hasMinMax() &&
            !skipBoundsFields.contains(fieldId)) {
          if (globalMaxStats.find(fieldId) == globalMaxStats.end()) {
            globalMinStats[fieldId] = columnChunkStats;
            globalMaxStats[fieldId] = columnChunkStats;
          } else {
            globalMaxStats[fieldId] = arrow::Statistics::CompareAndGetMax(
                globalMaxStats[fieldId], columnChunkStats);
            globalMinStats[fieldId] = arrow::Statistics::CompareAndGetMin(
                globalMinStats[fieldId], columnChunkStats);
          }
        }
      }
    }

    for (const auto& [fieldId, minStats] : globalMinStats) {
      const auto lowerBound = minStats->MinValue();
      dataFileStats->lowerBounds[fieldId] =
          encoding::Base64::encode(lowerBound.data(), lowerBound.size());
    }
    for (const auto& [fieldId, maxStats] : globalMaxStats) {
      const auto upperBound = maxStats->MaxValue();
      dataFileStats->upperBounds[fieldId] =
          encoding::Base64::encode(upperBound.data(), upperBound.size());
    }
    return;
  }

  const auto* dwrfMetadata = static_cast<const DwrfFileMetadata*>(metadata);
  VELOX_CHECK_NOT_NULL(dwrfMetadata);

  const auto& footer = dwrfMetadata->footer();
  dataFileStats->numRecords = footer.numberOfRows();

  for (auto i = 0; i < footer.stripesSize(); ++i) {
    dataFileStats->splitOffsets.emplace_back(footer.stripes(i).offset());
  }

  StatsContext statsContext(WriterVersion::DWRF_5_0);
  for (auto i = 1; i < footer.statisticsSize(); ++i) {
    const auto fieldId = static_cast<int32_t>(i);
    auto columnStats =
        buildColumnStatisticsFromProto(footer.statistics(i), statsContext);
    VELOX_CHECK_NOT_NULL(columnStats);

    if (columnStats->getNumberOfValues().has_value()) {
      dataFileStats->valueCounts[fieldId] = columnStats->getNumberOfValues().value();
    }
    if (columnStats->getRawSize().has_value()) {
      dataFileStats->columnsSizes[fieldId] = columnStats->getRawSize().value();
    }

    const auto hasNull = columnStats->hasNull().value_or(false);
    const auto numberOfValues = columnStats->getNumberOfValues().value_or(0);
    dataFileStats->nullValueCounts[fieldId] = hasNull
        ? std::max<int64_t>(0, dataFileStats->numRecords - numberOfValues)
        : 0;

    if (skipBoundsFields.contains(fieldId)) {
      continue;
    }

    if (const auto* intStats =
            dynamic_cast<const dwio::common::IntegerColumnStatistics*>(
                columnStats.get())) {
      if (intStats->getMinimum().has_value()) {
        const auto lowerBound = folly::to<std::string>(intStats->getMinimum().value());
        dataFileStats->lowerBounds[fieldId] =
            encoding::Base64::encode(lowerBound.data(), lowerBound.size());
      }
      if (intStats->getMaximum().has_value()) {
        const auto upperBound = folly::to<std::string>(intStats->getMaximum().value());
        dataFileStats->upperBounds[fieldId] =
            encoding::Base64::encode(upperBound.data(), upperBound.size());
      }
    } else if (
        const auto* doubleStats =
            dynamic_cast<const dwio::common::DoubleColumnStatistics*>(
                columnStats.get())) {
      if (doubleStats->getMinimum().has_value()) {
        const auto lowerBound =
            folly::to<std::string>(doubleStats->getMinimum().value());
        dataFileStats->lowerBounds[fieldId] =
            encoding::Base64::encode(lowerBound.data(), lowerBound.size());
      }
      if (doubleStats->getMaximum().has_value()) {
        const auto upperBound =
            folly::to<std::string>(doubleStats->getMaximum().value());
        dataFileStats->upperBounds[fieldId] =
            encoding::Base64::encode(upperBound.data(), upperBound.size());
      }
    } else if (
        const auto* stringStats =
            dynamic_cast<const dwio::common::StringColumnStatistics*>(
                columnStats.get())) {
      if (stringStats->getMinimum().has_value()) {
        const auto& lowerBound = stringStats->getMinimum().value();
        dataFileStats->lowerBounds[fieldId] =
            encoding::Base64::encode(lowerBound.data(), lowerBound.size());
      }
      if (stringStats->getMaximum().has_value()) {
        const auto& upperBound = stringStats->getMaximum().value();
        dataFileStats->upperBounds[fieldId] =
            encoding::Base64::encode(upperBound.data(), upperBound.size());
      }
    }
  }
}

} // namespace facebook::velox::connector::hive::iceberg
