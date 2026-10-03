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

#include "velox/experimental/cudf/connectors/hive/CudfSplitReader.h"

#include "velox/connectors/hive/delta/HiveDeltaSplit.h"

#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace facebook::velox::cudf_velox::connector::hive::delta {

// Velox CPU Delta and Hive namespaces, shared by the cuDF Delta sources.
namespace velox_delta = ::facebook::velox::connector::hive::delta;
namespace velox_hive = ::facebook::velox::connector::hive;

/// Reads Delta Lake Parquet splits on the GPU using the same HiveDeltaSplit
/// contract consumed by the CPU DeltaSplitReader.
class CudfDeltaSplitReader : public CudfSplitReader {
 public:
  CudfDeltaSplitReader(
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
      cudf::ast::expression const* subfieldFilterExpr);

 protected:
  /// Builds the physical projection and constant-injection plan for one Delta
  /// data file before constructing the cuDF reader.
  void prepareSplitInternal(dwio::common::RuntimeStats& runtimeStats) override;

  /// Defers the logical filter when it must observe synthesized columns.
  cudf::ast::expression const* pushdownFilter() const override;

  /// Uses temporary GPU memory when a deferred filter still has to run.
  rmm::device_async_resource_ref determineCudfMemoryResource() const override;

  /// Reads physical data, injects Delta constants, and applies deferred
  /// filters before returning the assembled table.
  std::optional<std::unique_ptr<cudf::table>> readNextChunk() override;

 private:
  /// Describes a column synthesized after the Parquet read.
  struct InjectedColumn {
    // Identifies the column's position in the assembled table.
    size_t outputIndex;

    // Identifies the physical name used by the scan.
    std::string name;

    // Holds a partition or information value; null means SQL null.
    std::optional<std::string> value;

    // Specifies the Velox type used to construct the cuDF scalar.
    TypePtr veloxType;
  };

  // Clears state derived from the previous split preparation.
  void resetDeltaSplit();

  // Reads the footer and caches the top-level file schema and split row count.
  void cacheSchemaFromMetadata();

  // Converts the split byte range into its covered Parquet row range.
  std::pair<std::size_t, std::size_t> computeSplitRowRange() const;

  // Classifies file-backed and synthesized columns and updates projection.
  void adaptColumns();

  // Decides whether to push the logical filter or apply it after injection.
  void prepareSubfieldFilter();

  // Builds a cuDF scalar using Velox's CPU-compatible partition parser.
  std::unique_ptr<cudf::scalar> makeInjectedScalar(
      const InjectedColumn& column) const;

  // Builds an all-null column, including the child layout required by nested
  // cuDF types.
  std::unique_ptr<cudf::column> makeAllNullColumn(
      const TypePtr& type,
      cudf::size_type numRows,
      rmm::device_async_resource_ref memoryResource) const;

  // Expands one synthesized value to the number of rows in the file chunk.
  std::unique_ptr<cudf::column> makeInjectedColumn(
      const InjectedColumn& column,
      cudf::size_type numRows,
      rmm::device_async_resource_ref memoryResource) const;

  // Interleaves physical columns and synthesized constants in scan order.
  std::unique_ptr<cudf::table> buildOutputTable(
      std::unique_ptr<cudf::table>&& table,
      rmm::device_async_resource_ref memoryResource,
      std::optional<cudf::size_type> rowCountOverride) const;

  // Preserves the common CPU/GPU Delta split contract.
  std::shared_ptr<const velox_delta::HiveDeltaSplit> deltaSplit_;

  // Records constants and missing columns to inject after file decoding.
  std::vector<InjectedColumn> injectedColumns_;

  // Records top-level physical names from the current Parquet footer.
  std::unordered_set<std::string> fileColumnNames_;

  // Tracks the number of rows covered by the split's byte range.
  std::size_t splitRowCount_{0};

  // Indicates that no physical Parquet column is needed.
  bool noColumnsToRead_{false};

  // Prevents emitting an injected-only split more than once.
  bool syntheticTableProduced_{false};

  // Indicates that the logical filter must run after column injection.
  bool deferSubfieldFilter_{false};
};

} // namespace facebook::velox::cudf_velox::connector::hive::delta
