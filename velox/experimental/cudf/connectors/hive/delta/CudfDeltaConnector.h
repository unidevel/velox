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

#include "velox/experimental/cudf/connectors/hive/CudfHiveConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfTableScanSupport.h"

#include "velox/connectors/hive/HiveConnector.h"

namespace facebook::velox::cudf_velox::connector::hive::delta {

using namespace facebook::velox::connector;
using namespace facebook::velox::config;

/// Provides GPU-accelerated reads for Delta Lake data files selected by an
/// upstream coordinator.
class CudfDeltaConnector final
    : public ::facebook::velox::connector::hive::HiveConnector,
      public CudfTableScanSupport {
 public:
  CudfDeltaConnector(
      const std::string& id,
      std::shared_ptr<const ConfigBase> config,
      folly::Executor* executor);

  /// The cuDF Delta reader cannot produce MAP columns or prune nested
  /// subfields. Such scans use the CPU Hive reader and the normal Velox-to-cuDF
  /// boundary for downstream GPU operators.
  std::optional<std::string> unsupportedGpuScanReason(
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& assignments) const override;

  /// Creates a cuDF Delta data source when cuDF is registered and otherwise
  /// falls back to the CPU Hive data source and its Delta split reader.
  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  bool canAddDynamicFilter() const override {
    return false;
  }

  /// Splits are prepared in the background like cuDF Hive and Iceberg splits.
  /// The base CudfHiveDataSource::setFromDataSource() adopts the prepared
  /// reader, and all Delta split state lives in that reader, so no Delta
  /// override is needed.
  bool supportsSplitPreload() const override {
    return true;
  }

 private:
  // Configures cuDF-specific reader behavior for this connector instance.
  const std::shared_ptr<CudfHiveConfig> cudfHiveConfig_;
};

/// Creates CudfDeltaConnector instances for the Delta catalog.
class CudfDeltaConnectorFactory final : public ConnectorFactory {
 public:
  static constexpr const char* kDeltaConnectorName = "delta";

  CudfDeltaConnectorFactory() : ConnectorFactory(kDeltaConnectorName) {}

  explicit CudfDeltaConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  /// Creates a connector using the supplied catalog configuration and I/O
  /// executor.
  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override;
};

} // namespace facebook::velox::cudf_velox::connector::hive::delta
