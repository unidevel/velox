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

#include "velox/experimental/cudf/connectors/hive/CudfHiveDataSource.h"
#include "velox/experimental/cudf/connectors/hive/delta/CudfDeltaSplitReader.h"

namespace facebook::velox::cudf_velox::connector::hive::delta {

/// Creates cuDF split readers while retaining the Delta-specific split
/// metadata supplied by the CPU-compatible HiveDeltaSplit contract.
class CudfDeltaDataSource : public ::facebook::velox::cudf_velox::connector::
                                hive::CudfHiveDataSource {
 public:
  CudfDeltaDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& columnHandles,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<CudfHiveConfig>& cudfHiveConfig);

 protected:
  /// Creates the Delta-specific split reader for the current split.
  std::unique_ptr<CudfSplitReader> createCudfSplitReader() override;

  /// Retains HiveDeltaSplit metadata and normalizes its file fields for cuDF.
  void convertSplit(std::shared_ptr<ConnectorSplit> split) override;

 private:
  // Preserves Delta-specific partition and information-column metadata.
  std::shared_ptr<const velox_delta::HiveDeltaSplit> deltaSplit_;
};

} // namespace facebook::velox::cudf_velox::connector::hive::delta
