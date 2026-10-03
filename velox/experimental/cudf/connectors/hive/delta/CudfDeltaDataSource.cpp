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
#include "velox/experimental/cudf/connectors/hive/delta/CudfDeltaDataSource.h"

#include "velox/common/Casts.h"

namespace facebook::velox::cudf_velox::connector::hive::delta {

CudfDeltaDataSource::CudfDeltaDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<CudfHiveConfig>& cudfHiveConfig)
    : CudfHiveDataSource(
          outputType,
          tableHandle,
          columnHandles,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          cudfHiveConfig) {}

void CudfDeltaDataSource::convertSplit(std::shared_ptr<ConnectorSplit> split) {
  deltaSplit_ = checkedPointerCast<const velox_delta::HiveDeltaSplit>(split);
  CudfHiveDataSource::convertSplit(std::move(split));
}

std::unique_ptr<CudfSplitReader> CudfDeltaDataSource::createCudfSplitReader() {
  return std::make_unique<CudfDeltaSplitReader>(
      split_,
      deltaSplit_,
      tableHandle_,
      outputType_,
      readColumnNames_,
      fileHandleFactory_,
      executor_,
      connectorQueryCtx_,
      cudfHiveConfig_,
      ioStatistics_,
      ioStats_,
      subfieldFilterAst_);
}

} // namespace facebook::velox::cudf_velox::connector::hive::delta
