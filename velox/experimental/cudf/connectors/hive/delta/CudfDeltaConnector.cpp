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
#include "velox/experimental/cudf/connectors/hive/delta/CudfDeltaConnector.h"
#include "velox/experimental/cudf/connectors/hive/delta/CudfDeltaDataSource.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/connectors/hive/HiveDataSource.h"

namespace facebook::velox::cudf_velox::connector::hive::delta {

using namespace facebook::velox::connector;

namespace {

bool containsMap(const TypePtr& type) {
  if (type->isMap()) {
    return true;
  }
  if (type->isRow()) {
    for (size_t index = 0; index < type->size(); ++index) {
      if (containsMap(type->childAt(index))) {
        return true;
      }
    }
  } else if (type->isArray()) {
    return containsMap(type->childAt(0));
  }
  return false;
}

} // namespace

CudfDeltaConnector::CudfDeltaConnector(
    const std::string& id,
    std::shared_ptr<const velox::config::ConfigBase> config,
    folly::Executor* executor)
    : velox::connector::hive::HiveConnector(id, config, executor),
      cudfHiveConfig_(std::make_shared<CudfHiveConfig>(config)) {
  VLOG(1) << "cuDF Delta connector created";
}

std::optional<std::string> CudfDeltaConnector::unsupportedGpuScanReason(
    const ConnectorTableHandlePtr& /*tableHandle*/,
    const ColumnHandleMap& assignments) const {
  for (const auto& [_, columnHandle] : assignments) {
    const auto* hiveColumn =
        dynamic_cast<const velox::connector::hive::HiveColumnHandle*>(
            columnHandle.get());
    if (hiveColumn == nullptr) {
      return "Delta table scan has a column handle that is not a HiveColumnHandle";
    }
    if (!hiveColumn->requiredSubfields().empty() ||
        !hiveColumn->extractions().empty()) {
      return fmt::format(
          "cuDF Delta reader does not prune nested subfields of column '{}'",
          hiveColumn->name());
    }
    if (containsMap(hiveColumn->schemaType()) ||
        containsMap(hiveColumn->dataType())) {
      return fmt::format(
          "cuDF Delta reader does not support MAP column '{}'",
          hiveColumn->name());
    }
  }
  return std::nullopt;
}

std::unique_ptr<DataSource> CudfDeltaConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  // A scan the cuDF reader cannot produce runs as a CPU TableScan (see
  // CudfTableScanSupport) and needs the CPU data source.
  if (cudfIsRegistered() &&
      !unsupportedGpuScanReason(tableHandle, columnHandles).has_value()) {
    return std::make_unique<CudfDeltaDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        &fileHandleFactory_,
        ioExecutor_,
        connectorQueryCtx,
        cudfHiveConfig_);
  }

  return std::make_unique<velox::connector::hive::HiveDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      &fileHandleFactory_,
      ioExecutor_,
      connectorQueryCtx,
      hiveConfig_);
}

std::shared_ptr<Connector> CudfDeltaConnectorFactory::newConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> config,
    folly::Executor* ioExecutor,
    folly::Executor* /*cpuExecutor*/) {
  return std::make_shared<CudfDeltaConnector>(id, config, ioExecutor);
}

} // namespace facebook::velox::cudf_velox::connector::hive::delta
