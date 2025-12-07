# Delta Lake Connector for Velox

This directory contains the Velox implementation of the Delta Lake connector for Presto Native Execution.

## Overview

The Delta Lake connector enables Velox to read Delta Lake tables natively in C++. It leverages the existing Hive connector infrastructure while providing Delta-specific functionality.

## Architecture

### Components

1. **HiveDeltaSplit** ([`DeltaSplit.h`](DeltaSplit.h), [`DeltaSplit.cpp`](DeltaSplit.cpp))
   - Extends `HiveConnectorSplit` to represent a Delta Lake data file split
   - Contains file path, byte range (start/length), partition values, and metadata
   - Reuses Hive's file reading infrastructure for Parquet/ORC files

### Integration with Presto

The Delta connector bridges between Presto's Java coordinator and Velox's C++ execution engine:

```
Java Coordinator (Presto)          C++ Worker (Velox)
┌─────────────────────┐           ┌──────────────────────┐
│ DeltaSplit (Java)   │  ──────>  │ HiveDeltaSplit (C++) │
│ DeltaColumnHandle   │  ──────>  │ HiveColumnHandle     │
│ DeltaTableHandle    │  ──────>  │ HiveTableHandle      │
└─────────────────────┘           └──────────────────────┘
         │                                   │
         │ JSON Protocol                     │
         │ (key: hive-delta)                 │
         └───────────────────────────────────┘
```

## File Structure

```
delta/
├── README.md              # This file
├── DeltaSplit.h          # Delta split header
└── DeltaSplit.cpp        # Delta split implementation
```

## Key Features

### Supported Operations
- ✅ Table scans (SELECT queries)
- ✅ Partition pruning
- ✅ Predicate pushdown (via Hive infrastructure)
- ✅ Column pruning
- ✅ Parquet file format
- ✅ ORC file format (if Delta table uses ORC)

### Not Yet Supported
- ❌ Write operations (INSERT, UPDATE, DELETE)
- ❌ Time travel queries
- ❌ Delta-specific features (merge, optimize, vacuum)
- ❌ Change data feed
- ❌ Deletion vectors

## Usage

### Prerequisites

1. Delta Lake tables must be accessible via the configured file system (S3, HDFS, local, etc.)
2. Protocol files must be generated: `cd presto_cpp/presto_protocol && make presto_protocol`
3. Connector must be registered in the Presto Native worker configuration

### Configuration

The Delta connector is configured through the Presto coordinator. No Velox-specific configuration is required.

### Example Query Flow

1. **Coordinator**: User executes `SELECT * FROM delta.schema.table`
2. **Coordinator**: Delta metadata layer reads `_delta_log` to get file list
3. **Coordinator**: Creates `DeltaSplit` objects for each file/range
4. **Coordinator**: Serializes splits to JSON with key `hive-delta`
5. **Worker**: `DeltaPrestoToVeloxConnector` deserializes JSON to `HiveDeltaSplit`
6. **Worker**: Velox reads Parquet/ORC files using Hive infrastructure
7. **Worker**: Returns results to coordinator

## Implementation Details

### HiveDeltaSplit

The `HiveDeltaSplit` class extends `HiveConnectorSplit` and inherits all its functionality:

```cpp
struct HiveDeltaSplit : public connector::hive::HiveConnectorSplit {
  HiveDeltaSplit(
      const std::string& connectorId,
      const std::string& filePath,
      dwio::common::FileFormat fileFormat,
      uint64_t start,
      uint64_t length,
      const std::unordered_map<std::string, std::optional<std::string>>& partitionKeys,
      // ... other parameters
  );
};
```

**Key Parameters:**
- `connectorId`: Catalog identifier (e.g., "delta")
- `filePath`: Full path to the data file (e.g., "s3://bucket/table/part-00000.parquet")
- `fileFormat`: File format enum (PARQUET or ORC)
- `start`: Starting byte offset in the file
- `length`: Number of bytes to read
- `partitionKeys`: Map of partition column names to values
- `customSplitInfo`: Metadata including `table_format=hive-delta`

### File Format Support

Delta Lake tables typically use Parquet format, but can also use ORC. The connector automatically detects and handles both:

- **Parquet**: Default format, fully supported
- **ORC**: Supported if Delta table is configured to use ORC

### Partition Handling

Partition columns are handled through the `partitionKeys` map:
- Keys: Partition column names
- Values: String representation of partition values (or `std::nullopt` for NULL)

Example:
```cpp
partitionKeys = {
  {"year", "2024"},
  {"month", "01"},
  {"day", "15"}
}
```

### Custom Split Info

The `customSplitInfo` map contains Delta-specific metadata:
```cpp
customSplitInfo = {
  {"table_format", "hive-delta"},  // Identifies this as a Delta split
  {"schema", "default"},            // Schema name
  {"table", "events"}               // Table name
}
```

## Development

### Building

The Delta connector is built as part of the Velox library:

```bash
cd presto-native-execution
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Testing

Unit tests should be added to verify:
1. Split creation and serialization
2. File format detection
3. Partition value handling
4. Integration with Hive file readers

### Adding New Features

To add Delta-specific features:

1. **Extend HiveDeltaSplit** if new split-level metadata is needed
2. **Update Protocol** in `presto_cpp/presto_protocol/connector/delta/`
3. **Modify Connector** in `presto_cpp/main/connectors/DeltaPrestoToVeloxConnector.cpp`
4. **Add Tests** to verify the new functionality

## Troubleshooting

### Common Issues

**Issue**: "Unexpected split type" error
- **Cause**: Protocol mismatch between coordinator and worker
- **Solution**: Regenerate protocol files and rebuild

**Issue**: "File not found" errors
- **Cause**: File system configuration mismatch
- **Solution**: Verify S3/HDFS credentials and paths

**Issue**: "Unsupported file format" error
- **Cause**: Delta table uses unsupported format
- **Solution**: Check Delta table format, ensure it's Parquet or ORC

### Debug Logging

Enable debug logging to trace split processing:
```
--v=1  # Verbose logging level 1
```

## Performance Considerations

### Optimization Tips

1. **Partition Pruning**: Use partition columns in WHERE clauses
2. **Column Pruning**: Select only needed columns
3. **File Skipping**: Leverage Delta's statistics for file skipping
4. **Caching**: Enable file handle caching for repeated reads

### Benchmarking

Compare performance with:
- Hive connector (baseline)
- Iceberg connector (similar architecture)
- Java-based Delta connector

## Future Enhancements

### Planned Features

1. **Write Support**: INSERT, UPDATE, DELETE operations
2. **Time Travel**: Read historical versions of tables
3. **Deletion Vectors**: Support for Delta Lake 2.0+ deletion vectors
4. **Optimize Integration**: Leverage Delta's optimize/vacuum operations
5. **Change Data Feed**: Support for CDC queries

### Performance Improvements

1. **Vectorized Statistics**: Use Delta statistics for better pruning
2. **Async I/O**: Parallel file reading
3. **Predicate Pushdown**: Delta-specific predicate optimization
4. **Metadata Caching**: Cache Delta transaction log

## References

### Related Code
- **Protocol**: [`presto_cpp/presto_protocol/connector/delta/`](../../../presto_cpp/presto_protocol/connector/delta/)
- **Connector**: [`presto_cpp/main/connectors/DeltaPrestoToVeloxConnector.{h,cpp}`](../../../presto_cpp/main/connectors/)
- **Java Delta**: [`presto-delta/`](../../../../../presto-delta/)

### Documentation
- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Velox Connectors](https://facebookincubator.github.io/velox/develop/connectors.html)
- [Presto Native Execution](https://prestodb.io/docs/current/presto_cpp.html)

## Contributing

When contributing to the Delta connector:

1. Follow Velox coding standards
2. Add unit tests for new functionality
3. Update this README with new features
4. Ensure backward compatibility with existing Delta tables

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file in the repository root.