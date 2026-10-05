# INT96 nanosecond fixture

`int96_nanoseconds.parquet` contains a UTC timestamp column `ts` with
nanoseconds since the epoch `[-1001, -1, 0, 1, 1001, null]`. It exercises
negative sub-millisecond fractions and nulls in the GPU Parquet reader.

It is generated independently because the cuDF INT96 writer converts
nanosecond input to microseconds, truncating `-1ns` to zero before writing.
Generate with PyArrow (the test does not require Python):

```python
import pyarrow as pa
import pyarrow.parquet as pq

values = [-1001, -1, 0, 1, 1001, None]
table = pa.table({"ts": pa.array(values, type=pa.timestamp("ns", tz="UTC"))})
pq.write_table(table, "int96_nanoseconds.parquet", use_deprecated_int96_timestamps=True)
assert pq.read_metadata("int96_nanoseconds.parquet").schema.column(0).physical_type == "INT96"
assert pq.read_table("int96_nanoseconds.parquet")["ts"].cast(pa.int64()).to_pylist() == values
```
