# Sharing code between the cuDF Iceberg and Delta readers

`CudfDeltaSplitReader` was written by following `CudfIcebergSplitReader`, so
both carry their own copy of the logic that turns a Parquet file read into the
table the scan must return. This note describes what is duplicated, what a
shared version would look like, and a low-risk order for getting there.

## What is duplicated today

| Concern | Iceberg | Delta | State |
| --- | --- | --- | --- |
| Split row range from row group offsets | `computeSplitRowRange` | `computeSplitRowRange` | Identical |
| Top-level file column names from the footer | `cacheSchemaFromMetadata` | `cacheSchemaFromMetadata` | Near-identical |
| Column classification: info, partition, missing, file | `adaptColumns` | `adaptColumns` | Same shape, format-specific details |
| Record of a synthesized column | `InjectedColumn` | `InjectedColumn` | Same fields |
| Partition/info value to cuDF scalar | `makeInjectedScalar` | `makeInjectedScalar` | Delta also applies the session time zone |
| Interleave file and synthesized columns | `buildOutputTable` | `buildOutputTable` | Same algorithm |
| Splits with no file columns | `noColumnsToRead_`, synthetic table | same | Delta also keeps the row count through a placeholder column |
| All-null nested column | none | `makeAllNullColumn` | Delta only |
| Filter on synthesized columns | Folded against the constant by `CudfIcebergFilterTransform`. Splits can be skipped and data-column predicates stay pushed down. | Whole filter deferred until after the read | Iceberg is more capable |

The copies have already diverged in ways that matter:

- Iceberg's `makeInjectedScalar` does not pass the session time zone for
  timestamp partition values. Delta needed that fix to match the CPU reader, so
  Hive-migrated Iceberg tables likely have the same bug.
- Delta defers its entire filter whenever any partition or missing column is
  read. In partitioned tables that means the Parquet reader gets no filter, so
  data-column predicates cannot prune row groups.

## Target shape

Move the shared mechanics into the base reader, or into a small helper it owns,
and leave each format with only its own column sources.

```cpp
// Base reader (sketch).
class CudfSplitReader {
 protected:
  struct SynthesizedColumn {
    size_t outputIndex;                   // Position in the assembled table.
    std::string name;
    std::optional<std::string> value;     // nullopt: all null.
    TypePtr type;
  };

  // Where a read column's values come from.
  struct ColumnSource {
    enum class Kind { kFile, kConstant, kAllNull } kind;
    std::optional<std::string> value;     // Set for kConstant.
  };

  // Implemented per format and called once per read column: info and
  // partition columns are constants, columns absent from the file are
  // all-null, and everything else is read from the file.
  virtual ColumnSource classifyColumn(const std::string& name,
                                      const TypePtr& type) const;

  // Shared: footer schema, split row range, classification, removal of
  // synthesized columns from the Parquet projection, filter folding,
  // row-count-only reads, interleaving, scalar creation with the session
  // time zone, and all-null nested columns.
  void prepareSynthesizedColumns();
  std::unique_ptr<cudf::table> assembleOutputTable(...);
};
```

With that in place:

- `CudfDeltaSplitReader` keeps only `classifyColumn` (info, then partition,
  then missing, rejecting id-mode columns missing from the file), the
  deletion-vector rejection, and its connector glue. That is roughly 120 lines,
  down from about 390.
- `CudfIcebergSplitReader` keeps row index handling, positional, equality and
  deletion-vector deletes, equality-delete key columns, and its split-specific
  decimal pushdown filters.

`CudfIcebergFilterTransform` is already format-agnostic apart from its
namespace, file name and two error messages. It would move to
`connectors/hive/` as the filter-folding step of `prepareSynthesizedColumns`.
`foldFilterOnConstant` needs the session time zone parameter for timestamp
partition values. Until it has one, a timestamp predicate folds to `kUnknown`
and is deferred, which is still correct.

## Suggested order

1. **Delta adopts shared helpers first.** Add the helpers in a new file under
   `connectors/hive/` and switch Delta to them. Iceberg is untouched, so the
   diff against upstream Velox stays small. The cuDF Delta read tests and the
   62 Presto native Delta tests cover this step.
2. **Delta adopts filter folding.** Move `CudfIcebergFilterTransform` to the
   shared location (Iceberg includes it from there) and replace Delta's
   whole-filter deferral. This adds split skipping by partition value and keeps
   data-column predicates pushed down. Add Delta tests for a partition
   predicate that skips a split and one that keeps row group pruning.
3. **Iceberg migrates.** Switch `CudfIcebergSplitReader` to the shared helpers
   and delete its copies, which also fixes its partition time zone handling.
   The cuDF Iceberg read and deletion-vector tests (128 and 18 today) cover
   this step. This is the only step that changes upstream Iceberg code, so it
   is best proposed upstream as its own change.

## Risks

- Steps 2 and 3 touch upstream Iceberg code. Keeping them as separate commits
  makes them easy to propose upstream or drop.
- Iceberg's filter transform assumes injected columns sit at known indices in
  the assembled table. Delta builds its table the same way, but this should be
  asserted when Delta adopts it.
- Iceberg prepends a row index column that Delta never needs. The shared
  assembly must keep treating prepended columns as outside the logical schema,
  as `castColumnsToVeloxTypes` already does.
