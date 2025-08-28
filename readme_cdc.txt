# CDC Processing with Ingest and MergeLoad

This document describes the Change Data Capture (CDC) processing functionality implemented in the `Ingest` and `MergeLoad` classes for loading data into a Delta Lake table. The code supports CDC operations (inserts, updates, deletes) from a source Parquet file, typically generated from a SQL Server CDC query, and merges them into a target Delta table.

## Overview

The `Ingest` class (in `ingest.py`) orchestrates the ingestion process, while the `MergeLoad` class (in `strategy.py`) handles the CDC merge logic. The system processes CDC data with `__operation` values:
- **1**: Delete (marks records as `dl_is_deleted = 1`, `dl_iscurrent = 0`).
- **2**: Insert (adds new records with `dl_is_deleted = 0`, `dl_iscurrent = 1`).
- **4**: Update (updates existing records if `dl_rowhash` differs).

Key features:
- Schema evolution to add new columns.
- Metadata column management (`dl_is_deleted`, `dl_iscurrent`, `dl_lastmodifiedutc`).
- Watermark updates for incremental processing using `__start_lsn` or a custom column.
- Deduplication and data quality checks (configurable).

## Configuration

To use CDC processing, configure the `Ingest` class with a JSON configuration. Below is an example configuration tailored for CDC:

```json
{
  "curatedProperties": {
    "targetLoadType": "merge",
    "schemaName": "test_schema",
    "lakehouseName": "test_lakehouse",
    "primaryKeyList": ["code", "primeseq", "state", "effdate", "classcode", "seqcode", "territory", "volcomp", "rateeffdate"],
    "partitionKeyList": ["dl_partitionkey"],
    "columnList": [
      "code", "primeseq", "state", "effdate", "classcode", "seqcode", "territory", 
      "exmod", "minprem", "exposure", "premium", "rate", "xcluamt", "losscons", 
      "form", "paragraph", "volcomp", "hazard", "cvrg", "disctype", "classuffix", 
      "diffprem", "diffrate", "rateeffdate", "losscost", 
      "dl_rowhash", "dl_partitionkey", "dl_iscurrent", "dl_recordstartdateutc", 
      "dl_recordenddateutc", "dl_createddateutc", "dl_lastmodifiedutc", 
      "dl_sourcefilepath", "dl_sourcefilename", "dl_eltid", "dl_runid", "dl_is_deleted"
    ],
    "duplicateCheckEnabled": false,
    "raiseErrors": true
  },
  "rawProperties": {
    "fileType": "parquet",
    "lakehouseName": "test_lakehouse"
  },
  "sourceSystemProperties": {
    "sourceSystemName": "test_system",
    "ingestType": "cdc",
    "isDynamicQuery": true,
    "isCdc": false,
    "sourceWatermarkIdentifier": "__start_lsn"
  },
  "datasetName": "test_table",
  "datasetTypeName": "database",
  "datasetSchema": "dbo"
}
```

### Configuration Notes
- **`"isCdc": false`**: Set to `false` if the source Parquet file contains net changes (e.g., from a CDC query with `row_number()` filtering). Set to `true` if raw CDC data needs processing in `_process_cdc_data`.
- **`"sourceWatermarkIdentifier"`**: Use `__start_lsn` for CDC watermarking, or specify a column like `effdate` if `__start_lsn` is unavailable.
- **`"primaryKeyList"`**: Must match the primary keys used in the CDC query’s `PARTITION BY`.
- **`"columnList"`**: Include all data and metadata columns. Ensure `dl_is_deleted` is included.
- **`"partitionKeyList"`**: Typically includes `dl_partitionkey` and optionally `dl_iscurrent`.

## Usage Steps

### Prerequisites
- **Spark Environment**: A Spark environment with Delta Lake support (e.g., Databricks, Microsoft Fabric).
- **Dependencies**: Ensure `pyspark`, `delta-spark`, and required libraries are installed.
- **Storage**: Access to a lakehouse (e.g., ADLS Gen2 with `abfss://` paths).
- **Source Data**: A Parquet file with CDC data, including `__operation`, `__start_lsn`, `__seqval`, and data columns.

### Steps
1. **Prepare Source and Target Data**:
   - Generate test datasets using `generate_datasets.py` (provided separately) to create:
     - `/tmp/target_data.parquet`: Initial Delta table state.
     - `/tmp/source_data.parquet`: CDC changes (inserts, updates, deletes).
   - Load the target data into the Delta table:
     ```python
     from pyspark.sql import SparkSession
     spark = SparkSession.builder.appName("CDCSetup").getOrCreate()
     spark.read.format("parquet").load("/tmp/target_data.parquet").write.format("delta").mode("overwrite").save("abfss://<lakehouse_path>/Tables/test_schema/test_table")
     ```

2. **Configure Ingest**:
   - Save the configuration JSON to a file (e.g., `config.json`) or define it in code.
   - Replace `<lakehouse_path>` with your actual lakehouse path (e.g., `abfss://container@storageaccount.dfs.fabric.microsoft.com/lakehouse_id`).

3. **Run Ingestion**:
   - Execute the ingestion process:
     ```python
     from ingest import Ingest
     config_json = {...}  # Load or define the configuration JSON
     ingest = Ingest(config_json).source_data("/tmp/source_data.parquet").target_lakehouse("test_lakehouse").start_ingest(elt_id="test_elt_001", run_id="test_run_001")
     ```

4. **Verify Results**:
   - Check the updated Delta table:
     ```python
     spark.read.format("delta").load("abfss://<lakehouse_path>/Tables/test_schema/test_table").show()
     ```
   - Expected outcomes:
     - Inserted records (`__operation = 2`): New rows with `dl_is_deleted = 0`, `dl_iscurrent = 1`.
     - Updated records (`__operation = 4`): Updated columns where `dl_rowhash` differs, with `dl_lastmodifiedutc` updated.
     - Deleted records (`__operation = 1`): Existing rows updated to `dl_is_deleted = 1`, `dl_iscurrent = 0`, `dl_lastmodifiedutc` updated.
   - Verify the watermark JSON file in `abfss://<metadata_lakehouse_path>/Files/datasets/<folder>/watermark/<file>.json` for the updated `__start_lsn`.

5. **Check Metrics**:
   - Retrieve ingestion metrics:
     ```python
     print(ingest.metrics("json"))
     ```
   - Metrics include inserted, updated, and deleted row counts, and any errors.

## CDC Processing Details

### Source Data Requirements
- The source Parquet file must include:
  - **CDC Columns**: `__operation` (1, 2, or 4), `__start_lsn`, `__seqval`.
  - **Primary Keys**: Columns listed in `primaryKeyList` (e.g., `code, primeseq, state, effdate, classcode, seqcode, territory, volcomp, rateeffdate`).
  - **Data Columns**: Business columns (e.g., `exmod, minprem, exposure`, etc.).
  - **Metadata Columns**: `dl_rowhash`, `dl_partitionkey`, etc., added by the ingestion process.
- If `isCdc: true`, the source data must include `row_num` for deduplication in `_process_cdc_data`.

### Merge Logic
The `MergeLoad.ingest` method performs the following:
- **Validation**: Checks for `__operation` in the source data.
- **Merge Predicate**: Matches source (`sd`) and target (`bd`) on `primaryKeyList`.
- **CDC Operations**:
  - **Inserts/Updates (`__operation IN (2, 4)`)**: Updates existing records if `dl_rowhash` differs, or inserts new records, excluding CDC columns (`__operation`, `__start_lsn`, etc.).
  - **Deletes (`__operation = 1`)**: Sets `dl_is_deleted = 1`, `dl_iscurrent = 0`, and updates `dl_lastmodifiedutc`.
- **Non-CDC Merge**: Updates records where `dl_rowhash` differs and inserts all new records.

### Watermark Updates
- If `isDynamicQuery: true` and `ingestType: "cdc"`, the `update_watermark_query` method generates a watermark JSON file with the maximum `__start_lsn` (or `sourceWatermarkIdentifier`) and a SQL query for the next incremental load.

## Testing

Use the provided `generate_datasets.py` to create test datasets:
- **Target Dataset**: `/tmp/target_data.parquet` (initial Delta table with two records).
- **Source Dataset**: `/tmp/source_data.parquet` (CDC changes: one insert, one update, one delete).

Steps:
1. Run `generate_datasets.py` in a Spark environment to create Parquet files.
2. Load the target dataset into the Delta table (see Usage Steps).
3. Run the ingestion with the source dataset and configuration.
4. Verify the Delta table and watermark file.

## Troubleshooting

- **Schema Errors**:
  - Ensure `dl_is_deleted`, `dl_iscurrent`, and `dl_lastmodifiedutc` exist in the target table (handled by `_ensure_target_metadata_columns`).
  - Verify source data includes `__operation`, `__start_lsn`, `__seqval`.
- **Merge Errors**:
  - Check logs for schemas and merge predicate:
    ```python
    print("Source schema:")
    source_data.printSchema()
    print("Target schema:")
    DeltaTable.forPath(spark, table_path).toDF().printSchema()
    print(f"Merge predicate: {merge_predicate}")
    ```
- **Watermark Issues**:
  - Confirm `__start_lsn` or `sourceWatermarkIdentifier` exists in the source data.
  - Check the generated watermark JSON file for correctness.
- **Environment**:
  - Ensure Delta Lake version supports `withSchemaEvolution()` and merge operations.
  - Verify lakehouse path accessibility.

## Notes
- **Performance**: For large datasets, optimize `primaryKeyList` and `partitionKeyList` to reduce shuffle operations.
- **Schema Evolution**: Enabled by default to add new columns from the source.
- **Deduplication**: Enable via `"duplicateCheckEnabled": true` if needed, but ensure `primaryKeyList` is defined.

For further assistance, contact the data engineering team or refer to the Delta Lake documentation.
