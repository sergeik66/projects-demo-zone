# den_nbk_pdi_001_load_datasets_metadata

Fabric notebook that **reads root-level `.json` metadata files** from previously created OneLake shortcuts and loads them into a Delta table `catalog.datasets_test`.

The script is production-ready, idempotent (via MERGE), skips files inside `watermark` folders, lower-cases all extracted string values for consistency, and includes detailed logging.

## Purpose

Extract dataset metadata from JSON configuration files located **directly in the root** of each shortcut folder (e.g. `Files/<shortcut_name>/some_dataset.json`).

- Only root-level `.json` files are processed
- Files inside any subfolder named `watermark` (case-insensitive) are skipped
- All extracted string fields are converted to lowercase
- Results are upserted into `catalog.datasets_test` using `shortcut_name` + `dataset_name` as the natural key

Typical use case:
- After running `den_nbk_pdi_001_create_shortcuts`, this notebook catalogs the datasets for governance, discovery, or downstream pipelines.

## Features

- Uses the same `shortcuts_config(_prd).yaml` as the shortcut creation notebook → perfect alignment
- Reliable JSON parsing with `wholeTextFiles` + double `from_json` (handles nested objects safely)
- Case-insensitive skipping of `watermark` subfolders
- All string columns lower-cased for consistency (`dataset_name`, `database_name`, `ingestType`, etc.)
- Idempotent MERGE (upsert) — safe to rerun
- Deduplication safety net
- Clear logging + final table preview

## Prerequisites

- Microsoft Fabric notebook environment
- Lakehouse attached
- Shortcuts already created (via `den_nbk_pdi_001_create_shortcuts`)
- JSON metadata files present **in the root** of each shortcut folder  
  Example structure:
```
Files/prdandprc_policy_dp_datasets/some_dataset.json          → processed
Files/prdandprc_policy_dp_datasets/watermark/other.json       → skipped
Files/prdandprc_policy_dp_datasets/subfolder/file.json        → skipped (not root)
```

## Target Table

**Schema**: `catalog`  
**Table**: `datasets_test`

Columns:
- `shortcut_name`              STRING
- `dataset_name`               STRING (lowercase)
- `dataset_type_name`          STRING (lowercase)
- `database_name`              STRING (lowercase)
- `dataset_schema`             STRING (lowercase)
- `source_system_ingest_type`  STRING (lowercase)
- `target_load_type`           STRING (lowercase)
- `filter_expression`          STRING (lowercase)
- `source_watermark_identifier` STRING (lowercase)

## Configuration File

Reuses the same file as shortcut creation:

The script reads the `shortcuts` list to know which folders to scan.

## How to Use

1. Ensure shortcuts exist and root-level `.json` files are present.
2. Attach notebook to your lakehouse.
3. Run all cells (or just the last one).
4. Check logs and final table display.

Example log output:
```
2026-01-15 11:45:23 [INFO] Processing shortcut 'prdandprc_policy_dp_datasets' → root files only: Files/prdandprc_policy_dp_datasets/*.json
2026-01-15 11:45:25 [INFO] Extracted & deduplicated 12 root-level records.
2026-01-15 11:45:26 [INFO] Successfully merged 12 records into catalog.datasets_test
```

## Related Notebooks / Pipeline

- `den_nbk_pdi_001_create_shortcuts` – creates the shortcuts this notebook depends on
- Typical pipeline sequence:
  1. Create shortcuts
  2. Load metadata → `catalog.datasets_test`
  3. Use table for dataset discovery, lineage, or orchestration

### Recommendations

- Run this notebook immediately after shortcut creation in the same pipeline
- Consider adding a `load_timestamp` column if audit trail is needed:
  ```python
  from pyspark.sql.functions import current_timestamp
  df_final = df_final.withColumn("load_timestamp", current_timestamp())
