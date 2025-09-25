# Data Quality Rule Processor

## Overview
This Python script processes data quality rules from an Excel file, converts them to JSON format, and writes the results to a Delta Lake table. It is designed to run in a Microsoft Fabric environment with Spark and OneLake integration.

## Features
- Reads data quality rules from an Excel file
- Converts rules to JSON format with schema validation
- Writes processed rules to a Delta Lake table
- Includes comprehensive error handling and logging
- Uses configuration-driven file paths and parameters
- Optimized for performance with minimal data conversions

## Prerequisites
- Microsoft Fabric environment with Spark support
- Access to OneLake storage
- Required Python packages:
  - `pyspark`
  - `pandas`
  - `fsspec`
  - `jsonschema`
  - `spark_engine.common.lakehouse` (custom module)
- Excel file containing data quality rules
- Proper access permissions for OneLake and lakehouse tables

## Configuration
The script uses a `CONFIG` dictionary to manage settings. Update the following parameters in the script:

```python
CONFIG = {
    "workspace_id": "36fa502d-ee99-4479-84ef-4f6278542c0f",
    "lakehouse_id": "e07f913f-34ad-4395-9f91-97d2e055c6d3",
    "metadata_lakehouse_name": "den_lhw_pdi_001_metadata",
    "observability_lakehouse_name": "den_lhw_pdi_001_observability",
    "excel_file_name": "Distribution Delegated Authority - PBI_RPT_DPR_001_E&S_HO_POLICY_DATA - DQ Rule Book_E&S HO.xlsx",
    "json_file_name": "dq_template_output.json",
    "sheet_name": "Rule Master ES_HO_PD",
    "skip_rows": 1,
    "table_name": "dim_dq_rule_master",
    "schema_name": "data_quality"
}
```

## Usage
1. Ensure all prerequisites are met and configuration is updated.
2. Place the Excel file in the appropriate OneLake directory.
3. Run the script in a Fabric notebook or Spark environment:

```bash
python dq_rule_processor.py
```

## Script Structure
- `validate_config`: Validates configuration parameters
- `create_file_paths`: Generates OneLake file paths
- `process_excel_to_spark_df`: Reads Excel and applies transformations
- `create_dq_json_struct`: Creates structured JSON column
- `save_json_to_onelake`: Saves JSON to OneLake
- `write_to_delta_table`: Writes to Delta Lake table
- `main`: Orchestrates the entire process

## Input
- Excel file with data quality rules (specified in `CONFIG`)
- Sheet name: `Rule Master ES_HO_PD`
- Skips first row (header)

## Output
- JSON file containing processed rules (`dq_template_output.json`)
- Delta Lake table (`dim_dq_rule_master`) in the specified schema

## Error Handling
- Validates configuration parameters
- Validates JSON schema for rule constraints
- Includes comprehensive exception handling
- Provides detailed error messages for debugging

## Notes
- The script assumes the target Delta table exists. If it doesn't, the write operation will be skipped.
- JSON schema validation ensures rule constraints meet required format.
- Spark session is properly managed with cleanup in the `finally` block.

## Maintenance
- Update `CONFIG` dictionary for new environments or file names
- Monitor error logs for any processing issues
- Ensure OneLake storage and lakehouse permissions are maintained
