# Data Quality Rule Processor

## Overview
This Python script processes data quality rules from an Excel file, converts them to JSON format, and writes the results to a Delta Lake table. It is designed to run in a Microsoft Fabric environment with Spark and OneLake integration. The script includes row count validation, custom package verification, and comprehensive error handling.

## Features
- Reads data quality rules from an Excel file stored in OneLake
- Validates row counts between Excel input and processed DataFrame
- Cleans string columns to remove non-breaking spaces and extra whitespace
- Converts rules to JSON format with schema validation
- Validates custom .whl package installation before processing
- Writes processed rules to a Delta Lake table with final row count validation
- Dynamically retrieves lakehouse information from Fabric environment
- Includes comprehensive error handling and logging
- Supports conditional execution with `run_me` parameter
- Optimized for performance with minimal data conversions

## Prerequisites
- Microsoft Fabric environment with Spark support
- Access to OneLake storage
- Custom `spark_engine` .whl package published to the Spark environment
- Required Python packages:
  - `pyspark`
  - `pandas`
  - `fsspec`
  - `jsonschema`
  - `pkg_resources`
  - Custom modules: `spark_engine.common.observability`, `spark_engine.common.lakehouse`
- Excel file containing data quality rules
- Proper access permissions for OneLake and lakehouse tables

## Configuration
The script uses a `CONFIG` dictionary to manage settings. Update the following parameters in the script:

```python
CONFIG = {
    "workspace_id": "Dynamicaly set based on the runtime environment",
    "metadata_lakehouse_id": "Dynamicaly set based on the runtime environment",
    "metadata_lakehouse_name": "den_lhw_pdi_001_metadata",
    "observability_lakehouse_name": "den_lhw_pdi_001_observability",
    "observability_lakehouse_id": "Dynamicaly set based on the runtime environment",
    "excel_file_name": "DnA Product Pricing - Common Policy Data - DQ Rulebook.xlsx",
    "json_file_name": "dq_template_output.json",
    "sheet_name": "Rule Master Policy Data Product",
    "skip_rows": 1,
    "table_name": "dim_dq_rule_master",
    "schema_name": "data_quality",
    "whl_name": "spark_engine-0.1.0-py3-none-any.whl"
}
```

**Note**: `workspace_id`, `metadata_lakehouse_id`, and `observability_lakehouse_id` are dynamically set using `get_lakehouse_info()` function.

## Usage
1. Ensure all prerequisites are met and configuration is updated.
2. Place the Excel file in the appropriate OneLake directory.
3. Run the script in a Fabric notebook or Spark environment:

```python
# For full execution
main(run_me=True)

# To skip execution (useful for testing configuration)
main(run_me=False)
```

## Script Structure
- `validate_config`: Validates configuration parameters
- `create_file_paths`: Generates OneLake file paths using dynamic lakehouse info
- `get_lakehouse_info`: Retrieves lakehouse information from Fabric environment
- `validate_row_counts`: Compares Excel and processed DataFrame row counts
- `process_excel_to_spark_df`: Reads Excel, cleans data, applies transformations, and validates row counts
- `create_dq_json_struct`: Creates structured JSON column
- `save_json_to_onelake`: Saves JSON to OneLake
- `write_to_delta_table`: Writes to Delta Lake table with final row count validation
- `check_whl_published`: Verifies custom .whl package installation
- `main`: Orchestrates the entire process with conditional execution

## Input
- Excel file with data quality rules (specified in `CONFIG`)
- Sheet name: `Rule Master Policy Data Product`
- Skips first row (header)
- Filters out rows where `DQ Rule Constraint` is empty
- Orders by `DQ Rule Quarantine Flag`

## Output
- JSON file containing processed rules (`dq_template_output.json`) with clean string values
- Delta Lake table (`dim_dq_rule_master`) in the `data_quality` schema

## Validation
### Row Count Validation
1. **Excel Input Validation**:
   - Ensures Excel file contains data rows
   - Compares Excel row count vs. processed DataFrame row count
   - Warns if processed rows exceed Excel rows (potential duplication)
   - Requires minimum 1 processed row

2. **Delta Table Validation**:
   - Verifies final Delta table is not empty after write operation
   - Logs final row count for monitoring

### Package Validation
- Checks if custom `spark_engine` .whl package is installed
- Retries up to 10 times with 60-second intervals
- Aborts if package is not found after max attempts

## Error Handling
- Validates configuration parameters
- Validates JSON schema for rule constraints
- Checks for empty input data at multiple stages
- Handles invalid JSON in `dq_rule_constraint`
- Comprehensive exception handling with detailed logging
- Provides detailed error messages for debugging

## Data Cleaning
- Removes non-breaking spaces (`\u00a0`) from all string columns in the Excel input
- Strips leading/trailing whitespace to ensure clean data in JSON and Delta outputs
- Applied at the pandas DataFrame stage for efficiency

## Notes
- The script dynamically retrieves lakehouse IDs and workspace ID from the Fabric environment
- Custom .whl package validation ensures required dependencies are available
- The target Delta table is created if it doesn't exist using `GDAPObservability`
- JSON schema validation ensures rule constraints meet required format
- Non-breaking spaces and whitespace are cleaned from all string columns
- Execution can be conditionally skipped using `run_me=False`
- Spark session management is handled by the Fabric environment

## Maintenance
- Update `CONFIG` dictionary for new environments or file names
- Ensure custom .whl package is published to the Spark environment
- Monitor logs for row count validation warnings
- Verify input Excel data for valid JSON in `DQ Rule Constraint` column
- Maintain OneLake storage and lakehouse permissions

## Verification
### Check JSON Output
- Verify the generated JSON file for clean `dq_rule_dimension` values (no `\u00a0` or trailing spaces)
- Check row count in JSON file matches processed DataFrame count

### Check Delta Table
- Query the Delta table to confirm clean data and correct row count:
  ```python
  spark.read.format("delta").load(table_path).select("dq_rule_dimension").show(truncate=False)
  spark.read.format("delta").load(table_path).count()  # Should match processed row count
  ```

### Monitor Logs
- Check for row count validation messages:
  ```plaintext
  Excel row count: X
  Processed DataFrame row count: Y
  Row count validation passed: Y rows processed from X Excel rows
  Final Delta table row count: Y
  ```
- Verify .whl package validation:
  ```plaintext
  Package from spark_engine-0.1.0-py3-none-any.whl is installed in the Spark environment.
  ```

### Troubleshooting
- **Row Count Mismatch**: Check Excel filtering conditions (`DQ Rule Constraint != ""`)
- **Package Not Found**: Verify .whl file is published to Spark environment
- **Empty Delta Table**: Check JSON validation and write operation logs
- **Lakehouse Access**: Ensure proper permissions for both metadata and observability lakehouses
