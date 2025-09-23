# Fabric Pipeline Orchestrator Script

## Overview
This Python script is designed to orchestrate the execution of a data pipeline in Microsoft Fabric. It performs the following key tasks:
- Checks if a specified `.whl` package (e.g., `spark_engine-0.1.0-py3-none-any.whl`) is installed in the Spark environment, with retry logic.
- If the package is installed, it retrieves lakehouse information, constructs pipeline parameters, and triggers a data pipeline run.
- Monitors the pipeline run status and logs the outcome.
- Raises errors if the package is not found or if the pipeline fails.

The script is intended for use in environments like Microsoft Fabric notebooks or Spark sessions where `notebookutils` and `FabricInterface` are available.

## Prerequisites
- Microsoft Fabric environment with access to lakehouses and data pipelines.
- The `FabricInterface` class must be defined or imported (not included in this script; assume it's part of the environment or a custom module).
- Required lakehouses must exist:
  - `den_lhw_pdi_001_metadata`
  - `den_lhw_dpr_001_raw_files`
  - `den_lhw_pdi_001_observability`
- The target pipeline (e.g., `dfa_pln_dpr_001_policy_dp_scheduled`) must exist in the workspace.
- Authentication headers for Fabric API must be handled by `FabricInterface`.
- Installed libraries: `requests`, `pkg_resources`, etc. (most are standard or environment-provided).

## Usage
1. Save the script as `document_this.py`.
2. Run it in a Python environment (e.g., Fabric notebook or Spark session):
