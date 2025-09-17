# Dataset Query File Generator

## Overview
This Python notebook (`den_nbk_pdi_001_cicd_generate_dataset_query_file.ipynb`) is designed to generate or update JSON query files for datasets based on STTM (Source To Target Mapping) changes as part of a CI/CD post-deployment process. It supports both watermark-based and CDC (Change Data Capture) ingestion types.

## Purpose
The notebook:
- Scans a specified OneLake directory for dataset configuration files
- Generates SQL queries based on dataset configurations
- Creates watermark JSON files for incremental data processing
- Handles both watermark and CDC ingestion types
- Supports dynamic query filtering

## Prerequisites
- Python 3.8+
- Required libraries:
  - `notebookutils`
  - `fsspec`
- Access to OneLake storage
- Valid workspace and lakehouse IDs
- Properly formatted dataset configuration files in JSON format

## Installation
1. Ensure all required Python libraries are installed:
```bash
pip install notebookutils fsspec
