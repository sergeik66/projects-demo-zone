"""
load_datasets_metadata.py - Production Ready (YAML-Driven)

Reads the same shortcuts.yaml config used by create_shortcuts.py
For each shortcut defined in the YAML:
- Resolves the actual folder path in the current lakehouse (Files/<name>/)
- Reads all *.json files inside it
- Extracts metadata from JSON (based on your sample)
- Loads into catalog.datasets with shortcut_name = the 'name' from YAML

This ensures perfect alignment between shortcut creation and metadata loading.
"""

import json
import logging
from typing import List, Dict, Any

import yaml
import sempy.fabric as fabric
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# =============================================================================
# Setup
# =============================================================================

spark = SparkSession.builder.getOrCreate()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Detect PRD vs non-PRD like your original script
ws_name = spark.conf.get("trident.workspace.name", "").upper()
is_prd = "-PRD" in ws_name
config_file_name = "shortcuts"
config_variant = "_prd" if is_prd else ""

workspace_id = fabric.get_workspace_id()
lakehouse_id = fabric.get_lakehouse_id()

CONFIG_PATH = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/shortcuts_config/"
    f"{config_file_name}{config_variant}.yaml"
)

TARGET_SCHEMA = "catalog"
TARGET_TABLE = "datasets"
TARGET_FQ = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

# Fields to extract: target_column → list of nested keys in JSON
FIELD_MAPPING = {
    "dataset_name": ["datasetName"],
    "dataset_type_name": ["datasetTypeName"],
    "database_name": ["databaseName"],
    "source_system_ingest_type": ["sourceSystemProperties", "ingestType"],
    "target_load_type": ["curatedProperties", "targetLoadType"]
}

# Optional: skip shortcuts containing these substrings
SKIP_SHORTCUT_NAMES_CONTAINING = {"watermark"}

# =============================================================================
# Helpers
# =============================================================================

def load_shortcuts_config(path: str) -> Dict[str, Any]:
    """Load the shortcuts YAML config from OneLake."""
    from notebookutils import mssparkutils
    try:
        content = mssparkutils.fs.head(path)
        return yaml.safe_load(content)
    except Exception as e:
        raise FileNotFoundError(f"Could not read shortcuts config from {path}: {e}")

def get_nested_value(obj: dict, keys: List[str]):
    """Safely extract nested value: obj['a']['b']['c']"""
    try:
        for key in keys:
            obj = obj[key]
        return str(obj) if obj is not None else None
    except (KeyError, TypeError):
        return None

def ensure_target_table():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_FQ} (
        shortcut_name             STRING,
        dataset_name              STRING,
        dataset_type_name         STRING,
        database_name             STRING,
        source_system_ingest_type STRING,
        target_load_type          STRING
    ) USING DELTA
    """)
    logger.info(f"Ensured table {TARGET_FQ} exists.")

def build_permissive_json_schema() -> StructType:
    return StructType([
        StructField("datasetName", StringType(), True),
        StructField("datasetTypeName", StringType(), True),
        StructField("databaseName", StringType(), True),
        StructField("sourceSystemProperties", 
            StructType([StructField("ingestType", StringType(), True)]), True),
        StructField("curatedProperties", 
            StructType([StructField("targetLoadType", StringType(), True)]), True),
    ])

# =============================================================================
# Main Execution
# =============================================================================

def run_load_datasets_metadata():
    ensure_target_table()

    logger.info(f"Loading shortcuts config from: {CONFIG_PATH}")
    config = load_shortcuts_config(CONFIG_PATH)

    shortcuts = config.get("shortcuts", [])
    if not shortcuts:
        logger.info("No shortcuts defined in config.")
        return

    all_data_frames = []

    for entry in shortcuts:
        shortcut_name = entry.get("name")
        if not shortcut_name:
            logger.warning("Skipping entry missing 'name'")
            continue

        if any(skip in shortcut_name.lower() for skip in SKIP_SHORTCUT_NAMES_CONTAINING):
            logger.info(f"Skipping shortcut '{shortcut_name}' (matches skip filter)")
            continue

        # Shortcut appears as folder: Files/<name>/
        folder_path = f"Files/{shortcut_name}/"
        json_pattern = f"{folder_path}*.json"

        logger.info(f"Processing shortcut '{shortcut_name}' → {json_pattern}")

        try:
            df_raw = (
                spark.read
                .schema(build_permissive_json_schema())
                .option("multiline", "true")
                .json(json_pattern)
            )

            if df_raw.rdd.isEmpty():
                logger.info(f"No JSON files found in shortcut '{shortcut_name}'")
                continue

            # Add shortcut_name column
            df = df_raw.withColumn("shortcut_name", lit(shortcut_name))

            # Extract fields
            for target_col, json_keys in FIELD_MAPPING.items():
                df = df.withColumn(
                    target_col,
                    col(".".join(json_keys)) if len(json_keys) > 1 else col(json_keys[0])
                )

            # Select final columns
            df_final = df.select(
                "shortcut_name",
                "dataset_name",
                "dataset_type_name",
                "database_name",
                "source_system_ingest_type",
                "target_load_type"
            )

            all_data_frames.append(df_final)

        except Exception as e:
            logger.error(f"Failed to process shortcut '{shortcut_name}': {e}")
            continue

    if not all_data_frames:
        logger.info("No data extracted from any shortcut.")
        return

    # Union all
    df_combined = all_data_frames[0]
    for df in all_data_frames[1:]:
        df_combined = df_combined.unionByName(df, allowMissingColumns=True)

    record_count = df_combined.count()
    logger.info(f"Extracted {record_count} dataset records. Merging into {TARGET_FQ}...")

    # Idempotent MERGE (upsert) on natural key: shortcut_name + dataset_name
    delta_table = DeltaTable.forName(spark, TARGET_FQ)

    delta_table.alias("target").merge(
        df_combined.alias("source"),
        "target.shortcut_name = source.shortcut_name AND target.dataset_name = source.dataset_name"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    logger.info(f"Successfully loaded/updated {record_count} records in {TARGET_FQ}")

    # Show results
    display(spark.sql(f"SELECT * FROM {TARGET_FQ} ORDER BY shortcut_name, dataset_name"))


# =============================================================================
# Run
# =============================================================================

run_load_datasets_metadata()
