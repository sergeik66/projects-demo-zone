"""
load_datasets_metadata.py - Final Version

Processes shortcuts defined in shortcuts.yaml.
For each shortcut:
  - Scans Files/<shortcut_name>/** recursively
  - Skips any subfolder exactly named "watermark" (case-insensitive)
  - Reads all *.json files from all other (sub)folders
  - Extracts metadata and upserts into catalog.datasets
"""

import logging
from typing import List, Dict, Any

import yaml
import sempy.fabric as fabric
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, input_file_name, lower, split
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

ws_name = spark.conf.get("trident.workspace.name", "").upper()
is_prd = "-PRD" in ws_name
config_variant = "_prd" if is_prd else ""

workspace_id = fabric.get_workspace_id()
lakehouse_id = fabric.get_lakehouse_id()

CONFIG_PATH = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/shortcuts_config/"
    f"shortcuts{config_variant}.yaml"
)

TARGET_SCHEMA = "catalog"
TARGET_TABLE = "datasets"
TARGET_FQ = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

FIELD_MAPPING = {
    "dataset_name": ["datasetName"],
    "dataset_type_name": ["datasetTypeName"],
    "database_name": ["databaseName"],
    "source_system_ingest_type": ["sourceSystemProperties", "ingestType"],
    "target_load_type": ["curatedProperties", "targetLoadType"]
}

# Subfolder name to completely skip (case-insensitive)
SKIP_SUBFOLDER_NAME = "watermark"

# =============================================================================
# Helpers
# =============================================================================

def load_shortcuts_config(path: str) -> Dict[str, Any]:
    from notebookutils import mssparkutils
    try:
        content = mssparkutils.fs.head(path)
        return yaml.safe_load(content)
    except Exception as e:
        raise FileNotFoundError(f"Config not found or unreadable: {path} → {e}")

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
    logger.info(f"Target table {TARGET_FQ} ready.")

def build_json_schema() -> StructType:
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

    logger.info(f"Loading shortcuts config from {CONFIG_PATH}")
    config = load_shortcuts_config(CONFIG_PATH)
    shortcuts = config.get("shortcuts", [])

    if not shortcuts:
        logger.info("No shortcuts defined in config.")
        return

    all_data_frames = []

    for entry in shortcuts:
        shortcut_name = entry.get("name")
        if not shortcut_name:
            logger.warning("Skipping entry without 'name'")
            continue

        shortcut_folder = f"Files/{shortcut_name}"
        json_pattern = f"{shortcut_folder}/**/*.json"   # Recursive

        logger.info(f"Processing shortcut '{shortcut_name}' → recursive scan: {json_pattern}")

        try:
            # Read all JSON files recursively
            df_raw = (
                spark.read
                .schema(build_json_schema())
                .option("multiline", "true")
                .json(json_pattern)
            )

            if df_raw.rdd.isEmpty():
                logger.info(f"No JSON files found under '{shortcut_name}'")
                continue

            # Add input file name to detect path
            df_with_path = df_raw.withColumn("file_path", input_file_name())

            # Extract folder parts and check for "watermark" subfolder
            # Split path and get all directory parts after the shortcut name
            df_with_path = df_with_path.withColumn(
                "path_parts",
                split(col("file_path"), "/")
            )

            # Check if any directory segment == "watermark" (case-insensitive)
            from pyspark.sql.functions import array_contains, lower as spark_lower
            df_with_path = df_with_path.withColumn(
                "has_watermark_folder",
                array_contains(spark_lower(col("path_parts")), SKIP_SUBFOLDER_NAME.lower())
            )

            # Filter out files that are inside a "watermark" folder
            df_filtered = df_with_path.filter(~col("has_watermark_folder"))

            if df_filtered.rdd.isEmpty():
                logger.info(f"All JSON files in '{shortcut_name}' are inside a 'watermark' folder → skipped")
                continue

            # Drop helper columns
            df_clean = df_filtered.drop("file_path", "path_parts", "has_watermark_folder")

            # Add shortcut_name
            df_clean = df_clean.withColumn("shortcut_name", lit(shortcut_name))

            # Extract required fields
            for target_col, keys in FIELD_MAPPING.items():
                nested = ".".join(keys)
                df_clean = df_clean.withColumn(target_col, col(nested))

            df_final = df_clean.select(
                "shortcut_name",
                "dataset_name",
                "dataset_type_name",
                "database_name",
                "source_system_ingest_type",
                "target_load_type"
            )

            all_data_frames.append(df_final)

        except Exception as e:
            logger.error(f"Error processing shortcut '{shortcut_name}': {e}")
            continue

    if not all_data_frames:
        logger.info("No valid data extracted from any shortcut.")
        return

    # Combine all
    df_combined = all_data_frames[0]
    for df in all_data_frames[1:]:
        df_combined = df_combined.unionByName(df, allowMissingColumns=True)

    count = df_combined.count()
    logger.info(f"Extracted {count} records. Upserting into {TARGET_FQ}...")

    # Idempotent merge
    DeltaTable.forName(spark, TARGET_FQ).alias("target").merge(
        df_combined.alias("source"),
        "target.shortcut_name = source.shortcut_name AND target.dataset_name = source.dataset_name"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    logger.info(f"Successfully updated {TARGET_FQ} with {count} records.")

    # Final display
    display(spark.sql(f"SELECT * FROM {TARGET_FQ} ORDER BY shortcut_name, dataset_name"))


# =============================================================================
# Run
# =============================================================================

run_load_datasets_metadata()
