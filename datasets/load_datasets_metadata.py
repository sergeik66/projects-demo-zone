"""
load_datasets_metadata.py - Production Ready

This script runs in a Microsoft Fabric notebook AFTER create_shortcuts.py has successfully created shortcuts.

It scans all top-level folders under Files/ (i.e., your shortcuts), skips any folder named "watermark" (case-insensitive),
reads every *.json file in those folders, extracts the required metadata fields from the JSON (based on your sample),
and upserts into a single Delta table: catalog.datasets

Target columns:
- shortcut_name
- dataset_name
- dataset_type_name
- database_name
- source_system_ingest_type
- target_load_type

Fully idempotent – safe to rerun.
"""

import logging
from pathlib import PurePosixPath

import sempy.fabric as fabric
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, lit, col, lower, when, coalesce,
    regexp_extract, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# Setup
# =============================================================================

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

SHORTCUT_ROOT = "Files/"                     # All shortcuts are created under Files/
EXCLUDED_FOLDERS = {"watermark"}             # Skip these folder names (case-insensitive)

TARGET_SCHEMA = "catalog"
TARGET_TABLE = "datasets"
TARGET_FQ = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

# Mapping: target column → JSON path in the sample file
FIELD_MAPPING = {
    "dataset_name":             ["datasetName"],
    "dataset_type_name":        ["datasetTypeName"],
    "database_name":            ["databaseName"],
    "source_system_ingest_type": ["sourceSystemProperties", "ingestType"],
    "target_load_type":         ["curatedProperties", "targetLoadType"]
}

# =============================================================================
# Helper Functions
# =============================================================================

def ensure_target_table():
    """Create the catalog.datasets Delta table if it doesn't exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    
    schema_sql = """
    CREATE TABLE IF NOT EXISTS {target} (
        shortcut_name             STRING,
        dataset_name              STRING,
        dataset_type_name         STRING,
        database_name             STRING,
        source_system_ingest_type STRING,
        target_load_type          STRING
    ) USING DELTA
    """.format(target=TARGET_FQ)
    
    spark.sql(schema_sql)
    logger.info(f"Ensured Delta table {TARGET_FQ} exists.")

def list_shortcut_folders() -> list[str]:
    """Return list of shortcut folder paths (e.g. Files/raw_iis/, Files/curated_policy/) excluding watermark."""
    try:
        df = spark.sql(f"LIST '{SHORTCUT_ROOT}'")
        folders = (
            df
            .filter(col("is_directory"))
            .filter(~lower(col("name")).isin([f.lower() for f in EXCLUDED_FOLDERS]))
            .select(col("name"))
            .rdd.map(lambda r: r.name.rstrip("/"))
            .collect()
        )
        paths = [f"{SHORTCUT_ROOT}{folder}/" for folder in folders]
        logger.info(f"Found {len(paths)} shortcut folders (excluding watermark): {folders}")
        return paths
    except Exception as e:
        logger.error(f"Failed to list Files/ folder: {e}")
        return []

def build_json_schema() -> StructType:
    """Build a permissive schema that allows missing fields."""
    fields = [
        StructField("datasetName", StringType(), True),
        StructField("datasetTypeName", StringType(), True),
        StructField("databaseName", StringType(), True),
        StructField("sourceSystemProperties", 
            StructType([
                StructField("ingestType", StringType(), True)
            ]), True),
        StructField("curatedProperties", 
            StructType([
                StructField("targetLoadType", StringType(), True)
            ]), True),
    ]
    return StructType(fields)

def extract_fields(df_json, shortcut_name):
    """Extract required fields using nested column access."""
    df = df_json.withColumn("shortcut_name", lit(shortcut_name))
    
    # Direct fields
    df = df.withColumn("dataset_name", col("datasetName"))
    df = df.withColumn("dataset_type_name", col("datasetTypeName"))
    df = df.withColumn("database_name", col("databaseName"))
    
    # Nested fields with safety
    df = df.withColumn("source_system_ingest_type", 
                       col("sourceSystemProperties.ingestType"))
    df = df.withColumn("target_load_type", 
                       col("curatedProperties.targetLoadType"))
    
    # Select only needed columns
    return df.select(
        "shortcut_name",
        "dataset_name",
        "dataset_type_name",
        "database_name",
        "source_system_ingest_type",
        "target_load_type"
    )

# =============================================================================
# Main Execution
# =============================================================================

def run_load_datasets_metadata():
    ensure_target_table()
    
    shortcut_folders = list_shortcut_folders()
    if not shortcut_folders:
        logger.info("No shortcut folders found to process.")
        return
    
    all_data_frames = []
    
    for folder_path in shortcut_folders:
        shortcut_name = PurePosixPath(folder_path.rstrip("/")).name
        json_pattern = f"{folder_path}*.json"
        
        logger.info(f"Processing shortcut '{shortcut_name}' → {json_pattern}")
        
        try:
            # Read all JSON files (multiline possible)
            df_raw = (
                spark.read
                .schema(build_json_schema())
                .option("multiline", "true")
                .json(json_pattern)
            )
            
            if df_raw.rdd.isEmpty():
                logger.info(f"No JSON files found in {shortcut_name}")
                continue
            
            df_extracted = extract_fields(df_raw, shortcut_name)
            all_data_frames.append(df_extracted)
            
        except Exception as e:
            logger.error(f"Failed to read/process JSON files in {shortcut_name}: {e}")
            continue
    
    if not all_data_frames:
        logger.info("No data extracted from any shortcut. Nothing to load.")
        return
    
    # Union all data
    df_final = all_data_frames[0]
    for df in all_data_frames[1:]:
        df_final = df_final.unionByName(df, allowMissingColumns=True)
    
    logger.info(f"Total records extracted: {df_final.count()}")
    
    # Upsert into target table (idempotent: merge on natural key)
    # Natural key = shortcut_name + dataset_name (unique per JSON file)
    delta_table = DeltaTable.forName(spark, TARGET_FQ)
    
    delta_table.alias("target").merge(
        df_final.alias("source"),
        "target.shortcut_name = source.shortcut_name AND target.dataset_name = source.dataset_name"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    logger.info(f"Successfully merged data into {TARGET_FQ}")

# =============================================================================
# Run it
# =============================================================================

run_load_datasets_metadata()

# Optional: Show final content
display(spark.sql(f"SELECT * FROM {TARGET_FQ} ORDER BY shortcut_name, dataset_name"))
