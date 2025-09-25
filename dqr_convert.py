# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
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

# JSON Schema for validation
DQ_RULE_CONSTRAINT_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "kwargs": {"type": "object"},
        "meta": {"type": "object"},
    },
    "required": ["type", "kwargs"],
}

def validate_config(config):
    """Validate configuration parameters"""
    required_keys = ["workspace_id", "lakehouse_id", "metadata_lakehouse_name", 
                    "observability_lakehouse_name", "excel_file_name", "json_file_name",
                    "sheet_name", "skip_rows", "table_name", "schema_name"]
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing configuration keys: {missing_keys}")

def create_file_paths(config):
    """Create file paths using configuration"""
    base_path = f"abfss://{config['workspace_id']}@onelake.dfs.fabric.microsoft.com/{config['lakehouse_id']}/Files/data_quality"
    return {
        "excel_path": f"{base_path}/{config['excel_file_name']}",
        "json_path": f"{base_path}/{config['json_file_name']}",
        "table_path": f"abfss://{config['workspace_id']}@onelake.dfs.fabric.microsoft.com/{config['lakehouse_id']}/Tables/{config['schema_name']}/{config['table_name']}"
    }

def process_excel_to_spark_df(spark, excel_path, sheet_name, skip_rows):
    """Read Excel and convert to Spark DataFrame with transformations"""
    try:
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
        
        # Read Excel directly into pandas with optimized settings
        pd_df = pd.read_excel(
            io=excel_path,
            sheet_name=sheet_name,
            skiprows=skip_rows,
            header=0,
            dtype=str
        ).dropna(how="all")
        
        if pd_df.empty:
            raise ValueError("Excel file is empty or contains no valid data after dropping null rows")
        
        # Clean string columns: strip whitespace and replace non-breaking spaces
        for col_name in pd_df.columns:
            if pd_df[col_name].dtype == "object":
                pd_df[col_name] = pd_df[col_name].astype(str).str.replace('\u00a0', '', regex=False).str.strip()
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame(pd_df)
        
        # Apply transformations
        window_spec = Window.orderBy("DQ Active Flag")
        df = (df.filter(col("Group") != "not active")
              .withColumn("DQ Active Flag", lit(1))
              .withColumn("DQ Effective Date", current_timestamp())
              .withColumn("DQ Expiration Date", lit("2099-12-31 23:59:59").cast("timestamp"))
              .withColumn("#", row_number().over(window_spec)))
        
        return df
    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise

def create_dq_json_struct(df):
    """Create structured JSON column"""
    try:
        logger.info("Creating JSON structure for DataFrame")
        return df.select(
            to_json(struct(
                col("#").alias("dq_rule_master_key"),
                col("DQ Rule ID").alias("dq_rule_id"),
                col("Data Product Name").alias("data_product_name"),
                col("Sub Domain Name").alias("sub_domain_name"),
                col("DQ Rule Description").alias("dq_rule_description"),
                col("DQ Rule Constraint").alias("dq_rule_constraint"),
                col("DQ Rule Dimension").alias("dq_rule_dimension"),
                col("DQ Screen Type").alias("dq_screen_type"),
                col("DQ Rule Applicable Lakehouse").alias("dq_rule_applicable_lakehouse"),
                col("DQ Rule Applicable Schema").alias("dq_rule_applicable_schema"),
                col("DQ Rule Applicable Object").alias("dq_rule_applicable_object"),
                col("DQ Rule Applicable Attribute").alias("dq_rule_applicable_attribute"),
                col("DQ Rule Failure Action").alias("dq_rule_failure_action"),
                col("DQ Rule Severity Score").alias("dq_rule_severity_score"),
                col("DQ Active Flag").alias("is_current_flag"),
                col("DQ Effective Date").alias("row_effective_date"),
                col("DQ Expiration Date").alias("row_expiration_date")
            )).alias("dq_json")
        )
    except Exception as e:
        logger.error(f"Error creating JSON structure: {str(e)}")
        raise

def save_json_to_onelake(json_data, json_path):
    """Save JSON data to OneLake"""
    storage_options = {
        "account_name": "onelake",
        "account_host": "onelake.dfs.fabric.microsoft.com",
    }
    try:
        logger.info(f"Saving JSON to {json_path}")
        onelake_fs = fsspec.filesystem("abfss", **storage_options)
        with onelake_fs.open(json_path, "w") as json_file:
            json.dump(json_data, json_file, indent=4)
    except Exception as e:
        logger.error(f"Error saving JSON to OneLake: {str(e)}")
        raise

def write_to_delta_table(spark, json_path, table_path, lakehouse_manager):
    """Write JSON data to Delta table"""
    try:
        if lakehouse_manager.check_if_table_exists(CONFIG["table_name"], CONFIG["schema_name"]):
            logger.info(f"Writing to Delta table at {table_path}")
            df = (spark.read
                  .option("multiLine", True)
                  .json(json_path)
                  .selectExpr(
                      "cast(dq_rule_master_key as int) as dq_rule_master_key",
                      "dq_rule_id",
                      "data_product_name",
                      "sub_domain_name",
                      "dq_rule_description",
                      "to_json(dq_rule_constraint) as dq_rule_constraint",
                      "dq_rule_dimension",
                      "dq_screen_type",
                      "dq_rule_applicable_lakehouse",
                      "dq_rule_applicable_schema",
                      "dq_rule_applicable_object",
                      "dq_rule_applicable_attribute",
                      "dq_rule_failure_action",
                      "cast(dq_rule_severity_score as double) as dq_rule_severity_score",
                      "cast(is_current_flag as boolean) as is_current_flag",
                      "cast(row_effective_date as timestamp) as row_effective_date",
                      "cast(row_expiration_date as timestamp) as row_expiration_date"
                  ))
            
            df.write.mode("overwrite").save(table_path)
            logger.info(f"Successfully wrote to Delta table at {table_path}")
        else:
            logger.warning(f"Table {CONFIG['table_name']} does not exist in schema {CONFIG['schema_name']}")
    except Exception as e:
        logger.error(f"Error writing to Delta table: {str(e)}")
        raise

def main():
    """Main function to process DQ rules"""
    spark = SparkSession.builder.appName("DQRuleProcessor").getOrCreate()
    
    try:
        # Validate configuration
        logger.info("Validating configuration")
        validate_config(CONFIG)
        
        # Initialize lakehouse managers
        logger.info("Initializing lakehouse managers")
        metadata_lakehouse = LakehouseManager(CONFIG["metadata_lakehouse_name"])
        observability_lakehouse = LakehouseManager(CONFIG["observability_lakehouse_name"])
        
        # Create file paths
        logger.info("Creating file paths")
        paths = create_file_paths(CONFIG)
        
        # Process Excel to Spark DataFrame
        logger.info("Processing Excel file")
        df = process_excel_to_spark_df(spark, paths["excel_path"], CONFIG["sheet_name"], CONFIG["skip_rows"])
        
        # Log DataFrame schema and row count for debugging
        logger.info(f"Input DataFrame schema: {df.schema}")
        logger.info(f"Input DataFrame row count: {df.count()}")
        
        # Create JSON structure
        logger.info("Creating JSON structure")
        df_json = create_dq_json_struct(df)
        
        # Check if df_json is empty
        if df_json.count() == 0:
            raise ValueError("JSON DataFrame is empty, cannot proceed with JSON processing")
        
        # Collect JSON data as a list of dictionaries
        logger.info("Collecting JSON data")
        json_rows = df_json.select("dq_json").collect()
        dq_json = [json.loads(row.dq_json) for row in json_rows]
        
        if not dq_json:
            raise ValueError("No valid JSON data collected from DataFrame")
        
        # Validate and parse dq_rule_constraint for each record
        logger.info("Validating JSON constraints")
        for idx in dq_json:
            if not isinstance(idx, dict):
                logger.error(f"Expected dictionary, got {type(idx)}: {idx}")
                raise TypeError(f"JSON record is not a dictionary: {idx}")
            try:
                idx["dq_rule_constraint"] = json.loads(idx["dq_rule_constraint"])
                validate(instance=idx["dq_rule_constraint"], schema=DQ_RULE_CONSTRAINT_SCHEMA)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in dq_rule_constraint: {idx['dq_rule_constraint']}")
                raise
        
        # Save JSON to OneLake
        logger.info("Saving JSON to OneLake")
        save_json_to_onelake(dq_json, paths["json_path"])
        logger.info(f"DQ template conversion complete. JSON saved at: {paths['json_path']}")
        
        # Write to Delta table
        logger.info("Writing to Delta table")
        write_to_delta_table(spark, paths["json_path"], paths["table_path"], observability_lakehouse)
        
    except Exception as e:
        logger.error(f"Error in DQ rule processing: {str(e)}")
        raise
    finally:
        logger.info("Stopping Spark session")
        spark.stop()

if __name__ == "__main__":
    main()
