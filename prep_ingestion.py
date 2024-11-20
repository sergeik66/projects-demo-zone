# Databricks notebook source
from onyx.ingestion.lib.prep.extract.raw_extract import RawExtract
from onyx.ingestion.lib.prep.load.load_delta import LoadDelta
from onyx.ingestion.lib.prep.transform.transform_dataframe import TransformDataframe
from onyx.ingestion.lib.prep.utils import utility_functions
from onyx.common.lib.table_utils import get_external_location_url
from pyspark.sql.functions import *
from delta.tables import *
from onyx.ingestion.lib.prep.ffl.cls_flexible_file_loader import FlexibleFileLoader as ffl
import json
import os

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
region_name = spark.conf.get("spark.databricks.clusterUsageTags.region")


# COMMAND ----------

def get_current_timestamp() -> object:
    return spark.sql("select current_timestamp() as current_datetime").collect()[0].current_datetime


# COMMAND ----------

processing_start_time = get_current_timestamp()

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("feed_name", "")
dbutils.widgets.text("source_config_folder_name", "")
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("etl_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("etl_start_date_time", "")
dbutils.widgets.text("product_name", "")
dbutils.widgets.text("ffl_file_path", "")


# COMMAND ----------

# MAGIC %run "../includes/connection/general_parameters"

# COMMAND ----------

def send_msg_to_queue(message_metadata: object, metadata_config_dict: object) -> object:
    """

    Args:
        message_metadata ():
        metadata_config_dict ():
    """
    message_body = {
        "product_name": product_name,
        "feed_name": feed_name,
        "dataset_name": metadata_config_dict['datasetName'],
        "source_system": metadata_config_dict['sourceSystemProperties']['sourceSystemName'],
        "metadata": message_metadata,
        "zone": "Prepared",
        "stage": "Transformation",
        "orchestration_tool": "databricks",
        "zone_end_date_time": str(get_current_timestamp()),
        "etl_id": etl_id,
        "run_id": run_id
    }
    message_body = json.dumps(message_body)
    cloudprovider = spark.conf.get("spark.databricks.cloudProvider")

    if cloudprovider == 'Azure':
        from azure.storage.queue import QueueClient
        try:
            storage_queue_url = dbutils.secrets.get(scope=secretsScope, key="onyx-adls-queue-id")
            storage_key = dbutils.secrets.get(scope=secretsScope, key="onyx-adls-key")
            queue_client = QueueClient.from_queue_url(queue_url=storage_queue_url, credential=storage_key)
            queue_client.send_message(message_body)
        except Exception as error:
            raise error

    elif cloudprovider == 'AWS':
        import boto3
        try:
            queue_url = dbutils.secrets.get(scope=secretsScope, key="standard_queue_url")
            client = boto3.client('sqs', region_name=region_name)
            client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body
            )
        except Exception as error:
            raise error

    else:
        print(f"DOF does not support {cloudprovider} cloud ")


# COMMAND ----------

try:
    ####################################################################################################
    # widget inputs
    feed_name = dbutils.widgets.get("feed_name")
    file_name = dbutils.widgets.get("file_name")
    source_config_folder_name = dbutils.widgets.get("source_config_folder_name")
    etl_id = dbutils.widgets.get("etl_id")
    run_id = dbutils.widgets.get("run_id")
    product_name = dbutils.widgets.get("product_name")
    ffl_file_path = dbutils.widgets.get("ffl_file_path")
    etl_start_date_time = dbutils.widgets.get("etl_start_date_time")
    # derived params
    metadata_json_path = f"{get_external_location_url('metadata')}/datasets/{source_config_folder_name}/{file_name}.json"
    # metadata params
    load_date_timestamp = current_timestamp()
    load_date = current_date()

    ####################################################################################################
    # Get Metadata JSON File
    metadata_config_dict = utility_functions.read_json_file(spark, metadata_json_path)
    metadata_config_dict["feedName"] = feed_name
    prep_properties = metadata_config_dict["prepProperties"]
    catalog_name = prep_properties.get("catalogName", os.getenv("ONYX_ENV_PRODUCT_CATALOG"))
    schema_name = "ext_" + metadata_config_dict["sourceSystemProperties"]["sourceSystemName"]

    ####################################################################################################
    # Extract Raw Data
    raw_extract_obj = RawExtract(spark, metadata_config_dict)
    # FIXME Ideally the logic for this entire block can be encapsulated in metadata_config along with other FFL refactors
    if "ffl_config" in metadata_config_dict:
        fileType = metadata_config_dict["fileType"]
        ffl_config = metadata_config_dict["ffl_config"]
        source_container_name = metadata_config_dict["rawProperties"]["containerName"]

        external_location = f"{get_external_location_url(source_container_name)}/{source_config_folder_name}"
        spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog_name}.storage_volumes.{source_config_folder_name} LOCATION '{external_location}'")

        base_volume_path = f"/Volumes/{catalog_name}/storage_volumes"
        sourceFile = f"{base_volume_path}/{ffl_file_path}"
        ffl_instance = ffl(base_volume_path=base_volume_path, file_path=sourceFile, file_format=fileType, file_loader_params=ffl_config)
        input_df = ffl_instance.load_file()
    else:
        input_df = raw_extract_obj.get_raw_input_df()  # get the streaming dataframe

        # Select mentioned columns only
        col_list = prep_properties["columnList"].split(',')
        input_df = input_df.select(*col_list)

        ####################################################################################################    

    # Transform Data
    # transform input dict
    transform_input = {}
    transform_input["input_df"] = input_df
    transform_input["spark_instance"] = spark
    transform_input["dbutils"] = dbutils
    transform_input["metadata_config_dict"] = metadata_config_dict
    transform_input["schema_name"] = schema_name
    transform_input["etl_id"] = etl_id
    transform_input["etl_start_date_time"] = etl_start_date_time
    transform_input["secret_scope"] = secretsScope
    transform_input["source_config_folder_name"] = source_config_folder_name
    transform_input["load_date_timestamp"] = load_date_timestamp
    transform_input["load_date"] = load_date
    transform_input["feed_name"] = feed_name
    transform_input["product_name"] = product_name
    # create Transform class obj
    transform_obj = TransformDataframe(**transform_input)

    if prep_properties["enableSensitiveFieldCheck"]:
        global_config_path = f"{get_external_location_url('metadata')}/global/global_config.json"
        global_config_dict = utility_functions.read_json_file(spark, global_config_path)
        encryption_enabled = False
        for i in global_config_dict['global_configuration']:
            if i['config_name'] == 'data_privacy_mode' and i['enable'] == True:
                encryption_enabled = True

        if encryption_enabled:
            transform_obj.encrypt_sensitivie_fields()
        else:
            transform_obj.masking_columns_to_null()
    transform_input["input_df"] = transform_obj.input_df

    ####################################################################################################
    # Load Data in Delta
    # load delta input dict
    load_delta_input = transform_input.copy()
    del load_delta_input["load_date_timestamp"]
    del load_delta_input["load_date"]
    del load_delta_input["product_name"]
    callable_methods = []
    if prep_properties["duplicateCheckEnabled"]:
        callable_methods.append("process_duplicate_data")
    callable_methods.append("add_other_columns")
    if prep_properties["deletionCaptureEnabled"]:
        callable_methods.append("get_deletion_capture_df")
    callable_methods.append('get_row_count')
    load_delta_input["transform_instance"] = transform_obj
    load_delta_input["callable_methods"] = callable_methods
    load_delta_input["dbutils"] = dbutils
    # create LoadDelta class obj
    load_delta_obj = LoadDelta(**load_delta_input)
    if "ffl_config" in metadata_config_dict:
        load_delta_obj.perform_load_operation_for_ffl()
    else:
        load_delta_obj.perform_load_operation()
    ####################################################################################################
    # Capture prep table metrics for rowcounts
    sql = f"describe history {load_delta_obj.catalog_name}.{schema_name}.{load_delta_obj.table_name}"
    pddf = spark.sql(sql).where(f"operation != 'OPTIMIZE' and timestamp >= '{processing_start_time}'").orderBy(desc("version")).toPandas()
    total_rows_written = 0
    total_data_written = 0
    for index, row in pddf.iterrows():
        total_rows_written += int(row['operationMetrics'].get('numOutputRows', 0))
        total_data_written += int(row['operationMetrics'].get('numOutputBytes', 0))

    ####################################################################################################
    data = {
        "total_rows_read": transform_obj.input_row_count,
        "total_rows_written": total_rows_written,
        "total_data_written": total_data_written,
        "zone_start_date_time": str(processing_start_time)
    }

except Exception as error:
    data = {
        "error_message": str(error),
        "zone_start_date_time": str(processing_start_time)
    }
    
    print("Exception occured while processing the data: ", error)
    raise error
finally:
    message_metadata = {"runOutput": data}
    send_msg_to_queue(message_metadata, metadata_config_dict)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(data))
