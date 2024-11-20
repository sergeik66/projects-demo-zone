# Databricks notebook source
import json
from os import getenv
from pyspark.sql.functions import col, lit
from onyx.common.lib.error_and_retry_handlers import delta_operation_handler
from onyx.common.lib.etl_zone_log_table_handler import EtlZoneLogTableHandler
from onyx.common.lib.etl_log_table_handler import EtlLogTableHandler
from onyx.common.lib.application_logging import get_onyx_logger
from onyx.common.lib.table_utils import get_external_location_url
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType, DoubleType
from datetime import datetime
from typing import Union

logger = get_onyx_logger("notebook_process_logs")

# Uncomment if debug output required
# from onyx.common.lib.application_logging import turn_on_global_debug
# turn_on_global_debug() 

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("etl_id", "")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

cloud_provider = spark.conf.get("spark.databricks.cloudProvider")

# COMMAND ----------

# MAGIC %run "../connection/general_parameters"

# COMMAND ----------

# FIXME This widget value (run_id) doesn't get used - can stop passing it.
pipeline_run_id = dbutils.widgets.get("run_id")
etl_id = dbutils.widgets.get("etl_id")

etl_zone_log_table_handler = EtlZoneLogTableHandler(spark)
etl_log_table_handler = EtlLogTableHandler(spark)
catalog_name = getenv("ONYX_ENV_LOGS_CATALOG")

# COMMAND ----------

# DBTITLE 1,Get Raw Zone Run Status
def get_raw_zone_final_status(events_list):
    # iterate raw zone messages of a run
    for event in events_list:
        # check if any raw zone copy activity is failed
        if "errors" in event['metadata']:
            # This key may exist but be empty
            if len(event['metadata']['errors']) > 0 or event['metadata']['executionDetails'][0]['status'] == 'Canceled':
                return 'Fail'
    return 'Success'

# COMMAND ----------

# DBTITLE 1,get_source_ingestion_log_schema
def get_source_ingestion_log_schema() -> StructType:
    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("feed_name", StringType(), True),
        StructField("metadata", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("data_read", LongType(), True),
        StructField("data_written", LongType(), True),
        StructField("files_read", IntegerType(), True),
        StructField("files_written", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("throughput", DoubleType(), True),
        StructField("copy_duration", IntegerType(), True),
        StructField("inserted_by", StringType(), True),
        StructField("modified_by", StringType(), True),
        StructField("zone", StringType(), True)
    ])
    return schema

# COMMAND ----------

# DBTITLE 1,Get Prepared Zone Run Status
def get_prepared_zone_final_status(events_list):
    # iterate raw zone messages of a run
    for event in events_list:
        # check if any prep zone copy activity is failed
        if 'error_message' in event['metadata']:
            return 'Fail'
    return 'Success'

# COMMAND ----------

# DBTITLE 1,Get Prepared Zone Total Data Size in MB
def get_prepared_data_size(events_list):
    data_size = 0
    # iterate raw zone messages of a run
    for event in events_list:
        # calculate data size
        try:
            data_size += event['metadata']['runOutput']['total_data_written']
        except KeyError:
            pass
    return data_size #in bytes

# COMMAND ----------

# DBTITLE 1,Get Raw Zone Total Data Size in MB
def get_raw_data_size(events_list):
    data_size = 0
    # iterate raw zone messages of a run
    for event in events_list:
        # calculate data size
        try:
            data_size += event['metadata']['dataWritten']
        except KeyError:
            pass
    return data_size # in bytes

# COMMAND ----------

# DBTITLE 1,Get Raw Zone Start Time
def get_raw_zone_start_time(res):
    if "executionDetails" in res['metadata'] :
        return res['metadata']['executionDetails'][0]['start']
    return None

# COMMAND ----------

# DBTITLE 1,Get Prep Zone Start Time
def get_prepared_zone_start_time(res):
    if "runOutput" in res['metadata'] :
        return res['metadata']['runOutput']['zone_start_date_time']
    return None


# COMMAND ----------

def get_current_user():
    return delta_operation_handler(lambda: spark.sql("select current_user() as current_user").collect()[0].current_user)

# COMMAND ----------

# DBTITLE 1,Insert Log entry in etl.log
def insert_zone_level_log(container_name, table_name, queue_dict, etl_id, zone, is_existing_id):
    data_size = 0
    zone_start_date_time = '1900-01-01T00:00:00.0000000Z'
    storage_path = f"{get_external_location_url(container_name)}/{table_name}"
    stage = None
    if(zone == 'Raw'):
        # get raw zone final status
        status = get_raw_zone_final_status(queue_dict[etl_id]['Raw'])
        # get the first item in the list which is raw zone's latest record of a
        # run id
        res = queue_dict[etl_id]['Raw'][0]
        zone_start_date_time = get_raw_zone_start_time(queue_dict[etl_id]['Raw'][-1])
        stage = 'Ingestion'
        if status == 'Success':
            data_size = get_raw_data_size(queue_dict[etl_id]['Raw'])
    elif(zone == 'Prepared'):
        # get prepared zone final status
        status = get_prepared_zone_final_status(queue_dict[etl_id]['Prepared'])
        zone_start_date_time = get_prepared_zone_start_time(queue_dict[etl_id]['Prepared'][-1])
        # get the first item in the list which is prepared zone's latest record
        # of a run id
        res = queue_dict[etl_id]['Prepared'][0]
        stage = res['stage']
        if status == 'Success':
            data_size = get_prepared_data_size(queue_dict[etl_id]['Prepared'])
    if not is_existing_id:
        # prepare the value to be inserted
        rows = [[int(etl_id),
                 res['run_id'],
                 zone_start_date_time,
                 res['product_name'],
                 res['feed_name'],
                 res['zone'],
                 stage,
                 res['zone_end_date_time'],
                 status,
                 data_size]]
        etl_zone_log_df = spark.createDataFrame(rows,
                                           ['etl_id',
                                            'run_id',
                                            'zone_start_date_time',
                                            'product_name',
                                            'feed_name',
                                            'zone',
                                            'stage',
                                            'zone_end_date_time',
                                            'zone_status',
                                            'total_data_storage'])
        # prepare dataframe with extended columns
        etl_zone_log_df = (etl_zone_log_df
                                .withColumn('zone_start_date_time', col('zone_start_date_time').cast('timestamp'))
                                .withColumn('zone_end_date_time', col('zone_end_date_time').cast('timestamp')))
        # write to etl_log table
        delta_operation_handler(
            f=lambda: etl_zone_log_df.coalesce(1).write.format('delta').mode('append').partitionBy('product_name', 'feed_name', 'zone', 'stage').save(storage_path),
            retry_count=5
        )
    else:
        input_params = {
            'zone_end_date_time': res['zone_end_date_time'],
            'status': status,
            'zone': res['zone'],
            'etl_id': res['etl_id'],
            'total_data_storage': data_size,
            'stage': stage
        }
        etl_zone_log_table_handler.update_status_for_etl_zone_log(**input_params)
    if status == 'Fail':
        container_name,table_name = ('audit', 'etl_log')
        etl_log_table_handler.mark_etl_failed(etl_id)


# COMMAND ----------

def insert_etl_zone_log(queue_dict):
    # iterate queue messages
    container_name,table_name = ('audit', 'etl_zone_log')
    for etl_id in queue_dict:
        already_recorded = delta_operation_handler(lambda: spark.sql(
            f"SELECT * FROM {catalog_name}.audit.etl_zone_log where etl_id = '{etl_id}' "))
        for key, value in queue_dict[etl_id].items():
            already_raw_recorded = already_recorded.filter(col('zone') == key)
            is_existing_id = len(already_raw_recorded.take(1)) == 1
            insert_zone_level_log(container_name, table_name, queue_dict, etl_id, key, is_existing_id)

# COMMAND ----------

def add_raw_metadata_columns(response_json, df_log_append):
    response_json = response_json['metadata']
    status = 'Success'
    files_read = None
    files_written = None
    throughput = None
    data_read = None
    data_written = None
    copy_duration = None
    if 'errors' in response_json and len(response_json['errors']) > 0 or response_json['executionDetails'][0]['status'] == 'Canceled':
        status = 'Failed'
    if 'filesRead' in response_json: # for blob source
        files_read = response_json['filesRead']
    elif 'rowsRead' in response_json: #for database source
        files_read = response_json['rowsRead']
    if 'filesWritten' in response_json: # for blob source
        files_written = response_json['filesWritten']
    elif 'rowsCopied' in response_json: #for database source
        files_written = response_json['rowsCopied']
    if 'throughput' in response_json: 
        throughput = response_json['throughput']
    if 'dataRead' in response_json:
        data_read = response_json['dataRead']
    if 'dataWritten' in response_json:
        data_written = response_json['dataWritten']
    if 'copyDuration' in response_json:
        copy_duration = response_json['copyDuration']
    return (df_log_append.withColumn('data_read', lit(data_read).cast('bigint'))
                         .withColumn('data_written', lit(data_written).cast('bigint'))
                         .withColumn('files_read', lit(files_read))
                         .withColumn('files_written', lit(files_written))
                         .withColumn('status', lit(status))
                         .withColumn('throughput', lit(throughput).cast('double'))
                         .withColumn('copy_duration', lit(copy_duration).cast('integer')))

# COMMAND ----------

def add_prep_metadata_columns(response_json, df_log_append):
    response_json = response_json['metadata']['runOutput']
    status = 'Success'
    if 'error_message' in response_json:
        status = 'Failed'
    try:
        total_rows_read = response_json['total_rows_read']
    except KeyError:
        total_rows_read = -1
    try:
        total_rows_written = response_json['total_rows_written']
    except KeyError:
        total_rows_written = -1
    return (df_log_append.withColumn('data_read', lit(total_rows_read).cast('bigint'))
                  .withColumn('data_written', lit(total_rows_written).cast('bigint'))
                  .withColumn('status', lit(status)))

# COMMAND ----------

# DBTITLE 1,convert_string_to_datetime

def convert_string_to_datetime(timestamp_string: str) -> Union[datetime, None]:
    """
    Try to convert a string to a datetime

    Args:
        timestamp_string (str): The string to be converted

    Returns:
        Union[datetime, None]: The converted datetime or, if conversion failed, None
    """
    ts_formats = [
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO 8601
        "%Y-%m-%d %H:%M:%S.%f",  # Other timestamp formats seen in messages
        "%Y-%m-%d %H:%M:%S"
    ]

    return_value = None
    for fmt in ts_formats:
        try:
            return_value = datetime.strptime(timestamp_string, fmt)
            break
        except ValueError:
            pass
    if return_value is None:
        logger.warning(f"Unable to convert string '{timestamp_string}' into a datetime. A null value will be written instead.")
    return return_value

# COMMAND ----------

# DBTITLE 1,Process messages for AWS
def get_msg_from_sqs_and_add_entry():
    import boto3
    region_name = spark.conf.get("spark.databricks.clusterUsageTags.region")
    print("Creating queue client: ")
    queue_url = dbutils.secrets.get(scope=secretsScope, key="standard_queue_url")
    logger.debug(f"SQS URL composed as: '{queue_url}'")
    client = boto3.client('sqs',region_name=region_name)
    
    get_queue_msg_count = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'All',
        ]
    )

    # Get Approx msgs count in queue
    # We don't actually use this for anything - it's not reliable (as per docs/the name
    # including "Approximate") - but it might be useful to see an approximate count in output
    no_of_msg_in_queue = get_queue_msg_count.get("Attributes")['ApproximateNumberOfMessages']
    print("Message count: " + str(no_of_msg_in_queue))
    current_user = get_current_user()
    logger.debug(f"Current user: {current_user}")
    container_name,table_name = ('audit', 'source_ingestion_log')
    storage_path = f"{get_external_location_url(container_name)}/{table_name}"
    logger.debug(f"Path for source_ingestion_log: '{storage_path}'")
    queue_dict = {}
    sqs_message_batch = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["All"],
        MaxNumberOfMessages=10  # This is the maximum supported value
    )
    logger.debug(f"Init batch contained {len(sqs_message_batch.get('Messages', []))} messages")
    # Setup for this iteration
    last_call_returned_no_messages = False
    messages_to_delete = []

    # Docs say receive_message() can return nothing, and to repeat the request to verify the
    # queue is actually empty
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#receive-message
    while (len(sqs_message_batch.get("Messages", [])) > 0) or (last_call_returned_no_messages is False):
        logger.debug(f"The batch currently contains {len(sqs_message_batch.get('Messages', []))} messages")
        logger.debug(f"Last call received no messages: {last_call_returned_no_messages}")

        if len(sqs_message_batch.get("Messages", [])) == 0:
            last_call_returned_no_messages = True
        else:
            last_call_returned_no_messages = False
        row_values = []

        print(f'Adding records to source ingestion log table for etl id : {etl_id}')
        for message in sqs_message_batch.get("Messages", []):
            try:
                res = json.loads(message.get("Body"))  # Transform to dictionary
                logger.debug(f"'res' variable value: {res}")
                # filter the messages assocaited with the pipeline run id which
                # got triggered
                is_raw_zone = None
                if "Raw" == res['zone']:
                    is_raw_zone = True
                    zone_start_date_time = get_raw_zone_start_time(res)
                elif "Prepared" == res['zone']:
                    zone_start_date_time = get_prepared_zone_start_time(res)
                # casting values to string to avoid type inference issue with NULL values when creating the dataframe
                
                # Derive the new row
                if is_raw_zone:
                    res_metadata = res.get("metadata")
                else:  # prepared
                    res_metadata = res.get("metadata").get("runOutput")

                logger.debug(f"'res_metadata' variable value: {res_metadata}")
                if "errors" in res_metadata and len(res_metadata["errors"]) > 0:
                    new_row_status = "Failed"
                else:
                    new_row_status = "Success"

                if "filesRead" in res_metadata:
                    new_row_files_read = res_metadata.get("filesRead", None)
                else:
                    new_row_files_read = res_metadata.get("rowsRead", None)

                if "filesWritten" in res_metadata:
                    new_row_files_written = res_metadata.get("filesWritten", None)
                else:
                    new_row_files_written = res_metadata.get("rowsCopied", None)

                if is_raw_zone:
                    new_row_data_read = res_metadata.get("dataRead", -1)
                else:  # prepared
                    new_row_data_read = res_metadata.get("total_rows_read", -1)

                new_row_start_time = convert_string_to_datetime(zone_start_date_time)
                new_row_end_time = convert_string_to_datetime(res["zone_end_date_time"])

                # Ordinal position is material. Must match schema.
                new_row = [
                    str(res["run_id"]),  # run_id
                    str(res["product_name"]),  # product_name
                    str(res["source_system"]),  # source_system
                    str(res["dataset_name"]),  # dataset_name
                    str(res["feed_name"]),  # feed_name
                    json.dumps(res["metadata"]),  # metadata
                    new_row_start_time,  # start_time
                    new_row_end_time,  # end_time
                    new_row_data_read,  # data_read
                    res_metadata.get("dataWritten", None),  # data_written
                    new_row_files_read,  # files_read
                    new_row_files_written,  # files_written
                    new_row_status,  # status
                    res_metadata.get("throughput", None),  # throughput
                    res_metadata.get("copyDuration", None),  # copy_duration
                    current_user,  # inserted_by
                    current_user,  # modified_by
                    res.get("zone", None)  # zone
                ]
                logger.debug(f"Adding new row to write batch: {new_row}")
                row_values.append(new_row)

                # process etl log
                if res['etl_id'] not in queue_dict:
                    queue_dict[res['etl_id']] = {res['zone']: []}
                elif res['zone'] not in queue_dict[res['etl_id']]:
                    queue_dict[res['etl_id']][res['zone']] = []
                queue_dict[res['etl_id']][res['zone']].append(res)
                queue_dict[res['etl_id']][res['zone']].sort(
                    key=lambda x: x['zone_end_date_time'], reverse=True)

                logger.debug(f"Queueing processed message with ID '{message.get('MessageId')}' and receipt handle '{message.get('ReceiptHandle')}' for deletion")
                messages_to_delete.append(
                    {
                        "Id": message.get("MessageId"),
                        "ReceiptHandle": message.get("ReceiptHandle")
                    }
                )
            except Exception as e:
                print(f'Exception occured while processing the message for etl id: {etl_id} ; and queue message Id: {message.get("MessageId")}.')
                print(f'Please check the message and clear if necessary then rerun this notebook to process the event messages. Exception type: {type(e)}; Exception Message: {e}')
                raise e

        # Write any rows we've collected
        if len(row_values) > 0:
            logger.debug(f"We have {len(row_values)} rows to write. Assembling DataFrame...")
            schema = get_source_ingestion_log_schema()
            df_log_append = spark.createDataFrame(
                row_values,
                schema=schema
            )
            logger.debug("Writing Dataframe...")
            delta_operation_handler(
                f=lambda: df_log_append.write.format('delta').mode("append").partitionBy(
                    "product_name",
                    "feed_name",
                    "source_system",
                    "dataset_name").save(storage_path),
                retry_count=5
            )
            logger.debug("Wrote DataFrame")
            del df_log_append

        # Delete the batch contents from the queue
        if len(messages_to_delete) > 0:
            logger.debug(f"Trying to delete {len(messages_to_delete)} messages...")
            client.delete_message_batch(
                QueueUrl=queue_url,
                Entries=messages_to_delete
            )

        print(f'Adding records to etl log table for etl id: {etl_id}')
        # Pull messages for the next iteration
        sqs_message_batch = client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=10
        )
        insert_etl_zone_log(queue_dict)
        messages_to_delete = []
    logger.debug("Didn't receive any messages for the last two calls - the queue should be empty now")

# COMMAND ----------

# DBTITLE 1,Process messages for Azure
def get_msg_from_queue_and_add_entry():
    from azure.storage.blob import BlobServiceClient
    from azure.storage.queue import QueueClient
    storage_key = dbutils.secrets.get(scope=secretsScope, key="onyx-adls-key")
    storage_queue_url = dbutils.secrets.get(scope=secretsScope, key="onyx-adls-queue-id")
    dls_account_name = dbutils.secrets.get(scope=secretsScope, key="onyx-adls-name")
    print("Creating queue client: ")
    queue_client = QueueClient.from_queue_url(queue_url=storage_queue_url, credential=storage_key)

    # Get Approx msgs count in queue
    properties = queue_client.get_queue_properties()
    no_of_msg_in_queue = properties.approximate_message_count
    print("Message count: " + str(no_of_msg_in_queue))
    current_user = get_current_user()
    container_name,table_name = ('audit', 'source_ingestion_log')
    storage_path = f"{get_external_location_url(container_name)}/{table_name}"
    queue_dict = {}
    while no_of_msg_in_queue > 0:
        response = queue_client.receive_messages()
        print(f'Adding records to source ingestion log table for etl id : {etl_id}')
        for message in response:
            try:
                event = message.content
                res = json.loads(event)  # Transform to dictionary

                if res['metadata'] == "Error: Unable to parse JSON metadata":
                    # We'll use the etl_start_date_time for the logging
                    res['metadata'] = {"errors": [res['metadata']], "executionDetails": [{"status": "Failed", "start": res['etl_start_date_time'], "comment": "start time has been reset to etl_start_date_time!"}]}
                
                metadata = json.dumps(res['metadata'])

                # filter the messages assocaited with the pipeline run id which
                # got triggered
                is_raw_zone = False
                if "Raw" == res['zone']:
                    is_raw_zone = True
                    zone_start_date_time = get_raw_zone_start_time(res)
                elif "Prepared" == res['zone']:
                    zone_start_date_time = get_prepared_zone_start_time(res)
                # casting values to string to avoid type inference issue with NULL values when creating the dataframe
                row_values = [(
                    str(res['product_name']),
                    str(res['run_id']),
                    str(res['feed_name']),
                    str(res['dataset_name']),
                    str(res['source_system']),
                    metadata,
                    str(zone_start_date_time),
                    str(res['zone_end_date_time']),
                    str(current_user),
                    str(current_user)
                )]
                df_log_append = spark.createDataFrame(
                    row_values, [
                        'product_name', 'run_id', 'feed_name', 'dataset_name', 'source_system', 'metadata', 'start_time','end_time', 'inserted_by', 'modified_by'])
                df_log_append = (df_log_append.withColumn('start_time',col('start_time').cast('timestamp'))
                                                .withColumn('end_time',col('end_time').cast('timestamp'))
                                                .withColumn('zone',lit(res['zone'])))
                if is_raw_zone:
                    df_log_append = add_raw_metadata_columns(res, df_log_append)
                else:
                    df_log_append = add_prep_metadata_columns(res, df_log_append)
                    
                if zone_start_date_time: #temp change
                    delta_operation_handler(
                        f=lambda: df_log_append.write.format('delta').mode("append").partitionBy(
                            "product_name",
                            "feed_name",
                            "source_system",
                            "dataset_name").save(storage_path),
                        retry_count=5
                    )
                # process etl log
                if res['etl_id'] not in queue_dict:
                    queue_dict[res['etl_id']] = {res['zone']: []}
                elif res['zone'] not in queue_dict[res['etl_id']]:
                    queue_dict[res['etl_id']][res['zone']] = []
                queue_dict[res['etl_id']][res['zone']].append(res)
                queue_dict[res['etl_id']][res['zone']].sort(
                    key=lambda x: x['zone_end_date_time'], reverse=True)
                queue_client.delete_message(message)
            except Exception as e:
                print(f"Exception occured while processing the message for etl id: {etl_id} ; and queue message Id: {message.id}.")
                print(f"Please check the message and clear if necessary then rerun this notebook to process the event messages. Exception type: {type(e)}; Exception Message: {e}")
                raise e
        print(f'Adding records to etl log table for etl id: {etl_id}')
        properties = queue_client.get_queue_properties()
        no_of_msg_in_queue = properties.approximate_message_count
        insert_etl_zone_log(queue_dict)

# COMMAND ----------

# DBTITLE 1,Initiate logging
if cloud_provider == 'Azure':
    try:
        get_msg_from_queue_and_add_entry()
    except Exception as error:
        raise error

elif cloud_provider == 'AWS':
    try:
        get_msg_from_sqs_and_add_entry()
    except Exception as error:
        raise error
else:
    print(f"DOF does not support {cloud_provider} cloud ")
