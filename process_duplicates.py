from pyspark.sql import DataFrame, Window
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit, current_date, current_timestamp, col, count, row_number
from pyspark.sql.types import StructField, StructType, TimestampType, DateType, StringType, BooleanType, IntegerType
from typing import List
from onyx.common.lib import email_util
from onyx.common.lib import secrets
from onyx.common.lib.table_utils import get_table_reference, check_table_exists
from onyx.common.lib.table_utils import get_external_location_url


def create_quarantine_table(spark: SparkSession,
                            source_df: DataFrame,
                            catalog_name: str,
                            schema_name: str,
                            table_name: str,
                            root_path: str,
                            exclude_cols: list):
    """
    This function creates a quarantine table with the schema of the incoming dataframe in the respective schema.

    Args:
        spark (SparkSession): A SparkSession to be used for processing.
        source_df (DataFrame): Source DataFrame which will be checked for duplicates.
        catalog_name (str): Name of the catalog for the target table.
        schema_name (str): Name of the schema for the target table.
        table_name (str): Name of the target table.
        root_path (str): Quarantine Table root path.
        exclude_cols (list): List of columns to exclude in Quarantine Table.
    """
    table_identifier = get_table_reference(catalog_name, schema_name, table_name + '_quarantined')
    if check_table_exists(spark, table_identifier) is False:
        table_path = root_path + '/' + table_name + '_quarantined'
        fieldsList = source_df.drop(*exclude_cols).schema.fields
        if schema_name.split('_')[0] == 'ext':
            extraFields = [
                StructField("LoadDatetime", TimestampType(), False),
                StructField("LoadDate", DateType(), False),
                StructField("ETL_ID", StringType(), False),
                StructField("source_system", StringType(), False),
                StructField("Is_Deleted", BooleanType(), False),
                StructField("ETL_Start_Date_Time", TimestampType(), False),
                StructField("DQ_Status", StringType(), False),
                StructField("Quarantined_Date_Time", TimestampType(), False),
                StructField("Log_Status", StringType(), False)
            ]
        else:
            extraFields = [
                StructField("Quarantined_Date_Time", TimestampType(), False),
                StructField("ETL_ID", StringType(), False),
                StructField("Log_Status", StringType(), False)
            ]
        entitySchema = StructType(fieldsList + extraFields)
        entityEmpty = spark.createDataFrame([], entitySchema)
        entityEmpty.write.format('delta').mode('Append').option('mergeSchema', True).save(table_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_identifier} USING DELTA LOCATION '{table_path}'")
        print(f'--- {table_identifier} table created successfully ---')


def create_quarantine_log_table(spark: SparkSession, catalog_name: str):
    """
    Creates the quarantine log table if it does not exist.

    Args:
        spark (SparkSession): The SparkSession object.
        catalog_name (str): Name of the Databricks catalog
    """

    schema_name = 'audit'
    table_name = 'quarantine_log'
    table_path = f"{get_external_location_url('audit')}/quarantine/quarantine_log"
    table_identifier = get_table_reference(catalog_name, schema_name, table_name)

    query = f"""
        CREATE TABLE IF NOT EXISTS {table_identifier}
        (
            product_name STRING,
            feed_name STRING,
            source_system STRING,
            dataset_name STRING,
            etl_id STRING,
            total_rows INT,
            quarantine_type STRING,
            quarantine_level STRING,
            log_date DATE,
            log_datetime TIMESTAMP,
            primary_key_list STRING,
            is_notification_enabled BOOLEAN,
            notification_datetime TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (product_name, feed_name, source_system, dataset_name)
        LOCATION '{table_path}'
        TBLPROPERTIES(delta.autoOptimize.optimizeWrite = true)
    """

    if check_table_exists(spark, table_identifier) is False:
        spark.sql(query)
        print(f'Created Table: {table_identifier}, Location : {table_path} ')
    else:
        print(f'Table {table_identifier} already exists, Location : {table_path} ')


def remove_duplicates(input_df: DataFrame, pk_list: List[str], quarantine_type: str = 'quarantine_all_duplicates') -> (DataFrame, DataFrame):
    """
    This function processes duplicate records in the incoming dataframe based on the specified quarantine type.
    It either removes all instances of duplicates ('quarantine_all_duplicates') or keeps one instance of each
    duplicate record while removing the rest ('retain_one_duplicate').

    Args:
        input_df (DataFrame): The incoming DataFrame.
        pk_list (list): The primary key columns list.
        quarantine_type (str): The quarantine type. Can be either 'quarantine_all_duplicates'
                               or 'retain_one_duplicate'.

    Returns:
        DataFrame: The DataFrame with appropriate duplicates removed based on the quarantine type.
        DataFrame: The DataFrame containing the duplicates that were identified and processed.
    """
    partition_fields = Window.partitionBy(*pk_list).orderBy(*pk_list)

    if quarantine_type == 'quarantine_all_duplicates':
        # Remove all instances of duplicates
        counted_df = input_df.withColumn('count', count('*').over(partition_fields))
        non_duplicate_df = counted_df.filter(col('count') == 1).drop('count')
        duplicate_df = counted_df.filter(col('count') > 1).drop('count')
    elif quarantine_type == 'retain_one_duplicate':
        # Remove extra instances of duplicates, keep one instance of each
        numbered_df = input_df.withColumn('row_number', row_number().over(partition_fields))
        non_duplicate_df = numbered_df.filter(col('row_number') == 1).drop('row_number')
        duplicate_df = numbered_df.filter(col('row_number') > 1).drop('row_number')
    else:
        raise ValueError(f"Invalid quarantine type: {quarantine_type}. Expected 'quarantine_all_duplicates' or 'retain_one_duplicate'.")

    return non_duplicate_df, duplicate_df


def quarantine_duplicates(duplicate_df: DataFrame,
                          schema_name: str,
                          table_name: str,
                          root_path: str,
                          etl_id: str,
                          source_system: str,
                          etl_start_date_time: str):
    """
    This function quarantines the duplicate records from the incoming dataframe and loads any duplicate records into its corresponding quarantine table.

    Args:
        duplicate_df (DataFrame): The DataFrame with duplicate rows.
        schema_name (str): The schema name.
        table_name (str): The table name.
        root_path (str): The root path.
        etl_id (str): The ETL ID.
        source_system (str): The source system.
        etl_start_date_time (str): The ETL start date time.

    Returns:
        None
    """

    table_path = root_path + '/' + table_name + '_quarantined'
    final_df = (
        duplicate_df.withColumn("Quarantined_Date_Time", current_timestamp())
        .withColumn("ETL_ID", lit(str(etl_id)))
        .withColumn("Log_Status", lit('Quarantined'))
    )
    # Check if the first part of db_name is 'ext'
    if schema_name.split('_')[0] == 'ext':
        final_df = final_df.withColumn('source_system', lit(source_system))
        final_df = final_df.withColumn("ETL_Start_Date_Time",
                                       lit(etl_start_date_time).cast('timestamp'))

    final_df.write.format('delta').mode('Append').option('mergeSchema', True).save(table_path)

    print(f'--- Quarantined duplicate rows to {table_name}_quarantined table successfully ---')


def update_quarantine_log(spark: SparkSession,
                          final_df: DataFrame,
                          product_name: str,
                          feed_name: str,
                          source_system: str,
                          table_name: str,
                          etl_id: str,
                          pk_list: list):
    """
    This function logs an entry into the quarantine log table with the summary of the quarantined duplicate records.

    Args:
        spark (SparkSession): The SparkSession object.
        final_df (DataFrame): The DataFrame with duplicate rows.
        product_name (str): The product name.
        feed_name (str): The feed name.
        source_system (str): The source system.
        table_name (str): The table name.
        etl_id (str): The ETL ID.
        pk_list (list): The primary key columns list.

    Returns:
        None
    """

    duplicate_cnt = final_df.count()

    schema = StructType([
        StructField('product_name', StringType(), True),
        StructField('feed_name', StringType(), True),
        StructField('source_system', StringType(), True),
        StructField('dataset_name', StringType(), True),
        StructField('etl_id', StringType(), True),
        StructField('total_rows', IntegerType(), True),
        StructField('quarantine_type', StringType(), True),
        StructField('quarantine_level', StringType(), True),
        StructField('primary_key_list', StringType(), True)
    ])

    log_df = spark.createDataFrame([
        (
            product_name,
            feed_name,
            source_system,
            table_name,
            etl_id,
            duplicate_cnt,
            'Duplicate',
            'Standard',
            '~'.join(pk_list)
        )], schema)

    log_final_df = (log_df
                    .withColumn('log_date', current_date())
                    .withColumn('log_datetime', current_timestamp())
                    .withColumn('is_notification_enabled', lit(False))  # TODO: Add this as part of RETCC-7227
                    .withColumn('notification_datetime', lit(None).cast('timestamp')))  # TODO: Add this as part of RETCC-7227

    table_path = f"{get_external_location_url('audit')}/quarantine/quarantine_log"
    log_final_df.write.format('delta').mode('append').save(table_path)

    print(f'--- Found {duplicate_cnt} duplicate rows,quarantined log table is updated ---')


def check_duplicates(spark: SparkSession,
                     input_df: DataFrame,
                     catalog_name: str,
                     schema_name: str,
                     table_name: str,
                     root_path: str,
                     pk_list: List[str],
                     etl_id: str,
                     source_system: str,
                     feed_name: str,
                     etl_start_date_time: str,
                     product_name: str,
                     quarantine_type: str,
                     tclist: List[str] = [],
                     qclist: List[str] = []):

    """
    Check for duplicates and quarantine them

    This function checks for duplicate records in the incoming dataframe based on the specified quarantine type,
    and processes them accordingly. It can either quarantine all instances of duplicates or retain one
    instance of each duplicate record. The duplicate records are quarantined in a separate table, and a log entry
    is made in the quarantine log table. The de-duplicated dataset is returned.

    Args:
        spark (SparkSession): The SparkSession object.
        input_df (DataFrame): The incoming DataFrame.
        catalog_name (str): The catalog name
        schema_name (str): The schema name.
        table_name (str): The entity name.
        root_path (str): The root path.
        pk_list (list): The primary key columns list.
        etl_id (str): The ETL ID.
        source_system (str): The source system.
        feed_name (str): The feed name.
        etl_start_date_time (str): The ETL start date time.
        product_name (str): The product name.
        quarantine_type (str): The quarantine type. Can be either 'quarantine_all_duplicates'
                               or 'retain_one_duplicate'.
        tclist (list): The list of columns to be excluded in quarantine table.
        qclist (list): The list of columns to be excluded in source DataFrame.

    Returns:
        DataFrame: The de-duplicated DataFrame.
    """

    # Remove Duplicate records from the incoming dataframe
    final_df, duplicate_df = remove_duplicates(input_df, pk_list, quarantine_type)

    # Check for duplicate records
    if duplicate_df.first():
        # Exclude the columns from the quarantine table
        duplicate_df = duplicate_df.drop(*qclist)
        create_quarantine_table(spark, final_df, catalog_name, schema_name, table_name, root_path, tclist)
        quarantine_duplicates(duplicate_df, schema_name, table_name, root_path, etl_id, source_system, etl_start_date_time)
        update_quarantine_log(spark, duplicate_df, product_name, feed_name, source_system, table_name, etl_id, pk_list)

    else:
        print('--- No duplicates found in the source ---')

    return final_df


def create_unified_view(spark: SparkSession,
                        catalog_name: str,
                        schema_name: str,
                        table_name: str):
    """
    This function creates a unified view of the quarantined and non-quarantined data.

    Args:
        spark (SparkSession): A SparkSession to be used for processing.
        catalog_name (str): Name of the catalog used in both table and view reference
        schema_name (str): Name of the schema used in both table and view reference
        table_name (str): Name of the target table used in both table and view reference
    """
    table_identifier = get_table_reference(catalog_name, schema_name, table_name)
    view_identifier = get_table_reference(catalog_name, schema_name, "vw_" + table_name + "_combined")
    table_exists = check_table_exists(spark, table_identifier)
    quarantined_table_exists = check_table_exists(spark, f"{table_identifier}_quarantined")

    if table_exists and quarantined_table_exists:
        (spark.sql(f"""CREATE OR REPLACE VIEW {view_identifier}
      AS
      (select *, null as quarantined_date_time, null as log_status FROM {table_identifier}
       union all
       select * from {table_identifier}_quarantined)
      """)
         )
        print(f'--- {view_identifier} view created successfully ---')


def data_deduplication_options(spark):
    """
    :param spark:   spark instance
    """
    value_df = spark.sql(
        "SELECT Value FROM v_datahub_master_configuration WHERE Config_Name ='ENABLE_DATA_DEDUPLICATION_ALERTING'")
    return value_df.first()['Value']


def data_deduplication_alert(spark,
                             dbutils,
                             key_vault_scope,
                             environment_name,
                             etl_id):
    """
    :param spark:   spark instance
    :param dbutils:   dbutils instance
    :param key_vault_scope:   keyVaultScope of your end
    :param environment_name:   environment_name
    :param etl_id:   ETL Id
    """
    powerbi_report_link = secrets.get_powerbi_values(dbutils, key_vault_scope).get("powerbi_report_link")
    data_deduplication_enable = data_deduplication_options()
    if data_deduplication_enable:
        deduplicationdetaildf = spark.sql(
            f" SELECT table_name,quarantine_type,quarantine_level,Total_Rows as Noofduplicates,etl_id as ETLId\
        FROM audit.quarantine_log WHERE etl_id= '{etl_id}' AND Total_Rows > 0 GROUP BY table_name,quarantine_type,quarantine_level,\
        Noofduplicates,ETLId"
        )

        table_count = deduplicationdetaildf.count()
        error_details = deduplicationdetaildf.toPandas().to_html(index=False)

        if table_count > 0:
            email_body = "Duplicates Notification - Data deduplication"
            email_message = """<h2>Notification Summary</h2>

            <p>A data deduplication failure has been found.The duplicates have been quarantined in log table.Please see the following details for more information:</p>

            <p><b>Data Duplicates Details:</b></p>

            <p>Duplicates Environment : {environment_name}</p>
            <p>No. of Entities: {table_count} </p>
            <p>{error_details}</p>
            <p>Please click on the Power BI report link below for further details.</p>
            <p><a href= {powerbi_report_link}>Data Deduplication  Report</a></p>

            <p style="color:red;">*Please do not reply to this email.</p>
            <p>Thanks<br>Support Team</p>""".format(environment_name=environment_name,
                                                    table_count=table_count,
                                                    error_details=error_details,
                                                    powerbi_report_link=powerbi_report_link
                                                    )
            attachment_path = ''

            email_util.send_email(dbutils,
                                  key_vault_scope,
                                  email_body,
                                  email_message,
                                  attachment_path)
