# Databricks notebook source
from datetime import datetime
import os
from pyspark.sql.functions import lit
from delta.tables import DeltaTable
from pyspark.sql.types import StringType, TimestampType
from onyx.common.lib.error_and_retry_handlers import delta_operation_handler
from onyx.common.lib.application_logging import get_onyx_logger
from onyx.common.lib.table_utils import get_table_reference, check_table_exists
from onyx.common.lib.onyx_cloud_config import OnyxCloudConfig
from onyx.common.lib.table_utils import get_external_location_url

logger = get_onyx_logger(__name__)

# COMMAND ----------


class EtlZoneLogTableHandler:
    def __init__(self, spark):
        logger.info("Instantiating EtlZoneLogTableHandler...")
        self.spark = spark
        self.table_catalog = os.getenv('ONYX_ENV_LOGS_CATALOG')
        self.table_schema = "audit"
        self.table_name = "etl_zone_log"
        self.config = OnyxCloudConfig(spark)
        self.table_reference = get_table_reference(self.table_catalog, self.table_schema, self.table_name)
        logger.debug(f"__init__ - Values: table_catalog {self.table_catalog} ; table_schema {self.table_schema} ; table_name {self.table_name} ; table_reference {self.table_reference}")

        if check_table_exists(self.spark, self.table_reference) is False:
            self.create_etl_zone_log()
            logger.info(f"Created Table: {self.table_reference}")
        else:
            logger.info(f"Table {self.table_reference} already exists")

    def create_etl_zone_log(self):
        """
        Creates an ETL zone log table if it does not exist. If the table exists and does not have 'invocation_id' column, it adds the column.
        """
        storage_path = f"{get_external_location_url(self.table_schema)}/{self.table_name}"
        query = f"""CREATE TABLE IF NOT EXISTS {self.table_reference}
        (
          run_id STRING,
          etl_id BIGINT,
          product_name STRING,
          feed_name STRING,
          zone STRING,
          stage STRING,
          zone_start_date_time TIMESTAMP,
          zone_end_date_time TIMESTAMP,
          zone_status STRING,
          total_data_storage BIGINT,
          total_models_shared INT,
          total_retry_count INT,
          total_error_count INT,
          invocation_id STRING
        )
        USING DELTA
        PARTITIONED BY (product_name, feed_name, zone, stage)
        LOCATION '{storage_path}'
        TBLPROPERTIES(delta.autoOptimize.optimizeWrite = true)
        """
        self.spark.sql(query)

    def get_etl_zone_log_schema(self):
        """
        This function returns the schema of the audit.etl_zone_log table
        """
        return delta_operation_handler(
            lambda: self.spark.table(f"{self.table_reference}").schema
        )

    def get_pending_zone_tasks_by_etl_id(self, etl_id: int):
        """
        This function returns the etl_zone_log records for the etl_id that are not successful.

        Args:
            etl_id (int): This is a Integer parameter that represents the etl_id from the audit.etl_log table.

        Returns:
            df (dataframe): Dataframe with the record from etl_zone_log table with the etl_id.
        """
        return delta_operation_handler(
            lambda: self.spark.sql(
                f"select * from {self.table_reference} where etl_id = {etl_id} and zone_status != 'Success'"
            )
        )

    def get_existing_records_for_etl_id(self, etl_id: int):
        """
        This function returns True if there is a record for etl_id in the etl_zone_log table

        Args:
            etl_id (int): This is a Integer parameter that represents the etl_id table.

        Returns:
            df (dataframe): A dataframe with the record from etl_zone_log table with the etl_id.
        """
        return delta_operation_handler(
            lambda: self.spark.sql(
                f"select * from {self.table_reference} where etl_id = {etl_id} and !((zone  = 'Raw' and stage = 'Ingestion') or (zone = 'Prepared' and stage = 'Transformation'))"
            )
        )

    def generate_pending_etl_zone_log_entries(
        self,
        stages_in_workflow: dict,
        run_id: str,
        etl_id: int,
        product_name: str,
        feed_name: str,
        zone_status: str = "Pending",
    ):
        """
        This function generates the etl_zone_log records for the etl_id that are not successful.

        Args:
            stages_in_workflow (dict): This is a dictionary that contains the stages in the workflow.
            run_id (str): This is a string parameter that represents the run_id from the orchestration tool used that is stored in audit.etl_log.run_id column.
            etl_id (int): This is a Integer parameter that represents the etl_id from the audit.etl_log table.
            product_name (str): This is a string parameter that represents the product_name from the orchestration tool used that is stored in audit.etl_log.product_name column.
            feed_name (str): This is a string parameter that represents the feed_name from the orchestration tool used that is stored in audit.etl_log.feed_name column.
            zone_status (str): This is a string parameter that represents the zone_status from the orchestration tool used that is stored in audit.etl_zone_log.zone_status column.

        Returns:
            df (dataframe): dataframe with the record from etl_zone_log table with the etl_id.
        """
        existing_zone_log_df = self.get_existing_records_for_etl_id(etl_id)
        if existing_zone_log_df.count() == 0:
            schema = self.get_etl_zone_log_schema()
            log_rows = []
            for log_row in stages_in_workflow.values():
                new_row = (
                    run_id,
                    etl_id,
                    product_name,
                    feed_name,
                    log_row["zone"],
                    log_row["stage"],
                    datetime(1900, 1, 1),
                    None,
                    zone_status,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                log_rows.append(new_row)
            pending_etl_zone_entries = self.spark.createDataFrame(log_rows, schema)
            delta_operation_handler(
                lambda: pending_etl_zone_entries.write.mode("append").saveAsTable(
                    f"{self.table_reference}"
                )
            )
        else:
            storage_path = f"{get_external_location_url(self.table_schema)}/{self.table_name}"
            etl_zone_log_df = DeltaTable.forPath(
                self.spark, storage_path
            )
            existing_zone_log_df = (
                existing_zone_log_df.withColumn("zone_status", lit("Pending"))
                .withColumn("zone_start_date_time", lit("1900-01-01T00:00:00.000+0000"))
                .withColumn("zone_end_date_time", lit(None).cast(TimestampType()))
                .withColumn("invocation_id", lit(None).cast(StringType()))
            )
            (
                delta_operation_handler(
                    lambda: etl_zone_log_df.alias("target")
                    .merge(
                        existing_zone_log_df.alias("updates"),
                        "target.run_id <=> updates.run_id and target.etl_id <=> updates.etl_id and target.zone <=> updates.zone and target.stage <=> updates.stage",
                    )
                    .whenMatchedUpdate(
                        set={
                            "target.zone_status": "updates.zone_status",
                            "target.zone_start_date_time": "updates.zone_start_date_time",
                            "target.zone_end_date_time": "updates.zone_end_date_time",
                            "target.invocation_id": "updates.invocation_id",
                        }
                    )
                    .execute()
                )
            )

    def update_status_for_etl_zone_log(self,
                                       zone_end_date_time: datetime,
                                       status: str,
                                       zone: str,
                                       etl_id: int,
                                       total_data_storage: int,
                                       stage: str):
        """
        This function updates the etl_zone_log table with the status and zone_end_date_time for the etl_id and zone.

        Args:
            zone_end_date_time (datetime): This is a datetime parameter that represents the zone_end_date_time from the orchestration tool used that is stored in audit.etl_zone_log.zone_end_date_time column.
            status (str): This is a string parameter that represents the status from the orchestration tool used that is stored in audit.etl_zone_log.zone_status column.
            zone (str): This is a string parameter that represents the zone from the orchestration tool used that is stored in audit.etl_zone_log.zone column.
            etl_id (int): This is a Integer parameter that represents the etl_id from the audit.etl_log table.
            total_data_storage (int): This is a Integer parameter that represents the total_data_storage from the orchestration tool used that is stored in audit.etl_zone_log.total_data_storage column.
            stage (str): This is a string parameter that represents the stage from the orchestration tool used that is stored in audit.etl_zone_log.stage column.
        """
        query = f"""update {self.table_reference} set
                        zone_end_date_time = '{zone_end_date_time}',
                        zone_status = '{status}',
                        total_data_storage = '{total_data_storage}'
                        where zone = '{zone}' and
                        etl_id = {etl_id} and stage = '{stage}'"""
        delta_operation_handler(lambda: self.spark.sql(query))

    def update_invocation_id_for_etl_zone_log(
            self, zone: str, stage: str, invocation_id: str, etl_id: str = None
    ):
        """
        This function updates the etl_zone_log table's 'invocation_id' column.

        By matching on values in the columns 'zone', 'stage' and 'etl_id'

        Args:
            zone (str): The 'zone' value to match on.
            stage (str): The 'stage' value to match on.
            invocation_id (str): The 'invocation_id' value to set.
            etl_id (str, optional): The 'etl_id' value to match on. Defaults to os.getenv("ONYX_ETL_ID"), or None.

        Raises:
            Exception (ValueError): No 'etl_id' value can be found.
        """
        etl_id = etl_id or int(os.getenv("ONYX_ETL_ID"))
        if not etl_id:
            logger.error("A value for the ETL ID was not passed as an argument via 'etl_id', or found in env var 'ONYX_ETL_ID'")
            raise ValueError("A value for the ETL ID was not passed as an argument via 'etl_id', or found in env var 'ONYX_ETL_ID'")
        logger.debug(f"Updating table {self.table_reference} invocation_id to {invocation_id}.")
        logger.debug(f"Targeting row for zone: {zone}, stage: {stage}, etl_id: {etl_id}.")

        query = f"""
            UPDATE {self.table_reference}
            SET invocation_id = '{invocation_id}'
            WHERE
                etl_id = {etl_id}
                AND LOWER(zone) = LOWER('{zone}')
                AND LOWER(stage) = LOWER('{stage}');
        """
        logger.debug(f"Issusing SQL: {query}")
        delta_operation_handler(lambda: self.spark.sql(query))

    def merge_updated_records(self, status_changed_records: object, product_name: str, feed_name: str):
        """
        This function updates the etl_zone_log table with the status and zone_end_date_time for the etl_id and zone.

        Args:
            status_changed_records (object): This is a dataframe that contains the status and zone_end_date_time for the etl_id and zone.
            product_name (str): This is a string parameter that represents the product_name from the orchestration tool used that is stored in audit.etl_log.product_name column.
            feed_name (str): This is a string parameter that represents the feed_name from the orchestration tool used that is stored in audit.etl_log.feed_name column.
        """
        storage_path = f"{get_external_location_url(self.table_schema)}/{self.table_name}"
        etl_zone_log_df = DeltaTable.forPath(
            self.spark, storage_path
        )
        (
            delta_operation_handler(
                lambda: etl_zone_log_df.alias("target")
                .merge(
                    status_changed_records.alias("updates"),
                    f"""target.run_id <=> updates.run_id and target.etl_id <=> updates.etl_id
                and target.zone <=> updates.zone and target.stage <=> updates.stage
                and target.product_name = '{product_name}' and target.feed_name = '{feed_name}'""",
                )
                .whenMatchedUpdate(
                    set={
                        "target.zone_status": "updates.current_zone_status",
                        "target.zone_start_date_time": "updates.current_start_time",
                        "target.zone_end_date_time": "updates.current_end_time",
                    }
                )
                .execute()
            )
        )
