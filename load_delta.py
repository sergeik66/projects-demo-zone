from delta.tables import DeltaTable
from onyx.common.lib.table_utils import get_table_reference, check_table_exists
from onyx.ingestion.lib.prep.utils import utility_functions
from onyx.common.lib.table_utils import get_external_location_url
from onyx.ingestion.lib.utility import generate_hash_udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, array, current_timestamp, udf
import os


class LoadDelta:

    def __init__(
            self,
            spark_instance,
            input_df,
            metadata_config_dict,
            feed_name,
            schema_name,
            secret_scope,
            etl_id,
            etl_start_date_time,
            transform_instance,
            callable_methods,
            source_config_folder_name,
            dbutils):
        self.metadata_config_dict = metadata_config_dict
        self.spark = spark_instance
        self.prep_properties = metadata_config_dict["prepProperties"]
        self.dataset_name = metadata_config_dict["datasetName"]
        self.table_name = utility_functions.get_table_name(self.metadata_config_dict)
        self.base_path = f"{get_external_location_url('prepared')}/{self.metadata_config_dict['sourceSystemProperties']['sourceSystemName']}"
        self.bronze_path = f"{self.base_path}/{self.table_name}"
        self.bronze_checkpoint_path = self.bronze_path + "/_checkpoint"
        self.target_file_format = metadata_config_dict["prepProperties"]["targetFileFormat"]
        self.target_load_type = metadata_config_dict["prepProperties"]["targetLoadType"]
        self.trigger_type = metadata_config_dict["prepProperties"]["triggerType"]
        self.target_file_format = self.prep_properties["targetFileFormat"]
        self.catalog_name = metadata_config_dict["prepProperties"].get("catalogName", os.getenv("ONYX_ENV_PRODUCT_CATALOG"))
        self.schema_name = schema_name
        self.table_identifier = get_table_reference(self.catalog_name, self.schema_name, self.table_name)
        self.secret_scope = secret_scope
        self.input_df = input_df
        self.etl_id = etl_id
        self.etl_start_date_time = etl_start_date_time
        self.feed_name = feed_name
        self.transform_instance = transform_instance
        self.callable_methods = callable_methods
        self.source_config_folder_name = source_config_folder_name
        self.load_options = {
            "path": self.bronze_path,
            "checkpointLocation": self.bronze_checkpoint_path
        }
        if self.target_load_type == 'overwrite':
            self.load_options["overwriteSchema"] = "true"
        else:
            self.load_options["mergeSchema"] = "true"
        self.dbutils = dbutils

    def perform_prerequisites(self):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.schema_name}")
        table_exists = check_table_exists(self.spark, self.table_identifier)

        if table_exists is False:
            self.target_load_type = "overwrite"

    def get_trigger_option(self):
        """
        :return: trigger load option
        """
        trigger_dict = {
            # FIXME RETCC-5467: attempted fix, did not work. Leave in place as the docs at least
            # _say_ Trigger.Once guarantees one micro-batch, whereas Trigger.AvailableNow() can
            # produce n
            # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers
            "availableNow": {"availableNow": True},
            "once": {"once": True},
            "batch": {"once": True},  # TODO leaving this for backward compatibility. To be removed in next release.
            "continuous": {"processingTime": "5 seconds"}
        }
        return trigger_dict[self.trigger_type]

    def perform_post_checks(self):
        save_path = f"{self.base_path}/{self.table_name}"
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.table_identifier} USING DELTA LOCATION '{save_path}'")

        if ((self.metadata_config_dict["datasetTypeName"] == 'database')
                and (self.metadata_config_dict["sourceSystemProperties"]["ingestType"] == 'watermark')
                and (self.metadata_config_dict["sourceSystemProperties"]["isDynamicQuery"])):
            self.update_watermark_query()

        if ((self.metadata_config_dict["datasetTypeName"] == 'database')
                and (self.metadata_config_dict["sourceSystemProperties"]["ingestType"] == 'cdc')
                and (self.metadata_config_dict["sourceSystemProperties"]["isDynamicQuery"])):
            self.update_cdc_query()

    def update_cdc_query(self):
        source_watermark_identifier = \
            self.metadata_config_dict["sourceSystemProperties"]["sourceEntityWatermarkIdentifier"]

        watermark_identifier_row = \
            (self.spark.sql(
                f'select max(start_lsn_timestamp) as maxvalue from {self.table_identifier}')
             .first())

        if watermark_identifier_row is None:
            print(f"No records exist in {self.table_identifier}. Skipping cdc watermark query file update.")
            return

        source_watermark_identifier_value = watermark_identifier_row['maxvalue']

        if source_watermark_identifier_value is None:
            print(f"No cdc watermark value found in {self.table_identifier}. Skipping cdc watermark query file update.")
            return

        primary_key_list = \
            (",".join(self.metadata_config_dict["prepProperties"]["primaryKeyList"])
             if "primaryKeyList" in self.metadata_config_dict["prepProperties"].keys()
             else "*")

        column_list = \
            (",".join(self.metadata_config_dict["sourceSystemProperties"]["includeSpecificColumns"])
             if "includeSpecificColumns" in self.metadata_config_dict["sourceSystemProperties"].keys()
             else "*")

        filter_expression = \
            ((" " + self.metadata_config_dict["sourceSystemProperties"]["filterExpression"])
             if "filterExpression" in self.metadata_config_dict["sourceSystemProperties"].keys()
             else "")

        dataset_schema = self.metadata_config_dict["datasetSchema"]
        operation_column = self.metadata_config_dict['sourceSystemProperties']['operationColumn']
        query_extract_json = {
            "query": f"WITH CDC AS (SELECT ROW_NUMBER() OVER(PARTITION BY {primary_key_list} ORDER BY {source_watermark_identifier} DESC ) AS Row_Num, {column_list}  "
                     f"FROM cdc.{dataset_schema}_{self.dataset_name}_CT where {operation_column} <> 3 ) "
                     f" SELECT * FROM CDC "
                     f" WHERE Row_Num = 1 and {source_watermark_identifier} >  sys.fn_cdc_map_time_to_lsn('smallest greater than',CAST('{source_watermark_identifier_value}' as datetime2)) {filter_expression}",
            "sourceEntityWatermarkIdentifier": f"{source_watermark_identifier}",
            "watermarkValue": f"{source_watermark_identifier_value}"}

        file_location = f"{get_external_location_url('metadata')}/datasets/{self.source_config_folder_name}/watermark/{dataset_schema}_{self.dataset_name}.json"
        utility_functions.create_json_file(dbutils=self.dbutils, file_location=file_location,
                                           json_data=query_extract_json, replace_file=True
                                           )

    def update_watermark_query(self):
        source_watermark_identifier = \
            self.metadata_config_dict["sourceSystemProperties"]["sourceEntityWatermarkIdentifier"]

        watermark_identifier_row = \
            (self.spark.sql(
                f'select max({source_watermark_identifier}) as maxvalue from {self.table_identifier}')
             .first())

        if watermark_identifier_row is None:
            print(f"No records exist in {self.table_identifier}. Skipping watermark query file update.")
            return

        source_watermark_identifier_value = watermark_identifier_row['maxvalue']

        if source_watermark_identifier_value is None:
            print(f"No watermark value found in {self.table_identifier}. Skipping watermark query file update.")
            return

        column_list = \
            (",".join(self.metadata_config_dict["sourceSystemProperties"]["includeSpecificColumns"])
             if "includeSpecificColumns" in self.metadata_config_dict["sourceSystemProperties"].keys()
             else "*")

        filter_expression = \
            ((" " + self.metadata_config_dict["sourceSystemProperties"]["filterExpression"])
             if "filterExpression" in self.metadata_config_dict["sourceSystemProperties"].keys()
             else "")

        dataset_schema = self.metadata_config_dict["datasetSchema"]

        query_extract_json = {
            "query": f"SELECT {column_list} FROM {dataset_schema}.{self.dataset_name} "
                     f"WHERE {source_watermark_identifier} > CAST('{source_watermark_identifier_value}' AS "
                     f"datetime2){filter_expression}",
            "sourceEntityWatermarkIdentifier": f"{source_watermark_identifier}",
            "watermarkValue": f"{source_watermark_identifier_value}"}

        file_location = f"{get_external_location_url('metadata')}/datasets/{self.source_config_folder_name}/watermark/{dataset_schema}_{self.dataset_name}.json"
        utility_functions.create_json_file(dbutils=self.dbutils, file_location=file_location,
                                           json_data=query_extract_json, replace_file=True
                                           )

    def perform_load_operation(self):
        """
        This method performs the load operation for the landing zone
        """

        trigger_options = self.get_trigger_option()
        self.perform_prerequisites()

        write_method = (lambda source_df: source_df.writeStream
                        .format(self.target_file_format)
                        .outputMode("append")
                        .queryName(f"{self.table_name}_prep")
                        .trigger(**trigger_options)
                        .options(**self.load_options)
                        )

        write_methods_dict = {
            'overwrite': (write_method(self.input_df)
                          .foreachBatch(self.perform_overwrite_operation())),
            'append_all': (write_method(self.input_df)
                           .foreachBatch(self.perform_append_all_operation())),
            'append_new': (write_method(self.input_df)
                           .foreachBatch(self.perform_compare_append())),
            'merge': (write_method(self.input_df)
                      .foreachBatch(self.perform_merge_operation())),
            'scd2': (write_method(self.input_df)
                     .foreachBatch(self.perform_scd2_operation()))
        }
        write_methods_dict[self.target_load_type].start().awaitTermination()

        self.perform_post_checks()

    def perform_write_operation_for_ffl(self, write_mode):
        transformed_df = self.execute_callable_methods(self.input_df)
        (transformed_df.write
            .format(self.target_file_format)
            .mode(write_mode)
            .save(self.bronze_path))

    def perform_compare_append_for_ffl(self):
        transformed_df = self.execute_callable_methods(self.input_df)
        target_table_delta = DeltaTable.forName(
            self.spark, self.table_identifier)
        (target_table_delta.alias("target").merge(transformed_df.alias("source"),
                                                  utility_functions.build_pk_join_string(
                                                      self.metadata_config_dict['prepProperties'][
                                                          'primaryKeyList'])).whenNotMatchedInsertAll().execute())

    def perform_merge_operation_for_ffl(self):
        transformed_df = self.execute_callable_methods(self.input_df)
        primary_key_list = self.metadata_config_dict["prepProperties"]["primaryKeyList"]
        merge_keys_list = utility_functions.build_pk_join_string(primary_key_list)
        target_table_delta = DeltaTable.forName(self.spark, self.table_identifier)
        target_table_df = self.spark.table(self.table_identifier)
        prep_merge_column_list_string = utility_functions.build_prep_merge_column_list_string(
            secret_scope=self.secret_scope,
            dataset_config_json=self.metadata_config_dict,
            target_table_df=target_table_df,
            prep_new_data_df=transformed_df)
        (target_table_delta.alias("target")
            .merge(transformed_df.alias("source"), merge_keys_list)
            .whenMatchedUpdateAll(prep_merge_column_list_string)
            .whenNotMatchedInsertAll()
            .execute())

    def perform_load_operation_for_ffl(self):
        """
        This method performs the FFL load operation for the landing zone
        """
        # FIXME You may notice this is similar to LoadDelta._perform_merge_operation. The initial port of the FFL
        #  is intended to be just that, a port, but this can/should be integrated into LoadDelta in future iterations
        self.perform_prerequisites()
        if self.target_load_type == 'overwrite':
            self.perform_write_operation_for_ffl('overwrite')
        elif self.target_load_type == 'append_all':
            self.perform_write_operation_for_ffl('append')
        elif self.target_load_type == 'append_new':
            self.perform_compare_append_for_ffl()
        elif self.target_load_type == 'merge':
            self.perform_merge_operation_for_ffl()
        elif self.target_load_type == 'scd2':
            raise Exception("SCD2 is not supported for the FFL")
        else:
            self.perform_write_operation_for_ffl('append')
        self.perform_post_checks()

    def execute_callable_methods(self, batch_df):
        if self.callable_methods:
            for method in self.callable_methods:
                batch_df = getattr(
                    self.transform_instance, method)(
                    batch_df, self.target_load_type)
        return batch_df

    def perform_compare_append(self):
        def _compare_append(batch_df, batch_id):
            batch_df = self.execute_callable_methods(batch_df)
            target_table_delta = DeltaTable.forName(
                self.spark, self.table_identifier)
            (target_table_delta.alias("target").merge(batch_df.alias("source"), utility_functions.build_pk_join_string(
                self.metadata_config_dict['prepProperties']['primaryKeyList'])).whenNotMatchedInsertAll().execute())

        return _compare_append

    def perform_merge_operation(self):
        def _perform_merge_operation(batch_df, batch_id):
            transformed_df = self.execute_callable_methods(batch_df)
            primary_key_list = self.metadata_config_dict["prepProperties"]["primaryKeyList"]
            merge_keys_list = utility_functions.build_pk_join_string(
                primary_key_list)
            target_table_delta = DeltaTable.forName(
                self.spark, self.table_identifier)
            target_table_df = self.spark.table(self.table_identifier)
            prep_merge_column_list_string = utility_functions.build_prep_merge_column_list_string(
                self.secret_scope,
                self.metadata_config_dict,
                target_table_df,
                transformed_df)
            (target_table_delta.alias("target")
             .merge(transformed_df.alias("source"), merge_keys_list)
             .whenMatchedUpdateAll(prep_merge_column_list_string)
             .whenNotMatchedInsertAll()
             .execute()
             )

        return _perform_merge_operation

    def perform_scd2_operation(self):
        def _perform_scd2_operation(batch_df, batch_id):
            transformed_df = self.execute_callable_methods(batch_df)
            primary_key_list = self.metadata_config_dict["prepProperties"]["primaryKeyList"]
            metadata_cols = ['LoadDate', 'Active_Start_Datetime', 'Active_End_Datetime', 'Is_Current', 'LoadDatetime', 'ETL_ID', 'ETL_Start_Date_Time', 'DQ_Status', 'Hash_Diff', '_rescued_data']
            # add scd2 columns for existing tables
            utility_functions.add_scd2_columns(self.spark.table(self.table_identifier), self.bronze_path, metadata_cols, primary_key_list)
            # schema evolution- adding new columns to the table
            utility_functions.add_new_columns(self.spark, transformed_df, self.spark.table(self.table_identifier), self.table_identifier)

            target_table_delta = DeltaTable.forName(self.spark, self.table_identifier)
            target_table_df = self.spark.table(self.table_identifier).where('Is_Current == True')
            target_table_df, transformed_df = utility_functions.column_sync_up(target_table_df, transformed_df)
            source_cols = transformed_df.columns
            final_cols = list(set(source_cols) - set(metadata_cols + primary_key_list))
            hash_cols = list(map(lambda x: col(x).cast("string"), final_cols))
            udfSha256Python_DF = udf(generate_hash_udf.udfSha256Python, StringType())
            transformed_df = transformed_df.withColumn('Hash_Diff', udfSha256Python_DF(array(*hash_cols)))
            join_condition = [col(f'a.{key}') == col(f'b.{key}') for key in primary_key_list]
            updates_df = transformed_df.alias('a').join(target_table_df.alias('b'), [*join_condition, col('a.Hash_Diff') == col('b.Hash_Diff')], 'anti')
            latest_df = transformed_df.alias('a').join(target_table_df.alias('b'), [*join_condition, col('a.Hash_Diff') != col('b.Hash_Diff')]).selectExpr('a.*')
            final_df = utility_functions.build_scd2_mergekey_columns(updates_df, latest_df, primary_key_list)
            merge_condition = " AND ".join([f"source.mergekey{str(index+1)} = target.{key}" for index, key in enumerate(primary_key_list)])
            insert_expr = {f"target.{i}": f"source.{i}" for i in transformed_df.drop('_rescued_data').columns}

            (target_table_delta.alias('target')
             .merge(final_df.alias('source'), merge_condition)
             .whenMatchedUpdate(condition="target.Hash_Diff <> source.Hash_Diff and target.is_current = True",
                                set={"target.active_end_datetime": current_timestamp(), "target.is_current": lit(False)}
                                )
             .whenNotMatchedInsert(values=insert_expr)
             .execute())
        return _perform_scd2_operation

    def perform_overwrite_operation(self):
        def _perform_overwrite_operation(batch_df, batch_id):
            batch_df = self.execute_callable_methods(batch_df)
            if self.transform_instance.input_row_count > 0:
                (batch_df.write
                 .format("delta")
                 .mode("overwrite")
                 .save(self.bronze_path)
                 )
            else:
                print('No rows to process to Prep table:', self.table_identifier)
        return _perform_overwrite_operation

    def perform_append_all_operation(self):
        def _perform_append_all_operation(batch_df, batch_id):
            batch_df = self.execute_callable_methods(batch_df)
            (batch_df.write
             .format("delta")
             .mode("append")
             .save(self.bronze_path)
             )

        return _perform_append_all_operation
