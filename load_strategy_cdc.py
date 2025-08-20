from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window
import json
from datetime import datetime, timezone
from typing import List, Optional

from spark_engine.ingestion.metadata import Metadata
from spark_engine.common.lakehouse import LakehouseManager, SchemaManager
from spark_engine.ingestion.strategy import LoadTypes
from spark_engine.common.file_handler import FileHandler
from spark_engine.ingestion.metrics import IngestionMetrics, GroupMetrics
from spark_engine.data_quality.quarantine import QuarantineVersion, QuarantineLog
from spark_engine.data_quality.data_quality import Expectations
import notebookutils

class Ingest:
    """
    The ingest class which takes the metadata config and processes a single table to the lakehouse, with CDC support for inserts, deletes, and updates.
    """

    schema_manager = SchemaManager()

    METRICS: IngestionMetrics

    def __init__(self, ingest_config: str) -> None:
        config = ingest_config if isinstance(ingest_config, dict) else json.loads(ingest_config)
        self.batch_time = datetime.now(timezone.utc).isoformat()
        self.load_type = config["curatedProperties"]["targetLoadType"]
        self.source_format = config["rawProperties"]["fileType"].lower()
        self.source_options = config["curatedProperties"].get("fileOptions")
        self.source_system = config["sourceSystemProperties"]["sourceSystemName"]
        self.target_table = config["datasetName"]
        self.source_table_schema = config.get("datasetSchema")
        self.target_schema = config["curatedProperties"]["schemaName"]
        self.target_lakehouse_name = config["curatedProperties"]["lakehouseName"]
        self.source_lakehouse_name = config["rawProperties"]["lakehouseName"]
        self.column_list = config["curatedProperties"].get("columnList")
        self.candidate_keys = config["curatedProperties"].get("primaryKeyList")
        self.partition_keys = config["curatedProperties"].get("partitionKeyList")
        self.custom_file_options = config["curatedProperties"].get("customFileOptions")
        self.duplicate_check_enabled = config["curatedProperties"].get("duplicateCheckEnabled")
        self.duplicate_check_quarantine_type = config["curatedProperties"].get("duplicateCheckQuarantineType")
        self.dataset_type_name = config["datasetTypeName"]
        self.ingest_type = config["sourceSystemProperties"]["ingestType"]
        self.is_dynamic_query = config["sourceSystemProperties"].get("isDynamicQuery")
        self.include_specific_columns = config["sourceSystemProperties"].get("includeSpecificColumns")
        self.source_watermark_identifier = config["sourceSystemProperties"].get("sourceWatermarkIdentifier")
        self.metadata_lakehouse_name = "den_lhw_pdi_001_metadata"
        self.raise_errors = config["curatedProperties"].get("raiseErrors", True)
        self.expectations = config["curatedProperties"].get("expectations")
        self.quarantine_strategy = config["curatedProperties"].get("quarantineStrategy")
        self.is_cdc = config["sourceSystemProperties"].get("isCdc", False)  # Flag for CDC processing

        self._target_lakehouse = None
        self._source_lakehouse = None
        self._metadata_lakehouse = None

        self._validate_config()
        self._set_load_strategy(self.load_type)

        self.filterExpression = (
            (" " + config["sourceSystemProperties"]["filterExpression"])
            if "filterExpression" in config["sourceSystemProperties"].keys()
            else ""
        )
        self.include_specific_columns = (
            ",".join(self.include_specific_columns)
            if "includeSpecificColumns" in config["sourceSystemProperties"].keys()
            else "*"
        )

    def _validate_config(self):
        if self.source_format == "excel" and not self.custom_file_options:
            raise ValueError(
                f"Config source format is '{self.source_format}' but customFileOptions are missing."
            )
        if self.load_type == "merge" and not self.candidate_keys:
            raise ValueError(
                f"Config load type is '{self.load_type}' but primaryKeyList is missing."
            )
        if self.partition_keys and not isinstance(self.partition_keys, list):
            raise ValueError(
                f"Config partition keys must be a list. Type is {type(self.partition_keys)}."
            )
        if self.candidate_keys and not isinstance(self.candidate_keys, list):
            raise ValueError(
                f"Config primary keys must be a list. Type is {type(self.candidate_keys)}."
            )
        if self.column_list and not isinstance(self.column_list, list):
            raise ValueError(
                f"Config column list must be a list. Type is {type(self.column_list)}."
            )
        if self.duplicate_check_enabled and not self.duplicate_check_quarantine_type:
            raise ValueError(
                f"Config duplicate check is enabled but quarantine type is missing."
            )
        if self.duplicate_check_enabled and not self.candidate_keys:
            raise ValueError(
                f"Config duplicate check is enabled but primary keys are missing."
            )
        if self.is_cdc and not self.candidate_keys:
            raise ValueError(
                f"CDC processing enabled but primaryKeyList is missing."
            )

    def target_lakehouse(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        self._target_lakehouse = LakehouseManager(lakehouse_name, workspace_id)
        return self

    def source_lakehouse(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        self._source_lakehouse = LakehouseManager(lakehouse_name, workspace_id)
        return self

    def metadata_lakehouse(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        self._metadata_lakehouse = LakehouseManager(lakehouse_name, workspace_id)
        return self

    def _set_load_strategy(self, load_type: str):
        load_type = load_type.upper()
        try:
            self._load_type = LoadTypes.get_load_type(load_type)
        except AttributeError as e:
            raise ValueError(f"Load strategy: {load_type} is not supported.") from e
        return self

    def source_data(self, source_file: str):
        if not self._source_lakehouse:
            self.source_lakehouse(self.source_lakehouse_name)
        regex_lakehouse_path = "^abfss:\/\/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}@onelake.dfs.fabric.microsoft.com\/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
        if re.search(regex_lakehouse_path, source_file):
            self.source_file = source_file
        else:
            self.source_file = (
                f"{self._source_lakehouse.lakehouse_path}/Files/{source_file}"
            )
        self._source_data = self._read_source_data(
            self.source_file,
            self.source_format,
            self.source_options,
            self.custom_file_options,
        )
        return self

    def _read_source_data(
        self,
        source_file: str,
        source_format: str,
        source_options: str,
        custom_file_options: str,
    ) -> DataFrame:
        file_handler = FileHandler(source_format, source_options, custom_file_options)
        return file_handler.read_file(source_file)

    def _set_candidate_keys(self, candidate_keys: List[str]):
        if not candidate_keys:
            return None
        candidate_key_list = [item.lower() for item in candidate_keys]
        if not all(item in self._source_data.columns for item in candidate_key_list):
            raise ValueError(
                "There are columns in the Candidate Key that are not in the datafile!"
            )
        return candidate_key_list

    def _set_partition_keys(self, partition_keys: List[str]) -> List[str]:
        if not partition_keys:
            return ["dl_partitionkey", "dl_iscurrent"]
        return partition_keys

    def _log_quarantine(self, quarantine_metrics: list[dict]):
        quarantine_log = QuarantineLog()
        for q in quarantine_metrics:
            quarantine_log.add_log_data(
                source_system=self.source_system,
                dataset_name=self.target_table,
                elt_id=self.elt_id,
                run_id=self.run_id,
                total_rows=q["inserts"],
                quarantine_type=q["quarantine_type"],
                log_datetime=str(datetime.now(timezone.utc).isoformat()),
                primary_key_list=",".join(self.candidate_keys if self.candidate_keys else [])
            )
        quarantine_log.write_log()

    def _process_cdc_data(self, source_data: DataFrame) -> DataFrame:
        """
        Process CDC data to handle inserts (__operation = 2), deletes (__operation = 1), and updates (__operation = 4).
        """
        if not self.is_cdc:
            return source_data

        # Define window to order by __start_lsn and __seqval within primary key
        window_spec = Window.partitionBy(*self.candidate_keys).orderBy("___start_lsn", "___seqval")
        cdc_df = source_data.withColumn("row_num", row_number().over(window_spec))

        # Select latest operation per primary key
        latest_changes = cdc_df.groupBy(*self.candidate_keys).agg(max_("row_num").alias("max_row_num"))
        latest_cdc_df = cdc_df.join(
            latest_changes,
            (cdc_df[self.candidate_keys[0]] == latest_changes[self.candidate_keys[0]]) &
            (cdc_df["row_num"] == latest_changes["max_row_num"]),
            "inner"
        ).select(cdc_df["*"])

        # Add dl_is_deleted and update dl_ columns
        processed_df = latest_cdc_df.withColumn(
            "dl_is_deleted",
            when(col("___operation") == 1, lit(1)).otherwise(lit(0))
        ).withColumn(
            "dl_iscurrent",
            when(col("___operation") == 1, lit(0)).otherwise(lit(1))
        ).withColumn(
            "dl_lastmodifiedutc",
            lit(self.batch_time)
        ).select(self.column_list + ["dl_is_deleted", "dl_iscurrent", "dl_lastmodifiedutc"])

        return processed_df

    def start_ingest(self, elt_id: str = "0", run_id: str = "0") -> None:
        """
        Ingest source data into target table after setting required configs.
        """
        self.elt_id = elt_id
        self.run_id = run_id
        if not self._target_lakehouse:
            self.target_lakehouse(self.target_lakehouse_name)

        quarantine_table = None

        self.METRICS = IngestionMetrics()
        self.ALLMETRICS = GroupMetrics()
        self.METRICS.eltId = self.elt_id
        self.METRICS.runId = self.run_id
        self.METRICS.startTime = self.batch_time
        self.METRICS.lakehouse = self.target_lakehouse_name
        self.METRICS.deltaSchema = self.target_schema
        self.METRICS.deltaObject = self.target_table
        self.METRICS.sourceDataCount = self._source_data.count()

        try:
            # Apply column list if specified
            self._source_data = (
                self._source_data.selectExpr(self.column_list)
                if self.column_list
                else self._source_data
            )

            # Process CDC data if enabled
            self._source_data = self._process_cdc_data(self._source_data)

            metadata = Metadata(
                {
                    "meta_column_prefix": "dl_",
                    "batch_time": self.batch_time,
                    "elt_id": self.elt_id,
                    "run_id": self.run_id,
                },
                sorted(self._source_data.columns),
            )
            self._source_data = metadata.add_ingest_metadata_columns(self._source_data)

            self.target_schema = self.schema_manager.clean_schema_name(self.target_schema)
            self.target_table = self.schema_manager.clean_table_name(self.target_table)
            self.candidate_keys = self._set_candidate_keys(self.candidate_keys)
            self.partition_keys = self._set_partition_keys(self.partition_keys)

            table_path = f"{self._target_lakehouse.lakehouse_path}/Tables/{self.target_schema}/{self.target_table}"
            target_exists = self._target_lakehouse.check_if_table_exists(self.target_table, self.target_schema)

            quarantine_table = (
                QuarantineVersion(None).configure(table_path).set_current_version()
            )

            self._load_type.configure_load(
                table_path=table_path,
                candidate_keys=self.candidate_keys,
                partition_keys=self.partition_keys,
                batch_time=self.batch_time,
            )

            if self.duplicate_check_enabled:
                self._source_data = self._load_type.deduplicate(
                    self._source_data, self.duplicate_check_quarantine_type
                )
                if self._load_type.quarantine_metrics:
                    self.ALLMETRICS.quarantine.append(self._load_type.quarantine_metrics)

            expectation_success = True
            dq = Expectations(
                self.target_lakehouse_name,
                self.target_schema,
                self.target_table,
                self._source_data
            ).set_expectations(
                self.target_table,
                self.target_schema,
                self.target_lakehouse_name
            )
            if dq.expectations:
                dq.perform_expectations(self.candidate_keys)
                dq.quarantine(self.quarantine_strategy)
                self._source_data = dq.expectation_df.drop(*[c for c in dq.expectation_df.columns if c.startswith("dq_")])
                if dq.quarantine_metrics:
                    self.ALLMETRICS.quarantine.append(dq.quarantine_metrics)
                self.ALLMETRICS.expectations = dq.expectation_metrics
                expectation_success = dq.expectation_metrics["success"]

            if expectation_success:
                if self.load_type == "merge" and not target_exists:
                    self._load_type.first_time_load(self._source_data)
                else:
                    self._load_type.ingest(self._source_data)
                self.METRICS.success = True

                inserts, updates, deletes, output_bytes = self._load_type.get_table_metrics(table_path)
                self.METRICS.recordInserts = inserts
                self.METRICS.recordUpdates = updates
                self.METRICS.recordDeletes = deletes
                self.METRICS.outputBytes = output_bytes

            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()
            self.ALLMETRICS.ingestion = self.METRICS.to_dict()

            if self.ALLMETRICS.quarantine:
                self._log_quarantine(self.ALLMETRICS.quarantine)

        except Exception as e:
            self.METRICS.error = str(e)
            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()
            if quarantine_table:
                quarantine_table.restore_current_version()
            if self.raise_errors:
                raise Exception(e)

        return self

    def metrics(self, frmt: str = "json"):
        if frmt == "json":
            return self.ALLMETRICS.to_json()
        if frmt == "dict":
            return self.ALLMETRICS.to_dict()

    def perform_pre_check(self, dataset_config_folder_name: str = "", dataset_file_name: str = "") -> None:
        self.dataset_config_folder_name = dataset_config_folder_name
        self.dataset_file_name = dataset_file_name
        if self.dataset_type_name == "database" and self.ingest_type == "watermark" and self.is_dynamic_query:
            self.update_watermark_query()
        return self

    def update_watermark_query(self) -> None:
        source_watermark_identifier_row = self._source_data.agg({"dl_watermark": "max"}).collect()[0][0]
        if source_watermark_identifier_row is None:
            print(f"No records exist in {self.target_table}. Skipping watermark query file update.")
            return
        source_watermark_identifier_value = source_watermark_identifier_row
        if source_watermark_identifier_value is None:
            print(f"No watermark value found in {self.target_table}. Skipping watermark query file update.")
            return
        include_specific_columns = self.include_specific_columns
        filter_expression = self.filterExpression
        source_table_schema = self.source_table_schema
        source_table = self.target_table
        query_extract_json = {
            "query": f"SELECT {include_specific_columns}, {self.source_watermark_identifier} as dl_watermark FROM {source_table_schema}.{source_table} "
                     f"WHERE {self.source_watermark_identifier} > CAST('{source_watermark_identifier_value}' AS datetime2){filter_expression}",
            "sourceWatermarkIdentifier": f"{self.source_watermark_identifier}",
            "watermarkValue": f"{source_watermark_identifier_value}"
        }
        if not self._metadata_lakehouse:
            self.metadata_lakehouse(self.metadata_lakehouse_name)
        file_location = f"{self._metadata_lakehouse.lakehouse_path}/Files/datasets/{self.dataset_config_folder_name}/watermark/{self.dataset_file_name}.json"
        notebookutils.fs.put(file_location, json.dumps(query_extract_json), overwrite=True)
        return self
