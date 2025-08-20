from typing import List, Optional
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr, desc, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark_engine.sparkconf import spark
from spark_engine.data_quality.quarantine import QuarantineTypes


class LoadStrategy(ABC):
    def configure_load(
        self,
        table_path: str,
        candidate_keys: Optional[List],
        partition_keys: Optional[List],
        batch_time: Optional[datetime],
    ) -> None:
        """
        Configure columns for ingestion
        """
        if not batch_time:
            batch_time = datetime.now(timezone.utc).isoformat()
        self.batch_time = batch_time

        # column config
        self._candidate_keys = candidate_keys
        self._partition_keys = partition_keys

        self.table_path = table_path

        # merge conditions
        self._dataframe_condition = ""


    def first_time_load(self, source_data: DataFrame) -> None:
        """
        Use a DataFrame write for first time load when table needs to exist before merging.
        """
        source_data.write.format("delta").partitionBy(self._partition_keys).option(
            "delta.checkpoint.writeStatsAsStruct", "true"
        ).option("delta.checkpoint.writeStatsAsJson", "false").save(self.table_path)

    def deduplicate(self, source_data: DataFrame, quarantine_type: str):
        """
        De-duplicate the source data.
        """
        window = Window.partitionBy(*self._candidate_keys).orderBy(
            col("dl_lastmodifiedutc").desc()
        )
        df = source_data.withColumn("row_number", row_number().over(window))
        source_data = df.filter(col("row_number") == 1).drop("row_number")

        quarantine = QuarantineTypes.get_quarantine_type(quarantine_type)
        quarantine.configure(self.table_path)
        quarantine.quarantine(df)
        self.quarantine_metrics = quarantine.quarantine_metrics

        return source_data

    @staticmethod
    def _create_merge_predicate(merge_predicate: list[str]) -> str:
        return " and ".join([f"sd.{item} = bd.{item}" for item in merge_predicate])

    def get_table_metrics(self, table_path: str) -> tuple:
        df = DeltaTable.forPath(spark, table_path).history().where(f"timestamp > '{self.batch_time}'")
        df = (
            df.selectExpr(
                "operation",
                "operationMetrics.numTargetRowsInserted",
                "operationMetrics.numTargetRowsUpdated",
                "operationMetrics.numTargetRowsDeleted",
                "operationMetrics.numOutputRows",
                "operationMetrics.numUpdatedRows",
                "operationMetrics.numDeletedRows",
                "operationMetrics.numOutputBytes",
            )
            .orderBy(desc("timestamp"))
        )

        inserts = updates = deletes = output_bytes = 0
        for row in df.collect():
            (
                op,
                tar_inserts,
                tar_updates,
                tar_deletes,
                rec_inserts,
                rec_updates,
                rec_deletes,
                rec_bytes_output,
            ) = row
            if op in {
                "CREATE TABLE AS SELECT",
                "WRITE",
                "CREATE OR REPLACE TABLE",
                "CREATE OR REPLACE TABLE AS SELECT",
            }:
                inserts += int(rec_inserts)
                output_bytes += int(rec_bytes_output)
            elif op == "MERGE":
                inserts += int(tar_inserts)
                updates += int(tar_updates)
                deletes += int(tar_deletes)
            elif op == "UPDATE":
                updates += int(rec_updates)
            elif op == "DELETE":
                deletes += int(rec_deletes)

        return inserts, updates, deletes, output_bytes

    @abstractmethod
    def ingest(self, source_data: DataFrame) -> None:
        raise NotImplementedError
    
    @staticmethod
    def _get_upd_col(df: DataFrame, exclusions: list[str]) -> dict:
        upd_col = {}
        for i in df.columns:
            if(i in exclusions):
                continue
            upd_col[i] = "sd." + i

        return upd_col


class OverwriteLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame):
        source_data.write.format("delta").partitionBy(self._partition_keys).option(
            "delta.checkpoint.writeStatsAsStruct", "true"
        ).option("delta.checkpoint.writeStatsAsJson", "false"
        ).option("overwriteSchema", "true").mode("overwrite").save(
            self.table_path
        )

class AppendLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame):
        source_data.write.format("delta").partitionBy(self._partition_keys).option(
            "mergeSchema", "true"
        ).mode("append").save(
            self.table_path
        )

class MergeLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame):
        base_data = DeltaTable.forPath(spark, self.table_path)
        merge_predicate = self._create_merge_predicate(self._candidate_keys)
        upd_col = self._get_upd_col(df=source_data, exclusions=["dl_createddateutc"])

        (
            base_data.alias("bd").merge(
                source=source_data.alias("sd"), condition=merge_predicate
            )
            .withSchemaEvolution()
            .whenMatchedUpdate("bd.dl_rowhash <> sd.dl_rowhash", set=upd_col)
            .whenNotMatchedInsertAll()
            .execute()
        )


class LoadTypes:
    class LoadStrategies:
        OVERWRITE_LOAD = OverwriteLoad
        APPEND_LOAD = AppendLoad
        MERGE_LOAD = MergeLoad

    @classmethod
    def get_load_type(cls, load_type: str):
        load_type = load_type.upper()
        if not load_type.endswith("_LOAD"):
            load_type += "_LOAD"

        load_strategy = getattr(cls.LoadStrategies, load_type)

        return load_strategy()
