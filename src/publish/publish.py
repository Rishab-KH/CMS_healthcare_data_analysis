"""Serving publish: push gold reports to the read-optimised serving database.

Ports the original ``consume_data``. Reads a gold Delta table (latest snapshot)
and writes it to the serving database (Azure SQL Database) where BI tools query
it with low latency. Only the most recent ``_load_date`` partition is published.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from healthcare_pipeline.common.base import Stage
from healthcare_pipeline.common.config import Config
from healthcare_pipeline.common.database import DatabaseConnector
from healthcare_pipeline.common.storage import DeltaStore


class PublishStage(Stage):
    layer = "serving"

    def __init__(
        self,
        config: Config,
        store: DeltaStore,
        serving_db: DatabaseConnector,
        report_logical: str,
    ):
        super().__init__(config, store)
        self.serving_db = serving_db
        self.report_logical = report_logical
        self.gold_table = config.gold(report_logical)
        self.target_table = config.reports[report_logical]

    def run(self) -> None:
        df = self.store.read(self.gold_table)

        # Publish only the latest snapshot partition.
        latest = df.agg(F.max("_load_date").alias("d")).collect()[0]["d"]
        if latest is not None:
            df = df.filter(F.col("_load_date") == F.lit(latest))

        # Drop lake-internal audit columns before serving.
        serve_df = df.drop("_ingested_at_utc", "_source_system", "_load_date")

        self.validation.expect_non_empty(serve_df)
        self.serving_db.write_table(
            data_frame=serve_df,
            db_table=self.target_table,
            write_mode="overwrite",
            memory_partition=20,
        )
        self._logger.info(
            "Published %s -> serving table %s (snapshot %s).",
            self.gold_table,
            self.target_table,
            latest,
        )
