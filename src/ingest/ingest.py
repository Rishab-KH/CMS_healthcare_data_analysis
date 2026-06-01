"""Bronze ingestion: extract source tables as-is into the bronze layer.

Mirrors the original ``ingest_data`` but on Delta + Unity Catalog:

* High-watermark comes from ``MAX(id)`` on the **bronze Delta table** (no manual
  S3 object counting).
* Source extract is a partitioned JDBC read for parallelism.
* Landing is a Delta ``MERGE`` keyed on the table's id, which makes re-runs of the
  same window idempotent (a plain append would duplicate on retry).
* No business transformation here — bronze is a faithful copy of the source plus
  audit columns. Cleaning happens in silver.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from healthcare_pipeline.common.base import Stage
from healthcare_pipeline.common.config import Config, TableSpec
from healthcare_pipeline.common.database import DatabaseConnector
from healthcare_pipeline.common.storage import DeltaStore


class IngestStage(Stage):
    """Ingest one source table into bronze."""

    layer = "bronze"

    def __init__(
        self,
        config: Config,
        store: DeltaStore,
        source_db: DatabaseConnector,
        logical_table: str,
    ):
        super().__init__(config, store)
        self.source_db = source_db
        self.logical_table = logical_table
        self.spec: TableSpec = config.source_tables[logical_table]
        self.target = config.bronze(self.spec.name)

    def _build_query(self) -> str:
        watermark = self.store.max_value(self.target, self.spec.partition_column)
        if watermark is None:
            self._logger.info("Full load for source table '%s'.", self.spec.name)
            return f"(SELECT * FROM {self.spec.name}) AS t"
        self._logger.info(
            "Incremental load for '%s'; current watermark %s=%s.",
            self.spec.name,
            self.spec.partition_column,
            watermark,
        )
        return (
            f"(SELECT * FROM {self.spec.name} "
            f"WHERE {self.spec.partition_column} > {watermark}) AS t"
        )

    def run(self) -> None:
        query = self._build_query()
        df = self.source_db.read_table(
            query_or_table=query,
            partition_column=self.spec.partition_column,
            lower_bound=self.spec.lower_bound,
            upper_bound=self.spec.upper_bound,
            num_partitions=self.spec.num_partitions,
        )

        if df.rdd.isEmpty():
            self._logger.info("No new rows for '%s'; nothing to ingest.", self.spec.name)
            return

        # Stamp the logical load date for downstream partitioning / snapshots.
        df = df.withColumn("_load_date", F.current_date())

        self.store.merge(
            data_frame=df,
            table_fqn=self.target,
            merge_keys=[self.spec.partition_column],
            source_system=self.config.source_db.rdbms,
        )
        self.validation.profile(self.store.read(self.target), name=self.target)
