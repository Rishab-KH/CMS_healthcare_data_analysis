"""Delta Lake storage gateway.

This is the Azure/Databricks replacement for the original ``S3Connector``. Instead
of reading and writing Parquet objects on ``s3a://`` paths, we read and write
**managed Delta tables in Unity Catalog** (``catalog.schema.table``). Delta gives
us, for free, the things the original project had to hand-roll:

* ACID overwrites/appends (no half-written partitions),
* schema enforcement and evolution,
* ``MERGE`` for idempotent incremental loads (the original read ``max(id)`` and
  filtered the source — fine, but not idempotent on retries),
* time travel and ``DESCRIBE HISTORY`` for audit,
* table-level statistics instead of manual file counting.

Audit columns (``_ingested_at_utc``, ``_source_system``) are stamped on write so
every row is traceable back to a run — a standard practice in production lakes.
"""
from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from healthcare_pipeline.common.logger import get_logger

logger = get_logger(__name__)


class DeltaStore:
    """Read/write managed Delta tables in Unity Catalog."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._logger = logger

    # ------------------------------------------------------------------ #
    # Existence / metadata
    # ------------------------------------------------------------------ #
    def table_exists(self, table_fqn: str) -> bool:
        """Return True if the Unity Catalog table already exists."""
        try:
            return self.spark.catalog.tableExists(table_fqn)
        except Exception:  # pragma: no cover - defensive
            return False

    def max_value(self, table_fqn: str, column: str) -> Optional[int]:
        """Return the current high-watermark for incremental loads (or None)."""
        if not self.table_exists(table_fqn):
            return None
        row = self.spark.table(table_fqn).agg(F.max(column).alias("mx")).collect()[0]
        return row["mx"]

    # ------------------------------------------------------------------ #
    # Read
    # ------------------------------------------------------------------ #
    def read(self, table_fqn: str) -> DataFrame:
        self._logger.info("Reading Delta table %s ...", table_fqn)
        return self.spark.table(table_fqn)

    # ------------------------------------------------------------------ #
    # Write
    # ------------------------------------------------------------------ #
    @staticmethod
    def _with_audit_columns(data_frame: DataFrame, source_system: str) -> DataFrame:
        return data_frame.withColumn(
            "_ingested_at_utc", F.current_timestamp()
        ).withColumn("_source_system", F.lit(source_system))

    def overwrite(
        self,
        data_frame: DataFrame,
        table_fqn: str,
        partition_by: Optional[List[str]] = None,
        source_system: str = "healthcare_pipeline",
    ) -> None:
        """Idempotent full-refresh write of a layer table."""
        try:
            self._logger.info("Overwriting Delta table %s ...", table_fqn)
            writer = (
                self._with_audit_columns(data_frame, source_system)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.saveAsTable(table_fqn)
        except Exception as exp:
            self._logger.error(
                "Error overwriting %s. Check the stack trace. %s",
                table_fqn,
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Wrote Delta table %s successfully.", table_fqn)

    def merge(
        self,
        data_frame: DataFrame,
        table_fqn: str,
        merge_keys: List[str],
        partition_by: Optional[List[str]] = None,
        source_system: str = "healthcare_pipeline",
    ) -> None:
        """Idempotent upsert keyed on ``merge_keys`` (used for bronze ingestion).

        Creates the table on first run, then performs a Delta ``MERGE`` so re-runs
        of the same batch do not duplicate rows.
        """
        from delta.tables import DeltaTable

        staged = self._with_audit_columns(data_frame, source_system)

        try:
            if not self.table_exists(table_fqn):
                self._logger.info("Creating Delta table %s on first load.", table_fqn)
                writer = staged.write.format("delta").mode("overwrite")
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.saveAsTable(table_fqn)
                return

            self._logger.info("Merging %d cols into %s ...", len(staged.columns), table_fqn)
            target = DeltaTable.forName(self.spark, table_fqn)
            condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
            (
                target.alias("t")
                .merge(staged.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception as exp:
            self._logger.error(
                "Error merging into %s. Check the stack trace. %s",
                table_fqn,
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Merged into Delta table %s successfully.", table_fqn)

    def optimize(self, table_fqn: str, zorder_by: Optional[List[str]] = None) -> None:
        """Compact small files and optionally Z-order (post-load maintenance)."""
        sql = f"OPTIMIZE {table_fqn}"
        if zorder_by:
            sql += f" ZORDER BY ({', '.join(zorder_by)})"
        self._logger.info("Running maintenance: %s", sql)
        self.spark.sql(sql)
