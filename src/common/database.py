"""JDBC database connector.

Azure/Databricks replacement for the original ``DatabaseConnector``. Two real
differences from the original:

1. Credentials come from a :class:`JdbcConfig` (Key Vault backed secrets) and are
   passed as JDBC *properties*, never interpolated into the URL string. The
   original embedded ``user``/``password`` directly in the URL, which leaks them
   into logs (the original even logged the full URL).
2. Partitioned reads are first-class: ``partitionColumn`` / ``lowerBound`` /
   ``upperBound`` / ``numPartitions`` let Spark parallelise the extract across
   executors instead of pulling the whole table through one connection.

Source DB on Azure: *Azure Database for PostgreSQL*.
Serving DB on Azure: *Azure SQL Database* (or Azure Database for MySQL).
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from healthcare_pipeline.common.config import JdbcConfig
from healthcare_pipeline.common.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnector:
    """Read from / write to a relational database over JDBC."""

    def __init__(self, spark: SparkSession, jdbc: JdbcConfig):
        self.spark = spark
        self.jdbc = jdbc
        self._logger = logger

    def _properties(self) -> dict:
        return {
            "user": self.jdbc.user,
            "password": self.jdbc.password,
            "driver": self.jdbc.driver,
        }

    def read_table(
        self,
        query_or_table: str,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: Optional[int] = None,
        fetch_size: int = 10_000,
    ) -> DataFrame:
        """Read a table or push-down subquery into a Spark DataFrame.

        ``query_or_table`` may be a bare table name or a parenthesised subquery
        alias, e.g. ``"(SELECT * FROM drug WHERE id > 10) AS t"``.
        """
        try:
            self._logger.info("Reading from %s database ...", self.jdbc.rdbms)
            reader = (
                self.spark.read.format("jdbc")
                .option("url", self.jdbc.url)
                .option("dbtable", query_or_table)
                .option("fetchsize", fetch_size)
                .options(**self._properties())
            )
            if partition_column and lower_bound is not None and upper_bound is not None:
                reader = (
                    reader.option("partitionColumn", partition_column)
                    .option("lowerBound", str(lower_bound))
                    .option("upperBound", str(upper_bound))
                    .option("numPartitions", str(num_partitions or 1))
                )
            data_frame = reader.load()
        except Exception as exp:
            self._logger.error(
                "Error reading from database. Check the stack trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Read from database successfully.")
            return data_frame

    def write_table(
        self,
        data_frame: DataFrame,
        db_table: str,
        write_mode: str = "overwrite",
        memory_partition: Optional[int] = None,
        batch_size: int = 10_000,
    ) -> None:
        """Publish a DataFrame to the serving database."""
        try:
            self._logger.info("Writing DataFrame to table %s ...", db_table)
            if memory_partition:
                current = data_frame.rdd.getNumPartitions()
                if current > memory_partition:
                    data_frame = data_frame.coalesce(memory_partition)
                elif current < memory_partition:
                    data_frame = data_frame.repartition(memory_partition)
            (
                data_frame.write.format("jdbc")
                .option("url", self.jdbc.url)
                .option("dbtable", db_table)
                .option("batchsize", batch_size)
                .options(**self._properties())
                .mode(write_mode)
                .save()
            )
        except Exception as exp:
            self._logger.error(
                "Error writing to database. Check the stack trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Wrote DataFrame to %s successfully.", db_table)
