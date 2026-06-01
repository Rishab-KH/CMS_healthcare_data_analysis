"""Spark session helper.

On a Databricks cluster the ``SparkSession`` already exists and is configured
with the right Delta, Unity Catalog and ADLS credentials by the platform, so we
should *reuse* it rather than building one (the original project's
``create_spark_object`` was written for local/EMR Spark).

``get_spark`` returns the active session and only falls back to ``builder`` for
local unit tests, where it wires Delta Lake in explicitly.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from healthcare_pipeline.common.logger import get_logger

logger = get_logger(__name__)


def get_spark(app_name: str = "healthcare_pipeline") -> SparkSession:
    """Return the active Spark session, or a Delta-enabled local one for tests."""
    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active Spark session '%s'.", active.sparkContext.appName)
        return active

    logger.info("No active session found; building local Delta session '%s'.", app_name)
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        logger.warning("delta-spark not installed; building plain session.")
        return builder.getOrCreate()
