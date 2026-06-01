"""Data-quality validation.

Keeps the spirit of the original ``Validation`` class (row counts, sample rows,
schema logging) but adds assertion-style checks that *fail the job* when an
expectation is violated, instead of only logging. In a portfolio/production
context you generally want the pipeline to stop on a broken contract rather than
silently publish bad data downstream.
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from healthcare_pipeline.common.logger import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Raised when a data-quality expectation fails."""


class Validation:
    """Validate a DataFrame at a pipeline boundary."""

    def __init__(self):
        self._logger = logger

    def count_rows(self, data_frame: DataFrame) -> int:
        n = data_frame.count()
        self._logger.info("DataFrame contains %d rows.", n)
        return n

    def log_sample(self, data_frame: DataFrame, num_rows: int = 5) -> None:
        self._logger.info("First %d rows:", num_rows)
        pdf = data_frame.limit(num_rows).toPandas()
        self._logger.info("\n%s", pdf.to_string(index=False))

    def log_schema(self, data_frame: DataFrame) -> None:
        for field in data_frame.schema.fields:
            self._logger.info("\t%s", str(field))

    # ---- assertions ---------------------------------------------------- #
    def expect_non_empty(self, data_frame: DataFrame) -> None:
        if self.count_rows(data_frame) == 0:
            raise DataQualityError("Expected a non-empty DataFrame but got 0 rows.")

    def expect_unique(self, data_frame: DataFrame, keys: List[str]) -> None:
        total = data_frame.count()
        distinct = data_frame.select(*keys).distinct().count()
        if total != distinct:
            raise DataQualityError(
                f"Uniqueness check failed on {keys}: {total} rows but "
                f"{distinct} distinct key combinations ({total - distinct} dups)."
            )
        self._logger.info("Uniqueness check passed on %s.", keys)

    def expect_no_nulls(self, data_frame: DataFrame, columns: List[str]) -> None:
        from pyspark.sql import functions as F

        agg = data_frame.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
        ).collect()[0]
        offenders = {c: agg[c] for c in columns if agg[c] and agg[c] > 0}
        if offenders:
            raise DataQualityError(f"Null check failed for columns: {offenders}")
        self._logger.info("Null check passed on %s.", columns)

    def profile(
        self,
        data_frame: DataFrame,
        name: str,
        sample_rows: int = 5,
    ) -> None:
        """Convenience: log count + schema + sample for a named dataset."""
        self._logger.info("Profiling dataset '%s' ...", name)
        self.count_rows(data_frame)
        self.log_schema(data_frame)
        self.log_sample(data_frame, sample_rows)
