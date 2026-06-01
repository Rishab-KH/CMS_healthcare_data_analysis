"""Gold transformation: build business-level report tables.

Ports the original ``Transformation`` class. Joins/aggregates conformed silver
data into the two analytic products consumed by reporting:

* ``gold.drug_report``       — claims & cost rolled up per (brand, generic) drug,
                               enriched with antibiotic classification.
* ``gold.prescriber_report`` — per prescriber/drug claims & cost, enriched with
                               state name and prescriber type.

Gold tables are partitioned by ``_load_date`` so each daily run is retained as a
queryable snapshot (time-travel also remains available via Delta).
"""
from __future__ import annotations

from pyspark.sql import functions as F

from healthcare_pipeline.common.base import Stage


class DrugReportStage(Stage):
    layer = "gold"

    def run(self) -> None:
        presc_drug = self.store.read(self.config.silver("prescriber_drug"))
        drug = self.store.read(self.config.silver("drug")).alias("drug")

        agg = (
            presc_drug.select(
                "drug_brand_name",
                "drug",
                "total_claims",
                F.col("total_drug_cost").alias("total_cost"),
            )
            .groupBy("drug_brand_name", "drug")
            .agg(
                F.sum("total_claims").alias("total_claims"),
                F.sum("total_cost").alias("total_cost"),
            )
            .alias("pd")
        )

        report = agg.join(
            drug,
            on=(
                (F.col("pd.drug_brand_name") == F.col("drug.drug_brand_name"))
                & (F.col("pd.drug") == F.col("drug.drug"))
            ),
            how="inner",
        ).select("pd.*", "drug.drug_type")

        report = report.withColumn("_load_date", F.current_date())

        self.validation.expect_non_empty(report)
        self.validation.profile(report, name="gold.drug_report")
        self.store.overwrite(
            report, self.config.gold("drug_report"), partition_by=["_load_date"]
        )


class PrescriberReportStage(Stage):
    layer = "gold"

    def run(self) -> None:
        state = self.store.read(self.config.silver("state")).alias("state")
        presc = self.store.read(self.config.silver("prescriber")).alias("presc")
        presc_drug = (
            self.store.read(self.config.silver("prescriber_drug"))
            .select(
                "presc_id",
                "presc_fullname",
                "presc_specialty",
                "presc_state_code",
                "total_claims",
                "total_drug_cost",
            )
            .alias("pd")
        )

        report = (
            presc_drug.join(
                state,
                on=F.col("pd.presc_state_code") == F.col("state.state_code"),
                how="inner",
            )
            .join(
                presc,
                on=F.col("pd.presc_id") == F.col("presc.presc_id"),
                how="inner",
            )
            .select("pd.*", "state.state_name", "presc.presc_type")
        )

        report = report.withColumn("_load_date", F.current_date())

        self.validation.expect_non_empty(report)
        self.validation.profile(report, name="gold.prescriber_report")
        self.store.overwrite(
            report, self.config.gold("prescriber_report"), partition_by=["_load_date"]
        )
