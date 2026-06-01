"""Silver preprocessing: cleanse and conform bronze data.

Ports the original ``Preprocessing`` class. Each concrete stage reads one bronze
table, applies cleansing/standardisation, validates, and overwrites the
corresponding silver Delta table. Silver tables are conformed (renamed, typed,
deduplicated) and safe for analysts to consume.

Design note: the original packed four methods into one class sharing the same
buckets. Splitting them into one :class:`Stage` per entity makes each step
independently runnable, testable, and parallelisable as separate job tasks.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from healthcare_pipeline.common.base import Stage


class CleanStateStage(Stage):
    layer = "silver"

    def run(self) -> None:
        src = self.store.read(self.config.bronze("state"))
        df = src.select(
            F.col("state_id").alias("state_code"),
            "state_name",
            "city",
            "population",
        )
        df = df.groupBy("state_code", "state_name").agg(
            F.sum("population").alias("total_population"),
            F.countDistinct("city").alias("num_city"),
        )
        self.validation.expect_non_empty(df)
        self.validation.expect_unique(df, ["state_code"])
        self.validation.profile(df, name="silver.state")
        self.store.overwrite(df, self.config.silver("state"))


class CleanDrugStage(Stage):
    layer = "silver"

    def run(self) -> None:
        src = self.store.read(self.config.bronze("drug"))
        df = src.select(
            F.col("brnd_name").alias("drug_brand_name"),
            F.col("gnrc_name").alias("drug"),
            F.col("antbtc_drug_flag").alias("is_antibiotic"),
        )
        df = df.withColumn(
            "drug_type",
            F.when(F.col("is_antibiotic") == "Y", F.lit("Antibiotic")).otherwise(
                F.lit("Non-antibiotic")
            ),
        ).drop("is_antibiotic")
        # Collapse to one row per (brand, generic) — keep the latest type seen.
        df = df.groupBy("drug_brand_name", "drug").agg(
            F.last("drug_type", ignorenulls=True).alias("drug_type")
        )
        self.validation.expect_non_empty(df)
        self.validation.expect_unique(df, ["drug_brand_name", "drug"])
        self.validation.profile(df, name="silver.drug")
        self.store.overwrite(df, self.config.silver("drug"))


class CleanPrescriberStage(Stage):
    layer = "silver"

    def run(self) -> None:
        src = self.store.read(self.config.bronze("prescriber"))
        df = src.select(
            F.col("prscrbr_npi").alias("presc_id"),
            F.col("prscrbr_ent_cd").alias("presc_type"),
            F.col("prscrbr_city").alias("presc_city"),
            F.col("prscrbr_state_abrvtn").alias("presc_state_code"),
        )
        df = df.withColumn(
            "presc_type",
            F.when(F.col("presc_type") == "I", F.lit("Individual")).otherwise(
                F.lit("Organization")
            ),
        )
        self.validation.expect_non_empty(df)
        self.validation.expect_no_nulls(df, ["presc_id"])
        self.validation.profile(df, name="silver.prescriber")
        self.store.overwrite(df, self.config.silver("prescriber"))


class CleanPrescriberDrugStage(Stage):
    layer = "silver"

    def run(self) -> None:
        src = self.store.read(self.config.bronze("prescriber_drug"))
        df = src.select(
            F.col("prscrbr_npi").alias("presc_id"),
            F.col("prscrbr_last_org_name").alias("presc_lname"),
            F.col("prscrbr_first_name").alias("presc_fname"),
            F.col("prscrbr_state_abrvtn").alias("presc_state_code"),
            F.col("prscrbr_type").alias("presc_specialty"),
            F.col("brnd_name").alias("drug_brand_name"),
            F.col("gnrc_name").alias("drug"),
            F.col("tot_clms").alias("total_claims"),
            F.col("tot_drug_cst").alias("total_drug_cost"),
        )
        # Drop rows without a specialty (cannot attribute the claim).
        df = df.dropna(subset=["presc_specialty"])
        # Build a display name and drop the raw name parts.
        df = df.withColumn(
            "presc_fullname", F.concat_ws(" ", "presc_fname", "presc_lname")
        ).drop("presc_fname", "presc_lname")

        self.validation.expect_non_empty(df)
        self.validation.expect_no_nulls(df, ["presc_id", "drug_brand_name", "drug"])
        self.validation.profile(df, name="silver.prescriber_drug")
        self.store.overwrite(df, self.config.silver("prescriber_drug"))
