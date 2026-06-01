"""End-to-end pipeline orchestrator.

Builds the shared collaborators (Spark, Delta store, DB connectors) once and runs
every medallion stage in order. This is the equivalent of the original
``run_pipeline.main`` and is useful for a single-cluster run or local testing.

In production the Databricks Job (see ``resources/healthcare_job.yml``) runs each
layer as a *separate task* via the wheel entry points in ``entrypoints.py`` so
they can be retried and scaled independently — but both paths share this exact
stage code.
"""
from __future__ import annotations

from healthcare_pipeline.common.config import Config, load_config
from healthcare_pipeline.common.database import DatabaseConnector
from healthcare_pipeline.common.logger import get_logger
from healthcare_pipeline.common.spark import get_spark
from healthcare_pipeline.common.storage import DeltaStore
from healthcare_pipeline.ingest.ingest import IngestStage
from healthcare_pipeline.preprocess.preprocessing import (
    CleanDrugStage,
    CleanPrescriberDrugStage,
    CleanPrescriberStage,
    CleanStateStage,
)
from healthcare_pipeline.publish.publish import PublishStage
from healthcare_pipeline.transform.transformation import (
    DrugReportStage,
    PrescriberReportStage,
)

logger = get_logger(__name__)


def run(config: Config) -> None:
    logger.info("Healthcare pipeline starting for env=%s ...", config.env)

    spark = get_spark("healthcare_pipeline")
    store = DeltaStore(spark)
    source_db = DatabaseConnector(spark, config.source_db)
    serving_db = DatabaseConnector(spark, config.serving_db)

    # ---- Bronze: ingest every configured source table ------------------ #
    logger.info("== BRONZE: ingest raw source tables ==")
    for logical in config.source_tables:
        IngestStage(config, store, source_db, logical).execute()

    # ---- Silver: cleanse & conform ------------------------------------- #
    logger.info("== SILVER: cleanse & conform ==")
    CleanStateStage(config, store).execute()
    CleanDrugStage(config, store).execute()
    CleanPrescriberStage(config, store).execute()
    CleanPrescriberDrugStage(config, store).execute()

    # ---- Gold: business reports ---------------------------------------- #
    logger.info("== GOLD: build report tables ==")
    DrugReportStage(config, store).execute()
    PrescriberReportStage(config, store).execute()

    # ---- Serving: publish to read store -------------------------------- #
    logger.info("== SERVING: publish to read database ==")
    PublishStage(config, store, serving_db, "drug_report").execute()
    PublishStage(config, store, serving_db, "prescriber_report").execute()

    logger.info("Healthcare pipeline completed successfully.")


def main(env: str = "dev", conf_dir: str = "conf") -> None:
    config = load_config(env=env, conf_dir=conf_dir)
    run(config)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the healthcare medallion pipeline.")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--conf-dir", default="conf")
    args = parser.parse_args()
    main(env=args.env, conf_dir=args.conf_dir)
