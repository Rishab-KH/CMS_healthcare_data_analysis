"""Console entry points for Databricks ``python_wheel_task`` tasks.

Each medallion layer is a separate job task so it can be retried, monitored and
sized independently. The Databricks Job passes parameters like
``["--env", "prod", "--table", "drug"]``; on Databricks the secret resolver is
wired to ``dbutils.secrets`` (Key Vault backed scope), and locally it falls back
to environment variables.

Entry points are registered in ``pyproject.toml`` under ``[project.scripts]``.
"""
from __future__ import annotations

import argparse
from typing import Optional

from healthcare_pipeline.common.config import (
    Config,
    dbutils_secret_resolver,
    env_secret_resolver,
    load_config,
)
from healthcare_pipeline.common.database import DatabaseConnector
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


def _resolver():
    """Use dbutils secrets on Databricks; env vars otherwise."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        from healthcare_pipeline.common.spark import get_spark as _gs

        return dbutils_secret_resolver(DBUtils(_gs()))
    except Exception:
        return env_secret_resolver()


def _load(env: str, conf_dir: str) -> Config:
    return load_config(env=env, conf_dir=conf_dir, secret_resolver=_resolver())


def _base_args(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--env", default="dev")
    p.add_argument("--conf-dir", default="conf")
    return p


# --------------------------------------------------------------------------- #
# Bronze
# --------------------------------------------------------------------------- #
def ingest_task(argv: Optional[list] = None) -> None:
    p = _base_args("Ingest one source table into bronze.")
    p.add_argument("--table", required=True, help="Logical table key from config.")
    args = p.parse_args(argv)

    config = _load(args.env, args.conf_dir)
    spark = get_spark()
    store = DeltaStore(spark)
    source_db = DatabaseConnector(spark, config.source_db)
    IngestStage(config, store, source_db, args.table).execute()


# --------------------------------------------------------------------------- #
# Silver
# --------------------------------------------------------------------------- #
_SILVER = {
    "state": CleanStateStage,
    "drug": CleanDrugStage,
    "prescriber": CleanPrescriberStage,
    "prescriber_drug": CleanPrescriberDrugStage,
}


def preprocess_task(argv: Optional[list] = None) -> None:
    p = _base_args("Cleanse one bronze table into silver.")
    p.add_argument("--entity", required=True, choices=sorted(_SILVER))
    args = p.parse_args(argv)

    config = _load(args.env, args.conf_dir)
    store = DeltaStore(get_spark())
    _SILVER[args.entity](config, store).execute()


# --------------------------------------------------------------------------- #
# Gold
# --------------------------------------------------------------------------- #
_GOLD = {
    "drug_report": DrugReportStage,
    "prescriber_report": PrescriberReportStage,
}


def transform_task(argv: Optional[list] = None) -> None:
    p = _base_args("Build one gold report from silver.")
    p.add_argument("--report", required=True, choices=sorted(_GOLD))
    args = p.parse_args(argv)

    config = _load(args.env, args.conf_dir)
    store = DeltaStore(get_spark())
    _GOLD[args.report](config, store).execute()


# --------------------------------------------------------------------------- #
# Serving
# --------------------------------------------------------------------------- #
def publish_task(argv: Optional[list] = None) -> None:
    p = _base_args("Publish one gold report to the serving database.")
    p.add_argument("--report", required=True, choices=["drug_report", "prescriber_report"])
    args = p.parse_args(argv)

    config = _load(args.env, args.conf_dir)
    spark = get_spark()
    store = DeltaStore(spark)
    serving_db = DatabaseConnector(spark, config.serving_db)
    PublishStage(config, store, serving_db, args.report).execute()
