"""Configuration model and loader.

Replaces the original ``configparser`` + ``project.cfg`` approach with a typed,
environment-aware config:

* A base ``pipeline.yml`` describes structure that is identical across
  environments (table names, watermark bounds, medallion schema names).
* A per-environment overlay (``dev.yml`` / ``prod.yml``) overrides only what
  changes between environments (catalog name, storage account, hostnames).
* Secrets (DB user/password) are **never** stored in YAML. They are resolved at
  runtime from a Databricks secret scope backed by Azure Key Vault.

This keeps the repo safe to commit and makes promotion between environments a
config change rather than a code change.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yaml


# --------------------------------------------------------------------------- #
# Secret resolution
# --------------------------------------------------------------------------- #
SecretResolver = Callable[[str, str], str]


def dbutils_secret_resolver(dbutils: Any) -> SecretResolver:
    """Build a resolver backed by ``dbutils.secrets`` (Key Vault backed scope)."""

    def _resolve(scope: str, key: str) -> str:
        return dbutils.secrets.get(scope=scope, key=key)

    return _resolve


def env_secret_resolver() -> SecretResolver:
    """Resolver for local/unit-test use that reads from environment variables.

    Key Vault key ``source-db-password`` maps to env var ``SOURCE_DB_PASSWORD``.
    """
    import os

    def _resolve(scope: str, key: str) -> str:  # noqa: ARG001 - scope unused locally
        env_key = key.replace("-", "_").upper()
        value = os.environ.get(env_key)
        if value is None:
            raise KeyError(f"Secret '{key}' not found in environment as '{env_key}'")
        return value

    return _resolve


# --------------------------------------------------------------------------- #
# Config dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TableSpec:
    """Source table description used to drive JDBC partitioned reads."""

    name: str
    partition_column: str = "id"
    lower_bound: Optional[int] = None
    upper_bound: Optional[int] = None
    num_partitions: int = 20


@dataclass(frozen=True)
class JdbcConfig:
    """Connection details for a relational database accessed over JDBC."""

    rdbms: str  # postgresql | sqlserver | mysql
    host: str
    port: int
    database: str
    secret_scope: str
    user_key: str
    password_key: str
    _resolver: SecretResolver = field(repr=False, default=env_secret_resolver())

    @property
    def driver(self) -> str:
        return {
            "postgresql": "org.postgresql.Driver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "mysql": "com.mysql.cj.jdbc.Driver",
        }[self.rdbms]

    @property
    def url(self) -> str:
        if self.rdbms == "sqlserver":
            return (
                f"jdbc:sqlserver://{self.host}:{self.port};"
                f"databaseName={self.database};encrypt=true;trustServerCertificate=false"
            )
        return f"jdbc:{self.rdbms}://{self.host}:{self.port}/{self.database}"

    @property
    def user(self) -> str:
        return self._resolver(self.secret_scope, self.user_key)

    @property
    def password(self) -> str:
        return self._resolver(self.secret_scope, self.password_key)


@dataclass(frozen=True)
class Config:
    """Top-level pipeline configuration."""

    env: str
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    source_db: JdbcConfig
    serving_db: JdbcConfig
    source_tables: Dict[str, TableSpec]
    reports: Dict[str, str]

    def fqn(self, schema: str, table: str) -> str:
        """Return the Unity Catalog fully-qualified name ``catalog.schema.table``."""
        return f"{self.catalog}.{schema}.{table}"

    def bronze(self, table: str) -> str:
        return self.fqn(self.bronze_schema, table)

    def silver(self, table: str) -> str:
        return self.fqn(self.silver_schema, table)

    def gold(self, table: str) -> str:
        return self.fqn(self.gold_schema, table)


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in overlay.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config(
    env: str,
    conf_dir: str = "conf",
    secret_resolver: Optional[SecretResolver] = None,
) -> Config:
    """Load and merge ``pipeline.yml`` with the ``{env}.yml`` overlay.

    :param env: environment name, e.g. ``dev`` or ``prod``.
    :param conf_dir: directory containing the YAML files.
    :param secret_resolver: how DB secrets are resolved; defaults to env vars.
    """
    resolver = secret_resolver or env_secret_resolver()
    conf_path = Path(conf_dir)

    base = yaml.safe_load((conf_path / "pipeline.yml").read_text())
    overlay = yaml.safe_load((conf_path / f"{env}.yml").read_text())
    merged = _deep_merge(base, overlay)

    src = merged["source_db"]
    serving = merged["serving_db"]

    source_db = JdbcConfig(
        rdbms=src["rdbms"],
        host=src["host"],
        port=int(src["port"]),
        database=src["database"],
        secret_scope=src["secret_scope"],
        user_key=src["user_key"],
        password_key=src["password_key"],
        _resolver=resolver,
    )
    serving_db = JdbcConfig(
        rdbms=serving["rdbms"],
        host=serving["host"],
        port=int(serving["port"]),
        database=serving["database"],
        secret_scope=serving["secret_scope"],
        user_key=serving["user_key"],
        password_key=serving["password_key"],
        _resolver=resolver,
    )

    source_tables = {
        logical: TableSpec(
            name=spec["name"],
            partition_column=spec.get("partition_column", "id"),
            lower_bound=spec.get("lower_bound"),
            upper_bound=spec.get("upper_bound"),
            num_partitions=spec.get("num_partitions", 20),
        )
        for logical, spec in merged["source_db"]["tables"].items()
    }

    return Config(
        env=env,
        catalog=merged["catalog"],
        bronze_schema=merged["schemas"]["bronze"],
        silver_schema=merged["schemas"]["silver"],
        gold_schema=merged["schemas"]["gold"],
        source_db=source_db,
        serving_db=serving_db,
        source_tables=source_tables,
        reports=merged["serving_db"]["reports"],
    )
