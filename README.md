# Healthcare Medallion Pipeline — Azure Databricks + Delta Lake

An end-to-end data engineering pipeline that ingests CMS **Medicare Part D**
prescriber/drug data from a relational source, refines it through a
**bronze → silver → gold** medallion architecture on **Delta Lake / Unity
Catalog**, and publishes analytics-ready reports to a serving database for BI.

This is an Azure + Databricks re-implementation of an AWS S3 + PySpark project,
rebuilt around Delta tables, Unity Catalog governance, idempotent incremental
loads, a Template-Method stage design, and CI/CD-friendly deployment via
**Databricks Asset Bundles**.

> Source data: the four CMS tables — `state`, `drug`, `prescriber`, and the large
> `prescriber_drug` claims fact (~25M rows).

---

## Why this design

The original project wrote daily Parquet partitions to three S3 buckets
(raw/cleansed/curated), tracked incremental loads by counting S3 objects and
reading `MAX(id)`, and embedded DB credentials in the JDBC URL. This version
keeps the same business logic but modernises the platform and practices:

| Concern | Azure/Databricks |
|---|---|
| Object store | ADLS Gen2 + **managed Delta tables** |
| Table format | **Delta** (ACID, schema enforcement, time travel) |
| Catalog | **Unity Catalog** `catalog.schema.table` + grants |
| Zones | **bronze / silver / gold** |
| Compute | **Databricks** (reuses platform session) |
| Source DB | **Azure Database for PostgreSQL** |
| Serving DB |**Azure SQL Database** |
| Incremental | watermark + **Delta `MERGE`** (idempotent) |
| Config | typed YAML + **env overlays** |
| Secrets | **Key Vault-backed secret scope** |
| Orchestration | **Databricks Job DAG** (per-layer tasks) |
| Deployment | **Databricks Asset Bundles** (IaC) |
| Error handling | **Template-Method `Stage`** base class |

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for diagrams and lineage.

---

## Engineering practices on show

- **Medallion architecture** with clear contracts per layer (bronze = faithful
  copy, silver = conformed, gold = business product).
- **Idempotency** — bronze ingestion uses a high-watermark + Delta `MERGE`; silver
  and gold use full `overwrite`; reruns produce the same result.
- **Design patterns** — Template Method (`common/base.py::Stage`), Dependency
  Injection (connectors/stores passed into stages), Connector/Gateway
  (`DeltaStore`, `DatabaseConnector`).
- **Separation of concerns** — one stage class per entity, runnable in isolation.
- **Config as data** — base YAML + per-environment overlays; promotion is a config
  change, not a code change.
- **Secret hygiene** — credentials resolved at runtime from a Key Vault-backed
  secret scope; never committed, never put in the JDBC URL.
- **Data quality gates** — non-empty / uniqueness / null-free assertions that
  *fail the job* on a broken contract.
- **Auditability** — every row stamped with `_ingested_at_utc`, `_source_system`,
  `_load_date`; Delta `DESCRIBE HISTORY` for full change history.
- **Packaging & CI** — installable wheel with console entry points, unit tests,
  `ruff`/`black` config.
- **Infrastructure as code** — Terraform stub for Azure resources + Asset Bundle
  for the workspace job.

---

## Repository layout

```
healthcare-databricks-pipeline/
├── README.md
├── databricks.yml                 # Asset Bundle: targets dev/prod, builds wheel
├── pyproject.toml                 # packaging + console entry points
├── conf/
│   ├── pipeline.yml               # base config (table specs, schemas, reports)
│   ├── dev.yml  /  prod.yml        # environment overlays
├── resources/
│   └── healthcare_job.yml         # Databricks Job DAG (bronze→silver→gold→serve)
├── sql/
│   └── 00_setup_unity_catalog.sql # catalog, schemas, grants
├── infra/terraform/main.tf        # Azure resources (ADLS, KV, Databricks, DBs)
├── notebooks/run_pipeline_driver.py
├── tests/test_pipeline.py
├── docs/ARCHITECTURE.md
└── src/healthcare_pipeline/
    ├── common/                    # logger, config, spark, storage, database, validation, base
    ├── ingest/ingest.py           # bronze
    ├── preprocess/preprocessing.py# silver
    ├── transform/transformation.py# gold
    ├── publish/publish.py         # serving
    ├── pipeline/run_pipeline.py   # full-run orchestrator
    └── entrypoints.py             # wheel entry points for job tasks
```

---

## Setup

### 1. Provision Azure resources

```bash
cd infra/terraform
terraform init
terraform apply -var environment=dev
```

This creates the resource group, ADLS Gen2 storage, Key Vault, a premium
Databricks workspace (required for Unity Catalog), the PostgreSQL source server,
and the Azure SQL serving database. (The `.tf` is an illustrative stub — wire in
your own networking, admin passwords via `TF_VAR_*`, and Unity Catalog metastore
assignment.)

### 2. Create the secret scope (Key Vault-backed)

```bash
databricks secrets create-scope healthcare \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id <key-vault-resource-id> \
  --dns-name https://kv-healthcare-dev.vault.azure.net/
```

Store these keys in the Key Vault: `source-db-user`, `source-db-password`,
`serving-db-user`, `serving-db-password`.

### 3. Create the Unity Catalog objects

Run `sql/00_setup_unity_catalog.sql` in a SQL editor (substitute `${catalog}`
with `healthcare_dev` / `healthcare_prod`).

### 4. JDBC drivers

The Postgres and SQL Server JDBC drivers are bundled with the Databricks Runtime
used here (`15.4.x`). For older runtimes, add them as cluster libraries.

---

## Deploy & run

Deployment uses the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)
and Asset Bundles. Update the `workspace.host` values in `databricks.yml` first.

```bash
# Build the wheel + create/update the job in the dev workspace
databricks bundle deploy -t dev

# Trigger a run
databricks bundle run healthcare_pipeline -t dev

# Promote to prod
databricks bundle deploy -t prod
```

The job runs daily at 06:00 (`resources/healthcare_job.yml`) as a 12-task DAG:
four parallel bronze ingests fan out to silver cleansing, which fans into the two
gold reports, which publish to the serving DB.

### Run a single layer

Each layer is an independent wheel entry point:

```bash
healthcare-ingest      --env dev --table prescriber_drug
healthcare-preprocess  --env dev --entity drug
healthcare-transform   --env dev --report drug_report
healthcare-publish     --env dev --report drug_report
```

Or run everything in one process:

```bash
healthcare-pipeline --env dev
```

---

## Local development & tests

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Config tests need no Spark; transformation tests use a local Delta session.
export SOURCE_DB_USER=... SOURCE_DB_PASSWORD=...
export SERVING_DB_USER=... SERVING_DB_PASSWORD=...
pytest

ruff check src tests
black --check src tests
```

Locally the config loader resolves secrets from environment variables
(`source-db-password` → `SOURCE_DB_PASSWORD`); on Databricks it resolves them
from the Key Vault-backed scope via `dbutils.secrets`.

---

## Notes & extensions

Natural next steps for a production hardening pass: swap the custom DQ assertions
for **Delta Live Tables expectations** or **Great Expectations**; add **SCD Type 2**
history on the `prescriber`/`drug` dimensions; register tables in a data catalog
with column-level lineage; and add a CI workflow (GitHub Actions) that runs
`pytest` + `ruff` and `databricks bundle validate` on every PR.
