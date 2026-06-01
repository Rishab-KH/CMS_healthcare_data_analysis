# Architecture

## End-to-end flow

```mermaid
flowchart LR
    subgraph SRC["Source systems"]
        PG[("Azure Database<br/>for PostgreSQL<br/>(Medicare Part D)")]
    end

    subgraph LAKE["Azure Databricks Lakehouse — Unity Catalog + Delta Lake (ADLS Gen2)"]
        direction LR
        B["**Bronze**<br/>raw, source-faithful<br/>+ audit cols<br/><br/>state · drug<br/>prescriber<br/>prescriber_drug"]
        S["**Silver**<br/>cleansed & conformed<br/><br/>state · drug<br/>prescriber<br/>prescriber_drug"]
        G["**Gold**<br/>business reports<br/><br/>drug_report<br/>prescriber_report"]
        B -->|cleanse / standardise| S
        S -->|join / aggregate| G
    end

    subgraph SERVE["Serving"]
        SQL[("Azure SQL Database<br/>report.*")]
        BI["BI / dashboards"]
    end

    PG -->|partitioned JDBC read<br/>incremental MERGE| B
    G -->|publish latest snapshot| SQL
    SQL --> BI

    ORCH["Databricks Job (DAB)<br/>scheduled DAG"] -.orchestrates.-> LAKE
```

## Task DAG (Databricks Job)

```mermaid
flowchart TD
    is[ingest_state] --> cs[clean_state]
    idr[ingest_drug] --> cdr[clean_drug]
    ip[ingest_prescriber] --> cp[clean_prescriber]
    ipd[ingest_prescriber_drug] --> cpd[clean_prescriber_drug]

    cdr --> dr[drug_report]
    cpd --> dr
    cs --> pr[prescriber_report]
    cp --> pr
    cpd --> pr

    dr --> pdr[publish_drug_report]
    pr --> ppr[publish_prescriber_report]
```

## Data lineage

| Gold table          | Built from (silver)                          | Grain                       |
|---------------------|----------------------------------------------|-----------------------------|
| `drug_report`       | `prescriber_drug` ⨝ `drug`                   | (drug_brand_name, drug)     |
| `prescriber_report` | `prescriber_drug` ⨝ `state` ⨝ `prescriber`   | prescriber × drug claim row |

## Report layouts

**drug_report** — `drug_brand_name`, `drug`, `total_claims`, `total_cost`, `drug_type`

**prescriber_report** — `presc_id`, `presc_fullname`, `presc_specialty`, `presc_state_code`,
`total_claims`, `total_drug_cost`, `state_name`, `presc_type`
