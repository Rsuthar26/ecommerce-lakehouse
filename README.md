# Unified E-Commerce Data Platform

A production-grade data engineering platform built on AWS and Databricks, covering every data source type, ingestion pattern, and processing layer found in enterprise e-commerce systems.

---

## What this is

A full end-to-end data platform that ingests 18 different data sources — relational databases, NoSQL, REST APIs, GraphQL, Kafka streams, CDC, IoT sensors, file drops, web scraping, and SaaS exports — processes them through a medallion architecture (Bronze → Silver → Gold), and serves business-ready data for analytics and reporting.

Every component is real infrastructure on real cloud services. Nothing is simulated with Docker or mocked locally.

---

## Architecture
```
18 Data Sources
      │
      ├── Batch / Files / APIs     → Airflow (MWAA)
      ├── Real-time streams        → AWS MSK (Kafka)
      ├── CDC (Postgres changes)   → Debezium → MSK
      ├── Event-driven             → AWS Lambda + SQS
      └── Log streaming            → CloudWatch → Kinesis
      │
      ▼
S3 Raw Landing Zone          ← immutable, partitioned by source + date
      │
      ▼
Databricks (Delta Live Tables + PySpark)
  Bronze  ← raw ingestion, schema applied, nothing dropped
  Silver  ← cleaned, deduplicated, quarantine for bad records
  Gold    ← aggregated, joined, business-ready
      │
      ▼
Unity Catalog                ← governance, lineage, column-level security
      │
      ▼
Databricks SQL Warehouse
      │
      ▼
dbt Core → Tableau / Power BI
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud | AWS |
| Platform | Databricks |
| Storage | AWS S3 (Delta Lake) |
| Streaming | AWS MSK (Managed Kafka) |
| CDC | Debezium → MSK |
| Source DB (relational) | AWS RDS PostgreSQL |
| Source DB (NoSQL) | MongoDB Atlas |
| Processing | Databricks + PySpark |
| Stream processing | Spark Structured Streaming |
| Table format | Delta Lake |
| Pipelines | Delta Live Tables + Raw PySpark |
| Orchestration | Airflow (MWAA) + Databricks Workflows |
| Governance | Unity Catalog |
| Data quality | Great Expectations |
| Infrastructure | Terraform |
| Language | Python + PySpark + SQL |

---

## Data Sources

18 sources covering every ingestion pattern that exists in production systems:

| # | Source | Type |
|---|--------|------|
| 01 | RDS PostgreSQL | Relational database |
| 02 | Debezium CDC | Change data capture |
| 03 | MongoDB Atlas | NoSQL documents |
| 04 | MSK Kafka Clickstream | Real-time event stream |
| 05 | AWS SQS | Message queue |
| 06 | Stripe API | REST API |
| 07 | ShipStation API | REST API |
| 08 | Shopify GraphQL | GraphQL API |
| 09 | SFTP Drop | File transfer (CSV / Excel) |
| 10 | Partner S3 Drop | File transfer (Parquet / Avro) |
| 11 | ERP Export | Semi-structured (JSON / XML) |
| 12 | Reviews and Tickets | Unstructured text |
| 13 | S3 + Lambda | Event-driven binary metadata |
| 14 | Scrapy | Web scraping |
| 15 | MQTT IoT Sensors | Real-time IoT stream |
| 16 | CloudWatch Logs | Application log stream |
| 17 | GA4 Export | SaaS analytics export |
| 18 | AWS SES Email | Email event stream |

→ See [`generators/README.md`](generators/README.md) for the full breakdown of each source.

---

## Staff-Level Standards

Three things junior engineers skip that this project does not:

**Data Contracts** — schema agreements between producers and consumers defined before pipelines are built.

**Data Lineage** — Unity Catalog column-level lineage from every Gold metric back to its raw source record.

**Cost Governance** — every AWS resource tagged `project=ecommerce-lakehouse, env=dev`. MSK and MWAA are destroyed between sessions and rebuilt on demand.

---

## Data Quality

Every data source produces intentionally imperfect data — malformed records, missing fields, wrong types, duplicate events, referential integrity breaks. The Silver layer earns its existence by handling all of it:

- Bad records are never silently dropped
- Every rejection is counted, logged, and written to a quarantine table
- Data quality scores tracked per pipeline run via Great Expectations
- Alerts fire if rejection rate exceeds threshold

---

## Repo Structure
```
ecommerce-lakehouse/
├── generators/          18 data source simulators
├── pipelines/           Bronze → Silver → Gold (DLT + PySpark)
├── infrastructure/      Terraform — all AWS + Databricks resources
├── orchestration/       Airflow DAGs + Databricks Workflows
├── quality/             Great Expectations suites and checkpoints
├── demo/                start_demo.sh — full platform in 15 minutes
└── README.md
```

---

## Running a Demo
```
# Provision infrastructure (~25 minutes first time)
cd infrastructure/terraform
terraform init
terraform apply

# Generate 7 days of data across all 18 sources
bash demo/start_demo.sh

# Pipeline runs Bronze → Silver → Gold automatically
# Results visible in Databricks SQL Warehouse
```

Total cost per demo session: approximately $2–3.

---

## Build Status

| Layer | Status |
|---|---|
| All 18 data generators | ✅ Complete |
| AWS infrastructure (Terraform) | ✅ Complete — RDS, MSK, EC2, S3 |
| S3 Raw Landing Zone | 🔄 In progress |
| Bronze ingestion pipelines | 🔄 In progress |
| Silver cleaning layer | ⬜ Pending |
| Gold analytics layer | ⬜ Pending |
| Airflow orchestration | ⬜ Pending |
| Unity Catalog governance | ⬜ Pending |
| Great Expectations quality | ⬜ Pending |
| dbt + BI layer | ⬜ Pending |
