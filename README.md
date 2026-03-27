# ecommerce-lakehouse

A production-grade data engineering project built on AWS and Databricks. It ingests data from 18 real-world sources — relational databases, streaming brokers, REST APIs, files, IoT sensors, and more — processes it through a medallion architecture (Bronze → Silver → Gold), and serves it for analytics. Every component mirrors what you would build at a real company.

This is a learning project designed to take someone from beginner to staff-level data engineering knowledge. It is not a toy example. Every tool, pattern, and decision here exists because real systems require it.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         18 DATA SOURCES                             │
│  RDS Postgres · MongoDB · Kafka Clickstream · SQS · Stripe API      │
│  ShipStation API · Shopify GraphQL · SFTP Files · Partner S3        │
│  ERP Exports · Reviews/Tickets · Image Metadata · Scrapy            │
│  MQTT IoT Sensors · CloudWatch Logs · GA4 Export · SES Email        │
└────────────┬─────────────────┬──────────────────────┬──────────────┘
             │                 │                      │
     ┌───────▼──────┐  ┌───────▼──────┐     ┌────────▼────────┐
     │ Airflow MWAA │  │  AWS MSK     │     │  AWS Lambda     │
     │              │  │  (Kafka)     │     │  + Kinesis      │
     │ Batch / File │  │  Streaming / │     │  Event triggers │
     │ API ingestion│  │  CDC / IoT   │     │  Log streaming  │
     └───────┬──────┘  └───────┬──────┘     └────────┬────────┘
             │                 │                      │
             └─────────────────▼──────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   S3 Raw Landing    │
                    │   (Immutable zone)  │
                    │  partitioned by     │
                    │  source + date      │
                    └──────────┬──────────┘
                               │
              ┌────────────────▼─────────────────┐
              │         DATABRICKS               │
              │                                  │
              │  Delta Live Tables (DLT)          │
              │  + Raw PySpark Jobs               │
              │                                  │
              │  ┌──────────────────────────┐    │
              │  │  BRONZE  (raw, as-is)    │    │
              │  └────────────┬─────────────┘    │
              │               │                  │
              │  ┌────────────▼─────────────┐    │
              │  │  SILVER  (clean, typed)  │    │
              │  └────────────┬─────────────┘    │
              │               │                  │
              │  ┌────────────▼─────────────┐    │
              │  │  GOLD    (business ready)│    │
              │  └──────────────────────────┘    │
              └────────────────┬─────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   Unity Catalog     │
                    │  Lineage · Access   │
                    │  Column security    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │ Databricks SQL      │
                    │ Warehouse           │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   dbt Core          │
                    │   Analytics models  │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   BI / Dashboards   │
                    │  Tableau / Power BI │
                    └─────────────────────┘
```

---

## Medallion Architecture

The medallion architecture organises data into three layers — Bronze, Silver, and Gold. Each layer has a specific purpose and a specific level of trust.

**Bronze — Raw landing zone**
Exactly what arrived. Nothing changed, nothing removed. If the source sent malformed JSON, Bronze stores malformed JSON. This layer exists so you can always replay from the original data. It is append-only. You never fix Bronze — you fix downstream.

**Silver — Clean and typed**
Bronze data that has been parsed, typed, deduplicated, and validated. Columns have correct data types. Nulls are handled. Duplicate events are removed. CDC operations (INSERT/UPDATE/DELETE) are applied. This is where Great Expectations runs its checks. Silver is what most analytics queries read from.

**Gold — Business-ready**
Aggregated, joined, and shaped for a specific use case. Examples: `daily_revenue_by_channel`, `customer_lifetime_value`, `inventory_reorder_alerts`. Gold tables are what dashboards and BI tools connect to. They are rebuilt from Silver — never edited directly.

**Why three layers?**
If a bug is found in a Silver transformation, you can reprocess from Bronze without re-ingesting from the source. If a Gold model changes business logic, you reprocess from Silver without re-cleaning. Each layer is independently replayable.

---

## Data Sources

| # | Source | Type | What it contains |
|---|--------|------|-----------------|
| 01 | RDS PostgreSQL | Relational DB | Orders, customers, inventory, payments |
| 02 | Debezium CDC | Change stream | Every INSERT / UPDATE / DELETE from Postgres |
| 03 | MongoDB Atlas | NoSQL DB | Product catalog — nested docs, variants, attributes |
| 04 | AWS MSK Kafka | Event stream | Clickstream — page views, add-to-cart, checkout events |
| 05 | AWS SQS | Message queue | Order notifications — placed, confirmed, cancelled |
| 06 | Stripe API | REST API | Payments, charges, refunds, disputes |
| 07 | ShipStation API | REST API | Shipments, tracking, carriers, delivery status |
| 08 | Shopify GraphQL | GraphQL API | Products, collections, inventory, discounts |
| 09 | SFTP Drop | File (CSV/Excel) | Supplier catalogs, pricing sheets, inventory |
| 10 | Partner S3 Drop | File (Parquet/Avro) | Partner sales, affiliate data, cross-sell |
| 11 | ERP Export | Semi-structured | Finance ledger, purchase orders, invoices |
| 12 | Reviews / Tickets | Unstructured text | Customer reviews, support tickets, return reasons |
| 13 | S3 + Lambda | Event metadata | Product image metadata — filename, size, path |
| 14 | Scrapy | Web scraping | Competitor pricing, stock availability, promotions |
| 15 | MQTT IoT Sensors | Streaming telemetry | Warehouse temperature, humidity, stock weight |
| 16 | CloudWatch Logs | Log streaming | App errors, access logs, Lambda execution logs |
| 17 | GA4 Export | SaaS export | Web sessions, funnels, acquisition, conversions |
| 18 | AWS SES Email | Email | Order confirmations, shipping notifications |

---

## Tech Stack

### Cloud Infrastructure
| Tool | Role |
|------|------|
| AWS | Cloud provider |
| Terraform | Infrastructure as code — provisions everything |
| AWS S3 | Data lake storage (all three medallion layers) |
| AWS RDS PostgreSQL | OLTP source database |
| AWS MSK | Managed Kafka (real Kafka, not simulated) |
| AWS MWAA | Managed Airflow |
| AWS Lambda | Event-driven triggers |
| AWS Kinesis | Log streaming from CloudWatch |
| AWS SQS | Message queue ingestion |
| AWS SES | Email event capture |

### Data Platform
| Tool | Role |
|------|------|
| Databricks | Core compute platform |
| Delta Lake | Table format — ACID transactions, time travel |
| Delta Live Tables (DLT) | Declarative streaming pipelines |
| Apache Spark (PySpark) | Distributed batch processing |
| Spark Structured Streaming | Real-time Kafka consumption |
| Unity Catalog | Data governance, lineage, access control |

### Ingestion
| Tool | Role |
|------|------|
| Debezium | CDC capture from Postgres |
| Airflow (MWAA) | Batch and file orchestration |
| Databricks Workflows | Intra-Databricks job chains |
| PyMongo | MongoDB ingestion |
| Python requests | REST API ingestion (Stripe, ShipStation) |
| Scrapy | Web scraping |

### Quality and Governance
| Tool | Role |
|------|------|
| Great Expectations | Data quality — expectations and checkpoints |
| Unity Catalog | Column-level lineage and access policies |
| Terraform tags | Cost attribution per pipeline |

### Analytics Layer
| Tool | Role |
|------|------|
| dbt Core | Analytics engineering on top of Gold |
| Databricks SQL Warehouse | Query engine |
| Tableau / Power BI | Dashboards and reporting |

---

## Project Structure

```
unified-ecommerce-platform/
│
├── infra/                          # Terraform — all AWS + Databricks infrastructure
│   ├── aws/
│   │   ├── s3.tf
│   │   ├── rds.tf
│   │   ├── msk.tf
│   │   ├── mwaa.tf
│   │   ├── lambda.tf
│   │   └── networking.tf
│   └── databricks/
│       ├── workspace.tf
│       ├── unity_catalog.tf
│       └── clusters.tf
│
├── ingestion/                      # Source connectors and producers
│   ├── batch/
│   │   ├── stripe/
│   │   ├── shipstation/
│   │   ├── shopify/
│   │   ├── mongodb/
│   │   └── sftp/
│   ├── streaming/
│   │   ├── debezium/               # CDC config
│   │   ├── kafka_producers/        # Clickstream, IoT
│   │   └── kinesis/                # CloudWatch logs
│   └── files/
│       ├── erp_parser/
│       ├── partner_s3/
│       └── scrapy_spiders/
│
├── pipelines/                      # Databricks notebooks and DLT definitions
│   ├── dlt/
│   │   ├── bronze_streaming.py     # DLT streaming tables
│   │   ├── silver_cdc.py           # CDC apply logic
│   │   └── silver_clickstream.py
│   ├── pyspark/
│   │   ├── bronze_to_silver/
│   │   │   ├── orders.py
│   │   │   ├── products.py
│   │   │   └── payments.py
│   │   └── silver_to_gold/
│   │       ├── daily_revenue.py
│   │       ├── customer_ltv.py
│   │       └── inventory_alerts.py
│   └── ml/
│       └── feature_engineering/
│
├── orchestration/                  # Airflow DAGs
│   ├── dags/
│   │   ├── batch_ingestion.py
│   │   ├── api_connectors.py
│   │   ├── sftp_polling.py
│   │   └── trigger_databricks.py
│   └── plugins/
│
├── quality/                        # Great Expectations
│   ├── expectations/
│   │   ├── bronze_orders.json
│   │   ├── silver_customers.json
│   │   └── gold_revenue.json
│   └── checkpoints/
│
├── dbt/                            # dbt analytics models
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
│
├── contracts/                      # Data contracts — schema agreements
│   ├── orders_v1.yaml
│   ├── clickstream_v2.yaml
│   └── products_v1.yaml
│
├── docs/
│   ├── architecture/
│   ├── runbooks/
│   └── decisions/                  # Architecture decision records
│
└── tests/
    ├── unit/
    └── integration/
```

---

## Orchestration Design

Two orchestrators, each with a clear lane. They do not overlap.

**Airflow (MWAA) owns anything that crosses systems:**
- Polling SFTP servers for new files
- Calling external REST APIs (Stripe, ShipStation, Shopify)
- Consuming from SQS and routing to S3 or Kafka
- Triggering Databricks jobs once upstream systems have landed data
- Managing dependencies between external sources

**Databricks Workflows owns everything inside Databricks:**
- Chaining Bronze → Silver → Gold jobs
- Triggering DLT pipeline runs
- Running ML feature engineering after Silver is ready
- Notebook-to-notebook dependencies within the platform

This separation means: if an external API goes down, Airflow handles the retry. If a Spark job fails mid-chain, Databricks Workflows handles the retry. Neither tool is asked to do something it was not built for.

---

## DLT vs Raw PySpark

Not everything belongs in Delta Live Tables, and not everything belongs in raw PySpark.

| Use DLT for | Use raw PySpark for |
|-------------|---------------------|
| Streaming ingestion (Kafka, CDC, IoT) | Complex multi-source batch joins |
| Bronze → Silver auto-pipeline | Custom SCD Type 2 history tracking |
| Built-in data quality expectations | ML feature engineering |
| Auto-scaling streaming tables | Heavy deduplication across billions of rows |

The rule of thumb: DLT when the pipeline is continuous or declarative. PySpark when you need precise control over execution.

---

## How to Run It

> Prerequisites: AWS account, Databricks workspace, Terraform installed, Python 3.10+

### 1. Clone the repository
```bash
git clone https://github.com/your-username/unified-ecommerce-platform.git
cd unified-ecommerce-platform
```

### 2. Configure credentials
```bash
cp infra/aws/terraform.tfvars.example infra/aws/terraform.tfvars
# Fill in your AWS credentials, region, and Databricks workspace URL
```

### 3. Provision infrastructure
```bash
cd infra/aws
terraform init
terraform plan
terraform apply
```

### 4. Set up Databricks
```bash
cd infra/databricks
terraform init
terraform apply
# Creates the Unity Catalog, clusters, and DLT pipeline definitions
```

### 5. Configure Airflow
```bash
# Upload DAGs to the MWAA S3 bucket
aws s3 sync orchestration/dags/ s3://your-mwaa-bucket/dags/
```

### 6. Test a single source
```bash
pip install -r requirements.txt
python ingestion/batch/stripe/ingest.py --date 2024-01-01
```

### 7. Run the pipeline
```bash
# Trigger the DLT pipeline from the Databricks CLI
databricks pipelines start --pipeline-id <your-pipeline-id>
```

> **Cost note:** MSK brokers cost ~$0.21/hr and MWAA ~$0.08/hr. Pause or destroy these when not in active use. `terraform destroy` tears down everything.

---

## What You Learn From This Project

**Core data engineering**
- How distributed Spark jobs actually execute — drivers, executors, shuffles, spills
- How Kafka guarantees delivery and what happens when a consumer crashes mid-read
- How Delta Lake handles concurrent writes and why ACID matters at scale
- How CDC works — capturing every database change and applying it downstream
- How to process late-arriving events correctly using watermarks in Spark Structured Streaming

**Infrastructure and operations**
- Provisioning real cloud infrastructure with Terraform from scratch
- Designing for failure — what breaks, how to detect it, how to recover without data loss
- Cost governance — tagging resources and understanding what each pipeline actually costs
- Writing data contracts so schema changes do not silently break downstream tables

**Architecture thinking**
- When to use batch vs streaming — and what the operational cost is of getting it wrong
- When DLT is the right abstraction and when raw PySpark gives you control you actually need
- How to separate orchestration between two tools cleanly without creating a mess
- How to design a medallion architecture that is genuinely replayable, not just cosmetically layered

**Staff-level practices most tutorials skip**
- Unity Catalog column-level lineage — being able to prove exactly where a number came from
- Great Expectations checkpoints — catching bad data before it reaches Gold and corrupts reports
- SCD Type 2 — keeping a full history of how dimension records changed over time
- Architecture decision records — documenting why each design choice was made, not just what was chosen

---

## Design Principles

**Real services only.** No mocked brokers or Docker Kafka that disappears when the terminal closes. Everything here runs on actual infrastructure.

**Understand before you build.** Every component is studied broken before it is built working. The question is always: what fails when the network drops? When a machine crashes? When data is malformed?

**Staff-level non-negotiables are built in from day one.** Data contracts, lineage, and cost governance are not added at the end. Junior engineers skip them. Staff engineers do not.

**Both tools, not one.** DLT and raw PySpark. Airflow and Databricks Workflows. The project uses both in each pair because each combination covers cases the other cannot — not because more tools is better.

---

## Cost Estimate (Active Development)

| Service | Size | Approx. monthly cost |
|---------|------|----------------------|
| RDS PostgreSQL | db.t3.micro | ~$0 (free tier eligible) |
| MongoDB Atlas | M0 | Free forever |
| AWS MSK | kafka.t3.small, 1 broker | ~$150 |
| Databricks | Community Edition | Free |
| MWAA | mw1.small | ~$80 |
| S3 storage | under 100GB | ~$2 |

MSK and MWAA account for almost all the cost. Pause them when not in active use.

---

## Progress

- [ ] Infrastructure provisioned (Terraform)
- [ ] RDS PostgreSQL source running
- [ ] Debezium CDC capturing changes
- [ ] MSK Kafka cluster live
- [ ] Bronze layer ingestion (all 18 sources)
- [ ] Silver layer transformations
- [ ] Gold layer aggregations
- [ ] Unity Catalog governance configured
- [ ] Great Expectations quality checks
- [ ] Airflow DAGs complete
- [ ] dbt analytics models
- [ ] Full end-to-end pipeline test
