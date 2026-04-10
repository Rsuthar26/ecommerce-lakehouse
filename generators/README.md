# Data Sources

This folder contains all 18 data generators for the Unified E-Commerce Data Platform.

In a real company, these sources already exist — real customers are placing orders, real servers are firing events, real suppliers are sending files. A data engineer connects to those live systems. Here, we have no real users or live systems, so each generator simulates one real-world source. Everything downstream — S3, Bronze, Silver, Gold — is identical to what you would build in production.

---

## Running the Generators

```bash
# Fill 7 days of history before a demo
python generate.py --mode burst --days 7

# Fill 7 days with intentional dirty data (Silver cleaning practice)
python generate.py --mode burst --days 7 --dirty

# Run continuously as a live stream
python generate.py --mode stream

# Start all 18 at once
bash demo/start_demo.sh
```

---

## Source 01 — RDS PostgreSQL

**The operational heartbeat of the business. Every order, payment, and customer action lands here first.**

**Ingestion Pattern**
```
┌──────────────┐  SQL query   ┌──────────────┐  write JSON  ┌──────────────┐
│ PostgreSQL   │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│ RDS          │  on schedule │   MWAA       │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Relational database — OLTP |
| **In the real world** | The live database behind the e-commerce app. Every order, payment, customer registration, and inventory update is written here instantly by the application. The data engineer never writes to it — only reads from it. |
| **We simulate it with** | A generator that inserts realistic customers, orders, payments, and inventory rows into a real AWS RDS PostgreSQL instance with backdated timestamps and peak-hour patterns. |
| **It simulates** | The live operational database at the core of every real e-commerce business. |
| **How it works** | Airflow connects on a schedule, queries for records created or updated since the last run, and writes them as JSON to S3 partitioned by source and date. |
| **Dirty data** | Invalid emails, negative prices, nulls in required fields, placeholder strings like `free` or `TBD` in price columns. |
| **Bronze table** | `bronze.cdc_orders` · `bronze.cdc_customers` · `bronze.cdc_payments` |

---

## Source 02 — Debezium CDC

**Every change to the database — captured the moment it happens, without touching the application.**

**Ingestion Pattern**
```
┌──────────────┐  reads WAL   ┌──────────────┐  CDC events  ┌──────────────┐  write JSON  ┌──────────────┐
│ PostgreSQL   │─────────────▶│   Debezium   │─────────────▶│  Kafka MSK   │─────────────▶│   S3 Raw     │
│ RDS          │              │   on EC2     │              │              │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Change Data Capture — real-time stream |
| **In the real world** | Debezium runs as a Kafka Connect connector, reading PostgreSQL's internal change log (WAL) and converting every INSERT, UPDATE, and DELETE into a real-time Kafka event — without touching the application. |
| **We simulate it with** | The real Debezium connector deployed on EC2 pointed at Source 01. Source 01 produces the database writes. Debezium captures them automatically. No separate generator needed. |
| **It simulates** | The real-time change stream that keeps a data lake in sync with the operational database without polling it. |
| **How it works** | Debezium reads the PostgreSQL WAL continuously. Every change becomes a JSON event with before/after state and operation type (insert/update/delete), streamed into Kafka MSK and written to S3. |
| **Dirty data** | Partial events during database restarts, out-of-order events during high load, duplicate events during connector restarts. |
| **Bronze table** | `bronze.cdc_raw` |

---

## Source 03 — MongoDB Atlas

**The product catalogue. Nested documents, variants, images, and attributes — nothing fits neatly into rows.**

**Ingestion Pattern**
```
┌──────────────┐  PyMongo     ┌──────────────┐  write JSON  ┌──────────────┐
│   MongoDB    │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│   Atlas      │  on schedule │   MWAA       │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | NoSQL document database |
| **In the real world** | A NoSQL document database holding the product catalogue. Products have nested variants, images, and attributes. An Airflow DAG polls MongoDB on schedule and exports changed products to S3. |
| **We simulate it with** | A generator that writes realistic nested product documents to a real MongoDB Atlas M0 cluster using PyMongo, with arrays of variants, images, and realistic field variation between products. |
| **It simulates** | A live product catalogue database that a merchandising team manages and updates daily. |
| **How it works** | Airflow runs PyMongo queries on a schedule, pulls products updated since the last run, serialises the nested documents to JSON, and writes to S3 partitioned by date. |
| **Dirty data** | Missing image arrays, null variant prices, inconsistent field names between product categories, truncated descriptions. |
| **Bronze table** | `bronze.products_raw` |

---

## Source 04 — MSK Kafka Clickstream

**Every click, scroll, and add-to-cart — captured as it happens, at hundreds of thousands of events per day.**

**Ingestion Pattern**
```
┌──────────────┐  Kafka topic ┌──────────────┐  micro-batch ┌──────────────┐
│   Website    │─────────────▶│  Kafka MSK   │─────────────▶│   S3 Raw     │
│   / App      │  ~6 msgs/sec │              │  Spark SS    │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Real-time event stream |
| **In the real world** | The website fires a Kafka event for every user interaction — page view, product click, add to cart, checkout step. These flow into MSK at hundreds of thousands per day and are consumed by Spark Structured Streaming. |
| **We simulate it with** | A generator that publishes realistic clickstream JSON events to a real MSK Kafka topic at the correct cadence — ~6 events per second average, with peaks during simulated business hours. |
| **It simulates** | The live behavioural stream that every large e-commerce platform produces as customers browse. |
| **How it works** | Spark Structured Streaming consumes the Kafka topic continuously, micro-batches events every 30 seconds, and writes Parquet files to S3 Raw partitioned by event type and hour. |
| **Dirty data** | Missing session IDs, null product IDs on add-to-cart events, duplicate events from client retries, future timestamps from misconfigured client clocks. |
| **Bronze table** | `bronze.clickstream_raw` |

---

## Source 05 — AWS SQS

**Order notifications delivered reliably — even if the consumer is temporarily down.**

**Ingestion Pattern**
```
┌──────────────┐  SQS queue   ┌──────────────┐  write JSON  ┌──────────────┐
│ Application  │─────────────▶│    Lambda    │─────────────▶│   S3 Raw     │
│              │              │              │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Message queue — event-driven |
| **In the real world** | When an order is placed the application puts a message on SQS. Multiple downstream systems each consume their own copy. If a consumer is temporarily down, messages wait safely in the queue. |
| **We simulate it with** | A generator that produces order event JSON messages and puts them onto a real SQS queue. Lambda polls the queue, writes to S3 Raw, and deletes processed messages. Failed messages route to a Dead Letter Queue. |
| **It simulates** | The order event bus that decouples the application from its downstream consumers. |
| **How it works** | Lambda polls SQS every 20 seconds, batches up to 10 messages per invocation, writes them as JSON to S3 Raw, then deletes successfully processed messages from the queue. |
| **Dirty data** | Duplicate messages from SQS at-least-once delivery, malformed JSON from application bugs, missing order amounts, cancelled orders arriving after shipped status. |
| **Bronze table** | `bronze.order_events` |

---

## Source 06 — Stripe API

**Every payment, refund, and dispute — pulled from the source of truth for revenue.**

**Ingestion Pattern**
```
┌──────────────┐  REST API    ┌──────────────┐  write JSON  ┌──────────────┐
│  Stripe API  │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│              │  paginated   │   MWAA       │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | REST API — paginated |
| **In the real world** | Stripe processes every card transaction. An Airflow DAG calls the Stripe REST API on schedule, pulls new payment records using cursor-based pagination, and writes them to S3. |
| **We simulate it with** | A generator that produces realistic Stripe API response JSON — charges with realistic success and failure rates, refunds with real reasons, disputes — in the same paginated structure the real API returns. |
| **It simulates** | The live payment data a finance team uses to reconcile revenue and that a data team uses to cross-check order totals against actual collections. |
| **How it works** | Airflow calls the Stripe API using a cursor from the last successful run, pages through all new records, and writes each page as a JSON file to S3 partitioned by date. |
| **Dirty data** | Duplicate charges from API retries, disputed amounts differing from original charge, missing customer IDs on guest checkouts, currency mismatches. |
| **Bronze table** | `bronze.stripe_raw` |

---

## Source 07 — ShipStation API

**Every shipment, tracking update, and delivery confirmation — the logistics layer of the business.**

**Ingestion Pattern**
```
┌──────────────┐  REST API    ┌──────────────┐  write JSON  ┌──────────────┐
│ ShipStation  │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│     API      │  modifyDate  │   MWAA       │              │   Landing    │
└──────────────┘  filter      └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | REST API — incremental |
| **In the real world** | ShipStation manages all outbound shipments. An Airflow DAG polls the ShipStation REST API hourly using the `modifyDate` filter to pull only records changed since the last run. |
| **We simulate it with** | A generator that produces realistic ShipStation API response JSON. A 5-day-old shipment shows as delivered. A 2-hour-old shipment shows as label created. Status always reflects the age of the record. |
| **It simulates** | The live logistics data a customer service team uses to answer delivery queries and that a data team uses to measure carrier performance. |
| **How it works** | Airflow calls the ShipStation API hourly with a modifyDate filter, pulls only records updated since the last run, and writes to S3 partitioned by carrier and date. |
| **Dirty data** | Duplicate tracking numbers from carrier reuse, missing weight data on some carriers, delivery timestamps before ship timestamps, invalid postal codes. |
| **Bronze table** | `bronze.shipstation_raw` |

---

## Source 08 — Shopify GraphQL

**The storefront — products, collections, inventory, and discounts exposed through a GraphQL API.**

**Ingestion Pattern**
```
┌──────────────┐  GraphQL     ┌──────────────┐  write JSON  ┌──────────────┐
│  Shopify API │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│              │  paginated   │   MWAA       │              │   Landing    │
└──────────────┘  cursor      └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | GraphQL API — cursor paginated |
| **In the real world** | Shopify exposes its data via a GraphQL API. An Airflow DAG queries it using cursor-based pagination, requesting only the fields needed. The response is deeply nested JSON. |
| **We simulate it with** | A generator that produces realistic Shopify GraphQL response JSON — products with nested variants, collections with rules, and discount codes with usage counts and expiry dates. |
| **It simulates** | The live storefront configuration that a merchandising team manages and that a data team uses to correlate what is shown on the website with what is actually selling. |
| **How it works** | Airflow sends GraphQL queries with a cursor, pages through all updated products, flattens the nested response, and writes to S3 partitioned by object type and date. |
| **Dirty data** | Null variant inventory levels, expired discount codes still marked active, products with no images, inconsistent metafield types across product categories. |
| **Bronze table** | `bronze.shopify_raw` |

---

## Source 09 — SFTP File Drop

**Supplier data delivered the old-fashioned way — a file dropped on a server every morning.**

**Ingestion Pattern**
```
┌──────────────┐  SFTP poll   ┌──────────────┐  upload      ┌──────────────┐
│   Supplier   │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│  CSV / Excel │  SFTPHook    │   MWAA       │  to S3       │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | File transfer — SFTP |
| **In the real world** | Many suppliers are not technology companies. They upload a CSV or Excel file every morning to a shared SFTP folder. An Airflow SFTPHook polls the server, downloads new files, and uploads to S3. Each supplier uses different column names and formats. |
| **We simulate it with** | A generator that produces realistic CSV and Excel files for 5 simulated suppliers, each with deliberately different column names and schemas. Files land in a local directory that maps to the SFTP drop zone. |
| **It simulates** | The real supplier data integration that exists in almost every retail business and produces the messiest data a Silver layer will encounter. |
| **How it works** | Airflow SFTPHook polls the server daily, downloads any new files not yet processed, uploads them to S3 Raw partitioned by supplier and date, then archives the originals. |
| **Dirty data** | Different column names per supplier, merged cells in Excel files, missing headers, mixed date formats, blank rows between data rows, currency symbols inside numeric fields. |
| **Bronze table** | `bronze.supplier_files` |

---

## Source 10 — Partner S3 Drop

**Partner data delivered as Parquet or Avro files — picked up automatically without a trigger.**

**Ingestion Pattern**
```
┌──────────────┐  S3           ┌──────────────┐  Autoloader  ┌──────────────┐
│  Partner S3  │──────────────▶│   S3 Raw     │─────────────▶│  Databricks  │
│  Bucket      │  replication  │   Landing    │  detects     │   Bronze     │
└──────────────┘  cross-account└──────────────┘  new files   └──────────────┘
```

| | |
|---|---|
| **Type** | File transfer — S3 to S3 |
| **In the real world** | Partners write Parquet or Avro files to their S3 bucket. S3 Cross-Account Replication copies them to the platform's raw bucket. Databricks Autoloader detects new files automatically — no Airflow trigger needed. |
| **We simulate it with** | A generator that produces realistic Parquet and Avro files written directly to the S3 raw prefix. Autoloader picks them up automatically. Schema is embedded in the file so no column definitions are needed in advance. |
| **It simulates** | The modern partner integration pattern where pipelines evolve automatically as partners add new columns. |
| **How it works** | Files land in S3 Raw. Databricks Autoloader monitors the prefix using S3 event notifications, detects new files within seconds, and ingests them into Bronze automatically. |
| **Dirty data** | Schema evolution between partner file versions, null join keys, overlapping date ranges between files, Avro files with missing schema registry entries. |
| **Bronze table** | `bronze.partner_raw` |

---

## Source 11 — ERP Export

**The financial backbone — purchase orders, invoices, and ledger entries exported nightly.**

**Ingestion Pattern**
```
┌──────────────┐  nightly     ┌──────────────┐  S3 sensor   ┌──────────────┐
│  ERP System  │─────────────▶│   S3 Raw     │─────────────▶│   Airflow    │
│  JSON / XML  │  file export │   Landing    │  triggers    │   → Bronze   │
└──────────────┘  ~2am        └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Semi-structured file — nightly batch |
| **In the real world** | ERP systems are often old and tightly controlled. Direct database access is rarely possible. The standard integration is a nightly file export at ~2am. An Airflow S3 sensor waits for the files and triggers a PySpark job that reads both JSON and XML formats. |
| **We simulate it with** | A generator that produces realistic JSON ledger files and XML invoice files with nightly timestamps, mimicking the output of a real ERP batch job. A manifest file with record counts is included for validation. |
| **It simulates** | The financial data export that every company with an ERP system produces and that a data engineer must ingest without touching the ERP system directly. |
| **How it works** | Airflow S3 sensor polls for the manifest file after 2am. Once detected it triggers a PySpark job that reads both JSON and XML files, validates record counts against the manifest, and writes to Bronze. |
| **Dirty data** | XML files with missing closing tags, JSON files truncated mid-record, ledger amounts that do not balance, invoice dates before purchase order dates, duplicate invoice numbers. |
| **Bronze table** | `bronze.erp_raw` |

---

## Source 12 — Reviews and Tickets

**The voice of the customer — unstructured text that tells you what the data cannot.**

**Ingestion Pattern**
```
┌──────────────┐  API poll    ┌──────────────┐  write JSON  ┌──────────────┐
│  Review API  │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│  Support API │  on schedule │   MWAA       │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Unstructured text |
| **In the real world** | Reviews come from the website review system. Tickets come from the support platform. An Airflow DAG pulls new records via API and writes them to S3 Raw exactly as received — no cleaning at this stage. |
| **We simulate it with** | A generator that produces realistic review and ticket text with realistic rating distributions, category patterns, typos, and truncated entries. Text is intentionally imperfect. |
| **It simulates** | The voice-of-customer data that a data team uses to build sentiment analysis and topic modelling in the Gold layer. |
| **How it works** | Airflow pulls new reviews and tickets via API on a schedule, writes raw text and metadata as JSON to S3 partitioned by type and date, with no transformation at this stage. |
| **Dirty data** | Truncated reviews, HTML tags inside review text, emoji in ticket subjects, null customer IDs on anonymous reviews, duplicate tickets from retry logic. |
| **Bronze table** | `bronze.text_raw` |

---

## Source 13 — S3 + Lambda (Image Metadata)

**Every product image upload triggers an event — metadata captured within milliseconds.**

**Ingestion Pattern**
```
┌──────────────┐  S3 event    ┌──────────────┐  write JSON  ┌──────────────┐
│  S3 Image    │─────────────▶│    Lambda    │─────────────▶│   S3 Raw     │
│  Upload      │  ObjectCreate│              │  via SQS     │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Event-driven — binary metadata |
| **In the real world** | When a product photo is uploaded to S3, an ObjectCreated event fires Lambda within milliseconds. Lambda reads the image properties and writes a JSON metadata message to SQS, which lands in S3 Raw. |
| **We simulate it with** | A generator that produces realistic image metadata JSON events in the same structure Lambda would produce — written directly to S3 Raw since there are no real image uploads to trigger the Lambda. |
| **It simulates** | The event-driven metadata pipeline that tracks the state of product imagery and enables detection of products missing images or with broken links. |
| **How it works** | S3 ObjectCreated event triggers Lambda. Lambda extracts filename, size, dimensions, and format. Writes JSON to S3 Raw via SQS within milliseconds of the upload. |
| **Dirty data** | Zero-byte files, unsupported image formats, missing product ID in filename, duplicate events from S3 event delivery retries, corrupted EXIF metadata. |
| **Bronze table** | `bronze.image_metadata` |

---

## Source 14 — Scrapy (Competitor Pricing)

**What competitors charge for the same products — scraped twice a day.**

**Ingestion Pattern**
```
┌──────────────┐  Scrapy      ┌──────────────┐  write JSON  ┌──────────────┐
│  Competitor  │─────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│  Websites    │  spiders     │   MWAA       │  S3 pipeline │   Landing    │
└──────────────┘  6am + 6pm   └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Web scraping |
| **In the real world** | Scrapy spiders are triggered by Airflow at 6am and 6pm. Each spider crawls its assigned competitor site, extracts pricing data, and writes JSON to S3 Raw via Scrapy's built-in S3 pipeline exporter. |
| **We simulate it with** | A generator that produces realistic competitor pricing JSON records — with realistic price variance, stock availability patterns, and occasional promotions — written to S3 Raw in the same structure a real Scrapy spider produces. |
| **It simulates** | The competitive intelligence pipeline that e-commerce pricing teams rely on to make daily pricing decisions. |
| **How it works** | Airflow triggers Scrapy spiders at 6am and 6pm. Each spider crawls its assigned site, extracts product names, prices, and stock status, and writes JSON directly to S3 Raw via the Scrapy S3 exporter. |
| **Dirty data** | Price strings with currency symbols, out-of-stock items with no price, HTML entities in product names, duplicate products from pagination overlap, prices in different currencies. |
| **Bronze table** | `bronze.competitor_pricing` |

---

## Source 15 — MQTT IoT Sensors

**Warehouse conditions monitored every 10 seconds — temperature, humidity, stock weight.**

**Ingestion Pattern**
```
┌──────────────┐  MQTT        ┌──────────────┐  Kafka topic ┌──────────────┐
│  IoT Sensors │─────────────▶│  Kafka MSK   │─────────────▶│   S3 Raw     │
│  every 10s   │  broker      │              │  Spark SS    │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Real-time IoT stream |
| **In the real world** | Physical IoT sensors publish readings via MQTT to a broker every 10 seconds. A Kafka bridge forwards all messages to MSK. Spark Structured Streaming consumes the topic with a 60-second watermark to handle sensor clock drift. |
| **We simulate it with** | A generator that produces realistic sensor readings at the correct cadence — one per sensor per 10 seconds — published to the MSK Kafka topic. Values follow realistic ranges with occasional anomalies and missing readings. |
| **It simulates** | The IoT telemetry pipeline that warehouse operations teams use to monitor conditions and that a data team uses to detect anomalies and trigger alerts. |
| **How it works** | Sensors publish to MQTT broker every 10 seconds. Kafka bridge forwards to MSK. Spark Structured Streaming consumes with a 60-second watermark and writes micro-batches to S3 Raw. |
| **Dirty data** | Sensor clock drift causing out-of-order events, null readings during sensor sleep cycles, impossible values like -999°C from sensor faults, duplicate readings from MQTT retry logic. |
| **Bronze table** | `bronze.iot_telemetry` |

---

## Source 16 — CloudWatch Logs

**Every application error, access log, and Lambda execution — streamed continuously.**

**Ingestion Pattern**
```
┌──────────────┐  subscription ┌──────────────┐  gzip batch  ┌──────────────┐
│  CloudWatch  │──────────────▶│   Kinesis    │─────────────▶│   S3 Raw     │
│  Logs        │  filter       │   Firehose   │  every 60s   │   Landing    │
└──────────────┘               └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Application log stream |
| **In the real world** | AWS services write logs to CloudWatch automatically. A subscription filter streams log lines to Kinesis Firehose, which writes gzip-compressed batches to S3 Raw every 60 seconds. Airflow triggers hourly decompression and Bronze ingestion. |
| **We simulate it with** | A generator that produces realistic log lines — structured JSON application logs, HTTP access logs, and error logs with stack traces — written to S3 Raw in gzip-compressed batches matching the Kinesis Firehose output format. |
| **It simulates** | The operational log pipeline that engineering teams use to investigate incidents and that data teams use to build service observability dashboards. |
| **How it works** | CloudWatch subscription filter streams to Kinesis Firehose. Firehose buffers for 60 seconds and writes gzip-compressed batches to S3 Raw. Airflow triggers hourly PySpark decompression and Bronze ingestion. |
| **Dirty data** | Mixed log formats within the same file, truncated stack traces, null request IDs, log lines exceeding maximum length, binary characters in error messages. |
| **Bronze table** | `bronze.app_logs` |

---

## Source 17 — GA4 Export

**Web sessions, funnels, and acquisition data — always 24 hours behind by design.**

**Ingestion Pattern**
```
┌──────────────┐  daily export ┌──────────────┐  write JSON  ┌──────────────┐
│  GA4         │──────────────▶│   Airflow    │─────────────▶│   S3 Raw     │
│  BigQuery    │  previous day │   MWAA       │              │   Landing    │
└──────────────┘               └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | SaaS analytics export |
| **In the real world** | GA4 exports data to BigQuery automatically once per day. An Airflow DAG calls the BigQuery export API to download the previous day's events as JSON and uploads to S3 Raw. Data is always 24 hours behind — GA4 is not a real-time source. |
| **We simulate it with** | A generator that produces realistic GA4 event JSON in the exact BigQuery export schema — including the nested `event_params` key-value array structure. One JSON file per day written to S3 Raw. |
| **It simulates** | The web analytics export that marketing teams use to measure acquisition and that data teams join with order data to build full attribution models. |
| **How it works** | Airflow DAG runs at 6am daily, calls the BigQuery export API for yesterday's data, downloads the nested JSON, and writes to S3 Raw partitioned by date. |
| **Dirty data** | Null session IDs on direct traffic, missing campaign parameters on paid clicks, event_params with inconsistent value types, sampled data on high-traffic days. |
| **Bronze table** | `bronze.ga4_raw` |

---

## Source 18 — AWS SES Email

**Every open, click, bounce, and complaint — the health signal of the email channel.**

**Ingestion Pattern**
```
┌──────────────┐  SNS trigger ┌──────────────┐  write JSON  ┌──────────────┐
│  AWS SES     │─────────────▶│    Lambda    │─────────────▶│   S3 Raw     │
│  Events      │              │              │              │   Landing    │
└──────────────┘              └──────────────┘              └──────────────┘
```

| | |
|---|---|
| **Type** | Email event stream |
| **In the real world** | SES fires a notification to SNS for every email event. SNS triggers Lambda, which formats the event as JSON and writes to S3 Raw. Airflow ingests S3 Raw to Bronze on an hourly schedule. |
| **We simulate it with** | A generator that produces realistic SES email event JSON — with realistic open rates (~40%), click rates (~15%), bounce rates (~2%), and complaint rates (~0.1%) — written to S3 Raw in the same structure Lambda would produce. |
| **It simulates** | The email engagement pipeline that CRM teams use to manage deliverability and that data teams join with purchase data to measure the revenue impact of email campaigns. |
| **How it works** | SES fires events to SNS. SNS triggers Lambda. Lambda formats and writes JSON to S3 Raw. Airflow ingests hourly to Bronze partitioned by event type and date. |
| **Dirty data** | Duplicate events from SNS at-least-once delivery, bounces without bounce reason codes, clicks with malformed URLs, complaint events missing recipient addresses. |
| **Bronze table** | `bronze.email_events` |

---

## Data Quality

Every generator produces intentionally imperfect data by default — approximately 1–2% malformed records, 5% missing optional fields, and realistic failure states. Run with `--dirty` to increase noise significantly for Silver pipeline cleaning practice. Bad records are never silently dropped — every bad record is quarantined, counted, and tracked over time.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values. No connection strings are hardcoded anywhere in this project.

```bash
cp .env.example .env
```
