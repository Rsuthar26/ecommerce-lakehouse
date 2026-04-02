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

## All 18 Sources

| # | Source | In the real world | Here we used | It is a simulation of |
|---|--------|-------------------|--------------|----------------------|
| 01 | **RDS PostgreSQL** | A live PostgreSQL database written to by the application every time a customer places an order, makes a payment, or updates their account. The data engineer reads from it — never writes to it. | A generator that inserts realistic customers, orders, payments, and inventory rows into PostgreSQL with backdated timestamps and peak-hour patterns. | The live operational database at the core of every real e-commerce business. |
| 02 | **Debezium CDC** | Debezium runs as a Kafka Connect connector, reading PostgreSQL's internal change log (WAL) and converting every INSERT, UPDATE, and DELETE into a real-time Kafka event — without touching the application. | The real Debezium connector deployed on EC2 pointed at Source 01. Source 01 produces the database writes; Debezium captures them automatically. No separate generator needed. | The real-time change stream that keeps a data lake in sync with the operational database without polling it. |
| 03 | **MongoDB Atlas** | A NoSQL document database holding the product catalogue. Products have nested variants, images, and attributes. An Airflow DAG polls MongoDB on schedule and exports changed products to S3. | A generator that writes realistic nested product documents to a real MongoDB Atlas M0 cluster using PyMongo, with arrays of variants, images, and realistic field variation between products. | A live product catalogue database that a merchandising team manages and updates daily. |
| 04 | **MSK Kafka Clickstream** | The website fires a Kafka event for every user interaction — page view, product click, add to cart, checkout step. These flow into MSK at hundreds of thousands per day and are consumed by Spark Structured Streaming. | A generator that publishes realistic clickstream JSON events to a real MSK Kafka topic at the correct cadence — ~6 events per second average, with peaks during simulated business hours. | The live behavioural stream that every large e-commerce platform produces as customers browse. |
| 05 | **AWS SQS** | When an order is placed the application puts a message on SQS. Multiple downstream systems each consume their own copy. If a consumer is temporarily down, messages wait safely in the queue. | A generator that produces order event JSON messages and puts them onto a real SQS queue. Lambda polls the queue, writes to S3 Raw, and deletes processed messages. Failed messages route to a Dead Letter Queue. | The order event bus that decouples the application from its downstream consumers. |
| 06 | **Stripe API** | Stripe processes every card transaction. An Airflow DAG calls the Stripe REST API on schedule, pulls new payment records using cursor-based pagination, and writes them to S3. | A generator that produces realistic Stripe API response JSON — charges with realistic success and failure rates, refunds with real reasons, disputes — in the same paginated structure the real API returns. | The live payment data a finance team uses to reconcile revenue and that a data team uses to cross-check order totals against actual collections. |
| 07 | **ShipStation API** | ShipStation manages all outbound shipments. An Airflow DAG polls the ShipStation REST API hourly using the `modifyDate` filter to pull only records changed since the last run. | A generator that produces realistic ShipStation API response JSON. A 5-day-old shipment shows as delivered. A 2-hour-old shipment shows as label created. Status always reflects the age of the record. | The live logistics data a customer service team uses to answer delivery queries and that a data team uses to measure carrier performance. |
| 08 | **Shopify GraphQL** | Shopify exposes its data via a GraphQL API. An Airflow DAG queries it using cursor-based pagination, requesting only the fields needed. The response is deeply nested JSON. | A generator that produces realistic Shopify GraphQL response JSON — products with nested variants, collections with rules, and discount codes with usage counts and expiry dates. | The live storefront configuration that a merchandising team manages and that a data team uses to correlate what is shown on the website with what is actually selling. |
| 09 | **SFTP Drop** | Many suppliers are not technology companies. They upload a CSV or Excel file every morning to a shared SFTP folder. An Airflow SFTPHook polls the server, downloads new files, and uploads to S3. Each supplier uses different column names and formats. | A generator that produces realistic CSV and Excel files for 5 simulated suppliers, each with deliberately different column names and schemas. Files land in a local directory that maps to the SFTP drop zone. | The real supplier data integration that exists in almost every retail business and produces the messiest data a Silver layer will encounter. |
| 10 | **Partner S3 Drop** | Partners write Parquet or Avro files to their S3 bucket. S3 Cross-Account Replication copies them to the platform's raw bucket. Databricks Autoloader detects new files automatically — no Airflow trigger needed. | A generator that produces realistic Parquet and Avro files written to the S3 raw prefix. Autoloader picks them up automatically. Schema is embedded in the file so no column definitions are needed in advance. | The modern partner integration pattern where pipelines evolve automatically as partners add new columns. |
| 11 | **ERP Export** | ERP systems are often old and tightly controlled. Direct database access is rarely possible. The standard integration is a nightly file export at ~2am. An Airflow S3 sensor waits for the files and triggers a PySpark job that reads both JSON and XML formats. | A generator that produces realistic JSON ledger files and XML invoice files with nightly timestamps, mimicking the output of a real ERP batch job. A manifest file with record counts is included for validation. | The financial data export that every company with an ERP system produces and that a data engineer must ingest without touching the ERP system directly. |
| 12 | **Reviews and Tickets** | Reviews come from the website review system. Tickets come from the support platform. An Airflow DAG pulls new records via API and writes them to S3 Raw exactly as received — no cleaning at this stage. | A generator that produces realistic review and ticket text with realistic rating distributions, category patterns, typos, and truncated entries. Text is intentionally imperfect. | The voice-of-customer data that a data team uses to build sentiment analysis and topic modelling in the Gold layer. |
| 13 | **S3 + Lambda (Image Metadata)** | When a product photo is uploaded to S3, an ObjectCreated event fires Lambda within milliseconds. Lambda reads the image properties and writes a JSON metadata message to SQS, which lands in S3 Raw. | A generator that produces realistic image metadata JSON events in the same structure Lambda would produce — written directly to S3 Raw since there are no real image uploads to trigger the Lambda. | The event-driven metadata pipeline that tracks the state of product imagery and enables detection of products missing images or with broken links. |
| 14 | **Scrapy (Competitor Pricing)** | Scrapy spiders are triggered by Airflow at 6am and 6pm. Each spider crawls its assigned competitor site, extracts pricing data, and writes JSON to S3 Raw via Scrapy's built-in S3 pipeline exporter. | A generator that produces realistic competitor pricing JSON records — with realistic price variance, stock availability patterns, and occasional promotions — written to S3 Raw in the same structure a real Scrapy spider produces. | The competitive intelligence pipeline that e-commerce pricing teams rely on to make daily pricing decisions. |
| 15 | **MQTT IoT Sensors** | Physical IoT sensors publish readings via MQTT to a broker every 10 seconds. A Kafka bridge forwards all messages to MSK. Spark Structured Streaming consumes the topic with a 60-second watermark to handle sensor clock drift. | A generator that produces realistic sensor readings at the correct cadence — one per sensor per 10 seconds — published to the MSK Kafka topic. Values follow realistic ranges with occasional anomalies and missing readings. | The IoT telemetry pipeline that warehouse operations teams use to monitor conditions and that a data team uses to detect anomalies and trigger alerts. |
| 16 | **CloudWatch Logs** | AWS services write logs to CloudWatch automatically. A subscription filter streams log lines to Kinesis Firehose, which writes gzip-compressed batches to S3 Raw every 60 seconds. Airflow triggers hourly decompression and Bronze ingestion. | A generator that produces realistic log lines — structured JSON application logs, HTTP access logs, and error logs with stack traces — written to S3 Raw in gzip-compressed batches matching the Kinesis Firehose output format. | The operational log pipeline that engineering teams use to investigate incidents and that data teams use to build service observability dashboards. |
| 17 | **GA4 Export** | GA4 exports data to BigQuery automatically once per day. An Airflow DAG calls the BigQuery export API to download the previous day's events as JSON and uploads to S3 Raw. Data is always 24 hours behind — GA4 is not a real-time source. | A generator that produces realistic GA4 event JSON in the exact BigQuery export schema — including the nested `event_params` key-value array structure. One JSON file per day written to S3 Raw. | The web analytics export that marketing teams use to measure acquisition and that data teams join with order data to build full attribution models. |
| 18 | **AWS SES Email** | SES fires a notification to SNS for every email event. SNS triggers Lambda, which formats the event as JSON and writes to S3 Raw. Airflow ingests S3 Raw to Bronze on an hourly schedule. | A generator that produces realistic SES email event JSON — with realistic open rates (~40%), click rates (~15%), bounce rates (~2%), and complaint rates (~0.1%) — written to S3 Raw in the same structure Lambda would produce. | The email engagement pipeline that CRM teams use to manage deliverability and that data teams join with purchase data to measure the revenue impact of email campaigns. |

---

## Data Quality

Every generator produces intentionally imperfect data by default — approximately 1–2% malformed records, 5% missing optional fields, and realistic failure states. Run with `--dirty` to increase noise significantly for Silver pipeline cleaning practice. Bad records are never silently dropped — every bad record is quarantined, counted, and tracked over time.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values. No connection strings are hardcoded anywhere in this project.

```bash
cp .env.example .env
```
