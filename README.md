# High-Throughput Streaming Lakehouse with Apache Kafka & Iceberg

An end-to-end, distributed streaming data lakehouse pipeline architected to ingest high-frequency financial event telemetry, enforce schema constraints, isolate data quality violations via a Dead Letter Queue (DLQ), and commit ACID transactions directly to Apache Iceberg tables backed by MinIO S3 object storage.

---

## Architecture Overview

```text
               +----------------------------------+
               | Synthetic Transaction Stream     |
               | (Faker, Pydantic, Chaos Injector)|
               +-----------------+----------------+
                                 |
                                 |  JSON Events (~5 events/sec)
                                 v
               +----------------------------------+
               |     Kafka / Redpanda Broker      |
               | Topic: `financial_transactions`  |
               +-----------------+----------------+
                                 |
                                 |  Continuous Event Stream
                                 v
               +----------------------------------+
               |  PySpark Structured Streaming    |
               |  - Schema Enforcement & Parsing  |
               |  - Event-Time Watermarking (10s) |
               |  - Quality Audit & Stream Branch |
               +--------+----------------+--------+
                        |                |
           Valid Records|                | Malformed / Corrupt
                        v                v
     +-----------------------+    +-----------------------+
     | Apache Iceberg Table  |    | Dead Letter Queue     |
     | `lakehouse.db.        |    | `lakehouse.db.        |
     |  transactions`        |    |  dlq_events`          |
     | PARTITIONED BY        |    | Quarantined with      |
     | (currency, tx_date)   |    | error audit tags      |
     +-----------+-----------+    +-----------+-----------+
                 |                            |
                 +-------------+--------------+
                               |
                               v
               +----------------------------------+
               |  MinIO S3-Compatible Storage     |
               | Bucket: s3a://warehouse/iceberg  |
               | (Parquet Data, Manifests, State) |
               +----------------------------------+


Engineering Highlights
ACID Operations on Object Storage: Utilizes Apache Iceberg's hierarchical metadata architecture (snapshots, manifest lists, manifest files) over S3A to provide serializable snapshot isolation and zero-copy time travel.

Dead Letter Queue (DLQ) Stream Splitting: Implements real-time data governance by branching invalid records (e.g., negative amounts, missing currencies) into a quarantined audit table without interrupting stream consumption.

Dynamic Partition Pruning: Ingested transactions are partitioned by currency and tx_date, eliminating redundant file scans during analytical aggregations.

Stateful Watermarking: Applied a 10-second watermark against transaction event timestamps to handle late-arriving network packets while bounding Spark state-store memory usage.

Containerized Lakehouse Infrastructure: Fully portable environment powered by Docker Compose, encapsulating Redpanda (Kafka) and MinIO object storage.


Repository Structure
streaming-lakehouse-pipeline/
├── config/                  # Configuration parameters & environment profiles
├── docker/
│   └── docker-compose.yml   # Multi-container orchestration (Redpanda & MinIO)
├── docs/                    # Architecture diagrams and design documentation
├── producers/
│   └── event_producer.py    # High-throughput mock generator with anomaly injection
├── spark/
│   ├── streaming_processor.py # PySpark Structured Streaming & Iceberg sink pipeline
│   └── verify_lakehouse.py  # Analytical query engine & ACID snapshot verifier
├── tests/                   # Pipeline validation tests
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt


Tech Stack & Prerequisites
Layer                   Technology
Stream Ingestion        Redpanda (Apache Kafka-compatible C++ broker)
Stream Engine           Apache Spark 3.5.3 (PySpark Structured Streaming)
Table Format            Apache Iceberg 1.6.1
Object Store            MinIO (AWS S3-Compatible Distributed Storage)
Runtime & SDKs          Python 3.10+, Java OpenJDK 17, Boto3, Faker
Infrastructure          Docker Desktop & Docker Compose


Reproduction & Setup Guide
1. Environment Setup
Clone the repository and initialize a virtual environment:

PowerShell
git clone [https://github.com/Ankur-Singhh/streaming-lakehouse-pipeline.git](https://github.com/Ankur-Singhh/streaming-lakehouse-pipeline.git)
cd streaming-lakehouse-pipeline

python -m venv venv
.\venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
2. Boot Infrastructure
Start the Kafka broker and MinIO object store using Docker:

PowerShell
docker compose -f docker\docker-compose.yml up -d
MinIO Web Console: http://localhost:9001 (Credentials: admin / password123)

Redpanda Broker Endpoint: localhost:9092

3. Launch Stream Ingestion Engine
Execute the PySpark Structured Streaming application:

PowerShell
python spark\streaming_processor.py
4. Emit High-Throughput Stream
Open a second terminal window, activate the virtual environment, and run the transaction producer:

PowerShell
.\venv\Scripts\Activate.ps1
python producers\event_producer.py
5. Verify ACID Snapshots & Run Queries
In a separate terminal, execute analytical validations against the Iceberg tables:

PowerShell
python spark\verify_lakehouse.py
Live Verification & Snapshot Audits
Below are verifiable execution metrics retrieved directly from the Iceberg catalog after streaming ingestion:


1. Incremental ACID Snapshots Log
+-----------------------+-------------------+---------+-------------+
|committed_at           |snapshot_id        |operation|total_records|
+-----------------------+-------------------+---------+-------------+
|2026-11-11 16:49:31.526|5644616752402229918|append   |301          |
|2026-11-11 16:49:20.536|3015542045416483857|append   |295          |
|2026-11-11 16:49:11.093|2717698749597742247|append   |248          |
|2026-11-11 16:39:25.841|3208438484994133904|append   |212          |
+-----------------------+-------------------+---------+-------------+


2. Aggregations Across Partitions
+--------+-----+---------+-------+
|currency|count|total_vol|avg_amt|
+--------+-----+---------+-------+
|INR     |61   |44632.35 |731.68 |
|USD     |89   |63714.06 |715.89 |
|GBP     |77   |52739.43 |684.93 |
|EUR     |74   |57017.65 |770.51 |
+--------+-----+---------+-------+


3. Dead Letter Queue Quarantine Records
+------------------------------------+--------+-------+--------+-----------------------+
|transaction_id                      |user_id |amount |currency|rejection_reason       |
+------------------------------------+--------+-------+--------+-----------------------+
|c0a2b8a1-5704-4f30-8da2-052290ef12ec|USR_2648|-301.74|NULL    |NEGATIVE_OR_ZERO_AMOUNT|
|a4527385-5a40-4ffb-8c7d-258eb1794847|USR_7367|-150.54|NULL    |NEGATIVE_OR_ZERO_AMOUNT|
|0fd265f5-e4a5-4a3f-971e-2d2860fbf33d|USR_9485|-205.06|NULL    |NEGATIVE_OR_ZERO_AMOUNT|
+------------------------------------+--------+-------+--------+-----------------------+