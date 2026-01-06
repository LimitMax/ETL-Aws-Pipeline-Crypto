# AWS Crypto Data Pipeline

This project demonstrates an **industry-grade end-to-end data engineering pipeline on AWS**
using a **Bronze–Silver–Gold data lake architecture**.

The pipeline ingests cryptocurrency market data, processes it in batch, and exposes
business-ready datasets for analytics using Amazon Athena.

---

## 🏗️ Architecture Overview
EventBridge (Hourly)
↓
AWS Lambda (Bronze Ingestion)
↓
Amazon S3 (Bronze - Raw Events)
↓
Batch Processing (Silver)
↓
Amazon S3 (Silver - Parquet, Partitioned)
↓
Daily Aggregation (Gold)
↓
Amazon S3 (Gold - Parquet, Business-Ready)
↓
Amazon Athena (Analytics & Query)


---

## 🔧 Technology Stack

- **AWS Lambda** – Event-driven bronze ingestion
- **Amazon EventBridge** – Hourly scheduling
- **Amazon S3** – Data lake storage (bronze / silver / gold)
- **Amazon Athena** – Ad-hoc analytics on Parquet data
- **Python** – ETL logic (batch processing)
- **Pandas / PyArrow** – Parquet generation and aggregation
- **IAM Roles & OIDC** – Secure authentication (no credentials in code)
- **GitHub Actions** – CI/CD for Lambda deployment

---

## 🥉 Bronze Layer (Raw Ingestion)

**Purpose**
- Capture ingestion events and metadata
- Lightweight, append-only, no transformation

**Characteristics**
- Triggered hourly via EventBridge
- Implemented using AWS Lambda
- Stores raw ingestion metadata in S3

**Design Choice**
> Lambda is intentionally limited to lightweight ingestion to avoid
dependency size limits and long execution times.

---

## 🥈 Silver Layer (Clean & Structured Data)

**Purpose**
- Transform raw ingestion events into structured OHLCV market data
- Enforce schema consistency and deduplication

**Key Features**
- Batch processing (not event-driven)
- OHLCV data at **hourly granularity**
- Stored as **Parquet with partitioning**:

- Optimized for analytics and downstream processing

**Why Batch?**
> Batch processing is more cost-efficient and easier to manage for
transformations and deduplication compared to per-event execution.

---

## 🥇 Gold Layer (Business-Ready Aggregation)

**Purpose**
- Provide analytics-ready, business-focused datasets

**Current Gold Dataset**
- **Daily OHLCV aggregation per symbol**

**Gold Characteristics**
- Aggregated from Silver (never from Bronze)
- Stored as **partitioned Parquet**
- Optimized for reporting and dashboards

---

## 📊 Analytics Layer

- **Amazon Athena** is used to query both Silver and Gold datasets
- Separate Athena databases are used per layer:
- `crypto_analytics` → Silver (technical layer)
- `crypto_gold` → Gold (business layer)

This separation ensures clear data ownership and prevents mixing
technical and business concerns.

---

## 🔐 Security & Best Practices

- No AWS credentials stored in code or repository
- IAM Role-based access for AWS services
- Local development uses IAM users with least-privilege access
- Clear separation between ingestion, processing, and analytics layers

---

## 🚀 Current Status

- ✅ AWS account, budget, and IAM (least privilege) configured
- ✅ S3 data lake (bronze / silver / gold) implemented
- ✅ Bronze ingestion Lambda deployed via CI/CD
- ✅ Silver batch processing implemented (Parquet + partitioned)
- ✅ Gold daily aggregation implemented (Parquet + partitioned)
- ✅ Athena successfully querying Silver and Gold datasets
- ⏳ AWS Glue Data Catalog & scheduled Glue Jobs (planned)

---

## 📌 Future Enhancements

- Register Silver & Gold datasets in **AWS Glue Data Catalog**
- Run Silver & Gold pipelines as **scheduled Glue Jobs**
- Add data quality checks and monitoring
- Integrate BI visualization (e.g. QuickSight)

---

## 🧠 Key Takeaway

> This project focuses on **realistic, production-oriented design decisions**
rather than forcing all logic into serverless functions, demonstrating
how modern data platforms are built and operated in practice.
