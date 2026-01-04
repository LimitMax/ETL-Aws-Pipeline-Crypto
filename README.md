# AWS Crypto Data Pipeline

This project demonstrates an industry-grade data engineering pipeline on AWS
using a Bronze–Silver–Gold architecture.

## Architecture
- **AWS Lambda**: Lightweight bronze ingestion
- **Amazon S3**: Data lake (bronze / silver / gold)
- **AWS Glue**: Batch ETL for silver & gold layers (planned)

## Design Principles
- Lambda is limited to lightweight ingestion only
- Batch processing and deduplication are handled outside Lambda
- IAM Role-based security (no credentials in code)

## Current Status
- ✅ Bronze ingestion Lambda implemented
- ⏳ Silver & Gold layers in progress
