# Retail Sales Pipeline

## Description

Pipeline for execution data for retail from kafka, validation, load to DWH(MySQL) and creation incremental dashboard.

## Get started

1. Install dependencies: `pip install -r requirements.txt`
2. Enter config(kafka url and mysql url) `config/config.yaml`
3. Create tables: `python scripts/create_db.py`
4. Run consumer: `python scripts/kafka_consumer.py`
5. Load data to DWH: `python scripts/dwh_loader.py`
6. Create dashboard: `python scripts/dashboard_builder.py`

## Requirements

- Kafka
- MySQL 8.0+
- Python 3.8+