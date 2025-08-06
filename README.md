# Retail Sales Pipeline

## Description

Pipeline for execution data for retail from kafka, validation, load to DWH(PostgreSQL) and creation incremental dashboard.

## Get started

1. Install dependencies: `pip install -r requirements.txt`
2. Enter config(kafka url and mysql url) `config/config.yaml`
3. Create tables: `python scripts/create_db.py`
4. Run consumer: `python scripts/kafka_consumer.py`
5. Load data to DWH: `python scripts/dwh_loader.py`
6. Create dashboard: `python scripts/dashboard_builder.py`

## Requirements

- Kafka
- PostgreSQL 15+
- Python 3.8+

## Goal

- Get a receipt stream from Kafka
- Clean and validate data
- Write data to DWH (Star Schema)
- Enrich transactions with cost price and compensation
- Build an aggregated sales showcase for the current day

---

## Input data

### Kafka Topic: `receipts_raw`

Message example:
```json
{
"receipt_id": "R123456",
"store_id": "S001",
"datetime": "2025-07-31T12:45:00",
"items": [
{ "sku_id": "100234", "brand": "Dove", "price": 75.00, "quantity": 2, "discount": 10.0 },
{ "sku_id": "100245", "brand": "Arko", "price": 120.00, "quantity": 1, "discount": 0.0 }
]
}
```

### Tables for enrichment:

1. `sku_costs`:
- `sku_id`, `cost_price`, `updated_at`
- Use the latest cost price by `sku_id`

2. `sku_compensations`:
- `sku_id`, `compensation_percent`
- Compensation = `cost_price × (compensation_percent / 100) × quantity`

---

## Task

### 1. Kafka Consumer (Python)
- Implemented a consumer that reads from the Kafka topic `receipts_raw`
- Decompresses JSON by item-ам
- Saves data to a staging table in PostgreSQL

---

### 2. Cleaning and validating data

Before loading into DWH:

- Skipping messages without `receipt_id`, `store_id`, `datetime`, `items`
- Excluding items with:
- `sku_id` is empty
- `price <= 0` or `quantity <= 0`
- `discount < 0` or `discount > price`
- Checking date format (`datetime`)
- Ignoring transactions older than 7 days or with a date in the future
- Excluding duplicates by `receipt_id + sku_id`, if any

---

### 3. Dynamic discounts

Some products may not contain a direct `discount` value, but contain a `promotion_id`.
In this case:
- The `promotions` table is used: `promotion_id`, `sku_id`, `discount_value`, `start_date`, `end_date`
- Discounts are taken from `promotions` if it is active at the time of purchase
- If `discount` is present in `items`, it is a priority

---

### 4. DWH (Star Schema) model on Postgre or SQL

Tables created:
- `fact_sales`: `receipt_id`, `store_id`, `datetime`, `sku_id`, `brand`, `quantity`, `price`, `discount`
- `dim_sku`, `dim_store`, `dim_date` — at your discretion

---

### 4. Enrichment

Added to `fact_sales`:
- `cost_price` from `sku_costs` by latest date
- `compensation_percent` from `sku_compensations`

Calculated:
- `revenue = (price - discount) * quantity`
- `cost = cost_price * quantity`
- `compensation = compensation_percent / 100 * cost`
- `profit = revenue - cost + compensation`
- `profit_margin = profit / revenue`

---

### 5. Sales Dashboard: `sales_dashboard_today` (Incremental)

Build a dashboard with aggregation by **brands for the current day** with the following metrics:
- `brand`
- `total_sales`
- `total_discounts`
- `total_cost`
- `total_compensation`
- `total_profit`
- `profit_margin`

The dashboard should be **incremental**:
- Table is not deleted or recreated
- Only new data is added
- If the brand already exists, aggregates are recalculated only for it

---

A showcase with aggregation by **brands for the current day** with the following metrics is built:
- `brand`
- `total_sales`
- `total_discounts`
- `total_cost`
- `total_compensation`
- `total_profit`
- `profit_margin`