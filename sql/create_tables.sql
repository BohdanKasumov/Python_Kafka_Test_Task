CREATE DATABASE IF NOT EXISTS dwh;

USE dwh;

CREATE TABLE IF NOT EXISTS dim_sku (
    sku_id VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id VARCHAR(50) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    receipt_id VARCHAR(50),
    store_id VARCHAR(50),
    date_id DATE,
    sku_id VARCHAR(50),
    brand VARCHAR(100),
    quantity INT,
    price DECIMAL(10,2),
    discount DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    compensation_percent DECIMAL(5,2),
    revenue DECIMAL(10,2),
    cost DECIMAL(10,2),
    compensation DECIMAL(10,2),
    profit DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    PRIMARY KEY (receipt_id, sku_id),
    FOREIGN KEY (sku_id) REFERENCES dim_sku(sku_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE IF NOT EXISTS staging_receipts (
    receipt_id VARCHAR(50),
    store_id VARCHAR(50),
    datetime DATETIME,
    sku_id VARCHAR(50),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    quantity INT,
    discount DECIMAL(10,2),
    promotion_id VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS sales_dashboard_today (
    brand VARCHAR(100),
    total_sales DECIMAL(15,2),
    total_discounts DECIMAL(15,2),
    total_cost DECIMAL(15,2),
    total_compensation DECIMAL(15,2),
    total_profit DECIMAL(15,2),
    profit_margin DECIMAL(5,2),
    PRIMARY KEY (brand)
);

CREATE TABLE IF NOT EXISTS sku_costs (
    sku_id VARCHAR(50),
    cost_price DECIMAL(10,2),
    updated_at DATETIME,
    PRIMARY KEY (sku_id, updated_at)
);

CREATE TABLE IF NOT EXISTS sku_compensations (
    sku_id VARCHAR(50) PRIMARY KEY,
    compensation_percent DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS promotions (
    promotion_id VARCHAR(50),
    sku_id VARCHAR(50),
    discount_value DECIMAL(10,2),
    start_date DATETIME,
    end_date DATETIME,
    PRIMARY KEY (promotion_id, sku_id)
);