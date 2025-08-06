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
    price NUMERIC(10,2),
    discount NUMERIC(10,2),
    cost_price NUMERIC(10,2),
    compensation_percent NUMERIC(5,2),
    revenue NUMERIC(10,2),
    cost NUMERIC(10,2),
    compensation NUMERIC(10,2),
    profit NUMERIC(10,2),
    profit_margin NUMERIC(5,2),
    PRIMARY KEY (receipt_id, sku_id),
    FOREIGN KEY (sku_id) REFERENCES dim_sku(sku_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE IF NOT EXISTS staging_receipts (
    receipt_id VARCHAR(50),
    store_id VARCHAR(50),
    datetime TIMESTAMP,
    sku_id VARCHAR(50),
    brand VARCHAR(100),
    price NUMERIC(10,2),
    quantity INT,
    discount NUMERIC(10,2),
    promotion_id VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS sales_dashboard_today (
    brand VARCHAR(100),
    total_sales NUMERIC(15,2),
    total_discounts NUMERIC(15,2),
    total_cost NUMERIC(15,2),
    total_compensation NUMERIC(15,2),
    total_profit NUMERIC(15,2),
    profit_margin NUMERIC(5,2),
    PRIMARY KEY (brand)
);

CREATE TABLE IF NOT EXISTS sku_costs (
    sku_id VARCHAR(50),
    cost_price NUMERIC(10,2),
    updated_at TIMESTAMP,
    PRIMARY KEY (sku_id, updated_at)
);

CREATE TABLE IF NOT EXISTS sku_compensations (
    sku_id VARCHAR(50) PRIMARY KEY,
    compensation_percent NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS promotions (
    promotion_id VARCHAR(50),
    sku_id VARCHAR(50),
    discount_value NUMERIC(10,2),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    PRIMARY KEY (promotion_id, sku_id)
);