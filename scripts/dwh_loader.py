from sqlalchemy import create_engine, text
import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')
SQL_PATH = os.path.join(BASE_DIR, 'sql', 'enrich_sales.sql')

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

engine = create_engine(config['database']['url'])

def populate_dim_sku():
    insert_sku_query = """
    INSERT INTO dim_sku (sku_id, brand)
    SELECT DISTINCT sku_id, brand
    FROM staging_receipts
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_sku WHERE dim_sku.sku_id = staging_receipts.sku_id
    );
    """
    with engine.connect() as conn:
        conn.execute(text(insert_sku_query))
        conn.commit()
    print("Enter data to table dim_sku")

def populate_dim_store():
    insert_store_query = """
    INSERT INTO dim_store (store_id)
    SELECT DISTINCT store_id
    FROM staging_receipts
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_store WHERE dim_store.store_id = staging_receipts.store_id
    );
    """
    with engine.connect() as conn:
        conn.execute(text(insert_store_query))
        conn.commit()
    print("Enter data to table dim_store")

def populate_dim_date():
    insert_date_query = """
    INSERT INTO dim_date (date_id, day, month, year, weekday)
    SELECT DISTINCT DATE(datetime) AS date_id,
           DAY(datetime) AS day,
           MONTH(datetime) AS month,
           YEAR(datetime) AS year,
           DAYNAME(datetime) AS weekday
    FROM staging_receipts
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_date WHERE dim_date.date_id = DATE(staging_receipts.datetime)
    );
    """
    with engine.connect() as conn:
        conn.execute(text(insert_date_query))
        conn.commit()
    print("Enter data to table dim_date")

def load_to_dwh():
    with open(SQL_PATH, 'r') as file:
        enrich_query = file.read()
    with engine.connect() as conn:
        with conn.begin():
            populate_dim_sku()
            populate_dim_store()
            populate_dim_date()
            for statement in enrich_query.split(';'):
                if statement.strip():
                    try:
                        result = conn.execute(text(statement))
                        if result.returns_rows:
                            result.fetchall()
                    except Exception as e:
                        print(f"Error while executing statement: {statement[:50]}... | Error: {e}")
                        raise
        print("Data entered in fact_sales")

if __name__ == "__main__":
    load_to_dwh()