from sqlalchemy import create_engine, text
import os
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')
SQL_PATH = os.path.join(BASE_DIR, 'sql', 'create_tables.sql')

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

engine = create_engine(config['database']['url'].replace('/dwh', ''))

def check_database_exists(engine):
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'dwh'"
        )).scalar()
        return result > 0

def check_table_exists(engine, table_name):
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dwh' AND table_name = :table_name"
        ), {"table_name": table_name}).scalar()
        return result > 0

def execute_sql_script():
    required_tables = [
        'dim_sku', 'dim_store', 'dim_date', 'fact_sales',
        'staging_receipts', 'sales_dashboard_today',
        'sku_costs', 'sku_compensations', 'promotions'
    ]

    if not check_database_exists(engine):
        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE dwh"))
            conn.commit()
        print("DB dwh created")

    updated_engine = create_engine(config['database']['url'])
    with updated_engine.connect() as conn:
        conn.execute(text("USE dwh"))

    all_tables_exist = all(check_table_exists(updated_engine, table) for table in required_tables)

    if not all_tables_exist:
        with open(SQL_PATH, 'r') as file:
            sql_script = file.read()
        with engine.connect() as conn:
            for statement in sql_script.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        print("Table is created")
    else:
        print("Table have created already")

if __name__ == "__main__":
    execute_sql_script()