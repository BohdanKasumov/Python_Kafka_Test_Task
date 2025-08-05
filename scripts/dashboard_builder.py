from sqlalchemy import create_engine, text
import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')
SQL_PATH = os.path.join(BASE_DIR, 'sql', 'build_dashboard.sql')

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

engine = create_engine(config['database']['url'])

def build_dashboard():
    with open(SQL_PATH, 'r') as file:
        query = file.read()
    with engine.connect() as conn:
        for statement in query.split(';'):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()
    print("Dashboard updated")

if __name__ == "__main__":
    build_dashboard()