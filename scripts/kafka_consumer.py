from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
from sqlalchemy import create_engine
import yaml
import os
from data_validation import validate_receipt

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')

with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

engine = create_engine(config['database']['url'])

consumer_config = {
    'bootstrap.servers': config['kafka']['bootstrap_servers'],
    'group.id': 'receipts_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['receipts_raw'])

def save_to_staging(df):
    try:
        df = df.where(pd.notnull(df), None)
        df.to_sql('staging_receipts', engine, if_exists='append', index=False, dtype={
            'receipt_id': pd.StringDtype(),
            'store_id': pd.StringDtype(),
            'datetime': pd.DatetimeTZDtype(),
            'sku_id': pd.StringDtype(),
            'brand': pd.StringDtype(),
            'price': pd.Float64Dtype(),
            'quantity': pd.Int64Dtype(),
            'discount': pd.Float64Dtype(),
            'promotion_id': pd.StringDtype()
        })
        print(f"Successfully saved to staging_receipts: {df['receipt_id'].iloc[0]}")
    except Exception as e:
        print(f"Error in to_sql: {e}")
        raise

def process_message(message):
    try:
        receipt = json.loads(message.value().decode('utf-8'))
        if not validate_receipt(receipt):
            print(f"Invalid receipt: {receipt.get('receipt_id', 'Unknown')}")
            return

        rows = []
        for item in receipt['items']:
            row = {
                'receipt_id': receipt['receipt_id'],
                'store_id': receipt['store_id'],
                'datetime': pd.to_datetime(receipt['datetime']),
                'sku_id': item['sku_id'],
                'brand': item['brand'],
                'price': float(item['price']),
                'quantity': int(item['quantity']),
                'discount': float(item.get('discount', 0.0)),
                'promotion_id': item.get('promotion_id')
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['price'] = df['price'].astype(float)
        df['quantity'] = df['quantity'].astype(int)
        df['discount'] = df['discount'].astype(float)
        save_to_staging(df)
        print(f"Processed receipt: {receipt['receipt_id']}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            process_message(msg)
    except KeyboardInterrupt:
        consumer.close()
    except Exception as e:
        print(f"Consumer error: {e}")
        consumer.close()

if __name__ == "__main__":
    main()