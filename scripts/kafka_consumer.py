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
    df.to_sql('staging_receipts', engine, if_exists='append', index=False)


def process_message(message):
    try:
        receipt = json.loads(message.value().decode('utf-8'))
        if not validate_receipt(receipt):
            print(f"Invalid receipt: {receipt['receipt_id']}")
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


if __name__ == "__main__":
    main()
