from datetime import datetime, timedelta

def validate_receipt(receipt):
    if not all(key in receipt for key in ['receipt_id', 'store_id', 'datetime', 'items']):
        return False

    try:
        receipt_date = datetime.fromisoformat(receipt['datetime'])
        current_date = datetime.now()
        if receipt_date > current_date or receipt_date < current_date - timedelta(days=7):
            return False
    except ValueError:
        return False

    for item in receipt['items']:
        if not item.get('sku_id'):
            return False
        if item['price'] <= 0 or item['quantity'] <= 0:
            return False
        discount = item.get('discount', 0.0)
        if discount < 0 or discount > item['price']:
            return False

    return True