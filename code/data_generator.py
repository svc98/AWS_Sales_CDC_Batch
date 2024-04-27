import uuid
import boto3
import random
import requests
from datetime import datetime
from decimal import Decimal
from config.config import configuration



BASE_URL = 'https://randomuser.me/api?nat=US'
session = boto3.Session(profile_name='default',
                        aws_access_key_id=configuration.get("AWS_ACCESS_KEY"),
                        aws_secret_access_key=configuration.get("AWS_SECRET_KEY"),
                        region_name=configuration.get("AWS_REGION"))
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('test_table')


def generate_order_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            'orderid': str(uuid.uuid4()),
            "customer_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            'product_name': random.choice(['Laptop', 'Phone', 'Tablet', 'Headphones', 'Charger']),
            'quantity': random.randint(1, 5),
            'price': Decimal(str(round(random.uniform(10.0, 500.0), 2))),
            'rating': int(random.choice(['5', '5', '5', '5', '5', '4', '4', '4', '3', '3', '3', '2', '2', '1'])),
            'purchase_date': datetime.now().isoformat()
        }


def insert_into_dynamodb(data):
    try:
        table.put_item(Item=data)
        print(f"Inserted data: {data}")
    except Exception as e:
        print(f"Error inserting data: {str(e)}")


if __name__ == '__main__':
    try:
        while True:
            data = generate_order_data()
            insert_into_dynamodb(data)
            # time.sleep(1)
    except KeyboardInterrupt:
        print("\nUser manually stopped the Order Generator Script!")