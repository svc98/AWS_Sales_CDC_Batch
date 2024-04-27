import base64
import json
from datetime import datetime


def lambda_handler(event, context):
    output_records = []

    for record in event['records']:
        try:
            # Decode the input data from base64
            payload = base64.b64decode(record['data'])
            payload_json = json.loads(payload)

            # Setup Variables
            event_name = payload_json['eventName']
            dynamodb_data = payload_json['dynamodb']
            approx_creation_datetime = payload_json['dynamodb']['ApproximateCreationDateTime']
            new_image = dynamodb_data['NewImage']
            creation_datetime = datetime.utcfromtimestamp(approx_creation_datetime).isoformat() + 'Z'                    # Convert the Unix timestamp to human-readable date format

            # Extract required fields from NewImage
            transformed_data = {
                'orderid': new_image['orderid']['S'],
                "customer_name": new_image['customer_name']['S'],
                'product_name': new_image['product_name']['S'],
                'quantity': int(new_image['quantity']['N']),
                'price': float(new_image['price']['N']),
                'rating': int(new_image['rating']['N']),
                'purchase_date': new_image['purchase_date']['S'],
                'cdc_event_type': event_name,
                'creation_datetime': creation_datetime
            }

            # Convert the transformed data to a JSON string and then encode it as base64
            transformed_data_str = json.dumps(transformed_data) + '\n'
            transformed_data_encoded = base64.b64encode(transformed_data_str.encode('utf-8')).decode('utf-8')

            # Append the transformed record to the output using "recordId"
            output_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': transformed_data_encoded
            })
        except Exception as e:
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']  # simply pass the original data back
            })

    return {
        'records': output_records
    }