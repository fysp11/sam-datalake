import json

from base64 import b64decode, b64encode


def lambda_handler(event, context):
    processed_records = [process_record(record) for record in event['records']]
    return dict(records=processed_records)


def process_record(record):
    decoded_data = b64decode(record['data'])
    parsed_data = json.loads(decoded_data)

    more_data = {**parsed_data, 'testing': True}

    bytes_data = json.dumps(more_data).encode('utf-8')
    done_data = b64encode(decoded_data).decode('utf-8')

    return {
        'recordId': record['recordId'],
        'result': 'Ok',
        'data': done_data,
    }
