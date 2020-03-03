import json

from os import getenv

import boto3
from faker import Faker

fake = Faker()


def lambda_handler(event=None, context=None):
    client = boto3.client('firehose')

    for x in range(500):
        profiles = [create_profile() for _ in range(500)]
        records = [{'Data': json.dumps(profile).encode()}
                   for profile in profiles]
        client.put_record_batch(
            DeliveryStreamName=getenv('TARGET_STREAM_NAME'),
            Records=records
        )


def create_profile():
    profile = dict(
        **fake.simple_profile(),
        age=fake.pyint(min_value=1, max_value=120),
    )

    profile['birthdate'] = profile['birthdate'].isoformat()

    return profile


lambda_handler()
