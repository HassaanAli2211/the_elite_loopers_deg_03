import logging
import os
import time
import boto3

logger = logging.getLogger()

endpoint = os.environ.get("ENDPOINT_URL")
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
bucket_name = os.environ.get("SMART_THERMO_BUCKET")

s3 = boto3.resource(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

def smartthermo():
    bucket = s3.Bucket(bucket_name)

    objects = list(bucket.objects.all())
    obj = list(filter(lambda o: o.key.endswith(".csv"), objects))[-1]
    obj = s3.Object(bucket_name, obj.key)

    csv_data = obj.get()['Body'].read().decode('utf-8')
    logger.info(csv_data)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    while True:
        smartthermo()
        time.sleep(60)
        