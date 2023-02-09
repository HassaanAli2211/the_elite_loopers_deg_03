import json
import time
from datetime import datetime

import boto3
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["kafka:29092"],
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

s3_client = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

while True:
    date = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
    bucket = "smart-thermo-sensor"
    key = f"smart_thermo/{date}.csv"
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
    json_data = df.to_json(orient="records")
    producer.send("smartthermo", value=json_data)
    time.sleep(60)
