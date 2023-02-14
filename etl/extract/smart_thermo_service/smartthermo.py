import boto3
import json
import time
from datetime import datetime
import pandas as pd
import logging
from kafka import KafkaProducer

logger = logging.getLogger()


producer = KafkaProducer(
    bootstrap_servers=["kafka:29092"],
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

s3_client = boto3.client("s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

def smartthermo():
    logger.info("Initiating smartthermoo function")
    got_date=None
    while True:
        date = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        
        bucket = "smart-thermo-sensor"
        key = f"smart_thermo/{date}.csv"
        time.sleep(10)
        try:
            if got_date != date:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                df = pd.read_csv(obj['Body'])
                json_data = df.to_json(orient='records')
                smart_data = json.loads(json_data)
                for i in smart_data:
                    if "Unnamed: 0" in i:
                        del i["Unnamed: 0"]
                        logger.info(i)
                        producer.send("smartthermo", value=i)
                got_date=date
        except Exception as e:
            logger.error(f"An error occured while processing data: {e}")
            pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    smartthermo()


