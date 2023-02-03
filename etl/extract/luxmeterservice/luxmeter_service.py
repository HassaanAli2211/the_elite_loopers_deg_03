import requests
import os
import json
import pandas as pd
import logging
from kafka import KafkaProducer

# producer = KafkaProducer(
#     bootstrap_servers=[producer_server_url],
#     value_serializer=lambda x: x.encode("utf-8")
#     )

logger = logging.getLogger()

producer = KafkaProducer(
    bootstrap_servers="broker:9093",
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("ascii"),
)


room_id = ["kitchen", "bedroom", "bathroom", "living_room"]
url_template = "http://sensor:3000/api/luxmeter/{}"

results = []
for room in room_id:
    url = url_template.format(room)
    response = requests.get(url)
    results.append(response.text)
producer.send(os.environ["KAFKA_TOPIC"], value={"record": results})
producer.flush()

logger.info(f"luxmeter: {results}")
