import json
import logging
import os

import requests
import uvicorn
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
from kafka import KafkaProducer

app = FastAPI()
logger = logging.getLogger()

producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVER"),
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

room_ids = ["kitchen", "bedroom", "bathroom", "living_room"]


@app.on_event("startup")
@repeat_every(seconds=60, wait_first=True)
async def lux_data():
    url_template = os.environ.get("LUXMETER_URL")
    for room in room_ids:

        url = f"{url_template}{room}"
        response = requests.get(url).json()
        last_measurement = response["measurements"][-1]
        final_data = {"room": room, "measurement": last_measurement}
        logger.info(f"Received Luxmeter data: {final_data}")
        result = producer.send("luxmeter", value=final_data)
        logger.info(f"Sent data to Kafka {room}: {result}")


def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3007)


if __name__ == "__main__":
    run_app()
