import json
import logging
import os

import pandas as pd
import requests
import uvicorn
from fastapi import FastAPI, Request
from fastapi_utils.tasks import repeat_every
from kafka import KafkaProducer

app = FastAPI()
logger = logging.getLogger()


producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)


room_id = ["kitchen", "bedroom", "bathroom", "living_room"]


@app.on_event("startup")
@repeat_every(seconds=60, wait_first=True)
def lux_data():
    for room in room_id:
        url_template = f"http://sensorsmock:3000/api/luxmeter/{room}"
        response = requests.get(url_template).json()
        updated_response = dict(
            (k, response[k]) for k in ["measurements"] if k in response
        )["measurements"][-1]
        response["measurements"] = updated_response
        try:
            result = producer.send("luxmeter", value=updated_response)
            logger.info(f"Received LuxMeter data{result}")
        except:
            logger.info(f"didn't run")


def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3007)


if __name__ == "__main__":
    run_app()
