from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
import logging
import os
import requests
import uvicorn
import json
from kafka import KafkaProducer

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVER"),
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

logger = logging.getLogger()

room_ids = ["kitchen", "bedroom", "bathroom", "living_room"]

@app.on_event("startup")
@repeat_every(seconds=60, wait_first=True)

def lux_data():

    for room in room_ids:
        url_template=os.environ.get("LUXMETER_URL")
        url= f"{url_template}{room}"
        response = requests.get(url).json()
        last_measurement = response["measurements"][-1]
        final_data = {
            "room": room,
            "measurement": last_measurement
        }
        result = producer.send("luxmeter", value=final_data)
        logger.info("Received Luxmeter data",result)

def run_app():

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3007)

if __name__ == "__main__":
    run_app()
    
