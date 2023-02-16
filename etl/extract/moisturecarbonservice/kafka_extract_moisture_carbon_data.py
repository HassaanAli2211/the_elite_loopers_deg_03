import os
from fastapi import FastAPI, Request
import logging
import uvicorn
import json
from kafka import KafkaProducer


logger = logging.getLogger()

app = FastAPI()
 
KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    acks=1,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)


@app.post("/api/moisturemate")
async def collect_moisture(request:Request):
    request_data = await request.json()
    producer.send("moisturemate", request_data)
    print(f"MoistureMate: {request_data}")    
    return{"msg:ok"}


@app.post("/api/carbonsense")
async def collect_carbon(request:Request):
    request_data = await request.json()
    producer.send("carbonsense", request_data)
    print(f"Carbonsesne: {request_data}")   
    return{"msg:ok"}

def run_app():
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run_app()
