# importing neccessory libraries
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
import logging
import os
import requests
import uvicorn
import json
# initialize the FastAPI instance
app = FastAPI()
# initialize the logger instance
logger = logging.getLogger()
# List of rooms
room_ids = ["kitchen", "bedroom", "bathroom", "living_room"]
# Use the startup event to schedule a repeating task using repeat_every
@app.on_event("startup")
@repeat_every(seconds=60, wait_first=True)
def lux_data():
    # Loop through each room ID
    for room in room_ids:
        # Build the full URL for the current room
        # Refference: your last lecture
        url_template=os.environ.get("LUXMETER_URL")
        url= f"{url_template}{room}"
        # Use the requests library to make a GET request to the URL
        response = requests.get(url).json()
        last_measurement = response["measurements"][-1]
        final_data = {
            "room": room,
            "measurement": last_measurement
        }
        logger.info("Received Luxmeter data",final_data)
          # Define a function to run the FastAPI app
def run_app():
    # Configure the logging to show INFO level messages
    logging.basicConfig(level=logging.INFO)
    # Start the FastAPI app using uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3007)
if __name__ == "__main__":
    run_app()
