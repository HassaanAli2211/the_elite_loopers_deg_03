from fastapi import FastAPI
import uvicorn
from fastapi_utils.tasks import repeat_every
import logging
app = FastAPI()
logger = logging.getLogger()
room_ids = ["kitchen", "bedroom", "bathroom", "living_room"]
@app.on_event("startup")
@repeat_every(seconds=60, wait_first=True)
def lux_data():
    for room in room_ids:
        url_template = f"http://sensorsmock:3000/api/luxmeter/{room}"
        response = requests.get(url_template).json()
        response_data = response.get('measurements')[-1] if 'measurements' in response else None
        logger.info(f"Received LuxMeter data{response_data}")
       
def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3007)
if __name__ == "__main__":
    run_app()
