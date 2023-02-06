import logging
from fastapi import FastAPI
import uvicorn
import requests
from fastapi_utils.tasks import repeat_every
app = FastAPI()
logger = logging.getLogger()
 
 
room_id = ["kitchen", "bedroom", "bathroom", "living_room"]

@app.on_event("startup")
@repeat_every(seconds=60, wait_first=True)
def lux_data():
    for rooms in room_id:
        url_template = f"http://sensorsmock:3000/api/luxmeter/{rooms}"
        response = requests.get(url_template).json()
        Updated_respose = dict((k, response[k]) for k in ['measurements']
           if k in response)['measurements'][-1]
        response['measurements'] = Updated_respose
        logger.info(f"Received LuxMeter data:{Updated_respose}")

def run_app():
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=3001)
if __name__ == "__main__":
    run_app()


