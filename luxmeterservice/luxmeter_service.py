import requests
import pandas as pd
import logging
logger = logging.getLogger()
room_id = ["kitchen", "bedroom", "bathroom", "living_room"]
url_template = "http://0.0.0.0:3000/api/luxmeter/"
def get_lux_data():
    results = []
    for room in room_id:
        response = requests.get(url_template+room)
        response_json=response.json()
        results.append(response_json)
        logger.info(f"MoistureMate: {results}")   
    


if __name__ == "__main__":
    get_lux_data()
