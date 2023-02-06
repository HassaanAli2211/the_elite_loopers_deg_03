import logging

import uvicorn
from fastapi import FastAPI, Request

app = FastAPI()
logger = logging.getLogger()


@app.post("/api/moisturemate")
async def collect_moisture(request: Request):

    request_data = await request.json()
    logger.info(f"MoistureMate: {request_data}")

    return {"msg:ok"}


@app.post("/api/carbonsense")
async def collect_carbon(request: Request):
    request_data = await request.json()
    logger.info(f"carbonsense: {request_data}")
    return {"msg:ok"}


def run_app():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run_app()
