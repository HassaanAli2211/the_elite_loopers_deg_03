from fastapi import FastAPI, Request
import uvicorn
import json



app = FastAPI()


@app.post("/api/moisturemate")
async def collect_moisture(request:Request):
    request_data = await request.json()
    with open ("data.json", "w") as f:
        f.write(json.dumps(request_data))
        f.close()
    print(f"received : {request_data}")

    return{"msg:ok"}

@app.post("/api/carbonsense")
async def collect_carbon(request:Request):
    request_data = await request.json()
    with open ("carbonsensor_data.json", "w") as f:
        f.write(json.dumps(request_data))
        f.close()
    print(f"received : {request_data}")
    return{"msg:ok"}



def run_app():
 
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run_app()
