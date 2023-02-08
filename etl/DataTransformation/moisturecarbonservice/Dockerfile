FROM python:3.10-slim-buster

WORKDIR /temp

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY producer_carbon_moisture.py producer_carbon_moisture.py

ENV PYTHONPATH /app

CMD [ "python3", "producer_carbon_moisture.py"]

