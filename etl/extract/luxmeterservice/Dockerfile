FROM python:3.10-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY luxmeter_service.py luxmeter_service.py
ENV PYTHONPATH /app
CMD [ "python3", "luxmeter_service.py"]





