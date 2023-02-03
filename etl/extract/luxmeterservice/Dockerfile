
FROM python:3.10-slim-buster
RUN apt-get update && apt-get -y install cron
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab
RUN pip install requests
COPY . . 

COPY luxmeter_service.py luxmeter_service.py
ENV PYTHONPATH /app

# CMD [ "python3", "luxmeter_service.py"]
CMD ["cron", "-f"]




