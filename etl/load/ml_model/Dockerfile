FROM python:3.8-slim-buster
WORKDIR /app
#Refrence of line 4,5 & 10 from deg-group-04
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless 

COPY requirements.txt requirements.txt
COPY . . 
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y procps

CMD ["python3", "machinelearning.py"]   
