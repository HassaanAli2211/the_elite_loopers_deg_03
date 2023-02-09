# Readme for Spark Streaming and Kafka
This code demonstrates how to use PySpark to perform a join between two Kafka topics (moisturemate and carbonsense) and write the result to the console.


## Running the Code
-These instructions will help you run this code using docker-compose to set up the environment and dependencies.
## Prerequisites
You need to have docker and "docker-compose" installed on your machine.\
Setting up the Environment\
Clone the repository containing the code.\
Navigate to the directory containing the code.\
Run the following command to build the docker image:```docker-compose up --build```\
Then navigate to pyspark service logs using ```docker logs<container-id>```\
Click on the http://127.0.0.1:8888/lab and upload the notebook, you will find in this directory ```etl/transform/kafka_pyspark_streaming.ipynb``` and run all cells\
You will start seeing logs

## Built With
PySpark - The web framework used\
Kafka - The message broker used\
Docker - The containerization technology used
# Author
Muhmmad Asim\
Muhammad huzaifa Imtiaz
