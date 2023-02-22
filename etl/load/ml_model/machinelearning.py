import json
import logging
import pickle

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger()
with open("model.pkl", "rb") as f:
    model = pickle.load(f)
# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    "merged-data",
    bootstrap_servers=["kafka:29092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group-id",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
# Create a KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=["kafka:29092"],
    value_serializer=lambda m: json.dumps(m).encode("utf-8"),
)
for message in consumer:
    # Retrieve the data from the Kafka message
    data = message.value
    df = pd.DataFrame([data])
    new_df = df.drop(["room_id", "date"], axis=1)
    new_df = new_df.to_numpy()
    predict = model.predict(new_df)
    df["occupancy"] = predict
    # Convert DataFrame to dictionary and serialize to JSON
    df_dict = df.to_dict(orient="records")
    # json_data = json.dumps(df_dict)
    # logger.info(df_dict)
    producer.send("updated-data", value=df_dict[0])
