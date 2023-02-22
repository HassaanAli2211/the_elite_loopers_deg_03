import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import *

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Tutorial App")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
    .getOrCreate()
)

lux_meter_reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "luxmeter")
    .option("startingOffsets", "latest")
)
carbon_sense_reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "carbonsense")
    .option("startingOffsets", "latest")
)
moisture_mate_reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "moisturemate")
    .option("startingOffsets", "latest")
)
smart_thermo_reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "smartthermo")
    .option("startingOffsets", "latest")
)

lux_meter_df = lux_meter_reader.load()
carbon_sense_df = carbon_sense_reader.load()
moisture_mate_df = moisture_mate_reader.load()
smart_thermo_df = smart_thermo_reader.load()

lux_meter_schema = StructType(
    [
        StructField("room_id", StringType()),
        StructField(
            "measurements",
            StructType(
                [
                    StructField("timestamp", T.TimestampType()),
                    StructField("light_level", FloatType()),
                ]
            ),
        ),
    ]
)

carbon_sense_schema = StructType(
    [
        StructField("timestamp", T.TimestampType()),
        StructField("room_id", StringType()),
        StructField("co2", FloatType()),
    ]
)

moisture_mate_schema = StructType(
    [
        StructField("timestamp", T.TimestampType()),
        StructField("room_id", StringType()),
        StructField("humidity", FloatType()),
        StructField("humidity_ratio", FloatType()),
    ]
)

smart_thermo_schema = StructType(
    [
        StructField("timestamp", T.TimestampType()),
        StructField("room_id", StringType()),
        StructField("temperature", FloatType()),
    ]
)

lux_meter = lux_meter_df.withColumn(
    "message_content", F.from_json(F.col("value").cast("string"), lux_meter_schema)
)
carbon_sense = carbon_sense_df.withColumn(
    "message_content", F.from_json(F.col("value").cast("string"), carbon_sense_schema)
)
moisture_mate = moisture_mate_df.withColumn(
    "message_content", F.from_json(F.col("value").cast("string"), moisture_mate_schema)
)
smart_thermo = smart_thermo_df.withColumn(
    "message_content", F.from_json(F.col("value").cast("string"), smart_thermo_schema)
)

lux_meter = lux_meter.withColumn("value", col("value").cast("string"))
lux_meter = lux_meter.select(
    F.from_json(lux_meter.value, lux_meter_schema).alias("data")
)
lux_meter = lux_meter.select("data.*")

carbon_sense = carbon_sense.withColumn("value", col("value").cast("string"))
carbon_sense = carbon_sense.select(
    F.from_json(carbon_sense.value, carbon_sense_schema).alias("data")
)
carbon_sense = carbon_sense.select("data.*")

moisture_mate = moisture_mate.withColumn("value", col("value").cast("string"))
moisture_mate = moisture_mate.select(
    F.from_json(moisture_mate.value, moisture_mate_schema).alias("data")
)
moisture_mate = moisture_mate.select("data.*")

smart_thermo = smart_thermo.withColumn("value", col("value").cast("string"))
smart_thermo = smart_thermo.select(
    F.from_json(smart_thermo.value, smart_thermo_schema).alias("data")
)
smart_thermo = smart_thermo.select("data.*")

# Join the moisture_mate and carbon_sense DataFrames on "room_id" and "timestamp"
joined_df = moisture_mate.join(carbon_sense, on=["room_id", "timestamp"])

# Select relevant columns from lux_meter and join with the existing DataFrame on "room_id" and "timestamp"
lux_meter_df = lux_meter.select(
    "room_id",
    F.col("measurements.timestamp").alias("timestamp"),
    F.col("measurements.light_level").alias("light_level"),
)
joined_df = joined_df.join(lux_meter_df, on=["room_id", "timestamp"])

# Join the smart_thermo DataFrame with the existing DataFrame on "room_id" and "timestamp"
joined_df = joined_df.join(smart_thermo, on=["room_id", "timestamp"])

joined_df = joined_df.withColumnRenamed("timestamp", "date")
joined_df = joined_df.withColumn("Temperature", (joined_df["temperature"] - 32) * 5 / 9)
joined_df = joined_df.select(
    "date", "Temperature", "humidity", "light_level", "co2", "humidity_ratio", "room_id"
)

joined_df = joined_df.withColumnRenamed("light_level", "Light")
joined_df = joined_df.withColumnRenamed("co2", "CO2")
joined_df = joined_df.withColumnRenamed("humidity", "Humidity")
joined_df = joined_df.withColumnRenamed("humidity_ratio", "HumidityRatio")

Joined_query = (
    joined_df.writeStream.outputMode("append")
    .queryName("Joined_query")
    .format("console")
    .start()
)

Joined_query.status

df_to_kafka_streaming = joined_df.withColumn(
    "data_for_kafka", F.to_json(F.struct(*joined_df.columns))
).select(F.col("data_for_kafka").alias("value"))

df_to_kafka_streaming_query = (
    df_to_kafka_streaming.writeStream.format("kafka")
    .queryName("df_to_kafka_streaming_query")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("topic", "merged-data")
    .option("checkpointLocation", "./work/merged-data")
    .start()
    .awaitTermination()
)
df_to_kafka_streaming_query.status
