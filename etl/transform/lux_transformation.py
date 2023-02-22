import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Tutorial App")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
    .getOrCreate()
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

lux_meter_reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "luxmeter")
)

carbon_sense_df = carbon_sense_reader.load()

moisture_mate_df = moisture_mate_reader.load()

lux_meter_df = lux_meter_reader.load()

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

lux_meter_schema = StructType(
    [
        StructField("room_id", StringType()),
        StructField(
            "measurements",
            StructType(
                [
                    StructField("timestamp", TimestampType()),
                    StructField("light_level", FloatType()),
                ]
            ),
        ),
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

lux_meter = lux_meter.withColumn("value", col("value").cast("string"))
lux_meter = lux_meter.select(
    F.from_json(lux_meter.value, lux_meter_schema).alias("data")
)
lux_meter = lux_meter.select("data.*")

query_carbon = (
    carbon_sense.writeStream.queryName("carbonsense_query")
    .outputMode("append")
    .format("console")
    .start()
)
query_carbon.status

query_moisture = (
    moisture_mate.writeStream.queryName("moisturemate_query")
    .outputMode("append")
    .format("console")
    .start()
)
query_moisture.status


query_lux = (
    lux_meter.writeStream.queryName("lux_meter_query")
    .outputMode("append")
    .format("console")
    .start()
)
query_lux.status
