from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from config import KAFKA_BROKERS, jdbc_write

# ---------------------------------------------------------------------
# Spark Session Setup
# ---------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RedpandaToTimescale-Ingest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------
weather_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("irradiance", DoubleType(), True)
])

energy_schema = StructType([
    StructField("sm_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("data", StructType([
        StructField("lastUpdate", StringType(), True),
        StructField("production", LongType(), True),
        StructField("consumption", LongType(), True)
    ]), True)
])

windpark_schema = StructType([
    StructField("windpark_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("rpm", DoubleType(), True)
])

# ---------------------------------------------------------------------
# Writer Callbacks
# ---------------------------------------------------------------------
def write_weather_batch(df, epoch_id):
    jdbc_write(df, "weather_data", epoch_id, "weather_batch")


def write_energy_batch(df, epoch_id):
    jdbc_write(df, "pv_data", epoch_id, "energy_batch")


def write_windpark_batch(df, epoch_id):
    jdbc_write(df, "windpark_data", epoch_id, "windpark_batch")


# ---------------------------------------------------------------------
# Stream 1: Weather Data Pipeline
# ---------------------------------------------------------------------
raw_weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "weather-raw") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_weather_df = raw_weather_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), weather_schema).alias("data")) \
    .select(
        col("data.timestamp").cast("timestamp").alias("time"),
        col("data.station_id").alias("station_id"),
        col("data.temperature").alias("temperature"),
        col("data.humidity").alias("humidity"),
        col("data.wind_speed").alias("wind_speed"),
        col("data.irradiance").alias("irradiance")
    )

weather_query = parsed_weather_df.writeStream \
    .foreachBatch(write_weather_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/weather") \
    .start()

# ---------------------------------------------------------------------
# Stream 2: Energy Data Pipeline
# ---------------------------------------------------------------------
raw_energy_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "energy-raw") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_energy_df = raw_energy_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), energy_schema).alias("data")) \
    .select(
        col("data.timestamp").cast("timestamp").alias("time"),
        col("data.sm_id").alias("sm_id"),
        col("data.data.production").alias("production"),
        col("data.data.consumption").alias("consumption")
    )

energy_query = parsed_energy_df.writeStream \
    .foreachBatch(write_energy_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/energy") \
    .start()

# ---------------------------------------------------------------------
# Stream 3: Windpark RPM Data Pipeline
# ---------------------------------------------------------------------
raw_windpark_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "windpark-raw") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_windpark_df = raw_windpark_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), windpark_schema).alias("data")) \
    .select(
        col("data.timestamp").cast("timestamp").alias("time"),
        col("data.windpark_id").alias("windpark_id"),
        col("data.rpm").alias("rpm")
    )

windpark_query = parsed_windpark_df.writeStream \
    .foreachBatch(write_windpark_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/windpark") \
    .start()

# ---------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------
spark.streams.awaitAnyTermination()
