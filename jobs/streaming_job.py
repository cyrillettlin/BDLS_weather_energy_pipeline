import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# ---------------------------------------------------------------------
# Configuration & Credentials
# ---------------------------------------------------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
DB_URL = "jdbc:postgresql://timescaledb:5432/pipeline_db"
DB_USER = "pipeline"
DB_PASSWORD = "pipeline"  # Das korrekt konfigurierte Passwort aus docker-compose.yml
JDBC_DRIVER = "org.postgresql.Driver"

# ---------------------------------------------------------------------
# Spark Session Setup
# ---------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RedpandaToTimescalePipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------
# Schemas — angepasst an die TATSAECHLICHEN Producer-Payloads
# ---------------------------------------------------------------------

# Weather-Producer sendet ein flaches JSON:
# {"station_id": "...", "timestamp": <epoch_seconds float>, "temperature": ..,
#  "humidity": .., "wind_speed": .., "irradiance": ..}
weather_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("irradiance", DoubleType(), True)
])

# Energy-Producer sendet ein verschachteltes JSON:
# {"sm_id": "...", "timestamp": <epoch_seconds float>,
#  "data": {"lastUpdate": .., "production": .., "consumption": .., "battery": {...}, "arrows": [...]}}
energy_schema = StructType([
    StructField("sm_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("data", StructType([
        StructField("lastUpdate", StringType(), True),
        StructField("production", LongType(), True),
        StructField("consumption", LongType(), True)
        # battery/arrows werden aktuell nicht persistiert
    ]), True)
])

# ---------------------------------------------------------------------
# Writer Helper Functions
# ---------------------------------------------------------------------
def write_weather_batch(df, epoch_id):
    if df.count() > 0:
        try:
            df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "weather_data") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", JDBC_DRIVER) \
                .mode("append") \
                .save()
            print(f"[weather_batch epoch={epoch_id}] {df.count()} Zeilen geschrieben.", flush=True)
        except Exception as e:
            print(f"[weather_batch epoch={epoch_id}] JDBC write failed: {e}", flush=True)


def write_energy_batch(df, epoch_id):
    if df.count() > 0:
        try:
            df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "pv_data") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", JDBC_DRIVER) \
                .mode("append") \
                .save()
            print(f"[energy_batch epoch={epoch_id}] {df.count()} Zeilen geschrieben.", flush=True)
        except Exception as e:
            print(f"[energy_batch epoch={epoch_id}] JDBC write failed: {e}", flush=True)

# ---------------------------------------------------------------------
# Stream 1: Weather Data Pipeline
# weather_data-Tabelle: time, station_id, temperature, humidity, wind_speed, irradiance
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
# pv_data-Tabelle: time, sm_id, production, consumption
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
# Await Termination
# ---------------------------------------------------------------------
spark.streams.awaitAnyTermination()