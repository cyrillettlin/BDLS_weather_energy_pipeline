import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Config
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://timescaledb:5432/pipeline_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline")

spark = (
    SparkSession.builder
    .appName("EnergyStreamProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. PV DATA PROCESSING
# ==========================================

pv_schema = StructType([
    StructField("sm_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("data", StructType([
        StructField("lastUpdate", StringType(), True),
        StructField("production", DoubleType(), True),
        StructField("consumption", DoubleType(), True)
    ]), True)
])

pv_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", "energy-raw")
    .option("startingOffsets", "earliest")
    .load()
)

pv_parsed = (
    pv_raw
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), pv_schema).alias("data"))
    .select(
        to_timestamp(from_unixtime(col("data.timestamp"))).alias("time"),
        col("data.sm_id").alias("sm_id"),
        col("data.data.production").alias("pv_power"),
        col("data.data.consumption").alias("load_power"),
        (col("data.data.production") - col("data.data.consumption")).alias("grid_power")
    )
    .filter(col("time").isNotNull())
)

def write_pv_to_postgres(df, epoch_id):
    df.write \
      .format("jdbc") \
      .option("url", POSTGRES_URL) \
      .option("dbtable", "pv_data") \
      .option("user", POSTGRES_USER) \
      .option("password", POSTGRES_PASSWORD) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

pv_query = (
    pv_parsed.writeStream
    .foreachBatch(write_pv_to_postgres)
    .outputMode("update")
    .start()
)

# ==========================================
# 2. WEATHER DATA PROCESSING
# ==========================================

weather_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("irradiance", DoubleType(), True)
])

weather_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", "weather-raw")
    .option("startingOffsets", "earliest")
    .load()
)

weather_parsed = (
    weather_raw
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), weather_schema).alias("data"))
    .select(
        to_timestamp(from_unixtime(col("data.timestamp"))).alias("time"),
        col("data.station_id").alias("station_id"),
        col("data.temperature").alias("temperature"),
        col("data.humidity").alias("humidity"),
        col("data.wind_speed").alias("wind_speed"),
        col("data.irradiance").alias("irradiance")
    )
    .filter(col("time").isNotNull())
)

def write_weather_to_postgres(df, epoch_id):
    df.write \
      .format("jdbc") \
      .option("url", POSTGRES_URL) \
      .option("dbtable", "weather_data") \
      .option("user", POSTGRES_USER) \
      .option("password", POSTGRES_PASSWORD) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

weather_query = (
    weather_parsed.writeStream
    .foreachBatch(write_weather_to_postgres)
    .outputMode("update")
    .start()
)

spark.streams.awaitAnyTermination()