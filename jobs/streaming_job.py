import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# ---------------------------------------------------------------------
# Configuration & Credentials
# ---------------------------------------------------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
DB_URL = "jdbc:postgresql://timescaledb:5432/pipeline_db"
DB_USER = "pipeline"
DB_PASSWORD = "pipeline"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_PROPS = {"user": DB_USER, "password": DB_PASSWORD, "driver": JDBC_DRIVER}

# ---------------------------------------------------------------------
# PV-Ertragsschaetzung: Konfiguration
# ---------------------------------------------------------------------
PV_PEAK_POWER_W = float(os.getenv("PV_PEAK_POWER_W", "5000"))
PERFORMANCE_RATIO = float(os.getenv("PV_PERFORMANCE_RATIO", "0.80"))
TOLERANCE_PCT = float(os.getenv("PV_TOLERANCE_PCT", "0.15"))

PV_WEATHER_STATION_ID = os.getenv("PV_WEATHER_STATION_ID", "villmergen")

WEATHER_POLL_INTERVAL_SECONDS = int(os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "900"))

# Wie alt darf die zuletzt bekannte Wettermessung maximal sein, damit sie
# noch fuer eine Schaetzung verwendet wird? (z.B. falls der Wetter-Producer
# laengere Zeit ausfaellt, sollen keine Schaetzungen mit veralteten Werten
# entstehen). Grosszuegig auf das 2-fache des Poll-Intervalls gesetzt.
MAX_WEATHER_AGE_SECONDS = int(os.getenv("PV_MAX_WEATHER_AGE_SECONDS", str(WEATHER_POLL_INTERVAL_SECONDS * 2)))

# ---------------------------------------------------------------------
# Spark Session Setup
# ---------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RedpandaToTimescalePipeline") \
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

# ---------------------------------------------------------------------
# Writer Helper Functions
# ---------------------------------------------------------------------
def write_weather_batch(df, epoch_id):
    count = df.count()
    if count > 0:
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
            print(f"[weather_batch epoch={epoch_id}] {count} Zeilen geschrieben.", flush=True)
        except Exception as e:
            print(f"[weather_batch epoch={epoch_id}] JDBC write failed: {e}", flush=True)


def write_energy_batch(df, epoch_id):
    count = df.count()
    if count > 0:
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
            print(f"[energy_batch epoch={epoch_id}] {count} Zeilen geschrieben.", flush=True)
        except Exception as e:
            print(f"[energy_batch epoch={epoch_id}] JDBC write failed: {e}", flush=True)


# ---------------------------------------------------------------------
# GEAENDERT: Stream 3 als As-of-Lookup statt Stream-Stream-Join.
#
# Nach mehreren Versuchen mit Sparks nativem Stream-Stream-Join (Range-
# Bedingung ohne Equality, OR-verknuepfte Bucket-Equality, konstanter
# Dummy-Equality-Key) blieb pv_estimates leer, obwohl die Zeitfenster in
# den persistierten Daten sich nachweislich ueberlappten. Die Ursache
# liegt vermutlich im Zusammenspiel aus Watermark-State-Buffering und
# zwei unabhaengigen Kafka-Consumer-Gruppen (energy_query/weather_query
# vs. estimate_query lesen denselben Kafka-Topic separat und unabhaengig
# voneinander ein), was fuer den fachlich eigentlich einfachen Bedarf
# ("nimm die zuletzt bekannte Wettermessung") unnoetig fragil ist.
#
# Stattdessen: Bei jedem Energie-Batch wird direkt per JDBC die aktuell
# juengste Wetterzeile aus der bereits von Stream 1 befuellten Tabelle
# weather_data gelesen (klassisches As-of-/Punkt-Lookup). Das ist robust,
# leicht nachvollziehbar und passt semantisch besser zum Anwendungsfall
# als ein zeitfenstergebundener Stream-Stream-Join.
# ---------------------------------------------------------------------
def write_estimate_batch(df, epoch_id):
    count = df.count()
    print(f"[estimate_batch epoch={epoch_id}] Batch aufgerufen mit {count} Zeilen.", flush=True)
    if count == 0:
        return

    try:
        latest_weather_rows = spark.read.jdbc(
            url=DB_URL, table="weather_data", properties=JDBC_PROPS
        ).filter(col("station_id") == PV_WEATHER_STATION_ID) \
         .orderBy(col("time").desc()) \
         .limit(1) \
         .collect()
    except Exception as e:
        print(f"[estimate_batch epoch={epoch_id}] JDBC read (weather_data) failed: {e}", flush=True)
        return

    if not latest_weather_rows:
        print(f"[estimate_batch epoch={epoch_id}] Noch keine Wetterdaten fuer Station '{PV_WEATHER_STATION_ID}' vorhanden - ueberspringe.", flush=True)
        return

    latest_weather = latest_weather_rows[0]
    weather_time = latest_weather["time"]
    irradiance = latest_weather["irradiance"]

    now_utc = datetime.now(timezone.utc)
    weather_time_utc = weather_time if weather_time.tzinfo is not None else weather_time.replace(tzinfo=timezone.utc)
    weather_age = now_utc - weather_time_utc

    if weather_age > timedelta(seconds=MAX_WEATHER_AGE_SECONDS):
        print(
            f"[estimate_batch epoch={epoch_id}] Letzte Wettermessung ist {weather_age} alt "
            f"(> {MAX_WEATHER_AGE_SECONDS}s) - ueberspringe, um keine veralteten Schaetzungen zu erzeugen.",
            flush=True
        )
        return

    estimate_df = df.select(
        col("time"),
        col("sm_id"),
        col("production").alias("actual_production")
    ).withColumn("station_id", lit(PV_WEATHER_STATION_ID)) \
     .withColumn("irradiance", lit(irradiance)) \
     .withColumn("expected_production", lit(irradiance) / 1000.0 * PV_PEAK_POWER_W * PERFORMANCE_RATIO) \
     .withColumn("expected_production_min", col("expected_production") * (1 - TOLERANCE_PCT)) \
     .withColumn("expected_production_max", col("expected_production") * (1 + TOLERANCE_PCT)) \
     .withColumn("deviation_w", col("actual_production") - col("expected_production"))

    out_count = estimate_df.count()
    try:
        estimate_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "pv_estimates") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", JDBC_DRIVER) \
            .mode("append") \
            .save()
        print(
            f"[estimate_batch epoch={epoch_id}] {out_count} Zeilen in pv_estimates geschrieben "
            f"(irradiance={irradiance} von Wetterzeit {weather_time}).",
            flush=True
        )
    except Exception as e:
        print(f"[estimate_batch epoch={epoch_id}] JDBC write failed: {e}", flush=True)


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
# Stream 3: PV-Ertragsschaetzung - As-of-Lookup auf jedem Energie-Batch
# (nutzt dieselbe geparste Energie-Quelle wie Stream 2, aber eigener
# Checkpoint und eigene foreachBatch-Logik, siehe write_estimate_batch)
# ---------------------------------------------------------------------
estimate_query = parsed_energy_df.writeStream \
    .foreachBatch(write_estimate_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/pv_estimates") \
    .start()

# ---------------------------------------------------------------------
# Await Termination
# ---------------------------------------------------------------------
spark.streams.awaitAnyTermination()