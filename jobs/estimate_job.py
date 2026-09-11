from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from config import (
    KAFKA_BROKERS, DB_URL, JDBC_PROPS,
    PV_PEAK_POWER_W, PERFORMANCE_RATIO, TOLERANCE_PCT,
    PV_WEATHER_STATION_ID, WIND_WEATHER_STATION_ID,
    WIND_RPM_FACTOR, WIND_RPM_TOLERANCE_PCT,
    MAX_WEATHER_AGE_SECONDS, jdbc_write
)

_required = {
    "PV_WEATHER_STATION_ID": PV_WEATHER_STATION_ID,
    "WIND_WEATHER_STATION_ID": WIND_WEATHER_STATION_ID,
}
_missing = [name for name, value in _required.items() if not value]
if _missing:
    raise RuntimeError(
        f"estimate_job.py: fehlende Pflicht-Umgebungsvariablen: {', '.join(_missing)}"
    )

spark = SparkSession.builder \
    .appName("RedpandaToTimescale-Estimates") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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


def _get_latest_weather(station_id, epoch_id, log_prefix, limit=1):
    try:
        rows = spark.read.jdbc(
            url=DB_URL, table="weather_data", properties=JDBC_PROPS
        ).filter(col("station_id") == station_id) \
         .orderBy(col("time").desc()) \
         .limit(limit) \
         .collect()
    except Exception as e:
        print(f"[{log_prefix} epoch={epoch_id}] JDBC read (weather_data) failed: {e}", flush=True)
        return []

    if not rows:
        print(f"[{log_prefix} epoch={epoch_id}] Noch keine Wetterdaten fuer Station '{station_id}' vorhanden - ueberspringe.", flush=True)
        return []

    weather_time = rows[0]["time"]
    now_utc = datetime.now(timezone.utc)
    weather_time_utc = weather_time if weather_time.tzinfo is not None else weather_time.replace(tzinfo=timezone.utc)
    if now_utc - weather_time_utc > timedelta(seconds=MAX_WEATHER_AGE_SECONDS):
        print(f"[{log_prefix} epoch={epoch_id}] Wetterdaten zu alt - ueberspringe.", flush=True)
        return []

    return rows


def _extrapolate_irradiance(rows, epoch_id):
    latest = rows[0]
    irradiance_now = latest["irradiance"]
    if len(rows) < 2 or irradiance_now is None:
        return irradiance_now, latest["time"]

    prev = rows[1]
    latest_t = latest["time"].replace(tzinfo=timezone.utc) if latest["time"].tzinfo is None else latest["time"]
    prev_t = prev["time"].replace(tzinfo=timezone.utc) if prev["time"].tzinfo is None else prev["time"]
    dt = (latest_t - prev_t).total_seconds()
    age = (datetime.now(timezone.utc) - latest_t).total_seconds()

    if dt <= 0 or prev["irradiance"] is None:
        return irradiance_now, latest["time"]

    rate = (irradiance_now - prev["irradiance"]) / dt
    extrapolated = max(0.0, min(irradiance_now + rate * age, 1200.0))
    print(f"[irr_interp epoch={epoch_id}] letzte={irradiance_now}, rate={rate:.3f}/s, extrapoliert={extrapolated:.1f}", flush=True)
    return extrapolated, latest["time"]


def write_estimate_batch(df, epoch_id):
    count = df.count()
    print(f"[estimate_batch epoch={epoch_id}] Batch aufgerufen mit {count} Zeilen.", flush=True)
    if count == 0:
        return

    weather_rows = _get_latest_weather(PV_WEATHER_STATION_ID, epoch_id, "estimate_batch", limit=2)
    if not weather_rows:
        return
    irradiance, weather_time = _extrapolate_irradiance(weather_rows, epoch_id)

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

    written = jdbc_write(estimate_df, "pv_estimates", epoch_id, "estimate_batch")
    if written:
        print(f"[estimate_batch epoch={epoch_id}] irradiance={irradiance:.1f} von Wetterzeit {weather_time}.", flush=True)


def write_wind_estimate_batch(df, epoch_id):
    count = df.count()
    print(f"[wind_estimate_batch epoch={epoch_id}] Batch aufgerufen mit {count} Zeilen.", flush=True)
    if count == 0:
        return

    df = df.filter(col("actual_rpm").isNotNull())
    if df.count() == 0:
        print(f"[wind_estimate_batch epoch={epoch_id}] Alle Zeilen hatten actual_rpm=NULL - ueberspringe.", flush=True)
        return

    weather_rows = _get_latest_weather(WIND_WEATHER_STATION_ID, epoch_id, "wind_estimate_batch")
    if not weather_rows:
        return
    wind_speed = weather_rows[0]["wind_speed"]
    weather_time = weather_rows[0]["time"]
    if wind_speed is None:
        print(f"[wind_estimate_batch epoch={epoch_id}] Windgeschwindigkeit fehlt - ueberspringe.", flush=True)
        return

    estimate_df = df.select(
        col("time"),
        col("windpark_id"),
        col("actual_rpm")
    ).withColumn("station_id", lit(WIND_WEATHER_STATION_ID)) \
     .withColumn("wind_speed", lit(wind_speed)) \
     .withColumn("expected_rpm", lit(wind_speed) * WIND_RPM_FACTOR) \
     .withColumn("expected_rpm_min", col("expected_rpm") * (1 - WIND_RPM_TOLERANCE_PCT)) \
     .withColumn("expected_rpm_max", col("expected_rpm") * (1 + WIND_RPM_TOLERANCE_PCT)) \
     .withColumn("deviation_rpm", col("actual_rpm") - col("expected_rpm")) \
     .drop("expected_rpm")

    written = jdbc_write(estimate_df, "windpark_estimates", epoch_id, "wind_estimate_batch")
    if written:
        print(f"[wind_estimate_batch epoch={epoch_id}] wind_speed={wind_speed} von Wetterzeit {weather_time}.", flush=True)


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

estimate_query = parsed_energy_df.writeStream \
    .foreachBatch(write_estimate_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/pv_estimates") \
    .start()

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
        col("data.rpm").alias("actual_rpm")
    )

wind_estimate_query = parsed_windpark_df.writeStream \
    .foreachBatch(write_wind_estimate_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/windpark_estimates") \
    .start()

spark.streams.awaitAnyTermination()