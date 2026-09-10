import os

# ---------------------------------------------------------------------
# Konfiguration & Credentials (gemeinsam fuer Ingest- und Estimate-Job)
# ---------------------------------------------------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
DB_URL = "jdbc:postgresql://timescaledb:5432/pipeline_db"
DB_USER = "pipeline"
DB_PASSWORD = "pipeline"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_PROPS = {"user": DB_USER, "password": DB_PASSWORD, "driver": JDBC_DRIVER}

# Diese Werte werden nur vom estimate_job gebraucht. Der ingest_job setzt sie
# im Container nicht (siehe docker-compose), daher hier optional mit os.getenv
# statt os.environ[...] - sonst crasht der Ingest-Job schon beim Import.
PV_PEAK_POWER_W = float(os.getenv("PV_PEAK_POWER_W", "5000"))
PERFORMANCE_RATIO = float(os.getenv("PV_PERFORMANCE_RATIO", "0.80"))
TOLERANCE_PCT = float(os.getenv("PV_TOLERANCE_PCT", "0.15"))
PV_WEATHER_STATION_ID = os.getenv("PV_WEATHER_STATION_ID")
WIND_WEATHER_STATION_ID = os.getenv("WIND_WEATHER_STATION_ID")
WIND_RPM_MIN_FACTOR = float(os.getenv("WIND_RPM_MIN_FACTOR", "0"))
WIND_RPM_MAX_FACTOR = float(os.getenv("WIND_RPM_MAX_FACTOR", "0"))
WEATHER_POLL_INTERVAL_SECONDS = int(os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "900"))
MAX_WEATHER_AGE_SECONDS = int(
    os.getenv("PV_MAX_WEATHER_AGE_SECONDS", str(WEATHER_POLL_INTERVAL_SECONDS * 2))
)


def jdbc_write(df, table, epoch_id, log_prefix):
    """Gemeinsame JDBC-Write-Hilfsfunktion fuer foreachBatch-Handler."""
    count = df.count()
    if count == 0:
        return count
    try:
        df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", table) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", JDBC_DRIVER) \
            .mode("append") \
            .save()
        print(f"[{log_prefix} epoch={epoch_id}] {count} Zeilen geschrieben.", flush=True)
    except Exception as e:
        print(f"[{log_prefix} epoch={epoch_id}] JDBC write failed: {e}", flush=True)
    return count