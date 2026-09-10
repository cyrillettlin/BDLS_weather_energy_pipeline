import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-meteo-producer")

# ---------------------------------------------------------------------
# Konfiguration (aus .env / docker-compose environment)
# ---------------------------------------------------------------------
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-raw")

LATITUDE = os.environ.get("LATITUDE", "47.3508")
LONGITUDE = os.environ.get("LONGITUDE", "8.2435")
STATION_ID = os.environ.get("STATION_ID", "villmergen")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "900"))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# "current"-Parameter der Open-Meteo API:
# https://open-meteo.com/en/docs -> Abschnitt "Current Weather Variables"
CURRENT_PARAMS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "shortwave_radiation",  # Globalstrahlung (GHI) in W/m^2 - Basis fuer die PV-Schaetzung
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)


def fetch_current_weather() -> dict:
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current": ",".join(CURRENT_PARAMS),
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    current = payload.get("current", {})
    if not current:
        raise ValueError(f"Open-Meteo Antwort enthaelt keinen 'current'-Block: {payload}")

    # Open-Meteo liefert 'time' als ISO-String ohne Zeitzonen-Suffix (UTC,
    # da timezone=UTC gesetzt ist). Wir konvertieren auf Unix-Epoch-Sekunden,
    # damit das Format zum bestehenden Spark-Schema passt.
    obs_time = current.get("time")
    if obs_time:
        dt = datetime.fromisoformat(obs_time).replace(tzinfo=timezone.utc)
        ts = dt.timestamp()
    else:
        ts = time.time()

    return {
        "station_id": STATION_ID,
        "timestamp": ts,
        "temperature": current.get("temperature_2m"),
        "humidity": current.get("relative_humidity_2m"),
        "wind_speed": current.get("wind_speed_10m"),
        "irradiance": current.get("shortwave_radiation"),
    }


def delivery_report(err, msg):
    if err is not None:
        log.error("Zustellung fehlgeschlagen: %s", err)


def main():
    log.info(
        "Open-Meteo Producer gestartet fuer Station '%s' (lat=%s, lon=%s, Intervall=%ss)",
        STATION_ID, LATITUDE, LONGITUDE, POLL_INTERVAL_SECONDS,
    )

    while True:
        try:
            record = fetch_current_weather()
            producer.send(KAFKA_TOPIC, key=STATION_ID, value=record)
            producer.flush()
            log.info("Wetterdaten publiziert: %s", record)
        except Exception as exc:
            log.error("Fehler beim Abrufen/Senden der Open-Meteo-Daten: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()