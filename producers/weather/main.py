import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-meteo-producer")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-raw")
WEATHER_LOCATIONS = json.loads(
    os.environ.get(
        "WEATHER_LOCATIONS",
        '[]',
    )
)
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "900"))
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# "current"-Parameter der Open-Meteo API:
# https://open-meteo.com/en/docs -> Abschnitt "Current Weather Variables"
CURRENT_PARAMS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "shortwave_radiation",  # Globalstrahlung (GHI) in W/m^2 - Basis fuer die PV-Schätzung
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)


def fetch_current_weather(location: dict) -> dict:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": ",".join(CURRENT_PARAMS),
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    current = payload.get("current", {})
    if not current:
        raise ValueError(f"In der Response von Open-Meteo ist 'current' leer: {payload}")

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
        "station_id": location["station_id"],
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
    if len(WEATHER_LOCATIONS) == 0:
        log.error("Keine Wetterstation konfiguriert, Producer wird beendet.")
        return
    
    log.info(
        "Open-Meteo Producer gestartet fuer %s (Intervall=%ss)",
        ", ".join(location["station_id"] for location in WEATHER_LOCATIONS),
        POLL_INTERVAL_SECONDS,
    )

    while True:
        for location in WEATHER_LOCATIONS:
            try:
                record = fetch_current_weather(location)
                producer.send(
                    KAFKA_TOPIC,
                    key=location["station_id"],
                    value=record,
                )
                producer.flush()
                log.info("Wetterdaten publiziert: %s", record)
            except Exception as exc:
                log.error(
                    "Fehler beim Abrufen/Senden für Station '%s': %s",
                    location.get("station_id", "unbekannt"),
                    exc,
                )

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()