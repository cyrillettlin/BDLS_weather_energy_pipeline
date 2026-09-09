"""
Weather-Producer: pollt die Open-Meteo API (kostenlos, kein API-Key nötig)
und publiziert die Werte als JSON auf ein Redpanda/Kafka-Topic.

Doku: https://open-meteo.com/en/docs
"""
import json
import logging
import os
import time

import requests
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weather-producer")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-raw")

LATITUDE = float(os.environ.get("LATITUDE", "47.0502"))
LONGITUDE = float(os.environ.get("LONGITUDE", "8.3093"))
STATION_ID = os.environ.get("STATION_ID", "luzern")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "900"))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

producer = Producer({"bootstrap.servers": KAFKA_BROKERS})


def delivery_report(err, msg):
    if err is not None:
        log.error("Zustellung fehlgeschlagen: %s", err)


def fetch_weather() -> dict:
    """
    Holt aktuelle Wetterwerte + Sonneneinstrahlung.
    Hinweis: shortwave_radiation gibt es bei Open-Meteo nur im
    hourly/minutely_15-Block, nicht im current-Block - daher separater
    Lookup auf die passende Stunde.
    """
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "hourly": "shortwave_radiation",
        "forecast_days": 1,
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    current = data["current"]
    obs_time = current["time"]
    if len(obs_time) == 16:  # "YYYY-MM-DDTHH:MM" ohne Sekunden/Zeitzone
        obs_time += ":00Z"
    elif not obs_time.endswith("Z"):
        obs_time += "Z"

    now_hour_key = current["time"][:13]  # "YYYY-MM-DDTHH"
    irradiance = None
    for t, val in zip(data["hourly"]["time"], data["hourly"]["shortwave_radiation"]):
        if t[:13] == now_hour_key:
            irradiance = val
            break

    return {
        "time": obs_time,
        "station_id": STATION_ID,
        "temperature": current.get("temperature_2m"),
        "humidity": current.get("relative_humidity_2m"),
        "wind_speed": current.get("wind_speed_10m"),
        "irradiance": irradiance,
    }


def main():
    log.info(
        "Weather-Producer gestartet: station=%s lat=%s lon=%s intervall=%ss",
        STATION_ID, LATITUDE, LONGITUDE, POLL_INTERVAL_SECONDS,
    )
    while True:
        try:
            record = fetch_weather()
            producer.produce(
                KAFKA_TOPIC,
                key=STATION_ID.encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report,
            )
            producer.flush(10)
            log.info("Wetterdaten publiziert: %s", record)
        except Exception as exc:
            log.error("Fehler beim Abrufen/Publizieren der Wetterdaten: %s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
