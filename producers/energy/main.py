import json
import logging
import os
import time
import requests
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("solar-manager-producer")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "energy-raw")

SM_USER = os.environ.get("SOLAR_MANAGER_USER", "")
SM_PASSWORD = os.environ.get("SOLAR_MANAGER_PASSWORD", "")
SM_ID = os.environ.get("SOLAR_MANAGER_SM_ID", "0033000C3133510F35303638")
POLL_INTERVAL_SECONDS = int(os.environ.get("SOLAR_MANAGER_POLL_INTERVAL_SECONDS", "60"))

BASE_URL = "https://cloud.solar-manager.ch/v1"

producer = Producer({"bootstrap.servers": KAFKA_BROKERS})

def delivery_report(err, msg):
    if err is not None:
        log.error("Zustellung fehlgeschlagen: %s", err)

def fetch_solar_manager_data() -> dict:
    if not SM_USER or not SM_PASSWORD:
        raise ValueError("SOLAR_MANAGER_USER und SOLAR_MANAGER_PASSWORD muessen gesetzt sein.")

    url = f"{BASE_URL}/chart/gateway/{SM_ID}"
    
    resp = requests.get(
        url,
        auth=(SM_USER, SM_PASSWORD),
        headers={"accept": "application/json"},
        timeout=10
    )
    resp.raise_for_status()
    data = resp.json()

    return {
        "sm_id": SM_ID,
        "timestamp": time.time(),
        "data": data
    }

def main():
    if not SM_USER or not SM_PASSWORD:
        log.warning("Keine Solar Manager Zugangsdaten angegeben. Beende Container...")
        return

    log.info("Solar Manager Producer gestartet für Gateway: %s (Intervall: %ss)", SM_ID, POLL_INTERVAL_SECONDS)
    
    while True:
        try:
            record = fetch_solar_manager_data()
            producer.produce(
                KAFKA_TOPIC,
                key=SM_ID.encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report,
            )
            producer.flush(10)
            log.info("Solar Manager Daten publiziert fuer SM-ID: %s", SM_ID)
        except Exception as exc:
            log.error("Fehler beim Abrufen der Solar Manager Daten: %s", exc)
            
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()