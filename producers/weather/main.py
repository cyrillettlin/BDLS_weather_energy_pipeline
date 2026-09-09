import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Config
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
SM_ID = os.getenv("SOLAR_MANAGER_SM_ID", "")
STATION_ID = "WEATHER_STATION_01"

print(f"Connecting to Kafka Brokers at: {KAFKA_BROKERS}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print("Main Producer started cleanly. Sending data to energy-raw and weather-raw...")

try:
    while True:
        now_ts = time.time()
        now_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        # ==========================================
        # 1. PV & ENERGY DATA (energy-raw)
        # ==========================================
        production = random.randint(1000, 3500)
        consumption = random.randint(200, 800)

        pv_payload = {
            "sm_id": SM_ID,
            "timestamp": now_ts,
            "data": {
                "lastUpdate": now_iso,
                "production": production,
                "consumption": consumption,
                "battery": {
                    "capacity": 58,
                    "batteryCharging": max(0, production - consumption),
                    "batteryDischarging": 0
                },
                "arrows": [
                    {"direction": "fromPVToGrid", "value": max(0, production - consumption)},
                    {"direction": "fromGridToConsumer", "value": 0},
                    {"direction": "fromPVToConsumer", "value": consumption}
                ]
            }
        }

        producer.send('energy-raw', key=SM_ID, value=pv_payload)

        # ==========================================
        # 2. WEATHER DATA (weather-raw)
        # ==========================================
        weather_payload = {
            "station_id": STATION_ID,
            "timestamp": now_ts,
            "temperature": round(random.uniform(15.0, 28.0), 1),
            "humidity": round(random.uniform(40.0, 80.0), 1),
            "wind_speed": round(random.uniform(2.0, 15.0), 1),
            "irradiance": round(random.uniform(200.0, 900.0), 1)
        }

        producer.send('weather-raw', key=STATION_ID, value=weather_payload)

        producer.flush()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent PV & Weather Data successfully.")
        
        time.sleep(5)

except KeyboardInterrupt:
    print("Producer stopped by user.")
except Exception as e:
    print(f"Producer error: {e}")