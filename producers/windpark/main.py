import os
import json
import logging
import threading
import time
from collections import deque

import cv2
import numpy as np
from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("windpark-producer")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "windpark-raw")
WINDPARK_ID = os.environ["WINDPARK_ID"]
PUBLISH_INTERVAL_SECONDS = float(
    os.environ.get("WINDPARK_PUBLISH_INTERVAL_SECONDS", "60")
)
STREAM_URL = (
    "https://terra-livestream.eu/"
    "dlr-windenergie-live/dlr-windenergie-live.m3u8"
)

# Square in which a passing red blade is detected.
GATE_X, GATE_Y, GATE_SIZE = 1000, 180, 240
MIN_RED_AREA = 100
MIN_SATURATION, MIN_VALUE = 100, 80

SAMPLE_INTERVAL = 0.25
DISPLAY_INTERVAL = 1 / 30
SMOOTHING_WINDOW = 15.0
NUMBER_OF_BLADES = 3

producer = Producer({"bootstrap.servers": KAFKA_BROKERS})

def delivery_report(error, message):
    if error is not None:
        log.error("Kafka delivery failed: %s", error)

def open_stream():
    capture = cv2.VideoCapture(STREAM_URL, cv2.CAP_FFMPEG)
    capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    return capture

class LatestFrame:
    def __init__(self):
        self.capture = open_stream()
        if not self.capture.isOpened():
            raise RuntimeError("Livestream could not be opened.")
        self.frame = None
        self.lock = threading.Lock()
        self.running = True
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        while self.running:
            success, frame = self.capture.read()
            if success:
                with self.lock:
                    self.frame = frame
            else:
                self.capture.release()
                log.warning("Livestream interrupted; reconnecting")
                time.sleep(0.25)
                self.capture = open_stream()

    def snapshot(self):
        with self.lock:
            return None if self.frame is None else self.frame.copy()

    def close(self):
        self.running = False
        self.capture.release()
        self.thread.join(timeout=2)

def detect_blade(frame):
    gate = frame[GATE_Y : GATE_Y + GATE_SIZE, GATE_X : GATE_X + GATE_SIZE]
    hsv = cv2.cvtColor(gate, cv2.COLOR_BGR2HSV)
    red = cv2.inRange(hsv, (0, MIN_SATURATION, MIN_VALUE), (12, 255, 255))
    red |= cv2.inRange(hsv, (170, MIN_SATURATION, MIN_VALUE), (179, 255, 255))
    red = cv2.morphologyEx(red, cv2.MORPH_OPEN, np.ones((3, 3), np.uint8))
    count, _, stats, _ = cv2.connectedComponentsWithStats(red, connectivity=8)
    largest_area = max(stats[1:, cv2.CC_STAT_AREA], default=0) if count > 1 else 0
    return largest_area >= MIN_RED_AREA, int(largest_area), red

def calculate_rpm(passages):
    if len(passages) < 2:
        return None
    duration = passages[-1] - passages[0]
    blade_passes = len(passages) - 1
    return blade_passes * 60 / (duration * NUMBER_OF_BLADES) if duration > 0 else None

def publish_rpm(rpm):
    record = {
        "windpark_id": WINDPARK_ID,
        "timestamp": time.time(),
        "rpm": rpm,
    }
    producer.produce(
        KAFKA_TOPIC,
        key=WINDPARK_ID.encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush(10)
    log.info("Published RPM for %s: %s", WINDPARK_ID, rpm)

def main():
    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
        "fflags;nobuffer|flags;low_delay|avioflags;direct"
    )
    stream = LatestFrame()
    passages = deque()
    was_present = False
    last_rpm = None
    last_sample = 0.0
    next_publish = time.monotonic() + PUBLISH_INTERVAL_SECONDS

    log.info(
        "Windpark producer started for %s (publish interval: %ss)",
        WINDPARK_ID,
        PUBLISH_INTERVAL_SECONDS,
    )
    try:
        while True:
            now = time.monotonic()
            if now - last_sample >= SAMPLE_INTERVAL:
                last_sample = now
                frame = stream.snapshot()
                if frame is not None:
                    try:
                        blade_present, _, _ = detect_blade(frame)
                        if blade_present and not was_present:
                            passages.append(now)
                        was_present = blade_present

                        while (
                            len(passages) > 1
                            and passages[1] < now - SMOOTHING_WINDOW
                        ):
                            passages.popleft()
                        new_rpm = calculate_rpm(passages)
                        if new_rpm is not None:
                            last_rpm = new_rpm
                    except cv2.error as error:
                        log.warning("Frame processing failed; keeping last RPM: %s", error)

            if now >= next_publish:
                try:
                    publish_rpm(last_rpm)
                except Exception as error:
                    log.error("RPM publish failed; will retry with last value: %s", error)
                next_publish = now + PUBLISH_INTERVAL_SECONDS

            time.sleep(DISPLAY_INTERVAL)
    finally:
        stream.close()
        producer.flush(10)
        log.info("Windpark producer stopped")

if __name__ == "__main__":
    main()
