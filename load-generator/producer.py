"""Replay per-patient JSON files into the Kafka ``raw-topic``.

Every ``*.json`` file in ``INPUT_DIR`` holds one patient as a list of records.
The producer adds a send-time ``timestamp`` (UTC, ISO-8601 with milliseconds)
to every record and publishes it as JSON.

Configuration (environment variables, CLI flags take precedence):

``INPUT_DIR``        folder with the patient files (default ``output_json_files``)
``RATE_HZ``          fixed-rate mode: records per patient per second (default 1)
``KAFKA_BOOTSTRAP``  Kafka bootstrap servers (default ``kafka:29092``)
``RAW_TOPIC``        target topic (default ``raw-topic``)
``REPLAY_MODE``      ``fixed-rate`` (default): every tick sends the next record
                     of every patient; each patient cycles through its own
                     file independently, so files may differ in length.
                     ``original-timing``: records are sent in the order and
                     with the spacing of their ``measured_at`` field (real
                     data from ``convert_dwc.py``), accelerated by ``SPEEDUP``.
``SPEEDUP``          original-timing mode: time acceleration factor (default 1)
``LOOP``             ``1`` (default): start over at the end of the data;
                     ``0``: stop once all records were sent (``--no-loop``)
``LOG_MESSAGES``     ``1``: log every sent record (default ``0``: progress only)
"""

import argparse
import heapq
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore
from kafka.errors import (  # type: ignore
    KafkaError, KafkaTimeoutError, NoBrokersAvailable, TopicAlreadyExistsError)

# Topics used by the pipeline (producer -> flink-job -> prink job -> consumer)
PIPELINE_TOPICS = ["raw-topic", "processed-topic", "prink-topic"]


def log(msg):
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}', flush=True)


def ensure_topics(bootstrap, topics):
    """Create the pipeline topics if they do not exist yet, so that downstream
    jobs can subscribe before the first record arrives."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    except KafkaError as e:
        log(f"Could not create admin client: {e}")
        return
    try:
        existing = set(admin.list_topics())
        for name in topics:
            if name in existing:
                continue
            try:
                admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])
                log(f"Created topic {name}")
            except TopicAlreadyExistsError:
                pass
            except KafkaError as e:
                log(f"Could not create topic {name}: {e}")
    finally:
        admin.close()


def env_bool(name, default):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def utc_now_iso():
    # e.g. 2026-01-01T12:00:00.123Z (UTC, millisecond precision)
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(text):
    """Parse an ISO-8601 timestamp to epoch seconds (naive = UTC)."""
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def load_datasets(folder):
    files = sorted(f for f in os.listdir(folder) if f.endswith(".json"))
    datasets = []
    for name in files:
        with open(os.path.join(folder, name), "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            raise ValueError(f"{name}: expected a JSON list of records")
        if data:
            datasets.append(data)
        else:
            log(f"Skipping empty file {name}")
    return datasets


def fixed_rate_schedule(datasets, rate_hz, loop):
    """Yield (offset_seconds, record): one record per patient per tick; each
    patient cycles through its own records independently."""
    period = 1.0 / rate_hz
    tick = 0
    while True:
        sent_any = False
        for data in datasets:
            if tick < len(data) or loop:
                sent_any = True
                yield tick * period, data[tick % len(data)]
        if not sent_any:
            return
        tick += 1


def original_timing_schedule(datasets, rate_hz, speedup, loop):
    """Yield (offset_seconds, record) following ``measured_at`` of the records
    (merged across patients), compressed by ``speedup``. Records without a
    parseable ``measured_at`` are spaced 1/RATE_HZ after their predecessor."""
    period = 1.0 / rate_hz
    timelines = []
    for data in datasets:
        times, previous = [], None
        for record in data:
            t = parse_iso(record.get("measured_at")) if isinstance(record, dict) else None
            if t is None:
                t = 0.0 if previous is None else previous + period
            times.append(t)
            previous = t
        timelines.append(times)
    t0 = min(times[0] for times in timelines)
    t_end = max(times[-1] for times in timelines)
    cycle = (t_end - t0) + period
    offset = 0.0
    while True:
        heap = [(times[0], i, 0) for i, times in enumerate(timelines)]
        heapq.heapify(heap)
        while heap:
            t, i, j = heapq.heappop(heap)
            yield offset + (t - t0) / speedup, datasets[i][j]
            if j + 1 < len(timelines[i]):
                heapq.heappush(heap, (timelines[i][j + 1], i, j + 1))
        if not loop:
            return
        offset += cycle / speedup


def connect(bootstrap, retries=5):
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                max_block_ms=10000,  # fail if unable to send within 10 seconds
                retries=0,
            )
        except (NoBrokersAvailable, KafkaTimeoutError) as e:
            retries -= 1
            if retries <= 0:
                raise
            log(f"Kafka not available ({e}). Retrying...")
            time.sleep(5)


def run(cfg):
    datasets = load_datasets(cfg.input_dir)
    if not datasets:
        log(f"No patient files found in {cfg.input_dir}")
        return 1
    log(f"Loaded {len(datasets)} patient files from {cfg.input_dir} "
        f"(mode={cfg.replay_mode}, rate={cfg.rate_hz} Hz, speedup={cfg.speedup}, loop={cfg.loop})")

    producer = connect(cfg.kafka_bootstrap)
    ensure_topics(cfg.kafka_bootstrap, list(dict.fromkeys(PIPELINE_TOPICS + [cfg.topic])))

    if cfg.replay_mode == "original-timing":
        schedule = original_timing_schedule(datasets, cfg.rate_hz, cfg.speedup, cfg.loop)
    else:
        schedule = fixed_rate_schedule(datasets, cfg.rate_hz, cfg.loop)

    start = time.monotonic()
    sent = failed = 0
    last_report = start
    for offset, record in schedule:
        delay = start + offset - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        message = dict(record)
        message["timestamp"] = utc_now_iso()
        payload = json.dumps(message).encode("utf-8")
        try:
            producer.send(cfg.topic, value=payload)
            sent += 1
            if cfg.log_messages:
                log(f"Sent to {cfg.topic}: {payload.decode('utf-8')}")
        except KafkaError as e:
            failed += 1
            log(f"Failed to send message: {e}")
        now = time.monotonic()
        if now - last_report >= 10:
            log(f"Sent {sent} messages ({failed} failed)")
            last_report = now
    producer.flush()
    log(f"Finished: sent {sent} messages ({failed} failed)")
    return 0


def parse_config(argv=None):
    ap = argparse.ArgumentParser(description="Replay patient JSON files into Kafka.")
    ap.add_argument("--input-dir", default=os.getenv("INPUT_DIR", "output_json_files"))
    ap.add_argument("--rate-hz", type=float, default=float(os.getenv("RATE_HZ", "1")))
    ap.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"))
    ap.add_argument("--topic", default=os.getenv("RAW_TOPIC", "raw-topic"))
    ap.add_argument("--replay-mode", choices=("fixed-rate", "original-timing"),
                    default=os.getenv("REPLAY_MODE", "fixed-rate"))
    ap.add_argument("--speedup", type=float, default=float(os.getenv("SPEEDUP", "1")))
    ap.add_argument("--loop", dest="loop", action="store_true", default=env_bool("LOOP", True))
    ap.add_argument("--no-loop", dest="loop", action="store_false")
    ap.add_argument("--log-messages", action="store_true", default=env_bool("LOG_MESSAGES", False))
    cfg = ap.parse_args(argv)
    if cfg.rate_hz <= 0 or cfg.speedup <= 0:
        ap.error("RATE_HZ and SPEEDUP must be > 0")
    return cfg


def main(argv=None):
    cfg = parse_config(argv)
    try:
        return run(cfg)
    except (NoBrokersAvailable, KafkaTimeoutError) as e:
        log(f"Failed to connect to Kafka broker after multiple attempts: {e}")
    except Exception as e:  # noqa: BLE001
        log(f"Error in producer: {e}")
        print(traceback.format_exc(), flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
