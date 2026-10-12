#!/usr/bin/env python3
"""Per-stage latency and throughput of the pipeline, computed from the Kafka topics.

Reads the records of a time window from raw-topic, processed-topic and the
Prink output topic and prints AGGREGATE statistics only (counts, latency
percentiles, throughput) - never record contents, keys or identifiers, so it
can be run on real data.

Every stage stamps its output: the producer adds the field ``timestamp`` (send
time, UTC ISO-8601 ms) and sets the Kafka CreateTime to the same instant;
flink-job and the Prink job set the CreateTime of their output records to the
time they write them (the producer ``timestamp`` field is passed through).
From this the script derives

  end-to-end  producer -> <topic>   CreateTime(topic record) - timestamp field
  per stage   raw -> processed       CreateTime(processed) - CreateTime(raw)
              processed -> prink     CreateTime(prink) - CreateTime(processed)

Per-stage values need the same record in both topics; records are matched by
(recordid, timestamp) in memory (the Prink profile must pass both through, as
the bundled profiles do). The upstream topics are read ``--lookback-seconds``
further back than the window, so records that spent longer than the window in
a stage (e.g. Prink's delta buffer) still find their counterpart; set it above
the expected stage latency. All timestamps come from the clock of one machine
(single-host stack), so no clock offset is involved.

Records produced before the stack was changed (e.g. Flink sinks that copied
the input CreateTime) show a per-stage latency of ~0.

Usage (needs confluent-kafka; inside the stack the consumer image has it):

  docker compose run --rm --no-deps -v "$PWD/scripts:/scripts:ro" kafka-consumer \\
      python /scripts/latency_from_topics.py --bootstrap kafka:29092 --since-seconds 120

  python3 scripts/latency_from_topics.py --bootstrap localhost:9092 --prink-topic prink-icu-qi-v2
"""

import argparse
import json
import math
import sys
import time
import uuid
from datetime import datetime, timezone

STAGES = ("raw", "processed", "prink")


def parse_iso_ms(text):
    """ISO-8601 timestamp -> epoch milliseconds (naive = UTC), None if invalid."""
    if not isinstance(text, str) or not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(round(dt.timestamp() * 1000))


def percentile(sorted_values, q):
    """Nearest-rank percentile (q in [0, 100]) of an ascending list."""
    if not sorted_values:
        return None
    rank = max(1, int(math.ceil(q / 100.0 * len(sorted_values))))
    return sorted_values[min(rank, len(sorted_values)) - 1]


def summarize(values):
    """count, mean, p50, p95, p99, max of a list of numbers."""
    values = sorted(values)
    if not values:
        return {"n": 0}
    return {"n": len(values), "mean": sum(values) / len(values), "p50": percentile(values, 50),
            "p95": percentile(values, 95), "p99": percentile(values, 99), "max": values[-1]}


class TopicSample:
    """Timestamps of the records of one topic (no contents are kept).

    Records with CreateTime before ``window_start_ms`` (lookback) are only
    kept for matching (``by_key``), not counted in the statistics."""

    def __init__(self, window_start_ms=None):
        self.window_start_ms = window_start_ms
        self.create_ms = []      # Kafka CreateTime per record in the window
        self.e2e_ms = []         # CreateTime - producer timestamp field
        self.by_key = {}         # (recordid, timestamp) -> CreateTime, incl. lookback
        self.window_keys = []    # keys of the records in the window
        self.unparseable = 0
        self.read = 0            # records read, incl. lookback

    def add(self, value_bytes, create_ms):
        self.read += 1
        in_window = self.window_start_ms is None or create_ms >= self.window_start_ms
        if in_window:
            self.create_ms.append(create_ms)
        try:
            record = json.loads(value_bytes)
        except (ValueError, TypeError):
            self.unparseable += 1
            return
        if not isinstance(record, dict):
            if in_window:
                self.unparseable += 1
            return
        sent = parse_iso_ms(record.get("timestamp"))
        if sent is None:
            return
        rid = record.get("recordid")
        key = None if rid is None else (str(rid), record.get("timestamp"))
        if key is not None:
            self.by_key.setdefault(key, create_ms)
        if in_window:
            self.e2e_ms.append(create_ms - sent)
            if key is not None:
                self.window_keys.append(key)

    def throughput(self):
        if len(self.create_ms) < 2:
            return None
        span = (max(self.create_ms) - min(self.create_ms)) / 1000.0
        return len(self.create_ms) / span if span > 0 else None


def stage_latencies(upstream, downstream):
    """CreateTime differences of the downstream records in the window that
    have a counterpart upstream (window or lookback)."""
    out = []
    for key in dict.fromkeys(downstream.window_keys):
        t_up = upstream.by_key.get(key)
        if t_up is not None:
            out.append(downstream.by_key[key] - t_up)
    return out


def read_topic(bootstrap, topic, since_ms, max_records, timeout_s, lookback_ms=0):
    """Read the records of ``topic`` with CreateTime >= since_ms - lookback_ms
    (or all if since_ms is None) up to the end offsets at call time."""
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

    sample = TopicSample(since_ms)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "latency-script-" + uuid.uuid4().hex[:8],
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest",
    })
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        tmeta = metadata.topics.get(topic)
        if tmeta is None or tmeta.error is not None or not tmeta.partitions:
            print(f"  {topic}: not found", file=sys.stderr)
            return sample
        partitions = sorted(tmeta.partitions)
        ends = {}
        starts = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            ends[p] = high
            starts[p] = low
        if since_ms is not None:
            found = consumer.offsets_for_times(
                [TopicPartition(topic, p, since_ms - lookback_ms) for p in partitions], timeout=10)
            for tp in found:
                starts[tp.partition] = tp.offset if tp.offset >= 0 else ends[tp.partition]
        pending = {p for p in partitions if starts[p] < ends[p]}
        if not pending:
            return sample
        consumer.assign([TopicPartition(topic, p, starts[p]) for p in pending])
        deadline = time.monotonic() + timeout_s
        while pending and sample.read < max_records and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pending.discard(msg.partition())
                    continue
                raise KafkaException(msg.error())
            if msg.offset() >= ends[msg.partition()]:
                pending.discard(msg.partition())
                continue
            ts_type, ts = msg.timestamp()
            if ts_type != 0 and ts >= 0:  # 0 = TIMESTAMP_NOT_AVAILABLE
                sample.add(msg.value(), ts)
            if msg.offset() + 1 >= ends[msg.partition()]:
                pending.discard(msg.partition())
    finally:
        consumer.close()
    return sample


def fmt(value):
    return "-" if value is None else f"{value:.0f}"


def report(samples, topics):
    lines = ["", "Throughput (records in the window, by Kafka CreateTime)",
             f"  {'topic':<24} {'records':>9} {'msg/s':>9}"]
    for stage in STAGES:
        s = samples[stage]
        tp = s.throughput()
        lines.append(f"  {topics[stage]:<24} {len(s.create_ms):>9} {('-' if tp is None else f'{tp:.1f}'):>9}"
                     + (f"   ({s.unparseable} not JSON)" if s.unparseable else ""))

    rows = [
        ("producer -> " + topics["raw"], samples["raw"].e2e_ms),
        ("producer -> " + topics["processed"], samples["processed"].e2e_ms),
        ("producer -> " + topics["prink"], samples["prink"].e2e_ms),
        ("stage flink-job (raw -> processed)", stage_latencies(samples["raw"], samples["processed"])),
        ("stage Prink (processed -> prink)", stage_latencies(samples["processed"], samples["prink"])),
    ]
    lines += ["", "Latency in ms",
              f"  {'':<38} {'n':>8} {'mean':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}"]
    for name, values in rows:
        st = summarize(values)
        lines.append(f"  {name:<38} {st['n']:>8} " + " ".join(
            f"{fmt(st.get(k)):>8}" for k in ("mean", "p50", "p95", "p99", "max")))
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--raw-topic", default="raw-topic")
    ap.add_argument("--processed-topic", default="processed-topic")
    ap.add_argument("--prink-topic", default="prink-topic")
    window = ap.add_mutually_exclusive_group()
    window.add_argument("--since-seconds", type=float, default=120.0,
                        help="read records written in the last N seconds (default 120)")
    window.add_argument("--from-beginning", action="store_true", help="read the whole topics")
    ap.add_argument("--lookback-seconds", type=float, default=120.0,
                    help="read raw/processed this much earlier to match records across stages; "
                         "set above the stage latency (default 120)")
    ap.add_argument("--max-records", type=int, default=1000000,
                    help="records read per topic, incl. lookback (default 1000000)")
    ap.add_argument("--timeout", type=float, default=120.0, help="seconds per topic (default 120)")
    args = ap.parse_args(argv)

    topics = {"raw": args.raw_topic, "processed": args.processed_topic, "prink": args.prink_topic}
    since_ms = None if args.from_beginning else int((time.time() - args.since_seconds) * 1000)
    lookback_ms = int(args.lookback_seconds * 1000)
    samples = {}
    for stage in STAGES:
        samples[stage] = read_topic(args.bootstrap, topics[stage], since_ms, args.max_records, args.timeout,
                                    lookback_ms=0 if stage == "prink" else lookback_ms)
    window_text = "whole topics" if since_ms is None else f"last {args.since_seconds:.0f} s"
    print(f"Window: {window_text} (lookback {args.lookback_seconds:.0f} s for matching), "
          f"at most {args.max_records} records read per topic")
    for stage in STAGES:
        if samples[stage].read >= args.max_records:
            print(f"  note: {topics[stage]} hit --max-records; statistics cover only part of the window")
    print(report(samples, topics))
    return 0


if __name__ == "__main__":
    sys.exit(main())
