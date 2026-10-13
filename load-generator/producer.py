"""Replay per-patient record files into the Kafka ``raw-topic``.

Input: every record file found *recursively* under ``INPUT_DIR`` holds one
patient. Sub-folders (e.g. one per unit/ward) are allowed and may be added
while the producer runs (see ``RESCAN_SECONDS``). Supported formats:

``*.json`` / ``*.json.gz``        JSON array of records (legacy format; one
                                  file is loaded at a time per patient)
``*.jsonl`` / ``*.jsonl.gz``      JSON Lines, one record per line; streamed
``*.ndjson`` / ``*.ndjson.gz``    line by line, never loaded as a whole

Hidden files and folders, files whose name contains ``manifest`` and files
with other extensions (e.g. ``*.part`` while a file is still being written)
are ignored. Only one open reader per patient is kept, so memory use does not
grow with the size of the data set (except for legacy ``*.json`` arrays).

The producer adds a send-time ``timestamp`` (UTC, ISO-8601 with milliseconds)
to every record, publishes it as JSON keyed by ``userid`` (so all records of a
patient go to the same partition, in order) and sets the Kafka record
timestamp (CreateTime) to the same instant.

Configuration (environment variables, CLI flags take precedence):

``INPUT_DIR``        folder with the patient files (default ``output_json_files``)
``RATE_HZ``          fixed-rate mode: records per patient per second (default 1)
``KAFKA_BOOTSTRAP``  Kafka bootstrap servers (default ``kafka:29092``)
``RAW_TOPIC``        target topic (default ``raw-topic``)
``REPLAY_MODE``      ``fixed-rate`` (default): every tick sends the next record
                     of every patient; each patient cycles through its own
                     file independently, so files may differ in length.
                     ``original-timing``: records are sent with the spacing of
                     their ``measured_at`` field (real data from
                     ``convert_dwc.py``), accelerated by ``SPEEDUP``. Patients
                     present at start keep their relative alignment.
``SPEEDUP``          original-timing mode: time acceleration factor (default 1)
``LOOP``             ``1`` (default): every patient starts over at the end of
                     its file; ``0``: a patient stops after its last record
                     (``--no-loop``)
``RESCAN_SECONDS``   ``> 0``: rescan ``INPUT_DIR`` every N seconds and start
                     replaying newly added patient files (default ``0`` = off).
                     With rescanning the producer keeps running even when all
                     patients have finished (``LOOP=0``), waiting for new files.
``STATS_SECONDS``    interval of the throughput log line (default 10)
``LOG_MESSAGES``     ``1``: log every sent record (default ``0``: aggregate
                     progress only; record contents and file names are never
                     logged otherwise)
``TOPIC_PARTITIONS`` partitions of topics created by the producer (default 1;
                     normally ``kafka-init`` creates the topics)
``KAFKA_COMPRESSION`` ``lz4`` (default if the lz4 package is installed),
                     ``gzip``, ``snappy``, ``zstd`` or ``none``
``KAFKA_LINGER_MS``  producer batching delay (default 20)
``KAFKA_BATCH_SIZE`` producer batch size in bytes (default 262144)
"""

import argparse
import gzip
import heapq
import json
import os
import sys
import time
import traceback
import zlib
from datetime import datetime, timedelta, timezone

try:  # kafka-python is only needed for sending (the unit tests run without it)
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore
    from kafka.errors import (  # type: ignore
        KafkaError, KafkaTimeoutError, NoBrokersAvailable, TopicAlreadyExistsError)
except ImportError:  # pragma: no cover - depends on the environment
    KafkaProducer = KafkaAdminClient = NewTopic = None

    class KafkaError(Exception):
        pass

    class KafkaTimeoutError(KafkaError):
        pass

    class NoBrokersAvailable(KafkaError):
        pass

    class TopicAlreadyExistsError(KafkaError):
        pass

# Topics used by the pipeline (producer -> flink-job -> prink job -> consumer)
PIPELINE_TOPICS = ["raw-topic", "processed-topic", "prink-topic"]

# Longest suffixes first (".jsonl.gz" before ".json")
RECORD_SUFFIXES = (".jsonl.gz", ".ndjson.gz", ".json.gz", ".jsonl", ".ndjson", ".json")


def log(msg):
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}', flush=True)


def env_bool(name, default):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def iso_from_ms(epoch_ms):
    """UTC ISO-8601 with millisecond precision, e.g. 2026-01-01T12:00:00.123Z."""
    dt = datetime.fromtimestamp(epoch_ms // 1000, timezone.utc) + timedelta(milliseconds=epoch_ms % 1000)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now_iso():
    return iso_from_ms(int(time.time() * 1000))


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


# ---------------------------------------------------------------------------
# Input discovery and streaming


def is_record_file(name):
    """True for patient record files (see module docstring)."""
    lower = name.lower()
    if lower.startswith(".") or "manifest" in lower:
        return False
    return lower.endswith(RECORD_SUFFIXES)


def discover_files(root):
    """All record files below ``root`` (recursive, sorted, symlinks followed
    once). Hidden folders are skipped."""
    found = []
    seen_dirs = set()
    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        real = os.path.realpath(dirpath)
        if real in seen_dirs:  # symlink loop
            dirnames[:] = []
            continue
        seen_dirs.add(real)
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for name in sorted(filenames):
            if is_record_file(name) and os.path.isfile(os.path.join(dirpath, name)):
                found.append(os.path.join(dirpath, name))
    return found


def iter_file_records(path, on_bad_line=None):
    """Yield the records (dicts) of one patient file.

    JSON Lines files (optionally gzip-compressed) are streamed line by line;
    legacy JSON arrays are loaded as a whole. Lines that are not JSON objects
    are skipped (``on_bad_line`` is called for each of them).
    """
    lower = path.lower()
    compressed = lower.endswith(".gz")
    base = lower[:-3] if compressed else lower
    opener = gzip.open if compressed else open
    with opener(path, "rt", encoding="utf-8") as fh:
        if base.endswith(".json"):
            data = json.load(fh)
            if not isinstance(data, list):
                raise ValueError("expected a JSON array of records")
            for record in data:
                if isinstance(record, dict):
                    yield record
                elif on_bad_line:
                    on_bad_line()
            return
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                record = None
            if isinstance(record, dict):
                yield record
            elif on_bad_line:
                on_bad_line()


class PatientStream:
    """Lazy reader of one patient file that can cycle through it."""

    def __init__(self, path, index):
        self.path = path
        self.index = index          # used in log lines instead of the file name
        self.passes = 0             # completed passes through the file
        self.records_in_pass = 0
        self.finished = False
        self.error = None           # exception class name if the file could not be read
        self.bad_lines = 0
        self._it = None

    def _count_bad_line(self):
        self.bad_lines += 1

    def next_record(self, loop):
        """The next record, starting over at the end of the file if ``loop``.
        Returns None once the patient is finished (end of file without loop,
        empty file or read error)."""
        while not self.finished:
            if self._it is None:
                self._it = iter_file_records(self.path, self._count_bad_line)
                self.records_in_pass = 0
            try:
                record = next(self._it)
            except StopIteration:
                self._it = None
                empty = self.records_in_pass == 0
                self.passes += 1
                if empty or not loop:
                    self.finished = True
                continue
            except (OSError, EOFError, ValueError, zlib.error, UnicodeDecodeError) as e:
                self.error = type(e).__name__
                self.close()
                self.finished = True
                continue
            self.records_in_pass += 1
            return record
        return None

    @property
    def new_pass(self):
        """True right after the first record of a repeated pass was returned."""
        return self.passes > 0 and self.records_in_pass == 1

    def close(self):
        if self._it is not None:
            self._it.close()
            self._it = None


class PatientPool:
    """The patient files under ``input_dir``; discovers new files on rescan."""

    def __init__(self, input_dir, rescan_seconds=0.0):
        self.input_dir = input_dir
        self.rescan_seconds = rescan_seconds
        self.patients = []
        self._known = set()
        self.next_rescan = rescan_seconds

    def scan(self):
        """Add record files not seen before; returns the new PatientStreams."""
        new = []
        for path in discover_files(self.input_dir):
            if path in self._known:
                continue
            self._known.add(path)
            patient = PatientStream(path, len(self.patients))
            self.patients.append(patient)
            new.append(patient)
        return new

    @property
    def rescanning(self):
        return self.rescan_seconds > 0

    def maybe_rescan(self, offset):
        """Rescan if the schedule offset (seconds since start) reached the
        next rescan time; returns the new patients."""
        if not self.rescanning or offset < self.next_rescan:
            return []
        self.next_rescan = offset + self.rescan_seconds
        new = self.scan()
        if new:
            log(f"Rescan: {len(new)} new patient files ({len(self.patients)} in total)")
        return new

    def active(self):
        return sum(1 for p in self.patients if not p.finished)

    def close(self):
        for p in self.patients:
            p.close()


# ---------------------------------------------------------------------------
# Schedules: generators of (offset_seconds, patient, record). patient/record
# are None for idle steps (nothing to send, wait until offset).


def fixed_rate_schedule(pool, rate_hz, loop):
    """One record per active patient per tick; every patient cycles through its
    own file independently. Patients added by a rescan start at the next tick."""
    period = 1.0 / rate_hz
    tick = 0
    while True:
        offset = tick * period
        pool.maybe_rescan(offset)
        sent_any = False
        for patient in list(pool.patients):
            if patient.finished:
                continue
            record = patient.next_record(loop)
            if record is None:
                continue
            sent_any = True
            yield offset, patient, record
        if not sent_any:
            if not pool.rescanning:
                return
            yield offset, None, None
        tick += 1


def original_timing_schedule(pool, rate_hz, speedup, loop):
    """Records follow their ``measured_at`` (merged across patients),
    compressed by ``speedup``.

    Patients present at start share one time origin (the earliest first
    ``measured_at``), so their relative alignment is kept. A patient found by
    a rescan starts when it is found. Records without a parseable
    ``measured_at`` are spaced 1/RATE_HZ after their predecessor; times going
    backwards within a file are clamped. With ``loop`` every patient starts
    over after its last record (gap 1/RATE_HZ in data time).
    """
    period = 1.0 / rate_hz
    state = {}      # patient index -> [origin, base_offset, previous_time]
    heap = []
    seq = [0]

    def data_time(patient, record):
        st = state[patient.index]
        t = parse_iso(record.get("measured_at"))
        previous = st[2]
        if t is None:
            t = st[0] if previous is None else previous + period
        elif previous is not None and t < previous:
            t = previous
        st[2] = t
        return t

    def push(patient, record):
        st = state[patient.index]
        if patient.new_pass:  # start the next cycle right after the previous one
            st[1] += (st[2] - st[0] + period) / speedup
            st[2] = None
        t = data_time(patient, record)
        seq[0] += 1
        heapq.heappush(heap, (st[1] + (t - st[0]) / speedup, seq[0], patient, record))

    def push_next(patient):
        record = patient.next_record(loop)
        if record is not None:
            push(patient, record)

    # initial patients: common origin = earliest first measured_at
    firsts = [(p, p.next_record(loop)) for p in pool.patients if not p.finished]
    firsts = [(p, r) for p, r in firsts if r is not None]
    times = [parse_iso(r.get("measured_at")) for _, r in firsts]
    times = [t for t in times if t is not None]
    t0 = min(times) if times else 0.0
    for patient, record in firsts:
        state[patient.index] = [t0, 0.0, None]
        push(patient, record)

    while True:
        next_offset = heap[0][0] if heap else float("inf")
        if pool.rescanning and pool.next_rescan <= next_offset:
            now = pool.next_rescan
            yield now, None, None
            for patient in pool.maybe_rescan(now):
                record = patient.next_record(loop)
                if record is None:
                    continue
                t = parse_iso(record.get("measured_at"))
                state[patient.index] = [t if t is not None else 0.0, now, None]
                push(patient, record)
            continue
        if not heap:
            return
        offset, _, patient, record = heapq.heappop(heap)
        yield offset, patient, record
        push_next(patient)


# ---------------------------------------------------------------------------
# Kafka


def ensure_topics(bootstrap, topics, partitions=1):
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
                admin.create_topics([NewTopic(name=name, num_partitions=partitions, replication_factor=1)])
                log(f"Created topic {name} ({partitions} partitions)")
            except TopicAlreadyExistsError:
                pass
            except KafkaError as e:
                log(f"Could not create topic {name}: {e}")
    finally:
        admin.close()


def resolve_compression(name):
    """Compression codec for kafka-python; lz4 falls back to gzip if the lz4
    package is missing."""
    name = (name or "lz4").strip().lower()
    if name in ("none", "", "off"):
        return None
    if name == "lz4":
        try:
            import lz4.frame  # noqa: F401
        except ImportError:
            log("lz4 package not installed, using gzip compression")
            return "gzip"
    return name


def connect(cfg, retries=5):
    if KafkaProducer is None:
        raise RuntimeError("kafka-python is not installed")
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=cfg.kafka_bootstrap,
                acks=1,
                compression_type=resolve_compression(cfg.compression),
                linger_ms=cfg.linger_ms,
                batch_size=cfg.batch_size,
                buffer_memory=64 * 1024 * 1024,
                # one request in flight per connection keeps per-partition order on retries
                retries=3,
                max_in_flight_requests_per_connection=1,
                max_block_ms=10000,  # send() fails if the buffer stays full for 10 s
            )
        except (NoBrokersAvailable, KafkaTimeoutError) as e:
            retries -= 1
            if retries <= 0:
                raise
            log(f"Kafka not available ({e}). Retrying...")
            time.sleep(5)


class Stats:
    """Aggregate throughput logging (never record contents)."""

    def __init__(self, interval, show_paths_below=None):
        self.interval = interval
        self.show_paths_below = show_paths_below  # log file paths (relative to it) only if set
        self._reported = set()
        self.sent = 0
        self.failed = 0          # send() raised
        self.async_failed = 0    # delivery failed in the background
        self.behind = 0.0        # max seconds behind schedule in this interval
        self._last_time = time.monotonic()
        self._last_sent = 0

    def on_error(self, exc):
        self.async_failed += 1
        if self.async_failed <= 5:
            log(f"Delivery failed: {type(exc).__name__}")

    def report_file_problems(self, pool):
        """Log unreadable files and skipped lines once per file, by index (file
        names may contain patient identifiers on real data)."""
        for p in pool.patients:
            if p.index in self._reported or not (p.error or (p.bad_lines and p.passes)):
                continue
            self._reported.add(p.index)
            where = f" ({os.path.relpath(p.path, self.show_paths_below)})" if self.show_paths_below else ""
            what = f"read error {p.error}" if p.error else f"{p.bad_lines} invalid lines skipped"
            log(f"Patient file #{p.index}: {what}{where}")

    def maybe_report(self, pool, force=False):
        now = time.monotonic()
        elapsed = now - self._last_time
        if not force and elapsed < self.interval:
            return
        self.report_file_problems(pool)
        rate = (self.sent - self._last_sent) / elapsed if elapsed > 0 else 0.0
        log(f"throughput {rate:.1f} msg/s | sent {self.sent} | failed {self.failed + self.async_failed} | "
            f"active patients {pool.active()}/{len(pool.patients)} | behind schedule {self.behind:.2f} s")
        self._last_time = now
        self._last_sent = self.sent
        self.behind = 0.0


def record_key(record):
    userid = record.get("userid")
    return None if userid is None else str(userid).encode("utf-8")


def run(cfg):
    if not os.path.isdir(cfg.input_dir):
        log(f"Input folder {cfg.input_dir} does not exist")
        return 1
    pool = PatientPool(cfg.input_dir, cfg.rescan_seconds)
    pool.scan()
    if not pool.patients and not pool.rescanning:
        log(f"No patient files found in {cfg.input_dir}")
        return 1
    log(f"Found {len(pool.patients)} patient files under {cfg.input_dir} "
        f"(mode={cfg.replay_mode}, rate={cfg.rate_hz} Hz, speedup={cfg.speedup}, loop={cfg.loop}, "
        f"rescan={cfg.rescan_seconds or 'off'})")

    producer = connect(cfg)
    ensure_topics(cfg.kafka_bootstrap, list(dict.fromkeys(PIPELINE_TOPICS + [cfg.topic])), cfg.topic_partitions)

    if cfg.replay_mode == "original-timing":
        schedule = original_timing_schedule(pool, cfg.rate_hz, cfg.speedup, cfg.loop)
    else:
        schedule = fixed_rate_schedule(pool, cfg.rate_hz, cfg.loop)

    stats = Stats(cfg.stats_seconds, cfg.input_dir if cfg.log_messages else None)
    start = time.monotonic()
    try:
        for offset, patient, record in schedule:
            delay = start + offset - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            elif -delay > stats.behind:
                stats.behind = -delay
            stats.maybe_report(pool)
            if patient is None:
                continue
            now_ms = int(time.time() * 1000)
            message = dict(record)
            message["timestamp"] = iso_from_ms(now_ms)
            payload = json.dumps(message).encode("utf-8")
            try:
                producer.send(cfg.topic, key=record_key(record), value=payload,
                              timestamp_ms=now_ms).add_errback(stats.on_error)
                stats.sent += 1
                if cfg.log_messages:
                    log(f"Sent to {cfg.topic}: {payload.decode('utf-8')}")
            except KafkaError as e:
                stats.failed += 1
                if stats.failed <= 5:
                    log(f"Failed to send message: {type(e).__name__}")
    finally:
        producer.flush()
        pool.close()
        stats.maybe_report(pool, force=True)
    log(f"Finished: sent {stats.sent} messages ({stats.failed + stats.async_failed} failed)")
    return 0


def parse_config(argv=None):
    ap = argparse.ArgumentParser(description="Replay per-patient record files into Kafka.")
    ap.add_argument("--input-dir", default=os.getenv("INPUT_DIR", "output_json_files"))
    ap.add_argument("--rate-hz", type=float, default=float(os.getenv("RATE_HZ", "1")))
    ap.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"))
    ap.add_argument("--topic", default=os.getenv("RAW_TOPIC", "raw-topic"))
    ap.add_argument("--replay-mode", choices=("fixed-rate", "original-timing"),
                    default=os.getenv("REPLAY_MODE", "fixed-rate"))
    ap.add_argument("--speedup", type=float, default=float(os.getenv("SPEEDUP", "1")))
    ap.add_argument("--loop", dest="loop", action="store_true", default=env_bool("LOOP", True))
    ap.add_argument("--no-loop", dest="loop", action="store_false")
    ap.add_argument("--rescan-seconds", type=float, default=float(os.getenv("RESCAN_SECONDS", "0") or 0))
    ap.add_argument("--stats-seconds", type=float, default=float(os.getenv("STATS_SECONDS", "10") or 10))
    ap.add_argument("--log-messages", action="store_true", default=env_bool("LOG_MESSAGES", False))
    ap.add_argument("--topic-partitions", type=int, default=int(os.getenv("TOPIC_PARTITIONS", "1") or 1))
    ap.add_argument("--compression", default=os.getenv("KAFKA_COMPRESSION", "lz4"))
    ap.add_argument("--linger-ms", type=int, default=int(os.getenv("KAFKA_LINGER_MS", "20") or 20))
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("KAFKA_BATCH_SIZE", "262144") or 262144))
    cfg = ap.parse_args(argv)
    if cfg.rate_hz <= 0 or cfg.speedup <= 0:
        ap.error("RATE_HZ and SPEEDUP must be > 0")
    if cfg.rescan_seconds < 0 or cfg.stats_seconds <= 0 or cfg.topic_partitions < 1:
        ap.error("RESCAN_SECONDS must be >= 0, STATS_SECONDS > 0 and TOPIC_PARTITIONS >= 1")
    return cfg


def main(argv=None):
    cfg = parse_config(argv)
    try:
        return run(cfg)
    except (NoBrokersAvailable, KafkaTimeoutError) as e:
        log(f"Failed to connect to Kafka broker after multiple attempts: {e}")
    except Exception as e:  # noqa: BLE001
        log(f"Error in producer: {type(e).__name__}")
        if cfg.log_messages:
            print(traceback.format_exc(), flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
