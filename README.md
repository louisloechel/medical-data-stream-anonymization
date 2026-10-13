# Anonymization of Medical Data Streams using [Prink](https://github.com/PrivacyEngineering/prink)

![Docker](https://img.shields.io/badge/docker-ready-blue?logo=docker)
![Flink](https://img.shields.io/badge/flink-1.19-orange?logo=apacheflink)
![Kafka](https://img.shields.io/badge/kafka-streaming-black?logo=apachekafka)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
---
## Overview
This repository contains a demo application for a Master's thesis project focused on the anonymization of medical data streams using [Prink](https://github.com/PrivacyEngineering/prink). The application consists of dockerized microservices to stream medical data and perform in-transit anonymization.

<details>
<summary><strong>Architecture Diagram</strong></summary>

![System Architecture](else/architecture_prink.png)
</details>

### Key Features
- **Dockerized microservices**
- **Streaming medical data**
- **In-transit anonymization using [Prink](https://github.com/PrivacyEngineering/prink)**

---


## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

### Build & Run
Runs on a single machine (Linux/macOS, amd64 or arm64) with synthetic data from `load-generator/output_json_files`.
```bash
make up            # = docker compose -p streamanon up --build -d
make smoke-test    # end-to-end check, exits non-zero on failure (scripts/smoke-test.sh)
make logs S="flink-taskmanager og-prink-job"   # follow logs of selected services
```
Plain `docker compose up --build -d` works as well (run `scripts/smoke-test.sh` afterwards).
The first build downloads Maven/pip dependencies and takes several minutes.
Both Flink jobs are submitted automatically once the JobManager is ready (`scripts/flink-submit.sh`)
and re-submitted if the cluster restarts; Grafana dashboards and the Prometheus datasource are
provisioned from `grafana/provisioning`.

#### Resource settings
The defaults fit Docker Desktop with ~8 GB memory (JobManager 1 GB, TaskManager 2 GB, Kafka 512 MB heap;
~6 GB in total). On a server, copy `.env.example` to `.env` and raise `FLINK_TM_PROCESS_SIZE`,
`FLINK_TM_MEM_LIMIT` etc.; the container memory limit must be larger than the Flink process size.

| Service         | URL                                                               | Default Credentials      |
|-----------------|-------------------------------------------------------------------|-------------------------|
| Prink Overview  | [http://localhost:8081/#/overview](http://localhost:8081/#/overview) | -                       |
| Grafana         | [http://localhost:3000/login](http://localhost:3000/login)           | `admin` / `admin`       |
| Prometheus      | [http://localhost:9090/query](http://localhost:9090/query)           | -                       |
> **Note:** To log in to Grafana, use credentials `admin/admin`, then choose `skip`.

### ...When finished
```bash
make down          # = docker compose -p streamanon down  (make clean also removes the volumes)
```
<details>
<summary><strong>Dashboard Views</strong></summary>

![](else/Dashboard_patient.png)
![](else/Dashboard_news2_comp_tall.png)
![](else/Dashboard_ICU_all.png)
![](else/Dashboard_QA_tall.png)
![](else/prediction.png)
![](else/news2_dashboard_comp_2.png)
![](else/dashboard_privacy_params.png)
![](else/Dashboard_latency_tall.png)

</details>

### Configuration

#### Privacy Parameters

| Parameter | Default Value |
|-----------|---------------|
| `k`       | 5             |
| `l`       | 3             |
| `delta`   | 125           |
| `beta`    | 50000         |
| `zeta`    | 10000         |
| `mu`      | 100           |

> **Note:** The privacy parameters can be configured in the [GangesEvaluation.java Prink Job](og-prink/src/main/java/ganges/GangesEvaluation.java) file.

#### Prink Kafka consumer properties
- `bootstrap.servers`: Kafka broker addresses.
- `group.id`: Consumer group ID.
- `key.deserializer`: Deserializer for the message key.
- `value.deserializer`: Deserializer for the message value.

> **Note:** The consumer properties (connecting Prink to other topics) can be configured in the [GangesEvaluation.java Prink Job](og-prink/src/main/java/ganges/GangesEvaluation.java) file.

#### Flink Configuration
- The Flink configuration file is located at `flink-conf.yaml` (mounted read-only and used as `conf/config.yaml` by the Flink 1.19 containers; memory sizes are set via environment variables, see `.env.example`).
- Key configurations include:
  - `jobmanager.rpc.address`: Address of the job manager.
  - `taskmanager.numberOfTaskSlots`: Number of task slots per task manager (overridden by `TASKMANAGER_SLOTS`, default 4).

#### Real monitor data (local only)
Real data must never be committed. Place exports in `load-generator/input/`
(ignored by git and by the Docker build) and convert them into per-patient
JSON files in `load-generator/output_dwc/` (also ignored):

```bash
cd load-generator
python3 convert_dwc.py --input input/<export>.csv.zip --output-dir output_dwc \
    --bucket-seconds 1 --ffill-seconds 0
```

`convert_dwc.py` maps Philips DWC numerics (`c_label`/`c_sub_label`) to
`resp`, `bps` (systolic only), `pulse`, `spo2` and `temp`, merges them into one
record per patient and time bucket, and prints aggregate statistics only. See
the module docstring for the mapping, filters and forward-fill options.

#### Load generator
`load-generator/producer.py` is configured via environment variables (also
settable in `docker-compose.yml` / `.env`):

| Variable          | Default             | Meaning                                                        |
|-------------------|---------------------|----------------------------------------------------------------|
| `INPUT_DIR`       | `output_json_files` | folder with one file per patient, searched recursively (below) |
| `RATE_HZ`         | `1`                 | `fixed-rate`: records per patient per second                   |
| `KAFKA_BOOTSTRAP` | `kafka:29092`       | Kafka bootstrap servers                                        |
| `REPLAY_MODE`     | `fixed-rate`        | `fixed-rate` or `original-timing` (uses `measured_at`)         |
| `SPEEDUP`         | `1`                 | `original-timing`: time acceleration factor                    |
| `LOOP`            | `1`                 | `0`: stop after the last record instead of starting over       |
| `RESCAN_SECONDS`  | `0`                 | `> 0`: look for new patient files every N seconds (0 = off)    |
| `STATS_SECONDS`   | `10`                | interval of the throughput log line                            |
| `LOG_MESSAGES`    | `0`                 | `1`: log every sent record                                     |

Input files: `*.json` (JSON array, the format of `generateData.py`/`convert_dwc.py`),
`*.jsonl` (JSON Lines, one record per line) and `*.jsonl.gz`; sub-folders (e.g. one per unit)
are scanned recursively. Hidden files, files with `manifest` in their name and other extensions
(e.g. `*.part`) are ignored. JSON Lines files are streamed line by line (one open reader per
patient), so the data set may be much larger than the memory; a `*.json` array is loaded one
file at a time per patient.

In `fixed-rate` mode every patient cycles through its own file independently,
so patient files may have different lengths. Records are sent keyed by `userid`
(all records of a patient go to the same partition, in order) and the Kafka
record timestamp equals the added `timestamp` field. The producer batches and
compresses (lz4) and logs only aggregates, e.g.
`throughput 240.0 msg/s | sent 52800 | failed 0 | active patients 240/240 | behind schedule 0.00 s`
(record contents and file names are logged only with `LOG_MESSAGES=1`).

#### Synthetic data
`load-generator/generateData.py` creates the synthetic patient files (random values only; needs
`pip install -r load-generator/requirements.txt`):

```bash
cd load-generator
python3 generateData.py                    # schema v1 -> output_json_files/ (20 patients x 10000 records)
cd ..
make synthetic-v2                          # ICU schema v2 -> load-generator/output_json_files_v2/
```

The v2 data set is not committed. The load-generator image generates it at build time with the same
arguments as `make synthetic-v2` (`--schema v2 --num-users 10 --num-timestamps 600 --interval-minutes 1
--seed 2`): 10 patients with 600 records each. The seed and the pinned Faker version give the same values
in every build; the timestamps start one day before generation. The set has all optional ICU fields
(demographics, rhythm, ventilation, ACVPU, ...), so the full schema can be smoke-tested without real data:

```bash
INPUT_DIR=output_json_files_v2 PRINK_JOB_ARGS="--profile icu-qi-v2 --outputTopic prink-topic" make up
make smoke-test
```

`--outputTopic prink-topic` routes the profile's output to the topic the consumer and the smoke test read
(the profile itself writes to `prink-icu-qi-v2`). Set `PRINK_MASK_SALT` for the Prink job to salt the
hashed `userid`/`caseid` (see `og-prink/README.md`).

### Scaling to an ICU workload

Target: ~240 patients for 24 h at ~1 Hz per patient (~10 values per record, ~9 GB of JSON) on one VM with
16 vCPU, 32 GB RAM and ~50 GB free disk. The settings live in `.env` (see the "Scaling profile" in
`.env.example`).

**Input.** Convert/generate one file per patient as `*.jsonl.gz` (smallest, streamed), grouped in one
folder per unit, and mount the folder read-only into the producer with a `docker-compose.override.yml`
next to `docker-compose.yml` (picked up automatically by `docker compose`, ignored by git):

```yaml
services:
  load-generator:
    volumes:
      - /data/icu-replay:/data:ro      # /data/icu-replay/<unit>/<patient>.jsonl.gz
    environment:
      INPUT_DIR: /data
      RESCAN_SECONDS: "60"             # units copied in later are picked up without a restart
```

Copy new files under a temporary name (e.g. `*.part`) and rename them when complete, so the rescan never
sees a half-written file. Convert a legacy JSON array with
`python3 -c 'import json,sys; [print(json.dumps(r)) for r in json.load(open(sys.argv[1]))]' p.json | gzip > p.jsonl.gz`.

**Kafka disk.** Each partition is trimmed when it is older than `KAFKA_LOG_RETENTION_HOURS` (default 6) *or*
larger than `KAFKA_LOG_RETENTION_BYTES` (default 3 GiB per partition), whichever comes first. Kafka deletes
whole segments (`KAFKA_LOG_SEGMENT_BYTES`, default 256 MiB), so the worst-case disk use of the pipeline is

```
3 topics x TOPIC_PARTITIONS x (KAFKA_LOG_RETENTION_BYTES + KAFKA_LOG_SEGMENT_BYTES)
```

(plus profile-specific output topics such as `prink-icu-qi-v2`, which count like a fourth topic). With the
defaults (1 partition) that is ~9.8 GB; with 4 partitions and 1 GiB per partition ~15 GB. Keep it below
about half of the free disk (images, container logs and Prometheus need space as well). For orientation:
240 patients x 86 400 s = ~21 M records per topic and day; the producer and both Flink jobs write lz4
compressed batches, so a day of all three topics takes roughly 8-12 GB on disk (measured: ~140-180 bytes per
record and topic with synthetic ICU v2 records). Kafka keeps its data in the container;
`docker compose down` (or `make clean`) frees it.

**Parallelism.** Both Flink jobs default to parallelism 1. `FLINK_JOB_PARALLELISM` (NEWS2 job) only helps
with at least as many `raw-topic` partitions (`TOPIC_PARTITIONS`, created by `kafka-init`; existing topics
are increased, never decreased); messages are keyed by patient, so per-patient order is kept.
`PRINK_PARALLELISM` partitions the Prink job by patient: every released equivalence class still has >= k
distinct patients, but each subtask clusters only its share of the patients, which increases information
loss and, at the same `delta`, the delay (see `og-prink/README.md` "Parallelism"). The TaskManager needs
`TASKMANAGER_SLOTS >= FLINK_JOB_PARALLELISM + PRINK_PARALLELISM`; both jobs can also take `--parallelism <n>`
as program argument (`PRINK_JOB_ARGS`). The Prink job is the bottleneck: in a laptop test (Docker Desktop,
TaskManager limited to 4 CPUs, synthetic data, `icu-qi-v2` with k=5, l=2, delta=125) two Prink subtasks kept
up with 240 patients at 1 Hz (240 records/s, Prink stage p50 ~1.1 s, p99 ~2.1 s; NEWS2 stage p50 ~25 ms) but
anonymized only ~500 records/s when offered 2 500 records/s, i.e. `SPEEDUP=10` for 240 patients needs
considerably more Prink parallelism (at the cost of utility) or a smaller workload. Measure with the latency
script below and raise the parallelism only if Prink falls behind.

**Latency per stage.** The producer sets the Kafka CreateTime of each raw record to its `timestamp` field;
flink-job and the Prink job set the CreateTime of their output records to the time they write them (the
`timestamp` field is passed through). `scripts/latency_from_topics.py` reads a time window of all three
topics and prints only aggregates (count, throughput, mean/p50/p95/p99/max latency end-to-end and per stage),
never record contents, so it can be used on real data as well:

```bash
docker compose run --rm --no-deps -v "$PWD/scripts:/scripts:ro" kafka-consumer \
    python /scripts/latency_from_topics.py --bootstrap kafka:29092 --since-seconds 300
# with a profile that writes to its own topic: --prink-topic prink-icu-qi-v2
# if Prink is far behind, match records further back: --lookback-seconds 900
```

### Record schema

Records are JSON objects, one per patient and point in time. All fields except `userid` are optional and may
be missing or JSON `null`; records of the original synthetic schema (v1) remain valid.

**raw-topic** (producer; schema v2 = v1 + ICU fields)

| Field | Type | Meaning |
|-------|------|---------|
| `recordid`, `userid`, `caseid` | string | record id, patient id (data subject), case id |
| `username`, `userinitials` | string | patient name (synthetic data only) |
| `resp`, `bps`, `pulse`, `spo2` | number | respiration rate /min, systolic BP mmHg, pulse /min, SpO2 % |
| `temp` | number | temperature °C |
| `measured_at` | string | measurement time (ISO-8601), used by `REPLAY_MODE=original-timing` |
| `timestamp` | string | producer send time (ISO-8601 UTC), added by the producer, used for latency |
| `ageBand` | string | 10-year age band, e.g. `"60-69"`, `"100+"` |
| `sex` | string | `"M"`, `"F"` or null |
| `postcode2` | string | first two digits of the postcode, e.g. `"10"` |
| `ward` | string | ward name |
| `icd10` | string | ICD-10-GM code, e.g. `"J96.11"` |
| `rhythm` | string | ECG rhythm label of the patient monitor (Philips), e.g. `"Sinus-Rhythmus"` |
| `ectopy`, `ectopyCount` | string, int | ectopy category and count |
| `sqi` | string | signal quality `Good` / `Fair` / `Bad` |
| `ventMode` | string | ventilation mode |
| `o2` | boolean | on supplemental oxygen or ventilation (NEWS2 "air or oxygen") |
| `acvpu` | string | consciousness `"A"`, `"C"`, `"V"`, `"P"` or `"U"` |
| `rass`, `gcs` | int | Richmond Agitation-Sedation Scale, Glasgow Coma Scale |
| `spo2Scale` | int | NEWS2 SpO2 scale `1` (default when missing) or `2` |

**processed-topic** (`flink-job`): a copy of the raw record (all fields, including unknown ones) plus the
NEWS2 (RCP 2017) fields below. `recordid`, `userid`, `caseid`, `username`, `userinitials`, `ward`, `resp`,
`bps`, `pulse`, `spo2`, `temp`, `icd10` and `timestamp` are always present (null if missing).

| Field | Type | Meaning |
|-------|------|---------|
| `respNEWS`, `spo2NEWS`, `o2NEWS`, `bpsNEWS`, `pulseNEWS`, `consciousnessNEWS`, `tempNEWS` | int or null | sub-scores; null if the input is missing or invalid. `spo2NEWS` uses scale 2 if `spo2Scale` is 2 (on scale 2, SpO2 >= 93 without known `o2` gives null) |
| `news2Partial` | int | sum of the available sub-scores |
| `news2Components` | int | number of available sub-scores (0-7) |
| `news2Complete` | boolean | all seven sub-scores available |
| `news2Total` | int or null | NEWS2 total, only if `news2Complete` |

**Prink output** depends on the anonymization profile (see `og-prink/README.md`). With `icu-qi-v2`: `recordid`,
`userid` (salted hash), `caseid` (salted hash), `ageBand`, `sex`, `postcode2`, `ward`, `icd10` (generalized
quasi-identifiers), `resp`, `spo2`, `bps`, `pulse`, `temp`, `o2`, `news2Total` (ranges such as `"(36.0,38.7)"`),
`rhythm` (label or a generalization such as `"Supraventricular"`), `timestamp` and `infoloss`; all values as
strings or null. Every other field is dropped.

## Third-Party Licenses
This project uses third-party components:
- Apache Kafka, Zookeeper, Flink, Prometheus (Apache 2.0)
- Grafana (AGPL v3)

These components are used as-is via container images. 

## License

This repository’s own source code is licensed under the [MIT License](LICENSE).

