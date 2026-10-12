#!/usr/bin/env python3
"""Convert Philips DWC monitor numerics (one row per measurement) into the
per-patient JSON files that ``producer.py`` replays into Kafka.

Input
    A CSV export of the DWC numerics table, either plain (``.csv``) or zipped
    (``.csv.zip`` / ``.zip``; read in streaming mode, nothing is extracted to
    disk; ``__MACOSX/`` entries are skipped). Two column layouts are supported
    and detected from the header:

    * reduced layout: ``p_patnr, c_time_stamp_numeric_value, c_label,
      c_sub_label, c_value, c_falnr, c_pflege_oe_id, ...``
    * full numerics layout: ``p_patnr, c_time_stamp_numeric_value,
      c_label, c_sub_label, c_value, c_validity, c_sequence_number, ...``
      (no case number and no ward -> ``caseid``/``ward`` are null)

    Timestamps look like ``2024-06-01 00:00:01.340 +02:00``.

Output
    One JSON file per patient in ``--output-dir`` (``<p_patnr>.json``): a list
    of records sorted by time, compatible with ``producer.py``::

        {"recordid": str, "userid": str, "caseid": str|null,
         "username": str, "userinitials": null, "ward": str|null,
         "resp": num|null, "bps": num|null, "pulse": num|null,
         "spo2": num|null, "temp": float|null, "icd10": null,
         "measured_at": "2024-06-01T00:00:01.000+02:00"}

    ``userid`` and ``username`` are the patient number (there are no names in
    the source data), ``caseid`` is ``c_falnr``, ``ward`` is
    ``c_pflege_oe_id``. ``timestamp`` is added by the producer at send time.

How rows become records
    1. Only rows whose ``(c_label, c_sub_label)`` is listed in ``MAPPING`` are
       used. Each mapping has a priority: when several sources for the same
       field fall into one bucket, the lowest priority wins (heart rate ``HF``
       before ``Puls``; invasive ``ABPs`` before non-invasive ``NBPs``). Only
       the *systolic* sub-label is mapped to ``bps``.
    2. Rows are filtered: ``c_validity`` must be in ``--keep-validity``
       (default ``0``; only applies if the column exists) and the value must
       lie in a plausibility range (``RANGES``; disable with
       ``--no-range-filter``). Values of exactly 0 are typically "no signal".
    3. Measurements of a patient are grouped into time buckets of
       ``--bucket-seconds`` (default 1 s), because different parameters arrive
       with slightly different millisecond timestamps. Within a bucket the
       latest measurement of the highest-priority source wins.
       ``measured_at`` is the bucket start in the original UTC offset.
    4. Optional forward fill (``--ffill-seconds``, default 0 = off): a field
       that is missing in a bucket takes the last observed value of that field
       if it was measured at most N seconds before the bucket start. Forward
       fill never creates new buckets. It is useful for sparse parameters such
       as non-invasive blood pressure (typically every 15-60 min), e.g.
       ``--ffill-field bps=3600`` (per-field windows override
       ``--ffill-seconds``); the NEWS2 job otherwise sees ``bps = null``
       for most records. Filled values are reported in the summary.
       ``c_is_manual`` is not used.

Scalability
    The input does not need to be sorted. Mapped rows are spilled into
    ``--partitions`` temporary files (hash of the patient number) inside the
    output directory, and each partition is grouped in memory, so memory use
    is bounded by roughly ``input size / partitions`` and multi-GB exports
    work. The temporary files are removed at the end.

Only aggregate statistics are printed (never patient numbers or values).

Example::

    python3 convert_dwc.py --input input/export.csv.zip --output-dir output_dwc \\
        --bucket-seconds 1 --ffill-seconds 10 --ffill-field bps=3600
"""

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import statistics
import sys
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

# (c_label, c_sub_label) -> (output field, priority; lower wins within a bucket)
MAPPING = {
    # heart rate: ECG-derived HF preferred over pulse from ABP / pleth
    ("HF", "HF"): ("pulse", 0),
    ("HR", "HR"): ("pulse", 0),
    ("Puls", "Puls (ABP)"): ("pulse", 1),
    ("Puls", "Puls (SpO2)"): ("pulse", 2),
    ("Pulse", "Pulse (ABP)"): ("pulse", 1),
    ("Pulse", "Pulse (SpO2)"): ("pulse", 2),
    # respiration rate
    ("AF", "AF"): ("resp", 0),
    ("RR", "RR"): ("resp", 0),
    # systolic blood pressure: invasive preferred over non-invasive.
    # Only the systolic sub-label is mapped; mean/diastolic are ignored.
    ("ABP", "ABPs"): ("bps", 0),
    ("ART", "ARTs"): ("bps", 0),
    ("NBP", "NBPs"): ("bps", 1),
    # oxygen saturation (NEWS2 SpO2 scale 1)
    ("SpO2", "SpO2"): ("spo2", 0),
    # temperature
    ("Temp", "Temp"): ("temp", 0),
}

FIELDS = ("resp", "bps", "pulse", "spo2", "temp")

# Plausibility ranges (inclusive); values outside are treated as artefacts.
RANGES = {
    "resp": (1, 100),
    "bps": (1, 350),
    "pulse": (1, 350),
    "spo2": (1, 100),
    "temp": (20.0, 45.0),
}

TS_COLUMNS = ("c_time_stamp_numeric_value", "c_time_stamp_numeric")
REQUIRED = ("p_patnr", "c_label", "c_sub_label", "c_value")

TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:[.,](\d{1,6}))?"
    r"\s*(Z|[+-]\d{2}:?\d{2})?$"
)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_ts(text):
    """Parse a DWC timestamp. Returns (epoch_ms, utc_offset_minutes, had_offset)
    or None. Timestamps without an offset are interpreted as UTC."""
    m = TS_RE.match(text.strip())
    if not m:
        return None
    y, mo, d, h, mi, s, frac, off = m.groups()
    offset_min = 0
    had_offset = off is not None
    if off and off != "Z":
        sign = -1 if off[0] == "-" else 1
        off = off[1:].replace(":", "")
        offset_min = sign * (int(off[:2]) * 60 + int(off[2:]))
    try:
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s),
                      tzinfo=timezone(timedelta(minutes=offset_min)))
    except ValueError:
        return None
    ms = int((frac or "0").ljust(3, "0")[:3])
    epoch_ms = int((dt - EPOCH).total_seconds()) * 1000 + ms
    return epoch_ms, offset_min, had_offset


def format_ts(epoch_ms, offset_min):
    tz = timezone(timedelta(minutes=offset_min))
    dt = EPOCH + timedelta(milliseconds=epoch_ms)
    return dt.astimezone(tz).isoformat(timespec="milliseconds")


def clean_number(value, field):
    if field == "temp":
        return float(value)
    return int(value) if float(value).is_integer() else value


def open_csv_streams(path):
    """Yield (member_name, text stream) for the CSV file(s) in ``path``."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            members = [
                n for n in zf.namelist()
                if n.lower().endswith(".csv")
                and not n.startswith("__MACOSX/")
                and not os.path.basename(n).startswith("._")
            ]
            if not members:
                raise SystemExit("No CSV file found in zip archive")
            for name in members:
                with zf.open(name) as raw:
                    yield name, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
    else:
        with open(path, "r", encoding="utf-8-sig", newline="") as fh:
            yield os.path.basename(path), fh


def partition_of(patient, partitions):
    return int(hashlib.md5(patient.encode("utf-8")).hexdigest(), 16) % partitions


def spill(args, tmp_dir, stats):
    """Pass 1: stream the input, filter + map rows, spill to partition files."""
    keep_validity = None if args.keep_validity == "all" else set(args.keep_validity.split(","))
    writers, handles = {}, {}
    row_index = 0
    try:
        for member, stream in open_csv_streams(args.input):
            reader = csv.DictReader(stream)
            header = reader.fieldnames or []
            missing = [c for c in REQUIRED if c not in header]
            ts_col = next((c for c in TS_COLUMNS if c in header), None)
            if missing or ts_col is None:
                raise SystemExit(
                    "Input %s lacks required columns: %s"
                    % (member, ", ".join(missing + ([] if ts_col else ["c_time_stamp_numeric_value"]))))
            has_validity = "c_validity" in header
            has_seq = "c_sequence_number" in header
            stats["layout"].add("full" if has_validity else "reduced")
            for c in ("c_falnr", "c_pflege_oe_id"):
                if c not in header:
                    stats["columns_absent"].add(c)

            for row in reader:
                row_index += 1
                stats["rows_total"] += 1
                key = ((row.get("c_label") or "").strip(), (row.get("c_sub_label") or "").strip())
                target = MAPPING.get(key)
                if target is None:
                    stats["rows_unmapped"] += 1
                    continue
                field, prio = target
                stats["rows_mapped_by_label"]["%s/%s" % key] += 1

                if has_validity:
                    validity = (row.get("c_validity") or "").strip()
                    stats["validity_mapped_rows"][validity] += 1
                    if keep_validity is not None and validity not in keep_validity:
                        stats["dropped_validity"][field] += 1
                        continue

                patient = (row.get("p_patnr") or "").strip()
                if not patient:
                    stats["dropped_no_patient"] += 1
                    continue
                parsed = parse_ts(row.get(ts_col) or "")
                if parsed is None:
                    stats["dropped_bad_timestamp"] += 1
                    continue
                epoch_ms, offset_min, had_offset = parsed
                if not had_offset:
                    stats["timestamps_without_offset"] += 1
                try:
                    value = float((row.get("c_value") or "").strip())
                except ValueError:
                    stats["dropped_bad_value"][field] += 1
                    continue
                if value != value:  # NaN
                    stats["dropped_bad_value"][field] += 1
                    continue
                lo, hi = RANGES[field]
                if not args.no_range_filter and not (lo <= value <= hi):
                    stats["dropped_out_of_range"][field] += 1
                    continue
                seq = (row.get("c_sequence_number") or "").strip() if has_seq else ""
                try:
                    seq = int(seq) if seq else -1
                except ValueError:
                    seq = -1

                part = partition_of(patient, args.partitions)
                if part not in writers:
                    handles[part] = open(os.path.join(tmp_dir, "part_%04d.csv" % part),
                                         "w", encoding="utf-8", newline="")
                    writers[part] = csv.writer(handles[part])
                writers[part].writerow([
                    patient, epoch_ms, offset_min, field, prio, repr(value), seq, row_index,
                    (row.get("c_falnr") or "").strip(),
                    (row.get("c_pflege_oe_id") or "").strip(),
                ])
                stats["rows_used"] += 1
    finally:
        for h in handles.values():
            h.close()
    return sorted(handles)


def build_records(patient, rows, args, stats):
    """Pass 2 for one patient: bucket, select per field, forward fill."""
    bucket_ms = max(1, int(round(args.bucket_seconds * 1000)))
    ffill_ms = {f: int(round(args.ffill_by_field.get(f, args.ffill_seconds) * 1000))
                for f in FIELDS}
    rows.sort(key=lambda r: (r[0], r[5], r[6]))  # time, sequence number, file order

    buckets = []  # list of [bucket_start, offset, {field: (prio, epoch, value)}, caseid, ward]
    current = None
    for epoch_ms, offset_min, field, prio, value, seq, idx, caseid, ward in rows:
        start = (epoch_ms // bucket_ms) * bucket_ms
        if current is None or current[0] != start:
            current = [start, offset_min, {}, None, None]
            buckets.append(current)
        chosen = current[2].get(field)
        if chosen is not None:
            stats["bucket_collisions"][field] += 1
        # later rows win within the same priority; a better priority always wins
        if chosen is None or prio <= chosen[0]:
            current[2][field] = (prio, epoch_ms, value)
        if caseid:
            current[3] = caseid
        if ward:
            current[4] = ward

    last_seen = {}
    records = []
    for start, offset_min, chosen, caseid, ward in buckets:
        rec = {
            "recordid": hashlib.sha1(("%s|%d" % (patient, start)).encode("utf-8")).hexdigest()[:16],
            "userid": patient,
            "caseid": caseid,
            "username": patient,
            "userinitials": None,
            "ward": ward,
        }
        for field in FIELDS:
            hit = chosen.get(field)
            if hit is not None:
                _, epoch_ms, value = hit
                last_seen[field] = (epoch_ms, value)
                rec[field] = clean_number(value, field)
                stats["source_priority"]["%s:%d" % (field, hit[0])] += 1
            elif (ffill_ms[field] > 0 and field in last_seen
                  and start - last_seen[field][0] <= ffill_ms[field]):
                rec[field] = clean_number(last_seen[field][1], field)
                stats["ffilled"][field] += 1
            else:
                rec[field] = None
        rec["icd10"] = None
        rec["measured_at"] = format_ts(start, offset_min)
        records.append(rec)
    return records


def write_patient(out_dir, patient, records):
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", patient)
    path = os.path.join(out_dir, "%s.json" % safe)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("[\n")
        fh.write(",\n".join(json.dumps(r, ensure_ascii=False) for r in records))
        fh.write("\n]\n")


def print_summary(stats, per_patient_counts, missing, total_records, t_min, t_max):
    print("== convert_dwc summary (aggregates only) ==")
    print("input layout(s):", ", ".join(sorted(stats["layout"])))
    if stats["columns_absent"]:
        print("columns absent (fields set to null):", ", ".join(sorted(stats["columns_absent"])))
    print("rows read: %d, unmapped: %d, used: %d"
          % (stats["rows_total"], stats["rows_unmapped"], stats["rows_used"]))
    print("mapped rows by c_label/c_sub_label:", dict(stats["rows_mapped_by_label"].most_common()))
    if stats["validity_mapped_rows"]:
        print("c_validity of mapped rows:", dict(stats["validity_mapped_rows"].most_common()))
    for name in ("dropped_validity", "dropped_bad_value", "dropped_out_of_range"):
        if stats[name]:
            print("%s:" % name, dict(stats[name]))
    for name in ("dropped_no_patient", "dropped_bad_timestamp", "timestamps_without_offset"):
        if stats[name]:
            print("%s: %d" % (name, stats[name]))
    print("patients: %d, records: %d" % (len(per_patient_counts), total_records))
    if per_patient_counts:
        print("records per patient: min %d, median %g, max %d"
              % (min(per_patient_counts), statistics.median(per_patient_counts),
                 max(per_patient_counts)))
    if t_min is not None:
        span = (t_max - t_min) / 1000.0
        print("time span: %s .. %s (%.1f days)"
              % (format_ts(t_min, 0), format_ts(t_max, 0), span / 86400.0))
    if total_records:
        print("missingness per field (share of records with null):",
              {f: round(missing[f] / total_records, 4) for f in FIELDS})
    print("multiple values per field in one bucket (resolved):", dict(stats["bucket_collisions"]))
    print("records per source priority (field:priority):", dict(sorted(stats["source_priority"].items())))
    if stats["ffilled"]:
        print("forward-filled values:", dict(stats["ffilled"]))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--input", required=True, help="DWC numerics export (.csv or .csv.zip)")
    ap.add_argument("--output-dir", required=True, help="directory for <patient>.json files")
    ap.add_argument("--bucket-seconds", type=float, default=1.0,
                    help="time resolution used to merge parameters into one record (default 1)")
    ap.add_argument("--ffill-seconds", type=float, default=0.0,
                    help="forward-fill a missing field from a measurement at most this many "
                         "seconds old (default 0 = off)")
    ap.add_argument("--ffill-field", action="append", default=[], metavar="FIELD=SECONDS",
                    help="per-field forward-fill window overriding --ffill-seconds, e.g. "
                         "bps=3600 (repeatable; fields: %s)" % ", ".join(FIELDS))
    ap.add_argument("--keep-validity", default="0",
                    help="comma-separated c_validity values to keep, or 'all' (default '0'; "
                         "ignored if the column is absent)")
    ap.add_argument("--no-range-filter", action="store_true",
                    help="keep values outside the plausibility ranges")
    ap.add_argument("--partitions", type=int, default=16,
                    help="number of temporary partitions used for grouping (default 16)")
    args = ap.parse_args(argv)
    if args.bucket_seconds <= 0 or args.ffill_seconds < 0 or args.partitions < 1:
        ap.error("--bucket-seconds must be > 0, --ffill-seconds >= 0, --partitions >= 1")
    args.ffill_by_field = {}
    for spec in args.ffill_field:
        field, _, seconds = spec.partition("=")
        try:
            seconds = float(seconds)
        except ValueError:
            seconds = -1
        if field not in FIELDS or seconds < 0:
            ap.error("invalid --ffill-field %r (expected FIELD=SECONDS)" % spec)
        args.ffill_by_field[field] = seconds

    os.makedirs(args.output_dir, exist_ok=True)
    stats = defaultdict(int)
    for name in ("rows_mapped_by_label", "validity_mapped_rows", "dropped_validity",
                 "dropped_bad_value", "dropped_out_of_range", "bucket_collisions",
                 "source_priority", "ffilled"):
        stats[name] = Counter()
    stats["layout"], stats["columns_absent"] = set(), set()

    tmp_dir = tempfile.mkdtemp(prefix=".convert_dwc_", dir=args.output_dir)
    per_patient_counts, missing = [], Counter()
    total_records, t_min, t_max = 0, None, None
    try:
        parts = spill(args, tmp_dir, stats)
        for part in parts:
            per_patient = defaultdict(list)
            with open(os.path.join(tmp_dir, "part_%04d.csv" % part), encoding="utf-8", newline="") as fh:
                for p, epoch, off, field, prio, value, seq, idx, caseid, ward in csv.reader(fh):
                    per_patient[p].append((int(epoch), int(off), field, int(prio), float(value),
                                           int(seq), int(idx), caseid or None, ward or None))
            for patient in sorted(per_patient):
                records = build_records(patient, per_patient.pop(patient), args, stats)
                if not records:
                    continue
                write_patient(args.output_dir, patient, records)
                per_patient_counts.append(len(records))
                total_records += len(records)
                for r in records:
                    for f in FIELDS:
                        if r[f] is None:
                            missing[f] += 1
                first = parse_ts(records[0]["measured_at"])[0]
                last = parse_ts(records[-1]["measured_at"])[0]
                t_min = first if t_min is None else min(t_min, first)
                t_max = last if t_max is None else max(t_max, last)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print_summary(stats, per_patient_counts, missing, total_records, t_min, t_max)
    return 0


if __name__ == "__main__":
    sys.exit(main())
