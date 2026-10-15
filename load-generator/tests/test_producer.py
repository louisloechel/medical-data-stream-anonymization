"""Unit tests for producer.py (synthetic temp files only, no Kafka needed).

Run from the repository root: ``pytest load-generator/tests``
"""

import gzip
import itertools
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import producer  # noqa: E402


def records(patient, n, start_second=0, step=1):
    return [{"userid": patient, "recordid": f"{patient}-{i}", "resp": 12 + i,
             "measured_at": f"2026-01-01T00:{(start_second + i * step) // 60:02d}:{(start_second + i * step) % 60:02d}.000Z"}
            for i in range(n)]


def write_json(path, recs):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(recs, fh)


def write_jsonl(path, recs, compress=False, extra_lines=()):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    opener = gzip.open if compress else open
    with opener(path, "wt", encoding="utf-8") as fh:
        for r in recs:
            fh.write(json.dumps(r) + "\n")
        for line in extra_lines:
            fh.write(line + "\n")


def ids(schedule, n):
    """(offset, recordid) of the first n non-idle schedule steps."""
    out = []
    for offset, patient, record in schedule:
        if patient is None:
            continue
        out.append((round(offset, 6), record["recordid"]))
        if len(out) == n:
            break
    return out


# --- discovery ---------------------------------------------------------------

def test_discovery_is_recursive_and_filters_non_record_files(tmp_path):
    root = str(tmp_path)
    write_json(os.path.join(root, "a.json"), records("A", 1))
    write_jsonl(os.path.join(root, "unit1", "b.jsonl"), records("B", 1))
    write_jsonl(os.path.join(root, "unit1", "deeper", "c.jsonl.gz"), records("C", 1), compress=True)
    write_jsonl(os.path.join(root, "unit2", "d.ndjson"), records("D", 1))
    # ignored: manifests, hidden files/folders, partial files, other formats
    write_json(os.path.join(root, "unit1", "manifest.json"), [{"files": 2}])
    write_json(os.path.join(root, "unit2", "UNIT2_MANIFEST.jsonl"), [])
    write_jsonl(os.path.join(root, "unit2", ".hidden.jsonl"), records("H", 1))
    write_jsonl(os.path.join(root, ".tmp", "e.jsonl"), records("E", 1))
    write_jsonl(os.path.join(root, "unit2", "f.jsonl.part"), records("F", 1))
    with open(os.path.join(root, "notes.txt"), "w") as fh:
        fh.write("x")

    found = [os.path.relpath(p, root) for p in producer.discover_files(root)]
    assert found == ["a.json", os.path.join("unit1", "b.jsonl"),
                     os.path.join("unit1", "deeper", "c.jsonl.gz"), os.path.join("unit2", "d.ndjson")]


@pytest.mark.parametrize("name,expected", [
    ("p.json", True), ("p.JSON", True), ("p.json.gz", True), ("p.jsonl", True), ("p.jsonl.gz", True),
    ("p.ndjson.gz", True), ("manifest.json", False), ("run-manifest.jsonl.gz", False),
    (".p.json", False), ("p.jsonl.tmp", False), ("p.csv", False), ("p.gz", False),
])
def test_is_record_file(name, expected):
    assert producer.is_record_file(name) is expected


# --- reading -----------------------------------------------------------------

@pytest.mark.parametrize("name,compress", [("p.jsonl", False), ("p.jsonl.gz", True)])
def test_jsonl_and_gzip_are_streamed(tmp_path, name, compress):
    path = str(tmp_path / name)
    write_jsonl(path, records("P", 3), compress=compress, extra_lines=["", "not json", "[1, 2]"])
    bad = []
    it = producer.iter_file_records(path, lambda: bad.append(1))
    assert next(it)["recordid"] == "P-0"  # lazily, record by record
    assert [r["recordid"] for r in it] == ["P-1", "P-2"]
    assert len(bad) == 2  # "not json" and the array; empty lines are ignored


def test_legacy_json_array_and_gzip(tmp_path):
    write_json(str(tmp_path / "a.json"), records("A", 2))
    with gzip.open(str(tmp_path / "b.json.gz"), "wt", encoding="utf-8") as fh:
        json.dump(records("B", 2), fh)
    assert [r["recordid"] for r in producer.iter_file_records(str(tmp_path / "a.json"))] == ["A-0", "A-1"]
    assert [r["recordid"] for r in producer.iter_file_records(str(tmp_path / "b.json.gz"))] == ["B-0", "B-1"]


def test_patient_stream_loops_and_handles_broken_files(tmp_path):
    write_jsonl(str(tmp_path / "a.jsonl.gz"), records("A", 2), compress=True)
    stream = producer.PatientStream(str(tmp_path / "a.jsonl.gz"), 0)
    got = [stream.next_record(loop=True)["recordid"] for _ in range(5)]
    assert got == ["A-0", "A-1", "A-0", "A-1", "A-0"]
    assert stream.passes == 2 and stream.new_pass is True
    stream.close()

    # truncated gzip file: patient finishes with an error instead of crashing the producer
    data = gzip.compress(("\n".join(json.dumps(r) for r in records("T", 2000))).encode())
    (tmp_path / "t.jsonl.gz").write_bytes(data[: len(data) // 2])
    broken = producer.PatientStream(str(tmp_path / "t.jsonl.gz"), 1)
    n = 0
    while broken.next_record(loop=True) is not None:
        n += 1
    assert broken.finished and broken.error == "EOFError" and 0 < n < 2000

    # empty file does not loop forever
    write_jsonl(str(tmp_path / "e.jsonl"), [])
    empty = producer.PatientStream(str(tmp_path / "e.jsonl"), 2)
    assert empty.next_record(loop=True) is None and empty.finished


# --- fixed-rate scheduling ---------------------------------------------------

def test_fixed_rate_patients_cycle_independently(tmp_path):
    write_jsonl(str(tmp_path / "a.jsonl"), records("A", 2))
    write_jsonl(str(tmp_path / "u" / "b.jsonl.gz"), records("B", 3), compress=True)
    pool = producer.PatientPool(str(tmp_path))
    pool.scan()
    got = ids(producer.fixed_rate_schedule(pool, rate_hz=2, loop=True), 10)
    assert got == [(0.0, "A-0"), (0.0, "B-0"), (0.5, "A-1"), (0.5, "B-1"), (1.0, "A-0"), (1.0, "B-2"),
                   (1.5, "A-1"), (1.5, "B-0"), (2.0, "A-0"), (2.0, "B-1")]


def test_fixed_rate_without_loop_stops_per_patient(tmp_path):
    write_jsonl(str(tmp_path / "a.jsonl"), records("A", 1))
    write_json(str(tmp_path / "b.json"), records("B", 3))
    pool = producer.PatientPool(str(tmp_path))
    pool.scan()
    got = [(o, r["recordid"]) for o, p, r in producer.fixed_rate_schedule(pool, rate_hz=1, loop=False)]
    assert got == [(0.0, "A-0"), (0.0, "B-0"), (1.0, "B-1"), (2.0, "B-2")]
    assert pool.active() == 0


def test_only_one_reader_per_patient_is_open(tmp_path):
    for p in range(20):
        write_jsonl(str(tmp_path / f"p{p:02d}.jsonl.gz"), records(f"P{p}", 50), compress=True)
    pool = producer.PatientPool(str(tmp_path))
    pool.scan()
    steps = list(itertools.islice(producer.fixed_rate_schedule(pool, rate_hz=1, loop=True), 20 * 120))
    assert len(steps) == 2400
    assert all(p._it is not None for p in pool.patients)  # one open generator each, files re-opened per pass
    assert {p.passes for p in pool.patients} == {2}
    pool.close()
    assert all(p._it is None for p in pool.patients)


# --- rescan ------------------------------------------------------------------

def test_rescan_adds_new_unit_while_running(tmp_path):
    write_jsonl(str(tmp_path / "unit1" / "a.jsonl"), records("A", 100))
    pool = producer.PatientPool(str(tmp_path), rescan_seconds=2)
    pool.scan()
    schedule = producer.fixed_rate_schedule(pool, rate_hz=1, loop=True)
    first = ids(schedule, 2)
    assert first == [(0.0, "A-0"), (1.0, "A-1")]
    # a new unit appears while the producer runs; picked up at the next rescan (offset 2)
    write_jsonl(str(tmp_path / "unit2" / "b.jsonl.gz"), records("B", 100), compress=True)
    write_json(str(tmp_path / "unit2" / "manifest.json"), {"patients": 1})
    later = ids(schedule, 4)
    assert later == [(2.0, "A-2"), (2.0, "B-0"), (3.0, "A-3"), (3.0, "B-1")]
    assert len(pool.patients) == 2
    # already known files are not added twice
    assert pool.scan() == []


def test_rescan_waits_for_files_when_folder_starts_empty(tmp_path):
    pool = producer.PatientPool(str(tmp_path), rescan_seconds=5)
    pool.scan()
    schedule = producer.fixed_rate_schedule(pool, rate_hz=1, loop=False)
    idle = [next(schedule) for _ in range(3)]
    assert idle == [(0.0, None, None), (1.0, None, None), (2.0, None, None)]  # idle ticks, no busy loop
    write_jsonl(str(tmp_path / "late.jsonl"), records("L", 2))
    assert ids(schedule, 2) == [(5.0, "L-0"), (6.0, "L-1")]


def test_original_timing_follows_measured_at_and_rescan(tmp_path):
    # A: every 2 s from t=0, B: every 4 s from t=1 (data time)
    write_jsonl(str(tmp_path / "a.jsonl"), records("A", 3, start_second=0, step=2))
    write_jsonl(str(tmp_path / "b.jsonl.gz"), records("B", 2, start_second=1, step=4), compress=True)
    pool = producer.PatientPool(str(tmp_path), rescan_seconds=3)
    pool.scan()
    schedule = producer.original_timing_schedule(pool, rate_hz=1, speedup=2, loop=False)
    assert ids(schedule, 3) == [(0.0, "A-0"), (0.5, "B-0"), (1.0, "A-1")]
    # added at the rescan at offset 3: starts right away with its own timing (step 2 s -> 1 s at speedup 2)
    write_jsonl(str(tmp_path / "unit" / "c.jsonl"), records("C", 2, start_second=600, step=2))
    assert ids(schedule, 4) == [(2.0, "A-2"), (2.5, "B-1"), (3.0, "C-0"), (4.0, "C-1")]


def test_original_timing_loops_each_patient(tmp_path):
    write_jsonl(str(tmp_path / "a.jsonl"), records("A", 2, start_second=0, step=1))
    write_jsonl(str(tmp_path / "b.jsonl"), records("B", 1, start_second=1))
    pool = producer.PatientPool(str(tmp_path))
    pool.scan()
    got = ids(producer.original_timing_schedule(pool, rate_hz=1, speedup=1, loop=True), 6)
    # A: span 1 s + 1 s gap -> cycle 2 s; B: t=1 relative to the common origin, cycle 2 s
    assert got == [(0.0, "A-0"), (1.0, "B-0"), (1.0, "A-1"), (2.0, "A-0"), (3.0, "B-0"), (3.0, "A-1")]


# --- misc --------------------------------------------------------------------

def test_iso_from_ms_is_exact():
    assert producer.iso_from_ms(1767268800123) == "2026-01-01T12:00:00.123Z"
    assert producer.iso_from_ms(1767268800999) == "2026-01-01T12:00:00.999Z"


def test_record_key():
    assert producer.record_key({"userid": "0329615"}) == b"0329615"
    assert producer.record_key({"userid": 42}) == b"42"
    assert producer.record_key({}) is None


def test_config_defaults(monkeypatch):
    for name in ("RESCAN_SECONDS", "TOPIC_PARTITIONS", "KAFKA_COMPRESSION", "INPUT_DIR"):
        monkeypatch.delenv(name, raising=False)
    cfg = producer.parse_config([])
    assert cfg.rescan_seconds == 0 and cfg.topic_partitions == 1 and cfg.loop is True
    assert cfg.input_dir == "output_json_files"
    monkeypatch.setenv("RESCAN_SECONDS", "30")
    assert producer.parse_config([]).rescan_seconds == 30
