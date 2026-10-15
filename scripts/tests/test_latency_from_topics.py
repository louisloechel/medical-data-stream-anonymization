"""Unit tests for scripts/latency_from_topics.py (synthetic values, no Kafka)."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import latency_from_topics as lat  # noqa: E402


def test_percentile_nearest_rank():
    values = list(range(1, 101))
    assert lat.percentile(values, 50) == 50
    assert lat.percentile(values, 95) == 95
    assert lat.percentile(values, 99) == 99
    assert lat.percentile(values, 100) == 100
    assert lat.percentile([7], 99) == 7
    assert lat.percentile([], 50) is None


def test_summarize():
    st = lat.summarize([30, 10, 20])
    assert st == {"n": 3, "mean": 20, "p50": 20, "p95": 30, "p99": 30, "max": 30}
    assert lat.summarize([]) == {"n": 0}


def test_parse_iso_ms():
    assert lat.parse_iso_ms("2026-01-01T12:00:00.123Z") == 1767268800123
    assert lat.parse_iso_ms("2026-01-01T13:00:00.123+01:00") == 1767268800123
    assert lat.parse_iso_ms("garbage") is None
    assert lat.parse_iso_ms(None) is None


def rec(recordid, ts, **extra):
    return json.dumps(dict(recordid=recordid, userid="SYN-1", timestamp=ts, **extra)).encode()


def test_stage_latencies_join_by_recordid_and_timestamp():
    t0 = 1767268800000
    raw, processed, prink = lat.TopicSample(), lat.TopicSample(), lat.TopicSample()
    for i in range(10):
        ts = f"2026-01-01T12:00:{i:02d}.000Z"
        sent = t0 + i * 1000
        raw.add(rec(i, ts), sent)                    # producer: CreateTime == timestamp
        processed.add(rec(str(i), ts), sent + 5)     # recordid normalized to a string by flink-job
        if i % 2 == 0:
            prink.add(rec(str(i), ts), sent + 5 + 1000)  # Prink releases half of them, 1 s later
    raw.add(b"not json", t0)

    assert lat.summarize(raw.e2e_ms)["max"] == 0
    assert lat.summarize(processed.e2e_ms)["p50"] == 5
    assert sorted(lat.stage_latencies(raw, processed)) == [5] * 10
    assert sorted(lat.stage_latencies(processed, prink)) == [1000] * 5
    assert raw.unparseable == 1
    assert abs(processed.throughput() - 10 / 9.0) < 1e-9


def test_report_prints_aggregates_only():
    samples = {s: lat.TopicSample() for s in lat.STAGES}
    for i in range(3):
        samples["raw"].add(rec(i, "2026-01-01T12:00:00.000Z", username="Synthetic Person"), 1767268800000 + i)
    text = lat.report(samples, {"raw": "raw-topic", "processed": "processed-topic", "prink": "prink-topic"})
    assert "raw-topic" in text and "p99" in text
    assert "SYN-1" not in text and "Synthetic Person" not in text


def test_lookback_records_match_but_are_not_counted():
    window_start = 1767268900000
    processed, prink = lat.TopicSample(window_start), lat.TopicSample(window_start)
    ts = "2026-01-01T12:00:00.000Z"
    processed.add(rec(1, ts), window_start - 60000)   # lookback: written 60 s before the window
    prink.add(rec(1, ts), window_start + 5000)        # released inside the window
    assert len(processed.create_ms) == 0 and processed.read == 1
    assert lat.stage_latencies(processed, prink) == [65000]
    assert lat.stage_latencies(prink, processed) == []  # only in-window downstream records count
