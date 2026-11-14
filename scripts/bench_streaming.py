#!/usr/bin/env python3
"""Offline benchmark: streaming full-batch build memory vs. reading count.

Builds two real v1 snapshots from generated CSV inputs whose reading counts
differ by a factor of ten against one fixed meter dimension table, and
reports wall-clock time plus the Python-side peak memory measured with
tracemalloc (DuckDB's native buffers are intentionally not traced; the claim
being demonstrated is about Python objects).

Nothing is preloaded outside the timed region: the measured builds open and
parse the CSV files themselves inside ``build``; the only untraced work is a
tiny warm-up build on separate data that primes DuckDB code paths.

Usage (from the repository root, with the project venv):

    .venv/bin/python scripts/bench_streaming.py
    .venv/bin/python scripts/bench_streaming.py --hours-small 12500 --factor 10 --batch-rows 10000
"""
import argparse
import gc
import json
import sys
import tempfile
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from meter_lake.pipeline import build  # noqa: E402

START = datetime(2020, 1, 1, tzinfo=timezone.utc)
STEP_WH = 500  # 0.5 kWh per hour, comfortably inside the 10 kW capacity


def write_meters(path, count):
    with open(path, "w", encoding="utf-8", newline="") as stream:
        stream.write("meter_id,site,multiplier,max_kw\n")
        for index in range(count):
            stream.write(f"M-{index:03d},Site {index % 2},1,10\n")


def write_readings(path, meter_count, hours):
    """Stream rows hour-major so per-meter series interleave on disk."""
    with open(path, "w", encoding="utf-8", newline="") as stream:
        stream.write("meter_id,observed_at,register_wh\n")
        for hour in range(hours):
            stamp = (START + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M:%SZ")
            register = 1000 + hour * STEP_WH
            for index in range(meter_count):
                stream.write(f"M-{index:03d},{stamp},{register}\n")
    return meter_count * hours


def measured_build(meters, readings, output, batch_rows):
    """Time one build and report the Python-only peak via tracemalloc."""
    gc.collect()
    tracemalloc.start()
    begin = time.perf_counter()
    summary = build(meters, readings, output, batch_rows=batch_rows)
    elapsed = time.perf_counter() - begin
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return summary, elapsed, peak


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--meters", type=int, default=8, help="fixed dimension size")
    parser.add_argument("--hours-small", type=int, default=6250,
                        help="hourly boundaries per meter at the small scale")
    parser.add_argument("--factor", type=int, default=10, help="row-count growth factor")
    parser.add_argument("--batch-rows", type=int, default=10000)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="meter-bench-") as temporary:
        root = Path(temporary)
        meters = root / "meters.csv"
        write_meters(meters, args.meters)

        # Warm up DuckDB code paths on separate tiny data; never measured.
        warm = root / "warm.csv"
        write_readings(warm, args.meters, 48)
        build(meters, warm, root / "warm-lake", batch_rows=args.batch_rows)

        runs = []
        for label, hours in [("small", args.hours_small),
                             ("big", args.hours_small * args.factor)]:
            readings = root / f"readings-{label}.csv"
            expected = write_readings(readings, args.meters, hours)
            print(f"[bench] {label}: {expected} readings on disk, building ...", flush=True)
            summary, elapsed, peak = measured_build(
                meters, readings, root / f"lake-{label}", args.batch_rows)
            assert summary["readings"] == expected, (summary["readings"], expected)
            print(f"[bench] {label}: done in {elapsed:.1f}s, "
                  f"python peak {peak / 2**20:.2f} MiB", flush=True)
            runs.append({"scale": label, "readings": expected, "hours_per_meter": hours,
                         "elapsed_seconds": round(elapsed, 3),
                         "python_peak_bytes": peak,
                         "python_peak_mib": round(peak / 2**20, 2)})

    small, big = runs
    row_ratio = big["readings"] / small["readings"]
    peak_ratio = big["python_peak_bytes"] / small["python_peak_bytes"]
    time_ratio = big["elapsed_seconds"] / small["elapsed_seconds"]
    verdict = peak_ratio < row_ratio / 2
    report = {
        "batch_rows": args.batch_rows,
        "meters": args.meters,
        "data_preloaded_outside_timing": False,
        "runs": runs,
        "row_ratio": row_ratio,
        "python_peak_ratio": round(peak_ratio, 3),
        "elapsed_ratio": round(time_ratio, 3),
        "peak_grows_linearly_with_rows": not verdict,
    }
    print(json.dumps(report, indent=2))
    print(f"\nrows x{row_ratio:.0f} -> Python peak x{peak_ratio:.2f}, "
          f"elapsed x{time_ratio:.2f}: "
          + ("PASS, peak does not grow linearly with total rows"
             if verdict else "FAIL, Python peak scales with total rows"))
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
