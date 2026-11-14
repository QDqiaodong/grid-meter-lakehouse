"""Bounded-memory streaming full-batch build: batching, failures, CLI."""
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

import duckdb

import meter_lake.pipeline as pipeline
from meter_lake.pipeline import build

PROJECT = Path(__file__).resolve().parents[1]
DATA = PROJECT / "data"

EXPECTED_METERS = [
    ("M-A", "North Works", 1, 10),
    ("M-B", "Canal Pump", 1, 5),
]
EXPECTED_READINGS = [
    ("M-A", "2026-01-01T00:00:00Z", 1000),
    ("M-A", "2026-01-01T01:00:00Z", 2200),
    ("M-A", "2026-01-01T02:00:00Z", 3500),
    ("M-B", "2026-01-01T00:00:00Z", 300),
    ("M-B", "2026-01-01T01:00:00Z", 800),
    ("M-B", "2026-01-01T02:00:00Z", 1200),
]
EXPECTED_INTERVALS = [
    ("M-A", "North Works", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", 1200, Decimal("1.200000"), 10),
    ("M-A", "North Works", "2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z", 1300, Decimal("1.300000"), 10),
    ("M-B", "Canal Pump", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", 500, Decimal("0.500000"), 5),
    ("M-B", "Canal Pump", "2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z", 400, Decimal("0.400000"), 5),
]
EXPECTED_DAILY = [
    ("Canal Pump", "2026-01-01", 2, Decimal("0.900000")),
    ("North Works", "2026-01-01", 2, Decimal("2.500000")),
]
EXPECTED_TABLES = {
    "meters": EXPECTED_METERS,
    "readings": EXPECTED_READINGS,
    "interval_usage": EXPECTED_INTERVALS,
    "daily_site_usage": EXPECTED_DAILY,
}
PARQUET_PATHS = {
    "meters": "silver/meters.parquet",
    "readings": "silver/readings.parquet",
    "interval_usage": "gold/interval_usage.parquet",
    "daily_site_usage": "gold/daily_site_usage.parquet",
}


def fetch_relation(db, source):
    """Fetch every column, rendering TIMESTAMPTZ as UTC strings."""
    describe = db.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    select_list = []
    for name, kind, *_ in describe:
        if "TIMESTAMP" in kind:
            select_list.append(
                f"strftime({name} AT TIME ZONE 'UTC','%Y-%m-%dT%H:%M:%SZ') AS {name}")
        elif kind == "DATE":
            select_list.append(f"CAST({name} AS VARCHAR) AS {name}")
        else:
            select_list.append(name)
    return db.execute(f"SELECT {', '.join(select_list)} FROM {source}").fetchall()


class StreamingBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.meters = self.root / "meters.csv"
        self.readings = self.root / "readings.csv"
        self.meters.write_bytes((DATA / "meters.csv").read_bytes())
        self.readings.write_bytes((DATA / "readings.csv").read_bytes())
        self.out = self.root / "lake"

    def assert_not_published(self):
        self.assertFalse(self.out.exists())
        self.assertEqual(list(self.root.glob(".meter-build-*")), [])

    def test_four_tables_column_by_column(self):
        # batch_rows=2 forces three batches over the deliberately unordered input.
        result = build(self.meters, self.readings, self.out, batch_rows=2)
        self.assertEqual((result["meters"], result["readings"], result["intervals"]), (2, 6, 4))
        with duckdb.connect(str(self.out / "warehouse.duckdb"), read_only=True) as db:
            for table, expected in EXPECTED_TABLES.items():
                self.assertEqual(sorted(fetch_relation(db, table)), sorted(expected), table)
            for table, expected in EXPECTED_TABLES.items():
                parquet = str(self.out / PARQUET_PATHS[table]).replace("'", "''")
                rows = fetch_relation(db, f"read_parquet('{parquet}')")
                self.assertEqual(sorted(rows), sorted(expected), f"parquet:{table}")

    def test_batch_size_does_not_change_results(self):
        manifests = []
        for batch_rows in [1, 2, 3, 10000]:
            out = self.root / f"lake-{batch_rows}"
            build(self.meters, self.readings, out, batch_rows=batch_rows)
            manifests.append((out / "manifest.json").read_bytes())
        self.assertEqual(len(set(manifests)), 1)

    def test_batch_rows_rejected(self):
        for bad in [True, False, 0, -7, 2.5, "1000", None]:
            with self.subTest(batch_rows=bad):
                with self.assertRaises(ValueError):
                    build(self.meters, self.readings, self.out, batch_rows=bad)
                self.assert_not_published()

    def test_cross_batch_utc_equivalent_duplicate(self):
        # The +02:00 alias of M-A@00:00:00Z lands three batches after the
        # original (batch_rows=2), so only the DuckDB key can catch it.
        alias = "M-A,2026-01-01T02:00:00+02:00,1000\n"
        self.readings.write_text(self.readings.read_text() + alias)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build(self.meters, self.readings, self.out, batch_rows=2)
        self.assert_not_published()

    def test_cross_batch_duplicate_arriving_first(self):
        # Same UTC-equivalent pair, but the alias is scanned first.
        lines = self.readings.read_text().splitlines(keepends=True)
        lines.insert(1, "M-A,2026-01-01T02:00:00+02:00,1000\n")
        self.readings.write_text("".join(lines))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build(self.meters, self.readings, self.out, batch_rows=2)
        self.assert_not_published()

    def test_missing_field_rejected(self):
        self.readings.write_text(
            self.readings.read_text().replace("M-A,2026-01-01T01:00:00Z,2200",
                                              "M-A,2026-01-01T01:00:00Z"))
        with self.assertRaisesRegex(ValueError, "field count"):
            build(self.meters, self.readings, self.out, batch_rows=3)
        self.assert_not_published()

    def test_last_batch_bad_row_not_published(self):
        # batch_rows=1: the invalid register sits alone in the final batch.
        self.readings.write_text(
            self.readings.read_text() + "M-A,2026-01-01T03:00:00Z,1000000000000001\n")
        with self.assertRaisesRegex(ValueError, "invalid register"):
            build(self.meters, self.readings, self.out, batch_rows=1)
        self.assert_not_published()

    def test_zero_consumption_interval(self):
        self.readings.write_text(self.readings.read_text().replace(",3500", ",2200"))
        result = build(self.meters, self.readings, self.out, batch_rows=2)
        north = next(r for r in result["daily"] if r["site"] == "North Works")
        self.assertEqual(Decimal(north["kwh"]), Decimal("1.2"))
        with duckdb.connect(str(self.out / "warehouse.duckdb"), read_only=True) as db:
            row = db.execute(
                "SELECT delta_wh, kwh FROM interval_usage "
                "WHERE meter_id='M-A' AND interval_end=TIMESTAMPTZ '2026-01-01 02:00:00+00'"
            ).fetchone()
        self.assertEqual(row, (0, Decimal("0.000000")))

    def test_export_failure_cleans_staging(self):
        real_connect = duckdb.connect

        class Bomb:
            def __init__(self, inner):
                self.inner = inner

            def __enter__(self):
                self.db = self.inner.__enter__()
                return self

            def __exit__(self, *exc):
                return self.inner.__exit__(*exc)

            def execute(self, sql, *args, **kwargs):
                if str(sql).lstrip().upper().startswith("COPY"):
                    raise RuntimeError("injected export failure")
                return self.db.execute(sql, *args, **kwargs)

            def executemany(self, *args, **kwargs):
                return self.db.executemany(*args, **kwargs)

        with mock.patch.object(pipeline.duckdb, "connect",
                               side_effect=lambda *a, **k: Bomb(real_connect(*a, **k))):
            with self.assertRaisesRegex(RuntimeError, "export failure"):
                build(self.meters, self.readings, self.out, batch_rows=2)
        self.assert_not_published()

    def test_cli_batch_rows_one_end_to_end(self):
        command = [sys.executable, "-m", "meter_lake", "--meters", str(self.meters),
                   "--readings", str(self.readings), "--output", str(self.out),
                   "--batch-rows", "1"]
        proc = subprocess.run(command, cwd=PROJECT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = json.loads(proc.stdout)
        self.assertEqual((summary["meters"], summary["readings"], summary["intervals"]), (2, 6, 4))
        manifest = json.loads((self.out / "manifest.json").read_text())
        for name, source in [("meters.csv", self.meters), ("readings.csv", self.readings)]:
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            self.assertEqual(manifest["inputs"][name], digest)
            self.assertEqual((self.out / "bronze" / name).read_bytes(), source.read_bytes())
        with duckdb.connect(str(self.out / "warehouse.duckdb"), read_only=True) as db:
            self.assertEqual(sorted(fetch_relation(db, "daily_site_usage")), sorted(EXPECTED_DAILY))
        # An existing output is still refused, and an invalid batch size fails fast.
        again = subprocess.run(command, cwd=PROJECT, capture_output=True, text=True)
        self.assertEqual(again.returncode, 2)
        self.assertIn("already exists", again.stderr)
        bad = subprocess.run(command[:-1] + ["0"], cwd=PROJECT, capture_output=True, text=True)
        self.assertEqual(bad.returncode, 2)
        self.assertIn("batch_rows", bad.stderr)


if __name__ == "__main__":
    unittest.main()
