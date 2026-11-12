import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
import duckdb
from meter_lake.pipeline import build

DATA = Path(__file__).resolve().parents[1] / "data"


class LakeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.meters = self.root / "meters.csv"
        self.readings = self.root / "readings.csv"
        self.meters.write_bytes((DATA / "meters.csv").read_bytes())
        self.readings.write_bytes((DATA / "readings.csv").read_bytes())
        self.out = self.root / "lake"

    def run_build(self):
        return build(self.meters, self.readings, self.out)

    def test_layers_sql_semantics_and_exact_energy(self):
        result = self.run_build()
        self.assertEqual((result["meters"], result["readings"], result["intervals"]), (2,6,4))
        self.assertEqual((self.out / "bronze/readings.csv").read_bytes(), self.readings.read_bytes())
        with duckdb.connect(str(self.out / "warehouse.duckdb"), read_only=True) as db:
            self.assertEqual(db.execute("SELECT sum(kwh) FROM interval_usage").fetchone()[0], Decimal("3.400000"))
            self.assertEqual(db.execute("SELECT kwh FROM daily_site_usage WHERE site='North Works'").fetchone()[0], Decimal("2.500000"))
            parquet = str(self.out / "gold/interval_usage.parquet").replace("'", "''")
            self.assertEqual(db.execute(f"SELECT sum(kwh) FROM read_parquet('{parquet}')").fetchone()[0], Decimal("3.400000"))
        self.assertEqual(len(json.loads((self.out / "manifest.json").read_text())["inputs"]), 2)

    def test_replay_new_snapshot_and_refuse_overwrite(self):
        first = self.run_build()
        self.assertEqual(first, build(self.meters, self.readings, self.root / "replay"))
        before = (self.out / "manifest.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_build()
        self.assertEqual(before, (self.out / "manifest.json").read_bytes())

    def assert_invalid(self, old, new, message=None):
        self.readings.write_text(self.readings.read_text().replace(old, new))
        with self.assertRaises(ValueError):
            self.run_build()
        self.assertFalse(self.out.exists())
        self.assertEqual(list(self.root.glob(".meter-build-*")), [])

    def test_register_reset(self):
        self.assert_invalid(",3500", ",1500")

    def test_missing_hour(self):
        self.assert_invalid("2026-01-01T01:00:00Z,2200", "2026-01-01T04:00:00Z,2200")

    def test_unknown_meter(self):
        self.assert_invalid("M-B", "unknown")

    def test_duplicate_key(self):
        self.readings.write_text(self.readings.read_text()+"M-A,2026-01-01T00:00:00Z,1000\n")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.run_build()

    def test_capacity(self):
        self.assert_invalid(",3500", ",100000")

    def test_zero_usage_and_meter_multiplier(self):
        self.readings.write_text(self.readings.read_text().replace(",3500", ",2200"))
        self.meters.write_text(self.meters.read_text().replace("North Works,1,", "North Works,2,"))
        result = self.run_build()
        north = next(r for r in result["daily"] if r["site"] == "North Works")
        self.assertEqual(Decimal(north["kwh"]), Decimal("2.4"))

    def test_timezone_and_hour_contract(self):
        self.assert_invalid("2026-01-01T00:00:00Z", "2026-01-01T00:30:00Z")

    def test_empty_input(self):
        self.readings.write_text("meter_id,observed_at,register_wh\n")
        with self.assertRaisesRegex(ValueError, "empty"):
            self.run_build()


if __name__ == "__main__":
    unittest.main()
