"""End-to-end incremental revision tests against real DuckDB snapshots."""
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb

from meter_lake.incremental import SnapshotError, revise
from meter_lake.patch import FinalStateError, PatchError
from meter_lake.pipeline import build

PROJECT = Path(__file__).resolve().parents[1]
DATA = PROJECT / "data"
HEAD = "op,meter_id,observed_at,register_wh,expected_wh\n"
TS_EXPR = "strftime({col} AT TIME ZONE 'UTC','%Y-%m-%dT%H:%M:%SZ')"


def fetch_table(db, table, parquet_path=None):
    """Fetch all rows with TIMESTAMPTZ columns rendered as UTC strings.

    DuckDB only needs an external timezone database when converting
    TIMESTAMPTZ into Python datetimes; strftime keeps the project dependent
    on duckdb alone.
    """
    source = f"read_parquet('{str(parquet_path).replace(chr(39), chr(39) * 2)}')" \
        if parquet_path else table
    describe = db.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    select_list = []
    for name, kind, *_ in describe:
        if "TIMESTAMP" in kind:
            select_list.append(TS_EXPR.format(col=name) + f" AS {name}")
        else:
            select_list.append(name)
    return db.execute(f"SELECT {', '.join(select_list)} FROM {source}").fetchall()


def write(path, text):
    path.write_text(text)
    return path


class IncrementalFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.meters = self.root / "meters.csv"
        self.readings = self.root / "readings.csv"

    def base(self, meters_text, readings_text, name="base"):
        write(self.meters, meters_text)
        write(self.readings, readings_text)
        target = self.root / name
        build(self.meters, self.readings, target)
        return target

    def patch_file(self, text, name="changes.csv"):
        return write(self.root / name, HEAD + text)

    def connect(self, snapshot):
        db = duckdb.connect(str(snapshot / "warehouse.duckdb"), read_only=True)
        self.addCleanup(db.close)
        return db

    def assert_no_staging(self):
        leftovers = list(self.root.glob(".meter-incr-*")) + list(self.root.glob(".meter-build-*"))
        self.assertEqual(leftovers, [])


MIDNIGHT_METERS = (
    "meter_id,site,multiplier,max_kw\n"
    "M-A,North Works,1,10\n"
    "M-B,North Works,1,5\n"
)
MIDNIGHT_READINGS = (
    "meter_id,observed_at,register_wh\n"
    "M-A,2026-01-01T23:00:00Z,1000\n"
    "M-A,2026-01-02T00:00:00Z,2000\n"
    "M-A,2026-01-02T01:00:00Z,4000\n"
    "M-B,2026-01-01T23:00:00Z,100\n"
    "M-B,2026-01-02T00:00:00Z,300\n"
    "M-B,2026-01-02T01:00:00Z,500\n"
)


class SampleSnapshotTests(IncrementalFixture):
    def test_bundled_sample_numbers_and_directory_protection_hold(self):
        self.meters.write_bytes((DATA / "meters.csv").read_bytes())
        self.readings.write_bytes((DATA / "readings.csv").read_bytes())
        snapshot = self.base(
            self.meters.read_text(), self.readings.read_text(), name="sample")
        with self.connect(snapshot) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM meters").fetchone()[0], 2)
            self.assertEqual(
                db.execute("SELECT count(*) FROM readings").fetchone()[0], 6)
            self.assertEqual(
                db.execute("SELECT count(*) FROM interval_usage").fetchone()[0], 4)
            self.assertEqual(
                db.execute("SELECT kwh FROM daily_site_usage WHERE site='North Works'").fetchone()[0],
                Decimal("2.500000"))
            self.assertEqual(
                db.execute("SELECT kwh FROM daily_site_usage WHERE site='Canal Pump'").fetchone()[0],
                Decimal("0.900000"))
            self.assertEqual(
                db.execute("SELECT sum(kwh) FROM interval_usage").fetchone()[0],
                Decimal("3.400000"))
        with self.assertRaises(FileExistsError):
            build(self.meters, self.readings, snapshot)
        with self.assertRaises(FileExistsError):
            revise(snapshot, self.patch_file(""), snapshot)


class MidnightRevisionTests(IncrementalFixture):
    def setUp(self):
        super().setUp()
        self.base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS)

    def test_midnight_revision_splits_two_days_while_total_holds(self):
        patch = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n", "midnight.csv")
        new = self.root / "v2"
        summary = revise(self.base_snapshot, patch, new)
        with self.connect(new) as db:
            rows = {
                (r[0], r[1]): r for r in db.execute(
                    "SELECT meter_id,strftime(interval_start AT TIME ZONE 'UTC','%Y-%m-%dT%H:%M:%SZ'),"
                    "delta_wh,kwh FROM interval_usage ORDER BY meter_id,interval_start").fetchall()}
            self.assertEqual(rows[("M-A", "2026-01-01T23:00:00Z")][2], 1500)
            self.assertEqual(rows[("M-A", "2026-01-01T23:00:00Z")][3], Decimal("1.500000"))
            self.assertEqual(rows[("M-A", "2026-01-02T00:00:00Z")][2], 1500)
            self.assertEqual(rows[("M-A", "2026-01-02T00:00:00Z")][3], Decimal("1.500000"))
            # Untouched meter keeps exactly the same facts.
            self.assertEqual(rows[("M-B", "2026-01-01T23:00:00Z")][2], 200)
            self.assertEqual(rows[("M-B", "2026-01-02T00:00:00Z")][2], 200)
            daily = {
                (r[0], r[1]): r for r in db.execute(
                    "SELECT site,strftime(usage_date,'%Y-%m-%d'),meter_intervals,kwh "
                    "FROM daily_site_usage ORDER BY site,usage_date").fetchall()}
            self.assertEqual(daily[("North Works", "2026-01-01")][2], 2)
            self.assertEqual(daily[("North Works", "2026-01-01")][3], Decimal("1.700000"))
            self.assertEqual(daily[("North Works", "2026-01-02")][2], 2)
            self.assertEqual(daily[("North Works", "2026-01-02")][3], Decimal("1.700000"))
            self.assertEqual(db.execute("SELECT sum(kwh) FROM interval_usage").fetchone()[0],
                             Decimal("3.400000"))
        self.assertEqual(summary["intervals"], 4)
        audit = json.loads((new / "audit.json").read_text())
        self.assertEqual(audit["affected_site_days"], [
            {"site": "North Works", "usage_date": "2026-01-01"},
            {"site": "North Works", "usage_date": "2026-01-02"},
        ])
        self.assertEqual(audit["processing_scope"]["interval_rows_deleted"], 2)
        self.assertEqual(audit["processing_scope"]["interval_rows_inserted"], 2)
        self.assertEqual(audit["processing_scope"]["daily_site_days_reaggregated"], 2)
        self.assertEqual(audit["reused_interval_rows"], 2)

    def test_adjacent_revisions_and_unordered_inserts_are_order_independent(self):
        first = (
            "replace,M-A,2026-01-01T23:00:00Z,2500,1000\n"
            "replace,M-A,2026-01-02T00:00:00Z,3000,2000\n"
        )
        second = first.splitlines(keepends=True)[::-1]
        out_a = revise(self.base_snapshot, self.patch_file(first, "a.csv"), self.root / "a")
        out_b = revise(self.base_snapshot, self.patch_file("".join(second), "b.csv"),
                       self.root / "b")
        self.assertEqual(out_a, out_b)
        with self.connect(self.root / "a") as db_a, self.connect(self.root / "b") as db_b:
            for table in ["meters", "readings", "interval_usage", "daily_site_usage"]:
                self.assertEqual(sorted(fetch_table(db_a, table)),
                                 sorted(fetch_table(db_b, table)))
        # Unordered patch appending two later contiguous hours.
        extension = (
            "insert,M-A,2026-01-02T03:00:00Z,6000,\n"
            "insert,M-A,2026-01-02T02:00:00Z,5000,\n"
            "insert,M-B,2026-01-02T03:00:00Z,900,\n"
            "insert,M-B,2026-01-02T02:00:00Z,700,\n"
        )
        extended = revise(self.base_snapshot, self.patch_file(extension, "ext.csv"),
                          self.root / "ext")
        self.assertEqual(extended["readings"], 10)
        self.assertEqual(extended["intervals"], 8)
        audit = json.loads((self.root / "ext" / "audit.json").read_text())
        # Two new boundaries per meter each add one new interval; nothing is revoked.
        self.assertEqual(audit["processing_scope"]["interval_rows_deleted"], 0)
        self.assertEqual(audit["processing_scope"]["interval_rows_inserted"], 4)


class BoundaryLifecycleTests(IncrementalFixture):
    METERS = (
        "meter_id,site,multiplier,max_kw\n"
        "M-A,Site One,1,10\n"
        "M-B,Site Two,1,10\n"
    )
    READINGS = (
        "meter_id,observed_at,register_wh\n"
        "M-A,2025-12-31T23:00:00Z,1000\n"
        "M-A,2026-01-01T00:00:00Z,2000\n"
        "M-A,2026-01-01T01:00:00Z,3000\n"
        "M-A,2026-01-01T02:00:00Z,4000\n"
        "M-B,2025-12-31T23:00:00Z,500\n"
        "M-B,2026-01-01T00:00:00Z,1500\n"
        "M-B,2026-01-01T01:00:00Z,2500\n"
        "M-B,2026-01-01T02:00:00Z,3500\n"
    )

    def setUp(self):
        super().setUp()
        self.base_snapshot = self.base(self.METERS, self.READINGS)

    def test_delete_first_and_last_boundary_revokes_edge_intervals_and_empty_day(self):
        patch = self.patch_file(
            "delete,M-A,2025-12-31T23:00:00Z,,1000\n"
            "delete,M-A,2026-01-01T02:00:00Z,,4000\n")
        new = self.root / "trimmed"
        revise(self.base_snapshot, patch, new)
        with self.connect(new) as db:
            kept = db.execute(
                "SELECT strftime(observed_at AT TIME ZONE 'UTC','%Y-%m-%dT%H:%M:%SZ'),register_wh "
                "FROM readings WHERE meter_id='M-A' ORDER BY observed_at").fetchall()
            self.assertEqual(kept, [
                ("2026-01-01T00:00:00Z", 2000),
                ("2026-01-01T01:00:00Z", 3000)])
            remaining = db.execute(
                "SELECT strftime(interval_start AT TIME ZONE 'UTC','%Y-%m-%dT%H:%M:%SZ'),delta_wh "
                "FROM interval_usage WHERE meter_id='M-A' ORDER BY interval_start").fetchall()
            self.assertEqual(remaining, [("2026-01-01T00:00:00Z", 1000)])
            # The edge intervals on 2025-12-31 and the last hour are revoked;
            # Site One's 2025-12-31 daily row must not linger.
            self.assertEqual(db.execute(
                "SELECT count(*) FROM daily_site_usage "
                "WHERE site='Site One' AND usage_date=DATE '2025-12-31'").fetchone()[0], 0)
            # Site Two is untouched and still aggregated for both days.
            self.assertEqual(db.execute(
                "SELECT count(*) FROM daily_site_usage WHERE site='Site Two'").fetchone()[0], 2)

    def test_revoking_last_fact_of_a_site_day_removes_its_daily_row(self):
        # Site Two only has M-B; deleting the boundaries of its 2025-12-31
        # interval must leave no daily row for that site-day.
        patch = self.patch_file("delete,M-B,2025-12-31T23:00:00Z,,500\n")
        new = self.root / "day-removed"
        revise(self.base_snapshot, patch, new)
        with self.connect(new) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM interval_usage "
                "WHERE site='Site Two' AND CAST(interval_start AS DATE)=DATE '2025-12-31'"
            ).fetchone()[0], 0)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM daily_site_usage "
                "WHERE site='Site Two' AND usage_date=DATE '2025-12-31'").fetchone()[0], 0)
            # Later Site Two facts and their day survive.
            self.assertEqual(db.execute(
                "SELECT count(*) FROM daily_site_usage "
                "WHERE site='Site Two' AND usage_date=DATE '2026-01-01'").fetchone()[0], 1)

    def test_internal_delete_creates_gap_and_whole_batch_fails(self):
        patch = self.patch_file("delete,M-A,2026-01-01T01:00:00Z,,3000\n")
        with self.assertRaisesRegex(FinalStateError, "not one hour"):
            revise(self.base_snapshot, patch, self.root / "bad")
        self.assertFalse((self.root / "bad").exists())
        self.assert_no_staging()

    def test_delete_below_two_boundaries_fails(self):
        patch = self.patch_file(
            "delete,M-A,2026-01-01T00:00:00Z,,2000\n"
            "delete,M-A,2026-01-01T01:00:00Z,,3000\n"
            "delete,M-A,2026-01-01T02:00:00Z,,4000\n")
        with self.assertRaisesRegex(FinalStateError, "at least 2"):
            revise(self.base_snapshot, patch, self.root / "short")

    def test_final_rollback_and_capacity_violations_fail_as_final_constraints(self):
        rollback = self.patch_file(
            "replace,M-A,2026-01-01T01:00:00Z,900,3000\n", "rollback.csv")
        with self.assertRaisesRegex(FinalStateError, "rolled back"):
            revise(self.base_snapshot, rollback, self.root / "rollback")
        overload = self.patch_file(
            "replace,M-A,2026-01-01T01:00:00Z,30000,3000\n", "overload.csv")
        with self.assertRaisesRegex(FinalStateError, "capacity"):
            revise(self.base_snapshot, overload, self.root / "overload")
        self.assert_no_staging()


class PreconditionFailureTests(IncrementalFixture):
    METERS = MIDNIGHT_METERS

    def setUp(self):
        super().setUp()
        self.base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS)
        self.before = sorted(p.relative_to(self.base_snapshot) for p in
                             self.base_snapshot.rglob("*") if p.is_file())
        self.before_bytes = {
            p.relative_to(self.base_snapshot): p.read_bytes()
            for p in self.base_snapshot.rglob("*") if p.is_file()}

    def assert_base_untouched(self):
        self.assertEqual(
            sorted(p.relative_to(self.base_snapshot) for p in
                   self.base_snapshot.rglob("*") if p.is_file()), self.before)
        for rel, data in self.before_bytes.items():
            self.assertEqual((self.base_snapshot / rel).read_bytes(), data)

    def reject(self, text, pattern, error=PatchError):
        target = self.root / "rejected"
        with self.assertRaisesRegex(error, pattern):
            revise(self.base_snapshot, self.patch_file(text), target)
        self.assertFalse(target.exists())
        self.assert_no_staging()
        self.assert_base_untouched()

    def test_expected_wh_mismatch_unknown_meter_bad_op_and_bad_time(self):
        self.reject("replace,M-A,2026-01-02T00:00:00Z,2500,1999\n", "expected_wh mismatch")
        self.reject("insert,M-X,2026-01-02T02:00:00Z,100,\n", "unknown meter")
        self.reject("freeze,M-A,2026-01-02T00:00:00Z,2000,2000\n", "op must be one of")
        self.reject("insert,M-A,2026-01-02T00:30:00Z,100,\n", "hour boundary")

    def test_equivalent_offset_duplicate_key_rejected(self):
        self.reject(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n"
            "replace,M-A,2026-01-02T01:00:00+01:00,4100,4000\n",
            "duplicate normalized key")

    def test_corrected_patch_reruns_after_failure(self):
        bad = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,1999\n", "bad.csv")
        with self.assertRaises(PatchError):
            revise(self.base_snapshot, bad, self.root / "retry")
        good = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n", "good.csv")
        summary = revise(self.base_snapshot, good, self.root / "retry")
        self.assertEqual(summary["readings"], 6)

    def test_equal_replace_header_only_zero_and_huge_delta_are_accepted(self):
        equal = revise(self.base_snapshot,
                       self.patch_file("replace,M-A,2026-01-02T00:00:00Z,2000,2000\n",
                                       "equal.csv"),
                       self.root / "equal")
        self.assertEqual(equal["intervals"], 4)
        audit = json.loads((self.root / "equal" / "audit.json").read_text())
        self.assertEqual(audit["changed_boundaries"], [])
        self.assertEqual(audit["processing_scope"]["interval_rows_inserted"], 0)
        header_only = revise(self.base_snapshot, self.patch_file("", "empty.csv"),
                             self.root / "header")
        self.assertEqual(header_only, equal)
        zero = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS.replace("2000", "1000"),
                         name="zerobase")
        revise(zero, self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,1000,1000\n", "zero.csv"),
            self.root / "zero")
        huge_meters = (
            "meter_id,site,multiplier,max_kw\nM-H,Big,1,1000000\n")
        huge_readings = (
            "meter_id,observed_at,register_wh\n"
            "M-H,2026-01-01T00:00:00Z,999999999999998\n"
            "M-H,2026-01-01T01:00:00Z,999999999999999\n"
            "M-H,2026-01-01T02:00:00Z,1000000000000000\n")
        huge_base = self.base(huge_meters, huge_readings, name="huge")
        revise(huge_base, self.patch_file(
            "replace,M-H,2026-01-01T01:00:00Z,999999999999998,999999999999999\n",
            "huge.csv"), self.root / "huge-v2")
        with self.connect(self.root / "huge-v2") as db:
            self.assertEqual(
                db.execute("SELECT delta_wh FROM interval_usage ORDER BY interval_start").fetchall(),
                [(0,), (2,)])


class RebuildEquivalenceTests(IncrementalFixture):
    METERS = (
        "meter_id,site,multiplier,max_kw\n"
        "M-A,North Works,2,100\n"
        "M-B,North Works,1,50\n"
        "M-C,Canal Pump,1,200\n"
    )
    READINGS = (
        "meter_id,observed_at,register_wh\n"
        "M-A,2026-01-01T22:00:00Z,1000\n"
        "M-A,2026-01-01T23:00:00Z,2000\n"
        "M-A,2026-01-02T00:00:00Z,3000\n"
        "M-A,2026-01-02T01:00:00Z,4000\n"
        "M-B,2026-01-01T22:00:00Z,500\n"
        "M-B,2026-01-01T23:00:00Z,700\n"
        "M-B,2026-01-02T00:00:00Z,900\n"
        "M-B,2026-01-02T01:00:00Z,1100\n"
        "M-C,2026-01-03T00:00:00Z,10\n"
        "M-C,2026-01-03T01:00:00Z,30\n"
        "M-C,2026-01-03T02:00:00Z,60\n"
    )

    def setUp(self):
        super().setUp()
        self.base_snapshot = self.base(self.METERS, self.READINGS)

    def rows(self, snapshot, table):
        with self.connect(snapshot) as db:
            return fetch_table(db, table)

    def parquet_rows(self, snapshot, layer, table):
        with self.connect(snapshot) as db:
            return fetch_table(db, table, parquet_path=snapshot / layer / f"{table}.parquet")

    def assert_table_sets_equal(self, left, right, table, keys):
        left_rows = {tuple(r[k] for k in keys): r for r in self.rows(left, table)}
        right_rows = {tuple(r[k] for k in keys): r for r in self.rows(right, table)}
        self.assertEqual(set(left_rows), set(right_rows), table)
        for key in left_rows:
            self.assertEqual(left_rows[key], right_rows[key], f"{table} {key}")

    def test_two_generations_match_full_rebuild_table_by_table_and_parquet(self):
        first_patch = self.root / "p1.csv"
        first_patch.write_text(HEAD +
            "replace,M-A,2026-01-02T00:00:00Z,3500,3000\n"
            "insert,M-C,2026-01-03T03:00:00Z,100,\n")
        v2 = self.root / "v2"
        revise(self.base_snapshot, first_patch, v2)
        second_patch = self.root / "p2.csv"
        second_patch.write_text(HEAD +
            "replace,M-A,2026-01-01T23:00:00Z,2500,2000\n"
            "delete,M-B,2026-01-01T22:00:00Z,,500\n")
        v3 = self.root / "v3"
        revise(v2, second_patch, v3)

        # Export the final silver tables as normal full-batch inputs and rebuild.
        with self.connect(v3) as db:
            meter_csv = db.execute(
                "SELECT meter_id,site,multiplier,max_kw FROM meters ORDER BY meter_id").fetchall()
            reading_csv = db.execute(
                "SELECT meter_id,strftime(observed_at AT TIME ZONE 'UTC','%Y-%m-%dT%H:%M:%SZ'),"
                "register_wh FROM readings ORDER BY meter_id,observed_at").fetchall()
        rebuilt_meters = self.root / "rebuild-meters.csv"
        rebuilt_readings = self.root / "rebuild-readings.csv"
        rebuilt_meters.write_text(
            "meter_id,site,multiplier,max_kw\n" +
            "".join(f"{m},{s},{mu},{kw}\n" for m, s, mu, kw in meter_csv))
        rebuilt_readings.write_text(
            "meter_id,observed_at,register_wh\n" +
            "".join(f"{m},{t},{r}\n" for m, t, r in reading_csv))
        rebuilt = self.root / "rebuilt"
        build(rebuilt_meters, rebuilt_readings, rebuilt)

        keys = {
            "meters": [0],
            "readings": [0, 1],
            "interval_usage": [0, 2, 3],
            "daily_site_usage": [0, 1],
        }
        for table, table_keys in keys.items():
            self.assert_table_sets_equal(v3, rebuilt, table, table_keys)
        for layer, table in [("silver", "meters"), ("silver", "readings"),
                             ("gold", "interval_usage"), ("gold", "daily_site_usage")]:
            self.assertEqual(
                sorted(map(list, self.parquet_rows(v3, layer, table))),
                sorted(map(list, self.parquet_rows(rebuilt, layer, table))),
                table)

        # Two generations of lineage and provenance are retained in order.
        manifest = json.loads((v3 / "manifest.json").read_text())
        self.assertEqual(manifest["mode"], "incremental")
        self.assertEqual(manifest["schema_version"], 2)
        roles = [(s["role"], s["file"]) for s in manifest["sources"]]
        self.assertEqual([r[0] for r in roles], ["root", "root", "patch", "patch"])
        for source in manifest["sources"]:
            stored = v3 / source["file"]
            self.assertTrue(stored.is_file())
            self.assertEqual(
                hashlib_sha(stored), source["sha256"])
        self.assertEqual(
            manifest["parent"]["manifest_sha256"], hashlib_sha(v2 / "manifest.json"))


def hashlib_sha(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class ClosureScopeTests(IncrementalFixture):
    METERS = (
        "meter_id,site,multiplier,max_kw\n"
        "M-A,North Works,1,100\n"
        "M-B,North Works,1,100\n"
        "M-C,South Yard,1,100\n"
        "M-D,South Yard,1,100\n"
    )
    READINGS = (
        "meter_id,observed_at,register_wh\n"
        + "".join(f"M-A,2026-01-01T{h:02d}:00:00Z,{h * 1000}\n" for h in range(0, 6))
        + "".join(f"M-B,2026-01-01T{h:02d}:00:00Z,{h * 200}\n" for h in range(0, 6))
        + "".join(f"M-C,2026-01-02T{h:02d}:00:00Z,{h * 100}\n" for h in range(0, 6))
        + "".join(f"M-D,2026-01-02T{h:02d}:00:00Z,{h * 50}\n" for h in range(0, 6))
    )

    def setUp(self):
        super().setUp()
        self.base_snapshot = self.base(self.METERS, self.READINGS)

    def test_only_adjacency_closure_and_affected_site_days_are_processed(self):
        before = {r[:3]: r for r in self.rows_of(self.base_snapshot, "interval_usage")}
        patch = self.patch_file(
            "replace,M-A,2026-01-01T02:00:00Z,2500,2000\n")
        new = self.root / "scoped"
        revise(self.base_snapshot, patch, new)
        after = {r[:3]: r for r in self.rows_of(new, "interval_usage")}
        changed_keys = {
            ("M-A", "2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z"),
            ("M-A", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
        }
        self.assertEqual(set(before) | changed_keys, set(after) | changed_keys)
        for key, row in before.items():
            if key not in changed_keys:
                self.assertEqual(after[key], row, f"untouched fact changed: {key}")
        audit = json.loads((new / "audit.json").read_text())
        self.assertEqual(len(audit["old_interval_candidates"]), 2)
        self.assertEqual(len(audit["new_interval_candidates"]), 2)
        self.assertEqual(audit["affected_site_days"],
                         [{"site": "North Works", "usage_date": "2026-01-01"}])
        self.assertEqual(audit["processing_scope"]["daily_site_days_reaggregated"], 1)
        # The same-site untouched meter is still present in the recomputed day.
        with self.connect(new) as db:
            row = db.execute(
                "SELECT meter_intervals,kwh FROM daily_site_usage "
                "WHERE site='North Works' AND usage_date=DATE '2026-01-01'").fetchone()
            self.assertEqual(row[0], 10)
            south = db.execute(
                "SELECT count(*) FROM daily_site_usage WHERE site='South Yard'").fetchone()[0]
            self.assertEqual(south, 1)

    def rows_of(self, snapshot, table):
        with self.connect(snapshot) as db:
            return db.execute(
                "SELECT meter_id,"
                + TS_EXPR.format(col="interval_start") + ","
                + TS_EXPR.format(col="interval_end") + ","
                "delta_wh,kwh,site,max_kw "
                f"FROM {table} ORDER BY meter_id,interval_start").fetchall()


class SelfContainedAndReplayTests(IncrementalFixture):
    def test_move_snapshot_hide_parent_then_query_parquet_and_revise_again(self):
        base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS, name="root-base")
        patch1 = self.root / "p1.csv"
        patch1.write_text(HEAD + "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n")
        v2 = self.root / "gen2"
        revise(base_snapshot, patch1, v2)
        relocated = self.root / "moved" / "gen2"
        shutil.move(str(v2), str(relocated))
        # Hide the parent entirely; the moved snapshot must stand alone.
        hidden = self.root / "hidden-parent"
        shutil.move(str(base_snapshot), str(hidden))
        try:
            with duckdb.connect(str(relocated / "warehouse.duckdb"), read_only=True) as db:
                self.assertEqual(
                    db.execute("SELECT count(*) FROM interval_usage").fetchone()[0], 4)
                parquet = str(relocated / "gold" / "interval_usage.parquet").replace("'", "''")
                self.assertEqual(
                    db.execute(f"SELECT sum(kwh) FROM read_parquet('{parquet}')").fetchone()[0],
                    Decimal("3.400000"))
            patch2 = self.root / "p2.csv"
            patch2.write_text(HEAD + "replace,M-A,2026-01-02T01:00:00Z,4500,4000\n")
            v3 = self.root / "gen3"
            revise(relocated, patch2, v3)
            with duckdb.connect(str(v3 / "warehouse.duckdb"), read_only=True) as db:
                self.assertEqual(
                    db.execute("SELECT count(*) FROM interval_usage").fetchone()[0], 4)
            # Provenance chain is fully inside the new snapshot.
            manifest = json.loads((v3 / "manifest.json").read_text())
            for source in manifest["sources"]:
                self.assertTrue((v3 / source["file"]).is_file())
            self.assertEqual(
                manifest["parent"]["manifest_sha256"], hashlib_sha(relocated / "manifest.json"))
        finally:
            shutil.move(str(hidden), str(base_snapshot))

    def test_replay_to_another_directory_is_deterministic(self):
        base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS)
        patch = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n", "replay.csv")
        first = self.root / "first"
        second = self.root / "second"
        revise(base_snapshot, patch, first)
        revise(base_snapshot, patch, second)
        for name in ["manifest.json", "audit.json"]:
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())
        for rel in ["silver/meters.parquet", "silver/readings.parquet",
                    "gold/interval_usage.parquet", "gold/daily_site_usage.parquet"]:
            self.assertEqual((first / rel).read_bytes(), (second / rel).read_bytes())

    def test_stale_patch_against_child_is_rejudged_not_accepted(self):
        base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS)
        patch = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n", "move.csv")
        child = self.root / "child"
        revise(base_snapshot, patch, child)
        # The same patch now has a stale expected_wh against the child state.
        with self.assertRaisesRegex(PatchError, "expected_wh mismatch"):
            revise(child, patch, self.root / "grandchild-stale")

    def test_overwriting_existing_target_is_refused(self):
        base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS)
        patch = self.patch_file("", "empty.csv")
        target = self.root / "taken"
        revise(base_snapshot, patch, target)
        with self.assertRaises(FileExistsError):
            revise(base_snapshot, patch, target)

    def test_missing_and_unsupported_bases_fail_without_creating_databases(self):
        patch = self.patch_file("")
        with self.assertRaises(FileNotFoundError):
            revise(self.root / "does-not-exist", patch, self.root / "x1")
        bogus = self.root / "bogus"
        bogus.mkdir()
        with self.assertRaisesRegex(SnapshotError, "manifest"):
            revise(bogus, patch, self.root / "x2")
        unsupported = self.root / "unsupported"
        unsupported.mkdir()
        (unsupported / "manifest.json").write_text(json.dumps(
            {"schema_version": 9, "mode": "streaming"}))
        (unsupported / "warehouse.duckdb").write_bytes(b"not a database")
        with self.assertRaises(SnapshotError):
            revise(unsupported, patch, self.root / "x3")
        self.assertEqual([p.name for p in self.root.iterdir() if p.name.startswith("x")], [])


class StagingFailureTests(IncrementalFixture):
    def test_injected_failures_publish_nothing_and_clean_staging(self):
        base_snapshot = self.base(MIDNIGHT_METERS, MIDNIGHT_READINGS)
        patch = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n", "fail.csv")
        for point in ["staged_provenance", "database_repaired", "parquet_export", "audit_write"]:
            target = self.root / f"failed-{point}"
            with self.assertRaises(RuntimeError):
                revise(base_snapshot, patch, target, fail_after=point)
            self.assertFalse(target.exists())
            self.assert_no_staging()
        # A normal run after injected failures still succeeds.
        target = self.root / "recovered"
        revise(base_snapshot, patch, target)
        self.assertTrue((target / "manifest.json").is_file())


class CliEndToEndTests(IncrementalFixture):
    def env(self):
        env = dict(__import__("os").environ)
        env["PYTHONPATH"] = str(PROJECT) + (
            ":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        return env

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "meter_lake.incremental", *args],
            cwd=PROJECT, env=self.env(), text=True, capture_output=True, check=False)

    def test_full_cli_flow_build_patch_query_and_failure_exit_codes(self):
        meters = self.root / "meters.csv"
        readings = self.root / "readings.csv"
        meters.write_text(MIDNIGHT_METERS)
        readings.write_text(MIDNIGHT_READINGS)
        base_snapshot = self.root / "cli-base"
        full = subprocess.run(
            [sys.executable, "-m", "meter_lake",
             "--meters", str(meters), "--readings", str(readings),
             "--output", str(base_snapshot)],
            cwd=PROJECT, env=self.env(), text=True, capture_output=True, check=False)
        self.assertEqual(full.returncode, 0, full.stderr)
        patch = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n", "cli-patch.csv")
        new = self.root / "cli-v2"
        ok = self.run_cli("--base", str(base_snapshot), "--changes", str(patch),
                          "--output", str(new))
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertTrue((new / "warehouse.duckdb").is_file())
        query = subprocess.run(
            [sys.executable, "-m", "meter_lake.query", str(new / "warehouse.duckdb")],
            cwd=PROJECT, env=self.env(), text=True, capture_output=True, check=False)
        self.assertEqual(query.returncode, 0, query.stderr)
        payload = json.loads(query.stdout)
        self.assertEqual(len(payload), 2)
        # Conflict patch exits non-zero and creates no snapshot.
        bad_patch = self.patch_file(
            "replace,M-A,2026-01-02T00:00:00Z,2500,1999\n", "cli-bad.csv")
        rejected = self.root / "cli-rejected"
        bad = self.run_cli("--base", str(base_snapshot), "--changes", str(bad_patch),
                           "--output", str(rejected))
        self.assertNotEqual(bad.returncode, 0)
        self.assertIn("expected_wh mismatch", bad.stderr)
        self.assertFalse(rejected.exists())
        # Existing output is refused through the CLI as well.
        repeat = self.run_cli("--base", str(base_snapshot), "--changes", str(patch),
                              "--output", str(new))
        self.assertNotEqual(repeat.returncode, 0)
        self.assertIn("already exists", repeat.stderr)


if __name__ == "__main__":
    unittest.main()
