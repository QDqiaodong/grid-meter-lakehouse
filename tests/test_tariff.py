"""Synthetic monthly site tariff comparison tests.

Covers strict tariff parsing, off-peak two-meter coincidence, tier boundaries,
missing hours versus legitimate zero consumption, preference for unrounded
costs, atomic failure behaviour and byte-identical reruns over real snapshots.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import shutil

from meter_lake.pipeline import build
from meter_lake.tariff import (
    BadFactError,
    MonthError,
    ReportError,
    SnapshotError,
    TariffError,
    compare_month,
    load_tariff,
    parse_month,
    render_report,
    write_report,
)
from meter_lake.tariff.model import decimal_text, fixed_text

PROJECT = Path(__file__).resolve().parents[1]
DATA = PROJECT / "data"


def utc(year, month, day, hour):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def stamp(instant):
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_tariff(path, **overrides):
    document = {
        "tariff_id": "T-FLAT",
        "segments": [{"id": "flat", "start_hour": 0, "end_hour": 24,
                      "price_per_kwh": "1.00"}],
        "tiers": [{"id": "t0", "threshold_kwh": "0", "price_per_kwh": "0"}],
        "demand_price_per_kw": "0",
        "fixed_fee": "0",
    }
    document.update(overrides)
    path.write_text(json.dumps(document, indent=2) + "\n")
    return path


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.meters = self.root / "meters.csv"
        self.readings = self.root / "readings.csv"

    def snapshot(self, meters_text, readings_text, name="lake"):
        self.meters.write_text(meters_text)
        self.readings.write_text(readings_text)
        target = self.root / name
        build(self.meters, self.readings, target)
        return target

    def tariff(self, name="tariff.json", **overrides):
        return write_tariff(self.root / name, **overrides)


class ParseMonthTests(unittest.TestCase):
    def test_month_bounds_are_utc_half_open_and_exact(self):
        month = parse_month("2026-02")
        self.assertEqual(stamp(month.start), "2026-02-01T00:00:00Z")
        self.assertEqual(stamp(month.end), "2026-03-01T00:00:00Z")
        self.assertEqual(month.hours, 28 * 24)
        month = parse_month("2024-02")
        self.assertEqual(month.hours, 29 * 24)

    def test_illegal_months_rejected(self):
        for text in ["2026-1", "2026-00", "2026-13", "2026-01-01",
                     "0000-01", "9999-12", 202601, None, " 2026-01"]:
            with self.subTest(text=text):
                with self.assertRaises(MonthError):
                    parse_month(text)


class TariffContractTests(Fixture):
    def test_accepts_strict_plan_with_decimal_strings(self):
        tariff = load_tariff(self.tariff(
            segments=[
                {"id": "a", "start_hour": 6, "end_hour": 12, "price_per_kwh": "0.25"},
                {"id": "b", "start_hour": 12, "end_hour": 24, "price_per_kwh": "0.40"},
                {"id": "c", "start_hour": 0, "end_hour": 6, "price_per_kwh": "0.10"},
            ],
            tiers=[
                {"id": "a", "threshold_kwh": "0", "price_per_kwh": "0.01"},
                {"id": "b", "threshold_kwh": "10.5", "price_per_kwh": "0.02"},
                {"id": "c", "threshold_kwh": "20", "price_per_kwh": "0.03"},
            ],
            demand_price_per_kw="2.35", fixed_fee="7.00",
        ))
        self.assertEqual([(s.start_hour, s.end_hour) for s in tariff.segments],
                         [(0, 6), (6, 12), (12, 24)])
        self.assertEqual([t.threshold for t in tariff.tiers],
                         [Decimal("0"), Decimal("10.5"), Decimal("20")])
        self.assertEqual(tariff.demand_price, Decimal("2.35"))
        self.assertEqual(tariff.fixed_fee, Decimal("7.00"))

    def test_json_booleans_floats_negatives_and_exponents_rejected(self):
        bad_segments = {"id": "flat", "start_hour": 0, "end_hour": 24,
                        "price_per_kwh": "0.50"}
        cases = [
            {"segments": [dict(bad_segments, price_per_kwh=0.5)]},
            {"segments": [dict(bad_segments, price_per_kwh="-0.5")]},
            {"segments": [dict(bad_segments, price_per_kwh="1e-3")]},
            {"segments": [dict(bad_segments, price_per_kwh="01.0")]},
            {"segments": [dict(bad_segments, start_hour=True)]},
            {"segments": [dict(bad_segments, start_hour=0.0)]},
            {"fixed_fee": -1},
            {"fixed_fee": "+1.00"},
            {"demand_price_per_kw": "1,000.00"},
            {"tiers": [{"id": "t", "threshold_kwh": "-1", "price_per_kwh": "0"}]},
            {"tiers": [{"id": "t", "threshold_kwh": 0, "price_per_kwh": "0"}]},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(TariffError):
                    load_tariff(self.tariff(**overrides))

    def test_duplicate_json_keys_rejected(self):
        path = self.root / "dupe.json"
        path.write_text(
            '{"tariff_id":"A","tariff_id":"B","segments":[],"tiers":[],'
            '"demand_price_per_kw":"0","fixed_fee":"0"}')
        with self.assertRaisesRegex(TariffError, "duplicate JSON key"):
            load_tariff(path)

    def test_non_utf8_manifest_and_unsupported_constants(self):
        path = self.root / "nan.json"
        path.write_text(
            '{"tariff_id":"N","segments":[],"tiers":[],'
            '"demand_price_per_kw":NaN,"fixed_fee":"0"}')
        with self.assertRaisesRegex(TariffError, "non-finite"):
            load_tariff(path)
        path = self.root / "latin.json"
        path.write_bytes(b"\xff\xfe")
        with self.assertRaises(TariffError):
            load_tariff(path)

    def test_hour_coverage_must_be_exact_mutually_exclusive(self):
        cases = [
            [{"id": "a", "start_hour": 0, "end_hour": 23, "price_per_kwh": "1"}],
            [{"id": "a", "start_hour": 1, "end_hour": 24, "price_per_kwh": "1"}],
            [{"id": "a", "start_hour": 0, "end_hour": 12, "price_per_kwh": "1"},
             {"id": "b", "start_hour": 12, "end_hour": 13, "price_per_kwh": "1"}],
            [{"id": "a", "start_hour": 0, "end_hour": 12, "price_per_kwh": "1"},
             {"id": "b", "start_hour": 11, "end_hour": 24, "price_per_kwh": "1"}],
            [{"id": "a", "start_hour": 0, "end_hour": 24, "price_per_kwh": "1"},
             {"id": "b", "start_hour": 0, "end_hour": 1, "price_per_kwh": "1"}],
        ]
        for segments in cases:
            with self.subTest(segments=segments):
                with self.assertRaises(TariffError):
                    load_tariff(self.tariff(segments=segments))

    def test_unsorted_segments_accepted_and_duplicates_identified(self):
        tariff = load_tariff(self.tariff(segments=[
            {"id": "b", "start_hour": 12, "end_hour": 24, "price_per_kwh": "1"},
            {"id": "a", "start_hour": 0, "end_hour": 12, "price_per_kwh": "1"},
        ]))
        self.assertEqual([s.segment_id for s in tariff.segments], ["a", "b"])
        with self.assertRaisesRegex(TariffError, "unique"):
            load_tariff(self.tariff(segments=[
                {"id": "same", "start_hour": 0, "end_hour": 12, "price_per_kwh": "1"},
                {"id": "same", "start_hour": 12, "end_hour": 24, "price_per_kwh": "1"},
            ]))

    def test_tier_boundaries_must_start_at_zero_and_increase(self):
        cases = [
            [{"id": "a", "threshold_kwh": "5", "price_per_kwh": "1"}],
            [{"id": "a", "threshold_kwh": "0", "price_per_kwh": "1"},
             {"id": "b", "threshold_kwh": "0", "price_per_kwh": "1"}],
            [{"id": "a", "threshold_kwh": "10", "price_per_kwh": "1"},
             {"id": "b", "threshold_kwh": "5", "price_per_kwh": "1"}],
        ]
        for tiers in cases:
            with self.subTest(tiers=tiers):
                with self.assertRaises(TariffError):
                    load_tariff(self.tariff(tiers=tiers))

    def test_unknown_fields_and_bad_shapes_rejected(self):
        cases = [
            {"extra": 1},
            {"segments": {}},
            {"tiers": []},
            {"tariff_id": ""},
            {"segments": [{"id": "a", "start_hour": 0, "end_hour": 24,
                           "price_per_kwh": "1", "note": "x"}]},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(TariffError):
                    load_tariff(self.tariff(**overrides))


class ComparisonFixture(Fixture):
    """Two meters on one site with a deterministic full January 2026."""

    HOURS = 31 * 24

    def two_meter_snapshot(self, rows_a, rows_b=None, name="lake", hours=None):
        hours = self.HOURS if hours is None else hours
        rows_b = rows_b if rows_b is not None else {hour: 0 for hour in range(hours)}
        readings = ["meter_id,observed_at,register_wh"]
        for meter, delta in [("M-A", rows_a), ("M-B", rows_b)]:
            register = 0
            readings.append(f"{meter},{stamp(utc(2026, 1, 1, 0))},{register}")
            for hour in range(hours):
                register += delta.get(hour, 0)
                readings.append(
                    f"{meter},{stamp(utc(2026, 1, 1, 0) + timedelta(hours=hour + 1))},{register}")
        meters = ("meter_id,site,multiplier,max_kw\n"
                  "M-A,North Works,1,1000000\nM-B,North Works,1,1000000\n")
        return self.snapshot(meters, "\n".join(readings) + "\n", name)

    def corrupt(self, snapshot, sql):
        import duckdb
        with duckdb.connect(str(snapshot / "warehouse.duckdb")) as db:
            db.execute(sql)


class OffPeakTwoMeterTests(ComparisonFixture):
    def test_peak_hour_coincidence_and_offpeak_segment_pricing(self):
        # M-A and M-B peaks at different hours are not additive at the site.
        rows_a = {hour: 0 for hour in range(self.HOURS)}
        rows_b = {hour: 0 for hour in range(self.HOURS)}
        rows_a[0] = 1000      # 00:00, site 1.0 + 0.5 = 1.5 kWh
        rows_a[3] = 1000
        rows_b[0] = 500
        rows_b[4] = 2000      # 04:00, site = 2.0 kWh (M-B alone)
        snapshot = self.two_meter_snapshot(rows_a, rows_b)
        tariff = self.tariff(
            segments=[
                {"id": "off", "start_hour": 0, "end_hour": 6, "price_per_kwh": "0.20"},
                {"id": "on", "start_hour": 6, "end_hour": 24, "price_per_kwh": "0.85"},
            ],
            demand_price_per_kw="3.00", fixed_fee="12.00")
        report = compare_month(snapshot, "2026-01", "North Works", [tariff])
        self.assertTrue(report["comparable"])
        # Naive per-table peak sum would be 1.0 + 2.0 = 3.0 kWh; the site peak
        # at 00:00 is 1.5 kWh and at 04:00 is 2.0 kWh.
        self.assertEqual(report["peak"]["hourly_site_kwh"], "2.000000")
        self.assertEqual([hour["interval_start"] for hour in report["peak"]["hours"]],
                         [stamp(utc(2026, 1, 1, 4))])
        block = report["comparison"][0]
        components = block["components"]
        # All four facts fall in the 0..6h off-peak segment: 1+1+0.5+2 kWh.
        self.assertEqual(components["energy_yuan"], "0.9")
        self.assertEqual(components["demand_charge_yuan"], "6")
        self.assertEqual(components["fixed_fee_yuan"], "12")
        self.assertEqual(components["total_yuan"], "18.9")
        self.assertEqual(components["total_yuan_rounded"], "18.90")
        self.assertEqual(components["total_fen"], 1890)
        hours = {(row["meter_id"], row["interval_start"]): row["kwh"]
                 for row in report["hourly_basis"]}
        self.assertEqual(len(hours), 2 * self.HOURS)
        self.assertEqual(hours[("M-A", "2026-01-01T03:00:00Z")], "1.000000")
        self.assertEqual(hours[("M-B", "2026-01-01T04:00:00Z")], "2.000000")
        self.assertEqual(hours[("M-B", "2026-01-01T05:00:00Z")], "0.000000")
        priced = {(meter["meter_id"], row["interval_start"]): row["segment_id"]
                  for meter in block["meters"] for row in meter["hours"]}
        self.assertEqual(priced[("M-B", "2026-01-01T04:00:00Z")], "off")

    def test_all_tied_peak_hours_are_reported(self):
        snapshot = self.two_meter_snapshot({hour: 300 for hour in range(self.HOURS)})
        report = compare_month(snapshot, "2026-01", "North Works", [self.tariff()])
        self.assertEqual(report["peak"]["hourly_site_kwh"], "0.300000")
        self.assertEqual(len(report["peak"]["hours"]), self.HOURS)
        self.assertEqual(report["peak"]["hours"][0]["interval_start"],
                         "2026-01-01T00:00:00Z")
        self.assertEqual(report["comparison"][0]["components"]["energy_yuan"], "223.2")


class TierBoundaryTests(ComparisonFixture):
    def test_tiers_use_marginal_surcharge_only_and_shared_site_total(self):
        # Site total exactly 10.0 kWh: 0..5 at 0.10, 5..10 at 0.30, none above.
        rows_a = {hour: 0 for hour in range(self.HOURS)}
        rows_b = {hour: 0 for hour in range(self.HOURS)}
        rows_a[0] = 5000
        rows_b[0] = 5000
        snapshot = self.two_meter_snapshot(rows_a, rows_b)
        tariff = self.tariff(
            segments=[{"id": "flat", "start_hour": 0, "end_hour": 24,
                       "price_per_kwh": "0"}],
            tiers=[
                {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.10"},
                {"id": "t2", "threshold_kwh": "5", "price_per_kwh": "0.30"},
                {"id": "t3", "threshold_kwh": "10", "price_per_kwh": "0.70"},
            ])
        report = compare_month(snapshot, "2026-01", "North Works", [tariff])
        self.assertEqual(report["site_month"]["total_kwh"], "10.000000")
        components = report["comparison"][0]["components"]
        self.assertEqual(components["energy_yuan"], "0")
        self.assertEqual(components["tier_surcharge_yuan"], "2")
        allocation = report["comparison"][0]["tier_allocation"]
        self.assertEqual([row["kwh"] for row in allocation],
                         ["5.000000", "5.000000", "0.000000"])
        self.assertEqual([row["surcharge_yuan"] for row in allocation],
                         ["0.5", "1.5", "0"])

    def test_exact_boundary_hits_charge_the_lower_tier_only(self):
        # Total exactly 5.0 kWh: tiers 2 and 3 get nothing.
        rows_a = {hour: 0 for hour in range(self.HOURS)}
        rows_a[0] = 5000
        snapshot = self.two_meter_snapshot(rows_a)
        tariff = self.tariff(
            tiers=[
                {"id": "a", "threshold_kwh": "0", "price_per_kwh": "0.10"},
                {"id": "b", "threshold_kwh": "5", "price_per_kwh": "0.90"},
                {"id": "c", "threshold_kwh": "5.000001", "price_per_kwh": "9.99"},
            ])
        report = compare_month(snapshot, "2026-01", "North Works", [tariff])
        allocation = report["comparison"][0]["tier_allocation"]
        self.assertEqual([row["kwh"] for row in allocation],
                         ["5.000000", "0.000000", "0.000000"])
        self.assertEqual(report["comparison"][0]["components"]["tier_surcharge_yuan"],
                         "0.5")

    def test_rounding_only_applies_to_final_total_and_ranking_ignores_it(self):
        # Two plans differ by 0.001 yuan; unrounded totals decide the order.
        rows = {hour: 0 for hour in range(self.HOURS)}
        snapshot = self.two_meter_snapshot(rows)
        first = self.tariff(name="a.json", tariff_id="A", fixed_fee="0.004")
        second = self.tariff(name="b.json", tariff_id="B", fixed_fee="0.005")
        report = compare_month(snapshot, "2026-01", "North Works", [second, first])
        totals = [(b["tariff_id"], b["components"]["total_yuan"],
                   b["components"]["total_yuan_rounded"], b["rank"])
                  for b in report["comparison"]]
        self.assertEqual(totals, [
            ("A", "0.004", "0.00", 1),
            ("B", "0.005", "0.01", 2),
        ])
        self.assertEqual(report["comparison"][0]["components"]["total_fen"], 0)


class MissingAndZeroTests(ComparisonFixture):
    def test_zero_is_a_fact_but_a_missing_hour_blocks_ranking(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        tariff = self.tariff()
        report = compare_month(snapshot, "2026-01", "North Works", [tariff])
        self.assertTrue(report["comparable"])
        self.assertEqual(report["site_month"]["total_kwh"], "0.000000")
        self.assertEqual(report["comparison"][0]["components"]["total_yuan"], "0")
        self.assertEqual(len(report["peak"]["hours"]), self.HOURS)

    def test_partial_month_is_named_and_ranking_is_withheld(self):
        # Only the first 48 hours are covered; the other 696 are missing.
        rows = {hour: 100 for hour in range(48)}
        snapshot = self.two_meter_snapshot(rows, hours=48, name="partial")
        report = compare_month(snapshot, "2026-01", "North Works",
                               [self.tariff()])
        self.assertFalse(report["comparable"])
        self.assertEqual(report["comparison"], [])
        self.assertEqual(report["hourly_basis"], [])
        self.assertIsNone(report["peak"])
        self.assertIsNone(report["site_month"])
        gaps = {gap["meter_id"]: gap for gap in report["gaps"]}
        self.assertEqual(sorted(gaps), ["M-A", "M-B"])
        self.assertEqual(gaps["M-A"]["missing_hours"], 696)
        self.assertEqual(gaps["M-A"]["ranges"], [
            {"start": "2026-01-03T00:00:00Z", "end": "2026-02-01T00:00:00Z",
             "hours": 696}])

    def test_interior_gap_ranges_merge_adjacent_hours(self):
        snapshot = self.two_meter_snapshot({hour: 100 for hour in range(self.HOURS)})
        # Simulate a snapshot missing two interior M-A hours.
        self.corrupt(snapshot,
                     "DELETE FROM interval_usage WHERE meter_id='M-A' AND "
                     "interval_start IN (TIMESTAMPTZ '2026-01-01 03:00:00+00', "
                     "TIMESTAMPTZ '2026-01-01 04:00:00+00')")
        report = compare_month(snapshot, "2026-01", "North Works",
                               [self.tariff()])
        self.assertEqual(len(report["gaps"]), 1)
        gap = report["gaps"][0]
        self.assertEqual(gap["meter_id"], "M-A")
        self.assertEqual(gap["missing_hours"], 2)
        self.assertEqual(gap["ranges"], [
            {"start": "2026-01-01T03:00:00Z", "end": "2026-01-01T05:00:00Z",
             "hours": 2}])


class BadFactTests(ComparisonFixture):
    def test_negative_energy_and_wrong_width_rejected(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        self.corrupt(snapshot, "UPDATE interval_usage SET kwh=-1 WHERE meter_id='M-B' AND "
                              "interval_start=TIMESTAMPTZ '2026-01-01 00:00:00+00'")
        with self.assertRaisesRegex(BadFactError, "invalid kwh"):
            compare_month(snapshot, "2026-01", "North Works", [self.tariff()])

    def test_duplicate_hourly_fact_rejected(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        self.corrupt(snapshot,
                     "INSERT INTO interval_usage SELECT meter_id,site,interval_start,"
                     "interval_end,delta_wh,kwh,max_kw FROM interval_usage "
                     "WHERE meter_id='M-A' AND interval_start=TIMESTAMPTZ "
                     "'2026-01-01 00:00:00+00'")
        with self.assertRaisesRegex(BadFactError, "duplicates"):
            compare_month(snapshot, "2026-01", "North Works", [self.tariff()])

    def test_site_mismatch_and_non_hour_interval_rejected(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        self.corrupt(snapshot,
                     "UPDATE interval_usage SET interval_end=interval_end + "
                     "INTERVAL '30 minutes' WHERE meter_id='M-A' AND "
                     "interval_start=TIMESTAMPTZ '2026-01-01 00:00:00+00'")
        with self.assertRaises(BadFactError):
            compare_month(snapshot, "2026-01", "North Works", [self.tariff()])


class SnapshotAndOutputContractTests(ComparisonFixture):
    def test_unknown_site_snapshot_version_and_missing_files(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        with self.assertRaises(MonthError):
            compare_month(snapshot, "2026-01", "Nowhere", [self.tariff()])
        with self.assertRaises(FileNotFoundError):
            compare_month(self.root / "absent", "2026-01", "North Works",
                          [self.tariff()])
        broken = self.root / "broken"
        broken.mkdir()
        (broken / "manifest.json").write_text(
            json.dumps({"schema_version": 3, "mode": "streaming", "timezone": "UTC"}))
        (broken / "warehouse.duckdb").write_bytes(b"junk")
        with self.assertRaises(SnapshotError):
            compare_month(broken, "2026-01", "North Works", [self.tariff()])

    def test_snapshot_manifest_must_stay_utc_and_be_json(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        manifest = json.loads((snapshot / "manifest.json").read_text())
        manifest["timezone"] = "Asia/Shanghai"
        (snapshot / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(SnapshotError, "UTC"):
            compare_month(snapshot, "2026-01", "North Works", [self.tariff()])
        (snapshot / "manifest.json").write_text("not json")
        with self.assertRaises(SnapshotError):
            compare_month(snapshot, "2026-01", "North Works", [self.tariff()])

    def test_duplicate_tariff_ids_and_empty_list_rejected(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        first = self.tariff(name="one.json", tariff_id="SAME")
        second = self.tariff(name="two.json", tariff_id="SAME")
        with self.assertRaisesRegex(TariffError, "unique"):
            compare_month(snapshot, "2026-01", "North Works", [first, second])
        with self.assertRaises(TariffError):
            compare_month(snapshot, "2026-01", "North Works", [])

    def test_old_report_survives_bad_input_and_write_failure(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        output = self.root / "report.json"
        good = compare_month(snapshot, "2026-01", "North Works", [self.tariff()])
        write_report(good, output)
        before = output.read_bytes()
        with self.assertRaises(TariffError):
            write_report(compare_month(snapshot, "2026-01", "North Works",
                                       [self.tariff(segments=[
                                           {"id": "flat", "start_hour": 0,
                                            "end_hour": 23,
                                            "price_per_kwh": "1"}])]), output)
        with self.assertRaises(MonthError):
            write_report(compare_month(snapshot, "2026-13", "North Works",
                                       [self.tariff()]), output)
        self.assertEqual(output.read_bytes(), before)

    def test_write_failure_keeps_old_report_and_leaves_no_temp(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        output = self.root / "report.json"
        good = compare_month(snapshot, "2026-01", "North Works", [self.tariff()])
        write_report(good, output)
        before = output.read_bytes()
        # A directory squatting on the report path makes publication fail.
        blocked = self.root / "blocked"
        blocked.mkdir()
        with self.assertRaises(ReportError):
            write_report(good, blocked)
        self.assertTrue(blocked.is_dir())
        with self.assertRaises(ReportError):
            write_report({"bad": object()}, output)
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()
                                if p.name.startswith(".report.json.")), [])

    def test_rerun_produces_byte_identical_report(self):
        snapshot = self.two_meter_snapshot({hour: 100 for hour in range(self.HOURS)})
        tariff = self.tariff()
        first = self.root / "one.json"
        second = self.root / "two.json"
        write_report(compare_month(snapshot, "2026-01", "North Works", [tariff]), first)
        write_report(compare_month(snapshot, "2026-01", "North Works", [tariff]), second)
        self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_snapshot_files_are_not_modified(self):
        snapshot = self.two_meter_snapshot({hour: 100 for hour in range(self.HOURS)})
        before = (snapshot / "warehouse.duckdb").read_bytes()
        manifest_before = (snapshot / "manifest.json").read_bytes()
        compare_month(snapshot, "2026-01", "North Works", [self.tariff()])
        self.assertEqual((snapshot / "warehouse.duckdb").read_bytes(), before)
        self.assertEqual((snapshot / "manifest.json").read_bytes(), manifest_before)


class RecomputeAndRoundingTests(ComparisonFixture):
    def test_report_is_self_contained_for_recomputation(self):
        rows_a = {hour: 200 + hour for hour in range(self.HOURS)}
        rows_b = {hour: 50 for hour in range(self.HOURS)}
        snapshot = self.two_meter_snapshot(rows_a, rows_b)
        tariff = self.tariff(
            segments=[
                {"id": "night", "start_hour": 0, "end_hour": 7, "price_per_kwh": "0.30"},
                {"id": "day", "start_hour": 7, "end_hour": 19, "price_per_kwh": "0.80"},
                {"id": "evening", "start_hour": 19, "end_hour": 24, "price_per_kwh": "1.10"},
            ],
            tiers=[
                {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.02"},
                {"id": "t2", "threshold_kwh": "100", "price_per_kwh": "0.05"},
                {"id": "t3", "threshold_kwh": "150", "price_per_kwh": "0.09"},
            ],
            demand_price_per_kw="0.75", fixed_fee="9.99")
        report = json.loads(render_report(
            compare_month(snapshot, "2026-01", "North Works", [tariff])))
        # Everything needed to recompute the bill is inside the report.
        shutil.rmtree(snapshot)
        plan = report["tariffs"][0]
        price_at = {}
        for segment in plan["segments"]:
            for hour in range(segment["start_hour"], segment["end_hour"]):
                price_at[hour] = Decimal(segment["price_per_kwh"])
        energy = Decimal(0)
        per_meter = {}
        for row in report["hourly_basis"]:
            kwh = Decimal(row["kwh"])
            energy += kwh * price_at[int(row["interval_start"][11:13])]
            per_meter[row["meter_id"]] = per_meter.get(row["meter_id"], Decimal(0)) + kwh
        total_kwh = sum(per_meter.values(), Decimal(0))
        self.assertEqual(fixed_text(total_kwh, 6), report["site_month"]["total_kwh"])
        surcharge = Decimal(0)
        for index, tier in enumerate(plan["tiers"]):
            lower = Decimal(tier["threshold_kwh"])
            if index + 1 < len(plan["tiers"]):
                upper = Decimal(plan["tiers"][index + 1]["threshold_kwh"])
                quantity = max(Decimal(0), min(total_kwh, upper) - lower)
            else:
                quantity = max(Decimal(0), total_kwh - lower)
            surcharge += quantity * Decimal(tier["price_per_kwh"])
        demand = Decimal(report["peak"]["hourly_site_kwh"]) * Decimal(
            plan["demand_price_per_kw"])
        recomputed = energy + surcharge + demand + Decimal(plan["fixed_fee"])
        block = report["comparison"][0]
        self.assertEqual(decimal_text(energy), block["components"]["energy_yuan"])
        self.assertEqual(decimal_text(surcharge), block["components"]["tier_surcharge_yuan"])
        self.assertEqual(decimal_text(demand), block["components"]["demand_charge_yuan"])
        self.assertEqual(decimal_text(recomputed), block["components"]["total_yuan"])
        self.assertEqual(format(recomputed.quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP), "f"),
            block["components"]["total_yuan_rounded"])
        self.assertEqual(len(block["meters"]), 2)
        self.assertEqual({m["meter_id"] for m in block["meters"]}, set(report["meters"]))

    def test_half_up_only_at_the_final_cent(self):
        snapshot = self.two_meter_snapshot({hour: 0 for hour in range(self.HOURS)})
        tariff = self.tariff(fixed_fee="0.125")
        block = compare_month(snapshot, "2026-01", "North Works", [tariff])["comparison"][0]
        self.assertEqual(block["components"]["total_yuan"], "0.125")
        self.assertEqual(block["components"]["total_yuan_rounded"], "0.13")
        tariff = self.tariff(name="down.json", fixed_fee="0.124")
        block = compare_month(snapshot, "2026-01", "North Works", [tariff])["comparison"][0]
        self.assertEqual(block["components"]["total_yuan_rounded"], "0.12")


class CliTests(ComparisonFixture):
    def env(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT) + (
            ":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        return env

    def run_module(self, *args):
        return subprocess.run(
            [sys.executable, "-m", *args], cwd=PROJECT, env=self.env(),
            text=True, capture_output=True, check=False)

    def month_files(self, name, days=31):
        hours = days * 24
        rows = ["meter_id,observed_at,register_wh"]
        for meter, base in [("M-A", 250), ("M-B", 40)]:
            register = 0
            rows.append(f"{meter},{stamp(utc(2026, 1, 1, 0))},{register}")
            for hour in range(hours):
                register += base + (hour % 24) * 3
                rows.append(f"{meter},{stamp(utc(2026, 1, 1, 0) + timedelta(hours=hour + 1))},{register}")
        meters = self.root / f"{name}-meters.csv"
        readings = self.root / f"{name}-readings.csv"
        meters.write_text("meter_id,site,multiplier,max_kw\n"
                          "M-A,North Works,1,1000000\nM-B,North Works,1,1000000\n")
        readings.write_text("\n".join(rows) + "\n")
        return meters, readings

    def test_real_cli_over_full_and_revised_snapshots(self):
        meters, readings = self.month_files("cli")
        base = self.root / "cli-base"
        built = self.run_module("meter_lake", "--meters", str(meters),
                                "--readings", str(readings), "--output", str(base))
        self.assertEqual(built.returncode, 0, built.stderr)
        peak_aware = self.tariff_file(DATA / "tariffs" / "01-peak-aware.json")
        flat_saver = self.tariff_file(DATA / "tariffs" / "02-flat-saver.json")
        out_full = self.root / "full.json"
        run = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                              "--month", "2026-01", "--site", "North Works",
                              "--tariff", str(peak_aware),
                              "--tariff", str(flat_saver),
                              "--output", str(out_full))
        self.assertEqual(run.returncode, 0, run.stderr)
        summary = json.loads(run.stdout)
        self.assertTrue(summary["comparable"])
        self.assertEqual([r["rank"] for r in summary["ranked"]], [1, 2])
        full = json.loads(out_full.read_text())

        patch = self.root / "revise.csv"
        # Revise every M-A boundary upward by 50 Wh per elapsed hour, shifting
        # the whole month's energy upward through the real incremental engine.
        lines = ["op,meter_id,observed_at,register_wh,expected_wh"]
        register = 0
        for hour in range(31 * 24 + 1):
            instant = stamp(utc(2026, 1, 1, 0) + timedelta(hours=hour))
            if hour:
                register += 250 + ((hour - 1) % 24) * 3
            lines.append(f"replace,M-A,{instant},{register + 50 * hour},{register}")
        patch.write_text("\n".join(lines) + "\n")
        revised = self.root / "cli-v2"
        applied = self.run_module("meter_lake.incremental", "--base", str(base),
                                  "--changes", str(patch), "--output", str(revised))
        self.assertEqual(applied.returncode, 0, applied.stderr)
        out_v2 = self.root / "v2.json"
        run = self.run_module("meter_lake.tariff", "--snapshot", str(revised),
                              "--month", "2026-01", "--site", "North Works",
                              "--tariff", str(peak_aware),
                              "--tariff", str(flat_saver),
                              "--output", str(out_v2))
        self.assertEqual(run.returncode, 0, run.stderr)
        revised_report = json.loads(out_v2.read_text())
        self.assertEqual(revised_report["source"]["snapshot"]["schema_version"], 2)
        self.assertGreater(Decimal(revised_report["site_month"]["total_kwh"]),
                           Decimal(full["site_month"]["total_kwh"]))
        for before, after in zip(full["comparison"], revised_report["comparison"]):
            self.assertGreater(Decimal(after["components"]["total_yuan"]),
                               Decimal(before["components"]["total_yuan"]))
        # Repeating the same comparison is byte identical.
        again = self.root / "v2-again.json"
        run = self.run_module("meter_lake.tariff", "--snapshot", str(revised),
                              "--month", "2026-01", "--site", "North Works",
                              "--tariff", str(peak_aware),
                              "--tariff", str(flat_saver),
                              "--output", str(again))
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(again.read_bytes(), out_v2.read_bytes())

    def tariff_file(self, source):
        target = self.root / Path(source).name
        target.write_bytes(Path(source).read_bytes())
        return target

    def test_cli_failure_keeps_previous_report_and_inputs(self):
        meters, readings = self.month_files("keep", days=31)
        base = self.root / "keep-base"
        built = self.run_module("meter_lake", "--meters", str(meters),
                                "--readings", str(readings), "--output", str(base))
        self.assertEqual(built.returncode, 0, built.stderr)
        good_tariff = self.tariff_file(DATA / "tariffs" / "01-peak-aware.json")
        output = self.root / "keep.json"
        run = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                              "--month", "2026-01", "--site", "North Works",
                              "--tariff", str(good_tariff), "--output", str(output))
        self.assertEqual(run.returncode, 0, run.stderr)
        before = output.read_bytes()
        snapshot_bytes = (base / "warehouse.duckdb").read_bytes()
        bad = self.tariff(segments=[
            {"id": "gap", "start_hour": 0, "end_hour": 23, "price_per_kwh": "1"}])
        bad_attempt = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                      "--month", "2026-01", "--site", "North Works",
                                      "--tariff", str(bad), "--output", str(output))
        self.assertNotEqual(bad_attempt.returncode, 0)
        self.assertIn("segments", bad_attempt.stderr)
        month_attempt = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                        "--month", "2026-13", "--site", "North Works",
                                        "--tariff", str(good_tariff),
                                        "--output", str(output))
        self.assertNotEqual(month_attempt.returncode, 0)
        site_attempt = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                       "--month", "2026-01", "--site", "Nowhere",
                                       "--tariff", str(good_tariff),
                                       "--output", str(output))
        self.assertNotEqual(site_attempt.returncode, 0)
        self.assertIn("unknown site", site_attempt.stderr)
        injected = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                   "--month", "2026-01", "--site", "North Works",
                                   "--tariff", str(good_tariff), "--output", str(output),
                                   "--fail-after", "report_ready")
        self.assertNotEqual(injected.returncode, 0)
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual((base / "warehouse.duckdb").read_bytes(), snapshot_bytes)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()
                                if p.name.startswith(".keep.json.")), [])
        # Refuse to write into the read-only snapshot or over an input.
        inside = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                 "--month", "2026-01", "--site", "North Works",
                                 "--tariff", str(good_tariff),
                                 "--output", str(base / "report.json"))
        self.assertNotEqual(inside.returncode, 0)
        self.assertIn("read-only snapshot", inside.stderr)
        self.assertFalse((base / "report.json").exists())
        clobber = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                  "--month", "2026-01", "--site", "North Works",
                                  "--tariff", str(good_tariff),
                                  "--output", str(good_tariff))
        self.assertNotEqual(clobber.returncode, 0)
        self.assertEqual(good_tariff.read_bytes(),
                         (DATA / "tariffs" / "01-peak-aware.json").read_bytes())
        # Partial month is a successful run that withholds ranking.
        partial = self.run_module("meter_lake.tariff", "--snapshot", str(base),
                                  "--month", "2026-02", "--site", "North Works",
                                  "--tariff", str(good_tariff),
                                  "--output", str(self.root / "feb.json"))
        self.assertEqual(partial.returncode, 0, partial.stderr)
        feb = json.loads(partial.stdout)
        self.assertFalse(feb["comparable"])
        self.assertEqual(feb["ranked"], [])


if __name__ == "__main__":
    unittest.main()
