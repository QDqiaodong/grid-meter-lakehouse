"""Regression tests for Decimal-context isolation in the tariff comparison.

The monthly comparison must produce byte-identical, equally legal reports no
matter what the caller's :class:`decimal.Context` looks like: low precision,
alternative rounding modes, pre-set flags or enabled traps.  On both success
and failure the caller's context must be restored untouched, and a failed run
must never overwrite an already published report.

The full-month, multi-meter fixtures here are accompanied by an independent
reference implementation of the documented bill formula (see
:func:`expected_totals`) written straight from the fixture's block data; the
assertions compare the product under test against that independent
calculation rather than against another code path of the implementation.
"""
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import (
    Decimal,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    DivisionByZero,
    Inexact,
    InvalidOperation,
    Overflow,
    Rounded,
    Underflow,
    localcontext,
    getcontext,
)
import tempfile
import unittest
from pathlib import Path

from meter_lake.pipeline import build
from meter_lake.tariff import (
    MonthError,
    TariffError,
    compare_month,
    load_tariff,
    render_report,
    write_report,
)
from meter_lake.tariff.compare import _to_fen
from meter_lake.tariff.model import NUMERIC_PRECISION, fixed_text, numeric_context

HOURS = 31 * 24
MONTH = "2026-01"
SITE = "North Works"
ALL_TRAPS = (Inexact, Rounded, InvalidOperation, Overflow, Underflow,
             DivisionByZero)


def utc(year, month, day, hour):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def stamp(instant):
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def context_snapshot():
    ctx = getcontext()
    return {
        "prec": ctx.prec,
        "rounding": ctx.rounding,
        "traps": dict(ctx.traps),
        "flags": dict(ctx.flags),
        "Emax": ctx.Emax,
        "Emin": ctx.Emin,
        "clamp": ctx.clamp,
    }


@contextmanager
def hostile_context(prec, rounding, traps=(), preset_flags=()):
    """Install a deliberately hostile global Decimal context for a block."""
    with localcontext() as ctx:
        ctx.prec = prec
        ctx.rounding = rounding
        ctx.Emax = 425
        ctx.Emin = -425
        ctx.clamp = 1
        for signal in ctx.traps:
            ctx.traps[signal] = 0
        for signal in traps:
            ctx.traps[signal] = 1
        ctx.clear_flags()
        for signal in preset_flags:
            ctx.flags[signal] = 1
        before = context_snapshot()
        yield ctx
        after = context_snapshot()
        assert after == before, "caller Decimal context was mutated by the call"


# --------------------------------------------------------------------------- #
# Full-month, multi-meter fixture
# --------------------------------------------------------------------------- #

# Three meters whose January 2026 is described as constant-delta blocks:
# (start_hour, end_hour_exclusive, delta_wh_per_hour).  M1 carries large
# magnitudes and crosses tier boundaries, M2 carries fractional (sub-Wh-scale
# kWh) magnitudes, M3 stays at zero.  Hours 20..24 tie for the site peak at
# 1.001 + 0.001 = 1.002 kWh.
BLOCKS = {
    "M1": [(0, 20, 0), (20, 25, 1001), (25, 48, 500), (48, 720, 100), (720, 744, 0)],
    "M2": [(0, 20, 0), (20, 25, 1), (25, 48, 2), (48, 720, 0), (720, 744, 3)],
    "M3": [(0, 744, 0)],
}
METER_IDS = ("M1", "M2", "M3")


def write_hourly_snapshot(root, blocks, meters=METER_IDS, hours=HOURS):
    """Build a v1 snapshot from the block description."""
    meters_csv = root / "meters.csv"
    readings_csv = root / "readings.csv"
    meters_csv.write_text(
        "meter_id,site,multiplier,max_kw\n"
        + "".join(f"{meter},{SITE},1,1000000\n" for meter in meters)
    )
    lines = ["meter_id,observed_at,register_wh"]
    first = utc(2026, 1, 1, 0)
    for meter in meters:
        register = 0
        lines.append(f"{meter},{stamp(first)},{register}")
        per_hour = {}
        for start, end, delta in blocks[meter]:
            for hour in range(start, end):
                per_hour[hour] = delta
        for hour in range(hours):
            register += per_hour[hour]
            lines.append(
                f"{meter},{stamp(first + timedelta(hours=hour + 1))},{register}")
    readings_csv.write_text("\n".join(lines) + "\n")
    target = root / "lake"
    build(meters_csv, readings_csv, target)
    return target


def write_plan(path, tariff_id, segments, tiers, demand, fixed):
    document = {
        "tariff_id": tariff_id,
        "segments": segments,
        "tiers": tiers,
        "demand_price_per_kw": demand,
        "fixed_fee": fixed,
    }
    path.write_text(json.dumps(document, indent=2) + "\n")
    return path


def plan_a(root):
    return write_plan(
        root / "a.json", "T-A",
        segments=[
            {"id": "night", "start_hour": 0, "end_hour": 8, "price_per_kwh": "0.30"},
            {"id": "day", "start_hour": 8, "end_hour": 19, "price_per_kwh": "0.80"},
            {"id": "evening", "start_hour": 19, "end_hour": 24, "price_per_kwh": "1.10"},
        ],
        tiers=[
            {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.10"},
            {"id": "t2", "threshold_kwh": "50", "price_per_kwh": "0.30"},
            {"id": "t3", "threshold_kwh": "80", "price_per_kwh": "0.70"},
        ],
        demand="2.50", fixed="11.00")


def plan_b(root, fixed="11.001"):
    return write_plan(
        root / "b.json", "T-B",
        segments=[
            {"id": "night", "start_hour": 0, "end_hour": 8, "price_per_kwh": "0.20"},
            {"id": "day", "start_hour": 8, "end_hour": 19, "price_per_kwh": "0.60"},
            {"id": "evening", "start_hour": 19, "end_hour": 24, "price_per_kwh": "0.90"},
        ],
        tiers=[
            {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.05"},
            {"id": "t2", "threshold_kwh": "50", "price_per_kwh": "0.20"},
            {"id": "t3", "threshold_kwh": "80", "price_per_kwh": "0.50"},
        ],
        demand="2.50", fixed=fixed)


def plan_c(root):
    # One millifen more than plan B: distinct unrounded totals, identical fen.
    return write_plan(
            root / "c.json", "T-C",
            segments=[
                {"id": "night", "start_hour": 0, "end_hour": 8, "price_per_kwh": "0.20"},
                {"id": "day", "start_hour": 8, "end_hour": 19, "price_per_kwh": "0.60"},
                {"id": "evening", "start_hour": 19, "end_hour": 24, "price_per_kwh": "0.90"},
            ],
            tiers=[
                {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.05"},
                {"id": "t2", "threshold_kwh": "50", "price_per_kwh": "0.20"},
                {"id": "t3", "threshold_kwh": "80", "price_per_kwh": "0.50"},
            ],
            demand="2.50", fixed="11.002")


def expected_totals(blocks, hours, plan):
    """Independent reference calculation straight from the fixture data.

    Reimplements the documented formula with explicit Decimal arithmetic; it
    deliberately shares no helper with the package under test.
    """
    segment_price = [None] * 24
    for segment in plan["segments"]:
        for hour in range(segment["start_hour"], segment["end_hour"]):
            segment_price[hour] = Decimal(segment["price_per_kwh"])
    tiers = [(Decimal(t["threshold_kwh"]), Decimal(t["price_per_kwh"]))
             for t in plan["tiers"]]
    demand_price = Decimal(plan["demand_price_per_kw"])
    fixed_fee = Decimal(plan["fixed_fee"])

    per_hour = {meter: {} for meter in blocks}
    meter_kwh = {}
    energy = Decimal(0)
    site_hourly = {}
    for meter, meter_blocks in blocks.items():
        total = Decimal(0)
        for start, end, delta_wh in meter_blocks:
            kwh_each = Decimal(delta_wh) / Decimal(1000)
            for hour in range(start, end):
                per_hour[meter][hour] = kwh_each
                site_hourly.setdefault(hour, Decimal(0))
                total += kwh_each
                energy += kwh_each * segment_price[hour % 24]
        meter_kwh[meter] = total
    for hour in range(hours):
        site_hourly.setdefault(hour, Decimal(0))
        for meter in blocks:
            site_hourly[hour] += per_hour[meter].get(hour, Decimal(0))
    total_kwh = sum(site_hourly.values(), Decimal(0))

    surcharge = Decimal(0)
    allocation = []
    for index, (threshold, price) in enumerate(tiers):
        if index + 1 < len(tiers):
            upper = tiers[index + 1][0]
            quantity = max(Decimal(0), min(total_kwh, upper) - threshold)
        else:
            quantity = max(Decimal(0), total_kwh - threshold)
        amount = quantity * price
        surcharge += amount
        allocation.append((quantity, amount))

    peak = max(site_hourly.values())
    peak_hours = [hour for hour in range(hours) if site_hourly[hour] == peak]
    demand = peak * demand_price
    total = energy + surcharge + demand + fixed_fee
    rounded = total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return {
        "total_kwh": total_kwh,
        "meter_kwh": meter_kwh,
        "peak": peak,
        "peak_hours": peak_hours,
        "site_hourly": site_hourly,
        "energy": energy,
        "surcharge": surcharge,
        "allocation": allocation,
        "demand": demand,
        "fixed": fixed_fee,
        "total": total,
        "rounded": rounded,
        "fen": _to_fen(rounded),
    }


class ContextIsolationFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.snapshot = write_hourly_snapshot(self.root, BLOCKS)
        self.plans = [plan_a(self.root), plan_b(self.root), plan_c(self.root)]

    def report(self):
        return compare_month(self.snapshot, MONTH, SITE, self.plans)

    def hostile_contexts(self):
        return [
            ("prec2-half-even-trapped", 2, ROUND_HALF_EVEN,
             (Inexact, Rounded), ()),
            ("prec1-round-down-all-trapped", 1, ROUND_DOWN,
             ALL_TRAPS, ()),
            ("prec6-ceil-inexact", 6, ROUND_CEILING, (Inexact,), ()),
            ("prec42-floor-rounded", 42, ROUND_FLOOR, (Rounded,), ()),
            ("prec2-preset-flags", 2, ROUND_HALF_EVEN, (),
             (Inexact, Rounded, DivisionByZero)),
            ("prec3-invalid-trap", 3, ROUND_DOWN, (InvalidOperation,), ()),
        ]


class ReportInvarianceTests(ContextIsolationFixture):
    def test_golden_report_is_unchanged_under_every_hostile_context(self):
        golden = render_report(self.report())
        for name, prec, rounding, traps, flags in self.hostile_contexts():
            with self.subTest(context=name):
                with hostile_context(prec, rounding, traps, flags):
                    payload = render_report(self.report())
                self.assertEqual(payload, golden)

    def test_load_tariff_is_identical_under_hostile_contexts(self):
        golden = [load_tariff(path).document() for path in self.plans]
        golden_sha = [load_tariff(path).input_sha256 for path in self.plans]
        for name, prec, rounding, traps, flags in self.hostile_contexts():
            with self.subTest(context=name):
                with hostile_context(prec, rounding, traps, flags):
                    docs = [load_tariff(path).document() for path in self.plans]
                    shas = [load_tariff(path).input_sha256 for path in self.plans]
                self.assertEqual(docs, golden)
                self.assertEqual(shas, golden_sha)

    def test_fixed_text_is_context_independent_but_still_rejects_overprecision(self):
        value = Decimal("82.248")
        for name, prec, rounding, traps, flags in self.hostile_contexts():
            with self.subTest(context=name):
                with hostile_context(prec, rounding, traps, flags):
                    self.assertEqual(fixed_text(value, 6), "82.248000")
                    self.assertEqual(fixed_text(Decimal("0"), 6), "0.000000")
                    with self.assertRaises(ValueError):
                        fixed_text(Decimal("0.1234567"), 6)

    def test_to_fen_matches_cent_quantized_values(self):
        for text, fen in [("0.00", 0), ("0.01", 1), ("86.70", 8670),
                          ("50.34", 5034), ("1234567.89", 123456789),
                          ("-1.23", -123)]:
            self.assertEqual(_to_fen(Decimal(text)), fen)
        with self.assertRaises(ValueError):
            _to_fen(Decimal("1.001"))

    def test_numeric_context_uses_documented_precision_and_no_traps(self):
        with numeric_context() as ctx:
            self.assertEqual(ctx.prec, NUMERIC_PRECISION)
            self.assertEqual(ctx.rounding, ROUND_HALF_UP)
            self.assertTrue(not any(ctx.traps.values()))


class IndependentFullMonthRegressionTests(ContextIsolationFixture):
    """Full-month multi-table synthesis checked against an independent calc."""

    def test_mixed_magnitude_tier_boundaries_and_tied_peak(self):
        plan_docs = [json.loads(path.read_text()) for path in self.plans]
        expected = {
            doc["tariff_id"]: expected_totals(BLOCKS, HOURS, doc)
            for doc in plan_docs
        }
        report = self.report()

        # Site-level synthesis, independently recomputed.
        self.assertEqual(len(report["meters"]), 3)
        self.assertEqual(report["hours_per_meter"], HOURS)
        self.assertTrue(report["comparable"])
        self.assertEqual(report["gaps"], [])
        site_total = sum(expected_totals(BLOCKS, HOURS, plan_docs[0])
                         ["meter_kwh"].values(), Decimal(0))
        self.assertEqual(report["site_month"]["total_kwh"],
                         fixed_text(site_total, 6))
        self.assertEqual(report["site_month"]["total_kwh"], "83.828000")

        # Large and fractional magnitudes mixed in the same site hour.
        basis = {(row["meter_id"], row["interval_start"]): row
                 for row in report["hourly_basis"]}
        hour20 = stamp(utc(2026, 1, 1, 20))
        self.assertEqual(basis[("M1", hour20)]["kwh"], "1.001000")
        self.assertEqual(basis[("M2", hour20)]["kwh"], "0.001000")
        self.assertEqual(basis[("M1", hour20)]["site_hour_kwh"], "1.002000")
        self.assertEqual(basis[("M3", hour20)]["site_hour_kwh"], "1.002000")

        # Coincident tied peak: hours 20..24, all 1.002 kWh.
        self.assertEqual(report["peak"]["hourly_site_kwh"], "1.002000")
        peak_hours = [row["interval_start"] for row in report["peak"]["hours"]]
        first = utc(2026, 1, 1, 0)
        self.assertEqual(peak_hours,
                         [stamp(first + timedelta(hours=hour))
                          for hour in range(20, 25)])

        by_id = {block["tariff_id"]: block for block in report["comparison"]}
        for tariff_id, want in expected.items():
            with self.subTest(tariff=tariff_id):
                block = by_id[tariff_id]
                components = block["components"]
                self.assertEqual(Decimal(components["energy_yuan"]),
                                 want["energy"])
                self.assertEqual(Decimal(components["tier_surcharge_yuan"]),
                                 want["surcharge"])
                self.assertEqual(Decimal(components["demand_charge_yuan"]),
                                 want["demand"])
                self.assertEqual(Decimal(components["fixed_fee_yuan"]),
                                 want["fixed"])
                self.assertEqual(Decimal(components["total_yuan"]),
                                 want["total"])
                self.assertEqual(components["total_yuan_rounded"],
                                 format(want["rounded"], "f"))
                self.assertEqual(components["total_fen"], want["fen"])
                got_alloc = [
                    (Decimal(row["kwh"]), Decimal(row["surcharge_yuan"]))
                    for row in block["tier_allocation"]]
                self.assertEqual(got_alloc, want["allocation"])

        # Tier boundaries for the 83.828 kWh site total: 50 / 30 / 3.828.
        allocation = by_id["T-A"]["tier_allocation"]
        self.assertEqual([row["kwh"] for row in allocation],
                         ["50.000000", "30.000000", "3.828000"])

    def test_exact_tier_threshold_pays_nothing_in_the_next_tier(self):
        # Rebuild a fixture whose site total lands exactly on 50 kWh.
        exact_blocks = {
            "M1": [(0, 50, 1000), (50, HOURS, 0)],
            "M2": [(0, HOURS, 0)],
            "M3": [(0, HOURS, 0)],
        }
        root = Path(self.temp.name) / "exact"
        root.mkdir()
        snapshot = write_hourly_snapshot(root, exact_blocks)
        plan = write_plan(
            root / "exact.json", "T-EXACT",
            segments=[{"id": "flat", "start_hour": 0, "end_hour": 24,
                       "price_per_kwh": "0"}],
            tiers=[
                {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.10"},
                {"id": "t2", "threshold_kwh": "50", "price_per_kwh": "0.30"},
                {"id": "t3", "threshold_kwh": "80", "price_per_kwh": "0.70"},
            ],
            demand="0", fixed="0")
        for name, prec, rounding, traps, flags in self.hostile_contexts()[:3]:
            with self.subTest(context=name):
                with hostile_context(prec, rounding, traps, flags):
                    report = compare_month(snapshot, MONTH, SITE, [plan])
                allocation = report["comparison"][0]["tier_allocation"]
                self.assertEqual([row["kwh"] for row in allocation],
                                 ["50.000000", "0.000000", "0.000000"])
                self.assertEqual(
                    report["comparison"][0]["components"]["tier_surcharge_yuan"],
                    "5")

    def test_subfen_difference_decides_ranking_but_rounded_fen_can_tie(self):
        # T-B (11.001 fixed) beats T-C (11.002) by exactly 0.001 yuan, yet both
        # round to the same fen; ranking must follow the unrounded total.
        report = self.report()
        ordered = [(block["rank"], block["tariff_id"],
                    block["components"]["total_yuan"],
                    block["components"]["total_yuan_rounded"],
                    block["components"]["total_fen"])
                   for block in report["comparison"]]
        ranks = {row[1]: row[0] for row in ordered}
        self.assertEqual(ranks, {"T-B": 1, "T-C": 2, "T-A": 3})
        b = next(row for row in ordered if row[1] == "T-B")
        c = next(row for row in ordered if row[1] == "T-C")
        self.assertEqual(Decimal(c[2]) - Decimal(b[2]), Decimal("0.001"))
        self.assertEqual(b[3], c[3])
        self.assertEqual(b[4], c[4])

        for name, prec, rounding, traps, flags in self.hostile_contexts():
            with self.subTest(context=name):
                with hostile_context(prec, rounding, traps, flags):
                    hostile = self.report()
                self.assertEqual(
                    [(block["rank"], block["tariff_id"])
                     for block in hostile["comparison"]],
                    [(block["rank"], block["tariff_id"])
                     for block in report["comparison"]])

    def test_ranking_keeps_competition_ties_on_equal_unrounded_totals(self):
        root = Path(self.temp.name) / "ties"
        root.mkdir()
        snapshot = write_hourly_snapshot(root, BLOCKS)
        first = write_plan(
            root / "x.json", "T-X",
            segments=[{"id": "flat", "start_hour": 0, "end_hour": 24,
                       "price_per_kwh": "1"}],
            tiers=[{"id": "t0", "threshold_kwh": "0", "price_per_kwh": "0"}],
            demand="0", fixed="0.004")
        second = write_plan(
            root / "y.json", "T-Y",
            segments=[{"id": "flat", "start_hour": 0, "end_hour": 24,
                       "price_per_kwh": "1"}],
            tiers=[{"id": "t0", "threshold_kwh": "0", "price_per_kwh": "0"}],
            demand="0", fixed="0.004")
        with hostile_context(2, ROUND_DOWN, ALL_TRAPS):
            report = compare_month(snapshot, MONTH, SITE, [second, first])
        self.assertEqual(
            [(block["tariff_id"], block["rank"])
             for block in report["comparison"]],
            [("T-X", 1), ("T-Y", 1)])


class ContextPreservationTests(ContextIsolationFixture):
    def test_success_restores_precision_rounding_flags_traps_and_limits(self):
        before = context_snapshot()
        for name, prec, rounding, traps, flags in self.hostile_contexts():
            with self.subTest(context=name):
                with hostile_context(prec, rounding, traps, flags):
                    self.report()
                    load_tariff(self.plans[0])
        self.assertEqual(context_snapshot(), before)

    def test_failure_restores_context_and_rejects_illegal_plans(self):
        bad_cases = {
            "uncovered hour": {
                "segments": [{"id": "flat", "start_hour": 0, "end_hour": 23,
                              "price_per_kwh": "1"}]},
            "overlapping hours": {
                "segments": [
                    {"id": "a", "start_hour": 0, "end_hour": 12, "price_per_kwh": "1"},
                    {"id": "b", "start_hour": 11, "end_hour": 24, "price_per_kwh": "1"}]},
            "bad tier order": {
                "tiers": [
                    {"id": "a", "threshold_kwh": "0", "price_per_kwh": "1"},
                    {"id": "b", "threshold_kwh": "0", "price_per_kwh": "1"}]},
            "unknown field": {"extra": 1},
            "float price": {
                "segments": [{"id": "flat", "start_hour": 0, "end_hour": 24,
                              "price_per_kwh": 0.5}]},
        }
        for label, overrides in bad_cases.items():
            with self.subTest(case=label):
                path = self.root / f"bad-{label.split()[0]}.json"
                document = {
                    "tariff_id": "BAD",
                    "segments": [{"id": "flat", "start_hour": 0, "end_hour": 24,
                                  "price_per_kwh": "1"}],
                    "tiers": [{"id": "t0", "threshold_kwh": "0",
                               "price_per_kwh": "0"}],
                    "demand_price_per_kw": "0",
                    "fixed_fee": "0",
                }
                document.update(overrides)
                path.write_text(json.dumps(document))
                with hostile_context(2, ROUND_DOWN, ALL_TRAPS,
                                     (Inexact, Rounded)):
                    with self.assertRaises(TariffError):
                        load_tariff(path)
                    with self.assertRaises(TariffError):
                        compare_month(self.snapshot, MONTH, SITE, [path])

    def test_duplicate_keys_and_nonfinite_constants_rejected_under_traps(self):
        dupe = self.root / "dupe.json"
        dupe.write_text(
            '{"tariff_id":"A","tariff_id":"B","segments":['
            '{"id":"f","start_hour":0,"end_hour":24,"price_per_kwh":"1"}],'
            '"tiers":[{"id":"t","threshold_kwh":"0","price_per_kwh":"0"}],'
            '"demand_price_per_kw":"0","fixed_fee":"0"}')
        nan = self.root / "nan.json"
        nan.write_text(
            '{"tariff_id":"N","segments":['
            '{"id":"f","start_hour":0,"end_hour":24,"price_per_kwh":"1"}],'
            '"tiers":[{"id":"t","threshold_kwh":"0","price_per_kwh":"0"}],'
            '"demand_price_per_kw":NaN,"fixed_fee":"0"}')
        with hostile_context(1, ROUND_FLOOR, ALL_TRAPS):
            with self.assertRaises(TariffError):
                load_tariff(dupe)
            with self.assertRaises(TariffError):
                load_tariff(nan)

    def test_non_comparable_gap_report_is_context_invariant(self):
        partial_root = self.root / "partial"
        partial_root.mkdir()
        partial = write_hourly_snapshot(partial_root, BLOCKS, hours=48)
        golden = render_report(
            compare_month(partial, MONTH, SITE, [self.plans[0]]))
        with hostile_context(2, ROUND_HALF_EVEN, ALL_TRAPS):
            report = compare_month(partial, MONTH, SITE, [self.plans[0]])
            self.assertFalse(report["comparable"])
            self.assertEqual(report["comparison"], [])
            self.assertIsNone(report["peak"])
            payload = render_report(report)
        self.assertEqual(payload, golden)
        missing = {gap["meter_id"]: gap["missing_hours"] for gap in report["gaps"]}
        self.assertEqual(missing, {meter: HOURS - 48 for meter in METER_IDS})

    def test_failed_runs_never_overwrite_an_existing_report(self):
        output = self.root / "report.json"
        write_report(self.report(), output)
        before = output.read_bytes()
        bad_plan = self.root / "bad.json"
        write_plan(bad_plan, "BAD",
                   segments=[{"id": "flat", "start_hour": 0, "end_hour": 23,
                              "price_per_kwh": "1"}],
                   tiers=[{"id": "t0", "threshold_kwh": "0",
                           "price_per_kwh": "0"}],
                   demand="0", fixed="0")
        with hostile_context(2, ROUND_DOWN, ALL_TRAPS, (Inexact,)):
            with self.assertRaises(TariffError):
                write_report(
                    compare_month(self.snapshot, MONTH, SITE, [bad_plan]),
                    output)
            with self.assertRaises(MonthError):
                compare_month(self.snapshot, "2026-13", SITE, self.plans)
            with self.assertRaises(MonthError):
                compare_month(self.snapshot, MONTH, "Nowhere", self.plans)
        self.assertEqual(output.read_bytes(), before)
        leftovers = [p.name for p in self.root.iterdir()
                     if p.name.startswith(f".{output.name}.")]
        self.assertEqual(leftovers, [])
        # A later successful run still reproduces the same bytes.
        write_report(self.report(), output)
        self.assertEqual(output.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
