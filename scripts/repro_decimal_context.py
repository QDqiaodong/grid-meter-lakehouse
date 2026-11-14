#!/usr/bin/env python3
"""Reproduce the Decimal-context sensitivity of the monthly tariff comparison.

Builds a small but complete January 2026 snapshot (two meters, mixed large
and fractional magnitudes), computes the golden report under a clean decimal
context, then re-runs the same legal inputs under hostile caller contexts:
low precision, enabled Inexact/Rounded traps, a non-default rounding mode and
pre-set sticky flags.  Every scenario must produce byte-identical report
bytes and leave the caller's context (precision, rounding, traps, flags)
untouched.

Usage (from the repository root, with the project venv):

    .venv/bin/python scripts/repro_decimal_context.py

On the pre-fix baseline every hostile scenario FAILs (exception or distorted
bytes); on the fixed tree all scenarios PASS.  To see the baseline failures:

    git stash push -- meter_lake/
    .venv/bin/python scripts/repro_decimal_context.py   # expect FAILs
    git stash pop
"""
import json
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from decimal import (
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    Inexact,
    InvalidOperation,
    Rounded,
    getcontext,
    localcontext,
)
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from meter_lake.pipeline import build  # noqa: E402
from meter_lake.tariff import compare_month, load_tariff, render_report  # noqa: E402

HOURS = 31 * 24


def stamp(instant):
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_fixture(root):
    """Two meters: M-A mixes 1.001 kWh and 0.1 kWh hours, M-B adds 0.001 kWh."""
    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = ["meter_id,observed_at,register_wh"]
    for meter, delta in [("M-A", {h: 1001 if h < 5 else 100 for h in range(HOURS)}),
                         ("M-B", {h: 1 for h in range(HOURS)})]:
        register = 0
        rows.append(f"{meter},{stamp(first)},{register}")
        for hour in range(HOURS):
            register += delta[hour]
            rows.append(f"{meter},{stamp(first + timedelta(hours=hour + 1))},{register}")
    meters = root / "meters.csv"
    readings = root / "readings.csv"
    meters.write_text("meter_id,site,multiplier,max_kw\n"
                      "M-A,North Works,1,1000000\nM-B,North Works,1,1000000\n")
    readings.write_text("\n".join(rows) + "\n")
    snapshot = root / "lake"
    build(meters, readings, snapshot)
    tariff = root / "tariff.json"
    tariff.write_text(json.dumps({
        "tariff_id": "T-DEMO",
        "segments": [
            {"id": "night", "start_hour": 0, "end_hour": 8, "price_per_kwh": "0.30"},
            {"id": "day", "start_hour": 8, "end_hour": 24, "price_per_kwh": "0.85"},
        ],
        "tiers": [
            {"id": "t1", "threshold_kwh": "0", "price_per_kwh": "0.10"},
            {"id": "t2", "threshold_kwh": "50", "price_per_kwh": "0.30"},
        ],
        "demand_price_per_kw": "2.50",
        "fixed_fee": "11.005",
    }, indent=2) + "\n")
    return snapshot, tariff


def context_state():
    ctx = getcontext()
    return (ctx.prec, ctx.rounding, dict(ctx.traps), dict(ctx.flags))


def run_scenario(name, prec, rounding, traps=(), preset_flags=()):
    """Run the comparison under a hostile context; return (ok, detail)."""
    snapshot, tariff, golden = run_scenario.fixture
    with localcontext() as ctx:
        ctx.prec = prec
        ctx.rounding = rounding
        for signal in ctx.traps:
            ctx.traps[signal] = 0
        for signal in traps:
            ctx.traps[signal] = 1
        ctx.clear_flags()
        for signal in preset_flags:
            ctx.flags[signal] = 1
        before = context_state()
        try:
            load_tariff(tariff)
            payload = render_report(compare_month(snapshot, "2026-01",
                                                  "North Works", [tariff]))
        except Exception as exc:  # noqa: BLE001 - report any escape
            detail = f"raised {type(exc).__name__}: {exc}"
            detail += "\n" + "\n".join(traceback.format_exc().splitlines()[-4:])
            return False, detail
        after = context_state()
        if after != before:
            return False, f"caller context mutated: before={before} after={after}"
        if payload != golden:
            return False, "report bytes differ from the clean-context golden report"
        return True, "identical bytes, caller context preserved"


def main():
    root = Path(tempfile.mkdtemp(prefix="decimal-context-repro-"))
    snapshot, tariff = build_fixture(root)
    with localcontext() as clean:
        clean.prec = 28
        clean.rounding = ROUND_HALF_EVEN
        for signal in clean.traps:
            clean.traps[signal] = 0
        clean.clear_flags()
        golden = render_report(compare_month(snapshot, "2026-01",
                                             "North Works", [tariff]))
    run_scenario.fixture = (snapshot, tariff, golden)

    scenarios = [
        ("precision=2", dict(prec=2, rounding=ROUND_HALF_EVEN)),
        ("precision=2, Inexact trap", dict(prec=2, rounding=ROUND_HALF_EVEN,
                                           traps=(Inexact,))),
        ("Rounded trap enabled", dict(prec=28, rounding=ROUND_HALF_EVEN,
                                      traps=(Rounded,))),
        ("rounding=ROUND_DOWN", dict(prec=28, rounding=ROUND_DOWN)),
        ("pre-set Inexact/Rounded flags", dict(prec=28, rounding=ROUND_HALF_EVEN,
                                               preset_flags=(Inexact, Rounded))),
        ("precision=1, all traps", dict(prec=1, rounding=ROUND_DOWN,
                                        traps=(Inexact, Rounded, InvalidOperation))),
    ]
    failures = 0
    for name, kwargs in scenarios:
        ok, detail = run_scenario(name, **kwargs)
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        failures += 0 if ok else 1
    print(f"\n{len(scenarios) - failures}/{len(scenarios)} scenarios passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
