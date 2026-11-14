"""Read-only monthly tariff comparison over v1/v2 meter-lake snapshots.

All money math uses :class:`decimal.Decimal`.  Hourly energy comes from the
snapshot's ``DECIMAL(24,6)`` facts, so every component stays exact; only the
final total is rounded (HALF_UP) to cents.  Snapshot files are opened
read-only and never modified.

Every decimal operation happens inside :func:`meter_lake.tariff.model.numeric_context`,
a private high-precision, trap-free context: the caller's precision, rounding
mode, flags and trap enablements never influence the result, and are restored
unchanged whether the comparison succeeds or fails.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import os
import re
import tempfile

import duckdb

from .model import (
    TariffError,
    decimal_text,
    fixed_text,
    load_tariff,
    numeric_context,
)

REPORT_SCHEMA = "meter-lake-site-month-tariff-v1"
CENT = Decimal("0.01")
MONTH_RE = re.compile(r"^([0-9]{4})-(0[1-9]|1[0-2])$")
STAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SUPPORTED = {(1, "full-batch"), (2, "incremental")}
TABLES = {"meters", "interval_usage"}


def _to_fen(rounded_yuan):
    """Convert a cent-quantized yuan Decimal to an integer fen count.

    The arithmetic is performed on the decimal's sign/digits/exponent tuple
    rather than through an operator, so the result is independent of the
    active context's precision and traps (a two-significant-digit caller
    context would otherwise round ``86.70 * 100`` or raise on the
    multiplication).  The input is guaranteed quantized to cents upstream.
    """
    sign, digits, exponent = rounded_yuan.as_tuple()
    if exponent < -2:
        raise ValueError("total must be quantized to whole fen before conversion")
    integer = 0
    for digit in digits:
        integer = integer * 10 + digit
    integer *= 10 ** (exponent + 2)
    return -integer if sign else integer


class MonthError(ValueError):
    """The requested month or site does not fit the comparison contract."""


class SnapshotError(ValueError):
    """The snapshot directory is missing, unsupported, or not a v1/v2 lake."""


class BadFactError(ValueError):
    """At least one hourly fact violates the accounting contract."""


class ReportError(OSError):
    """The report could not be serialized or published atomically."""


@dataclass(frozen=True)
class Month:
    year: int
    month: int
    start: datetime
    end: datetime

    @property
    def label(self):
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def hours(self):
        return int((self.end - self.start).total_seconds() // 3600)


def parse_month(text):
    if not isinstance(text, str):
        raise MonthError("month must be a string in YYYY-MM form")
    match = MONTH_RE.fullmatch(text)
    if not match:
        raise MonthError(f"month must be UTC month YYYY-MM: {text!r}")
    year, month = int(match.group(1)), int(match.group(2))
    try:
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    except ValueError as exc:
        raise MonthError(f"month {text!r} is outside the supported calendar: {exc}") from exc
    return Month(year, month, start, end)


def stamp(instant):
    return (
        f"{instant.year:04d}-{instant.month:02d}-{instant.day:02d}"
        f"T{instant.hour:02d}:{instant.minute:02d}:{instant.second:02d}Z"
    )


def _sha256_bytes(payload):
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_source(base):
    base = Path(base)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"snapshot directory does not exist: {base}")
    manifest_path = base / "manifest.json"
    warehouse = base / "warehouse.duckdb"
    if not manifest_path.is_file():
        raise SnapshotError("snapshot has no manifest.json; only v1/v2 snapshots are comparable")
    if not warehouse.is_file():
        raise SnapshotError("snapshot is missing warehouse.duckdb")
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotError("snapshot manifest.json is not valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise SnapshotError("snapshot manifest.json must be a JSON object")
    pair = (manifest.get("schema_version"), manifest.get("mode"))
    if pair not in SUPPORTED:
        raise SnapshotError(
            "unsupported snapshot: schema_version={!r}, mode={!r}".format(*pair)
        )
    if manifest.get("timezone") != "UTC":
        raise SnapshotError("snapshot timezone must be UTC")
    return {
        "base": base,
        "warehouse": warehouse,
        "manifest_sha256": _sha256_bytes(manifest_bytes),
        "warehouse_sha256": _sha256_file(warehouse),
        "schema_version": manifest["schema_version"],
        "mode": manifest["mode"],
        "timezone": "UTC",
    }


def _read_facts(source, month, site):
    try:
        db = duckdb.connect(str(source["warehouse"]), read_only=True)
    except duckdb.Error as exc:
        raise SnapshotError(f"cannot open snapshot warehouse read-only: {exc}") from exc
    with db:
        tables = {row[0] for row in db.execute("SHOW TABLES").fetchall()}
        if TABLES - tables:
            raise SnapshotError("snapshot is missing required physical tables")
        meter_rows = db.execute(
            "SELECT meter_id FROM meters WHERE site=? ORDER BY meter_id", [site]
        ).fetchall()
        members = [row[0] for row in meter_rows]
        if not members:
            known = db.execute("SELECT DISTINCT site FROM meters ORDER BY site").fetchall()
            sites = ", ".join(row[0] for row in known) or "(none)"
            raise MonthError(f"unknown site {site!r}; snapshot sites: {sites}")
        rows = db.execute(
            """
            SELECT meter_id, site,
                   strftime(interval_start AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
                   strftime(interval_end AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
                   delta_wh, kwh
            FROM interval_usage
            WHERE interval_start >= CAST(? AS TIMESTAMPTZ)
              AND interval_start <  CAST(? AS TIMESTAMPTZ)
              AND (site = ? OR meter_id IN (SELECT meter_id FROM meters WHERE site = ?))
            ORDER BY meter_id, interval_start
            """,
            [stamp(month.start), stamp(month.end), site, site],
        ).fetchall()
    return members, rows


def _classify(members, rows, site):
    member_set = set(members)
    problems = []
    facts = {}
    for meter, fact_site, start_text, end_text, delta_wh, kwh in rows:
        where = f"{meter}@{start_text}"
        if meter not in member_set:
            problems.append(f"fact {where} belongs to a meter outside site {site!r}")
            continue
        if fact_site != site:
            problems.append(f"fact {where} stores site {fact_site!r}, expected {site!r}")
            continue
        try:
            start = datetime.strptime(start_text, STAMP_FORMAT).replace(tzinfo=timezone.utc)
            end = datetime.strptime(end_text, STAMP_FORMAT).replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            problems.append(f"fact {where} has an invalid timestamp")
            continue
        if start.minute or start.second or start.microsecond:
            problems.append(f"fact {where} does not start on a UTC hour boundary")
            continue
        if end - start != timedelta(hours=1):
            problems.append(f"fact {where} does not cover exactly one hour")
            continue
        if not isinstance(delta_wh, int) or delta_wh < 0:
            problems.append(f"fact {where} has an invalid delta_wh")
            continue
        if not isinstance(kwh, Decimal) or not kwh.is_finite() or kwh < 0:
            problems.append(f"fact {where} has an invalid kwh")
            continue
        if (meter, start) in facts:
            problems.append(f"fact {where} duplicates an hourly key")
            continue
        facts[(meter, start)] = kwh
    if problems:
        raise BadFactError("bad hourly facts; comparison rejected:\n" + "\n".join(problems))
    return facts


def _gap_ranges(missing):
    ranges = []
    for instant in missing:
        if ranges and ranges[-1][1] == instant:
            ranges[-1][1] = instant + timedelta(hours=1)
        else:
            ranges.append([instant, instant + timedelta(hours=1)])
    return [
        {"start": stamp(start), "end": stamp(end), "hours": int((end - start).total_seconds() // 3600)}
        for start, end in ranges
    ]


def _collect(snapshot, month, site):
    source = _snapshot_source(snapshot)
    members, rows = _read_facts(source, month, site)
    facts = _classify(members, rows, site)

    expected = [month.start + timedelta(hours=index) for index in range(month.hours)]
    gaps = []
    for meter in members:
        missing = [instant for instant in expected if (meter, instant) not in facts]
        if missing:
            gaps.append({
                "meter_id": meter,
                "missing_hours": len(missing),
                "ranges": _gap_ranges(missing),
            })
    return source, members, facts, expected, gaps


def _segment_prices(tariff):
    prices = [None] * 24
    for segment in tariff.segments:
        for hour in range(segment.start_hour, segment.end_hour):
            prices[hour] = segment
    if any(segment is None for segment in prices):
        raise TariffError(f"tariff {tariff.tariff_id!r} does not cover every hour")
    return prices


def _tier_allocation(tariff, total_kwh):
    # Runs inside the private numeric context opened by compare_month.
    rows = []
    surcharge = Decimal(0)
    for index, tier in enumerate(tariff.tiers):
        if index + 1 < len(tariff.tiers):
            upper = tariff.tiers[index + 1].threshold
            quantity = max(Decimal(0), min(total_kwh, upper) - tier.threshold)
        else:
            quantity = max(Decimal(0), total_kwh - tier.threshold)
        amount = quantity * tier.price
        surcharge += amount
        rows.append({
            "tier_id": tier.tier_id,
            "threshold_kwh": decimal_text(tier.threshold),
            "price_per_kwh": decimal_text(tier.price),
            "kwh": fixed_text(quantity, 6),
            "surcharge_yuan": decimal_text(amount),
        })
    return surcharge, rows


def _price(tariff, total_kwh, peak_kwh, basis):
    """Return the exact comparison block for one tariff.

    All arithmetic stays in the private numeric context; the only rounding is
    the final cent quantize of the total, performed with an explicit
    ROUND_HALF_UP so the caller's rounding mode cannot change the report.
    """
    segment_of = _segment_prices(tariff)
    energy = Decimal(0)
    meters = []
    for meter in sorted(basis):
        hours = []
        for instant, kwh in basis[meter]:
            segment = segment_of[instant.hour]
            amount = kwh * segment.price
            energy += amount
            hours.append({
                "interval_start": stamp(instant),
                "interval_end": stamp(instant + timedelta(hours=1)),
                "kwh": fixed_text(kwh, 6),
                "segment_id": segment.segment_id,
                "price_per_kwh": decimal_text(segment.price),
                "energy_yuan": decimal_text(amount),
            })
        meters.append({"meter_id": meter, "hours": hours})
    surcharge, allocation = _tier_allocation(tariff, total_kwh)
    demand = peak_kwh * tariff.demand_price
    fixed_fee = tariff.fixed_fee
    total = energy + surcharge + demand + fixed_fee
    rounded = total.quantize(CENT, rounding=ROUND_HALF_UP)
    return {
        "tariff_id": tariff.tariff_id,
        "components": {
            "energy_yuan": decimal_text(energy),
            "tier_surcharge_yuan": decimal_text(surcharge),
            "demand_charge_yuan": decimal_text(demand),
            "fixed_fee_yuan": decimal_text(fixed_fee),
            "total_yuan": decimal_text(total),
            "total_yuan_rounded": format(rounded, "f"),
            "total_fen": _to_fen(rounded),
        },
        "tier_allocation": allocation,
        "meters": meters,
    }


def _rank(blocks):
    ordered = sorted(blocks, key=lambda block: (
        Decimal(block["components"]["total_yuan"]), block["tariff_id"]))
    previous_total = None
    rank = 0
    for position, block in enumerate(ordered, start=1):
        total = Decimal(block["components"]["total_yuan"])
        if total != previous_total:
            rank = position
            previous_total = total
        block["rank"] = rank
    return ordered


def compare_month(snapshot, month, site, tariff_paths):
    """Build a deterministic comparison report for one site and UTC month.

    ``month`` is ``YYYY-MM``; ``site`` must exist in the snapshot dimension;
    ``tariff_paths`` is a nonempty iterable of strict JSON tariff files.

    The whole comparison — tariff loading, fact aggregation, formatting,
    pricing and ranking — runs inside
    :func:`meter_lake.tariff.model.numeric_context`.  The caller's decimal
    context (precision, rounding, flags, traps) therefore cannot change the
    report bytes or turn a legal snapshot into an error, and is restored
    unchanged on both success and failure.
    """
    with numeric_context():
        month = parse_month(month)
        if not isinstance(site, str) or site.strip() == "":
            raise MonthError("site must be a nonempty string")
        paths = [Path(path) for path in tariff_paths]
        if not paths:
            raise TariffError("at least one tariff file is required")
        tariffs = [load_tariff(path) for path in paths]
        identifiers = [tariff.tariff_id for tariff in tariffs]
        if len(identifiers) != len(set(identifiers)):
            raise TariffError("tariff_id values must be unique across the comparison")
        source, members, facts, expected, gaps = _collect(snapshot, month, site)

        tariff_docs = sorted((tariff.document() for tariff in tariffs),
                             key=lambda doc: doc["tariff_id"])
        source_summary = {
            "snapshot": {
                "manifest_sha256": source["manifest_sha256"],
                "warehouse_sha256": source["warehouse_sha256"],
                "schema_version": source["schema_version"],
                "mode": source["mode"],
                "timezone": source["timezone"],
            },
            "meters": members,
            "tariffs": sorted(
                ({"file": path.name, "tariff_id": tariff.tariff_id,
                  "sha256": tariff.input_sha256}
                 for path, tariff in zip(paths, tariffs)),
                key=lambda item: (item["tariff_id"], item["file"]),
            ),
        }
        report = {
            "report_schema": REPORT_SCHEMA,
            "currency": "CNY",
            "month": {"utc": month.label, "start": stamp(month.start), "end": stamp(month.end)},
            "site": site,
            "comparable": not gaps,
            "meters": members,
            "hours_per_meter": month.hours,
            "gaps": gaps,
            "tariffs": tariff_docs,
            "source": source_summary,
        }
        if gaps:
            # A missing hour makes the coincident site peak unknowable; pricing and
            # ranking are intentionally withheld rather than computed on partial data.
            report["hourly_basis"] = []
            report["peak"] = None
            report["site_month"] = None
            report["comparison"] = []
            return report

        basis = {}
        for meter in members:
            basis[meter] = [(instant, facts[(meter, instant)]) for instant in expected]
        hourly_basis = []
        site_hourly = {}
        for instant in expected:
            total = sum((facts[(meter, instant)] for meter in members), Decimal(0))
            site_hourly[instant] = total
            for meter in members:
                hourly_basis.append({
                    "meter_id": meter,
                    "interval_start": stamp(instant),
                    "interval_end": stamp(instant + timedelta(hours=1)),
                    "kwh": fixed_text(facts[(meter, instant)], 6),
                    "site_hour_kwh": fixed_text(total, 6),
                })
        peak_value = max(site_hourly.values())
        peak_hours = [
            {"interval_start": stamp(instant), "site_kwh": fixed_text(site_hourly[instant], 6)}
            for instant in expected if site_hourly[instant] == peak_value
        ]
        total_kwh = sum(site_hourly.values(), Decimal(0))
        blocks = []
        for tariff in tariffs:
            blocks.append(_price(tariff, total_kwh, peak_value, basis))
        report.update({
            "hourly_basis": hourly_basis,
            "peak": {
                "hourly_site_kwh": fixed_text(peak_value, 6),
                "hours": peak_hours,
            },
            "site_month": {
                "total_kwh": fixed_text(total_kwh, 6),
                "meter_count": len(members),
                "hour_count": month.hours,
            },
            "comparison": _rank(blocks),
        })
        return report


def render_report(report):
    """Serialize a report to stable bytes (sorted keys, one trailing newline)."""
    try:
        return (json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReportError(f"report is not JSON-serializable: {exc}") from exc


def write_report(report, output):
    """Atomically publish ``report``; a failure leaves any old report intact."""
    output = Path(output)
    payload = render_report(report)
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{output.name}.", suffix=".tmp", dir=output.parent,
            delete=False,
        )
    except OSError as exc:
        raise ReportError(f"cannot stage report next to {output}: {exc}") from exc
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise ReportError(f"cannot publish report {output}: {exc}") from exc
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    return output
