"""Strict JSON tariff-plan parsing for monthly site price comparison.

Amounts are nonnegative decimal yuan carried as ``Decimal``; no float is used.
The parser rejects duplicate JSON keys, non-finite constants, unknown fields,
duplicate identifiers, uncovered/overlapping hour bands and non-increasing
tier boundaries.
"""
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

MAX_PRICE = Decimal("1000000000000")       # 10^12 yuan per unit
MAX_FIXED = Decimal("1000000000000")       # 10^12 yuan
MAX_THRESHOLD = Decimal("1000000000000000")  # 10^15 cumulative kWh
MAX_ID_LENGTH = 128

_AMOUNT_RE = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,6})?$")
_KEYS = {"tariff_id", "segments", "tiers", "demand_price_per_kw", "fixed_fee"}
_SEGMENT_KEYS = {"id", "start_hour", "end_hour", "price_per_kwh"}
_TIER_KEYS = {"id", "threshold_kwh", "price_per_kwh"}


class TariffError(ValueError):
    """A tariff input is malformed or outside the strict contract."""


@dataclass(frozen=True)
class Segment:
    segment_id: str
    start_hour: int
    end_hour: int
    price: Decimal


@dataclass(frozen=True)
class Tier:
    tier_id: str
    threshold: Decimal
    price: Decimal


@dataclass(frozen=True)
class Tariff:
    tariff_id: str
    segments: tuple
    tiers: tuple
    demand_price: Decimal
    fixed_fee: Decimal
    input_sha256: str

    def document(self):
        """Return the normalized, JSON-safe tariff used in the report."""
        return {
            "tariff_id": self.tariff_id,
            "segments": [
                {
                    "id": segment.segment_id,
                    "start_hour": segment.start_hour,
                    "end_hour": segment.end_hour,
                    "price_per_kwh": decimal_text(segment.price),
                }
                for segment in self.segments
            ],
            "tiers": [
                {
                    "id": tier.tier_id,
                    "threshold_kwh": decimal_text(tier.threshold),
                    "price_per_kwh": decimal_text(tier.price),
                }
                for tier in self.tiers
            ],
            "demand_price_per_kw": decimal_text(self.demand_price),
            "fixed_fee": decimal_text(self.fixed_fee),
        }


def decimal_text(value):
    """Canonical fixed notation without insignificant trailing zeros."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def fixed_text(value, places=6):
    """Exact fixed-point text at ``places`` decimals (no rounding loss)."""
    quantum = Decimal(1).scaleb(-places)
    exact = value.quantize(quantum)
    if exact != value:
        raise ValueError("value has more fractional digits than the report allows")
    return format(exact, "f")


def _money(value, field):
    if not isinstance(value, str):
        raise TariffError(f"{field} must be a decimal string of nonnegative yuan")
    if not _AMOUNT_RE.fullmatch(value):
        raise TariffError(
            f"{field} must be a nonnegative decimal without exponent or leading zeros: {value!r}"
        )
    if len(value.split(".")[0]) > 12:
        raise TariffError(f"{field} integer part is too large")
    amount = Decimal(value)
    if amount > MAX_PRICE:
        raise TariffError(f"{field} exceeds the supported maximum")
    return amount


def _threshold(value, field):
    if not isinstance(value, str):
        raise TariffError(f"{field} must be a decimal string of cumulative kWh")
    if not _AMOUNT_RE.fullmatch(value):
        raise TariffError(
            f"{field} must be a nonnegative decimal without exponent or leading zeros: {value!r}"
        )
    if len(value.split(".")[0]) > 15:
        raise TariffError(f"{field} integer part is too large")
    amount = Decimal(value)
    if amount > MAX_THRESHOLD:
        raise TariffError(f"{field} exceeds the supported maximum")
    return amount


def _identifier(value, field):
    if not isinstance(value, str) or not value or value.strip() == "":
        raise TariffError(f"{field} must be a nonempty string")
    if len(value) > MAX_ID_LENGTH:
        raise TariffError(f"{field} is longer than {MAX_ID_LENGTH} characters")
    if any(ord(char) < 32 for char in value):
        raise TariffError(f"{field} contains control characters")
    return value


def _hour(value, field):
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 24:
        raise TariffError(f"{field} must be an integer hour between 0 and 24")
    return value


def _keys(obj, required, where):
    if not isinstance(obj, dict):
        raise TariffError(f"{where} must be a JSON object")
    missing = required - set(obj)
    extra = set(obj) - required
    if missing or extra:
        details = []
        if missing:
            details.append("missing " + ", ".join(sorted(missing)))
        if extra:
            details.append("unexpected " + ", ".join(sorted(extra)))
        raise TariffError(f"{where} has wrong fields: {'; '.join(details)}")


def _object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TariffError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(value):
    raise TariffError(f"non-finite JSON constant {value} is not allowed")


def _json_object(path):
    path = Path(path)
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TariffError(f"{path}: tariff JSON must be UTF-8") from exc
    try:
        document = json.loads(
            text,
            object_pairs_hook=_object_pairs,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as exc:
        raise TariffError(f"{path}: invalid JSON: {exc}") from exc
    return raw, document


def load_tariff(path):
    """Read one tariff file, validate the whole contract and return a Tariff."""
    raw, document = _json_object(path)
    _keys(document, _KEYS, "tariff")
    tariff_id = _identifier(document["tariff_id"], "tariff_id")
    segments_raw = document["segments"]
    tiers_raw = document["tiers"]
    if not isinstance(segments_raw, list) or not segments_raw:
        raise TariffError("segments must be a nonempty JSON array")
    if not isinstance(tiers_raw, list) or not tiers_raw:
        raise TariffError("tiers must be a nonempty JSON array")

    segments = []
    for index, item in enumerate(segments_raw):
        _keys(item, _SEGMENT_KEYS, f"segments[{index}]")
        seg_id = _identifier(item["id"], f"segments[{index}].id")
        start = _hour(item["start_hour"], f"segments[{index}].start_hour")
        end = _hour(item["end_hour"], f"segments[{index}].end_hour")
        if start >= end:
            raise TariffError(f"segments[{index}] must have start_hour < end_hour")
        price = _money(item["price_per_kwh"], f"segments[{index}].price_per_kwh")
        segments.append(Segment(seg_id, start, end, price))
    segments.sort(key=lambda segment: (segment.start_hour, segment.end_hour))
    if segments[0].start_hour != 0:
        raise TariffError("segments must cover hour 0")
    for previous, current in zip(segments, segments[1:]):
        if current.start_hour != previous.end_hour:
            raise TariffError(
                "segments must be mutually exclusive and cover every hour from 0 to 24"
            )
    if segments[-1].end_hour != 24:
        raise TariffError("segments must cover hour 24")

    tiers = []
    for index, item in enumerate(tiers_raw):
        _keys(item, _TIER_KEYS, f"tiers[{index}]")
        tier_id = _identifier(item["id"], f"tiers[{index}].id")
        threshold = _threshold(item["threshold_kwh"], f"tiers[{index}].threshold_kwh")
        price = _money(item["price_per_kwh"], f"tiers[{index}].price_per_kwh")
        tiers.append(Tier(tier_id, threshold, price))
    tiers.sort(key=lambda tier: tier.threshold)
    if tiers[0].threshold != 0:
        raise TariffError("the first tier must start at cumulative kWh 0")
    for previous, current in zip(tiers, tiers[1:]):
        if current.threshold <= previous.threshold:
            raise TariffError("tier thresholds must be strictly increasing")

    demand_price = _money(document["demand_price_per_kw"], "demand_price_per_kw")
    if demand_price > MAX_FIXED:
        raise TariffError("demand_price_per_kw exceeds the supported maximum")
    fixed_fee = _money(document["fixed_fee"], "fixed_fee")

    segment_ids = [segment.segment_id for segment in segments]
    if len(segment_ids) != len(set(segment_ids)):
        raise TariffError("segment ids must be unique within the tariff")
    tier_ids = [tier.tier_id for tier in tiers]
    if len(tier_ids) != len(set(tier_ids)):
        raise TariffError("tier ids must be unique within the tariff")
    if tariff_id in segment_ids or tariff_id in tier_ids:
        raise TariffError("tariff_id must not reuse a segment or tier id")

    return Tariff(
        tariff_id=tariff_id,
        segments=tuple(segments),
        tiers=tuple(tiers),
        demand_price=demand_price,
        fixed_fee=fixed_fee,
        input_sha256=hashlib.sha256(raw).hexdigest(),
    )
