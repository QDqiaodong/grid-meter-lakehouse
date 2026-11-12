"""Strict cumulative-register ingestion for hourly consumption accounting."""
import csv
from datetime import datetime, timezone


def csv_rows(path, headers):
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != headers:
            raise ValueError(f"expected columns: {headers}")
        rows = list(reader)
    if any(None in row or any(v is None for v in row.values()) for row in rows):
        raise ValueError("inconsistent CSV field count")
    return rows


def read_inputs(meters_path, readings_path):
    meters = []
    known = set()
    for row in csv_rows(meters_path, ["meter_id", "site", "multiplier", "max_kw"]):
        meter, site = row["meter_id"], row["site"]
        multiplier, max_kw = int(row["multiplier"]), int(row["max_kw"])
        if not meter or not site or meter in known or not (1 <= multiplier <= 10000) or not (1 <= max_kw <= 1000000):
            raise ValueError("invalid or duplicate meter dimension")
        known.add(meter)
        meters.append((meter, site, multiplier, max_kw))
    if not meters:
        raise ValueError("empty meter dimension")
    readings = []
    seen = set()
    for row in csv_rows(readings_path, ["meter_id", "observed_at", "register_wh"]):
        meter = row["meter_id"]
        instant = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
        if instant.tzinfo is None:
            raise ValueError("readings require a timezone")
        instant = instant.astimezone(timezone.utc)
        register = int(row["register_wh"])
        if meter not in known or not (0 <= register <= 10**15) or instant.minute or instant.second or instant.microsecond:
            raise ValueError("unknown meter, invalid register, or non-hour boundary")
        if (meter, instant) in seen:
            raise ValueError("duplicate reading key")
        seen.add((meter, instant))
        readings.append((meter, instant, register))
    if not readings:
        raise ValueError("empty readings")
    if {r[0] for r in readings} != known:
        raise ValueError("every meter must have readings")
    return meters, readings
