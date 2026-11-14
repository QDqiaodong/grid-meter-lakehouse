"""Strict cumulative-register ingestion for hourly consumption accounting.

The meter dimension is small and stays in Python memory.  Readings stream
through bounded batches: at most one new batch is materialized while the
caller still holds the previous one, and no key set proportional to the
reading count is kept in Python — key uniqueness (including UTC-equivalent
instants across batches) is enforced by the DuckDB primary key instead.
"""
import csv
from datetime import datetime, timezone

METER_HEADERS = ["meter_id", "site", "multiplier", "max_kw"]
READING_HEADERS = ["meter_id", "observed_at", "register_wh"]
MAX_REGISTER = 10**15


def csv_rows(path, headers):
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != headers:
            raise ValueError(f"expected columns: {headers}")
        rows = list(reader)
    if any(None in row or any(v is None for v in row.values()) for row in rows):
        raise ValueError("inconsistent CSV field count")
    return rows


def read_meters(path):
    """Load and validate the bounded meter dimension table."""
    meters = []
    known = set()
    for row in csv_rows(path, METER_HEADERS):
        meter, site = row["meter_id"], row["site"]
        multiplier, max_kw = int(row["multiplier"]), int(row["max_kw"])
        if not meter or not site or meter in known or not (1 <= multiplier <= 10000) or not (1 <= max_kw <= 1000000):
            raise ValueError("invalid or duplicate meter dimension")
        known.add(meter)
        meters.append((meter, site, multiplier, max_kw))
    if not meters:
        raise ValueError("empty meter dimension")
    return meters, known


def iter_reading_batches(path, known, batch_rows):
    """Yield validated readings as lists of at most ``batch_rows`` tuples.

    Rows may be unordered.  Per-row metering rules (known meter, timezone,
    UTC hour boundary, register range, field count) are enforced here, one
    row at a time; duplicate normalized keys are left to the DuckDB primary
    key so memory stays bounded by the batch, not by the file.
    """
    batch = []
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != READING_HEADERS:
            raise ValueError(f"expected columns: {READING_HEADERS}")
        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValueError("inconsistent CSV field count")
            meter = row["meter_id"]
            instant = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
            if instant.tzinfo is None:
                raise ValueError("readings require a timezone")
            instant = instant.astimezone(timezone.utc)
            register = int(row["register_wh"])
            if meter not in known or not (0 <= register <= MAX_REGISTER) or instant.minute or instant.second or instant.microsecond:
                raise ValueError("unknown meter, invalid register, or non-hour boundary")
            batch.append((meter, instant, register))
            if len(batch) >= batch_rows:
                yield batch
                batch = []
    if batch:
        yield batch
