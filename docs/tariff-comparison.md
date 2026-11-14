# Monthly site tariff comparison

A read-only price comparison for one site in one UTC month.  It consumes a
published v1 full-batch snapshot or a v2 incremental snapshot and one or more
strict JSON tariff plans; it never writes to the snapshot, changes the
manifest, or creates a database.  The existing `python -m meter_lake`,
`python -m meter_lake.incremental` and `python -m meter_lake.query` entry
points and the DuckDB/Parquet snapshot layout are unchanged.

```sh
.venv/bin/python -m meter_lake.tariff \
  --snapshot run/lake-v2 \
  --month 2026-01 \
  --site "North Works" \
  --tariff data/tariffs/01-peak-aware.json \
  --tariff data/tariffs/02-flat-saver.json \
  --output run/north-works-2026-01.json
```

The bundled `data/` sample covers only two hours, so this command on
`run/lake-v2` truthfully reports the remaining monthly gaps and withholds
ranking; `tests/test_tariff.py` builds a full synthetic month to exercise a
complete bill.

Python API: `from meter_lake.tariff import compare_month, write_report`,
`compare_month(snapshot, "2026-01", "North Works", [path, ...])` returns the
report dictionary; `write_report` publishes it atomically.

## Tariff plan contract

One UTF-8 JSON object with exactly these fields (unknown or missing fields,
duplicate JSON keys, `NaN`/`Infinity` and JSON numbers where strings are
required are all rejected):

```json
{
  "tariff_id": "PEAK-AWARE",
  "segments": [
    {"id": "night", "start_hour": 0, "end_hour": 8, "price_per_kwh": "0.30"},
    {"id": "day", "start_hour": 8, "end_hour": 24, "price_per_kwh": "0.90"}
  ],
  "tiers": [
    {"id": "base", "threshold_kwh": "0", "price_per_kwh": "0.10"},
    {"id": "mid", "threshold_kwh": "50", "price_per_kwh": "0.20"}
  ],
  "demand_price_per_kw": "1.50",
  "fixed_fee": "30.00"
}
```

- `tariff_id`, segment `id` and tier `id` are nonempty strings (<=128 chars,
  no control characters); ids must be unique within a plan, and `tariff_id`
  must be unique across the plans compared in one run.
- `price_per_kwh`, `threshold_kwh`, `demand_price_per_kw` and `fixed_fee` are
  nonnegative decimal strings in yuan (up to 6 fractional digits, no `+`, no
  exponent, no leading zeros).  Money never passes through a float.
- `segments` must be mutually exclusive and cover every integer hour from 0 to
  24 exactly once; they may be listed in any order.  A fact is priced by the
  UTC hour of its `interval_start` (half-open `[h, h+1)`), so `start_hour` 0,
  `end_hour` 8 means hours 00:00–07:59 UTC.
- `tiers` are month-cumulative kWh adders, listed in any order, sorted by
  threshold: the first threshold must be 0 and thresholds strictly increase.
  Each tier's `price_per_kwh` applies only to the kWh inside that tier's band;
  the last tier is unbounded.  A monthly total that lands exactly on a
  threshold charges nothing in the next tier.
- `demand_price_per_kw` is charged on the site's coincident hourly total: the
  maximum over the month of the sum of all site meters' kWh in the same UTC
  hour (1 kWh in one hour = 1 kW).
- `fixed_fee` is charged once per site month.

## Comparison precondition

The month is a UTC half-open interval `[YYYY-MM-01T00:00Z, next month)`.
Comparison is possible only when **every meter currently assigned to the site
in the snapshot dimension has exactly one interval fact for every hour of the
month**, priced by its interval start.  Extra facts, non-hour widths, negative
energy, facts whose stored site disagrees with the dimension, invalid
timestamps and duplicate hourly keys are *bad facts* and reject the whole
comparison.  A missing hour is not a bad fact: it is reported per meter as
merged UTC gap ranges with a missing-hour count, and the run still succeeds
but `comparable` is `false`, `comparison` is empty, `peak`/`site_month` are
`null` and no ranking is produced — a coincident peak cannot be inferred from
partial data.  A zero kWh fact is a legitimate measurement and counts as
present.

## Cost formula

```
energy      = Σ_meter Σ_hour kwh × segment_price(hour of interval_start)
surcharge   = Σ_tier kWh_in_tier × tier_price_per_kwh     (marginal, last unbounded)
demand      = max_hour(Σ_meter kwh in that hour) × demand_price_per_kw
fixed       = fixed_fee
total       = energy + surcharge + demand + fixed
```

All components are computed exactly with `Decimal` from the snapshot's
`DECIMAL(24,6)` facts.  Only the final total is rounded, with
`ROUND_HALF_UP`, to fen; `total_yuan_rounded` and `total_fen` carry that
rounded value while `total_yuan` keeps the exact unrounded amount.

Plans are sorted by the **unrounded** total and then by `tariff_id`; equal
unrounded totals share the same rank (competition ranking, e.g. `1, 1, 3`).
Rounded cents never decide the order.

## Report

`report.json` is deterministic JSON (sorted keys, two-space indent, trailing
newline, no absolute paths or timestamps) and is self-contained: it can be
recomputed after the snapshot is deleted.

| key | content |
| --- | --- |
| `report_schema` | `meter-lake-site-month-tariff-v1` |
| `month`, `site`, `currency` | normalized UTC month bounds, site, `CNY` |
| `comparable`, `gaps` | precondition result; per-meter missing hours and merged gap ranges |
| `meters`, `hours_per_meter` | site meter ids from the dimension, expected hours |
| `tariffs` | normalized plans after validation (bands, tiers, prices) |
| `source` | snapshot `manifest_sha256`/`warehouse_sha256`/schema version/mode/timezone and each tariff file name + SHA256 |
| `hourly_basis` | one row per meter per hour: start/end, `kwh`, `site_hour_kwh` |
| `peak` | `hourly_site_kwh` and **every** tied `hours` entry |
| `site_month` | exact month `total_kwh`, meter and hour counts |
| `comparison` | one block per plan, ordered by rank |

Each comparison block has `rank`, `tariff_id`, `components`
(`energy_yuan`, `tier_surcharge_yuan`, `demand_charge_yuan`, `fixed_fee_yuan`,
exact `total_yuan`, `total_yuan_rounded`, `total_fen`), the per-tier
`tier_allocation` (`kwh`, `surcharge_yuan`), and `meters[].hours[]` with each
hour's `interval_start`/`interval_end`, `kwh`, chosen `segment_id`,
`price_per_kwh` and `energy_yuan`.  Amounts are canonical decimal strings
(trailing zeros removed); energy quantities are fixed six-decimal strings.

## Failure and replay

- Invalid month, unknown site, malformed/duplicate tariff, unsupported or
  broken snapshot and bad facts exit non-zero with a message and publish
  nothing.
- The report is written to a temporary file in the destination directory and
  atomically renamed.  Any failure — including a forced failure through the
  hidden `--fail-after report_ready` test hook — leaves a previously written
  report byte-identical and removes the temporary file.
- The snapshot is opened with DuckDB `read_only=True`; neither
  `warehouse.duckdb` nor `manifest.json` changes (verified in tests).
- Identical inputs (snapshot, month, site, tariff bytes) produce
  byte-identical report bytes, including when the tariff plans are passed in a
  different order.

Out of scope: local-calendar months and daylight-saving rules, meter swaps,
demand ratchets, power-factor adjustments, taxes, and cross-site allocation.
