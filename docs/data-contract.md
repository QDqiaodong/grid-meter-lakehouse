# Meter accounting v1

`meters.csv`: meter_id (unique nonempty), site (nonempty), multiplier (integer
1..10000), max_kw (integer 1..1000000, positive physical capacity).
`readings.csv`: meter_id, observed_at (ISO8601 with timezone), register_wh
(nonnegative cumulative integer Wh, <=10^15). Exact headers; no missing fields.
Every dimension meter has >=2 readings. Duplicate keys and unknown meters fail.
Rows may be unordered. Instants normalize to UTC, exactly at hour boundaries.

One fact row is a half-open interval [previous observed_at, observed_at).
Consumption kWh = (current register_wh - previous register_wh) * multiplier / 1000.
The first reading is only a boundary, never consumption. Adjacent instants must
be exactly one hour apart; register resets and capacity violations fail.
Zero consumption is legitimate. No interpolation, rollover or missing-hour
estimation is performed. UTC date of interval_start defines accounting day;
local tariff days and daylight saving rules are outside this baseline.

Layers:
- bronze: byte-identical input CSVs; SHA256 in manifest.json.
- silver: meters.parquet and readings.parquet with typed UTC timestamp/integers.
- gold: interval_usage.parquet and daily_site_usage.parquet; decimal(24,6) kWh.
- warehouse.duckdb: same four physical SQL tables, self-contained after moving.

Full-batch snapshots only, one writer, fresh output directory. Failed validation
leaves no output snapshot; an existing output is refused. There is no mutable
latest pointer, partition pruning, schema migration, streaming checkpoint, or
concurrent publisher protocol. Parquet files and database must be kept together
with their manifest when exchanging a snapshot.

Revision of an already published snapshot is a separate v2 contract built from
an explicit change patch; see `incremental-v2.md`.  It never mutates a v1
snapshot and reuses the same four physical tables and Parquet layer names.
