# Incremental revision v2

v1 snapshots are immutable full-batch outputs.  A v2 snapshot is created by
applying one ordered change patch to a published parent (either a v1
full-batch snapshot or another v2 snapshot).  The parent is never modified;
the output is a complete, self-contained snapshot in a directory that did not
exist before the build.

## Patch contract

Exact ordered header: `op,meter_id,observed_at,register_wh,expected_wh`.

| op | key in parent | register_wh | expected_wh |
| --- | --- | --- | --- |
| `insert` | must not exist | required, 0..10^15 | must be empty |
| `replace` | must exist | required | required and equal to the parent value |
| `delete` | must exist | must be empty | required and equal to the parent value |

- `meter_id` must exist in the fixed meter dimension; dimensions never change.
- `observed_at` keeps the v1 time contract: ISO8601 with a timezone,
  normalized to a UTC hour boundary.  The normalized `(meter_id, observed_at)`
  pair is the unique key, so the same instant expressed with a different
  offset is a duplicate and the whole patch is rejected.
- Rows may arrive in any order; a header-only patch is a legal no-op.
- A `replace` carrying the same register value is a legal no-op; it is
  recorded in `audit.json` as a `noop_replacee` but changes no boundary.
- Existence and `expected_wh` checks all target the single parent state.  The
  whole batch is applied before final accounting is checked; patch row order
  never affects validation.  Conflicts are reported with the patch line,
  meter and UTC time.  Final-state violations (fewer than two boundaries, a
  non-hour gap, a register rollback, a `kwh > max_kw` interval) are reported
  separately and also reject the entire batch; nothing is partially accepted.

## Dependency scope

Only boundaries whose register actually changes (real inserts, deletes and
value-changing replaces) are change keys.  Each change key contributes the
intervals it participates in on both sides:

- the old candidate set from adjacency in the parent state,
- the new candidate set from adjacency in the final state,

where an interval is identified by `(meter_id, interval_start, interval_end)`.
Adjacent changes merge into one deduplicated set, so no interval is metered
twice.  Inserted boundaries have no old-side intervals; deleted boundaries
have no new-side intervals; an interior revise touches both neighboring
intervals on both sides, including across midnight.

Scoped SQL then:

1. replaces only the changed boundary rows in `readings`;
2. deletes the old candidate interval facts and recomputes the new candidates
   with point lookups against the final `readings` (no full-table `lag`);
3. determines affected site-days from the UTC date of the interval starts and
   the meter's site, deletes only those daily rows and re-aggregates them from
   the final facts, including untouched meters of the same site.  A site-day
   with no intervals left loses its daily row.

All other interval facts are reused unchanged.  Energy math stays in SQL with
BIGINT deltas and `DECIMAL(24,6)` kWh; no Python float is involved.

## Snapshot layout

```
snapshot/
├── warehouse.duckdb        # four physical BASE TABLES, self-contained
├── silver/{meters,readings}.parquet
├── gold/{interval_usage,daily_site_usage}.parquet
├── manifest.json
├── audit.json
└── bronze/
    ├── roots/              # v1: byte-identical original meters.csv/readings.csv
    └── patches/
        ├── patch-0001-<sha12>.csv
        └── patch-0002-<sha12>.csv
```

Every v2 snapshot keeps the root CSVs byte-identical and accumulates every
patch applied along the chain, adding the current patch last.  Stored patches
are the raw patch bytes; merged tables are only ever exported as silver
Parquet, never relabeled as raw inputs.

`manifest.json` (`schema_version: 2`, `mode: "incremental"`) records:

- `timezone` and the final business `summary` (same shape as v1);
- `parent.mode`, `parent.schema_version` and the SHA256 of the direct
  parent's raw manifest bytes;
- `patch.file`, the patch's original file name and its SHA256;
- `sources`: every root CSV and patch in application order, with relative
  paths and hashes;
- `scope`: counts of replaced reading keys, deleted/inserted interval rows
  and re-aggregated site-days — measured from the actual closure, not filled
  in by hand.

`audit.json` is deterministic (sorted keys, no timestamps or absolute paths)
and lists the old/new value of every changed boundary, no-op replaces, the
deduplicated old and new interval candidate keys, the affected site-days and
the number of parent interval rows reused.  It explains exactly which facts
on either side were processed.

## Failure and replay semantics

- A missing or unsupported base fails explicitly; no database is created.
- Any validation, SQL, export or audit failure happens inside a staging
  directory, which is removed on exit.  The parent is untouched and no
  output directory is published.  An existing output is always refused.
- The same parent plus the same patch written to two different outputs
  produces identical business tables, manifest and audit; DuckDB/Parquet
  bytes need not match.
- Replaying an old patch against a child snapshot re-evaluates existence and
  `expected_wh` against that child; a stale precondition is a conflict, never
  a silent success.
- After moving a snapshot (and making the parent inaccessible), all four
  tables, both Parquet layers, the provenance chain and the next revision
  still work, because the database contains physical tables and all sources
  are copied into the snapshot.

Out of scope: meter swaps, multiplier changes, interpolation, rollover
explanations, local-calendar accounting, concurrent publishing and
crash-recovery protocols.
