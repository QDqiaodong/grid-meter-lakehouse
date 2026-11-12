"""Incremental snapshot revision built from a published parent snapshot.

The parent snapshot (a v1 full-batch snapshot or a v2 incremental snapshot) is
never modified.  A validated patch is applied to an in-memory copy of the
register state, only the dependency closure of changed boundaries is repaired
with scoped SQL, and the complete new snapshot is assembled in a staging
directory before an atomic rename publishes it.
"""
import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ..patch import (
    FinalStateError,
    PatchError,
    apply_patch,
    dependency_closure,
    read_patch,
    validate_final_state,
)

REQUIRED_TABLES = ["meters", "readings", "interval_usage", "daily_site_usage"]
STAGING_PREFIX = ".meter-incr-"


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _stamp(instant):
    return instant.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _interval_key(row):
    meter, start, end = row
    return {"meter_id": meter, "interval_start": _stamp(start), "interval_end": _stamp(end)}


class SnapshotError(ValueError):
    """The base snapshot is missing, unsupported, or not self-contained."""


def _load_parent(base):
    base = Path(base)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"base snapshot does not exist: {base}")
    manifest_path = base / "manifest.json"
    if not manifest_path.is_file():
        raise SnapshotError("base snapshot has no manifest.json; refusing to create a database implicitly")
    warehouse = base / "warehouse.duckdb"
    if not warehouse.is_file():
        raise SnapshotError("base snapshot is missing warehouse.duckdb")
    manifest = json.loads(manifest_path.read_text())
    version = manifest.get("schema_version")
    mode = manifest.get("mode")
    if version == 1 and mode == "full-batch":
        lineage = 1
    elif version == 2 and mode == "incremental":
        lineage = 2
    else:
        raise SnapshotError(f"unsupported base snapshot: schema_version={version!r}, mode={mode!r}")
    with duckdb.connect(str(warehouse), read_only=True) as db:
        tables = {r[0] for r in db.execute("SHOW TABLES").fetchall()}
        if set(REQUIRED_TABLES) - tables:
            raise SnapshotError("base snapshot is missing required physical tables")
        for table in REQUIRED_TABLES:
            kinds = db.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name=?", [table]
            ).fetchall()
            if kinds and kinds[0][0] != "BASE TABLE":
                raise SnapshotError(f"base table {table} is not a physical BASE TABLE")
        # Read timestamps as UTC strings to avoid any client-side timezone
        # database dependency; all timestamp math stays inside DuckDB.
        meter_rows = db.execute(
            "SELECT meter_id,site,multiplier,max_kw FROM meters ORDER BY meter_id"
        ).fetchall()
        reading_rows = db.execute(
            "SELECT meter_id, strftime(observed_at AT TIME ZONE 'UTC', "
            "'%Y-%m-%dT%H:%M:%SZ'), register_wh "
            "FROM readings ORDER BY meter_id,observed_at"
        ).fetchall()
    known = {m[0] for m in meter_rows}
    parent_state = {
        (meter, datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)): register
        for meter, stamp, register in reading_rows
    }
    return {
        "base": base,
        "warehouse": warehouse,
        "manifest": manifest,
        "manifest_bytes": manifest_path.read_bytes(),
        "lineage": lineage,
        "meters": meter_rows,
        "known": known,
        "parent_state": parent_state,
    }


def _copy_provenance(parent, snapshot, patch_path, patch_name):
    bronze = snapshot / "bronze"
    (bronze / "roots").mkdir(parents=True)
    (bronze / "patches").mkdir(parents=True)
    if parent["lineage"] == 1:
        for name in ["meters.csv", "readings.csv"]:
            shutil.copyfile(parent["base"] / "bronze" / name, bronze / "roots" / name)
        sources = [
            {"role": "root", "file": "bronze/roots/meters.csv", "original_name": "meters.csv",
             "sha256": _sha256(bronze / "roots" / "meters.csv")},
            {"role": "root", "file": "bronze/roots/readings.csv", "original_name": "readings.csv",
             "sha256": _sha256(bronze / "roots" / "readings.csv")},
        ]
    else:
        for source in parent["manifest"]["sources"]:
            shutil.copyfile(parent["base"] / source["file"], snapshot / source["file"])
        sources = [dict(source) for source in parent["manifest"]["sources"]]
    shutil.copyfile(patch_path, bronze / "patches" / patch_name)
    stored = f"bronze/patches/{patch_name}"
    sources.append({"role": "patch", "file": stored, "original_name": Path(patch_path).name,
                    "sha256": _sha256(bronze / "patches" / patch_name)})
    return sources


def _site_day_keys(interval_keys, meters):
    site_of = {m[0]: m[1] for m in meters}
    keys = set()
    for meter, start, _end in interval_keys:
        keys.add((site_of[meter], start.date()))
    return keys


def _repair(db, old_intervals, new_intervals, affected_days, changes):
    """Run only scoped DELETE/INSERT statements inside one transaction."""
    db.execute("BEGIN TRANSACTION")
    # Readings: replace each actually changed boundary key.
    changed_keys = {(c["meter_id"], c["observed_at"]) for c in changes}
    for meter, instant in sorted(changed_keys, key=lambda item: (item[0], item[1])):
        db.execute("DELETE FROM readings WHERE meter_id=? AND observed_at=?",
                   [meter, _stamp(instant)])
    for change in changes:
        if change["new_register"] is not None:
            db.execute("INSERT INTO readings VALUES (?,CAST(? AS TIMESTAMPTZ),?)",
                       [change["meter_id"], _stamp(change["observed_at"]),
                        change["new_register"]])
    # Old interval facts are revoked; new candidate facts recomputed locally.
    for meter, start, end in sorted(old_intervals, key=lambda row: (row[0], row[1], row[2])):
        db.execute(
            "DELETE FROM interval_usage WHERE meter_id=? AND interval_start=? AND interval_end=?",
            [meter, _stamp(start), _stamp(end)])
    for meter, start, end in sorted(new_intervals, key=lambda row: (row[0], row[1], row[2])):
        db.execute(
            """
            INSERT INTO interval_usage
            WITH bounds AS (
                SELECT ?::VARCHAR AS meter_id,
                       CAST(? AS TIMESTAMPTZ) AS interval_start,
                       CAST(? AS TIMESTAMPTZ) AS interval_end
            )
            SELECT b.meter_id, m.site, b.interval_start, b.interval_end,
                   cur.register_wh - prv.register_wh AS delta_wh,
                   CAST((cur.register_wh - prv.register_wh) * m.multiplier / 1000.0 AS DECIMAL(24,6)) AS kwh,
                   m.max_kw
            FROM bounds b
            JOIN meters m ON m.meter_id = b.meter_id
            JOIN readings prv ON prv.meter_id = b.meter_id AND prv.observed_at = b.interval_start
            JOIN readings cur ON cur.meter_id = b.meter_id AND cur.observed_at = b.interval_end
            """,
            [meter, _stamp(start), _stamp(end)])
    for site, day in sorted(affected_days, key=lambda item: (item[0], item[1])):
        db.execute("DELETE FROM daily_site_usage WHERE site=? AND usage_date=?",
                   [site, day.isoformat()])
        db.execute(
            """
            INSERT INTO daily_site_usage
            SELECT site, CAST(interval_start AS DATE) AS usage_date,
                   count(*) AS meter_intervals, sum(kwh) AS kwh
            FROM interval_usage
            WHERE site=? AND CAST(interval_start AS DATE)=CAST(? AS DATE)
            GROUP BY site, CAST(interval_start AS DATE)
            """,
            [site, day.isoformat()])
    db.execute("COMMIT")
    return {
        "reading_keys_replaced": len(changed_keys),
        "interval_rows_deleted": len(old_intervals),
        "interval_rows_inserted": len(new_intervals),
        "daily_site_days_reaggregated": len(affected_days),
    }


def _post_check(db):
    bad = db.execute("""
        SELECT count(*) FROM interval_usage
        WHERE interval_end - interval_start != INTERVAL '1 hour'
           OR delta_wh < 0 OR kwh > max_kw
    """).fetchone()[0]
    short = db.execute(
        "SELECT meter_id FROM readings GROUP BY meter_id HAVING count(*) < 2").fetchall()
    gaps = db.execute("""
        WITH ordered AS (
            SELECT meter_id, observed_at,
                   lag(observed_at) OVER (PARTITION BY meter_id ORDER BY observed_at) AS prev_at
            FROM readings
        )
        SELECT count(*) FROM ordered
        WHERE prev_at IS NOT NULL AND observed_at - prev_at != INTERVAL '1 hour'
    """).fetchone()[0]
    if bad or short or gaps:
        raise FinalStateError("post-repair SQL check failed for final meter state")


def _summary(db):
    rows = db.execute("""
        SELECT site, CAST(usage_date AS VARCHAR), meter_intervals, CAST(kwh AS VARCHAR)
        FROM daily_site_usage ORDER BY site, usage_date
    """).fetchall()
    return {
        "meters": db.execute("SELECT count(*) FROM meters").fetchone()[0],
        "readings": db.execute("SELECT count(*) FROM readings").fetchone()[0],
        "intervals": db.execute("SELECT count(*) FROM interval_usage").fetchone()[0],
        "daily": [dict(zip(["site", "date", "meter_intervals", "kwh"], row)) for row in rows],
    }


def revise(base, changes_path, output, *, fail_after=None):
    """Create a new v2 incremental snapshot; return its business summary.

    ``fail_after`` is a test hook taking values in
    {"staged_provenance", "database_repaired", "parquet_export",
    "audit_write"}: the build raises RuntimeError at that point so
    publish/cleanup behavior is observable.
    """
    output = Path(output)
    if output.exists():
        raise FileExistsError("output already exists; choose a new incremental directory")
    parent = _load_parent(base)
    rows = read_patch(changes_path)
    final_state, changes, noop = apply_patch(rows, parent["parent_state"], parent["known"])
    validate_final_state(final_state, parent["meters"])
    old_intervals, new_intervals = dependency_closure(changes, parent["parent_state"], final_state)
    affected_days = _site_day_keys(old_intervals | new_intervals, parent["meters"])

    output.parent.mkdir(parents=True, exist_ok=True)
    patch_bytes = Path(changes_path).read_bytes()
    patch_sha = hashlib.sha256(patch_bytes).hexdigest()
    prior_patches = sum(1 for s in parent["manifest"].get("sources", []) if s["role"] == "patch")
    patch_name = f"patch-{prior_patches + 1:04d}-{patch_sha[:12]}.csv"
    with tempfile.TemporaryDirectory(prefix=STAGING_PREFIX, dir=output.parent) as temporary:
        snapshot = Path(temporary) / "snapshot"
        for layer in ["silver", "gold"]:
            (snapshot / layer).mkdir(parents=True)
        sources = _copy_provenance(parent, snapshot, changes_path, patch_name)
        if fail_after == "staged_provenance":
            raise RuntimeError("injected failure after provenance staging")
        shutil.copyfile(parent["warehouse"], snapshot / "warehouse.duckdb")
        with duckdb.connect(str(snapshot / "warehouse.duckdb")) as db:
            db.execute("SET TimeZone='UTC'")
            scope = _repair(db, old_intervals, new_intervals, affected_days, changes)
            _post_check(db)
            summary = _summary(db)
            if fail_after == "database_repaired":
                raise RuntimeError("injected failure after database repair")
            if fail_after == "parquet_export":
                raise RuntimeError("injected failure before parquet export")
            for table, layer in [("readings", "silver"), ("meters", "silver"),
                                 ("interval_usage", "gold"), ("daily_site_usage", "gold")]:
                target = snapshot / layer / f"{table}.parquet"
                db.execute(
                    f"COPY {table} TO '{str(target).replace(chr(39), chr(39) * 2)}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")
        # The database is closed here; audit/manifest failures still leave
        # only the staging directory, which TemporaryDirectory removes.
        if fail_after == "audit_write":
            raise RuntimeError("injected failure before audit write")
        audit = _audit_document(parent, changes_path, patch_sha, changes, noop,
                                old_intervals, new_intervals, affected_days, scope)
        manifest = {
            "schema_version": 2,
            "mode": "incremental",
            "timezone": "UTC",
            "parent": {
                "mode": parent["manifest"]["mode"],
                "schema_version": parent["manifest"]["schema_version"],
                "manifest_sha256": hashlib.sha256(parent["manifest_bytes"]).hexdigest(),
            },
            "patch": {"file": f"bronze/patches/{patch_name}",
                      "original_name": Path(changes_path).name,
                      "sha256": patch_sha},
            "sources": sources,
            "scope": scope,
            "summary": summary,
        }
        (snapshot / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        (snapshot / "audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
        snapshot.rename(output)
    return summary


def _audit_document(parent, changes_path, patch_sha, changes, noop,
                    old_intervals, new_intervals, affected_days, scope):
    changed_boundaries = sorted(
        ({"meter_id": c["meter_id"], "observed_at": _stamp(c["observed_at"]),
          "old_register_wh": c["old_register"], "new_register_wh": c["new_register"]}
         for c in changes),
        key=lambda item: (item["meter_id"], item["observed_at"]))
    old_keys = sorted((_interval_key(row) for row in old_intervals),
                      key=lambda item: (item["meter_id"], item["interval_start"], item["interval_end"]))
    new_keys = sorted((_interval_key(row) for row in new_intervals),
                      key=lambda item: (item["meter_id"], item["interval_start"], item["interval_end"]))
    day_keys = [{"site": site, "usage_date": day.isoformat()}
                for site, day in sorted(affected_days, key=lambda item: (item[0], item[1]))]
    return {
        "audit_schema": "meter-lake-incremental-v2",
        "patch": {"original_name": Path(changes_path).name, "sha256": patch_sha},
        "parent_manifest_sha256": hashlib.sha256(parent["manifest_bytes"]).hexdigest(),
        "changed_boundaries": changed_boundaries,
        "noop_replacees": sorted(
            ({"meter_id": c["meter_id"], "observed_at": _stamp(c["observed_at"]),
              "register_wh": c["register_wh"]} for c in noop),
            key=lambda item: (item["meter_id"], item["observed_at"])),
        "old_interval_candidates": old_keys,
        "new_interval_candidates": new_keys,
        "affected_site_days": day_keys,
        "reused_interval_rows": parent["manifest"]["summary"]["intervals"] - len(old_intervals),
        "processing_scope": scope,
    }
