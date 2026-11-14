"""Build one self-contained, full-batch lake snapshot.

Readings stream from the staged bronze CSV into DuckDB in bounded batches
of ``batch_rows`` rows; Python never holds the full reading set or a key
set proportional to it.  Validation, ingestion and the manifest hashes all
operate on the same staged bronze bytes, so a published manifest always
describes exactly the bytes that were checked and loaded.
"""
import hashlib
import json
from importlib.resources import files
from pathlib import Path
import shutil
import tempfile
import duckdb
from .ingest import iter_reading_batches, read_meters

DEFAULT_BATCH_ROWS = 10000


def literal(path):
    return "'" + str(path).replace("'", "''") + "'"


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build(meters_path, readings_path, output, *, batch_rows=DEFAULT_BATCH_ROWS):
    """Build a v1 full-batch snapshot; return its business summary.

    ``batch_rows`` is the maximum number of reading rows held in Python and
    inserted per DuckDB statement; it must be a positive integer (booleans
    are rejected).  The output directory must not exist; any failure before
    publication removes the staging directory and leaves no output.
    """
    if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows < 1:
        raise ValueError("batch_rows must be a positive integer")
    output = Path(output)
    if output.exists():
        raise FileExistsError("output already exists; choose a new full-batch directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".meter-build-", dir=output.parent) as temporary:
        snapshot = Path(temporary) / "snapshot"
        for layer in ["bronze", "silver", "gold"]:
            (snapshot / layer).mkdir(parents=True, exist_ok=True)
        for source, name in [(meters_path, "meters.csv"), (readings_path, "readings.csv")]:
            shutil.copyfile(source, snapshot / "bronze" / name)
        staged = {name: snapshot / "bronze" / name for name in ["meters.csv", "readings.csv"]}
        meters, known = read_meters(staged["meters.csv"])
        with duckdb.connect(str(snapshot / "warehouse.duckdb")) as db:
            db.execute("SET TimeZone='UTC'")
            db.execute("CREATE TABLE meters(meter_id VARCHAR PRIMARY KEY,site VARCHAR,multiplier INTEGER,max_kw INTEGER)")
            db.executemany("INSERT INTO meters VALUES (?,?,?,?)", meters)
            db.execute("CREATE TABLE readings(meter_id VARCHAR, observed_at TIMESTAMPTZ,register_wh BIGINT, PRIMARY KEY(meter_id,observed_at))")
            total = 0
            covered = set()
            for batch in iter_reading_batches(staged["readings.csv"], known, batch_rows):
                # One parameterized multi-row INSERT per batch: far cheaper
                # than per-row statements, still bounded by batch_rows.
                placeholders = ",".join(["(?,?,?)"] * len(batch))
                params = [value for row in batch for value in row]
                try:
                    db.execute(f"INSERT INTO readings VALUES {placeholders}", params)
                except duckdb.ConstraintException as exc:
                    raise ValueError("duplicate reading key") from exc
                total += len(batch)
                covered.update(row[0] for row in batch)
            if not total:
                raise ValueError("empty readings")
            if covered != known:
                raise ValueError("every meter must have readings")
            db.execute(files("meter_lake").joinpath("transform.sql").read_text())
            bad = db.execute("""SELECT count(*) FROM interval_usage
                WHERE interval_end-interval_start != INTERVAL '1 hour'
                OR delta_wh < 0 OR kwh > max_kw""").fetchone()[0]
            counts = db.execute("SELECT meter_id,count(*) FROM readings GROUP BY meter_id HAVING count(*) < 2").fetchall()
            if bad or counts:
                raise ValueError("need >=2 hourly readings per meter, no gaps, resets, or capacity violations")
            for table, layer in [("readings", "silver"), ("meters", "silver"), ("interval_usage", "gold"), ("daily_site_usage", "gold")]:
                db.execute(f"COPY {table} TO {literal(snapshot/layer/(table+'.parquet'))} (FORMAT PARQUET, COMPRESSION ZSTD)")
            rows = db.execute("SELECT site,CAST(usage_date AS VARCHAR),meter_intervals,CAST(kwh AS VARCHAR) FROM daily_site_usage ORDER BY site,usage_date").fetchall()
            summary = {"meters": len(meters), "readings": total, "intervals": db.execute("SELECT count(*) FROM interval_usage").fetchone()[0],
                       "daily": [dict(zip(["site", "date", "meter_intervals", "kwh"], r)) for r in rows]}
        manifest = {"schema_version": 1, "mode": "full-batch", "timezone": "UTC", "inputs": {
            name: _sha256(staged[name]) for name in ["meters.csv", "readings.csv"]}, "summary": summary}
        (snapshot / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
        snapshot.rename(output)
    return summary
