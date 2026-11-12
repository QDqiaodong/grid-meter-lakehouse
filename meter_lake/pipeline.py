"""Build one self-contained, full-batch lake snapshot."""
import hashlib
import json
from importlib.resources import files
from pathlib import Path
import shutil
import tempfile
import duckdb
from .ingest import read_inputs


def literal(path):
    return "'" + str(path).replace("'", "''") + "'"


def build(meters_path, readings_path, output):
    output = Path(output)
    if output.exists():
        raise FileExistsError("output already exists; choose a new full-batch directory")
    meters, readings = read_inputs(meters_path, readings_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".meter-build-", dir=output.parent) as temporary:
        snapshot = Path(temporary) / "snapshot"
        for layer in ["bronze", "silver", "gold"]:
            (snapshot / layer).mkdir(parents=True, exist_ok=True)
        for source, name in [(meters_path, "meters.csv"), (readings_path, "readings.csv")]:
            shutil.copyfile(source, snapshot / "bronze" / name)
        with duckdb.connect(str(snapshot / "warehouse.duckdb")) as db:
            db.execute("SET TimeZone='UTC'")
            db.execute("CREATE TABLE meters(meter_id VARCHAR PRIMARY KEY,site VARCHAR,multiplier INTEGER,max_kw INTEGER)")
            db.executemany("INSERT INTO meters VALUES (?,?,?,?)", meters)
            db.execute("CREATE TABLE readings(meter_id VARCHAR, observed_at TIMESTAMPTZ,register_wh BIGINT, PRIMARY KEY(meter_id,observed_at))")
            db.executemany("INSERT INTO readings VALUES (?,?,?)", readings)
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
            summary = {"meters": len(meters), "readings": len(readings), "intervals": db.execute("SELECT count(*) FROM interval_usage").fetchone()[0],
                       "daily": [dict(zip(["site", "date", "meter_intervals", "kwh"], r)) for r in rows]}
        manifest = {"schema_version": 1, "mode": "full-batch", "timezone": "UTC", "inputs": {
            name: hashlib.sha256((snapshot/"bronze"/name).read_bytes()).hexdigest() for name in ["meters.csv", "readings.csv"]}, "summary": summary}
        (snapshot / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
        snapshot.rename(output)
    return summary
