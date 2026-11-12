import argparse
import json
import duckdb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database")
    args = parser.parse_args()
    with duckdb.connect(args.database, read_only=True) as db:
        rows = db.execute("SELECT site,CAST(usage_date AS VARCHAR),CAST(kwh AS VARCHAR) FROM daily_site_usage ORDER BY site,usage_date").fetchall()
    print(json.dumps(rows))


if __name__ == "__main__":
    main()
