import argparse
import json
from pathlib import Path
from .pipeline import build


def main():
    parser = argparse.ArgumentParser(description="Build a synthetic hourly meter lake snapshot")
    parser.add_argument("--meters", type=Path, default=Path("data/meters.csv"))
    parser.add_argument("--readings", type=Path, default=Path("data/readings.csv"))
    parser.add_argument("--output", type=Path, default=Path("run/lake"))
    args = parser.parse_args()
    try:
        result = build(args.meters, args.readings, args.output)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"lake build failed: {exc}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
