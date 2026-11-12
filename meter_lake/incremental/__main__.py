"""Command line entry point for incremental snapshot revision."""
import argparse
import json
from pathlib import Path

from .engine import SnapshotError, revise
from ..patch import FinalStateError, PatchError


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m meter_lake.incremental",
        description="Apply an ordered change patch to a published v1/v2 snapshot",
    )
    parser.add_argument("--base", type=Path, required=True, help="parent snapshot directory")
    parser.add_argument("--changes", type=Path, required=True, help="ordered change patch CSV")
    parser.add_argument("--output", type=Path, required=True, help="new snapshot directory (must not exist)")
    args = parser.parse_args(argv)
    try:
        summary = revise(args.base, args.changes, args.output)
    except FileExistsError as exc:
        parser.exit(2, f"incremental build failed: {exc}\n")
    except (OSError, PatchError, FinalStateError, SnapshotError) as exc:
        parser.exit(2, f"incremental build failed: {exc}\n")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
