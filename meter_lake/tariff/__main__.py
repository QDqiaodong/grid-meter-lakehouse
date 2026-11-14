"""Command line entry point for monthly site tariff comparison."""
import argparse
import json
from pathlib import Path
import sys

from .compare import ReportError, compare_month, write_report


def build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m meter_lake.tariff",
        description="Compare synthetic tariff plans for one site and UTC month "
                    "using a read-only v1/v2 snapshot",
    )
    parser.add_argument("--snapshot", type=Path, required=True,
                        help="v1/v2 snapshot directory (read-only)")
    parser.add_argument("--month", required=True, help="UTC month YYYY-MM")
    parser.add_argument("--site", required=True, help="site name from the meter dimension")
    parser.add_argument("--tariff", type=Path, action="append", required=True,
                        help="strict JSON tariff file; repeat for each competing plan")
    parser.add_argument("--output", type=Path, required=True,
                        help="report JSON path; written atomically")
    parser.add_argument("--fail-after", choices=["report_ready"],
                        help=argparse.SUPPRESS)
    return parser


def _inside(path, directory):
    try:
        path.resolve().relative_to(directory.resolve())
    except ValueError:
        return False
    return True


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if _inside(args.output, args.snapshot):
            raise ReportError(
                f"refusing to write the report inside the read-only snapshot: {args.output}")
        for tariff in args.tariff:
            if args.output.resolve() == tariff.resolve():
                raise ReportError(
                    f"refusing to overwrite a tariff input with the report: {tariff}")
        report = compare_month(args.snapshot, args.month, args.site, args.tariff)
        if args.fail_after == "report_ready":
            raise RuntimeError("injected failure before report write")
        write_report(report, args.output)
    except RuntimeError as exc:
        parser.exit(2, f"tariff comparison failed: {exc}\n")
    except (OSError, ValueError) as exc:
        # SnapshotError/MonthError/TariffError/BadFactError are ValueError
        # subclasses and ReportError an OSError; any failure happens before a
        # report is published, so a previous report stays byte-identical.
        parser.exit(2, f"tariff comparison failed: {exc}\n")
    summary = {
        "output": str(args.output),
        "month": report["month"]["utc"],
        "site": report["site"],
        "comparable": report["comparable"],
        "ranked": [
            {"rank": block["rank"], "tariff_id": block["tariff_id"],
             "total_yuan": block["components"]["total_yuan"],
             "total_yuan_rounded": block["components"]["total_yuan_rounded"]}
            for block in report["comparison"]
        ],
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
