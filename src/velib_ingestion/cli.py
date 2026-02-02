from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .client import VelibClient
from .exporter import export_dataframe
from .transformer import stations_to_dataframe
from .validator import validate_station_information, validate_station_status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="velib_ingestion",
        description="Ingestion and transformation for Velib Metropole GBFS",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate JSON payloads (light checks)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output file path (csv or parquet)",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "parquet"],
        default="csv",
        help="Output format when --output is set",
    )
    parser.add_argument(
        "--merge-how",
        type=str,
        default="inner",
        choices=["inner", "left", "right", "outer"],
        help="Merge strategy for stations join",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Do not drop duplicate station_id before merge",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    with VelibClient() as client:
        info = client.get_station_information(validate=args.validate)
        status = client.get_station_status(validate=args.validate)

    df = stations_to_dataframe(
        info,
        status,
        how=args.merge_how,
        drop_duplicates=not args.no_dedup,
    )

    if args.output:
        path = Path(args.output)
        export_dataframe(df, path, fmt=args.format)
        print(f"Exported {len(df)} rows to {path}")
        return 0

    print(df.head())
    print(f"Rows: {len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
