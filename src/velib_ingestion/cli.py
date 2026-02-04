from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .client import VelibClient
from .exporter import export_dataframe
from .kafka_stream import VelibKafkaConsumer, VelibKafkaProducer
from .transformer import stations_to_dataframe


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
    parser.add_argument(
        "--mode",
        type=str,
        default="batch",
        choices=["batch", "kafka-producer", "kafka-consumer"],
        help="Execution mode",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="",
        help="Kafka topic to consume when mode is kafka-consumer",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="In kafka-producer mode publish one snapshot and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.mode == "kafka-producer":
        with VelibKafkaProducer() as producer:
            if args.once:
                counts = producer.publish_snapshot(validate=args.validate)
                print(
                    "Published snapshot:"
                    f" info={counts['station_information']} status={counts['station_status']}"
                )
                return 0
            producer.publish_forever(validate=args.validate)
            return 0

    if args.mode == "kafka-consumer":
        if not args.topic:
            raise ValueError("--topic is required in kafka-consumer mode")
        with VelibKafkaConsumer(topic=args.topic) as consumer:
            consumer.consume_forever()
            return 0

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
