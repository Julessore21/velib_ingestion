from pathlib import Path

from velib_ingestion.client import VelibClient
from velib_ingestion.exporter import export_dataframe
from velib_ingestion.validator import validate_station_information, validate_station_status
from velib_ingestion.transformer import stations_to_dataframe


def main() -> None:
    client = VelibClient()
    info = client.get_station_information()
    status = client.get_station_status()

    validate_station_information(info, strict=False)
    validate_station_status(status, strict=False)

    df = stations_to_dataframe(info, status)
    print(f"Stations merged: {len(df)}")
    print(df.head())

    export_dataframe(df, Path("data/velib.csv"))


if __name__ == "__main__":
    main()
