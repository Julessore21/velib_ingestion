from __future__ import annotations

import pandas as pd


class TransformError(ValueError):
    pass


def stations_to_dataframe(
    station_information: dict,
    station_status: dict,
    how: str = "inner",
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    try:
        info_stations = station_information["data"]["stations"]
        status_stations = station_status["data"]["stations"]
    except (TypeError, KeyError) as exc:
        raise TransformError("Invalid payload structure for transformation") from exc

    info_df = pd.DataFrame(info_stations)
    status_df = pd.DataFrame(status_stations)

    if drop_duplicates:
        info_df = info_df.drop_duplicates(subset=["station_id"])
        status_df = status_df.drop_duplicates(subset=["station_id"])

    merged = pd.merge(info_df, status_df, on="station_id", how=how, suffixes=("_info", "_status"))
    return merged
