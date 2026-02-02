from __future__ import annotations

from typing import Iterable


class ValidationError(ValueError):
    pass


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def _ensure_keys(obj: dict, keys: Iterable[str], context: str) -> None:
    for key in keys:
        _ensure(key in obj, f"Missing key '{key}' in {context}")


def _ensure_type(value: object, expected: type | tuple[type, ...], context: str) -> None:
    _ensure(isinstance(value, expected), f"Invalid type for {context}")


def validate_station_information(payload: dict, strict: bool = True) -> bool:
    _ensure(isinstance(payload, dict), "Payload must be a dict")
    _ensure("data" in payload, "Missing 'data' at root")
    data = payload["data"]
    _ensure(isinstance(data, dict), "'data' must be a dict")
    _ensure("stations" in data, "Missing 'stations' in data")
    stations = data["stations"]
    _ensure(isinstance(stations, list), "'stations' must be a list")

    sample = stations if strict else stations[:1]
    for station in sample:
        _ensure(isinstance(station, dict), "Station entry must be a dict")
        _ensure_keys(station, ["station_id", "name", "lat", "lon"], "station")
        _ensure_type(station["station_id"], str, "station.station_id")
        _ensure_type(station["name"], str, "station.name")
        _ensure_type(station["lat"], (int, float), "station.lat")
        _ensure_type(station["lon"], (int, float), "station.lon")

    return True


def validate_station_status(payload: dict, strict: bool = True) -> bool:
    _ensure(isinstance(payload, dict), "Payload must be a dict")
    _ensure("data" in payload, "Missing 'data' at root")
    data = payload["data"]
    _ensure(isinstance(data, dict), "'data' must be a dict")
    _ensure("stations" in data, "Missing 'stations' in data")
    stations = data["stations"]
    _ensure(isinstance(stations, list), "'stations' must be a list")

    sample = stations if strict else stations[:1]
    for station in sample:
        _ensure(isinstance(station, dict), "Station entry must be a dict")
        _ensure_keys(
            station,
            ["station_id", "num_bikes_available", "num_docks_available"],
            "station",
        )
        _ensure_type(station["station_id"], str, "station.station_id")
        _ensure_type(station["num_bikes_available"], int, "station.num_bikes_available")
        _ensure_type(station["num_docks_available"], int, "station.num_docks_available")

        for flag in ["is_installed", "is_renting", "is_returning"]:
            if flag in station:
                _ensure_type(station[flag], (bool, int), f"station.{flag}")

    return True
