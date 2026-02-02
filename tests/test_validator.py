import pytest

from velib_ingestion.validator import ValidationError, validate_station_information, validate_station_status


def test_station_information_valid():
    payload = {
        "data": {
            "stations": [
                {"station_id": "123", "name": "Test", "lat": 48.1, "lon": 2.3}
            ]
        }
    }
    assert validate_station_information(payload) is True


def test_station_information_invalid_missing_key():
    payload = {"data": {"stations": [{"station_id": "123"}]}}
    with pytest.raises(ValidationError):
        validate_station_information(payload)


def test_station_status_valid():
    payload = {
        "data": {
            "stations": [
                {
                    "station_id": "123",
                    "num_bikes_available": 5,
                    "num_docks_available": 10,
                    "is_installed": 1,
                    "is_renting": 1,
                    "is_returning": 1,
                }
            ]
        }
    }
    assert validate_station_status(payload) is True


def test_station_status_invalid_type():
    payload = {
        "data": {
            "stations": [
                {
                    "station_id": "123",
                    "num_bikes_available": "five",
                    "num_docks_available": 10,
                }
            ]
        }
    }
    with pytest.raises(ValidationError):
        validate_station_status(payload)
