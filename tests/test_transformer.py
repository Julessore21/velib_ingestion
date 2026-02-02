from velib_ingestion.transformer import stations_to_dataframe


def test_stations_to_dataframe_merge():
    info = {
        "data": {
            "stations": [
                {"station_id": "123", "name": "Test", "lat": 48.1, "lon": 2.3}
            ]
        }
    }
    status = {
        "data": {
            "stations": [
                {
                    "station_id": "123",
                    "num_bikes_available": 5,
                    "num_docks_available": 10,
                }
            ]
        }
    }

    df = stations_to_dataframe(info, status)

    assert len(df) == 1
    assert "station_id" in df.columns
    assert "num_bikes_available" in df.columns
    assert "name" in df.columns
