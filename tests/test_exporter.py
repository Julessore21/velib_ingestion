import pandas as pd

from velib_ingestion.exporter import export_dataframe


def test_export_dataframe_csv(tmp_path):
    df = pd.DataFrame([{"station_id": "123", "num_bikes_available": 2}])
    out = tmp_path / "out.csv"

    export_dataframe(df, out, fmt="csv")

    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert "station_id" in content
