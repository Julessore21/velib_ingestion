from __future__ import annotations

from pathlib import Path

import pandas as pd


class ExportError(ValueError):
    pass


def export_dataframe(df: pd.DataFrame, path: Path, fmt: str = "csv") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = fmt.lower()

    if fmt == "csv":
        df.to_csv(path, index=False)
        return

    if fmt == "parquet":
        try:
            df.to_parquet(path, index=False)
        except ImportError as exc:
            raise ExportError("Parquet export requires pyarrow") from exc
        return

    raise ExportError(f"Unsupported format: {fmt}")
