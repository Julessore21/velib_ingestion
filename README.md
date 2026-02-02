# Velib Ingestion

Module d'ingestion et de transformation pour l'API open data Velib' Metropole (GBFS). Concu pour etre autonome, testable et facilement integrable dans un projet d'orchestration plus large.

Fonctionnalites principales
- Configuration via `.env` et variables d'environnement
- Client HTTP avec retry/backoff exponentiel
- Validation legere du JSON (controles de structure et types cles)
- Transformation en DataFrame pandas et jointure `station_information` + `station_status`
- Export CSV ou parquet (optionnel)
- Logging des appels API
- CLI simple via `python -m velib_ingestion`

Prerequis
- Python 3.10 ou plus

Installation
1. `pip install -r requirements.txt`
2. Copier `.env.example` en `.env` et ajuster si besoin
3. (Optionnel) Parquet: `pip install -e .[parquet]`

Utilisation rapide
```python
from pathlib import Path

from velib_ingestion.client import VelibClient
from velib_ingestion.validator import validate_station_information, validate_station_status
from velib_ingestion.transformer import stations_to_dataframe
from velib_ingestion.exporter import export_dataframe

client = VelibClient()
info = client.get_station_information()
status = client.get_station_status()

validate_station_information(info)
validate_station_status(status)

df = stations_to_dataframe(info, status)
export_dataframe(df, Path("data/velib.csv"))
```

CLI
```
python -m velib_ingestion --validate --output data/velib.csv --format csv
```

Structure
- `src/velib_ingestion/config.py` : lecture des settings
- `src/velib_ingestion/logger.py` : configuration du logging
- `src/velib_ingestion/client.py` : client HTTP et retries
- `src/velib_ingestion/validator.py` : validation JSON
- `src/velib_ingestion/transformer.py` : transformation DataFrame
- `src/velib_ingestion/exporter.py` : export CSV/parquet
- `src/velib_ingestion/cli.py` : CLI

Tests
- `pytest -q`

Integration dans un projet plus large
- Le package est installable et importable via `pyproject.toml`
- Les settings peuvent etre surcharges par variables d'environnement
- Le client accepte un `session` et un `settings` injectables

