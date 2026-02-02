from __future__ import annotations

from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

DEFAULT_GBFS_URL = (
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json"
)
DEFAULT_STATION_STATUS_URL = (
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
)
DEFAULT_STATION_INFORMATION_URL = (
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/"
    "station_information.json"
)
DEFAULT_SYSTEM_INFORMATION_URL = (
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/system_information.json"
)


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    gbfs_url: str = DEFAULT_GBFS_URL
    station_status_url: str = DEFAULT_STATION_STATUS_URL
    station_information_url: str = DEFAULT_STATION_INFORMATION_URL
    system_information_url: str = DEFAULT_SYSTEM_INFORMATION_URL
    timeout_seconds: int = 10
    retry_attempts: int = 3
    retry_backoff_seconds: int = 1
    log_level: str = "INFO"
    user_agent: str = "velib-ingestion/0.1.0"


def build_settings() -> Settings:
    return Settings(
        gbfs_url=os.getenv("VELIB_GBFS_URL", DEFAULT_GBFS_URL),
        station_status_url=os.getenv("VELIB_STATION_STATUS_URL", DEFAULT_STATION_STATUS_URL),
        station_information_url=os.getenv(
            "VELIB_STATION_INFORMATION_URL", DEFAULT_STATION_INFORMATION_URL
        ),
        system_information_url=os.getenv(
            "VELIB_SYSTEM_INFORMATION_URL", DEFAULT_SYSTEM_INFORMATION_URL
        ),
        timeout_seconds=_get_int("VELIB_TIMEOUT_SECONDS", 10),
        retry_attempts=_get_int("VELIB_RETRY_ATTEMPTS", 3),
        retry_backoff_seconds=_get_int("VELIB_RETRY_BACKOFF_SECONDS", 1),
        log_level=os.getenv("VELIB_LOG_LEVEL", "INFO"),
        user_agent=os.getenv("VELIB_USER_AGENT", "velib-ingestion/0.1.0"),
    )


_SETTINGS: Settings | None = None


def get_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = build_settings()
    return _SETTINGS
