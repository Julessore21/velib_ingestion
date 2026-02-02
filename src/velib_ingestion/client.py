from __future__ import annotations

import time
from typing import Iterable

import requests
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import Settings, get_settings
from .logger import get_logger
from .validator import validate_station_information, validate_station_status


class RetryableHTTPError(RuntimeError):
    def __init__(self, status_code: int, url: str):
        super().__init__(f"Retryable HTTP {status_code} for {url}")
        self.status_code = status_code
        self.url = url


class VelibClient:
    def __init__(
        self,
        settings: Settings | None = None,
        session: requests.Session | None = None,
        logger=None,
        retry_statuses: Iterable[int] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": self.settings.user_agent})
        self.logger = logger or get_logger(__name__)
        self.retry_statuses = set(retry_statuses or {429, 500, 502, 503, 504})

    def _get_json(self, url: str) -> dict:
        retrying = Retrying(
            stop=stop_after_attempt(self.settings.retry_attempts),
            wait=wait_exponential(multiplier=self.settings.retry_backoff_seconds),
            retry=retry_if_exception_type(
                (requests.ConnectionError, requests.Timeout, RetryableHTTPError)
            ),
            reraise=True,
        )

        for attempt in retrying:
            with attempt:
                return self._get_json_once(url)

        raise RuntimeError("Unreachable retry loop")

    def _get_json_once(self, url: str) -> dict:
        start = time.monotonic()
        response = self.session.get(url, timeout=self.settings.timeout_seconds)
        elapsed = time.monotonic() - start

        self.logger.info("GET %s %s in %.2fs", url, response.status_code, elapsed)

        if response.status_code in self.retry_statuses:
            raise RetryableHTTPError(response.status_code, url)
        if response.status_code >= 400:
            response.raise_for_status()

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(f"Invalid JSON response from {url}") from exc

    def get_gbfs(self) -> dict:
        return self._get_json(self.settings.gbfs_url)

    def get_system_information(self) -> dict:
        return self._get_json(self.settings.system_information_url)

    def get_station_information(self, validate: bool = False) -> dict:
        payload = self._get_json(self.settings.station_information_url)
        if validate:
            validate_station_information(payload, strict=False)
        return payload

    def get_station_status(self, validate: bool = False) -> dict:
        payload = self._get_json(self.settings.station_status_url)
        if validate:
            validate_station_status(payload, strict=False)
        return payload

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "VelibClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
