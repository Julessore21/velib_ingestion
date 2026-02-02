from unittest.mock import Mock

import requests

from velib_ingestion.client import VelibClient
from velib_ingestion.config import Settings


class DummyResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        raise requests.HTTPError("HTTP error")


def test_client_get_station_status():
    settings = Settings(
        gbfs_url="http://example/gbfs",
        station_status_url="http://example/status",
        station_information_url="http://example/info",
        system_information_url="http://example/system",
        timeout_seconds=5,
        retry_attempts=1,
        retry_backoff_seconds=0,
        log_level="INFO",
        user_agent="test-agent",
    )

    session = requests.Session()
    session.get = Mock(return_value=DummyResponse(200, {"data": {"stations": []}}))

    client = VelibClient(settings=settings, session=session)
    data = client.get_station_status()

    assert data == {"data": {"stations": []}}
    session.get.assert_called_once_with(settings.station_status_url, timeout=5)
