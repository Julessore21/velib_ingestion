from velib_ingestion.config import Settings
from velib_ingestion.kafka_stream import VelibKafkaProducer


class DummyFuture:
    def get(self, timeout):
        return None


class DummyProducer:
    def __init__(self):
        self.messages = []

    def send(self, topic, key, value):
        self.messages.append({"topic": topic, "key": key, "value": value})
        return DummyFuture()

    def flush(self, timeout=None):
        return None

    def close(self):
        return None


class DummyClient:
    def get_station_information(self, validate=False):
        return {
            "data": {
                "stations": [
                    {"station_id": "1", "name": "A", "lat": 48.1, "lon": 2.3},
                ]
            }
        }

    def get_station_status(self, validate=False):
        return {
            "data": {
                "stations": [
                    {"station_id": "1", "num_bikes_available": 4, "num_docks_available": 8},
                ]
            }
        }

    def close(self):
        return None


def test_publish_snapshot_sends_two_messages():
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
        poll_interval_seconds=60,
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic_status="status-topic",
        kafka_topic_information="info-topic",
        kafka_client_id="test-producer",
        kafka_group_id="test-group",
        kafka_enable_idempotence=True,
    )

    producer = DummyProducer()
    client = DummyClient()
    stream = VelibKafkaProducer(settings=settings, client=client, producer=producer)

    counts = stream.publish_snapshot(validate=True)

    assert counts == {"station_information": 1, "station_status": 1}
    assert len(producer.messages) == 2
    assert producer.messages[0]["topic"] == "info-topic"
    assert producer.messages[1]["topic"] == "status-topic"
