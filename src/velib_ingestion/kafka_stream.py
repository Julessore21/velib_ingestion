from __future__ import annotations

import json
import time
from typing import Any, Callable

from kafka import KafkaConsumer, KafkaProducer

from .client import VelibClient
from .config import Settings, get_settings
from .logger import get_logger
from .validator import validate_station_information, validate_station_status


def _json_serializer(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _json_deserializer(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8"))


class VelibKafkaProducer:
    def __init__(
        self,
        settings: Settings | None = None,
        client: VelibClient | None = None,
        producer: KafkaProducer | None = None,
        logger=None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or VelibClient(settings=self.settings)
        self.logger = logger or get_logger(__name__)
        self.producer = producer or KafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
            client_id=self.settings.kafka_client_id,
            value_serializer=_json_serializer,
            key_serializer=lambda v: v.encode("utf-8"),
            acks="all",
            enable_idempotence=self.settings.kafka_enable_idempotence,
        )

    def publish_snapshot(self, validate: bool = True) -> dict[str, int]:
        station_information = self.client.get_station_information(validate=False)
        station_status = self.client.get_station_status(validate=False)

        if validate:
            validate_station_information(station_information, strict=False)
            validate_station_status(station_status, strict=False)

        info_count = self._publish(
            topic=self.settings.kafka_topic_information,
            payload=station_information,
            key="station_information",
        )
        status_count = self._publish(
            topic=self.settings.kafka_topic_status,
            payload=station_status,
            key="station_status",
        )
        return {"station_information": info_count, "station_status": status_count}

    def publish_forever(self, validate: bool = True) -> None:
        self.logger.info(
            "Starting Kafka producer loop with interval=%ss on %s",
            self.settings.poll_interval_seconds,
            self.settings.kafka_bootstrap_servers,
        )
        while True:
            counts = self.publish_snapshot(validate=validate)
            self.logger.info(
                "Published snapshot info=%s status=%s",
                counts["station_information"],
                counts["station_status"],
            )
            time.sleep(self.settings.poll_interval_seconds)

    def _publish(self, topic: str, payload: dict[str, Any], key: str) -> int:
        stations = payload.get("data", {}).get("stations", [])
        future = self.producer.send(topic, key=key, value=payload)
        future.get(timeout=self.settings.timeout_seconds)
        self.producer.flush(timeout=self.settings.timeout_seconds)
        self.logger.info("Published %s stations to topic=%s", len(stations), topic)
        return len(stations)

    def close(self) -> None:
        self.client.close()
        self.producer.close()

    def __enter__(self) -> "VelibKafkaProducer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class VelibKafkaConsumer:
    def __init__(
        self,
        topic: str,
        settings: Settings | None = None,
        consumer: KafkaConsumer | None = None,
        logger=None,
    ) -> None:
        self.settings = settings or get_settings()
        self.logger = logger or get_logger(__name__)
        self.topic = topic
        self.consumer = consumer or KafkaConsumer(
            self.topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
            group_id=self.settings.kafka_group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=_json_deserializer,
        )

    def consume_forever(
        self, callback: Callable[[dict[str, Any]], None] | None = None
    ) -> None:
        self.logger.info("Listening on Kafka topic=%s", self.topic)
        for message in self.consumer:
            payload = message.value
            if callback is not None:
                callback(payload)
            else:
                stations = payload.get("data", {}).get("stations", [])
                self.logger.info(
                    "Received topic=%s offset=%s stations=%s",
                    message.topic,
                    message.offset,
                    len(stations),
                )

    def close(self) -> None:
        self.consumer.close()

    def __enter__(self) -> "VelibKafkaConsumer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
