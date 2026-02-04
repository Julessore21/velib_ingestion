"""Velib ingestion package."""

from .client import VelibClient
from .exporter import export_dataframe
from .kafka_stream import VelibKafkaConsumer, VelibKafkaProducer
from .transformer import stations_to_dataframe
from .validator import ValidationError, validate_station_information, validate_station_status

__all__ = [
    "VelibClient",
    "export_dataframe",
    "VelibKafkaConsumer",
    "VelibKafkaProducer",
    "stations_to_dataframe",
    "ValidationError",
    "validate_station_information",
    "validate_station_status",
]

__version__ = "0.1.0"
