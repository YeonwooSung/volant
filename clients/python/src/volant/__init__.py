"""Volant native protocol client (sync TCP MVP)."""

from .client import Client, FetchResult, ProduceResult
from .codec import (
    BrokerError,
    BrokerInfo,
    FetchRecord,
    MetadataResponse,
    PartitionInfo,
    ProduceMessage,
    TopicInfo,
)
from .frame import ProtocolError

__version__ = "0.2.0"

__all__ = [
    "BrokerError",
    "BrokerInfo",
    "Client",
    "FetchRecord",
    "FetchResult",
    "MetadataResponse",
    "PartitionInfo",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "__version__",
]
