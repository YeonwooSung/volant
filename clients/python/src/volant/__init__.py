"""Volant native protocol client (sync TCP MVP)."""

from .client import Client, FetchResult, JoinGroupResult, ProduceResult
from .codec import (
    Assignment,
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
    "Assignment",
    "BrokerError",
    "BrokerInfo",
    "Client",
    "FetchRecord",
    "FetchResult",
    "JoinGroupResult",
    "MetadataResponse",
    "PartitionInfo",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "__version__",
]
