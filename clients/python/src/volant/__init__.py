"""Volant native protocol client (sync TCP MVP)."""

from .assignor import range_assign, range_assign_multi
from .client import (
    Client,
    DeleteRecordsResult,
    DescribeGroupResult,
    FetchResult,
    JoinGroupResult,
    ProduceResult,
)
from .codec import (
    Assignment,
    BrokerError,
    BrokerInfo,
    FetchRecord,
    GroupListing,
    GroupMemberInfo,
    GroupState,
    MetadataResponse,
    OffsetListing,
    PartitionInfo,
    ProduceMessage,
    TopicInfo,
)
from .frame import ProtocolError
from .group import FetchedRecord, GroupConsumer

__version__ = "0.2.0"

__all__ = [
    "Assignment",
    "BrokerError",
    "BrokerInfo",
    "Client",
    "DeleteRecordsResult",
    "DescribeGroupResult",
    "FetchedRecord",
    "FetchRecord",
    "FetchResult",
    "GroupConsumer",
    "GroupListing",
    "GroupMemberInfo",
    "GroupState",
    "JoinGroupResult",
    "MetadataResponse",
    "OffsetListing",
    "PartitionInfo",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "range_assign",
    "range_assign_multi",
    "__version__",
]
