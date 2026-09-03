"""Volant native protocol client (sync TCP MVP)."""

from .assignor import range_assign, range_assign_multi
from .client import (
    Client,
    DeleteRecordsResult,
    DescribeConfigsResult,
    DescribeGroupResult,
    FetchResult,
    JoinGroupResult,
    ProduceResult,
)
from .codec import (
    AclBinding,
    Assignment,
    BrokerError,
    BrokerInfo,
    FetchRecord,
    GroupListing,
    GroupMemberInfo,
    GroupState,
    MembershipBroker,
    MembershipList,
    MetadataResponse,
    OffsetCommitEntry,
    OffsetEntry,
    OffsetFetchEntry,
    OffsetListing,
    PartitionInfo,
    ProduceMessage,
    TopicInfo,
    TxnOffsetCommit,
    TxnProduceResult,
)
from .frame import ProtocolError
from .group import FetchedRecord, GroupConsumer
from .txn import TransactionalProducer

__version__ = "0.2.0"

__all__ = [
    "AclBinding",
    "Assignment",
    "BrokerError",
    "BrokerInfo",
    "Client",
    "DeleteRecordsResult",
    "DescribeConfigsResult",
    "DescribeGroupResult",
    "FetchedRecord",
    "FetchRecord",
    "FetchResult",
    "GroupConsumer",
    "GroupListing",
    "GroupMemberInfo",
    "GroupState",
    "JoinGroupResult",
    "MembershipBroker",
    "MembershipList",
    "MetadataResponse",
    "OffsetCommitEntry",
    "OffsetEntry",
    "OffsetFetchEntry",
    "OffsetListing",
    "PartitionInfo",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "TransactionalProducer",
    "TxnOffsetCommit",
    "TxnProduceResult",
    "range_assign",
    "range_assign_multi",
    "__version__",
]
