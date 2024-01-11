from __future__ import annotations

import datetime
from enum import Enum


class ControlType(str, Enum):
    """Type of a control message."""

    HEARTBEAT = "Idle"
    FLOW = "Flow"
    NONE = ""


class Sequence:
    __slots__ = ["stream", "consumer"]

    def __init__(self, stream: int, consumer: int) -> None:
        self.stream = stream
        self.consumer = consumer


class Metadata:
    __slots__ = [
        "stream",
        "consumer",
        "sequence",
        "num_pending",
        "num_delivered",
        "timestamp",
        "domain",
    ]

    def __init__(
        self,
        stream: str,
        consumer: str,
        stream_sequence: int,
        consumer_sequence: int,
        num_pending: int,
        num_delivered: int,
        timestamp: datetime.datetime,
        domain: str | None,
    ) -> None:
        self.stream = stream
        self.consumer = consumer
        self.sequence = Sequence(stream_sequence, consumer_sequence)
        self.num_pending = num_pending
        self.num_delivered = num_delivered
        self.timestamp = timestamp
        self.domain = domain
