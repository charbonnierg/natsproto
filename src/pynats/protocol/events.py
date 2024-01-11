from __future__ import annotations

import abc
import enum
from typing import Generic, Literal, NoReturn, TypeVar, Union

from .payload import Error, Info, Message

T = TypeVar("T")


class EventType(str, enum.Enum):
    """Type of event."""

    CONNECT_REQUEST = "CONNECT_REQUEST"
    """The first INFO received from the server."""

    CONNECTED = "CONNECTED"
    """The first PONG received from the server."""

    PONG = "PONG"
    """A PONG response from the server."""

    MSG = "MSG"
    """A message received from the server."""

    ERROR = "ERROR"
    """An error received from the server."""

    INFO = "INFO"
    """An INFO received from the server."""

    CLOSED = "CLOSED"
    """The connection is closed."""


class BaseEvent(Generic[T], metaclass=abc.ABCMeta):
    type: EventType

    body: T

    def __repr__(self) -> str:
        return f"<nats protocol event={self.type}>"


class PongEvent(BaseEvent[NoReturn]):
    """A PONG event."""

    __slots__ = []

    type: Literal[EventType.PONG] = EventType.PONG


class ConnectionRequestEvent(BaseEvent[Info]):
    """A connection request event.

    Args:
        info: The INFO received from the server.
    """

    __slots__ = ["body"]

    type: Literal[EventType.CONNECT_REQUEST] = EventType.CONNECT_REQUEST

    def __init__(self, info: Info) -> None:
        self.body = info


class ConnectedEvent(BaseEvent[NoReturn]):
    """Marker for a connected event.

    A connected event does not exist in NATS protocol,
    instead, the first PONG event is used as a marker
    for a successful connection. This event is used
    to notify the user that the connection is successful,
    rather than let user distinguish between PONG and
    CONNECTED events.
    """

    __slots__ = []

    type: Literal[EventType.CONNECTED] = EventType.CONNECTED


class ErrorEvent(BaseEvent[Error]):
    """An error event.

    Args:
        error: The ERROR received from the server.
    """

    __slots__ = ["body"]

    type: Literal[EventType.ERROR] = EventType.ERROR

    def __init__(self, error: Error) -> None:
        self.body = error


class InfoEvent(BaseEvent[Info]):
    """An info event.

    Args:
        info: The INFO received from the server.
    """

    __slots__ = ["body"]

    type: Literal[EventType.INFO] = EventType.INFO

    def __init__(self, info: Info) -> None:
        self.body = info


class MsgEvent(BaseEvent[Message]):
    """A message event.

    Args:
        msg: The MSG received from the server.
    """

    __slots__ = ["body"]

    type: Literal[EventType.MSG] = EventType.MSG

    def __init__(self, msg: Message) -> None:
        self.body = msg


class ClosedEvent(BaseEvent[NoReturn]):
    """Marker for a closed event.

    A closed event does not exist in NATS protocol,
    instead, when the `receive_eof_from_server` method
    is called, this event is used to notify the user
    that the connection is closed.
    """

    __slots__ = []

    type: Literal[EventType.CLOSED] = EventType.CLOSED


Event = Union[
    PongEvent,
    ErrorEvent,
    InfoEvent,
    MsgEvent,
    ConnectionRequestEvent,
    ConnectedEvent,
    ClosedEvent,
]

# Constant events

PONG_EVENT = PongEvent()
CONNECTED_EVENT = ConnectedEvent()
CLOSED_EVENT = ClosedEvent()
