from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .aio.transport.abc import Transport as AIOTransport
    from .core.subscription import Subscription
    from .io.transport.abc import Transport as IOTransport


class NatsError(Exception):
    """Base class for all exceptions raised by this library."""

    pass


# Transport errors


class TransportError(Exception):
    def __init__(
        self,
        msg: str,
        transport: IOTransport | AIOTransport,
    ) -> None:
        self.msg = msg
        self.transport = transport
        self.transport_type = type(transport).__name__
        super().__init__(msg)


class TransportNotConnectedError(TransportError):
    """Error to be raised when trying to access the transport
    when it is not opened."""

    def __init__(self, transport: IOTransport | AIOTransport) -> None:
        super().__init__("Transport not connected", transport)


class TransportTLSUpgradeError(TransportError):
    """Error to be raised when TLS upgrade fails."""

    def __init__(self, transport: IOTransport | AIOTransport) -> None:
        super().__init__(f"TLS upgrade failed for {transport.uri()}", transport)


# Connection errors


class ConnectionError(NatsError):
    """Base class for all exceptions raised by the connection."""

    pass


class ConnectionClosedError(ConnectionError):
    """Error raised when the connection is closed."""

    pass


class ConnectionClosingError(ConnectionError):
    """Error raised when the connection is being closed."""

    pass


class ConnectionStaleError(ConnectionError):
    """Error raised when the connection is stale."""

    pass


# Client errors


class NatsClientError(NatsError):
    """Base class for all exceptions raised by the client."""

    pass


class BadCallbackTypeError(NatsClientError):
    """Error raised when the callback type is invalid."""

    pass


class SubscriptionNotStartedError(NatsClientError):
    """Error raised when the subscription is not started."""

    pass


class SubscriptionClosedError(NatsClientError):
    """Error raised when the subscription is closed."""

    pass


class SubscriptionSlowConsumerError(NatsClientError):
    """Error raised when the subscription is a slow consumer."""

    def __init__(self, sub: Subscription) -> None:
        self.sub = sub
        super().__init__(f"subscription {sub.sid()} is a slow consumer")


class MsgAlreadyAckdError(NatsClientError):
    pass


class NotJetStreamMsgError(NatsClientError):
    pass


class NoReplySubjectError(NatsClientError):
    pass
