from __future__ import annotations

import enum
from typing import NoReturn


class ErrorType(str, enum.Enum):
    UNKNOWN = ""
    UNKNOWN_PROTOCOL_OPERATION = "unknown protocol operation"
    ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT = "attempted to connect to route port"
    AUTHORIZATION_VIOLATION = "authorization violation"
    AUTHORIZATION_TIMEOUT = "authorization timeout"
    INVALID_CLIENT_PROTOCOL = "invalid client protocol"
    MAXIMUM_CONTROL_LINE_EXCEEDED = "maximum control line exceeded"
    PARSER_ERROR = "parser error"
    SECURE_CONNECTION_TLS_REQUIRED = "secure connection - tls required"
    STALE_CONNECTION = "stale connection"
    MAXIMUM_CONNECTIONS_EXCEEDED = "maximum connections exceeded"
    SLOW_CONSUMER = "slow consumer"
    MAXIMUM_PAYLOAD_VIOLATION = "maximum payload violation"
    INVALID_SUBJECT = "invalid subject"
    PERMISSION_VIOLATION_FOR_SUBSCRIPTION = "permissions violation for subscription to"
    PERMISSION_VIOLATION_FOR_PUBLISH = "permissions violation for publish to"


class Error:
    __slots__ = ["type", "message"]

    def __init__(self, type: ErrorType, message: str) -> None:
        self.type = type
        self.message = message

    def __repr__(self) -> str:
        return f"<nats server error={self.message}>"

    def is_recoverable(self) -> bool:
        """Returns True if the error is recoverable, e.g., client is allowed
        not to disconnect and can keep the connection opened."""
        return self.type in [
            ErrorType.PERMISSION_VIOLATION_FOR_SUBSCRIPTION,
            ErrorType.PERMISSION_VIOLATION_FOR_PUBLISH,
            ErrorType.INVALID_SUBJECT,
        ]

    def throw(self) -> NoReturn:
        """Raise an exception based on the error type."""
        if self.type == ErrorType.UNKNOWN_PROTOCOL_OPERATION:
            raise UnknownProtocolOperationError()
        elif self.type == ErrorType.ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT:
            raise AttemptedToConnectToRoutePortError()
        elif self.type == ErrorType.AUTHORIZATION_VIOLATION:
            raise AuthorizationViolationError()
        elif self.type == ErrorType.AUTHORIZATION_TIMEOUT:
            raise AuthorizationTimeoutError()
        elif self.type == ErrorType.INVALID_CLIENT_PROTOCOL:
            raise InvalidClientProtocolError()
        elif self.type == ErrorType.MAXIMUM_CONTROL_LINE_EXCEEDED:
            raise MaximumControlLineExceededError()
        elif self.type == ErrorType.PARSER_ERROR:
            raise ParserError()
        elif self.type == ErrorType.SECURE_CONNECTION_TLS_REQUIRED:
            raise SecureConnectionTLSRequiredError()
        elif self.type == ErrorType.STALE_CONNECTION:
            raise StaleConnectionError()
        elif self.type == ErrorType.MAXIMUM_CONNECTIONS_EXCEEDED:
            raise MaximumConnectionsExceededError()
        elif self.type == ErrorType.SLOW_CONSUMER:
            raise SlowConsumerError()
        elif self.type == ErrorType.MAXIMUM_PAYLOAD_VIOLATION:
            raise MaximumPayloadViolationError()
        elif self.type == ErrorType.INVALID_SUBJECT:
            raise InvalidSubjectError()
        elif self.type == ErrorType.PERMISSION_VIOLATION_FOR_SUBSCRIPTION:
            raise PermissionViolationForSubscriptionError(self.message)
        elif self.type == ErrorType.PERMISSION_VIOLATION_FOR_PUBLISH:
            raise PermissionViolationForPublishError(self.message)
        else:
            raise UnknownServerError()


class ServerError(Exception):
    """Base class for server errors."""

    def __init__(
        self,
        typ: ErrorType,
        msg: str | None = None,
    ) -> None:
        self.type = typ
        self.message = msg or typ.value
        super().__init__(self.message)


class UnrecoverableServerError(ServerError):
    pass


class RecoverableServerError(ServerError):
    pass


class UnknownServerError(UnrecoverableServerError):
    """Unknown error."""

    def __init__(self) -> None:
        super().__init__(ErrorType.UNKNOWN)


class UnknownProtocolOperationError(UnrecoverableServerError):
    """Unknown protocol error."""

    def __init__(self) -> None:
        super().__init__(ErrorType.UNKNOWN_PROTOCOL_OPERATION)


class AttemptedToConnectToRoutePortError(UnrecoverableServerError):
    """Client attempted to connect to a route port instead of the client port."""

    def __init__(self) -> None:
        super().__init__(ErrorType.ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT)


class AuthorizationViolationError(UnrecoverableServerError):
    """Client failed to authenticate to the server with credentials specified in the CONNECT message."""

    def __init__(self) -> None:
        super().__init__(ErrorType.AUTHORIZATION_VIOLATION)


class AuthorizationTimeoutError(UnrecoverableServerError):
    """Client took too long to authenticate to the server after establishing a connection (default 1 second)."""

    def __init__(self) -> None:
        super().__init__(ErrorType.AUTHORIZATION_TIMEOUT)


class InvalidClientProtocolError(UnrecoverableServerError):
    """Client specified an invalid protocol version in the CONNECT message."""

    def __init__(self) -> None:
        super().__init__(ErrorType.INVALID_CLIENT_PROTOCOL)


class MaximumControlLineExceededError(UnrecoverableServerError):
    """Message destination subject and reply subject length exceeded the maximum control line
    value specified by the max_control_line server option. The default is 1024 bytes."""

    def __init__(self) -> None:
        super().__init__(ErrorType.MAXIMUM_CONTROL_LINE_EXCEEDED)


class ParserError(UnrecoverableServerError):
    """Cannot parse the protocol message sent by the client."""

    def __init__(self) -> None:
        super().__init__(ErrorType.PARSER_ERROR)


class SecureConnectionTLSRequiredError(UnrecoverableServerError):
    """The server requires TLS and the client does not have TLS enabled."""

    def __init__(self) -> None:
        super().__init__(ErrorType.SECURE_CONNECTION_TLS_REQUIRED)


class StaleConnectionError(UnrecoverableServerError):
    """The server hasn't received a message from the client, including a PONG in too long."""

    def __init__(self) -> None:
        super().__init__(ErrorType.STALE_CONNECTION)


class MaximumConnectionsExceededError(UnrecoverableServerError):
    """This error is sent by the server when creating a new connection and the server has exceeded
    the maximum number of connections specified by the max_connections server option.
    The default is 64k."""

    def __init__(self) -> None:
        super().__init__(ErrorType.MAXIMUM_CONNECTIONS_EXCEEDED)


class SlowConsumerError(UnrecoverableServerError):
    """The server pending data size for the connection has reached the maximum size (default 10MB)."""

    def __init__(self) -> None:
        super().__init__(ErrorType.SLOW_CONSUMER)


class MaximumPayloadViolationError(UnrecoverableServerError):
    """Client attempted to publish a message with a payload size that exceeds the max_payload size configured
    on the server. This value is supplied to the client upon connection in the initial INFO message.
    The client is expected to do proper accounting of byte size to be sent to the server in order to
    handle this error synchronously."""

    def __init__(self) -> None:
        super().__init__(ErrorType.MAXIMUM_PAYLOAD_VIOLATION)


class InvalidSubjectError(RecoverableServerError):
    """Client sent a malformed subject (e.g. SUB foo. 90)."""

    def __init__(self) -> None:
        super().__init__(ErrorType.INVALID_SUBJECT)


class PermissionViolationForSubscriptionError(RecoverableServerError):
    """The user specified in the CONNECT message does not have permission to subscribe to the subject."""

    def __init__(self, msg: str) -> None:
        super().__init__(ErrorType.PERMISSION_VIOLATION_FOR_SUBSCRIPTION, msg)


class PermissionViolationForPublishError(RecoverableServerError):
    """The user specified in the CONNECT message does not have permissions to publish to the subject."""

    def __init__(self, msg: str) -> None:
        super().__init__(ErrorType.PERMISSION_VIOLATION_FOR_PUBLISH, msg)
