from __future__ import annotations


class ProtocolError(Exception):
    """Base class for protocol errors."""

    pass


# Parser errors


class ProtocolParserError(ProtocolError):
    """Raised when a protocol parser error occurs."""

    pass


class InvalidProtocolMessageError(ProtocolParserError):
    """Raised when an invalid protocol message is received."""

    pass


class UnexpectedProtocolMessageError(ProtocolParserError):
    """Raised when an unexpected protocol message is received."""

    pass


# Connection errors


class ConnectionError(ProtocolError):
    """Raised when a protocol connection error occurs."""

    pass


class ConnectionStateTransitionError(ConnectionError):
    """Raised when an invalid connection state transition is attempted."""

    pass


class ConnectionNotEstablishedError(ConnectionError):
    """Raised when the connection is not established."""

    pass


class ConnectionAlreadyEstablishedError(ConnectionError):
    """Raised when the connection is alread established."""

    pass


class ConnectionClosedError(ConnectionError):
    """Raised when the connection is closed."""

    pass


class ConnectionClosingError(ConnectionError):
    """Raised when the connection is closing."""

    pass


class ConnectionServerPoolEmpty(ConnectionClosedError):
    """Raised when the connection server pool is empty."""

    pass
