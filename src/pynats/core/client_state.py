from __future__ import annotations

from enum import Enum


class ClientState(str, Enum):
    """Client state."""

    CREATED = "CREATED"
    CONNECTED = "CONNECTED"
    CONNECTING = "CONNECTING"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class ClientStateMixin:
    def __init__(self) -> None:
        self.status = ClientState.CREATED

    def is_closed(self) -> bool:
        """Return True if client is closed."""
        return self.status == ClientState.CLOSED

    def is_closing(self) -> bool:
        """Return True if client is closing."""
        return self.status == ClientState.CLOSING

    def is_cancelled(self) -> bool:
        """Return True if client is cancelled."""
        return self.status == ClientState.CLOSED or self.status == ClientState.CLOSING

    def is_connected(self) -> bool:
        """Return True if the client is connected."""
        return self.status == ClientState.CONNECTED

    def is_disconnected(self) -> bool:
        """Return True if the client is disconnected."""
        return self.status == ClientState.DISCONNECTED

    def is_connecting(self) -> bool:
        """Return True if the client is connecting."""
        return self.status == ClientState.CONNECTING

    def is_reconnecting(self) -> bool:
        """Return True if the client is reconnecting."""
        return self.status == ClientState.RECONNECTING

    def _reset(self) -> None:
        """Reset the client state."""
        self.status = ClientState.CREATED
