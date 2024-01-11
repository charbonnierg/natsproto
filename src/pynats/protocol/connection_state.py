from __future__ import annotations

from enum import IntEnum

from pynats.protocol.errors import ConnectionStateTransitionError


class ConnectionState(IntEnum):
    """Connection state."""

    WAITING_FOR_SERVER_SELECTION = 0
    """Transport closed, connection not established."""

    WAITING_FOR_SERVER_INFO = 1
    """Transport opened, connection not established."""

    WAITING_FOR_CLIENT_CONNECT = 2
    """Server info received, client must sent CONNECT."""

    WAITING_FOR_CLIENT_PING = 3
    """Client sent CONNECT, client must send PING."""

    WAITING_FOR_SERVER_PONG = 4
    """Client sent CONNECT and PING, waiting for PONG from server."""

    CONNECTED = 5
    """Received PONG after CONNECT."""

    CLOSING = 6
    """Connection is closing. PONG, PUB and UNSUB are still allowed,
    but SUB and PING raises an error."""

    CLOSED = 7
    """Connection is closed. All operations except raise an error."""


class ConnectionStateMixin:
    """Connection state mixin exposes methods to check the connection state."""

    def __init__(self, state: ConnectionState | None = None) -> None:
        self.status = state or ConnectionState.WAITING_FOR_SERVER_SELECTION

    def _reset(self) -> None:
        """Reset the connection statet."""
        self.status = ConnectionState.WAITING_FOR_SERVER_SELECTION

    def is_closed(self) -> bool:
        """Check if the connection is closed.

        Returns:
            True if the connection is closed, False otherwise.
        """
        return self.status == ConnectionState.CLOSED

    def is_closing(self) -> bool:
        """Check if the connection is closing.

        Returns:
            True if the connection is closing, False otherwise.
        """
        return self.status == ConnectionState.CLOSING

    def is_cancelled(self) -> bool:
        """Check if the connection is cancelled.

        A connection is cancelled if it is closed or closing.

        Returns:
            True if the connection is cancelled, False otherwise.
        """
        return (
            self.status == ConnectionState.CLOSING
            or self.status == ConnectionState.CLOSED
        )

    def is_connected(self) -> bool:
        """Check if the connection is connected.

        Returns:
            True if the connection is connected, False otherwise.
        """
        return self.status == ConnectionState.CONNECTED

    def is_connecting(self) -> bool:
        """Check if the connection is connecting.

        Returns:
            True if the connection is connecting, False otherwise.
        """
        return (
            self.status == ConnectionState.WAITING_FOR_SERVER_INFO
            or self.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT
            or self.status == ConnectionState.WAITING_FOR_CLIENT_PING
            or self.status == ConnectionState.WAITING_FOR_SERVER_PONG
        )

    def is_waiting_for_server_info(self) -> bool:
        """Check if the connection is waiting for server INFO.

        Returns:
            True if the connection is waiting for server INFO, False otherwise.
        """
        return self.status == ConnectionState.WAITING_FOR_SERVER_INFO

    def is_waiting_for_client_connect(self) -> bool:
        """Check if the connection is waiting for client CONNECT.

        Returns:
            True if the connection is waiting for client CONNECT, False otherwise.
        """
        return self.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT

    def is_waiting_for_client_ping(self) -> bool:
        """Check if the connection is waiting for client PING.

        Returns:
            True if the connection is waiting for client PING, False otherwise.
        """
        return self.status == ConnectionState.WAITING_FOR_CLIENT_PING

    def is_waiting_for_server_pong(self) -> bool:
        """Check if the connection is waiting for server PONG.

        Returns:
            True if the connection is waiting for server PONG, False otherwise.
        """
        return self.status == ConnectionState.WAITING_FOR_SERVER_PONG

    def is_waiting_for_server_selection(self) -> bool:
        """Check if the connection is waiting for server selection.

        Returns:
            True if the connection is waiting for server selection, False otherwise.
        """
        return self.status == ConnectionState.WAITING_FOR_SERVER_SELECTION

    def did_not_expect_pong(self) -> bool:
        """Check if PONG control line was not expected.

        Returns:
            True if PONG control line was not expected, False otherwise.
        """

        return (
            self.status == ConnectionState.WAITING_FOR_SERVER_SELECTION
            or self.status == ConnectionState.WAITING_FOR_SERVER_INFO
            or self.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT
            or self.status == ConnectionState.WAITING_FOR_CLIENT_PING
            or self.status == ConnectionState.CLOSED
        )

    def did_not_expect_msg(self) -> bool:
        """Check if MSG control line was not expected.

        Returns:
            True if MSG control line was not expected, False otherwise.
        """

        return (
            self.status == ConnectionState.WAITING_FOR_SERVER_SELECTION
            or self.status == ConnectionState.WAITING_FOR_SERVER_INFO
            or self.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT
            or self.status == ConnectionState.WAITING_FOR_CLIENT_PING
            or self.status == ConnectionState.WAITING_FOR_SERVER_PONG
            or self.status == ConnectionState.CLOSED
        )

    def did_not_expect_err(self) -> bool:
        """Check if ERR control line was not expected.

        Returns:
            True if ERR control line was not expected, False otherwise.
        """

        return (
            self.status == ConnectionState.WAITING_FOR_SERVER_SELECTION
            or self.status == ConnectionState.WAITING_FOR_SERVER_INFO
            or self.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT
            or self.status == ConnectionState.WAITING_FOR_CLIENT_PING
            or self.status == ConnectionState.CLOSED
        )

    def mark_as_waiting_for_server_selection(self) -> None:
        """Mark the connection as waiting for server selection."""
        if self.status == ConnectionState.CLOSED:
            self.status = ConnectionState.WAITING_FOR_SERVER_SELECTION
            return
        raise ConnectionStateTransitionError

    def mark_as_waiting_for_server_info(self) -> None:
        """Mark the connection as waiting for server INFO."""
        if self.status == ConnectionState.WAITING_FOR_SERVER_SELECTION:
            self.status = ConnectionState.WAITING_FOR_SERVER_INFO
            return
        raise ConnectionStateTransitionError

    def mark_as_waiting_for_client_connect(self) -> None:
        """Mark the connection as waiting for client CONNECT."""
        if self.status == ConnectionState.WAITING_FOR_SERVER_INFO:
            self.status = ConnectionState.WAITING_FOR_CLIENT_CONNECT
            return
        raise ConnectionStateTransitionError

    def mark_as_waiting_for_client_ping(self) -> None:
        """Mark the connection as waiting for client PING."""
        if self.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT:
            self.status = ConnectionState.WAITING_FOR_CLIENT_PING
            return
        raise ConnectionStateTransitionError

    def mark_as_waiting_for_server_pong(self) -> None:
        """Mark the connection as waiting for server PONG."""
        if self.status == ConnectionState.WAITING_FOR_CLIENT_PING:
            self.status = ConnectionState.WAITING_FOR_SERVER_PONG
            return
        raise ConnectionStateTransitionError

    def mark_as_connected(self) -> None:
        """Mark the connection as connected."""
        if self.status == ConnectionState.WAITING_FOR_SERVER_PONG:
            self.status = ConnectionState.CONNECTED
            return
        raise ConnectionStateTransitionError

    def mark_as_closing(self) -> None:
        """Mark the connection as closing."""
        if (
            self.status == ConnectionState.CLOSED
            or self.status == ConnectionState.CLOSING
        ):
            raise ConnectionStateTransitionError
        # Mark as closed if we're not connected yet
        self.status = ConnectionState.CLOSING

    def mark_as_closed(self) -> None:
        """Mark the connection as closed."""
        if self.status == ConnectionState.CLOSED:
            return
        if self.status == ConnectionState.CLOSING:
            self.status = ConnectionState.CLOSED
            return
        raise ConnectionStateTransitionError
