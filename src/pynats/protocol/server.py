from __future__ import annotations

from urllib.parse import ParseResult, urlparse

from .payload import Info


class Server:
    """Data structure representing a NATS server."""

    def __init__(
        self,
        uri: ParseResult,
    ) -> None:
        """Create a new instance of the Server class.

        Args:
            uri: The URI of the server as obtained by `urlparse()`.
        """
        self.uri = uri
        self.connect_attempts = 0
        self.reconnect_attempts = 0
        self.did_connect = False
        self.info: Info | None = None

    def can_attempt_connect(self, limit: int) -> bool:
        """Check if the server can attempt to connect.

        Returns:
            True if the server has not reached the max connect attempts, else False
        """
        if limit == 0:
            raise ValueError(
                "max_connect_attempts must be greater than 0 or equal to -1"
            )
        if limit < 0:
            return True
        return self.connect_attempts < limit

    def can_attempt_reconnect(self, limit: int) -> bool:
        """Check if the server can attempt to reconnect.

        Returns:
            True if the server has not reached the max reconnect attempts, else False
        """
        if limit < 0:
            return True
        if limit == 0:
            return False
        return limit > self.reconnect_attempts

    def increment_connect_attempts(self) -> None:
        self.connect_attempts += 1

    def increment_reconnect_attempts(self) -> None:
        self.reconnect_attempts += 1

    def observe_connect(self) -> None:
        self.did_connect = True

    def set_info(self, info: Info) -> None:
        self.info = info

    @classmethod
    def new(cls, url: str) -> Server:
        """Create a new instance of the Server class.

        Args:
            url: The URL of the server.

        Returns:
            A new instance of the Server class.
        """
        return cls(uri=urlparse(url))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Server):
            return NotImplemented
        return self.uri == other.uri
