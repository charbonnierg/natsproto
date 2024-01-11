from __future__ import annotations

import random
from itertools import cycle
from urllib.parse import urlparse

from .server import Server


class ServerPool:
    """Data structure representing a pool of NATS servers."""

    def __init__(
        self,
        servers: list[Server],
        randomized: bool,
        max_connect_attempts: int,
        max_reconnect_attempts: int,
    ) -> None:
        if max_connect_attempts == 0:
            raise ValueError(
                "max_connect_attempts must be greater than 0 or equal to -1"
            )
        self.max_connect_attempts = max_connect_attempts
        self.max_reconnect_attempts = max_reconnect_attempts
        self.randomized = randomized
        self.servers = {srv.uri.netloc: srv for srv in servers}
        self._update_generator()

    def empty(self) -> bool:
        """Check if the pool is empty.

        Returns:
            True if the pool is empty, else False
        """
        return not self.servers

    def next(self) -> Server | None:
        """Get a server from the pool.

        If the pool is randomized, a random server will be returned.
        Otherwise, the servers will be returned in sequence.

        Returns:
            The next server in the pool, or None if the pool is empty.
        """
        while True:
            # Check if the pool is empty
            if self.empty():
                return None
            # Get the next server
            if self.randomized:
                srv = self._next_randomized()
            else:
                srv = self._next_deterministic()
            if srv.did_connect:
                # Check if the server can attempt to reconnect
                if srv.can_attempt_reconnect(self.max_reconnect_attempts):
                    srv.increment_reconnect_attempts()
                    return srv
                else:
                    self.remove(srv)
            else:
                # Check if the server can attempt to connect
                if srv.can_attempt_connect(self.max_connect_attempts):
                    srv.increment_connect_attempts()
                    return srv
                else:
                    self.remove(srv)

    def remove(self, server: str | Server) -> None:
        """Remove a server from the pool.

        Args:
            server: The server to remove from the pool, either as a string or a Server instance.

        Returns:
            None, this function does not raise an exception if the server is not in the pool.
        """
        if isinstance(server, str):
            uri = urlparse(server)
        else:
            uri = server.uri
        self.servers.pop(uri.netloc, None)
        self._update_generator()

    def add(self, server: str | Server) -> Server:
        """Add a server to the pool.

        Args:
            server: The server to add to the pool, either as a string or a Server instance.

        Returns:
            Either the server from the pool if it already exists, or the server that was added.
        """
        if isinstance(server, str):
            uri = urlparse(server)
            server = Server(uri)
        else:
            uri = server.uri
        if uri.netloc in self.servers:
            return self.servers[uri.netloc]
        self.servers[uri.netloc] = server
        self._update_generator()
        return server

    def contains(self, server: str | Server) -> Server | None:
        """Check if the pool contains a server.

        Args:
            server: The server to check for, either as a string or a Server instance.

        Returns:
            The server from the pool if it exists, else None.
        """
        if isinstance(server, str):
            uri = urlparse(server)
        else:
            uri = server.uri
        return self.servers.get(uri.netloc)

    def _update_generator(self) -> None:
        self._generator = cycle(self.servers)

    def _next_deterministic(self) -> Server:
        key = next(self._generator)
        return self.servers[key]

    def _next_randomized(self) -> Server:
        key = random.choice(list(self.servers))
        return self.servers[key]

    @classmethod
    def from_urls(
        cls,
        urls: list[str],
        randomized: bool = True,
        max_connect_attempts: int = 3,
        max_reconnect_attempts: int = 60,
    ) -> ServerPool:
        """Create a new server pool from a list of URLs.

        Args:
            urls: The URLs to add to the pool.
            randomized: Whether the pool should be randomized.
            max_connect_attempts: The max connect attempts for each server.
                If -1, the server will never be removed from the pool.
                O is not allowed.
            max_reconnect_attempts: The max reconnect attempts for each server.
                If -1, the server will never be removed from the pool.
                If 0, the server will be removed from the pool after the first failed reconnection attempt.

        Returns:
            A new server pool.
        """
        return ServerPool(
            [Server(urlparse(server)) for server in urls],
            randomized=randomized,
            max_connect_attempts=max_connect_attempts,
            max_reconnect_attempts=max_reconnect_attempts,
        )
