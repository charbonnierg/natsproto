from __future__ import annotations

import logging
import socket
import ssl
from contextlib import ExitStack
from urllib.parse import ParseResult

from ...errors import TransportNotConnectedError
from .abc import Transport

logger = logging.getLogger("nats.transport.asyncio_tcp")


class TcpTransport(Transport):
    def __init__(
        self,
        uri: ParseResult,
        buffer_size: int,
        tls_hostname: str | None = None,
        receive_timeout: float = 5.0,
    ):
        self._uri = uri
        self._url = uri.geturl()
        self._tls_hostname = tls_hostname or uri.hostname
        self._buffer_size = buffer_size
        self._stack = ExitStack()
        self._client: socket.socket | None = None
        self._bare_client: socket.socket | None = None
        self._connect_timeout = receive_timeout

    @property
    def client(self) -> socket.socket:
        """Returns the stream writer instance."""
        if self._client:
            return self._client
        raise TransportNotConnectedError(self)

    @property
    def tls_hostname(self) -> str | None:
        """Returns the hostname to use for TLS certificate validation."""
        if self._tls_hostname is not None:
            return self._tls_hostname
        return self._uri.hostname

    def uri(self) -> str:
        """Returns the uri used to connect to the server."""
        return self._url

    def connect(
        self,
    ):
        self._stack.__enter__()
        logger.debug("Opening TCP connection to %s", self._url)
        so = self._stack.enter_context(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        )
        so.settimeout(self._connect_timeout)
        so.connect((self._uri.hostname, self._uri.port))
        # We keep a reference to the initial transport we used when
        # establishing the connection in case we later upgrade to TLS
        # after getting the first INFO message. This is in order to
        # prevent the GC closing the socket after we send CONNECT
        # and replace the transport.
        #
        # See https://github.com/nats-io/asyncio-nats/issues/43
        self._bare_client = self._client = so

    def connect_tls(
        self,
        ssl_context: ssl.SSLContext,
    ) -> None:
        logger.debug("Upgrading TCP connection to %s to TLS", self._url)
        # manually recreate the stream reader/writer with a tls upgraded transport
        ssl_client = self._stack.enter_context(
            ssl_context.wrap_socket(
                self.client,
                server_hostname=self.tls_hostname,
            )
        )
        self._client = ssl_client

    def requires_tls_upgrade(self) -> bool:
        return True

    def write(self, payload: bytes) -> None:
        self.client.sendall(payload)

    def read(self, buffer_size: int) -> bytes:
        return self.client.recv(buffer_size)

    def close(self) -> None:
        self.client.shutdown(1)
        self._stack.close()
