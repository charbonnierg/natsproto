from __future__ import annotations

import asyncio
import ssl
from urllib.parse import ParseResult

from ...errors import TransportNotConnectedError, TransportTLSUpgradeError
from .abc import Transport


class TcpTransport(Transport):
    def __init__(
        self,
        uri: ParseResult,
        buffer_size: int,
        tls_hostname: str | None = None,
    ):
        self._uri = uri
        self._url = uri.geturl()
        self._tls_hostname = tls_hostname or uri.hostname
        self._buffer_size = buffer_size
        self._bare_io_reader: asyncio.StreamReader | None = None
        self._io_reader: asyncio.StreamReader | None = None
        self._bare_io_writer: asyncio.StreamWriter | None = None
        self._io_writer: asyncio.StreamWriter | None = None

    @property
    def writer(self) -> asyncio.StreamWriter:
        """Returns the stream writer instance."""
        if self._io_writer:
            return self._io_writer
        raise TransportNotConnectedError(self)

    @property
    def reader(self) -> asyncio.StreamReader:
        """Returns the stream reader instance."""
        if self._io_reader:
            return self._io_reader
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

    async def connect(
        self,
    ):
        r, w = await asyncio.open_connection(
            host=self._uri.hostname,
            port=self._uri.port,
            limit=self._buffer_size,
        )
        # We keep a reference to the initial transport we used when
        # establishing the connection in case we later upgrade to TLS
        # after getting the first INFO message. This is in order to
        # prevent the GC closing the socket after we send CONNECT
        # and replace the transport.
        #
        # See https://github.com/nats-io/asyncio-nats/issues/43
        self._bare_io_reader = self._io_reader = r
        self._bare_io_writer = self._io_writer = w

    async def connect_tls(
        self,
        ssl_context: ssl.SSLContext,
    ) -> None:
        # manually recreate the stream reader/writer with a tls upgraded transport
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport = await asyncio.get_running_loop().start_tls(
            self.writer.transport,
            protocol,
            ssl_context,
            server_hostname=self.tls_hostname,
        )
        if transport is None:
            raise TransportTLSUpgradeError(self)
        writer = asyncio.StreamWriter(
            transport, protocol, reader, asyncio.get_running_loop()
        )
        self._io_reader, self._io_writer = reader, writer

    def requires_tls_upgrade(self) -> bool:
        return True

    def write(self, payload: bytes) -> None:
        return self.writer.write(payload)

    def writelines(self, payload: list[bytes]) -> None:
        return self.writer.writelines(payload)

    async def read(self, buffer_size: int) -> bytes:
        return await self.reader.read(buffer_size)

    async def drain(self) -> None:
        return await self.writer.drain()

    async def wait_closed(self) -> None:
        return await self.writer.wait_closed()

    def close(self) -> None:
        return self.writer.close()

    def at_eof(self) -> bool:
        return self.reader.at_eof()
