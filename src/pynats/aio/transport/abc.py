from __future__ import annotations

import abc
import ssl


class Transport(abc.ABC):
    @abc.abstractmethod
    def uri(self) -> str:
        """
        Returns the uri used to connect to the server.
        """
        pass

    @abc.abstractmethod
    def requires_tls_upgrade(self) -> bool:
        """
        Returns if the transport requires a TLS upgrade.
        """
        pass

    @abc.abstractmethod
    async def connect(self):
        """
        Connects to a server using the implemented transport. The uri passed is of type ParseResult that can be
        obtained calling urllib.parse.urlparse.
        """
        pass

    @abc.abstractmethod
    async def connect_tls(
        self,
        ssl_context: ssl.SSLContext,
    ):
        """
        connect_tls is similar to connect except it tries to connect to a secure endpoint, using the provided ssl
        context.
        """
        pass

    @abc.abstractmethod
    def write(self, payload: bytes):
        """
        Write bytes to underlying transport. Needs a call to drain() to be successfully written.
        """
        pass

    @abc.abstractmethod
    def writelines(self, payload: list[bytes]):
        """
        Writes a list of bytes, one by one, to the underlying transport. Needs a call to drain() to be successfully
        written.
        """
        pass

    @abc.abstractmethod
    async def read(self) -> bytes:
        """
        Reads a sequence of bytes from the underlying transport.
        """
        pass

    @abc.abstractmethod
    async def drain(self):
        """
        Flushes the bytes queued for transmission when calling write() and writelines().
        """
        pass

    @abc.abstractmethod
    async def wait_closed(self):
        """
        Waits until the connection is successfully closed.
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        Closes the underlying transport.
        """
        pass

    @abc.abstractmethod
    def at_eof(self) -> bool:
        """
        Returns if underlying transport reader is at eof.
        """
        pass
