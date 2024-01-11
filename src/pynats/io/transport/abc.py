from __future__ import annotations

import abc
import ssl


class Transport(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def uri(self) -> str:
        """Returns the uri used to connect to the server."""
        ...

    @abc.abstractmethod
    def connect(
        self,
    ):
        ...

    @abc.abstractmethod
    def connect_tls(
        self,
        ssl_context: ssl.SSLContext,
    ) -> None:
        ...

    @abc.abstractmethod
    def requires_tls_upgrade(self) -> bool:
        ...

    @abc.abstractmethod
    def write(self, payload: bytes) -> None:
        ...

    @abc.abstractmethod
    def read(self, buffer_size: int) -> bytes:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...
