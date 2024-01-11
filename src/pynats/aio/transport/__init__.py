from urllib.parse import ParseResult

from .abc import Transport
from .asyncio_tcp import TcpTransport

__all__ = [
    "Transport",
    "TcpTransport",
    "transport_factory",
]


def transport_factory(
    uri: ParseResult,
    buffer_size: int,
    tls_hostname: str | None = None,
) -> Transport:
    """
    Factory method that returns a transport instance based on the uri scheme.
    """
    if uri.scheme in ("tcp", "nats", "tls", "nats+tls"):
        return TcpTransport(uri, buffer_size, tls_hostname)

    raise TypeError(f"Transport scheme not supported: {uri.scheme}")
