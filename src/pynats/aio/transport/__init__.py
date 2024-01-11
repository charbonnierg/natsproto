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
    rcv_buffer_size: int,
    rcv_chunk_size: int | None = None,
    tls_hostname: str | None = None,
) -> Transport:
    """
    Factory method that returns a transport instance based on the uri scheme.
    """
    if uri.scheme in ("tcp", "nats", "tls", "nats+tls"):
        return TcpTransport(
            uri=uri,
            rcv_buffer_size=rcv_buffer_size,
            rcv_chunk_size=rcv_chunk_size or rcv_buffer_size,
            tls_hostname=tls_hostname,
        )

    raise TypeError(f"Transport scheme not supported: {uri.scheme}")
