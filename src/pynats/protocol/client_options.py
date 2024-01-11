from __future__ import annotations

import ssl
from dataclasses import dataclass, field
from typing import Callable
from urllib.parse import urlparse

from pynats.core.pending_buffer import PendingBuffer

from .payload import ConnectOpts, Info
from .server import Server
from .server_pool import ServerPool

__lang__ = "natspy"
__version__ = "0.0.1"


@dataclass
class ClientOptions:
    """Client options."""

    # Server URLS
    servers: list[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    # Client name
    name: str | None = None
    # Connect opts
    connect_timeout: int = 2
    max_connect_attempts: int = 3
    max_reconnect_attempts: int = 60
    no_randomize: bool = False
    no_echo: bool = False
    verbose: bool = False
    pedantic: bool = False
    # Protocol parser
    max_control_line_size: int = 4096
    # Transport
    read_chunk_size: int = 4096
    # Pending buffer
    pending_buffer_size: int = 65536
    # Ping/Pong
    ping_interval: float = 60
    max_outstanding_pings: int = 3
    # TLS
    tls_required: bool = False
    tls_handshake_first: bool = False
    ssl_context: Callable[[], ssl.SSLContext] | None = None
    # Auth: all auth options are provided as callables to allow for dynamic values
    username: Callable[[], str] | None = None
    password: Callable[[], str] | None = None
    token: Callable[[], str] | None = None
    jwt_callback: Callable[[], str] | None = None
    nkey_callback: Callable[[], str] | None = None
    signature_callback: Callable[[str], str] | None = None

    def new_server_pool(self) -> ServerPool:
        """Create a new server pool according to options."""

        randomized = not self.no_randomize
        if not self.servers:
            raise ValueError("servers must not be empty")
        return ServerPool(
            [Server(urlparse(server)) for server in self.servers],
            randomized=randomized,
            max_connect_attempts=self.max_connect_attempts,
            max_reconnect_attempts=self.max_reconnect_attempts,
        )

    def new_pending_buffer(self) -> PendingBuffer:
        """Create a new pending buffer according to options."""
        return PendingBuffer(max_size=self.pending_buffer_size)

    def verify_server_info(self, info: Info) -> None:
        if self.tls_required and not info.tls_available:
            raise ValueError(
                "Profile requires TLS, but TLS is not available in the server"
            )
        if info.tls_required and not self.ssl_context:
            raise ValueError(
                "Server requires TLS, but no SSLContext was provided in the profile"
            )

        if info.auth_required:
            if not (
                self.username
                or self.password
                or self.token
                or self.jwt_callback
                or self.nkey_callback
            ):
                raise ValueError(
                    "Server requires auth, but no auth method was provided in the profile"
                )

    def get_connect_opts(self, server: Server) -> ConnectOpts:
        info = server.info
        if info is None:
            raise ValueError("Server info is not available")
        self.verify_server_info(info)
        opts = ConnectOpts(
            name=self.name,
            verbose=self.verbose,
            pedantic=self.pedantic,
            lang=__lang__,
            version=__version__,
            protocol=info.proto,
            headers=info.headers,
            no_responders=info.headers,
            tls_required=self.tls_required,
            no_echo=self.no_echo,
            signature=None,
            jwt=None,
            nkey=None,
            user=None,
            password=None,
            auth_token=None,
        )
        uri = server.uri
        # User/Pass auth
        if self.username or self.password:
            if self.token:
                raise ValueError("Cannot use both token and username/password")
            if self.username:
                if uri.username:
                    raise ValueError("Cannot use both username and uri username")
                opts.user = self.username()
            elif uri.username:
                opts.user = uri.username
            if self.password:
                if uri.password:
                    raise ValueError("Cannot use both password and uri password")
                opts.password = self.password()
            elif uri.password:
                opts.password = uri.password
        # Token auth
        elif self.token:
            if uri.username:
                raise ValueError("Cannot use both token and uri username")
            opts.auth_token = self.token()
        # User/Pass from URI
        elif uri.username and uri.password:
            opts.user = uri.username
            opts.password = uri.password
        # Token from URI
        elif uri.username:
            opts.auth_token = uri.username

        # JWT auth
        if self.jwt_callback:
            if self.nkey_callback:
                raise ValueError("Cannot use both jwt and nkey")
            opts.jwt = self.jwt_callback()

        if self.signature_callback and info.nonce:
            if self.nkey_callback:
                raise ValueError("Cannot use both signature and nkey")
            opts.signature = self.signature_callback(info.nonce)

        # NKey auth
        if self.nkey_callback:
            opts.nkey = self.nkey_callback()

        return opts
