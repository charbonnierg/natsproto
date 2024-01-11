from __future__ import annotations


class Version:
    __slots__ = ["major", "minor", "patch", "dev", "_version"]

    def __init__(self, major: int, minor: int, patch: int, dev: str) -> None:
        self.major = major
        self.minor = minor
        self.patch = patch
        self.dev = dev
        self._version = f"{major}.{minor}.{patch}-{dev}"

    def __str__(self) -> str:
        return self._version

    def __repr__(self) -> str:
        return self._version


class Info:
    __slots__ = [
        "server_id",
        "server_name",
        "version",
        "go",
        "host",
        "port",
        "headers",
        "max_payload",
        "proto",
        "client_id",
        "auth_required",
        "tls_required",
        "tls_verify",
        "tls_available",
        "connect_urls",
        "ws_connect_urls",
        "ldm",
        "git_commit",
        "jetstream",
        "ip",
        "client_ip",
        "nonce",
        "cluster",
        "domain",
        "xkey",
    ]

    def __init__(
        self,
        proto: int,
        server_id: str,
        server_name: str,
        version: Version,
        go: str,
        host: str,
        port: int,
        max_payload: int | None,
        headers: bool | None,
        client_id: int | None,
        auth_required: bool | None,
        tls_required: bool | None,
        tls_verify: bool | None,
        tls_available: bool | None,
        connect_urls: list[str] | None,
        ws_connect_urls: list[str] | None,
        ldm: bool | None,
        git_commit: str | None,
        jetstream: bool | None,
        ip: str | None,
        client_ip: str | None,
        nonce: str | None,
        cluster: str | None,
        domain: str | None,
        xkey: str | None,
    ) -> None:
        self.server_id = server_id
        self.server_name = server_name
        self.version = version
        self.go = go
        self.host = host
        self.port = port
        self.headers = headers
        self.max_payload = max_payload
        self.proto = proto
        self.client_id = client_id
        self.auth_required = auth_required
        self.tls_required = tls_required
        self.tls_verify = tls_verify
        self.tls_available = tls_available
        self.connect_urls = connect_urls
        self.ws_connect_urls = ws_connect_urls
        self.ldm = ldm
        self.git_commit = git_commit
        self.jetstream = jetstream
        self.ip = ip
        self.client_ip = client_ip
        self.nonce = nonce
        self.cluster = cluster
        self.domain = domain
        self.xkey = xkey
