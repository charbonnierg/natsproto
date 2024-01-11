from __future__ import annotations

from typing import Any


class ConnectOpts:
    __slots__ = [
        "verbose",
        "pedantic",
        "lang",
        "version",
        "protocol",
        "headers",
        "no_responders",
        "signature",
        "jwt",
        "nkey",
        "user",
        "password",
        "auth_token",
        "name",
        "no_echo",
        "tls_required",
    ]

    def __init__(
        self,
        verbose: bool,
        pedantic: bool,
        lang: str,
        version: str,
        protocol: int,
        tls_required: bool,
        headers: bool | None,
        no_responders: bool | None,
        signature: str | None,
        jwt: str | None,
        nkey: str | None,
        user: str | None,
        password: str | None,
        auth_token: str | None,
        name: str | None,
        no_echo: bool | None,
    ) -> None:
        self.verbose = verbose
        self.pedantic = pedantic
        self.lang = lang
        self.version = version
        self.protocol = protocol
        self.tls_required = tls_required
        self.headers = headers
        self.no_responders = no_responders
        self.signature = signature
        self.jwt = jwt
        self.nkey = nkey
        self.user = user
        self.password = password
        self.auth_token = auth_token
        self.name = name
        self.no_echo = no_echo

    def to_dict(self) -> dict[str, Any]:
        opts = {
            "verbose": self.verbose,
            "pedantic": self.pedantic,
            "tls_required": self.tls_required,
            "name": self.name,
            "lang": self.lang,
            "version": self.version,
            "protocol": self.protocol,
            "echo": not self.no_echo,
        }
        if self.headers is not None:
            opts["headers"] = self.headers
        if self.no_responders is not None:
            opts["no_responders"] = self.no_responders
        if self.signature is not None:
            opts["sig"] = self.signature
        if self.jwt is not None:
            opts["jwt"] = self.jwt
        if self.nkey is not None:
            opts["nkey"] = self.nkey
        if self.user is not None:
            opts["user"] = self.user
        if self.password is not None:
            opts["pass"] = self.password
        if self.auth_token is not None:
            opts["auth_token"] = self.auth_token

        return opts

    @classmethod
    def from_dict(cls, opts: dict[str, Any]) -> ConnectOpts:
        return cls(
            verbose=opts["verbose"],
            pedantic=opts["pedantic"],
            lang=opts["lang"],
            version=opts["version"],
            protocol=opts["protocol"],
            tls_required=opts.get("tls_required", False),
            headers=opts.get("headers"),
            no_responders=opts.get("no_responders"),
            signature=opts.get("sig"),
            jwt=opts.get("jwt"),
            nkey=opts.get("nkey"),
            user=opts.get("user"),
            password=opts.get("pass"),
            auth_token=opts.get("auth_token"),
            name=opts.get("name"),
            no_echo=opts.get("no_echo"),
        )
