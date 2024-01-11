from __future__ import annotations

import json

from ..payload import Info, Version


def parse_info(data: bytes) -> Info:
    raw_info = json.loads(data.decode())
    return Info(
        server_id=raw_info["server_id"],
        server_name=raw_info["server_name"],
        version=parse_version(raw_info["version"]),
        go=raw_info["go"],
        host=raw_info["host"],
        port=raw_info["port"],
        headers=raw_info["headers"],
        proto=raw_info["proto"],
        max_payload=raw_info.get("max_payload"),
        client_id=raw_info.get("client_id"),
        auth_required=raw_info.get("auth_required"),
        tls_required=raw_info.get("tls_required"),
        tls_verify=raw_info.get("tls_verify"),
        tls_available=raw_info.get("tls_available"),
        connect_urls=raw_info.get("connect_urls"),
        ws_connect_urls=raw_info.get("ws_connect_urls"),
        ldm=raw_info.get("ldm"),
        git_commit=raw_info.get("git_commit"),
        jetstream=raw_info.get("jetstream"),
        ip=raw_info.get("ip"),
        client_ip=raw_info.get("client_ip"),
        nonce=raw_info.get("nonce"),
        cluster=raw_info.get("cluster"),
        domain=raw_info.get("domain"),
        xkey=raw_info.get("xkey"),
    )


def parse_version(version: str) -> Version:
    semver = Version(0, 0, 0, "")
    v = version.split("-")
    if len(v) > 1:
        semver.dev = v[1]
    tokens = v[0].split(".")
    n = len(tokens)
    if n > 1:
        semver.major = int(tokens[0])
    if n > 2:
        semver.minor = int(tokens[1])
    if n > 3:
        semver.patch = int(tokens[2])
    semver._version = version
    return semver
