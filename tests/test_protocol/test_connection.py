from __future__ import annotations

import json

import pytest

from pynats.protocol import (
    ClientOptions,
    ConnectionProtocol,
    ConnectionState,
    ErrorType,
    Server,
)
from pynats.protocol.errors import (
    ConnectionServerPoolEmpty,
    ConnectionStateTransitionError,
)


class BaseTestConnectionProtocol:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.options = ClientOptions()

    def _simulate_connection(self, conn: ConnectionProtocol) -> None:
        conn.connect()
        conn.ping()
        self._simulate_pong(conn)

    def _simulate_connection_lost(self, conn: ConnectionProtocol):
        conn.mark_as_closing()
        conn.mark_as_closed()
        conn.status = ConnectionState.WAITING_FOR_SERVER_SELECTION

    def _simulate_server_info(
        self,
        conn: ConnectionProtocol,
        server_id: str = "test",
        server_name: str = "test",
        version: str = "0.0.0-test",
        go: str = "go0.0.0-test",
        host: str = "memory",
        port: int = 0,
        headers: bool = True,
        max_payload: int = 1024 * 1024,
        proto: int = 1,
        auth_required: bool | None = None,
        tls_required: bool | None = None,
        tls_verify: bool | None = None,
        tls_available: bool | None = None,
        connect_urls: list[str] | None = None,
        ws_connect_urls: list[str] | None = None,
        ldm: bool | None = None,
        git_commit: str | None = None,
        jetstream: bool | None = None,
        ip: str | None = None,
        client_ip: str | None = None,
        nonce: str | None = None,
        cluster: str | None = None,
        domain: str | None = None,
        xkey: str | None = None,
    ) -> None:
        info_dict = {
            "server_id": server_id,
            "server_name": server_name,
            "version": version,
            "go": go,
            "host": host,
            "port": port,
            "headers": headers,
            "max_payload": max_payload,
            "proto": proto,
        }
        if auth_required is not None:
            info_dict["auth_required"] = auth_required
        if tls_required is not None:
            info_dict["tls_required"] = tls_required
        if tls_verify is not None:
            info_dict["tls_verify"] = tls_verify
        if tls_available is not None:
            info_dict["tls_available"] = tls_available
        if connect_urls is not None:
            info_dict["connect_urls"] = connect_urls
        if ws_connect_urls is not None:
            info_dict["ws_connect_urls"] = ws_connect_urls
        if ldm is not None:
            info_dict["ldm"] = ldm
        if git_commit is not None:
            info_dict["git_commit"] = git_commit
        if jetstream is not None:
            info_dict["jetstream"] = jetstream
        if ip is not None:
            info_dict["ip"] = ip
        if client_ip is not None:
            info_dict["client_ip"] = client_ip
        if nonce is not None:
            info_dict["nonce"] = nonce
        if cluster is not None:
            info_dict["cluster"] = cluster
        if domain is not None:
            info_dict["domain"] = domain
        if xkey is not None:
            info_dict["xkey"] = xkey
        json_info = json.dumps(info_dict, separators=(",", ":"), sort_keys=True)
        cmd = f"INFO {json_info}\r\n".encode()
        conn.receive_data_from_server(cmd)

    def _simulate_ping(self, conn: ConnectionProtocol) -> None:
        conn.receive_data_from_server(b"PING\r\n")

    def _simulate_pong(self, conn: ConnectionProtocol) -> None:
        conn.receive_data_from_server(b"PONG\r\n")

    def _simulate_err(
        self, conn: ConnectionProtocol, err_type: ErrorType, subject: str | None = None
    ) -> None:
        if subject:
            if (
                err_type == ErrorType.PERMISSION_VIOLATION_FOR_PUBLISH
                or err_type == ErrorType.PERMISSION_VIOLATION_FOR_SUBSCRIPTION
            ):
                conn.receive_data_from_server(
                    f"-ERR '{err_type.value} {subject}'\r\n".encode()
                )
            else:
                raise ValueError(
                    f"Invalid error type {err_type}. Subject argument must be used with permission errors."
                )
        conn.receive_data_from_server(b"-ERR 'test'\r\n")


class TestConnectionProtocolSelectServer(BaseTestConnectionProtocol):
    def test_state_before_select_server(self):
        conn = ConnectionProtocol(self.options)
        assert conn.status == ConnectionState.WAITING_FOR_SERVER_SELECTION
        conn.select_server()
        assert conn.status == ConnectionState.WAITING_FOR_SERVER_INFO

    @pytest.mark.parametrize(
        "state",
        [
            ConnectionState.WAITING_FOR_SERVER_INFO,
            ConnectionState.WAITING_FOR_CLIENT_CONNECT,
            ConnectionState.WAITING_FOR_CLIENT_PING,
            ConnectionState.WAITING_FOR_SERVER_PONG,
            ConnectionState.CONNECTED,
            ConnectionState.CLOSING,
            ConnectionState.CLOSED,
        ],
    )
    def test_bad_state_before_select_server(self, state: ConnectionState):
        conn = ConnectionProtocol(self.options)
        conn.status = state
        with pytest.raises(ConnectionStateTransitionError):
            conn.select_server()

    def test_select_server_with_default_options(self):
        conn = ConnectionProtocol(self.options)
        conn.select_server()
        assert conn.status == ConnectionState.WAITING_FOR_SERVER_INFO
        assert conn.get_current_server() == Server.new("nats://localhost:4222")

    def test_select_server_with_connect_failed_and_no_more_connect_attempts(self):
        self.options.max_connect_attempts = 1
        conn = ConnectionProtocol(self.options)
        conn.select_server()
        self._simulate_connection_lost(conn)
        with pytest.raises(ConnectionServerPoolEmpty):
            conn.select_server()
        assert conn.status == ConnectionState.CLOSED
        assert conn.get_current_server() is None

    def test_select_server_with_connect_failed_and_remaining_connect_attempts(self):
        self.options.max_connect_attempts = 2
        conn = ConnectionProtocol(self.options)
        conn.select_server()

        self._simulate_connection_lost(conn)

        conn.select_server()

        assert conn.status == ConnectionState.WAITING_FOR_SERVER_INFO
        assert conn.get_current_server() == Server.new("nats://localhost:4222")

        self._simulate_connection_lost(conn)
        with pytest.raises(ConnectionServerPoolEmpty):
            conn.select_server()
        assert conn.status == ConnectionState.CLOSED

    def test_select_server_with_connect_succeeded_then_disconnected_and_no_more_reconnect_attempts(
        self,
    ):
        self.options.max_reconnect_attempts = 0
        conn = ConnectionProtocol(self.options)
        conn.select_server()
        self._simulate_server_info(conn)
        self._simulate_connection(conn)
        self._simulate_connection_lost(conn)
        with pytest.raises(ConnectionServerPoolEmpty):
            conn.select_server()
        assert conn.status == ConnectionState.CLOSED
        assert conn.get_current_server() is None

    def test_select_server_with_connect_succeeded_then_disconnected_and_remaining_reconnect_attempts(
        self,
    ):
        self.options.max_reconnect_attempts = 1
        conn = ConnectionProtocol(self.options)
        conn.select_server()
        self._simulate_server_info(conn)
        self._simulate_connection(conn)
        self._simulate_connection_lost(conn)
        conn.select_server()
        self._simulate_server_info(conn)
        self._simulate_connection(conn)
        self._simulate_connection_lost(conn)
        with pytest.raises(ConnectionServerPoolEmpty):
            conn.select_server()
        assert conn.status == ConnectionState.CLOSED
        assert conn.get_current_server() is None

    def test_select_server_with_multiple_servers_non_randomized(self):
        self.options.no_randomize = True
        self.options.servers = ["nats://localhost:4222", "nats://localhost:4223"]
        conn = ConnectionProtocol(self.options)

        conn.select_server()
        assert conn.is_waiting_for_server_info()
        assert conn.get_current_server() == Server.new("nats://localhost:4222")

        self._simulate_server_info(conn)
        self._simulate_connection(conn)
        self._simulate_connection_lost(conn)

        conn.select_server()

        assert conn.status == ConnectionState.WAITING_FOR_SERVER_INFO
        assert conn.get_current_server() == Server.new("nats://localhost:4223")

        self._simulate_server_info(conn)
        self._simulate_connection(conn)
        self._simulate_connection_lost(conn)

        conn.select_server()

        assert conn.status == ConnectionState.WAITING_FOR_SERVER_INFO
        assert conn.get_current_server() == Server.new("nats://localhost:4222")

        self._simulate_server_info(conn)
        self._simulate_connection(conn)
        self._simulate_connection_lost(conn)

        conn.select_server()

        assert conn.status == ConnectionState.WAITING_FOR_SERVER_INFO
        assert conn.get_current_server() == Server.new("nats://localhost:4223")


class TestConnectionProtocolConnect(BaseTestConnectionProtocol):
    def test_state_before_connect(self):
        conn = ConnectionProtocol(self.options)
        conn.select_server()
        self._simulate_server_info(conn)
        assert conn.status == ConnectionState.WAITING_FOR_CLIENT_CONNECT
        conn.connect()
        assert conn.status == ConnectionState.WAITING_FOR_CLIENT_PING
