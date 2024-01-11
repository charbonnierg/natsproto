import pytest

from pynats.protocol.connection_state import ConnectionState, ConnectionStateMixin
from pynats.protocol.errors import ConnectionStateTransitionError


class TestConnectionStateMixin:
    def test_default_connection_state(self):
        """Test the default connection state."""
        conn = ConnectionStateMixin()
        assert conn.status == ConnectionState.WAITING_FOR_SERVER_SELECTION

    def test_waiting_for_server_selection(self):
        conn = ConnectionStateMixin(ConnectionState.WAITING_FOR_SERVER_SELECTION)

        assert conn.is_cancelled() is False
        assert conn.is_closed() is False
        assert conn.is_closing() is False
        assert conn.is_connected() is False
        assert conn.is_connecting() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_connect() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_waiting_for_server_selection() is True

    def test_waiting_for_server_info(self):
        conn = ConnectionStateMixin(ConnectionState.WAITING_FOR_SERVER_INFO)

        assert conn.is_cancelled() is False
        assert conn.is_closed() is False
        assert conn.is_closing() is False
        assert conn.is_connected() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_client_connect() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_connecting() is True
        assert conn.is_waiting_for_server_info() is True

    def test_waiting_for_client_connect(self):
        conn = ConnectionStateMixin(ConnectionState.WAITING_FOR_CLIENT_CONNECT)

        assert conn.is_cancelled() is False
        assert conn.is_closed() is False
        assert conn.is_closing() is False
        assert conn.is_connected() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_connecting() is True
        assert conn.is_waiting_for_client_connect() is True

    def test_waiting_for_client_ping(self):
        conn = ConnectionStateMixin(ConnectionState.WAITING_FOR_CLIENT_PING)

        assert conn.is_cancelled() is False
        assert conn.is_closed() is False
        assert conn.is_closing() is False
        assert conn.is_connected() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_connect() is False

        assert conn.is_connecting() is True
        assert conn.is_waiting_for_client_ping() is True

    def test_waiting_for_server_pong(self):
        conn = ConnectionStateMixin(ConnectionState.WAITING_FOR_SERVER_PONG)

        assert conn.is_cancelled() is False
        assert conn.is_closed() is False
        assert conn.is_closing() is False
        assert conn.is_connected() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_connect() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_connecting() is True
        assert conn.is_waiting_for_server_pong() is True

    def test_connected(self):
        conn = ConnectionStateMixin(ConnectionState.CONNECTED)

        assert conn.is_cancelled() is False
        assert conn.is_closed() is False
        assert conn.is_closing() is False
        assert conn.is_connecting() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_connect() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_connected() is True

    def test_closing(self):
        conn = ConnectionStateMixin(ConnectionState.CLOSING)

        assert conn.is_closed() is False
        assert conn.is_connecting() is False
        assert conn.is_connected() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_connect() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_cancelled() is True
        assert conn.is_closing() is True

    def test_closed(self):
        conn = ConnectionStateMixin(ConnectionState.CLOSED)

        assert conn.is_closing() is False
        assert conn.is_connecting() is False
        assert conn.is_connected() is False
        assert conn.is_waiting_for_server_selection() is False
        assert conn.is_waiting_for_server_info() is False
        assert conn.is_waiting_for_client_connect() is False
        assert conn.is_waiting_for_client_ping() is False

        assert conn.is_cancelled() is True
        assert conn.is_closed() is True

    @pytest.mark.parametrize(
        "state,expected",
        [
            (ConnectionState.WAITING_FOR_SERVER_SELECTION, True),
            (ConnectionState.WAITING_FOR_SERVER_INFO, True),
            (ConnectionState.WAITING_FOR_CLIENT_CONNECT, True),
            (ConnectionState.WAITING_FOR_CLIENT_PING, True),
            (ConnectionState.WAITING_FOR_SERVER_PONG, False),
            (ConnectionState.CONNECTED, False),
            (ConnectionState.CLOSING, False),
            (ConnectionState.CLOSED, True),
        ],
    )
    def test_did_not_expect_pong(self, state: ConnectionState, expected: bool):
        conn = ConnectionStateMixin(state)

        assert conn.did_not_expect_pong() is expected

    @pytest.mark.parametrize(
        "state,expected",
        [
            (ConnectionState.WAITING_FOR_SERVER_SELECTION, True),
            (ConnectionState.WAITING_FOR_SERVER_INFO, True),
            (ConnectionState.WAITING_FOR_CLIENT_CONNECT, True),
            (ConnectionState.WAITING_FOR_CLIENT_PING, True),
            (ConnectionState.WAITING_FOR_SERVER_PONG, True),
            (ConnectionState.CONNECTED, False),
            (ConnectionState.CLOSING, False),
            (ConnectionState.CLOSED, True),
        ],
    )
    def test_did_not_expect_msg(self, state: ConnectionState, expected: bool):
        conn = ConnectionStateMixin(state)

        assert conn.did_not_expect_msg() is expected

    @pytest.mark.parametrize(
        "state,expected",
        [
            (ConnectionState.WAITING_FOR_SERVER_SELECTION, True),
            (ConnectionState.WAITING_FOR_SERVER_INFO, True),
            (ConnectionState.WAITING_FOR_CLIENT_CONNECT, True),
            (ConnectionState.WAITING_FOR_CLIENT_PING, True),
            (ConnectionState.WAITING_FOR_SERVER_PONG, False),
            (ConnectionState.CONNECTED, False),
            (ConnectionState.CLOSING, False),
            (ConnectionState.CLOSED, True),
        ],
    )
    def test_did_not_expect_err(self, state: ConnectionState, expected: bool):
        conn = ConnectionStateMixin(state)

        assert conn.did_not_expect_err() is expected

    @pytest.mark.parametrize(
        "state,error",
        [
            (ConnectionState.WAITING_FOR_SERVER_SELECTION, None),
            (ConnectionState.WAITING_FOR_SERVER_INFO, ConnectionStateTransitionError),
            (
                ConnectionState.WAITING_FOR_CLIENT_CONNECT,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_PING, ConnectionStateTransitionError),
            (ConnectionState.WAITING_FOR_SERVER_PONG, ConnectionStateTransitionError),
            (ConnectionState.CONNECTED, ConnectionStateTransitionError),
            (ConnectionState.CLOSING, ConnectionStateTransitionError),
            (ConnectionState.CLOSED, ConnectionStateTransitionError),
        ],
    )
    def test_mark_as_waiting_for_server_info(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_waiting_for_server_info()
            return

        conn.mark_as_waiting_for_server_info()
        assert conn.is_waiting_for_server_info() is True

    @pytest.mark.parametrize(
        "state,error",
        [
            (
                ConnectionState.WAITING_FOR_SERVER_SELECTION,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_SERVER_INFO, None),
            (
                ConnectionState.WAITING_FOR_CLIENT_CONNECT,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_PING, ConnectionStateTransitionError),
            (ConnectionState.WAITING_FOR_SERVER_PONG, ConnectionStateTransitionError),
            (ConnectionState.CONNECTED, ConnectionStateTransitionError),
            (ConnectionState.CLOSING, ConnectionStateTransitionError),
            (ConnectionState.CLOSED, ConnectionStateTransitionError),
        ],
    )
    def test_mark_as_waiting_for_client_connect(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_waiting_for_client_connect()
            return

        conn.mark_as_waiting_for_client_connect()
        assert conn.is_waiting_for_client_connect() is True

    @pytest.mark.parametrize(
        "state,error",
        [
            (
                ConnectionState.WAITING_FOR_SERVER_SELECTION,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_SERVER_INFO,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_CONNECT, None),
            (ConnectionState.WAITING_FOR_CLIENT_PING, ConnectionStateTransitionError),
            (ConnectionState.WAITING_FOR_SERVER_PONG, ConnectionStateTransitionError),
            (ConnectionState.CONNECTED, ConnectionStateTransitionError),
            (ConnectionState.CLOSING, ConnectionStateTransitionError),
            (ConnectionState.CLOSED, ConnectionStateTransitionError),
        ],
    )
    def test_mark_as_waiting_for_client_ping(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_waiting_for_client_ping()
            return

        conn.mark_as_waiting_for_client_ping()
        assert conn.is_waiting_for_client_ping() is True

    @pytest.mark.parametrize(
        "state,error",
        [
            (
                ConnectionState.WAITING_FOR_SERVER_SELECTION,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_SERVER_INFO,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_CLIENT_CONNECT,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_PING, None),
            (ConnectionState.WAITING_FOR_SERVER_PONG, ConnectionStateTransitionError),
            (ConnectionState.CONNECTED, ConnectionStateTransitionError),
            (ConnectionState.CLOSING, ConnectionStateTransitionError),
            (ConnectionState.CLOSED, ConnectionStateTransitionError),
        ],
    )
    def test_mark_as_waiting_for_server_pong(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_waiting_for_server_pong()
            return

        conn.mark_as_waiting_for_server_pong()
        assert conn.is_waiting_for_server_pong() is True

    @pytest.mark.parametrize(
        "state,error",
        [
            (
                ConnectionState.WAITING_FOR_SERVER_SELECTION,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_SERVER_INFO,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_CLIENT_CONNECT,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_PING, ConnectionStateTransitionError),
            (ConnectionState.WAITING_FOR_SERVER_PONG, None),
            (ConnectionState.CONNECTED, ConnectionStateTransitionError),
            (ConnectionState.CLOSING, ConnectionStateTransitionError),
            (ConnectionState.CLOSED, ConnectionStateTransitionError),
        ],
    )
    def test_mark_as_connected(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_connected()
            return

        conn.mark_as_connected()
        assert conn.is_connected() is True

    @pytest.mark.parametrize(
        "state,error",
        [
            (
                ConnectionState.WAITING_FOR_SERVER_SELECTION,
                None,
            ),
            (
                ConnectionState.WAITING_FOR_SERVER_INFO,
                None,
            ),
            (
                ConnectionState.WAITING_FOR_CLIENT_CONNECT,
                None,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_PING, None),
            (ConnectionState.WAITING_FOR_SERVER_PONG, None),
            (ConnectionState.CONNECTED, None),
            (ConnectionState.CLOSING, ConnectionStateTransitionError),
            (ConnectionState.CLOSED, ConnectionStateTransitionError),
        ],
    )
    def test_mark_as_closing(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_closing()
            return

        conn.mark_as_closing()
        assert conn.is_closing() is True

    @pytest.mark.parametrize(
        "state,error",
        [
            (
                ConnectionState.WAITING_FOR_SERVER_SELECTION,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_SERVER_INFO,
                ConnectionStateTransitionError,
            ),
            (
                ConnectionState.WAITING_FOR_CLIENT_CONNECT,
                ConnectionStateTransitionError,
            ),
            (ConnectionState.WAITING_FOR_CLIENT_PING, ConnectionStateTransitionError),
            (ConnectionState.WAITING_FOR_SERVER_PONG, ConnectionStateTransitionError),
            (ConnectionState.CONNECTED, ConnectionStateTransitionError),
            (ConnectionState.CLOSING, None),
            (ConnectionState.CLOSED, None),
        ],
    )
    def test_mark_as_closed(
        self, state: ConnectionState, error: type[Exception] | None
    ):
        conn = ConnectionStateMixin(state)

        if error:
            with pytest.raises(error):
                conn.mark_as_closed()
            return

        conn.mark_as_closed()
        assert conn.is_closed() is True
