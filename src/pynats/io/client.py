from __future__ import annotations

from contextlib import ExitStack
from timeit import default_timer

from ..core.client_state import ClientStateMixin
from ..errors import ConnectionClosedError, NatsServerError
from ..protocol import ClientOptions, ConnectionProtocol, EventType
from .connection import Connection
from .msg import Msg
from .subscription import SyncSubscriptionIterator
from .subscription_registry import SyncSubscriptionRegistry
from .transport import transport_factory


class Client(ClientStateMixin):
    def __init__(self, options: ClientOptions | None = None) -> None:
        super().__init__()
        self.options = options or ClientOptions()
        # Attributes used through all the client lifetime
        self._connect_attempts: int = 0
        self._exit_stack = ExitStack()
        self._server_pool = self.options.new_server_pool()
        self._pending_buffer = self.options.new_pending_buffer()
        self._subscriptions = SyncSubscriptionRegistry(self)
        self._protocol = ConnectionProtocol(self.options)
        self._current_connection_or_none: Connection | None = None

    def _create_new_connection(self) -> Connection:
        """Create a new connection."""
        server = self._protocol.select_server()
        conn = Connection(
            self._protocol,
            self._pending_buffer,
            transport_factory(
                server.uri,
                self.options.read_chunk_size,
                receive_timeout=self.options.connect_timeout,
            ),
        )
        conn.connect()
        self._current_connection_or_none = conn
        return conn

    def connect(self) -> None:
        if self._current_connection_or_none is not None:
            if self._current_connection_or_none.protocol.is_connected():
                return
        self._create_new_connection()

    def close(self) -> None:
        raise NotImplementedError

    def rtt(self) -> float:
        """Return the round trip time to the server.

        This function will send a PING to the server and wait for
        the PONG response. The round trip time is the time elapsed
        between the PING and the PONG.

        If the pending buffer is full, the time elapsed may be a lot
        longer than the actual RTT.
        """
        if self.is_cancelled():
            raise ConnectionClosedError()
        start = default_timer()
        conn = self._get_current_connection()
        if conn is None:
            conn = self._create_new_connection()
        conn.transport.write(conn.protocol.ping())
        while True:
            events = conn.protocol.events_received()
            if not events:
                conn._read_more(continue_on_timeout=False)
                continue
            data_to_send = conn.protocol.data_to_send()
            if data_to_send:
                conn.transport.write(data_to_send)
            for event in events:
                if event.type == EventType.INFO:
                    continue
                if event.type == EventType.PONG:
                    return default_timer() - start
                if event.type == EventType.ERROR:
                    raise NatsServerError(event.body)
                if event.type == EventType.MSG:
                    self._subscriptions.observe(Msg(self, event.body))
                    continue
                raise Exception(f"Unexpected event: {event.type}")
            raise ConnectionClosedError()

    def publish(
        self,
        subject: str,
        payload: bytes | None = None,
        reply: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send a publish command to the server."""
        self._send_publish(subject, reply, payload, headers)

    def request(
        self,
        subject: str,
        payload: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> Msg:
        """Send a request command to the server."""
        reply_subject = "REPLY"
        started = default_timer()
        conn = self._get_current_connection()
        if conn is None:
            raise ConnectionClosedError()
        sub = self.subscribe(reply_subject, max_msgs=1)
        self._send_publish(subject, reply_subject, payload, headers)
        if msg := sub.peek():
            return msg
        while True:
            events = conn.protocol.events_received()
            if events is None:
                try:
                    conn._read_more(continue_on_timeout=False)
                except TimeoutError:
                    if default_timer() - started > timeout:
                        raise
                continue
            data_to_send = conn.protocol.data_to_send()
            if data_to_send:
                conn.transport.write(data_to_send)
            for event in events:
                if event.type == EventType.INFO:
                    continue
                if event.type == EventType.PONG:
                    continue
                if event.type == EventType.ERROR:
                    raise NatsServerError(event.body)
                if event.type == EventType.MSG:
                    self._subscriptions.observe(Msg(self, event.body))
                    if msg := sub.peek():
                        return msg
                    continue
                raise Exception(f"Unexpected event: {event.type}")
            raise ConnectionClosedError()

    def subscribe(
        self,
        subject: str,
        queue: str | None = None,
        max_msgs: int = -1,
        pending_msgs_limit: int = 100,
        pending_bytes_limit: int = 1024 * 1024 * 8,
    ) -> SyncSubscriptionIterator:
        """Subscribe to a subject."""
        sid = self._subscriptions.next_sid()
        sub = SyncSubscriptionIterator(
            self,
            sid,
            subject,
            queue,
            max_msgs,
            pending_msgs_limit,
            pending_bytes_limit,
        )
        sub.start()
        return sub

    def _get_current_connection(self) -> Connection | None:
        """Get the current connection."""
        if self._current_connection_or_none is None:
            return None
        if not self._current_connection_or_none.protocol.is_connected():
            return None
        return self._current_connection_or_none

    def _write_to_pending_buffer(
        self,
        data: bytes,
        force_flush: bool = False,
        priority: bool = False,
    ) -> None:
        """Write bytes to the connection.

        Args:
            data: Bytes to write to the connection.
            force_flush: Force a flush operation, but do not wait.
            wait_flush: Force a flush operation and wait for it (takes precedence over force_flush)
            priority: Add the data to the front of the buffer.
        """
        conn = self._get_current_connection()
        empty = self._pending_buffer.is_empty()
        if self._pending_buffer.can_fit(len(data)):
            self._pending_buffer.append(data, priority=priority)
            if force_flush:
                if conn is None:
                    conn = self._create_new_connection()
                with self._pending_buffer.borrow() as pending:
                    conn.transport.write(b"".join(pending))
                return
            if conn and empty:
                with self._pending_buffer.borrow() as pending:
                    conn.transport.write(b"".join(pending))
            return

        if conn is None:
            conn = self._create_new_connection()

        self._pending_buffer.append(data, priority=priority)

        # Flush if the write queue is empty
        if empty:
            with self._pending_buffer.borrow() as pending:
                conn.transport.write(b"".join(pending))
            return
        # Always flush if the buffer is full
        if force_flush or self._pending_buffer.is_full():
            with self._pending_buffer.borrow() as pending:
                conn.transport.write(b"".join(pending))
            return

    def _send_publish(
        self,
        subject: str,
        reply: str | None,
        payload: bytes | None,
        headers: dict[str, str] | None,
    ) -> None:
        """Send a publish command to the server."""
        if self.is_cancelled():
            raise ConnectionClosedError()
        data = self._protocol.publish(subject, reply or "", payload or b"", headers)
        self._write_to_pending_buffer(data)

    def _send_subscribe(
        self,
        subject: str,
        queue: str | None,
        sid: int,
    ) -> None:
        """Send a subscribe command to the server."""
        conn = self._get_current_connection()
        if conn is None:
            conn = self._create_new_connection()
        self._pending_buffer.append(
            conn.protocol.subscribe(subject, queue or "", sid), False
        )
        with self._pending_buffer.borrow() as pending:
            conn.transport.write(b"".join(pending))

    def _send_unsubscribe(self, sid: int, max_msgs: int) -> None:
        """Send an unsubscribe command to the server."""
        conn = self._get_current_connection()
        if conn is None:
            conn = self._create_new_connection()
        self._pending_buffer.append(conn.protocol.unsubscribe(sid, max_msgs), False)
        with self._pending_buffer.borrow() as pending:
            conn.transport.write(b"".join(pending))
