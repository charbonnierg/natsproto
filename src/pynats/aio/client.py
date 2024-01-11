from __future__ import annotations

import contextlib
import uuid
from timeit import default_timer
from typing import Awaitable, Callable, overload

from anyio import (
    TASK_STATUS_IGNORED,
    Event,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    sleep,
)
from anyio.abc import TaskGroup, TaskStatus
from exceptiongroup import ExceptionGroup

from pynats.aio.msg import Msg
from pynats.errors import ConnectionClosedError

from ..core.client_state import ClientState, ClientStateMixin
from ..protocol import ClientOptions, ConnectionProtocol
from ..protocol.constant import PING_CMD
from .connection import Connection
from .subscription import QueueSubscriptionIterator, QueueSubscriptionWorker
from .subscription_registry import AsyncSubscriptionRegistry


class Client(ClientStateMixin):
    """NATS client."""

    def __init__(self, options: ClientOptions | None = None) -> None:
        """Create a new NATS client."""
        super().__init__()
        self.options = options or ClientOptions()
        # Attributes used through all the client lifetime
        self._protocol = ConnectionProtocol(self.options)
        self._exit_stack = contextlib.AsyncExitStack()
        self._pending_buffer = self.options.new_pending_buffer()
        self._subscriptions = AsyncSubscriptionRegistry(self)
        self._request_reply = _RequestReplyInbox(self, "_INBOX")
        self._pending_pongs: list[Event] = []
        self._waiters: list[_Waiter] = []
        self._task_group_or_none: TaskGroup | None = None
        self._closed_event_or_none: Event | None = None
        self._cancel_event_or_none: Event | None = None
        # Attributes used during a connection lifetime
        self._current_connection_or_none: Connection | None = None

    def __repr__(self) -> str:
        return f"<nats.aio.client.Client status={self.status.name}>"

    #############################
    # Public API                #
    #############################

    async def __aenter__(self) -> Client:
        """Enter the async context."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Exit the async context."""
        await self.close()

    async def connect(self) -> None:
        """Connect to the server."""
        # Allow a cancelled client to be reconnected
        if self.is_cancelled():
            self._reset()
        # Open the exit stack first
        # All resources will be opened using the exit stack,
        # in case of an error, all resources will be closed
        await self._exit_stack.__aenter__()
        # Callbacks are called in reverse order
        # Initialize the closed event
        self._closed_event_or_none = Event()
        self._cancel_event_or_none = Event()
        self._exit_stack.callback(self._closed_event_or_none.set)
        # Create a new task group
        self._task_group_or_none = await self._exit_stack.enter_async_context(
            create_task_group()
        )
        # Initialize the reconnect stream
        (
            self._reconnect_send_stream_or_none,
            self._reconnect_rcv_stream_or_none,
        ) = create_memory_object_stream[None](max_buffer_size=1)
        self._exit_stack.callback(self._reconnect_send_stream_or_none.close)
        self._exit_stack.callback(self._reconnect_rcv_stream_or_none.close)

        # Kick-off the reconnect loop
        await self._task_group_or_none.start(self._reconnect_loop)

    async def close(self) -> None:
        """Close the connection.

        This method is blocking and will wait for the connection to
        be closed.
        """
        # Mark client as cancelled
        self._cancel()
        await self._exit_stack.aclose()

    async def flush(self, timeout: float = 10) -> None:
        """Request a flush operation on the current connection
        and wait until it's done."""
        if self.is_cancelled():
            raise ConnectionClosedError()
        event = Event()
        self._pending_pongs.append(event)
        with fail_after(timeout):
            await self._write_to_pending_buffer(PING_CMD, force_flush=True)
            await event.wait()

    async def rtt(self, timeout: float = 5) -> float:
        """Return the round trip time to the server.

        This function will send a PING to the server and wait for
        the PONG response. The round trip time is the time elapsed
        between the PING and the PONG.

        If the pending buffer is full, the time elapsed may be a lot
        longer than the actual RTT.
        """
        if self.is_cancelled():
            raise ConnectionClosedError()
        with fail_after(timeout):
            start = default_timer()
            event = Event()
            self._pending_pongs.append(event)
            await self._write_to_pending_buffer(PING_CMD, force_flush=True)
            await event.wait()
            return default_timer() - start

    async def publish(
        self,
        subject: str,
        payload: bytes | None = None,
        reply: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> None:
        """Send a publish command to the server."""
        with fail_after(timeout):
            await self._send_publish(subject, reply, payload, headers)

    async def request(
        self,
        subject: str,
        payload: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> Msg:
        """Send a request command to the server."""
        with fail_after(timeout):
            return await self._request_reply.request(subject, payload, headers)

    @overload
    async def subscribe(
        self,
        subject: str,
        *,
        queue: str | None = None,
        max_msgs: int = -1,
        pending_msgs_limit: int = 100,
        pending_bytes_limit: int = 1024 * 1024 * 8,
    ) -> QueueSubscriptionIterator:
        ...

    @overload
    async def subscribe(
        self,
        subject: str,
        cb: Callable[[Msg], Awaitable[None]],
        *,
        queue: str | None = None,
        max_msgs: int = -1,
        pending_msgs_limit: int = 100,
        pending_bytes_limit: int = 1024 * 1024 * 8,
    ) -> QueueSubscriptionWorker:
        ...

    async def subscribe(
        self,
        subject: str,
        cb: Callable[[Msg], Awaitable[None]] | None = None,
        *,
        queue: str | None = None,
        max_msgs: int = -1,
        pending_msgs_limit: int = 100,
        pending_bytes_limit: int = 1024 * 1024 * 8,
    ) -> QueueSubscriptionWorker | QueueSubscriptionIterator:
        """Subscribe to a subject."""
        if cb is None:
            return await self._subscribe_iter(
                subject,
                queue=queue,
                max_msgs=max_msgs,
                pending_msgs_limit=pending_msgs_limit,
                pending_bytes_limit=pending_bytes_limit,
            )
        return await self._subscribe_cb(
            subject,
            cb,
            queue=queue,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

    async def wait_until_closed(self) -> None:
        """Wait until the connection is closed."""
        if not self._closed_event_or_none:
            return None
        await self._closed_event_or_none.wait()

    #############################
    # State utils               #
    #############################

    def _reset(self) -> None:
        self._closed_event_or_none = None
        self._task_group_or_none = None
        self._cancel_event_or_none = None
        self._current_connection_or_none = None
        self._reconnect_rcv_stream_or_none = None
        self._reconnect_send_stream_or_none = None
        self._pending_pongs.clear()
        self._pending_buffer.clear()
        self._subscriptions.clear()
        self._protocol._reset()
        super()._reset()

    def _cancel(self) -> None:
        # Mark client as cancelled
        if self.status != ClientState.CLOSED:
            self.status = ClientState.CLOSING
        # Ask the current connection to close
        if self._current_connection_or_none:
            self._current_connection_or_none.close_soon()
        # Set cancel event
        if self._cancel_event_or_none:
            self._cancel_event_or_none.set()

    def _ensure_cancel_event(self) -> Event:
        """Return the cancel event."""
        if not self._cancel_event_or_none:
            raise RuntimeError("Cancel event not initialized")
        return self._cancel_event_or_none

    def _ensure_task_group(self) -> TaskGroup:
        """Return the task group."""
        if not self._task_group_or_none:
            raise RuntimeError("Task group not initialized")
        return self._task_group_or_none

    def _get_current_connection(self) -> Connection | None:
        """Get the current connection."""
        if self._current_connection_or_none is None:
            return None
        if not self._current_connection_or_none.protocol.is_connected():
            return None
        return self._current_connection_or_none

    async def _wait_for_connection(self) -> Connection:
        """Wait for the next connection to be available."""
        # Check if current connection is available
        if conn := self._get_current_connection():
            return conn
        # Create a new waiter
        waiter = _Waiter()
        self._waiters.append(waiter)
        # Wait for the next connection
        return await waiter.wait()

    #############################
    # Operations helpers        #
    #############################

    async def _subscribe_iter(
        self,
        subject: str,
        queue: str | None = None,
        max_msgs: int = -1,
        pending_msgs_limit: int = 100,
        pending_bytes_limit: int = 1024 * 1024 * 8,
    ) -> QueueSubscriptionIterator:
        if self.is_cancelled():
            raise ConnectionClosedError()
        tg = self._ensure_task_group()
        sid = self._subscriptions.next_sid()
        subscription = QueueSubscriptionIterator(
            client=self,
            id=sid,
            subject=subject,
            queue=queue,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        await tg.start(subscription)
        return subscription

    async def _subscribe_cb(
        self,
        subject: str,
        callback: Callable[[Msg], Awaitable[None]],
        queue: str | None = None,
        max_msgs: int = -1,
        pending_msgs_limit: int = 100,
        pending_bytes_limit: int = 1024 * 1024 * 8,
    ) -> QueueSubscriptionWorker:
        if self.is_cancelled():
            raise ConnectionClosedError()
        tg = self._ensure_task_group()
        sid = self._subscriptions.next_sid()
        subscription = QueueSubscriptionWorker(
            callback=callback,
            client=self,
            id=sid,
            subject=subject,
            queue=queue,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        await tg.start(subscription)
        return subscription

    async def _send_unsubscribe(self, sid: int, limit: int | None = None) -> None:
        """Send an unsubscribe command to the server."""
        conn = self._get_current_connection()
        if conn is None:
            conn = await self._wait_for_connection()
        self._pending_buffer.append(conn.protocol.unsubscribe(sid, limit), False)
        await conn.writer.flush()

    async def _send_subscribe(
        self,
        subject: str,
        queue: str | None,
        sid: int,
    ) -> None:
        """Send a subscribe command to the server."""
        conn = self._get_current_connection()
        if conn is None:
            conn = await self._wait_for_connection()
        self._pending_buffer.append(
            conn.protocol.subscribe(subject, queue or "", sid), False
        )
        await conn.writer.flush()

    async def _send_publish(
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
        await self._write_to_pending_buffer(data)

    async def _write_to_pending_buffer(
        self,
        data: bytes,
        force_flush: bool = False,
        wait_flush: bool = False,
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

        if self._pending_buffer.can_fit(len(data)):
            self._pending_buffer.append(data, priority=priority)
            if force_flush or wait_flush:
                if conn is None:
                    conn = await self._wait_for_connection()
                await conn.writer.flush(wait=wait_flush)
                return
            # If we have a connection, we can kick the flusher
            # when it's empty
            if conn and conn.writer.is_idle():
                await conn.writer.flush(wait=False)
            return

        if conn is None:
            conn = await self._wait_for_connection()

        self._pending_buffer.append(data, priority=priority)

        # Flush if the write queue is empty
        if conn.writer.is_idle():
            await conn.writer.flush(wait=wait_flush)
            return
        # Always flush if the buffer is full
        if force_flush or self._pending_buffer.is_full():
            await conn.writer.flush(wait=wait_flush)
            return

    #############################
    # Long running tasks        #
    #############################

    async def _reconnect_loop(
        self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """Reconnect to the server."""
        parent_tg = self._ensure_task_group()
        # Start the reconnect loop
        while True:
            # Exit the loop if we're closing the client
            if self.is_cancelled():
                return

            # Take a server from the pool
            server = self._protocol.select_server()

            # Update client status
            if server.is_connection_attempt_a_reconnect():
                self.status = ClientState.RECONNECTING
            else:
                self.status = ClientState.CONNECTING

            # Apply reconnect delay
            if self.status == ClientState.RECONNECTING:
                await sleep(1)
                # We should wait for a bit in case of reconnect

            # Create a new connection
            current_connection = Connection.create(self, server)
            self._current_connection_or_none = current_connection

            # Kick-off the connection task
            try:
                async with create_task_group() as tg:
                    # Apply a timeout to the connection
                    # This includes the time to connect and the time to
                    # setup subscriptions.
                    with fail_after(self.options.connect_timeout):
                        # Wait for the connection to be established
                        await tg.start(current_connection)
                        # Create request/reply subscription
                        await self._request_reply._init_request_sub()
                        # Recreate all subscriptions
                        for sub in self._subscriptions._subs.values():
                            # Skip request/reply subscription
                            if sub.sid() == self._request_reply.sid():
                                continue
                            await self._send_subscribe(
                                sub.subject(), sub.queue(), sub.sid()
                            )
                        # Notify that task is started once the connection is
                        # established
                        if self.status == ClientState.CONNECTING:
                            self.status = ClientState.CONNECTED
                            waiters, self._waiters = self._waiters, []
                            for waiter in waiters:
                                waiter.set(current_connection)
                            task_status.started()

            except ExceptionGroup:
                if self.is_cancelled():
                    if not parent_tg.cancel_scope.cancel_called:
                        parent_tg.cancel_scope.cancel()
                    self.status = ClientState.CLOSED
                    return
                self.status = ClientState.DISCONNECTED
                continue

            if self.is_cancelled():
                if not parent_tg.cancel_scope.cancel_called:
                    parent_tg.cancel_scope.cancel()
                self.status = ClientState.CLOSED
                return


class _Waiter:
    def __init__(self) -> None:
        self.event = Event()
        self.connection: Connection | None = None

    async def wait(self) -> Connection:
        await self.event.wait()
        if self.connection is None:
            raise RuntimeError("Connection not set")
        return self.connection

    def set(self, connection: Connection) -> None:
        self.connection = connection
        self.event.set()


class _PendingReply:
    def __init__(self) -> None:
        self.event = Event()
        self.msg: Msg | None = None

    async def wait(self) -> Msg:
        await self.event.wait()
        if self.msg is None:
            raise RuntimeError("Message not set")
        return self.msg

    def set(self, msg: Msg) -> None:
        self.msg = msg
        self.event.set()


class _RequestReplyInbox:
    def __init__(self, client: Client, inbox_prefix: str) -> None:
        self.client = client
        self._resp_map: dict[str, _PendingReply] = {}
        self._inbox_prefix = bytearray(inbox_prefix.encode())
        self._sid: int | None = None
        self.reset()

    def _new_subject(self) -> str:
        return self._resp_sub_prefix.decode() + str(uuid.uuid4())

    def reset(self) -> None:
        self._resp_map.clear()
        self._resp_sub_prefix = self._inbox_prefix[:]
        self._resp_sub_prefix.extend(b".")
        self._resp_sub_prefix.extend("SOME_RANDOM_TOKEN".encode())
        self._resp_sub_prefix.extend(b".")

    async def _init_request_sub(self) -> None:
        self._resp_map = {}
        resp_mux_subject = self._resp_sub_prefix[:]
        resp_mux_subject.extend(b"*")
        sub = await self.client._subscribe_cb(
            resp_mux_subject.decode(), callback=self._request_sub_callback
        )
        self._sid = sub.sid()

    async def _request_sub_callback(self, msg: Msg) -> None:
        token = msg.subject()
        try:
            fut = self._resp_map.get(token)
            if not fut:
                return
            fut.set(msg)
        finally:
            self._resp_map.pop(token, None)

    async def request(
        self,
        subject: str,
        payload: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> Msg:
        fut = _PendingReply()
        reply = self._new_subject()
        self._resp_map[reply] = fut
        await self.client._send_publish(subject, reply, payload, headers)
        try:
            return await fut.wait()
        finally:
            self._resp_map.pop(reply, None)

    def sid(self) -> int:
        if self._sid is None:
            raise RuntimeError("Subscription not initialized")
        return self._sid
