from __future__ import annotations

from typing import TYPE_CHECKING

from anyio import TASK_STATUS_IGNORED, Event, create_task_group
from anyio.abc import TaskGroup, TaskStatus

from pynats.protocol.errors import ConnectionNotEstablishedError

from ...core.pending_buffer import PendingBuffer
from ...protocol import ConnectionProtocol, Server
from ..transport import Transport, transport_factory
from .monitor import Monitor
from .reader import Reader
from .writer import Writer

if TYPE_CHECKING:
    from ..client import Client


class Connection:
    def __init__(
        self,
        pending_buffer: PendingBuffer,
        protocol: ConnectionProtocol,
        transport: Transport,
        reader: Reader,
        writer: Writer,
        monitor: Monitor,
    ) -> None:
        self.pending_buffer = pending_buffer
        self.protocol = protocol
        self.transport = transport
        self.reader = reader
        self.writer = writer
        self.monitor = monitor
        self.task_group: TaskGroup | None = None
        self.cancel_event: Event | None = None

    def close_soon(self) -> None:
        """Request to close the connection.

        This method returns immediately, the connection will be closed
        eventually when the event loop is ran.
        """
        if not self.protocol.is_closed():
            self.protocol.receive_eof_from_client()
        if self.cancel_event and not self.cancel_event.is_set():
            self.cancel_event.set()

    async def __call__(
        self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """Run a single connection task for its entire lifetime."""

        self.cancel_event = Event()
        await self._open_transport()
        try:
            async with create_task_group() as tg:
                await tg.start(self.reader)
                await self._upgrade_transport_if_needed()
                self.transport.write(self.protocol.connect())
                self.transport.write(self.protocol.ping())
                await self.transport.drain()
                await self.reader.wait_until_connected()
                await tg.start(self.writer)
                await tg.start(self.monitor)
                task_status.started()
                await self.cancel_event.wait()
                tg.cancel_scope.cancel()
        finally:
            await self._close_transport()
            # FIXME: This is a bit weird, it's the only place in client
            # where we call .mark_as_something() on the protocol
            # instance. All other state transitions are handled
            # within the protocol itself
            # The problem is that connection protocol ends in a CLOSED
            # state, but needs to be in a WAITING_FOR_SERVER_SELECTION
            # state in order to reconnect. So for now, we mark it
            # manually here.
            if not self.protocol.is_cancelled():
                self.protocol.mark_as_closing()
            self.protocol.mark_as_closed()
            self.protocol.mark_as_waiting_for_server_selection()

    async def _close_transport(self) -> None:
        with self.pending_buffer.borrow() as pending:
            self.transport.writelines(pending)
            await self.transport.drain()
        self.transport.close()
        await self.transport.wait_closed()

    async def _open_transport(self) -> None:
        # Check if a TLS upgrade is required
        tls_upgrade_required = (
            self.protocol.options.ssl_context and self.transport.requires_tls_upgrade()
        )
        # TLS is asked and we must do the handshake first
        if (
            self.protocol.options.ssl_context
            and self.protocol.options.tls_handshake_first
        ):
            # If the transport requires a TLS upgrade, do a
            # TCP connection first
            if tls_upgrade_required:
                await self.transport.connect()
            # Connect using TLS or upgrade to TLS
            await self.transport.connect_tls(
                ssl_context=self.protocol.options.ssl_context(),
            )
        # Else, just connect, we'll upgrade to TLS later if needed
        else:
            await self.transport.connect()

    async def _upgrade_transport_if_needed(self) -> None:
        info = self.protocol.get_current_server_info()
        if not info:
            raise ConnectionNotEstablishedError
        # Check if a TLS upgrade is required
        if not self.transport.requires_tls_upgrade():
            return
        if not self.protocol.options.ssl_context:
            if not (info.tls_required or self.protocol.options.tls_required):
                return
            raise RuntimeError("TLS upgrade requested but no SSL context provided")
        # Upgrade to TLS
        await self.transport.drain()
        await self.transport.connect_tls(
            ssl_context=self.protocol.options.ssl_context(),
        )

    @classmethod
    def create(
        cls,
        client: Client,
        server: Server,
    ) -> Connection:
        """Create a new connection to be used with given server."""
        transport = transport_factory(
            server.uri,
            client.options.rcv_buffer_size,
        )
        # Create a new reader, writer and monitor
        reader = Reader(
            transport,
            client._protocol,
            client._subscriptions,
            client._pending_pongs,
        )
        writer = Writer(
            transport,
            client._protocol,
            client._pending_buffer,
        )
        monitor = Monitor(writer)
        # Create a new connection
        current_connection = Connection(
            pending_buffer=client._pending_buffer,
            protocol=client._protocol,
            transport=transport,
            reader=reader,
            writer=writer,
            monitor=monitor,
        )
        # Return the current connection
        return current_connection
