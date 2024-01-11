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

    async def run(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        """Run a single connection task for its entire lifetime."""

        self.cancel_event = Event()
        await self._open_transport()
        try:
            async with create_task_group() as tg:
                await tg.start(self.reader)
                await self._upgrade_transport_if_needed()
                await self._send_connect_command()
                await self.reader.wait_until_connected()
                await tg.start(self.writer)
                await tg.start(self.monitor)
                task_status.started()
                await self.cancel_event.wait()
                tg.cancel_scope.cancel()
        finally:
            await self._close_transport()

    async def _close_transport(self) -> None:
        """Close the transport."""
        with self.pending_buffer.borrow() as pending:
            self.transport.writelines(pending)
            await self.transport.drain()
        self.transport.close()
        await self.transport.wait_closed()

    async def _open_transport(self) -> None:
        """Open the transport."""
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
        """Upgrade the transport to TLS if needed."""
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

    async def _send_connect_command(self) -> None:
        """Send the connect command to the server."""
        # Send the connect message
        self.transport.write(self.protocol.connect())
        self.transport.write(self.protocol.ping())
        await self.transport.drain()

    @classmethod
    def create(
        cls,
        client: Client,
        server: Server,
    ) -> Connection:
        """Create a new connection to the server.

        The created connection is saved as the current connection
        but is not started.
        """
        transport = transport_factory(
            server.uri,
            client.options.read_chunk_size,
        )
        # Create a new reader, writer and monitor
        reader = Reader(
            client.options.read_chunk_size,
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
