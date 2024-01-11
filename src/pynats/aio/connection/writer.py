from __future__ import annotations

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    ClosedResourceError,
    EndOfStream,
    Event,
    create_memory_object_stream,
)
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from pynats.protocol.errors import ConnectionNotEstablishedError

from ...core.pending_buffer import PendingBuffer
from ...errors import ConnectionClosedError
from ...protocol import ConnectionProtocol
from ..transport import Transport


class Writer:
    """Writer is responsible for writing bytes to the transport.

    It runs as a task in the event loop and waits for flush requests
    from the connection. When a flush request is received, it will
    flush the pending buffer to the transport.

    If there is no pending flush request, the writer will wait until
    the next flush request is received.

    The writer task exits when the transport is closed or when the
    connection is closed.
    """

    def __init__(
        self,
        transport: Transport,
        protocol: ConnectionProtocol,
        pending_buffer: PendingBuffer,
    ) -> None:
        self.transport = transport
        self.protocol = protocol
        self.pending_buffer = pending_buffer
        self.flusher_queue_rcv: MemoryObjectReceiveStream[Event] | None = None
        self.flusher_queue_snd: MemoryObjectSendStream[Event] | None = None
        self.flush_queue_pending = 0

    def is_idle(self) -> bool:
        """Return True if writer is idle, I.E, there
        is no pending flush request."""
        return self.flush_queue_pending == 0

    def write(self, data: bytes) -> None:
        """Write data to the transport.

        This method returns immediately, the data will be written
        eventually when the event loop is ran.
        """
        if self.flusher_queue_snd is None:
            raise ConnectionNotEstablishedError
        # Put the data in the pending buffer
        self.pending_buffer.append(data)

    async def flush(self, wait: bool = False) -> None:
        """Request a flush operation to the writer.

        By default, this method returns immediately, the flush operation
        will be performed eventually when the event loop is ran.

        If `wait` is set to `True`, this method will wait until the
        flush operation is completed.
        """
        if self.flusher_queue_snd is None:
            raise ConnectionNotEstablishedError
        # Create a new event
        evt = Event()
        # Put the event in the queue
        try:
            await self.flusher_queue_snd.send(evt)
            self.flush_queue_pending += 1
        except (ClosedResourceError, BrokenResourceError):
            raise ConnectionClosedError
        # Wait for the event to be set
        if wait:
            await evt.wait()

    async def __call__(
        self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """Run until connection is closed or transport is closed."""

        # Safety check
        if self.protocol.is_closed():
            raise ConnectionClosedError

        (
            self.flusher_queue_snd,
            self.flusher_queue_rcv,
        ) = create_memory_object_stream()

        # Notify that task is started once the queue is created
        task_status.started()

        while True:
            if self.protocol.is_cancelled():
                return

            try:
                evt = await self.flusher_queue_rcv.receive()
                self.flush_queue_pending -= 1
            except (EndOfStream, BrokenResourceError):
                if not self.protocol.is_cancelled():
                    self.protocol.receive_eof_from_client()
                return

            if self.pending_buffer.is_empty():
                continue

            try:
                with self.pending_buffer.borrow() as pending:
                    self.transport.writelines(pending)
                    await self.transport.drain()
            finally:
                if not evt.is_set():
                    evt.set()
