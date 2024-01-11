from __future__ import annotations

from anyio import TASK_STATUS_IGNORED, Event
from anyio.abc import TaskStatus

from ...errors import ConnectionClosedError, NatsServerError, TransportError
from ...protocol import ConnectionProtocol, EventType
from ..msg import Msg
from ..subscription_registry import AsyncSubscriptionRegistry
from ..transport import Transport


class Reader:
    """Read data from the transport and parse it.

    Closes the connection when the transport is closed.
    """

    def __init__(
        self,
        read_buffer_size: int,
        transport: Transport,
        protocol: ConnectionProtocol,
        subscriptions: AsyncSubscriptionRegistry,
        pending_pongs: list[Event],
    ) -> None:
        self.read_buffer_size = read_buffer_size
        self.transport = transport
        self.protocol = protocol
        self.subscriptions = subscriptions
        self.pending_pongs = pending_pongs
        self._info_received: bool = False
        self._connected_event: Event | None = None

    async def wait_until_connected(self) -> None:
        if not self._connected_event:
            raise RuntimeError("Reader not started")
        await self._connected_event.wait()

    async def __call__(
        self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        self._connected_event = Event()
        while True:
            # Read events from the parser
            events = self.protocol.events_received()

            # If there is no more event in the parser,
            # read more data from the transport and continue
            if not events:
                if data := await self._read_more():
                    self.protocol.receive_data_from_server(data)
                continue

            # If there is data to send, send it
            data_to_send = self.protocol.data_to_send()
            if data_to_send:
                await self._reply(data_to_send)

            # Process events
            for event in events:
                if event.type == EventType.MSG:
                    msg_ = Msg(client=self.subscriptions.client, proto=event.body)
                    try:
                        self.subscriptions.observe(msg_)
                    except Exception:
                        pass
                    continue

                if event.type == EventType.PONG:
                    if self.pending_pongs:
                        self.pending_pongs.pop(0).set()
                    continue

                if event.type == EventType.INFO:
                    continue

                if event.type == EventType.CONNECT_REQUEST:
                    self._info_received = True
                    task_status.started()
                    continue

                if event.type == EventType.CONNECTED:
                    self._connected_event.set()
                    continue

                if event.type == EventType.ERROR:
                    error = event.body
                    # If the error is recoverable, we can continue
                    if error.is_recoverable():
                        continue
                    raise NatsServerError(error.message)

                if event.type == EventType.CLOSED:
                    raise ConnectionClosedError

    async def _read_more(self) -> bytes | None:
        if self.transport.at_eof():
            if not self.protocol.is_closed():
                self.protocol.receive_eof_from_server()
            return None

        # Read more data
        try:
            data_received = await self.transport.read(self.read_buffer_size)
        except TransportError:
            if not self.protocol.is_closed():
                self.protocol.receive_eof_from_server()
            return None

        return data_received

    async def _reply(self, data_to_send: bytes) -> None:
        try:
            self.transport.write(data_to_send)
            await self.transport.drain()
        except TransportError:
            if not self.protocol.is_closed():
                self.protocol.receive_eof_from_server()
