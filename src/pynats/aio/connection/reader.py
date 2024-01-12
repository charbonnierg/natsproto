from __future__ import annotations

from logging import getLogger

from anyio import TASK_STATUS_IGNORED, Event
from anyio.abc import TaskStatus

from ...errors import ConnectionClosedError, TransportError
from ...protocol import ConnectionProtocol, EventType
from ..msg import Msg
from ..subscription_registry import AsyncSubscriptionRegistry
from ..transport import Transport

logger = getLogger("pynats.aio.reader")


class Reader:
    """Read data from the transport, parse it and process it.

    The `Reader` class acts as a task to be started within an
    `anyio` task group.

    When started, it blocks until the first `INFO` protocol message
    is received from the server, which makes it easy to wait until
    the client is allowed to send a `CONNECT` protocol message.

    The fist `INFO` protocol message is received as a `ConnectRequestEvent`
    rather than an `InfoEvent`, so it is easy to distinguish it from
    the following `INFO` protocol messages.

    Same thing goes for the first `PONG` protocol message, which is
    received as a `ConnectedEvent` rather than a `PongEvent`.

    The `Reader` task exits when a `ClosedEvent` is received.
    """

    def __init__(
        self,
        transport: Transport,
        protocol: ConnectionProtocol,
        subscriptions: AsyncSubscriptionRegistry,
        pending_pongs: list[Event],
    ) -> None:
        """Create a new `Reader` instance.

        Args:
            transport: The transport to read data from.
            protocol: The protocol to use to parse data.
            subscriptions: The subscription registry to use to process
                incoming messages.
            pending_pongs: A list of `Event` objects to set when a `PONG`
                protocol message is received from the server.
        """
        self.transport = transport
        self.protocol = protocol
        self.subscriptions = subscriptions
        self.pending_pongs = pending_pongs
        self._info_received: bool = False
        self._connected_event: Event | None = None

    async def wait_until_connected(self) -> None:
        """Wait until the client is connected.

        Client is considered connected after:
        - it received an `INFO` protocol message from the server
        - it sent a `CONNECT` protocol message
        - it sent a `PING` protocol message
        - it received a `PONG` protocol message from the server.

        Note that the `Reader` task blocks until first INFO is received
        when started, so a typical use case is to:
        - start the `Reader` task
        - upgrade to TLS if needed
        - send the `CONNECT` protocol message
        - send the `PING` protocol message
        - wait until the client is connected

        Example:

        ```python
        async with create_task_group() as tg:
            await tg.start(reader)
            await client.connect()
            await client.ping()
            await reader.wait_until_connected()
        ```
        """
        if not self._connected_event:
            raise RuntimeError("Reader not started")
        await self._connected_event.wait()

    async def __call__(
        self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """Execute the `Reader` task.

        Raises:
            `ConnectionClosedError` when a `ClosedEvent` is received.
        """
        # Create the connected event
        self._connected_event = Event()
        # Enter infinite loop
        # Exit when a ClosedEvent is received.
        while True:
            # If there is data to send, send it
            if data_to_send := self.protocol.data_to_send():
                await self._reply(data_to_send)

            # Process events
            for event in self.protocol.events_received():
                # MSG or HMSG protocol messages
                if event.type == EventType.MSG:
                    msg_ = Msg(client=self.subscriptions.client, proto=event.body)
                    try:
                        self.subscriptions.observe(msg_)
                    except Exception:
                        pass
                    continue

                # PONG protocol message (except the first one)
                if event.type == EventType.PONG:
                    if self.pending_pongs:
                        self.pending_pongs.pop(0).set()
                    continue

                # INFO protocol message (except the first one)
                if event.type == EventType.INFO:
                    continue

                # ERROR protocol message
                if event.type == EventType.ERROR:
                    error = event.body
                    if error.is_recoverable():
                        continue
                    error.throw()

                # First INFO protocol message
                if event.type == EventType.CONNECT_REQUEST:
                    self._info_received = True
                    task_status.started()
                    continue

                # First PONG protocol message
                if event.type == EventType.CONNECTED:
                    self._connected_event.set()
                    continue

                # Exit the loop
                if event.type == EventType.CLOSED:
                    logger.warning("exiting reader task due to connection closed")
                    raise ConnectionClosedError

            # Read more data
            if data := await self._read_more():
                logger.warning(f"received data from server: {data!r}")
                self.protocol.receive_data_from_server(data)
                continue

            # EOF received
            logger.warning("received EOF from server")
            self.protocol.receive_eof_from_server()
            continue

    async def _read_more(self) -> bytes | None:
        if self.transport.at_eof():
            if not self.protocol._received_eof:
                self.protocol.receive_eof_from_server()
            return None

        try:
            data_received = await self.transport.read()
        except TransportError:
            if not self.protocol._received_eof:
                self.protocol.receive_eof_from_server()
            return None

        return data_received

    async def _reply(self, data_to_send: bytes) -> None:
        try:
            self.transport.write(data_to_send)
            await self.transport.drain()
        except TransportError:
            if not self.protocol._received_eof:
                self.protocol.receive_eof_from_server()
