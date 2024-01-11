from __future__ import annotations

from pynats.errors import ConnectionClosedError, NatsServerError

from ...core.pending_buffer import PendingBuffer
from ...protocol import ConnectionProtocol, EventType
from ..transport import Transport


class Connection:
    def __init__(
        self,
        connection: ConnectionProtocol,
        pending_buffer: PendingBuffer,
        transport: Transport,
    ) -> None:
        self.protocol = connection
        self.pending_buffer = pending_buffer
        self.transport = transport

    def _read_more(self, continue_on_timeout: bool = False) -> None:
        while True:
            try:
                received = self.transport.read(4096)
            except TimeoutError:
                if continue_on_timeout:
                    continue
                raise
            break
        if received == b"":
            self.protocol.receive_eof_from_server()
            raise ConnectionClosedError()
        self.protocol.receive_data_from_server(received)

    def _wait_for_connect_request_event(self) -> None:
        # Loop until an error is raised or we return
        while True:
            # Read events
            events = self.protocol.events_received()
            if not events:
                self._read_more()
                continue
            # Reply to server
            data_to_send = self.protocol.data_to_send()
            if data_to_send:
                self.transport.write(data_to_send)
            # Process events
            for event in events:
                if event.type == EventType.CONNECT_REQUEST:
                    return
                else:
                    raise Exception(f"Unexpected event: {event.type}")

    def _wait_for_connected_event(self) -> None:
        while True:
            events = self.protocol.events_received()
            if not events:
                self._read_more()
                continue
            data_to_send = self.protocol.data_to_send()
            if data_to_send:
                self.transport.write(data_to_send)
            for event in events:
                if event.type == EventType.CONNECTED:
                    return
                if event.type == EventType.ERROR:
                    raise NatsServerError(event.body)
                raise Exception(f"Unexpected event: {event.type}")

    def connect(self) -> None:
        self.transport.connect()
        try:
            # First wait for INFO message
            self._wait_for_connect_request_event()
            # Write CONNECT
            self.transport.write(self.protocol.connect())
            # Write PING
            self.transport.write(self.protocol.ping())
            # Then wait for PONG
            self._wait_for_connected_event()
        except BaseException:
            self.transport.close()
            raise

    def close(self) -> None:
        with self.pending_buffer.borrow() as pending:
            self.transport.write(b"".join(pending))
        self.transport.close()

    def publish(
        self, subject: str, payload: bytes, reply: str, headers: dict[str, str] | None
    ) -> None:
        self.transport.write(self.protocol.publish(subject, reply, payload, headers))
