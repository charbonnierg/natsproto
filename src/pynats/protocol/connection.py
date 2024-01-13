from __future__ import annotations

import json
from enum import IntEnum
from typing import Iterator

from pynats.errors import MaxPayloadSizeExceededError

from .client_options import ClientOptions
from .connection_state import ConnectionState, ConnectionStateMixin
from .constant import (
    CONNECT_OP_S,
    CRLF,
    CRLF_S,
    CRLF_SIZE,
    ERR_RE,
    HMSG_RE,
    HPUB_OP_S,
    INFO_RE,
    MSG_RE,
    OK_RE,
    PING_CMD,
    PING_RE,
    PONG_CMD,
    PONG_RE,
    PUB_OP_S,
    SUB_OP_S,
    UNSUB_OP_S,
)
from .errors import (
    ConnectionAlreadyEstablishedError,
    ConnectionClosedError,
    ConnectionClosingError,
    ConnectionNotEstablishedError,
    ConnectionServerPoolEmpty,
    ConnectionStateTransitionError,
    InvalidProtocolMessageError,
    UnexpectedProtocolMessageError,
)
from .events import (
    CONNECTED_EVENT,
    PONG_EVENT,
    ClosedEvent,
    ConnectionRequestEvent,
    ErrorEvent,
    Event,
    InfoEvent,
    MsgEvent,
)
from .payload import Info, Message, PendingMessage
from .serialization import encode_headers, parse_error, parse_headers, parse_info
from .server import Server
from .subscription import SubscriptionMap


class ParserState(IntEnum):
    """Parser state."""

    AWAITING_CONTROL_LINE = 1
    """Expecting the next line to begin with a protocol operation."""

    AWAITING_MSG_PAYLOAD = 2
    """Expecting the next line to contain pending message payload."""


class ConnectionProtocol(ConnectionStateMixin):

    """Sans-IO implementation of a NATS connection.

    References:
        - https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-40.md
        - https://beta-docs.nats.io/ref/protocols/client
    """

    def __init__(self, options: ClientOptions) -> None:
        """Initialize a new connection protocol.

        Args:
            options: The client options.

        Raises:
            ValueError: if the client options are invalid.
        """
        super().__init__()
        self.options = options
        self.server_pool = options.new_server_pool()
        # State
        self._parser_state = ParserState.AWAITING_CONTROL_LINE
        self._current_server_or_none: Server | None = None
        self._max_payload_size_or_none: int | None = None
        self._pending_message = PendingMessage()
        self._receive_buffer = bytearray()
        self._closed_event_sent = False
        self._received_eof = False
        self._outstanding_pings = 0
        self._subscriptions = SubscriptionMap()
        # Start parser
        self._events: list[Event] = []
        self._data_to_send: bytes = b""
        self._parser = self.__parse__()
        next(self._parser)

    def __repr__(self) -> str:
        return f"<nats protocol connection state={self._parser_state}>"

    def _reset(self) -> None:
        """Reset the connection."""
        super()._reset()
        self._parser_state = ParserState.AWAITING_CONTROL_LINE
        self._current_server_or_none = None
        self._max_payload_size_or_none = None
        self._pending_message = PendingMessage()
        self._receive_buffer = bytearray()
        self._closed_event_sent = False
        self._received_eof = False
        self._outstanding_pings = 0
        self._subscriptions.clear()
        self._events = []
        self._data_to_send = b""
        self._parser = self.__parse__()
        next(self._parser)

    def get_current_server(self) -> Server | None:
        """Returns the current server."""
        return self._current_server_or_none

    def get_current_server_info(self) -> Info | None:
        """Returns the current server info."""
        if self._current_server_or_none:
            return self._current_server_or_none.info
        return None

    def exceeded_outstanding_pings_limit(self) -> bool:
        """Returns a boolean indicating if the connection is stale."""
        if self.is_closed():
            return False
        return self._outstanding_pings >= self.options.max_outstanding_pings

    def select_server(self) -> Server:
        """Select the next server to connect to.

        This function resets the connection state.

        Raises:
            ConnectionError: When connection is not closed.
            ConnectionClosedError: When no server is available.

        Returns:
            The next server to connect to
        """
        if not self.is_waiting_for_server_selection():
            raise ConnectionStateTransitionError
        # Reset the state
        self._reset()
        # Select the next server from pool
        srv = self.server_pool.next()
        if srv is None:
            # Don't go through the CLOSING state if there
            # is no server available.
            self.status = ConnectionState.CLOSED
            raise ConnectionServerPoolEmpty
        # Update state
        self.mark_as_waiting_for_server_info()
        self._current_server_or_none = srv
        return self._current_server_or_none

    def connect(self) -> bytes:
        """Returns CONNECT command as bytes.

        Raises:
            ConnectionNotEstablishedError: When INFO was not received from server
            ConnectionClosedError: When connection is already closed.
            ConnectionClosingError: When connection is closing.
            AlreadyConnectedError: When CONNECT has alread been sent.

        Returns:
            The wired representation of a CONNECT command.
        """
        if self.is_waiting_for_server_selection() or self.is_waiting_for_server_info():
            raise ConnectionNotEstablishedError
        if self.is_closed():
            raise ConnectionClosedError
        if self.is_closing():
            raise ConnectionClosingError
        if not self.is_waiting_for_client_connect():
            raise ConnectionAlreadyEstablishedError
        # Safety check
        srv = self._current_server_or_none
        if srv is None:
            raise RuntimeError(
                "Please open a bug report. "
                "Current server should always be defined when "
                "connection state is WAITING_FOR_CLIENT_CONNECT"
            )
        # Gather connection options
        options = self.options.get_connect_opts(srv)
        # Encode connect options to JSON
        connect_opts = json.dumps(
            options.to_dict(),
            separators=(",", ":"),
            sort_keys=True,
        )
        # Update state
        self.mark_as_waiting_for_client_ping()
        # Return CONNECT byte string
        return f"{CONNECT_OP_S} {connect_opts}{CRLF_S}".encode()

    def ping(self) -> bytes:
        """Returns PING command as bytes.

        Raises:
            ConnectionNotEstablishedError: When CONNECT has not been sent yet.
            ConnectionClosedError: When connection is already closed.
            ConnectionClosingError: When connection is closing.

        Returns:
            The wired representation of a PING command.
        """
        if (
            self.is_waiting_for_server_selection()
            or self.is_waiting_for_server_info()
            or self.is_waiting_for_client_connect()
        ):
            raise ConnectionNotEstablishedError
        if self.is_closed():
            raise ConnectionClosedError
        if self.is_closing():
            raise ConnectionClosingError
        if self.is_waiting_for_client_ping():
            self.mark_as_waiting_for_server_pong()
        self._outstanding_pings += 1
        return PING_CMD

    def publish(
        self, subject: str, reply: str, payload: bytes, headers: dict[str, str] | None
    ) -> bytes:
        """Returns PUB or HPUB command as bytes.

        Raises:
            ValueError: When payload size exceeds max payload size.

        Returns:
            The wired representation of a PUB or HPUB command.
        """
        _check_subject(subject)
        payload_size = len(payload)
        if self._max_payload_size_or_none:
            if payload_size > self._max_payload_size_or_none:
                raise MaxPayloadSizeExceededError(
                    f"payload size ({payload_size}) exceeds max payload size ({self._max_payload_size_or_none})"
                )
        if not headers:
            return (
                (f"{PUB_OP_S} {subject} {reply} {payload_size}{CRLF_S}".encode())
                + payload
                + CRLF
            )
        encoded_hdr = encode_headers(headers)
        hdr_len = len(encoded_hdr)
        total_size = payload_size + hdr_len
        return (
            (f"{HPUB_OP_S} {subject} {reply} {hdr_len} {total_size}{CRLF_S}".encode())
            + encoded_hdr
            + payload
            + CRLF
        )

    def subscribe(
        self,
        subject: str,
        queue: str,
        sid: int,
        limit: int | None = None,
    ) -> bytes:
        """Returns SUB command as bytes.

        Raises:
            ConnectionNotEstablishedError: When CONNECT has not been sent yet.
            ConnectionClosedError: When connection is already closed.
            ConnectionClosingError: When connection is closing.

        Returns:
            The wired representation of a SUB command.
        """
        _check_subject(subject)
        if (
            self.is_waiting_for_server_selection()
            or self.is_waiting_for_server_info()
            or self.is_waiting_for_client_connect()
        ):
            raise ConnectionNotEstablishedError
        if self.is_closed():
            raise ConnectionClosedError
        if self.is_closing():
            raise ConnectionClosingError
        self._subscriptions.add(sid, limit or 0)
        cmd = f"{SUB_OP_S} {subject} {queue} {sid}{CRLF_S}".encode()
        return cmd

    def unsubscribe(self, sid: int, limit: int | None) -> bytes:
        """Returns UNSUB command as bytes.

        Raises:
            ConnectionNotEstablishedError: When CONNECT has not been sent yet.
            ConnectionClosedError: When connection is already closed.

        Returns:
            The wired representation of an UNSUB command.
        """
        if (
            self.is_waiting_for_server_selection()
            or self.is_waiting_for_server_info()
            or self.is_waiting_for_client_connect()
        ):
            raise ConnectionNotEstablishedError
        if self.is_closed():
            raise ConnectionClosedError
        if not limit:
            self._subscriptions.remove(sid)
        limit_s = "" if limit == 0 else f"{limit}"
        cmd = f"{UNSUB_OP_S} {sid} {limit_s}{CRLF_S}".encode()
        return cmd

    def receive_data_from_server(self, data: bytes) -> None:
        """Receives data from the network and parses it.

        This function raises a RuntimeError if the connection
        is closed.
        """
        if self.is_closed():
            raise ConnectionClosedError
        if self.is_closing():
            raise ConnectionClosingError
        if self.is_waiting_for_server_selection():
            raise ConnectionNotEstablishedError
        self._receive_buffer.extend(data)
        next(self._parser)

    def receive_eof_from_server(self) -> None:
        """Close the connection from server side

        This function can only be called once else it
        raises a RuntimeError.
        """
        if self.is_waiting_for_server_selection():
            raise ConnectionNotEstablishedError
        self._received_eof = True
        self.mark_as_closing()
        try:
            next(self._parser)
        except StopIteration:
            pass

    def receive_eof_from_client(self) -> None:
        """Close the connection from client side.

        This function can only be called once else it
        raises a RuntimeError.
        """
        self._received_eof = True
        self.mark_as_closing()
        try:
            next(self._parser)
        except StopIteration:
            pass

    def events_received(self) -> list[Event]:
        events, self._events = self._events, []
        return events

    def data_to_send(self) -> bytes:
        data, self._data_to_send = self._data_to_send, b""
        return data

    def __parse__(self) -> Iterator[None]:
        """
        Parses the data received and yields control to the caller.

        This function returns a generator that yields tuples of
        (event, data_to_send) where event is an instance of
        `nats_sans_io.events.Event` and data_to_send is the
        bytes to send back to the server.

        It is important to always send back the bytes to the server
        before processing the event, this allows automating the
        process of sending back PONGs.

        This function should never be called twice for a same
        transport. It is up to the caller to ensure that.
        """
        while True:
            # If buffer is empty, yield None, None to indicate
            # that we need more data. It is up to the caller to
            # read more data from the network and call the
            # `receive_data` method before continuing.
            if not self._receive_buffer:
                # Exit if closed
                if self._received_eof:
                    if not self._closed_event_sent:
                        self.mark_as_closed()
                        self._closed_event_sent = True
                        self._events.append(ClosedEvent())
                        yield None
                    return
                # We simply continue here because we expect
                # the caller to have called `receive_data`
                # before continuing iteration.
                yield None
                continue

            # Maybe we're waiting for a control line.
            if self._parser_state == ParserState.AWAITING_CONTROL_LINE:
                # In such case let's check for a message
                if msg := MSG_RE.match(self._receive_buffer):
                    # Just to be sure
                    if self.did_not_expect_msg():
                        raise UnexpectedProtocolMessageError
                    try:
                        subject, sid, _, reply, needed_bytes = msg.groups()
                        self._pending_message.populate(
                            subject,
                            reply,
                            sid,
                            needed_bytes,
                        )
                        del self._receive_buffer[: msg.end()]
                        self._parser_state = ParserState.AWAITING_MSG_PAYLOAD
                    except Exception:
                        raise InvalidProtocolMessageError

                # Or a message with headers
                elif msg := HMSG_RE.match(self._receive_buffer):
                    # Just to be sure
                    if self.did_not_expect_msg():
                        raise UnexpectedProtocolMessageError
                    try:
                        subject, sid, _, reply, header_size, needed_bytes = msg.groups()
                        self._pending_message.populate(
                            subject,
                            reply,
                            sid,
                            needed_bytes,
                            header_size,
                        )
                        del self._receive_buffer[: msg.end()]
                        self._parser_state = ParserState.AWAITING_MSG_PAYLOAD
                    except Exception:
                        raise InvalidProtocolMessageError

                # Or an OK event
                elif ok := OK_RE.match(self._receive_buffer):
                    # Do nothing and just skip.
                    del self._receive_buffer[: ok.end()]

                # Or an error event
                elif err := ERR_RE.match(self._receive_buffer):
                    # Just to be sure
                    if self.did_not_expect_err():
                        raise UnexpectedProtocolMessageError
                    try:
                        err_msg, *_ = err.groups()
                        del self._receive_buffer[: err.end()]
                        error = parse_error(err_msg)
                    except Exception:
                        raise InvalidProtocolMessageError
                    # Mark the connection as closed if the error is not recoverable
                    if not error.is_recoverable():
                        # Don't go through the CLOSING state if error
                        # is not recoverable.
                        self.status = ConnectionState.CLOSED
                    # Always yield the error even if the connection is closed
                    self._events.append(ErrorEvent(error))

                # Or a ping event
                elif ping := PING_RE.match(self._receive_buffer):
                    del self._receive_buffer[: ping.end()]
                    self._data_to_send += PONG_CMD

                # Or a pong event
                elif pong := PONG_RE.match(self._receive_buffer):
                    # Just to be sure
                    if self.did_not_expect_pong():
                        raise UnexpectedProtocolMessageError
                    del self._receive_buffer[: pong.end()]
                    if self._outstanding_pings > 0:
                        self._outstanding_pings -= 1
                    # Mark the connection as connected if we were waiting for a pong
                    if self.status == ConnectionState.WAITING_FOR_SERVER_PONG:
                        srv = self._current_server_or_none
                        if not srv:
                            raise RuntimeError(
                                "Please open a bug report. "
                                "Current server should always be defined"
                            )
                        self.mark_as_connected()
                        srv.observe_connect()
                        self._events.append(CONNECTED_EVENT)
                    else:
                        self._events.append(PONG_EVENT)
                # Or an info message
                elif info := INFO_RE.match(self._receive_buffer):
                    try:
                        info_line = info.groups()[0]
                        del self._receive_buffer[: info.end()]
                        server_info = parse_info(info_line)
                    except Exception:
                        raise InvalidProtocolMessageError
                    if not self._current_server_or_none:
                        raise RuntimeError(
                            "Please open a bug report. "
                            "Current server should always be defined"
                        )
                    self._current_server_or_none.set_info(server_info)
                    self._max_payload_size_or_none = server_info.max_payload
                    # Mark the connection as waiting for pong if we were waiting for info
                    if self.status == ConnectionState.WAITING_FOR_SERVER_INFO:
                        self.mark_as_waiting_for_client_connect()
                        self._events.append(ConnectionRequestEvent(server_info))
                    else:
                        self._events.append(InfoEvent(server_info))

                elif (
                    len(self._receive_buffer) < self.options.max_control_line_size
                    and CRLF in self._receive_buffer
                ):
                    raise InvalidProtocolMessageError

                continue

            if self._parser_state == ParserState.AWAITING_MSG_PAYLOAD:
                # Check if we have enough data in the buffer.
                if (
                    len(self._receive_buffer)
                    < self._pending_message.bytes_needed + CRLF_SIZE
                ):
                    continue

                hdr: dict[str, str] = {}
                subject = self._pending_message.subject
                sid = self._pending_message.sid
                reply = self._pending_message.reply
                bytes_needed = self._pending_message.bytes_needed
                header_bytes_needed = self._pending_message.header_bytes_needed
                # Consume msg payload from buffer and set next parser state.
                if header_bytes_needed > 0:
                    try:
                        hbuf = bytes(self._receive_buffer[:header_bytes_needed])
                        payload = bytes(
                            self._receive_buffer[header_bytes_needed:bytes_needed]
                        )
                        hdr = parse_headers(hbuf)
                        del self._receive_buffer[: bytes_needed + CRLF_SIZE]
                    except Exception:
                        raise InvalidProtocolMessageError
                else:
                    payload = bytes(self._receive_buffer[:bytes_needed])
                    del self._receive_buffer[: bytes_needed + CRLF_SIZE]
                # Reset parser state.
                self._pending_message.reset()
                self._parser_state = ParserState.AWAITING_CONTROL_LINE
                # Check if we should unsubscribe the subscription
                sub = self._subscriptions.get(sid)
                # We're not interested in this message if we do not have
                # a subscription
                if not sub:
                    continue
                sub.increment_delivered()
                if sub.should_unsubscribe():
                    self._subscriptions.remove(sid)
                    data = self.unsubscribe(sid, 0)
                    self._data_to_send += data
                # Yield the message
                self._events.append(
                    MsgEvent(
                        Message(
                            subject=subject,
                            sid=sid,
                            reply=reply,
                            data=payload,
                            data_length=len(payload),
                            hdr=hdr,
                        ),
                    )
                )
                continue


def _check_subject(sub: str) -> None:
    if not sub:
        raise ValueError("subject cannot be empty")
    if " " in sub:
        raise ValueError("subject cannot contain spaces")
