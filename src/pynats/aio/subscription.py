from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING, AsyncIterator, Awaitable, Callable, Optional

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    EndOfStream,
    Event,
    WouldBlock,
    create_memory_object_stream,
    create_task_group,
)
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from pynats.errors import SubscriptionNotStartedError, SubscriptionSlowConsumerError

from ..core.subscription import Subscription as SubscriptionABC
from ..core.subscription import SubscriptionStatistics

if TYPE_CHECKING:
    from .client import Client
    from .msg import Msg

# Default Pending Limits of Subscriptions
DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 128 * 1024 * 1024


logger = logging.getLogger("nats.aio.subscription")


class _JSI:
    pass


class Subscription(SubscriptionABC):
    @abc.abstractmethod
    async def drain(self):
        """
        Removes interest in a subject, but will process remaining messages.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def unsubscribe(self, limit: int = 0):
        """
        :param limit: Max number of messages to receive before unsubscribing.

        Removes interest in a subject, remaining messages will be discarded.

        If `limit` is greater than zero, interest is not immediately removed,
        rather, interest will be automatically removed after `limit` messages
        are received.
        """

    @abc.abstractmethod
    async def __call__(
        self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """Run the subscription"""
        raise NotImplementedError

    async def __aenter__(self) -> SubscriptionABC:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.drain()


class BaseQueueSubscription(Subscription):
    def __init__(
        self,
        client: Client,
        id: int,
        subject: str,
        queue: str | None,
        max_msgs: int,
        pending_msgs_limit: int,
        pending_bytes_limit: int,
    ) -> None:
        self._client = client
        self._id = id
        self._subject = subject
        self._queue = queue
        self._max_msgs = max_msgs
        self._received = 0
        self._closed: Event | None = None
        # Per subscription message processor.
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_queue_rcv: MemoryObjectReceiveStream[Msg] | None = None
        self._pending_queue_snd: MemoryObjectSendStream[Msg] | None = None
        self._stats = SubscriptionStatistics()
        # For JetStream enabled subscriptions.
        self._jsi: Optional[_JSI] = None

    def sid(self) -> int:
        return self._id

    def subject(self) -> str:
        return self._subject

    def queue(self) -> str | None:
        return self._queue or None

    def statistics(self) -> SubscriptionStatistics:
        return self._stats

    async def drain(self):
        """
        Removes interest in a subject, but will process remaining messages.
        """
        # Start by closing the sender
        if self._pending_queue_snd:
            self._pending_queue_snd.close()
        if not self._closed:
            return
        await self._closed.wait()

    async def unsubscribe(self, limit: int = 0):
        if not self._closed:
            raise SubscriptionNotStartedError()
        if limit == 0:
            if self._pending_queue_snd:
                await self._pending_queue_snd.aclose()
            if self._pending_queue_rcv:
                await self._pending_queue_rcv.aclose()
            if self._closed and not self._closed.is_set():
                self._closed.set()
            return
        else:
            self._max_msgs = self._stats.delivered + limit
            await self._closed.wait()

    def observe(self, msg: Msg) -> None:
        """
        Processes a message.
        """
        if self._pending_queue_snd is None:
            raise SubscriptionNotStartedError()
        if self._closed and self._closed.is_set():
            self._client._subscriptions.remove(self._id)
        try:
            self._pending_queue_snd.send_nowait(msg)
        except WouldBlock:
            raise SubscriptionSlowConsumerError(self)
        self._stats.observe_message_received(msg)


class QueueSubscriptionIterator(BaseQueueSubscription):
    async def messages(self) -> AsyncIterator[Msg]:
        if self._pending_queue_rcv is None:
            raise SubscriptionNotStartedError()
        async for msg in self._pending_queue_rcv:
            try:
                yield msg
            finally:
                self._stats.observe_message_processed(msg)
                if self._max_msgs > 0 and self._stats.delivered >= self._max_msgs:
                    try:
                        self._client._subscriptions.remove(self._id)
                        await self._client._send_unsubscribe(self._id, 0)
                    finally:
                        if self._closed and not self._closed.is_set():
                            self._closed.set()

    async def next_message(self) -> Msg:
        if self._pending_queue_rcv is None:
            raise SubscriptionNotStartedError()
        msg = await self._pending_queue_rcv.receive()
        self._stats.observe_message_processed(msg)
        if self._max_msgs > 0 and self._stats.delivered >= self._max_msgs:
            try:
                self._client._subscriptions.remove(self._id)
                await self._client._send_unsubscribe(self._id, 0)
            finally:
                if self._closed and not self._closed.is_set():
                    self._closed.set()
        return msg

    async def __call__(self, task_status: TaskStatus = TASK_STATUS_IGNORED) -> None:
        self._closed = Event()
        self._pending_queue_snd, self._pending_queue_rcv = create_memory_object_stream(
            self._pending_msgs_limit
        )
        self._client._subscriptions.add(self)
        try:
            await self._client._send_subscribe(self._subject, self._queue, self._id)
        except BaseException:
            self._client._subscriptions.remove(self._id)
            raise
        task_status.started()
        await self._closed.wait()


class QueueSubscriptionWorker(BaseQueueSubscription):
    def __init__(
        self,
        callback: Callable[[Msg], Awaitable[None]],
        client: Client,
        id: int,
        subject: str,
        queue: str | None,
        max_msgs: int,
        pending_msgs_limit: int,
        pending_bytes_limit: int,
    ) -> None:
        super().__init__(
            client,
            id,
            subject,
            queue,
            max_msgs,
            pending_msgs_limit,
            pending_bytes_limit,
        )
        self._callback = callback

    async def __call__(self, task_status: TaskStatus = TASK_STATUS_IGNORED) -> None:
        """
        Starts the subscription to receive messages.
        """
        async with create_task_group() as tg:
            self._closed = Event()
            # Create the pending queue
            (
                self._pending_queue_snd,
                self._pending_queue_rcv,
            ) = create_memory_object_stream(self._pending_msgs_limit)
            # Start processing messages
            tg.start_soon(self._loop)
            # Start subscription
            self._client._subscriptions.add(self)
            try:
                await self._client._send_subscribe(self._subject, self._queue, self._id)
            except BaseException:
                self._client._subscriptions.remove(self._id)
                raise
            task_status.started()

    async def _loop(self) -> None:
        try:
            await self._wait_for_msgs()
        finally:
            if self._closed and not self._closed.is_set():
                self._closed.set()

    async def _wait_for_msgs(self) -> None:
        if self._pending_queue_rcv is None:
            raise SubscriptionNotStartedError()
        while True:
            try:
                msg = await self._pending_queue_rcv.receive()
            except (EndOfStream, BrokenResourceError):
                return
            self._stats.observe_message_processed(msg)
            try:
                await self._callback(msg)
            except Exception as exc:
                logger.error(
                    "Unhandled exception catched in subscription callback", exc_info=exc
                )
