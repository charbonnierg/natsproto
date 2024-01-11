from __future__ import annotations

from asyncio import QueueEmpty
from queue import Queue
from typing import TYPE_CHECKING

from ..core.subscription import Subscription, SubscriptionStatistics
from .msg import Msg

if TYPE_CHECKING:
    from .client import Client


class SyncSubscriptionIterator(Subscription):
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
        self._stats = SubscriptionStatistics()
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_queue: Queue[Msg | None] = Queue(maxsize=pending_msgs_limit)

    def sid(self) -> int:
        return self._id

    def subject(self) -> str:
        return self._subject

    def queue(self) -> str | None:
        return self._queue or None

    def statistics(self) -> SubscriptionStatistics:
        return self._stats

    def peek(self) -> Msg | None:
        try:
            item = self._pending_queue.get_nowait()
        except QueueEmpty:
            return None
        if item is None:
            raise StopIteration

    def next(self, timeout: float | None = None) -> Msg:
        next_item = self._pending_queue.get(timeout=timeout)
        self._pending_queue.task_done()
        if next_item is None:
            raise StopIteration
        return next_item

    def observe(self, msg: Msg) -> None:
        self._pending_queue.put(msg)

    def close(self) -> None:
        self._pending_queue.put(None)
        self._client._subscriptions.remove(self._id)
        self._client._send_unsubscribe(self._id, 0)

    def __iter__(self) -> SyncSubscriptionIterator:
        return self

    def __next__(self) -> Msg:
        return self.next()

    def start(self) -> None:
        self._client._subscriptions.add(self)
        try:
            self._client._send_subscribe(
                self._subject,
                self._queue,
                self._id,
            )
        except BaseException:
            self._client._subscriptions.remove(self._id)
            raise
