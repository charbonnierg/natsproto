from __future__ import annotations


class Subscription:
    __slots__ = ["sid", "limit", "delivered"]

    def __init__(self, sid: int, limit: int = 0) -> None:
        self.sid = sid
        self.limit = limit
        self.delivered = 0

    def should_unsubscribe(self) -> bool:
        if self.limit <= 0:
            return False
        return self.delivered >= self.limit

    def set_limit(self, limit: int) -> None:
        self.limit = limit

    def increment_delivered(self) -> None:
        self.delivered += 1


class SubscriptionMap:
    __slots__ = ["_subs"]

    def __init__(self) -> None:
        self._subs: dict[int, Subscription] = {}

    def get(self, sid: int) -> Subscription | None:
        return self._subs.get(sid)

    def add(self, sid: int, limit: int = 0) -> None:
        if sid in self._subs:
            raise Exception("sid already exists")
        self._subs[sid] = Subscription(sid, limit)

    def remove(self, sid: int) -> None:
        self._subs.pop(sid, None)

    def clear(self) -> None:
        self._subs.clear()
