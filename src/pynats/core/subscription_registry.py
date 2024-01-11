from __future__ import annotations

from .msg import Msg
from .subscription import Subscription


class SubscriptionRegistry:
    """SubscriptionRegistry is a helper data structure to hold subscriptions."""

    __slots__ = ["_subs", "_last_sid"]

    def __init__(
        self,
    ) -> None:
        self._subs: dict[int, Subscription] = {}
        self._last_sid = 0

    def next_sid(self) -> int:
        """Get the next available subscription ID.

        Subscription IDs start at 1 and are incremented by 1.

        Returns:
            The next subscription ID.
        """
        self._last_sid += 1
        return self._last_sid

    def add(self, sub: Subscription) -> None:
        """Add a subscription to the registry.

        Args:
            sub: Subscription to add.

        Returns:
            None
        """
        self._subs[sub.sid()] = sub

    def remove(self, sub: int) -> None:
        """Remove a subscription from the registry.

        Args:
            sub: Subscription ID to remove.

        Returns:
            None
        """
        self._subs.pop(sub, None)

    def clear(self) -> None:
        """Clear the registry.

        This will remove all subscriptions from the registry
        and reset the last subscription ID to 0.

        Returns:
            None
        """
        self._subs.clear()
        self._last_sid = 0

    def observe(self, msg: Msg) -> None:
        """Observe a message.

        This will forward the message to the subscription
        which is registered for the message's subscription ID.
        Each subscription may implement its own logic to
        handle the message through its `observe` method.

        Args:
            msg: Message to observe.

        Returns:
            None
        """
        sub = self._subs.get(msg.sid())
        if not sub:
            return
        sub.observe(msg)
