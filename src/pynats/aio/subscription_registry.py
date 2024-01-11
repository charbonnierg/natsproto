from __future__ import annotations

from typing import TYPE_CHECKING

from ..core.subscription_registry import SubscriptionRegistry

if TYPE_CHECKING:
    from .client import Client


class AsyncSubscriptionRegistry(SubscriptionRegistry):
    """An asynchronous subscription registry."""

    client: Client

    def __init__(
        self,
        client: Client,
    ) -> None:
        super().__init__()
        self.client = client
