from __future__ import annotations

from ..core.subscription_registry import SubscriptionRegistry
from .client import Client


class SyncSubscriptionRegistry(SubscriptionRegistry):
    """A synchronous subscription registry."""

    client: Client

    def __init__(
        self,
        client: Client,
    ) -> None:
        super().__init__()
        self.client = client
