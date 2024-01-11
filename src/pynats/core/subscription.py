from __future__ import annotations

import abc

from .msg import Msg


class SubscriptionStatistics:
    __slots__ = ["pending_msgs", "pending_bytes", "delivered"]

    def __init__(self) -> None:
        self.pending_msgs = 0
        self.pending_bytes = 0
        self.delivered = 0

    def observe_message_received(self, msg: Msg) -> None:
        self.pending_msgs += 1
        self.pending_bytes += msg.size()

    def observe_message_processed(self, msg: Msg) -> None:
        self.pending_msgs -= 1
        self.pending_bytes -= msg.size()
        self.delivered += 1


class Subscription(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def sid(self) -> int:
        """
        Returns the subscription ID from a message.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def subject(self) -> str:
        """
        Returns the subject of the `Subscription`.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def queue(self) -> str | None:
        """
        Returns the queue name of the `Subscription` if part of a queue group.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def statistics(self) -> SubscriptionStatistics:
        """
        Returns the statistics of the `Subscription`.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def observe(self, msg: Msg) -> None:
        """
        Observe a message.
        """
        raise NotImplementedError
