from __future__ import annotations

import abc


class Msg(metaclass=abc.ABCMeta):
    """Interface for a message.

    Both async and sync client implementations define
    their own message classes which inherit from this
    interface.
    """

    @abc.abstractmethod
    def sid(self) -> int:
        """Returns the subscription ID from a message."""
        raise NotImplementedError

    @abc.abstractmethod
    def subject(self) -> str:
        """Returns the subject."""
        raise NotImplementedError

    @abc.abstractmethod
    def reply(self) -> str:
        """Returns the reply."""
        raise NotImplementedError

    @abc.abstractmethod
    def data(self) -> bytes:
        """Returns the data."""
        raise NotImplementedError

    @abc.abstractmethod
    def headers(self) -> dict[str, str]:
        """Returns the headers."""
        raise NotImplementedError

    @abc.abstractmethod
    def size(self) -> int:
        """Returns the size of the message."""
        raise NotImplementedError
