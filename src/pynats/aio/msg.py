from __future__ import annotations

from typing import TYPE_CHECKING

from ..core.msg import Msg as MsgABC
from ..errors import NoReplySubjectError
from ..protocol import Message as MessageProtocol

if TYPE_CHECKING:
    from .client import Client


class Msg(MsgABC):
    """
    Msg represents a message delivered by NATS.
    """

    __slots__ = [
        "_client",
        "_subject",
        "_reply",
        "_data",
        "_data_size",
        "_headers",
        "_sid",
    ]

    def __init__(self, client: Client, proto: MessageProtocol) -> None:
        self._client = client
        self._subject = proto.subject
        self._reply = proto.reply
        self._data = proto.data
        self._data_size = proto.data_length
        self._headers = proto.hdr
        self._sid = proto.sid

    def __repr__(self) -> str:
        return (
            f"Msg(sid={self._sid}, subject={self._subject}, "
            f"reply={self._reply}, size={self._data_size}, "
            f"headers={self._headers})"
        )

    def sid(self) -> int:
        return self._sid

    def subject(self) -> str:
        return self._subject

    def reply(self) -> str:
        return self._reply

    def data(self) -> bytes:
        return self._data

    def size(self) -> int:
        return self._data_size

    def headers(self) -> dict[str, str]:
        return self._headers

    async def respond(
        self,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        respond replies to the inbox of the message if there is one.
        """
        if not self.reply:
            raise NoReplySubjectError()
        await self._client.publish(
            subject=self._reply, payload=data or b"", reply="", headers=headers
        )
