from __future__ import annotations


class PendingMessage:
    """PendingMessage contains all data found in a MSG or a HMSG
    control line."""

    __slots__ = ["subject", "reply", "sid", "header_bytes_needed", "bytes_needed"]

    def __init__(self):
        self.subject = ""
        self.reply = ""
        self.sid = 0
        self.header_bytes_needed = 0
        self.bytes_needed = 0

    def populate(
        self,
        subject: bytes,
        reply: bytes | None,
        sid: bytes,
        needed_bytes: bytes,
        header_size: bytes | None = None,
    ) -> None:
        self.subject = subject.decode()
        if reply:
            self.reply = reply.decode()
        else:
            self.reply = ""
        if header_size:
            self.header_bytes_needed = int(header_size)
        else:
            self.header_bytes_needed = 0
        self.sid = int(sid)
        self.bytes_needed = int(needed_bytes)

    def reset(self) -> None:
        self.subject = ""
        self.reply = ""
        self.sid = 0
        self.header_bytes_needed = 0
        self.bytes_needed = 0


class Message:
    __slots__ = [
        "subject",
        "sid",
        "reply",
        "data",
        "data_length",
        "hdr",
    ]

    def __init__(
        self,
        subject: str,
        sid: int,
        reply: str,
        data: bytes,
        data_length: int,
        hdr: dict[str, str],
    ) -> None:
        self.subject = subject
        self.sid = sid
        self.reply = reply
        self.data = data
        self.data_length = data_length
        self.hdr = hdr
