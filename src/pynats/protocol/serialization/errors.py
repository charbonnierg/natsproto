from __future__ import annotations

from ..payload import Error, ErrorType


def parse_error(data: bytes) -> Error:
    """Parse an error message from the server.

    Args:
        data: The data to parse.

    Returns:
        The parsed error.
    """

    typ = ErrorType.UNKNOWN
    msg = data.decode().lower().strip()
    for candidate in ErrorType:
        if msg.startswith(candidate.value):
            return Error(candidate, msg)
    return Error(typ, msg)
