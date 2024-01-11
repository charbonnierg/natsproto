from __future__ import annotations

import string
from email.parser import BytesParser

from ..constant import (
    CRLF,
    CRLF_SIZE,
    NATS_DESCRIPTION_HDR,
    NATS_HDR_LINE,
    NATS_HDR_LINE_SIZE,
    NATS_STATUS_HDR,
    NATS_STATUS_HDR_MSG_LEN,
    SPC,
)

try:
    from fast_mail_parser import parse_email
except ImportError:
    parse_email = None


BYTES_PARSER = BytesParser()


def encode_headers(headers: dict[str, str] | None) -> bytes:
    hdr = bytearray()
    hdr.extend(NATS_HDR_LINE)
    hdr.extend(CRLF)
    if not headers:
        hdr.extend(CRLF)
        return bytes(hdr)
    for k, v in headers.items():
        key = k.strip()
        if not key:
            # Skip empty keys
            continue
        hdr.extend(key.encode())
        hdr.extend(b": ")
        value = v.strip()
        hdr.extend(value.encode())
        hdr.extend(CRLF)
    hdr.extend(CRLF)
    return bytes(hdr)


def parse_headers(headers: bytes | None) -> dict[str, str]:
    if not headers:
        return {}

    hdr: dict[str, str] | None = None
    raw_headers = headers[NATS_HDR_LINE_SIZE:]

    # If the first character is an empty space, then this is
    # an inline status message sent by the server.
    #
    # NATS/1.0 404\r\n\r\n
    # NATS/1.0 503\r\n\r\n
    # NATS/1.0 404 No Messages\r\n\r\n
    #
    # Note: it is possible to receive a message with both inline status
    # and a set of headers.
    #
    # NATS/1.0 100\r\nIdle Heartbeat\r\nNats-Last-Consumer: 1016\r\nNats-Last-Stream: 1024\r\n\r\n
    #
    if raw_headers[0] == SPC:
        # Special handling for status messages.
        line = headers[len(NATS_HDR_LINE) + 1 :]
        status = line[:NATS_STATUS_HDR_MSG_LEN]
        desc = line[NATS_STATUS_HDR_MSG_LEN + 1 : len(line) - CRLF_SIZE - CRLF_SIZE]
        stripped_status = status.strip().decode()

        # Process as status only when it is a valid integer.
        hdr = {}
        if stripped_status.isdigit():
            hdr[NATS_STATUS_HDR] = stripped_status

        # Move the raw_headers to end of line
        i = raw_headers.find(CRLF)
        raw_headers = raw_headers[i + CRLF_SIZE :]

        if len(desc) > 0:
            # Heartbeat messages can have both headers and inline status,
            # check that there are no pending headers to be parsed.
            i = desc.find(CRLF)
            if i > 0:
                hdr[NATS_DESCRIPTION_HDR] = desc[:i].decode()
                parsed_hdr = BYTES_PARSER.parsebytes(desc[i + CRLF_SIZE :])
                for k, v in parsed_hdr.items():
                    hdr[k] = v
            else:
                # Just inline status...
                hdr[NATS_DESCRIPTION_HDR] = desc.decode()

    if not len(raw_headers) > CRLF_SIZE:
        return {}

    #
    # Example header without status:
    #
    # NATS/1.0\r\nfoo: bar\r\nhello: world
    #
    raw_headers = headers[NATS_HDR_LINE_SIZE + CRLF_SIZE :]
    if parse_email:
        parsed_hdr = parse_email(raw_headers).headers
    else:
        parsed_hdr = {
            k.strip(): v.strip()
            for k, v in BYTES_PARSER.parsebytes(raw_headers).items()
        }
    if hdr:
        hdr.update(parsed_hdr)
    else:
        hdr = parsed_hdr

    if parse_email:
        to_delete = []
        for k in hdr.keys():
            if any(c in k for c in string.whitespace):
                to_delete.append(k)
        for k in to_delete:
            del hdr[k]
    return {}
