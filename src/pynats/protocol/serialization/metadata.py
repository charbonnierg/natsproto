from __future__ import annotations

import datetime

from ..constant import (
    JS_ACK_PREFIX_0,
    JS_ACK_PREFIX_1,
    JS_ACK_V1_IDX_CON_SEQ,
    JS_ACK_V1_IDX_CONSUMER,
    JS_ACK_V1_IDX_NUM_DELIVERED,
    JS_ACK_V1_IDX_NUM_PENDING,
    JS_ACK_V1_IDX_STREAM,
    JS_ACK_V1_IDX_STREAM_SEQ,
    JS_ACK_V1_IDX_TIME_SEQ,
    JS_ACK_V1_TOKEN_COUNT,
    JS_ACK_V2_IDX_CON_SEQ,
    JS_ACK_V2_IDX_CONSUMER,
    JS_ACK_V2_IDX_DOMAIN,
    JS_ACK_V2_IDX_NUM_DELIVERED,
    JS_ACK_V2_IDX_NUM_PENDNG,
    JS_ACK_V2_IDX_STREAM,
    JS_ACK_V2_IDX_STREAM_SEQ,
    JS_ACK_V2_IDX_TIME,
    JS_ACK_V2_TOKEN_COUNT,
    NATS_DESCRIPTION_HDR,
    NATS_STATUS_HDR,
    NATS_STATUS_HDR_CONTROL,
)
from ..payload import ControlType, Message, Metadata, Sequence


def extract_metadata_fields(reply: str) -> list[str] | None:
    tokens = reply.split(".")
    if (
        (
            len(tokens) == JS_ACK_V2_TOKEN_COUNT
            or len(tokens) >= JS_ACK_V2_TOKEN_COUNT - 1
        )
        and tokens[0] == JS_ACK_PREFIX_0
        and tokens[1] == JS_ACK_PREFIX_1
    ):
        return tokens
    return None


def parse_sequence(reply: str) -> Sequence | None:
    tokens = extract_metadata_fields(reply)
    if tokens is None:
        return None
    if len(tokens) == JS_ACK_V1_TOKEN_COUNT:
        return Sequence(
            stream=int(tokens[JS_ACK_V1_IDX_STREAM_SEQ]),
            consumer=int(tokens[JS_ACK_V1_IDX_CON_SEQ]),
        )
    else:
        return Sequence(
            stream=int(tokens[JS_ACK_V2_IDX_STREAM_SEQ]),
            consumer=int(tokens[JS_ACK_V2_IDX_CON_SEQ]),
        )


def parse_metadata(reply: str) -> Metadata | None:
    """Construct the metadata from the reply string"""
    tokens = extract_metadata_fields(reply)
    if tokens is None:
        return None
    if len(tokens) == JS_ACK_V1_TOKEN_COUNT:
        t = datetime.datetime.fromtimestamp(
            int(tokens[JS_ACK_V1_IDX_TIME_SEQ]) / 1_000_000_000.0
        )
        return Metadata(
            stream_sequence=int(tokens[JS_ACK_V1_IDX_STREAM_SEQ]),
            consumer_sequence=int(tokens[JS_ACK_V1_IDX_CON_SEQ]),
            num_delivered=int(tokens[JS_ACK_V1_IDX_NUM_DELIVERED]),
            num_pending=int(tokens[JS_ACK_V1_IDX_NUM_PENDING]),
            timestamp=t,
            stream=tokens[JS_ACK_V1_IDX_STREAM],
            consumer=tokens[JS_ACK_V1_IDX_CONSUMER],
            domain=None,
        )
    else:
        t = datetime.datetime.fromtimestamp(
            int(tokens[JS_ACK_V2_IDX_TIME]) / 1_000_000_000.0
        )

        # Underscore indicate no domain is set. Expose as empty string
        # to client.
        domain = tokens[JS_ACK_V2_IDX_DOMAIN]
        if domain == "_":
            domain = ""

        return Metadata(
            stream_sequence=int(tokens[JS_ACK_V2_IDX_STREAM_SEQ]),
            consumer_sequence=int(tokens[JS_ACK_V2_IDX_CON_SEQ]),
            num_delivered=int(tokens[JS_ACK_V2_IDX_NUM_DELIVERED]),
            num_pending=int(tokens[JS_ACK_V2_IDX_NUM_PENDNG]),
            timestamp=t,
            stream=tokens[JS_ACK_V2_IDX_STREAM],
            consumer=tokens[JS_ACK_V2_IDX_CONSUMER],
            domain=domain,
        )


def parse_control_type(msg: Message) -> ControlType | None:
    if msg.data_length > 0:
        return None

    status = msg.hdr.get(NATS_STATUS_HDR)
    if status != NATS_STATUS_HDR_CONTROL:
        return None

    desc = msg.hdr.get(NATS_DESCRIPTION_HDR)
    if desc == ControlType.HEARTBEAT:
        return ControlType.HEARTBEAT
    elif desc == ControlType.FLOW:
        return ControlType.FLOW
    return ControlType.NONE
