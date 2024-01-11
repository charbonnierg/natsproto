from __future__ import annotations

import re

# Regular expressions
MSG_RE = re.compile(
    b"\\AMSG\\s+([^\\s]+)\\s+([^\\s]+)\\s+(([^\\s]+)[^\\S\r\n]+)?(\\d+)\r\n"
)
HMSG_RE = re.compile(
    b"\\AHMSG\\s+([^\\s]+)\\s+([^\\s]+)\\s+(([^\\s]+)[^\\S\r\n]+)?([\\d]+)\\s+(\\d+)\r\n"
)
OK_RE = re.compile(b"\\A\\+OK\\s*\r\n")
ERR_RE = re.compile(b"\\A-ERR\\s+('.+')?\r\n")
PING_RE = re.compile(b"\\APING\\s*\r\n")
PONG_RE = re.compile(b"\\APONG\\s*\r\n")
INFO_RE = re.compile(b"\\AINFO\\s+([^\r\n]+)\r\n")

# Protocol operations
INFO_OP = b"INFO"
CONNECT_OP = b"CONNECT"
PUB_OP = b"PUB"
HPUB_OP = b"HPUB"
MSG_OP = b"MSG"
HMSG_OP = b"HMSG"
SUB_OP = b"SUB"
UNSUB_OP = b"UNSUB"
PING_OP = b"PING"
PONG_OP = b"PONG"
OK_OP = b"+OK"
ERR_OP = b"-ERR"

# Protocol syntax
MSG_END = b"\n"
CRLF = b"\r\n"
SPC = 32

# Constant payloads
OK_CMD = OK_OP + CRLF
PING_CMD = PING_OP + CRLF
PONG_CMD = PONG_OP + CRLF

# String constants
CONNECT_OP_S = CONNECT_OP.decode()
PUB_OP_S = PUB_OP.decode()
HPUB_OP_S = HPUB_OP.decode()
SUB_OP_S = SUB_OP.decode()
UNSUB_OP_S = UNSUB_OP.decode()
CRLF_S = CRLF.decode()

# Sizes
CRLF_SIZE = len(CRLF)
OK_SIZE = len(OK_CMD)
PING_SIZE = len(PING_CMD)
PONG_SIZE = len(PONG_CMD)
MSG_OP_SIZE = len(MSG_OP)
ERR_OP_SIZE = len(ERR_OP)

# JetStream Ack
JS_ACK_PREFIX_0 = "$JS"
JS_ACK_PREFIX_1 = "ACK"

# Subject without domain:
# $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
#
JS_ACK_V1_TOKEN_COUNT = 9
# JS Ack V1 Metadata fields
JS_ACK_V1_IDX_STREAM = 2
JS_ACK_V1_IDX_CONSUMER = 3
JS_ACK_V1_IDX_NUM_DELIVERED = 4
JS_ACK_V1_IDX_STREAM_SEQ = 5
JS_ACK_V1_IDX_CON_SEQ = 6
JS_ACK_V1_IDX_TIME_SEQ = 7
JS_ACK_V1_IDX_NUM_PENDING = 8

# Subject with domain:
# $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.
#   <cseq>.<tm>.<pending>.<a token with a random value>
#
JS_ACK_V2_TOKEN_COUNT = 12
# JS Ack V2 Metadata fields
JS_ACK_V2_IDX_DOMAIN = 2
JS_ACK_V2_IDX_ACC_HASH = 3
JS_ACK_V2_IDX_STREAM = 4
JS_ACK_V2_IDX_CONSUMER = 5
JS_ACK_V2_IDX_NUM_DELIVERED = 6
JS_ACK_V2_IDX_STREAM_SEQ = 7
JS_ACK_V2_IDX_CON_SEQ = 8
JS_ACK_V2_IDX_TIME = 9
JS_ACK_V2_IDX_NUM_PENDNG = 10

# JS Operations
JS_ACK_OP_ACK = b"+ACK"
JS_ACK_OP_NACK = b"-NAK"
JS_ACK_OP_PROGRESS = b"+WPI"
JS_ACK_OP_TERM = b"+TERM"

# JetStream heartbeats & control flow headers
NATS_HDR_LINE = bytearray(b"NATS/1.0")
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
NATS_STATUS_HDR = "Status"
NATS_DESCRIPTION_HDR = "Description"
NATS_STATUS_HDR_MSG_LEN = 3  # e.g. 20x, 40x, 50x
NATS_STATUS_HDR_NO_RESPONDER = "503"
NATS_STATUS_HDR_CONTROL = "100"

# States
AWAITING_CONTROL_LINE = 1
AWAITING_MSG_PAYLOAD = 2
