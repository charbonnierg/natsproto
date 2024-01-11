# Pynats

> Disclaimer: Experimental and work-in-progress. Inspired by [this article](https://fractalideas.com/blog/sans-io-when-rubber-meets-road/) from the author of the [websocket](https://websockets.readthedocs.io/en/stable/).

## Sans IO

This project aims at providing a [sans-IO protocol](https://sans-io.readthedocs.io/) implementation for NATS.

If you don't want to read the `README`, you can go straight to :

- [`connection_state.py`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/protocol/connection_state.py) and [`connection.py`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/protocol/connection.py) modules to see how the sans-io protocol is implemented.

- [`aio.connection.reader`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/aio/connection/reader.py): to see how an async reader is implemented on top of the sans-io protocol

- [`aio.connection.writer`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/aio/connection/writer.py): to see how an async writer is implemented on top of the sans-io protocol

- [`aio.connection.monitor`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/aio/connection/monitor.py): to see how a PING monitor is implemented on top of the sans-io protocol

- [`aio.connection.connection`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/aio/connection/connection.py): to see how a single NATS connection lifetime is implemented on top of the sans-io protocol.

- [`aio.client`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/aio/client.py): to see how an async client is implemented on top of all other modules cited above.

### Introduction to sans-IO

I can't explain why sans-io protocol are useful better than the [sans-io website](https://sans-io.readthedocs.io/how-to-sans-io.html) from [Brett Cannon](https://github.com/brettcannon).

> An I/O-free protocol implementation (colloquially referred to as a “sans-IO” implementation) is an implementation of a network protocol that contains no code that does any form of network I/O or any form of asynchronous flow control.

### Usage

- A `ConnectionProtocol` can be created out of `ClientOptions`:

```python
proto = ConnectionProtocol(ClientOptions())
```

- Once a `ConnectionProtocol` instance is created, it is necessary to call the `select_server` method:

```python
srv = proto.select_server()
```

- Once a server is selected, a transport (any implementation of the Transport interface) must be opened for the server. This part is pseudo-code:

```python
transport = MyFancyTransport()
# Transport can be async
await transport.open()
# Or sync
# transport.open()
```

- Once transport is opened, read from transport:

```python
data = await transport.read()
```

- Once data is received, feed the `ConnectionProtocol` instance:

```python
proto.receive_data_from_server(data)
```

- After receiving data, it is mandatory to check if there is data to send back to the server:

```python
data_to_send = proto.data_to_send()
if data_to_send:
    transport.write(data_to_send)
```

> This allows automating replying to `PING` requests with `PONG` commands without any knowledge from the client.

- Iterate over the events received. At this point in the connection, the only events than can be received are `ClosedEvent` and `ConnectRequestEvent`. A `ConnectRequestEvent` corresponds to the first `INFO` received from server. It exists to distinguish between the first `INFO` and other `INFO` messages received while connection is already established:

```python
events = proto.events_received()
for event in events:
    if event == EventType.CLOSED:
        raise Exception("Connection closed")
    # The first event to come is guaranteed to be a CONNECT_REQUEST but there is no way to tell that to typecheckers and IDEs.
    assert event.type == EventType.CONNECT_REQUEST
    break
```

- Now that we received server info, we can send a `CONNECT` command:

```python
transport.write(proto.connect())
```

- Right after connect, we must send a `PING`, and drain the transport in case of async transport:

```python
transport.write(proto.ping())
await transport.drain()
```

- Finally wait for a reply from the server:

```python
while True:
    events = proto.events_received()

    # Read more
    if not events:
        data = await transport.read()
        proto.receive_data_from_server(data)
        continue

    # Auto-reply
    data_to_send = proto.data_to_send()
    if data_to_send:
        transport.write(data_to_send)

    # Process events
    for event in events:
        if event.type == EventType.CLOSED:
            raise Exception("Connection closed")
        if event.type == EventType.CONNECTED:
            return
        if event.type == EventType.ERROR:
            raise Exception(event.body.message)
```

- And that's it we're connected ! We can now send a publish for example:

```python
transport.write(
    proto.publish("foo", reply="", payload=b"", headers={})
)
await transport.drain()
```

- Or send a subscribe command:

```python
transport.write(
    proto.subscribe("foo", queue="", sid=1)
)
await transport.drain()
```

- Or an unsubscribe command:

```python
transport.write(
    proto.unsubscribe("foo", limit=0)
)
await transport.drain()
```

- Finally, when transport is closed, we must notify the `ConnectionInstance`:

```python
try:
    await transport.read()
except OSError:
    proto.receive_eof_from_transport()
    raise
```

## Asynchronous

An `anyio` implementation of an async client is available in [`aio/client.py`](https://github.com/charbonnierg/natsproto/blob/main/src/pynats/aio/client.py). It is very experimental and likely full of bugs.

The goal of this repo is not to provide a production ready client imlementation, instead it's to give ideas about how an async client can be implemented on top of a sans-io protocol.

## Synchronous

Some code exist in `io/client.py` but it's even more experimental than what exists in async counterpart. I just wanted to make sure than I can connect and publish, but I didn't decide on any API yet. I wanted to avoid threads, but don't see how we can do subscribes without them.
