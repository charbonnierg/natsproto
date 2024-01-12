from __future__ import annotations

from anyio import TASK_STATUS_IGNORED, sleep
from anyio.abc import TaskStatus

from ...errors import ConnectionClosedError, ConnectionStaleError
from .writer import Writer


class Monitor:
    """Monitor the connection and send PINGs to the server.

    Monitor is responsible for sending PINGs to the server
    at the configured interval. If the connection is closed
    or the transport is closed, the monitor will exit.

    If the max outstanding pings is reached, the monitor
    will close the connection and exit.
    """

    def __init__(
        self,
        writer: Writer,
    ) -> None:
        self.ping_interval = writer.protocol.options.ping_interval
        self.protocol = writer.protocol
        self.writer = writer

    async def __call__(
        self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """Run until connection is closed or transport is closed.

        Closes the connection if the max outstanding pings is
        reached.
        """
        # Don't sleep if the connection is closed
        if self.protocol.is_closed():
            return

        # Signal that the task is started
        task_status.started()

        # Enter the loop
        while True:
            if self.protocol.is_cancelled():
                raise ConnectionClosedError

            await sleep(self.ping_interval)

            if self.protocol.is_cancelled():
                raise ConnectionClosedError

            self._check()

            self.writer.write(self.protocol.ping())
            await self.writer.flush(wait=True)

    def _check(self) -> None:
        if self.protocol.exceeded_outstanding_pings_limit():
            if not self.protocol.is_closed():
                self.protocol.receive_eof_from_client()
            raise ConnectionStaleError
