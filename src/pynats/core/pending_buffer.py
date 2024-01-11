from __future__ import annotations


class PendingBuffer:
    """PendingBuffer is a helper data structure to hold pending commands to be sent to the server."""

    __slots__ = ["data", "size", "max_size"]

    def __init__(self, max_size: int) -> None:
        """Initialize the pending buffer.

        Args:
            max_size: Maximum size of the buffer. If the buffer size exceeds this value, it will be considered full,
                but appending data will still be allowed.
        """
        self.data: list[bytes] = []
        self.max_size = max_size
        self.size = 0

    def borrow(self) -> _BorrowedBuffer:
        """Borrow all the data from the buffer and restore it on context
        exit if an exception is raised.

        Returns:
            A context manager that will empty the buffer on enter and restore it on exit when an exception is raised.
        """
        return _BorrowedBuffer(self)

    def append(self, data: bytes, priority: bool = False) -> None:
        """Append data to the buffer.

        Args:
            data: Data to append.
            priority: If True, the data will be inserted at the beginning of the buffer.

        Returns:
            The size of the buffer after appending the data.
        """
        if priority:
            self.data.insert(0, data)
        else:
            self.data.append(data)
        self.size += len(data)

    def extend(self, data: list[bytes], priority: bool = False) -> None:
        """Extend the buffer with data at the beginning.

        Args:
            data: Data to append.
            priority: If True, the data will be inserted at the beginning of the buffer.

        Returns:
            The size of the buffer after extending it with the data.
        """
        if priority:
            self.data = data + self.data
        else:
            self.data.extend(data)
        self.size += sum(len(v) for v in data)

    def clear(self) -> None:
        """Clear the buffer.

        This will empty the buffer and reset its size.

        Returns:
            None
        """
        self.data = []
        self.size = 0

    def is_full(self) -> bool:
        """Check if the buffer is full.

        Returns:
            True if the buffer is full, False otherwise.
        """
        if self.max_size <= 0:
            return False
        return self.size >= self.max_size

    def is_empty(self) -> bool:
        """Check if the buffer is empty.

        Returns:
            True if the buffer is empty, False otherwise.
        """
        return not self.data

    def can_fit(self, size: int) -> bool:
        """Check if the buffer can fit the given size.

        Args:
            size: Size to check.

        Returns:
            True if the buffer can fit the given size, False otherwise.
        """
        if self.max_size <= 0:
            return True
        if self.max_size <= 0:
            return True
        return self.size + size <= self.max_size


class _BorrowedBuffer:
    def __init__(self, buffer: PendingBuffer) -> None:
        self.buffer = buffer
        self._data = []

    def __enter__(self) -> list[bytes]:
        self._data = self.buffer.data[:]
        self.buffer.clear()
        return self._data

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if exc_type is not None and self._data:
            self.buffer.extend(self._data)
        self._data = []
