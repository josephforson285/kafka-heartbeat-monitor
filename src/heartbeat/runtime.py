"""Cooperative shutdown, shared by the producer and consumer loops."""

from __future__ import annotations

import signal
from contextlib import contextmanager
from typing import Iterator

_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class Shutdown:
    def __init__(self) -> None:
        self._requested = False

    def __bool__(self) -> bool:
        return self._requested

    def request(self, *_: object) -> None:
        self._requested = True


@contextmanager
def shutdown_on_signal() -> Iterator[Shutdown]:
    """Turn SIGINT/SIGTERM into a flag the loop can finish its current unit of work on.

    Killing mid-batch would either lose records or commit offsets for rows that
    were never written, so the loops check the flag at a safe point instead.
    """
    flag = Shutdown()
    previous = {sig: signal.getsignal(sig) for sig in _SIGNALS}
    for sig in _SIGNALS:
        signal.signal(sig, flag.request)
    try:
        yield flag
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
