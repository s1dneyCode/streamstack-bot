"""
Client-side token-bucket rate limiter shared by TmdbClient and OmdbClient.

Not a network client itself — it just blocks the calling thread until it's
safe to make another request, so overall throughput no longer depends on
every call site remembering its own time.sleep().
"""

import time


class RateLimiter:
    """
    Token-bucket limiter: refills at *rate* tokens/second, up to *burst*
    tokens of headroom for short bursts. acquire() blocks (sleeps) until a
    token is available, then consumes one.

    Not thread-safe — this bot is single-threaded throughout.
    """

    def __init__(self, rate: float, burst: int | None = None) -> None:
        self.rate = rate
        self.capacity = burst if burst is not None else max(1, int(rate))
        self._tokens = float(self.capacity)
        self._last_check = time.monotonic()

    def acquire(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_check
        self._last_check = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)

        if self._tokens < 1:
            wait = (1 - self._tokens) / self.rate
            time.sleep(wait)
            self._tokens = 0.0
            self._last_check = time.monotonic()
        else:
            self._tokens -= 1
