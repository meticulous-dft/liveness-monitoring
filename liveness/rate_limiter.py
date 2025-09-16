import threading
import time
from typing import Optional


class TokenBucket:
    """
    Simple thread-safe token bucket. Refill rate = tokens per second.
    acquire(1) blocks until a token is available.
    """

    def __init__(self, rate_per_sec: float, burst: Optional[float] = None):
        self.rate = max(rate_per_sec, 0.001)
        self.capacity = burst if burst is not None else max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.perf_counter()
        self.cv = threading.Condition()
        self._stopped = False

    def stop(self):
        with self.cv:
            self._stopped = True
            self.cv.notify_all()

    def acquire(self, n: float = 1.0) -> bool:
        with self.cv:
            while not self._stopped:
                now = time.perf_counter()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= n:
                    self.tokens -= n
                    return True
                # sleep until enough tokens expected
                need = (n - self.tokens) / self.rate
                self.cv.wait(timeout=max(need, 0.001))
            return False
