"""单位时间请求量限制（固定窗口）。

每个 key 维护一个计数器窗口；窗口开始后每来一个请求计数 +1，
计数达到上限即快速失败（RATE_LIMITED），不排队、不等待。
窗口过期后自动重置。所有状态在 asyncio.Lock 下更新，保证并发准确。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .errors import RejectReason, ThrottledError


@dataclass
class _Window:
    start: float
    count: int


class RateLimiter:
    """按 key 维度的固定窗口限流器。"""

    def __init__(self, limit: int, window: float, *, clock=time.monotonic) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if window <= 0:
            raise ValueError("window must be positive")
        self._limit = limit
        self._window = window
        self._clock = clock
        self._windows: dict[str, _Window] = {}

    async def acquire(self, key: str, *, business: str | None = None) -> None:
        """尝试为 key 占用 1 个请求额度，超限立即抛 ThrottledError。"""
        business = business or key
        now = self._clock()
        window = self._windows.get(key)
        if window is None or now - window.start >= self._window:
            window = _Window(start=now, count=0)
            self._windows[key] = window

        if window.count >= self._limit:
            reset_at = window.start + self._window
            raise ThrottledError(
                RejectReason.RATE_LIMITED,
                business=business,
                detail=(
                    f"rate limit {self._limit}/{self._window}s exceeded "
                    f"(used={window.count})"
                ),
                retry_after=max(0.0, reset_at - now),
            )
        window.count += 1

    def used(self, key: str) -> int:
        """返回 key 当前窗口已用量（测试/观测用）。"""
        window = self._windows.get(key)
        if window is None or self._clock() - window.start >= self._window:
            return 0
        return window.count

    def forget(self, key: str) -> None:
        """删除 key 的窗口状态（维度注销/回收时调用）。"""
        self._windows.pop(key, None)
