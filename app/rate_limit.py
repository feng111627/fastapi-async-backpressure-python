"""单位时间请求量限制（固定窗口）。

每个 key 维护一个计数器窗口；窗口开始后每来一个请求计数 +1，
计数达到上限即快速失败（RATE_LIMITED），不排队、不等待。
窗口过期后自动重置。所有状态在 asyncio.Lock 下更新，保证并发准确。

窗口按真实经过的时间生效：只要窗口没到期，无论请求是密集突发还是
隔一会儿来一个的慢速串行，计数都持续累积，空闲停顿不会清零已用量；
窗口自然滚动过去后计数（含本窗口拒绝数）按规则重置，额度正常恢复。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .errors import RejectReason, ThrottledError


@dataclass
class _Window:
    start: float
    count: int
    rejected: int = 0  # 本窗口内被拒次数，窗口滚动时随计数一起重置


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

    def _current(self, key: str, now: float) -> _Window:
        """取 key 当前有效窗口；不存在或已过期则开新窗口。"""
        window = self._windows.get(key)
        if window is None or now - window.start >= self._window:
            window = _Window(start=now, count=0)
            self._windows[key] = window
        return window

    async def acquire(self, key: str, *, business: str | None = None) -> None:
        """尝试为 key 占用 1 个请求额度，超限立即抛 ThrottledError。"""
        business = business or key
        now = self._clock()
        window = self._current(key, now)

        if window.count >= self._limit:
            window.rejected += 1
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

    def is_idle(self, key: str) -> bool:
        """key 当前没有生效中的窗口（窗口状态可回收）。"""
        window = self._windows.get(key)
        return window is None or self._clock() - window.start >= self._window

    def snapshot(self, key: str) -> dict[str, object]:
        """当前窗口的观测快照（只读，不消耗额度、不影响判定）。"""
        now = self._clock()
        window = self._windows.get(key)
        active = window is not None and now - window.start < self._window
        used = window.count if active and window is not None else 0
        rejected = window.rejected if active and window is not None else 0
        reset_at = window.start + self._window if active and window is not None else None
        return {
            "limit": self._limit,
            "window": self._window,
            "used": used,
            "remaining": max(0, self._limit - used),
            "rejected": rejected,
            "reset_in": max(0.0, reset_at - now) if reset_at is not None else 0.0,
        }

    def forget(self, key: str) -> None:
        """删除 key 的窗口状态（维度注销/回收时调用）。"""
        self._windows.pop(key, None)
