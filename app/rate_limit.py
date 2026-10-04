"""单位时间请求量限制（固定窗口）。

每个 key 维护一个计数器窗口；窗口开始后每来一个请求计数 +1，
计数达到上限即快速失败（默认 RATE_LIMITED，可通过 ``reason`` 定制，
全局层复用本限流器并改用 GLOBAL_RATE_LIMITED），不排队、不等待。
窗口过期后自动重置，已用量与“本窗口被拒数”一起清零。

计数只随真实经过的时间滚动：窗口是否过期完全由时钟决定，
与请求是否密集、中间是否有空闲停顿无关——慢速串行流量在窗口内
同样会被第 N+1 个请求触发拒绝。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .errors import RejectReason, ThrottledError


@dataclass
class _Window:
    start: float
    count: int
    rejected: int  # 本窗口内被拒的请求数，窗口滚动时随 count 一起重置


@dataclass(frozen=True)
class WindowStats:
    """某个 key 当前窗口的观测快照（只读，不影响限流判定）。"""

    limit: int
    window: float
    used: int
    remaining: int
    rejected: int
    retry_after: float  # 距窗口自然滚动、额度恢复的秒数（无窗口时为 0）


class RateLimiter:
    """按 key 维度的固定窗口限流器。"""

    def __init__(
        self,
        limit: int,
        window: float,
        *,
        clock=time.monotonic,
        reason: RejectReason = RejectReason.RATE_LIMITED,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if window <= 0:
            raise ValueError("window must be positive")
        self._limit = limit
        self._window = window
        self._clock = clock
        self._reason = reason
        self._windows: dict[str, _Window] = {}

    def _current(self, key: str, now: float) -> _Window:
        """取 key 的当前窗口；过期则按规则滚动重置（计数与被拒数清零）。"""
        window = self._windows.get(key)
        if window is None or now - window.start >= self._window:
            window = _Window(start=now, count=0, rejected=0)
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
                self._reason,
                business=business,
                detail=(
                    f"rate limit {self._limit}/{self._window}s exceeded "
                    f"(used={window.count}, remaining=0)"
                ),
                retry_after=max(0.0, reset_at - now),
            )
        window.count += 1

    def stats(self, key: str) -> WindowStats:
        """返回 key 当前窗口快照；纯读取，不占额度、不重置任何计数。"""
        now = self._clock()
        window = self._windows.get(key)
        if window is None or now - window.start >= self._window:
            # 窗口不存在或已自然滚动：视为全新窗口（不写入，避免观测污染状态）
            return WindowStats(
                limit=self._limit,
                window=self._window,
                used=0,
                remaining=self._limit,
                rejected=0,
                retry_after=0.0,
            )
        return WindowStats(
            limit=self._limit,
            window=self._window,
            used=window.count,
            remaining=max(0, self._limit - window.count),
            rejected=window.rejected,
            retry_after=max(0.0, window.start + self._window - now),
        )

    def used(self, key: str) -> int:
        """返回 key 当前窗口已用量（测试/观测用）。"""
        return self.stats(key).used

    def window_expired(self, key: str) -> bool:
        """key 的窗口是否已自然过期（空闲回收时可安全删除其状态）。"""
        window = self._windows.get(key)
        return window is None or self._clock() - window.start >= self._window

    def forget(self, key: str) -> None:
        """删除 key 的窗口状态（维度注销/回收时调用）。"""
        self._windows.pop(key, None)
