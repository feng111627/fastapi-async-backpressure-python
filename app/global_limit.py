"""全服务共享的容量保护层（叠在各业务限制之上）。

两类全局上限，所有业务合计计算，触顶**立即拒绝、不排队**：

- 全局单位时间放行上限：复用固定窗口 ``RateLimiter``（单 key），
  超限抛 ``GLOBAL_RATE_LIMITED``（429），与业务自身的
  ``RATE_LIMITED`` 可区分。
- 全局同时在途数上限：一个同步计数器（请求从进入闸门排队到处理
  结束都算在途），达到上限抛 ``GLOBAL_CONCURRENCY``（503），
  与业务自身的 ``QUEUE_FULL`` 可区分。

全局并发额度不排队，因此获取/释放都是同步操作；释放放在调用方的
``finally`` 中，取消/断开/异常路径都会立即归还，不会只减不增。
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

from .config import GlobalConfig
from .errors import RejectReason, ThrottledError
from .rate_limit import RateLimiter

_KEY = "global"


class GlobalLimiter:
    """全局限流 + 全局在途数上限的组合闸门。"""

    def __init__(self, cfg: GlobalConfig, *, clock=time.monotonic) -> None:
        if cfg.max_concurrency <= 0:
            raise ValueError("global max_concurrency must be positive")
        self._capacity = cfg.max_concurrency
        self._rate = RateLimiter(cfg.rate_limit, cfg.rate_window, clock=clock)
        self._active = 0
        # 观测计数
        self.total_admitted = 0
        self.total_rejected = 0

    # ---- 全局单位时间放行上限 -------------------------------------
    async def acquire_rate(self, *, business: str) -> None:
        """占用 1 个全局放行额度，超限立即抛 GLOBAL_RATE_LIMITED。"""
        try:
            await self._rate.acquire(_KEY, business="global")
        except ThrottledError as exc:
            raise ThrottledError(
                RejectReason.GLOBAL_RATE_LIMITED,
                business=business,
                detail=(
                    f"global rate limit exceeded, triggered by '{business}': "
                    f"{exc.detail}"
                ),
                retry_after=exc.retry_after,
            ) from None

    # ---- 全局同时在途数上限 ---------------------------------------
    def acquire_slot(self, *, business: str) -> None:
        """占用 1 个全局在途名额，满则立即抛 GLOBAL_CONCURRENCY（不排队）。"""
        if self._active >= self._capacity:
            self.total_rejected += 1
            raise ThrottledError(
                RejectReason.GLOBAL_CONCURRENCY,
                business=business,
                detail=(
                    f"global in-flight limit {self._capacity} reached, "
                    f"triggered by '{business}' (active={self._active})"
                ),
            )
        self._active += 1
        self.total_admitted += 1

    def release_slot(self) -> None:
        """归还在途名额（取消/断开/异常路径也必须走到这里）。"""
        if self._active <= 0:
            raise RuntimeError("global limiter released more than acquired")
        self._active -= 1

    @contextmanager
    def slot(self, business: str) -> Iterator[None]:
        """在途名额的上下文管理器：任何退出路径都保证归还。"""
        self.acquire_slot(business=business)
        try:
            yield
        finally:
            self.release_slot()

    # ---- 观测接口（只读，不消耗额度） -----------------------------
    @property
    def active(self) -> int:
        return self._active

    @property
    def capacity(self) -> int:
        return self._capacity

    def snapshot(self) -> dict[str, object]:
        """全局层观测快照。"""
        return {
            "active": self._active,
            "max_concurrency": self._capacity,
            "admitted": self.total_admitted,
            "rejected_concurrency": self.total_rejected,
            "rate": self._rate.snapshot(_KEY),
        }
