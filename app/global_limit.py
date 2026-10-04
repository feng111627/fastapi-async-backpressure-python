"""全服务共享的容量保护层（叠在各业务自己的限制之上）。

设计要点
--------
- 面向“某个业务突然爆量、自己没超额却吃光全服务资源”的场景：
  所有业务合起来共用一份同时处理数上限与单位时间放行上限。
- 全局层**不排队**：触顶立即拒绝（GLOBAL_CONCURRENCY /
  GLOBAL_RATE_LIMITED），避免爆量业务在全局层堆积、拖累其他业务。
- 与业务层同时生效、互不绕过：请求先过全局层，再过业务层；
  任一层拒绝都立即失败，且已通过层级的额度立刻归还。
- 取消/断开时额度在 finally 中归还，不会只减不增。
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager

from .config import GlobalConfig
from .errors import RejectReason, ThrottledError
from .rate_limit import RateLimiter, WindowStats

GLOBAL_KEY = "global"


class GlobalLimiter(AbstractAsyncContextManager["GlobalLimiter"]):
    """全局同时处理数上限（无队列）+ 全局单位时间放行上限。"""

    def __init__(self, cfg: GlobalConfig) -> None:
        if cfg.max_concurrency <= 0:
            raise ValueError("global max_concurrency must be positive")
        self.capacity = cfg.max_concurrency
        self._active = 0
        self._rate = RateLimiter(
            cfg.rate_limit,
            cfg.rate_window,
            reason=RejectReason.GLOBAL_RATE_LIMITED,
        )
        # 观测计数
        self.total_admitted = 0
        self.total_concurrency_rejected = 0

    # ---- 观测接口（只读，不影响判定）---------------------------------
    @property
    def active(self) -> int:
        return self._active

    def rate_stats(self) -> WindowStats:
        return self._rate.stats(GLOBAL_KEY)

    # ---- 核心 API -----------------------------------------------------
    async def check_rate(self, business: str) -> None:
        """仅做全局单位时间放行检查（single-flight 等待者路径使用）。"""
        await self._rate.acquire(GLOBAL_KEY, business=business)

    async def acquire(self, business: str, *, check_rate: bool = True) -> None:
        """申请全局额度：（可选）全局限流 + 占全局并发名额。

        两段之间没有 await，检查与占用在事件循环里是原子的；
        任一段失败都不会残留额度。``check_rate=False`` 供调用方
        已单独做过全局限流检查时使用，避免重复计数。
        """
        # 1) 全局单位时间放行上限（不占并发资源，超限直接快速失败）
        if check_rate:
            await self._rate.acquire(GLOBAL_KEY, business=business)
        # 2) 全局同时处理数上限：不排队，满即拒
        if self._active >= self.capacity:
            self.total_concurrency_rejected += 1
            raise ThrottledError(
                RejectReason.GLOBAL_CONCURRENCY,
                business=business,
                detail=(
                    f"global concurrency limit {self.capacity} reached "
                    f"(active={self._active})"
                ),
            )
        self._active += 1
        self.total_admitted += 1

    def release(self) -> None:
        """归还全局并发名额（正常结束/异常/取消都必须调用一次）。"""
        if self._active <= 0:
            raise RuntimeError("global limiter released more than acquired")
        self._active -= 1

    async def __aenter__(self) -> "GlobalLimiter":
        await self.acquire(GLOBAL_KEY)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.release()
        return None
