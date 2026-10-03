"""限流/背压管理器：组合速率限制、有界并发槽与请求合并。

请求处理顺序（每层失败都快速返回，不进入下层）：
1. 速率检查（令牌桶，按 ``rate_key``，默认 "global"）——超限抛
   :class:`RateLimitedError`；
2. 有界并发槽——队列满抛 :class:`QueueFullError`，排队超时抛
   :class:`QueueTimeoutError`，调用方取消则立即退出队列；
3. 拿到槽后执行业务：若该维度开启 ``coalesce`` 且提供了 ``resource_key``，
   同资源并发请求只真实执行一次（排队与执行都由 leader 代表大家完成），
   失败/超时不缓存，后续请求可重新执行；
4. 业务结束（或抛错、被取消）后释放并发槽。
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from .bounded_semaphore import BoundedSemaphore
from .coalescer import Coalescer
from .config import DimensionConfig
from .errors import RateLimitedError
from .rate_limiter import TokenBucketRateLimiter


class _DimensionState:
    __slots__ = ("config", "semaphore", "rate_limiter", "coalescer")

    def __init__(self, config: DimensionConfig) -> None:
        self.config = config
        self.semaphore = BoundedSemaphore(
            max_concurrency=config.max_concurrency,
            max_queue=config.max_queue,
        )
        self.rate_limiter = TokenBucketRateLimiter(
            rate=config.rate_per_second,
            burst=config.rate_burst,
        )
        self.coalescer = Coalescer()


class ThrottleManager:
    def __init__(self, dimensions: Dict[str, DimensionConfig]) -> None:
        self._dimensions = {
            name: _DimensionState(cfg) for name, cfg in dimensions.items()
        }

    def _state(self, dimension: str) -> _DimensionState:
        try:
            return self._dimensions[dimension]
        except KeyError:
            raise KeyError(f"unknown throttle dimension: {dimension!r}") from None

    def stats(self) -> dict[str, dict[str, int]]:
        """返回各维度当前占用与合并统计，供健康检查/压测观测。"""
        out: dict[str, dict[str, int]] = {}
        for name, st in self._dimensions.items():
            out[name] = {
                "in_flight": st.semaphore.in_flight,
                "waiting": st.semaphore.waiting,
                "coalesced_calls": st.coalescer.calls,
                "coalesced_executions": st.coalescer.executions,
            }
        return out

    def _check_rate(self, st: _DimensionState, rate_key: str) -> None:
        if not st.rate_limiter.allow(rate_key):
            raise RateLimitedError(
                retry_after=st.rate_limiter.retry_after(rate_key)
            )

    async def run(
        self,
        dimension: str,
        func: Callable[[], Awaitable[Any]],
        *,
        resource_key: Optional[str] = None,
        rate_key: Optional[str] = None,
    ) -> Any:
        st = self._state(dimension)

        # 1) 单位时间请求量限制：在排队/合并之前检查，超限立即失败。
        self._check_rate(st, rate_key or "global")

        if st.config.coalesce and resource_key is not None:
            # 2+3) 排队与真实执行都由共享任务（leader）代表所有等待者完成，
            #      槽位也只由它持有和释放；跟随者不重复占队列/槽。
            key = f"{dimension}:{resource_key}"

            async def _shared() -> Any:
                await st.semaphore.acquire(timeout=st.config.queue_timeout)
                try:
                    return await func()
                finally:
                    # 成功、失败、取消都会归还槽（任务可能在最后一个等待者
                    # 离开后仍 detached 运行，必须自己保证释放）。
                    st.semaphore.release()

            value, _is_leader = await st.coalescer.call_ex(key, _shared)
            return value

        # 2) 有界并发槽：队列有上限，支持排队超时与取消退出。
        await st.semaphore.acquire(timeout=st.config.queue_timeout)
        try:
            # 3) 执行业务。
            return await func()
        finally:
            # 4) 正常结束、异常、取消都会归还槽位。
            st.semaphore.release()
