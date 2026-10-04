"""限流/背压/合并的统一编排层。

请求生命周期（所有路径都保证资源归还）::

    GLOBAL_RATE_LIMITED / GLOBAL_CONCURRENCY 快速失败（全局层，不排队）
      -> RATE_LIMITED 快速失败（业务层，不占任何资源）
      -> 若启用 single-flight：原子登记
           - leader：进入 gate 排队（QUEUE_FULL / QUEUE_TIMEOUT），
             拿到额度后真实执行，成功分发、失败/超时广播且不缓存；
           - 等待者：不占 gate 额度，直接共享 leader 结果，
             各自受 processing_timeout 约束，取消只影响自己。
         未启用：每个请求各自进入 gate 排队执行。
      -> 客户端断开/取消/异常：全局与业务两层额度都在 finally 中归还。

全局层与业务层同时生效、互不绕过：先过全局（共享容量保护），
再过业务（各自额度）。全局并发名额只在“真实执行”期间持有
（single-flight 等待者不执行，不占全局并发名额，但仍计入全局限流）。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable

from .config import Settings
from .errors import RejectReason, ThrottledError
from .global_limit import GlobalLimiter
from .registry import GateRegistry
from .singleflight import LeaderToken, SingleFlight

Handler = Callable[..., Awaitable[dict[str, Any]]]


@dataclass
class DispatchResult:
    """一次请求的处理结果及来源标记。"""

    payload: dict[str, Any]
    coalesced: bool  # True 表示复用了同资源 leader 的结果


class BackpressureManager:
    """按业务装配 GateRegistry 与 single-flight 表，外加全局容量保护。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.registry = GateRegistry(settings)
        self.global_limiter = GlobalLimiter(settings.global_)
        self._singleflights: dict[str, SingleFlight] = {
            name: SingleFlight(name)
            for name, cfg in settings.businesses.items()
            if cfg.singleflight
        }

    def singleflight_for(self, business: str) -> SingleFlight | None:
        return self._singleflights.get(business)

    async def _run_with_timeout(
        self, business: str, resource: str, handler: Handler,
        cfg: Any, kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            return await asyncio.wait_for(
                handler(resource, **kwargs), timeout=cfg.processing_timeout
            )
        except asyncio.TimeoutError:
            raise ThrottledError(
                RejectReason.PROCESSING_TIMEOUT,
                business=business,
                resource=resource,
                detail=f"processing exceeded {cfg.processing_timeout:.3f}s",
            ) from None

    @asynccontextmanager
    async def _global_slot(
        self, business: str, *, check_rate: bool = True
    ) -> AsyncIterator[None]:
        """全局层额度：进入时申请，任何退出路径（含取消）都归还。"""
        await self.global_limiter.acquire(business, check_rate=check_rate)
        try:
            yield
        finally:
            self.global_limiter.release()

    async def dispatch(
        self,
        business: str,
        resource: str,
        handler: Handler,
        **kwargs: Any,
    ) -> DispatchResult:
        """全局限流/并发 + 业务限流 + 排队 + 合并 + 超时地执行一次调用。"""
        cfg = self.settings.get(business)

        sf: SingleFlight | None = None
        if cfg.singleflight:
            sf = self._singleflights.get(business)
            if sf is None:
                sf = SingleFlight(business)
                self._singleflights[business] = sf

        key = f"{business}:{resource}"

        if sf is None:
            # 未启用合并：全局层包住整个“业务限流 + 排队 + 执行”
            async with self._global_slot(business):
                async with self.registry.gate_slot(business):
                    payload = await self._run_with_timeout(
                        business, resource, handler, cfg, kwargs
                    )
            return DispatchResult(payload=payload, coalesced=False)

        # 启用合并：所有请求先过全局限流 + 业务限流
        # （含等待者，保证 QPS 口径一致；等待者不执行，不占并发名额）
        await self.global_limiter.check_rate(business)
        await self._acquire_rate_limit(business)

        # 2) 原子加入合并 flight（此处之前不允许出现 await）
        is_leader, handle = sf.join_or_create(key)

        if is_leader:
            assert isinstance(handle, LeaderToken)
            token = handle
            try:
                # 只有 leader 占用全局/业务并发额度；限流已在上方完成
                async with self._global_slot(business, check_rate=False):
                    async with self.registry.gate_slot(
                        business, check_rate=False
                    ):
                        payload = await self._run_with_timeout(
                            business, resource, handler, cfg, kwargs
                        )
            except BaseException as exc:
                # 失败/超时/取消都广播出去，且不缓存结果
                token.fail(exc)
                raise
            token.succeed(payload)
            return DispatchResult(payload=payload, coalesced=False)

        # 3) 等待者：共享 leader 结果，不占全局/业务并发名额
        fut = handle
        flight = sf._pending.get(key)  # noqa: SLF001
        wrapper = asyncio.ensure_future(asyncio.shield(fut))
        try:
            return DispatchResult(
                payload=await asyncio.wait_for(
                    wrapper, timeout=cfg.processing_timeout
                ),
                coalesced=True,
            )
        except asyncio.TimeoutError:
            raise ThrottledError(
                RejectReason.PROCESSING_TIMEOUT,
                business=business,
                resource=resource,
                detail=(
                    f"shared result not ready within "
                    f"{cfg.processing_timeout:.3f}s"
                ),
            ) from None
        finally:
            if not wrapper.done():
                wrapper.cancel()
            if flight is not None:
                flight.subscribers.discard(fut)

    async def _acquire_rate_limit(self, business: str) -> None:
        """仅做业务限流检查（不进入 gate），供合并等待者路径使用。"""
        registry = self.registry
        await registry._lock.acquire()  # noqa: SLF001
        try:
            registry._prune_idle_locked()  # noqa: SLF001
            limiter = registry._rate_limiter_locked(business)  # noqa: SLF001
            await limiter.acquire(business, business=business)
        finally:
            registry._lock.release()  # noqa: SLF001
