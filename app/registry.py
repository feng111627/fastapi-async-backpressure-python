"""按业务维度管理 RateLimiter / ConcurrencyGate 实例。

- 维度首次出现时按配置创建限流器与并发闸门。
- 每次申请前先做一轮空闲回收：active=0 且 waiting=0 的闸门
  （连同限流器窗口状态）会被移除，保证压测大量不同维度后
  注册表不会无限膨胀。
- ``gate_slot`` 以异步上下文管理器形式封装“限流检查 -> 排队进场 ->
  出场回收额度”，异常/取消路径也能正确释放。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from .config import BusinessConfig, Settings
from .gate import ConcurrencyGate
from .rate_limit import RateLimiter


class _Entry:
    """单个业务维度持有的运行时资源。"""

    __slots__ = ("gate", "rate_limiter")

    def __init__(self, cfg: BusinessConfig) -> None:
        self.gate = ConcurrencyGate(
            business=cfg.business,
            max_concurrency=cfg.max_concurrency,
            max_queue=cfg.max_queue,
            queue_timeout=cfg.queue_timeout,
        )
        self.rate_limiter = RateLimiter(cfg.rate_limit, cfg.rate_window)


class GateRegistry:
    """限流器与并发闸门的注册表。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._entries: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    def _get_or_create(self, business: str) -> tuple[BusinessConfig, _Entry | None]:
        """取已有维度；不存在则按配置创建。调用方需持有 _lock。"""
        cfg = self._settings.get(business)
        entry = self._entries.get(business)
        if entry is None:
            entry = _Entry(cfg)
            self._entries[business] = entry
        return cfg, entry

    def _prune_idle_locked(self) -> None:
        """回收完全空闲的维度。调用方需持有 _lock。

        “完全空闲”= 闸门无执行/排队 且 限流窗口已自然过期。
        窗口仍在生效的维度必须保留：否则慢速串行流量（请求一个个来、
        闸门在间隙回到空闲）会把窗口内已用掉的量悄悄清零，限流失效。
        窗口过期后已用量本就归零，此时回收不影响任何判定。
        """
        idle = [
            name
            for name, entry in self._entries.items()
            if entry.gate.active == 0
            and entry.gate.waiting == 0
            and entry.rate_limiter.is_idle(name)
        ]
        for name in idle:
            del self._entries[name]

    async def prune_idle(self) -> int:
        """主动回收空闲维度，返回回收数量（测试/观测用）。"""
        async with self._lock:
            before = len(self._entries)
            self._prune_idle_locked()
            return before - len(self._entries)

    def active_dimensions(self) -> int:
        """当前注册的维度数量（观测用）。"""
        return len(self._entries)

    def gate_for(self, business: str) -> ConcurrencyGate:
        """同步获取/创建闸门（测试用：不走限流、不做回收）。"""
        cfg = self._settings.get(business)
        entry = self._entries.get(business)
        if entry is None:
            entry = _Entry(cfg)
            self._entries[business] = entry
        return entry.gate

    @asynccontextmanager
    async def gate_slot(
        self, business: str, *, check_rate: bool = True
    ) -> AsyncIterator[ConcurrencyGate]:
        """限流检查 + 进入闸门的完整生命周期。

        锁只覆盖“回收/创建 + 限流检查”这一段；真正排队等待发生在锁外，
        避免不同业务维度互相阻塞。``gate.acquire()`` 在其首个 await 点之前
        是完全同步的（立刻占用执行额度或把等待者入队），因此锁释放后
        立即调用它，期间没有任何让出事件循环的间隙，维度不会被误回收。

        ``check_rate=False`` 供调用方已单独做过限流检查时使用
        （single-flight leader 路径），避免重复计数。

        - RATE_LIMITED 快速失败（不占任何执行/排队资源）；
        - 再进入闸门（可能 QUEUE_FULL / QUEUE_TIMEOUT）；
        - 正常返回、抛错或被取消时，执行额度都会归还。
        """
        try:
            await self._lock.acquire()
            self._prune_idle_locked()
            _, entry = self._get_or_create(business)
            if check_rate:
                await entry.rate_limiter.acquire(business, business=business)
            gate = entry.gate
        finally:
            # 注意：紧接着的 gate.acquire() 同步前导段会让维度进入
            # active>0 或 waiting>0 的受保护状态，之后才会真正挂起排队。
            self._lock.release()

        await gate.acquire()  # QUEUE_FULL / QUEUE_TIMEOUT 由此抛出
        try:
            yield gate
        finally:
            gate.release()
