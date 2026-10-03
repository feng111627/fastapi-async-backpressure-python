"""并发闸门：有界排队 + 排队超时 + 取消即释放。

设计要点
--------
系统内请求数 = 正在执行(active) + 排队等待(waiting)。

- ``active`` 上限 ``max_concurrency``；``active + waiting`` 上限
  ``max_concurrency + max_queue``。到达上限的新请求立即得到 QUEUE_FULL，
  不进入等待，因此队列不可能无限增长。
- 排队通过显式的 future 双端队列实现（FIFO），而不是
  ``Semaphore.acquire() + wait_for`` 的组合：后者在“超时/取消”与
  “许可恰好放行”同时发生时可能把许可泄漏给一个已经失败的请求。
  这里只在 future 尚未结束时才真正授予许可（active +1），
  从根上避免额度只减不增。
- 排队超时由调用方自身的 ``wait_for`` 驱动，无论是否有新请求进入，
  到期都会醒来并把自己从队列摘除；客户端断开（CancelledError）同理，
  排队名额立即回收。
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

from .errors import RejectReason, ThrottledError


@dataclass(frozen=True)
class _Waiter:
    future: asyncio.Future[None]
    expiry: float


class ConcurrencyGate(AbstractAsyncContextManager["ConcurrencyGate"]):
    """单个业务维度的并发与排队闸门。"""

    def __init__(
        self,
        business: str,
        max_concurrency: int,
        max_queue: int,
        queue_timeout: float,
        *,
        clock=time.monotonic,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if max_queue < 0:
            raise ValueError("max_queue must be non-negative")
        self.business = business
        self.capacity = max_concurrency
        self.max_queue = max_queue
        self.queue_timeout = queue_timeout
        self._clock = clock
        self._active = 0
        self._waiting: deque[_Waiter] = deque()
        # 观测计数
        self.total_admitted = 0
        self.total_queue_full = 0
        self.total_queue_timeout = 0

    # ---- 观测接口 -----------------------------------------------------
    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return len(self._waiting)

    # ---- 内部维护 -----------------------------------------------------
    def _purge(self, now: float) -> None:
        """摘除已取消或已过期的等待者（释放排队名额）。"""
        if not self._waiting:
            return
        alive = deque(
            w
            for w in self._waiting
            if not w.future.cancelled() and w.expiry > now
        )
        self._waiting = alive

    def _pump(self, now: float) -> None:
        """有执行额度时，把队首等待者放进来。"""
        self._purge(now)
        while self._active < self.capacity and self._waiting:
            waiter = self._waiting.popleft()
            if waiter.future.cancelled() or waiter.expiry <= now:
                # purge 之后理论上不会出现，双保险：跳过且不占额度
                continue
            self._active += 1
            if not waiter.future.done():
                waiter.future.set_result(None)
                return
            # 极端竞态：future 已取消/已结束，回收本次额度，尝试下一个
            self._active -= 1

    # ---- 核心 API -----------------------------------------------------
    async def acquire(self) -> None:
        """申请一个执行名额；满则 QUEUE_FULL，排队超时则 QUEUE_TIMEOUT。"""
        now = self._clock()
        self._purge(now)

        if self._active + len(self._waiting) >= self.capacity + self.max_queue:
            self.total_queue_full += 1
            raise ThrottledError(
                RejectReason.QUEUE_FULL,
                business=self.business,
                detail=(
                    f"all {self.capacity} exec slots and "
                    f"{self.max_queue} queue slots are occupied "
                    f"(active={self._active}, waiting={len(self._waiting)})"
                ),
            )

        if self._active < self.capacity:
            # 有空余执行额度，直接执行，不排队
            self._active += 1
            self.total_admitted += 1
            return

        # 进入有界队列等待
        waiter = _Waiter(
            future=asyncio.get_running_loop().create_future(),
            expiry=now + self.queue_timeout,
        )
        self._waiting.append(waiter)
        try:
            remaining = waiter.expiry - self._clock()
            await asyncio.wait_for(waiter.future, timeout=remaining)
        except TimeoutError:
            self.total_queue_timeout += 1
            raise ThrottledError(
                RejectReason.QUEUE_TIMEOUT,
                business=self.business,
                detail=(
                    f"queued longer than {self.queue_timeout:.3f}s "
                    f"(active={self._active}, waiting={len(self._waiting)})"
                ),
            ) from None
        except BaseException:
            # 客户端断开等取消：立刻摘除排队名额并向上传播
            self._remove(waiter)
            raise
        self.total_admitted += 1

    def _remove(self, waiter: _Waiter) -> None:
        try:
            self._waiting.remove(waiter)
        except ValueError:
            pass

    def release(self) -> None:
        """归还执行名额，并尝试唤醒一个排队者。"""
        if self._active <= 0:
            # 防御性检查：正常配对使用时不会发生
            raise RuntimeError(f"gate '{self.business}' released more than acquired")
        self._active -= 1
        self._pump(self._clock())

    async def __aenter__(self) -> "ConcurrencyGate":
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.release()
        return None
