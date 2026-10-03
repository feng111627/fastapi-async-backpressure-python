"""有界并发槽（bounded semaphore with bounded wait queue）。

语义：
- 同时处理数不超过 ``max_concurrency``；
- 超出并发上限的请求在 FIFO 队列中排队，队列长度不超过 ``max_queue``，
  队列满时立刻抛 :class:`QueueFullError`（快速失败，区别于排队超时）；
- 排队超过 ``queue_timeout`` 秒抛 :class:`QueueTimeoutError`；
- 请求被取消（如调用方断开连接）时立即离开队列；
  若恰好与"获得槽"竞争，则主动归还槽位，保证额度不泄漏。
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Optional

from .errors import QueueFullError, QueueTimeoutError


class BoundedSemaphore:
    def __init__(self, max_concurrency: int, max_queue: int) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if max_queue < 0:
            raise ValueError("max_queue must be >= 0")
        self._max = max_concurrency
        self._max_queue = max_queue
        self._active = 0
        self._queue: Deque[asyncio.Future[None]] = deque()

    async def acquire(self, timeout: Optional[float] = None) -> None:
        loop = asyncio.get_running_loop()

        # 有空闲槽且队列为空：立即获得（FIFO，不能插队）。
        if self._active < self._max and not self._queue:
            self._active += 1
            return

        # 队列有界：满了立即拒绝，杜绝无界堆积。
        if len(self._queue) >= self._max_queue:
            raise QueueFullError(
                f"queue full: {len(self._queue)}/{self._max_queue} waiting"
            )

        fut: asyncio.Future[None] = loop.create_future()
        self._queue.append(fut)

        timer: Optional[asyncio.TimerHandle] = None
        if timeout is not None:

            def _on_timeout() -> None:
                if not fut.done():
                    fut.set_exception(
                        QueueTimeoutError(f"queue wait timed out after {timeout}s")
                    )

            timer = loop.call_later(timeout, _on_timeout)

        try:
            await fut
        except QueueTimeoutError:
            self._remove_waiter(fut)
            raise
        except asyncio.CancelledError:
            if timer is not None:
                timer.cancel()
            if fut.cancelled():
                self._remove_waiter(fut)
            elif fut.done() and fut.exception() is None:
                # 槽恰好被授予本请求：仅归还，不转授他人。
                # 此调用方从未进入真正的处理阶段，不应替队首发放执行权。
                if self._active > 0:
                    self._active -= 1
            else:
                self._remove_waiter(fut)
            raise
        else:
            if timer is not None:
                timer.cancel()

    def release(self) -> None:
        """归还一个槽，并尽量把空闲槽授予队首仍有效的等待者。"""
        if self._active > 0:
            self._active -= 1
        self._pump()

    def _pump(self) -> None:
        while self._queue and self._active < self._max:
            waiter = self._queue.popleft()
            if waiter.done():
                # 已超时/取消的等待者，跳过且不占槽。
                continue
            self._active += 1
            waiter.set_result(None)

    def _remove_waiter(self, fut: asyncio.Future[None]) -> None:
        try:
            self._queue.remove(fut)
        except ValueError:
            pass

    @property
    def in_flight(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return len(self._queue)
