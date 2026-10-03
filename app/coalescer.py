"""单飞（singleflight）：同一 key 的并发重复请求只真正执行一次。

语义：
- 同一时刻、同一个 ``key`` 的多个调用共享一次真实执行，
  后续调用等待并复用首调（leader）的结果；
- 失败/超时的结果**不缓存**：异常会抛给所有等待者，随后到达的请求
  会获得一次全新的真实执行机会；
- 某个跟随者中途取消（调用方断开）不影响共享执行；
- 当所有等待者都离开时，正在进行的共享任务会被取消，立即释放其占用的
  下游/并发资源，避免无人等待的任务空跑。
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Optional


class _Flight:
    __slots__ = ("task", "waiters")

    def __init__(self, task: "asyncio.Task[Any]") -> None:
        self.task = task
        self.waiters = 1  # leader 算一个等待者


class Coalescer:
    def __init__(self) -> None:
        self._flights: Dict[str, _Flight] = {}
        self.calls = 0          # 进入合并器的总调用次数
        self.executions = 0     # 真正触发底层执行的次数（leader 次数）

    async def call(
        self,
        key: str,
        func: Callable[[], Awaitable[Any]],
    ) -> Any:
        value, _is_leader = await self.call_ex(key, func)
        return value

    async def call_ex(
        self,
        key: str,
        func: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, bool]:
        """同 :meth:`call`，额外返回本次调用是否为首调（leader）。"""
        loop = asyncio.get_running_loop()
        self.calls += 1
        flight = self._flights.get(key)
        is_leader = flight is None
        if is_leader:
            assert flight is None
            self.executions += 1
            flight = _Flight(self._spawn(loop, key, func))
            self._flights[key] = flight
        else:
            flight.waiters += 1
        task = flight.task

        try:
            value = await asyncio.shield(task)
            return value, is_leader
        except asyncio.CancelledError:
            # 仅当前调用方离开：减少等待计数；没人等了就取消共享任务。
            flight.waiters -= 1
            if flight.waiters <= 0 and not task.done():
                task.cancel()
            raise
        except BaseException:
            # 失败/超时不缓存：留给任务回调把 flight 移除，
            # 后续请求可重新触发真实执行。
            raise

    def _spawn(
        self,
        loop: asyncio.AbstractEventLoop,
        key: str,
        func: Callable[[], Awaitable[Any]],
    ) -> "asyncio.Task[Any]":
        async def _run() -> Any:
            return await func()

        task = loop.create_task(_run())

        def _cleanup(_t: "asyncio.Task[Any]") -> None:
            # 无论成功、失败还是取消，flight 都不保留结果。
            # 异常已（或将）通过 shield 抛给等待者；这里消费一次，
            # 避免在"无人等待任务异常"时出现 "exception was never retrieved" 噪音。
            if not _t.cancelled():
                _t.exception()
            entry = self._flights.get(key)
            if entry is not None and entry.task is task:
                del self._flights[key]

        task.add_done_callback(_cleanup)
        return task

    def active_keys(self) -> int:
        return len(self._flights)
