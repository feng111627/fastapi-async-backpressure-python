"""同资源并发请求合并（single-flight / 请求去重）。

语义
----
同一时间对同一 ``key`` 的 N 个调用，只有一个 leader 真正执行工作，
其余 N-1 个等待者共享 leader 的成功结果（底层只产生 1 次真实处理）。

- ``join_or_create`` 是**原子**原语（首个 await 点之前完成登记）：
  返回 leader 令牌或等待者订阅 future，不会出现“两个 leader”。
- 成功：leader 显式 ``succeed(result)`` 后结果才分发给等待者。
- 失败 / 超时 / 取消：``fail(exc)`` 广播错误并立刻删除条目，
  结果**绝不**复用，后续请求会得到全新的真实执行机会。
- 等待者各自受 timeout 约束（超时/取消只放弃自己的订阅，
  经 shield 隔断，不影响内部 future、leader 与其他等待者）。
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Union

from .errors import RejectReason, ThrottledError

Factory = Callable[[], Awaitable[Any]]

class _Flight:
    """一次合并执行：结果占位 + 订阅者集合。"""

    __slots__ = ("subscribers", "leader_done")

    def __init__(self) -> None:
        self.subscribers: set[asyncio.Future[Any]] = set()
        self.leader_done = False


class LeaderToken:
    """leader 凭证：负责在结束时广播结果/异常并清理条目。"""

    __slots__ = ("_sf", "_key", "_flight")

    def __init__(self, sf: "SingleFlight", key: str, flight: _Flight) -> None:
        self._sf = sf
        self._key = key
        self._flight = flight

    def succeed(self, result: Any) -> None:
        self._sf._finish_success(self._key, self._flight, result)

    def fail(self, exc: BaseException) -> None:
        self._sf._finish_failure(self._key, self._flight, exc)


JoinResult = tuple[bool, Union["LeaderToken", "asyncio.Future[Any]"]]


class SingleFlight:
    """按 key 合并并发调用。"""

    def __init__(self, business: str = "") -> None:
        self.business = business
        self._pending: dict[str, _Flight] = {}

    # ---- 观测接口 -----------------------------------------------------
    def in_flight(self, key: str | None = None) -> int:
        if key is None:
            return len(self._pending)
        return 1 if key in self._pending else 0

    def subscriber_count(self, key: str) -> int:
        flight = self._pending.get(key)
        return len(flight.subscribers) if flight is not None else 0

    # ---- 原子加入原语 -------------------------------------------------
    def join_or_create(
        self, key: str
    ) -> tuple[bool, Union[LeaderToken, asyncio.Future[Any]]]:
        """原子地加入 key 对应的 flight。

        返回 ``(True, LeaderToken)`` 表示本调用是 leader；
        返回 ``(False, future)`` 表示本调用是等待者，等待共享结果。
        本方法内部无 await，调用原子性由 asyncio 单线程保证。
        """
        flight = self._pending.get(key)
        if flight is not None:
            fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            flight.subscribers.add(fut)
            return False, fut

        flight = _Flight()
        self._pending[key] = flight  # 同步赋值：其他任务在此之后只能成为等待者
        return True, LeaderToken(self, key, flight)

    # ---- 结束分发（由 LeaderToken 调用）-------------------------------
    def _finish_success(
        self, key: str, flight: _Flight, result: Any
    ) -> None:
        if flight.leader_done:
            return
        flight.leader_done = True
        self._pending.pop(key, None)
        for fut in flight.subscribers:
            if not fut.done():
                fut.set_result(result)

    def _finish_failure(
        self, key: str, flight: _Flight, exc: BaseException
    ) -> None:
        if flight.leader_done:
            return
        flight.leader_done = True
        self._pending.pop(key, None)
        error = self._normalize(exc)
        for fut in flight.subscribers:
            if not fut.done():
                fut.set_exception(error)

    def _normalize(self, exc: BaseException) -> BaseException:
        """leader 因取消而中止时，给等待者一个可识别的背压错误。"""
        if isinstance(exc, (asyncio.CancelledError,)):
            return ThrottledError(
                RejectReason.PROCESSING_TIMEOUT,
                business=self.business,
                detail="leader was cancelled before producing a result",
            )
        return exc

    # ---- 便捷封装：自包含执行（测试/简单场景使用）---------------------
    async def execute(
        self,
        key: str,
        factory: Factory,
        *,
        timeout: float | None = None,
    ) -> Any:
        """原子登记 + leader 执行/等待者订阅的完整流程。"""
        is_leader, handle = self.join_or_create(key)
        if is_leader:
            assert isinstance(handle, LeaderToken)
            token = handle
            try:
                coro = factory()
                if timeout is None:
                    result = await coro
                else:
                    result = await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                error = ThrottledError(
                    RejectReason.PROCESSING_TIMEOUT,
                    business=self.business,
                    detail=f"processing exceeded {timeout:.3f}s",
                )
                token.fail(error)
                raise error from None
            except BaseException as exc:  # 含取消：广播后向上抛
                token.fail(exc)
                raise
            token.succeed(result)
            return result

        # 等待者：shield 隔断自身取消/超时对内部 future 的影响
        assert isinstance(handle, asyncio.Future)
        fut = handle
        flight = self._pending.get(key)
        wrapper = asyncio.ensure_future(asyncio.shield(fut))
        try:
            if timeout is None:
                return await wrapper
            return await asyncio.wait_for(wrapper, timeout=timeout)
        except asyncio.TimeoutError:
            raise ThrottledError(
                RejectReason.PROCESSING_TIMEOUT,
                business=self.business,
                detail=f"shared result not ready within {timeout:.3f}s",
            ) from None
        finally:
            if not wrapper.done():
                wrapper.cancel()
            if flight is not None:
                flight.subscribers.discard(fut)
