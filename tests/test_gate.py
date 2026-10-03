"""ConcurrencyGate：并发上限、有界队列、排队超时、取消即回收。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.errors import RejectReason, ThrottledError
from app.gate import ConcurrencyGate

logger = logging.getLogger("test.gate")


async def test_concurrency_cap_and_queue_full() -> None:
    """超过 并发+队列 容量的请求立即得到 QUEUE_FULL，且额度可回收复用。"""
    gate = ConcurrencyGate("report", max_concurrency=2, max_queue=1, queue_timeout=5)
    entered = 0
    results: list[str] = []

    async def job(i: int) -> None:
        nonlocal entered
        try:
            async with gate:
                entered += 1
                await asyncio.sleep(0.15)
            results.append(f"ok-{i}")
        except ThrottledError as exc:
            results.append(exc.reason.value)

    await asyncio.gather(*(job(i) for i in range(5)))

    full = sum(1 for r in results if r == "queue_full")
    ok = sum(1 for r in results if r.startswith("ok-"))
    logger.info("并发争用/队列满: ok=%d queue_full=%d (期望 ok=3, full=2)", ok, full)
    assert ok == 3  # 2 直接执行 + 1 排队
    assert full == 2
    assert gate.active == 0 and gate.waiting == 0
    assert gate.total_admitted == 3 and gate.total_queue_full == 2

    # 额度全部回收：新一轮请求应全部成功
    results.clear()
    await asyncio.gather(*(job(i) for i in range(2)))
    assert all(r.startswith("ok-") for r in results)
    assert gate.active == 0
    logger.info("额度回收后再请求: 全部成功, active 归零")


async def test_queue_timeout_distinct_reason() -> None:
    """排队超过 queue_timeout 返回 QUEUE_TIMEOUT，与 QUEUE_FULL 区分。"""
    gate = ConcurrencyGate("export", max_concurrency=1, max_queue=5, queue_timeout=0.1)
    reasons: list[str] = []

    async def hold() -> None:
        async with gate:
            await asyncio.sleep(0.35)

    async def queue_then_timeout() -> None:
        try:
            async with gate:
                reasons.append("ok")
        except ThrottledError as exc:
            reasons.append(exc.reason.value)

    await asyncio.gather(hold(), *(queue_then_timeout() for _ in range(2)))
    logger.info("排队超时场景: reasons=%s (期望包含 queue_timeout)", reasons)
    assert "queue_timeout" in reasons
    assert gate.active == 0 and gate.waiting == 0
    assert gate.total_queue_timeout >= 1


async def test_cancelled_waiter_releases_queue_slot() -> None:
    """排队中的请求被取消后，排队名额立即释放，后续请求能进队。"""
    gate = ConcurrencyGate("report", max_concurrency=1, max_queue=1, queue_timeout=5)

    async def hold(t: float) -> None:
        async with gate:
            await asyncio.sleep(t)

    leader = asyncio.ensure_future(hold(0.3))
    await asyncio.sleep(0.05)  # leader 占满唯一执行额度

    waiter = asyncio.ensure_future(gate.acquire())  # 占满唯一名额
    await asyncio.sleep(0.05)
    assert gate.waiting == 1

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert gate.waiting == 0
    logger.info("排队中取消: waiting 1 -> %d, 名额已释放", gate.waiting)

    # 后续请求可以正常排队并最终获得执行
    got = asyncio.ensure_future(gate.acquire())
    await asyncio.sleep(0.05)
    assert gate.waiting == 1
    await got  # 等 leader 完成后被唤醒
    assert gate.active == 1
    gate.release()
    await leader
    assert gate.active == 0 and gate.waiting == 0
    logger.info("取消后新请求排队成功, 最终 active=%d waiting=%d",
                gate.active, gate.waiting)


async def test_cancelled_running_releases_concurrency() -> None:
    """执行中的请求被取消（调用方断开），并发额度立即归还。"""
    gate = ConcurrencyGate("asset", max_concurrency=2, max_queue=0, queue_timeout=1)

    async def cancellable() -> None:
        async with gate:
            await asyncio.sleep(10)  # 模拟长处理，等待外部取消

    task = asyncio.ensure_future(cancellable())
    await asyncio.sleep(0.05)
    assert gate.active == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert gate.active == 0
    logger.info("执行中取消: active 1 -> %d, 并发额度已回收", gate.active)

    # 连续多轮取消后额度仍可反复申请
    for _ in range(5):
        t = asyncio.ensure_future(cancellable())
        await asyncio.sleep(0.01)
        assert gate.active == 1
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert gate.active == 0
    logger.info("连续 5 轮取消后 active=%d, 无额度泄漏", gate.active)


async def test_zero_queue_fails_fast() -> None:
    """max_queue=0 时，并发满直接 QUEUE_FULL。"""
    gate = ConcurrencyGate("x", max_concurrency=1, max_queue=0, queue_timeout=1)

    async def hold() -> None:
        async with gate:
            await asyncio.sleep(0.2)

    t1 = asyncio.ensure_future(hold())
    t2 = asyncio.ensure_future(hold())
    await asyncio.sleep(0.05)
    with pytest.raises(ThrottledError) as ei:
        await t2
    assert ei.value.reason is RejectReason.QUEUE_FULL
    await t1
    logger.info("零队列快速失败: reason=queue_full, 完成后 active=%d", gate.active)
