"""有界并发槽单元测试。"""

from __future__ import annotations

import asyncio

import pytest

from app.bounded_semaphore import BoundedSemaphore
from app.errors import QueueFullError, QueueTimeoutError


async def test_concurrency_cap():
    sem = BoundedSemaphore(2, 2)
    await sem.acquire()
    await sem.acquire()
    assert sem.in_flight == 2
    sem.release()
    sem.release()
    assert sem.in_flight == 0


async def test_queue_full_is_immediate():
    sem = BoundedSemaphore(1, 1)
    await sem.acquire()
    waiter = asyncio.ensure_future(sem.acquire(10))
    await asyncio.sleep(0)  # 进入队列
    assert sem.waiting == 1
    with pytest.raises(QueueFullError):
        await sem.acquire()
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    sem.release()


async def test_queue_timeout_distinguished_from_full():
    sem = BoundedSemaphore(1, 5)
    await sem.acquire()
    with pytest.raises(QueueTimeoutError):
        await sem.acquire(timeout=0.05)
    # 超时者不占队列、不占槽
    assert sem.waiting == 0 and sem.in_flight == 1
    sem.release()


async def test_waiter_woken_in_fifo_order():
    sem = BoundedSemaphore(1, 3)
    await sem.acquire()
    order: list[int] = []

    async def waiter(idx: int) -> None:
        await sem.acquire(timeout=5)
        order.append(idx)
        sem.release()

    tasks = [asyncio.ensure_future(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.05)
    sem.release()
    await asyncio.gather(*tasks)
    assert order == [0, 1, 2]


async def test_cancel_while_waiting_releases_everything():
    sem = BoundedSemaphore(1, 3)
    await sem.acquire()
    t = asyncio.ensure_future(sem.acquire(timeout=5))
    await asyncio.sleep(0)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert sem.waiting == 0
    sem.release()
    assert sem.in_flight == 0
    # 取消后额度仍可正常使用
    await sem.acquire(timeout=0.1)
    assert sem.in_flight == 1
    sem.release()


async def test_repeated_pressure_does_not_leak_slots():
    """连续压力后额度必须恢复，不能只减不增。"""
    sem = BoundedSemaphore(3, 10)
    rejected = 0

    async def one() -> None:
        nonlocal rejected
        try:
            await sem.acquire(timeout=1)
        except QueueFullError:
            rejected += 1
            return
        await asyncio.sleep(0.005)
        sem.release()

    for _ in range(5):
        await asyncio.gather(*[one() for _ in range(20)])
        assert sem.in_flight == 0 and sem.waiting == 0
    assert rejected > 0  # 确实触发过快速拒绝
