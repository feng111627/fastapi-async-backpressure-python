"""SingleFlight：同资源合并、失败/超时不缓存、等待者隔离。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.errors import RejectReason, ThrottledError
from app.singleflight import SingleFlight

logger = logging.getLogger("test.singleflight")


async def test_concurrent_requests_coalesced() -> None:
    calls = 0

    async def factory() -> dict:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return {"id": calls}

    sf = SingleFlight("asset")
    results = await asyncio.gather(
        *(sf.execute("asset:res-1", factory, timeout=2) for _ in range(8))
    )
    assert calls == 1, "同资源同时只应有 1 次真实执行"
    assert all(r == {"id": 1} for r in results)
    assert sf.in_flight() == 0
    logger.info("同资源合并: 8 请求 -> 1 次真实执行, 全部共享成功结果")


async def test_failure_is_not_cached() -> None:
    attempts = 0

    async def factory() -> str:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.01)
        if attempts == 1:
            raise RuntimeError("transient failure")
        return "recovered"

    sf = SingleFlight("asset")
    with pytest.raises(RuntimeError):
        await sf.execute("asset:r", factory, timeout=2)
    assert sf.in_flight("asset:r") == 0
    assert await sf.execute("asset:r", factory, timeout=2) == "recovered"
    assert attempts == 2
    logger.info("失败不缓存: 第 1 次失败后第 2 次获得真实执行并成功")


async def test_leader_timeout_allows_rerun() -> None:
    attempts = 0

    async def slow() -> str:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.5)
        return "late"

    sf = SingleFlight("asset")
    with pytest.raises(ThrottledError) as ei:
        await sf.execute("asset:r", slow, timeout=0.05)
    assert ei.value.reason is RejectReason.PROCESSING_TIMEOUT
    await asyncio.sleep(0.05)  # 等取消回调清理
    assert sf.in_flight() == 0
    # 超时的结果不能被复用：后续再来必须真实执行
    assert await sf.execute("asset:r", factory=lambda: _fast(), timeout=2) == "fast"
    assert attempts == 1  # 慢任务只跑了被取消的那一次
    logger.info("leader 超时: 条目已清除, 后续请求重新执行成功")


async def _fast() -> str:
    await asyncio.sleep(0.01)
    return "fast"


async def test_waiter_timeout_does_not_affect_leader() -> None:
    sf = SingleFlight("asset")

    async def work() -> str:
        await asyncio.sleep(0.25)
        return "leader-result"

    leader = asyncio.ensure_future(sf.execute("asset:r", work, timeout=2))
    await asyncio.sleep(0.03)  # leader 先就位
    with pytest.raises(ThrottledError) as ei:
        await sf.execute("asset:r", work, timeout=0.05)
    assert ei.value.reason is RejectReason.PROCESSING_TIMEOUT
    assert await leader == "leader-result"
    assert sf.in_flight() == 0
    logger.info("等待者超时: 自身 processing_timeout, leader 仍正常完成")


async def test_waiter_cancel_does_not_affect_others() -> None:
    sf = SingleFlight("asset")
    started = asyncio.Event()

    async def work() -> str:
        started.set()
        await asyncio.sleep(0.25)
        return "shared"

    leader = asyncio.ensure_future(sf.execute("asset:r", work, timeout=2))
    await started.wait()
    cancellee = asyncio.ensure_future(sf.execute("asset:r", work, timeout=2))
    other = asyncio.ensure_future(sf.execute("asset:r", work, timeout=2))
    await asyncio.sleep(0.05)
    cancellee.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancellee
    assert await leader == "shared"
    assert await other == "shared"
    assert sf.in_flight() == 0
    logger.info("等待者取消: 该等待者退出, leader 与另一等待者照常拿到结果")
