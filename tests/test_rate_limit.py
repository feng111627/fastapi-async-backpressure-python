"""RateLimiter：单位时间上限、快速失败、窗口重置。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.errors import RejectReason, ThrottledError
from app.rate_limit import RateLimiter

logger = logging.getLogger("test.rate_limit")


async def test_rejects_beyond_limit_fast() -> None:
    rl = RateLimiter(limit=5, window=1.0)
    for _ in range(5):
        await rl.acquire("report")
    with pytest.raises(ThrottledError) as ei:
        await rl.acquire("report")
    assert ei.value.reason is RejectReason.RATE_LIMITED
    assert ei.value.retry_after is not None and ei.value.retry_after > 0
    logger.info("单位时间上限: 第 6 个请求 reason=rate_limited, 未等待")


async def test_window_resets_after_expiry() -> None:
    clock = {"t": 100.0}
    rl = RateLimiter(limit=2, window=1.0, clock=lambda: clock["t"])
    await rl.acquire("k")
    await rl.acquire("k")
    with pytest.raises(ThrottledError):
        await rl.acquire("k")
    clock["t"] += 1.001  # 窗口过去
    await rl.acquire("k")
    assert rl.used("k") == 1
    logger.info("窗口重置: 过期后请求重新放行, used=%d", rl.used("k"))


async def test_slow_serial_requests_still_limited() -> None:
    """慢速串行：请求拉开间隔逐个到达，窗口内第 N+1 个照样被拒。

    中间的空闲停顿不能把已用掉的量清零；窗口自然滚动后额度恢复，
    本窗口的拒绝计数也随窗口一起重置。
    """
    clock = {"t": 1000.0}
    rl = RateLimiter(limit=3, window=1.0, clock=lambda: clock["t"])
    for _ in range(3):
        await rl.acquire("report")
        clock["t"] += 0.2  # 每个请求之间隔 0.2s 的空闲停顿
    snap = rl.snapshot("report")
    assert snap["used"] == 3 and snap["remaining"] == 0

    with pytest.raises(ThrottledError) as ei:
        await rl.acquire("report")
    assert ei.value.reason is RejectReason.RATE_LIMITED
    # retry_after 与实际剩余时间一致：窗口从 t=1000 开始，现在 t=1000.6
    assert ei.value.retry_after == pytest.approx(0.4, abs=1e-6)
    assert rl.snapshot("report")["rejected"] == 1
    logger.info(
        "慢速串行: 3 个间隔 0.2s 的请求占满窗口, 第 4 个被拒, "
        "retry_after=%.3f", ei.value.retry_after,
    )

    clock["t"] += 0.41  # 窗口自然滚动过去
    await rl.acquire("report")  # 恢复放行，不会永久锁死
    snap = rl.snapshot("report")
    assert snap["used"] == 1 and snap["rejected"] == 0
    logger.info("窗口滚动后: 额度恢复 used=1, 拒绝计数重置为 0")


async def test_concurrent_contention_no_overcount() -> None:
    """高并发下计数准确，成功数恰好等于上限。"""
    rl = RateLimiter(limit=10, window=1.0)
    allowed = 0
    rejected = 0

    async def hit() -> None:
        nonlocal allowed, rejected
        try:
            await rl.acquire("hot")
            allowed += 1
        except ThrottledError:
            rejected += 1

    await asyncio.gather(*(hit() for _ in range(100)))
    logger.info("100 并发争用 10 额度: allowed=%d rejected=%d", allowed, rejected)
    assert allowed == 10 and rejected == 90
