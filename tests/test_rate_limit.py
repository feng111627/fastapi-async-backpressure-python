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
