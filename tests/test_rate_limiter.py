"""令牌桶速率限制单元测试。"""

from __future__ import annotations

import asyncio

from app.rate_limiter import TokenBucketRateLimiter


async def test_burst_capacity_then_reject():
    rl = TokenBucketRateLimiter(rate=5, burst=3)
    assert [rl.allow("k") for _ in range(3)] == [True, True, True]
    assert rl.allow("k") is False


async def test_keys_are_independent():
    rl = TokenBucketRateLimiter(rate=1, burst=1)
    assert rl.allow("a") is True
    assert rl.allow("a") is False
    assert rl.allow("b") is True


async def test_tokens_refill_over_time():
    rl = TokenBucketRateLimiter(rate=10, burst=1)
    assert rl.allow("k") is True
    assert rl.allow("k") is False
    await asyncio.sleep(0.15)
    assert rl.allow("k") is True


async def test_retry_after_is_positive_when_empty():
    rl = TokenBucketRateLimiter(rate=2, burst=1)
    rl.allow("k")
    ra = rl.retry_after("k")
    assert 0 < ra <= 0.6


async def test_idle_buckets_are_pruned():
    rl = TokenBucketRateLimiter(rate=1000, burst=1, idle_ttl=0.0)
    rl.allow("idle")
    await asyncio.sleep(0.01)  # 等待令牌回满且空闲时间超过 TTL
    removed = await rl.prune_idle()
    assert removed >= 1
    assert "idle" not in rl._buckets
