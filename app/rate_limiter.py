"""按维度（key）的单位时间请求量限制：懒填充令牌桶。

- 每个维度一把桶，桶之间互不影响；
- 令牌按固定速率补充，桶容量（burst）限制突发；
- 不允许请求无限堆积：``allow`` 只做即时判定，超限立即返回 False；
- 长期空闲且已回满的桶会被周期性清扫，避免 key 无限增长带来的内存泄漏。
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional


class _Bucket:
    __slots__ = ("tokens", "updated_at")

    def __init__(self, tokens: float, now: float) -> None:
        self.tokens = tokens
        self.updated_at = now


class TokenBucketRateLimiter:
    """每 key 独立的令牌桶。

    参数
    ----
    rate: 每秒补充的令牌数（单位时间允许的平均请求量）。
    burst: 桶容量，即允许的瞬时突发上限；默认取 ``rate``（至少 1）。
    idle_ttl: 桶回满后空闲多久可被清扫（秒）。
    """

    _SWEEP_EVERY = 1024

    def __init__(
        self,
        rate: float,
        burst: Optional[float] = None,
        idle_ttl: float = 60.0,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self._rate = float(rate)
        self._capacity = max(1.0, float(burst if burst is not None else rate))
        self._idle_ttl = idle_ttl
        self._buckets: dict[str, _Bucket] = {}
        self._calls = 0
        self._lock = asyncio.Lock()

    def _refill(self, bucket: _Bucket, now: float) -> float:
        elapsed = now - bucket.updated_at
        if elapsed > 0:
            bucket.tokens = min(self._capacity, bucket.tokens + elapsed * self._rate)
            bucket.updated_at = now
        return bucket.tokens

    def allow(self, key: str, now: Optional[float] = None) -> bool:
        """尝试消费 1 个令牌；不足时立即返回 False（不等待、不入队）。"""
        now = time.monotonic() if now is None else now
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(self._capacity, now)
            self._buckets[key] = bucket
        else:
            self._refill(bucket, now)

        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            self._maybe_sweep(now)
            return True
        return False

    def retry_after(self, key: str, now: Optional[float] = None) -> float:
        """估计距离下一个令牌可用还需多少秒。"""
        now = time.monotonic() if now is None else now
        bucket = self._buckets.get(key)
        tokens = self._capacity if bucket is None else self._refill(bucket, now)
        if tokens >= 1.0:
            return 0.0
        return (1.0 - tokens) / self._rate

    def reset(self, key: str) -> None:
        self._buckets.pop(key, None)

    async def prune_idle(self, now: Optional[float] = None) -> int:
        """删除已回满且空闲超过 TTL 的桶，返回被清理的数量。"""
        now = time.monotonic() if now is None else now
        async with self._lock:
            stale = [
                k
                for k, b in self._buckets.items()
                if self._refill(b, now) >= self._capacity
                and now - b.updated_at >= self._idle_ttl
            ]
            for k in stale:
                del self._buckets[k]
            return len(stale)

    def _maybe_sweep(self, now: float) -> None:
        self._calls += 1
        if self._calls % self._SWEEP_EVERY != 0:
            return
        # 调用方已持有事件循环；同步清扫即可（无 await，不会被打断）。
        stale = [
            k
            for k, b in self._buckets.items()
            if self._refill(b, now) >= self._capacity
            and now - b.updated_at >= self._idle_ttl
        ]
        for k in stale:
            del self._buckets[k]
