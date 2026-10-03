"""请求合并（singleflight）单元测试。"""

from __future__ import annotations

import asyncio

import pytest

from app.coalescer import Coalescer


async def test_concurrent_calls_execute_once():
    c = Coalescer()
    execs = 0

    async def work() -> int:
        nonlocal execs
        execs += 1
        await asyncio.sleep(0.05)
        return execs

    results = await asyncio.gather(*[c.call("k", work) for _ in range(6)])
    assert results == [1] * 6
    assert execs == 1


async def test_failure_is_not_cached():
    c = Coalescer()
    tries = 0

    async def flaky() -> None:
        nonlocal tries
        tries += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("boom")

    out = await asyncio.gather(
        c.call("k", flaky), c.call("k", flaky), return_exceptions=True
    )
    assert len(out) == 2 and all(isinstance(e, RuntimeError) for e in out)
    assert tries == 1
    await asyncio.sleep(0)  # 等 flight 清理
    with pytest.raises(RuntimeError):
        await c.call("k", flaky)
    assert tries == 2  # 后续请求获得了真实执行机会


async def test_successful_result_is_not_cached_after_completion():
    c = Coalescer()
    tries = 0

    async def work() -> str:
        nonlocal tries
        tries += 1
        return f"v{tries}"

    assert await c.call("k", work) == "v1"
    assert await c.call("k", work) == "v2"  # 完成后再来 = 重新执行


async def test_follower_cancel_does_not_cancel_shared_execution():
    c = Coalescer()
    done = asyncio.Event()
    execs = 0

    async def slow() -> str:
        nonlocal execs
        execs += 1
        await asyncio.sleep(0.1)
        done.set()
        return "ok"

    leader = asyncio.ensure_future(c.call("k", slow))
    await asyncio.sleep(0.01)
    fol = asyncio.ensure_future(c.call("k", slow))
    await asyncio.sleep(0.01)
    fol.cancel()
    await asyncio.gather(fol, return_exceptions=True)
    assert await leader == "ok"
    assert execs == 1


async def test_all_waiters_cancelling_cancels_execution_and_cleans_key():
    c = Coalescer()
    started = asyncio.Event()
    execs = 0

    async def slow() -> None:
        nonlocal execs
        execs += 1
        started.set()
        await asyncio.sleep(10)

    t1 = asyncio.ensure_future(c.call("k", slow))
    await started.wait()
    t2 = asyncio.ensure_future(c.call("k", slow))
    await asyncio.sleep(0.02)
    t1.cancel()
    t2.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
    await asyncio.sleep(0.02)
    assert c.active_keys() == 0
    assert execs == 1


async def test_distinct_keys_run_separately():
    c = Coalescer()

    async def work() -> None:
        await asyncio.sleep(0.02)

    await asyncio.gather(c.call("a", work), c.call("b", work))
    assert c.executions == 2
