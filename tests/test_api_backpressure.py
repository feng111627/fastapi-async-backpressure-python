"""HTTP 层集成测试：拒绝区分、取消回收、请求合并。"""

from __future__ import annotations

import asyncio

import pytest


async def test_queue_full_and_timeout_have_distinct_feedback(client):
    """队列满(503 queue_full) 与排队超时(504 queue_timeout) 必须可区分。"""
    # export 维度在测试配置中：2 并发 / 2 队列 / 0.15s 排队超时
    holders = [
        asyncio.ensure_future(
            client.post("/work/export", json={"duration": 0.5})
        )
        for _ in range(4)
    ]
    await asyncio.sleep(0.05)  # 让前 4 个请求各就各位

    # 第 5 个：队列已满 -> 立即 503 queue_full
    r = await client.post("/work/export", json={"duration": 0.01})
    assert r.status_code == 503
    assert r.json()["error"] == "queue_full"

    # 释放一个排队位，再塞入一个请求让它排队等超时
    holders[3].cancel()
    await asyncio.gather(holders[3], return_exceptions=True)
    await asyncio.sleep(0.02)
    waiter = asyncio.ensure_future(
        client.post("/work/export", json={"duration": 0.01})
    )
    await asyncio.sleep(0.02)
    rw = await waiter
    assert rw.status_code == 504
    assert rw.json()["error"] == "queue_timeout"

    for t in holders[:3]:
        t.cancel()
    await asyncio.gather(*holders[:3], return_exceptions=True)
    await asyncio.sleep(0.05)


async def test_rate_limited_is_distinct_rejection(client):
    """webhook 维度 burst=2：同 client 第 3 个请求立即 429 rate_limited。"""
    good = await asyncio.gather(
        client.post("/work/webhook", json={"duration": 0.01, "client": "u1"}),
        client.post("/work/webhook", json={"duration": 0.01, "client": "u1"}),
    )
    assert [r.status_code for r in good] == [200, 200]
    r = await client.post(
        "/work/webhook", json={"duration": 0.01, "client": "u1"}
    )
    assert r.status_code == 429
    body = r.json()
    assert body["error"] == "rate_limited"
    assert int(r.headers["Retry-After"]) >= 1

    # 与排队超时/队列满明确不同
    assert body["reason"] != "queue_full"


async def test_rate_keys_are_independent(client):
    """不同调用方的速率额度互不影响。"""
    r1 = await client.post(
        "/work/webhook", json={"duration": 0.01, "client": "a"}
    )
    r2 = await client.post(
        "/work/webhook", json={"duration": 0.01, "client": "b"}
    )
    assert r1.status_code == r2.status_code == 200


async def test_coalesced_concurrent_requests_share_execution(client):
    """同一 report_id 的并发请求只执行一次，全部拿到结果。"""
    results = await asyncio.gather(
        *[
            client.post("/work/report/r-100", json={"duration": 0.15})
            for _ in range(5)
        ]
    )
    assert all(r.status_code == 200 for r in results), [r.text for r in results]
    stats = (await client.get("/stats")).json()["dimensions"]["report"]
    assert stats["coalesced_calls"] == 5
    assert stats["coalesced_executions"] == 1
    assert stats["in_flight"] == 0


async def test_coalesced_failure_can_reexecute(client):
    """合并执行失败后，flight 不缓存，后续请求必须能重新真实执行。"""
    bad = await asyncio.gather(
        client.post("/work/report/r-err", json={"duration": 0.02, "fail": True}),
        client.post("/work/report/r-err", json={"duration": 0.02, "fail": True}),
    )
    assert all(r.status_code == 500 for r in bad), [r.text for r in bad]
    await asyncio.sleep(0.02)
    ok = await client.post("/work/report/r-err", json={"duration": 0.01})
    assert ok.status_code == 200, ok.text
    stats = (await client.get("/stats")).json()["dimensions"]["report"]
    assert stats["coalesced_executions"] == 2  # 失败 1 次 + 重试 1 次


async def test_cancel_mid_processing_reclaims_slot(client):
    """处理中客户端断开（取消请求）后，并发额度立即回收。"""
    t = asyncio.ensure_future(
        client.post("/work/report/r-cancel", json={"duration": 1.0})
    )
    await asyncio.sleep(0.1)  # 确认已在处理中
    stats = (await client.get("/stats")).json()["dimensions"]["report"]
    assert stats["in_flight"] == 1

    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    await client._transport.wait_drained()  # type: ignore[attr-defined]
    await asyncio.sleep(0.05)  # 等待取消传播与槽位回收

    stats = (await client.get("/stats")).json()["dimensions"]["report"]
    assert stats["in_flight"] == 0, stats
    assert stats["waiting"] == 0

    # 额度恢复：新请求可以立即被处理
    r = await client.post("/work/report/r-cancel", json={"duration": 0.02})
    assert r.status_code == 200, r.text


async def test_slots_recover_after_sustained_pressure(client):
    """连续压测多轮后：无额度泄漏、无队列残留。"""
    async def round_(burst: int) -> tuple[int, int]:
        accepted = rejected = 0
        res = await asyncio.gather(
            *[
                client.post("/work/export", json={"duration": 0.05})
                for _ in range(burst)
            ]
        )
        for r in res:
            if r.status_code == 200:
                accepted += 1
            elif r.status_code in (503, 504):
                rejected += 1
            else:
                pytest.fail(f"unexpected status {r.status_code}: {r.text}")
        return accepted, rejected

    total_accepted = total_rejected = 0
    for _ in range(6):
        a, r = await round_(10)
        total_accepted += a
        total_rejected += r
        await asyncio.sleep(0.05)
        stats = (await client.get("/stats")).json()["dimensions"]["export"]
        assert stats["in_flight"] == 0, stats
        assert stats["waiting"] == 0, stats
    assert total_accepted > 0 and total_rejected > 0
