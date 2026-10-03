"""端到端 API 测试：限流拒绝、队列满、排队超时、客户端断开、额度回收。"""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from app.config import Settings
from app.config import BusinessConfig
from app.main import create_app
from app.registry import GateRegistry
from app.manager import BackpressureManager

logger = logging.getLogger("test.api")


async def _make_client(settings: Settings):
    """用给定配置构造一个独立 app + ASGI client。"""
    app = create_app()
    app.state.settings = settings
    app.state.manager = BackpressureManager(settings)
    transport = httpx.ASGITransport(app=app)
    return app, httpx.AsyncClient(transport=transport, base_url="http://bp")


@pytest.fixture()
async def client(monkeypatch: pytest.MonkeyPatch):
    """使用小阈值配置，便于压出各种拒绝场景。

    report 限流阈值较高（100/s），限流场景在独立应用里单独构造。
    """
    settings = Settings(
        businesses={
            "report": BusinessConfig(
                business="report",
                max_concurrency=2,
                max_queue=0,
                queue_timeout=0.3,
                rate_limit=100,
                rate_window=1.0,
                processing_timeout=5.0,
                singleflight=False,
            ),
            "asset": BusinessConfig(
                business="asset",
                max_concurrency=2,
                max_queue=4,
                queue_timeout=1.0,
                rate_limit=100,
                rate_window=1.0,
                processing_timeout=5.0,
                singleflight=True,
            ),
        },
        default=BusinessConfig(business="default"),
    )
    app = create_app()
    app.state.settings = settings
    app.state.manager = BackpressureManager(settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://bp"
    ) as c:
        yield c, app


async def test_rate_limited_distinct_status_and_reason(client) -> None:
    # 独立应用：每秒 3 个请求；容量(并发+队列) >= 6，gate 永不拒绝，
    # 因此 6 请求里恰好 3 通过、3 被 rate_limited，结果确定。
    settings = Settings(
        businesses={
            "report": BusinessConfig(
                business="report",
                max_concurrency=6,
                max_queue=6,
                queue_timeout=1.0,
                rate_limit=3,
                rate_window=1.0,
                processing_timeout=5.0,
                singleflight=False,
            ),
        },
        default=BusinessConfig(business="default"),
    )
    app, c = await _make_client(settings)
    async with c:
        async def one() -> httpx.Response:
            return await c.post(
                "/process/report", json={"resource": "x", "delay": 0.01}
            )

        responses = await asyncio.gather(*(one() for _ in range(6)))
        rate = [
            r for r in responses
            if r.status_code == 429
            and r.json()["error"]["reason"] == "rate_limited"
        ]
        accepted = [r for r in responses if r.status_code == 200]
        assert len(accepted) == 3
        assert len(rate) == 3
        assert float(rate[0].headers["retry-after"]) >= 0
        logger.info(
            "限流快速失败: 6 请求 -> 3 通过 + 3 个 HTTP429/rate_limited"
        )


async def test_queue_full_distinct_from_timeout(client) -> None:
    c, _ = client
    # report: 2 并发、0 队列。前 2 个立即占满，第 3 个立刻 503 queue_full。
    async def slow() -> httpx.Response:
        return await c.post("/process/report", json={"resource": "q", "delay": 0.3})

    leaders = [asyncio.ensure_future(slow()) for _ in range(2)]
    await asyncio.sleep(0.05)  # 确保前两个已进入执行
    extra = await slow()
    assert extra.status_code == 503
    assert extra.json()["error"]["reason"] == "queue_full"
    results = await asyncio.gather(*leaders)
    assert sorted(r.status_code for r in results) == [200, 200]
    logger.info("队列满: 容量2, 第3个请求 HTTP503/queue_full, 前2个成功")


async def test_queue_timeout_distinct_from_full() -> None:
    # 专门配置：2 并发 + 1 队列，排队超时 0.2s
    settings = Settings(
        businesses={
            "report": BusinessConfig(
                business="report",
                max_concurrency=2,
                max_queue=1,
                queue_timeout=0.2,
                rate_limit=100,
                rate_window=1.0,
                processing_timeout=5.0,
                singleflight=False,
            ),
        },
        default=BusinessConfig(business="default"),
    )
    app = create_app()
    app.state.settings = settings
    app.state.manager = BackpressureManager(settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://bp"
    ) as c:

        async def slow() -> httpx.Response:
            return await c.post(
                "/process/report", json={"resource": "t", "delay": 0.6}
            )

        leaders = [asyncio.ensure_future(slow()) for _ in range(2)]
        await asyncio.sleep(0.05)
        queued = asyncio.ensure_future(slow())  # 唯一排队者
        r = await queued
        assert r.status_code == 408
        assert r.json()["error"]["reason"] == "queue_timeout"
        results = await asyncio.gather(*leaders)
        assert all(x.status_code == 200 for x in results)
        logger.info("排队超时: 排队者 HTTP408/queue_timeout, 两个执行请求成功")


async def test_client_disconnect_releases_slots(client) -> None:
    """调用方中途断开：执行中的任务被取消，active 立刻归还。"""
    c, app = client

    async def slow_waiting_for_cancel() -> None:
        req = c.build_request(
            "POST", "/process/report",
            json={"resource": "cancel", "delay": 5.0},
        )
        task = asyncio.ensure_future(c.send(req))
        await asyncio.sleep(0.1)  # 等它进入执行
        assert app.state.manager.registry.gate_for("report").active == 1
        task.cancel()
        with pytest.raises(BaseException):
            await task
        # 取消后额度应立即回收
        await asyncio.sleep(0.05)
        assert app.state.manager.registry.gate_for("report").active == 0
        logger.info("客户端断开: active 1 -> 0, 额度立即回收")

    await slow_waiting_for_cancel()

    # 回收后新请求可以正常拿到全部额度
    async def quick() -> httpx.Response:
        return await c.post("/process/report", json={"resource": "ok", "delay": 0.05})

    rs = await asyncio.gather(*(quick() for _ in range(2)))
    assert all(r.status_code == 200 for r in rs)
    assert app.state.manager.registry.gate_for("report").active == 0
    logger.info("断开回收后再压 2 并发: 全部成功, active 归零")


async def test_singleflight_coalescing_and_recovery(client) -> None:
    c, app = client
    sf = app.state.manager.singleflight_for("asset")
    assert sf is not None

    async def one() -> tuple[int, bool]:
        r = await c.post("/process/asset", json={"resource": "fund-A", "delay": 0.3})
        body = r.json()
        return r.status_code, bool(body.get("coalesced"))

    results = await asyncio.gather(*(one() for _ in range(6)))
    codes = [code for code, _ in results]
    coalesced = [flag for _, flag in results]
    assert codes == [200] * 6
    assert coalesced.count(False) == 1 and coalesced.count(True) == 5
    logger.info("同资源合并: 6 个 asset 请求 -> 1 个真实执行, 5 个 coalesced")
