"""全局容量保护层 + 慢速串行限流的集成测试。

覆盖：
- 全局在途数上限：多业务合计触顶立即拒绝，原因与业务层 queue_full 可区分；
- 全局单位时间放行上限：各业务都没超自己的额度，合计超全局照样拒；
- 慢速串行流量：请求一个个来（闸门间隙回到空闲），窗口内第 N+1 个仍被拒，
  窗口到点后额度恢复，不会永久锁死；
- 取消/断开后全局在途名额立即回收；
- /status 同时暴露全局与各业务的占用/窗口用量/本窗口拒绝数，且查询不耗额度。
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx
import pytest

from app.config import BusinessConfig, GlobalConfig, Settings
from app.errors import RejectReason, ThrottledError
from app.main import create_app
from app.manager import BackpressureManager

logger = logging.getLogger("test.global_limit")


async def _handler(resource: str, *, delay: float = 0.0) -> dict[str, object]:
    await asyncio.sleep(delay)
    return {"resource": resource}


def _settings(
    *,
    global_max_concurrency: int = 100,
    global_rate_limit: int = 1000,
    report_rate_limit: int = 100,
    report_rate_window: float = 60.0,
    report_max_concurrency: int = 4,
    report_max_queue: int = 4,
) -> Settings:
    """业务额度宽松、全局额度按需收紧的配置工厂。"""
    return Settings(
        businesses={
            "report": BusinessConfig(
                business="report",
                max_concurrency=report_max_concurrency,
                max_queue=report_max_queue,
                queue_timeout=1.0,
                rate_limit=report_rate_limit,
                rate_window=report_rate_window,
                processing_timeout=5.0,
            ),
            "export": BusinessConfig(
                business="export",
                max_concurrency=4,
                max_queue=4,
                queue_timeout=1.0,
                rate_limit=100,
                rate_window=60.0,
                processing_timeout=5.0,
            ),
        },
        default=BusinessConfig(business="default"),
        global_limit=GlobalConfig(
            max_concurrency=global_max_concurrency,
            rate_limit=global_rate_limit,
            rate_window=60.0,
        ),
    )


async def test_global_concurrency_limit_rejects_immediately() -> None:
    """report+export 合计占满全局在途上限后，第 3 个请求立即被拒（不排队）。"""
    manager = BackpressureManager(_settings(global_max_concurrency=2))

    t1 = asyncio.ensure_future(
        manager.dispatch("report", "r1", _handler, delay=0.3)
    )
    t2 = asyncio.ensure_future(
        manager.dispatch("export", "e1", _handler, delay=0.3)
    )
    await asyncio.sleep(0.05)
    assert manager.global_limiter.active == 2

    started = time.monotonic()
    with pytest.raises(ThrottledError) as ei:
        await manager.dispatch("report", "r2", _handler, delay=0.01)
    elapsed = time.monotonic() - started
    assert ei.value.reason is RejectReason.GLOBAL_CONCURRENCY
    assert elapsed < 0.1, "触发全局上限必须立即拒绝，不能排队干等"
    logger.info(
        "全局在途上限: report+export 占满 2 名额, 第 3 个请求 "
        "reason=global_concurrency, 耗时 %.3fs（未排队）", elapsed,
    )

    await asyncio.gather(t1, t2)
    assert manager.global_limiter.active == 0
    logger.info("在途请求结束后全局名额归零: active=%d", manager.global_limiter.active)


async def test_global_and_business_rejections_are_distinguishable() -> None:
    """同样是 503，全局并发上限与业务队列满要给出不同的 reason。"""
    # 场景 A：全局额度充足，业务自己队列满 -> queue_full
    app_a = create_app()
    app_a.state.settings = _settings(
        global_max_concurrency=10, report_max_concurrency=1, report_max_queue=0
    )
    app_a.state.manager = BackpressureManager(app_a.state.settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_a), base_url="http://bp"
    ) as c:
        slow = asyncio.ensure_future(
            c.post("/process/report", json={"resource": "a", "delay": 0.3})
        )
        await asyncio.sleep(0.05)
        r = await c.post("/process/report", json={"resource": "b", "delay": 0.01})
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "queue_full"
        await slow
    logger.info("业务层拒绝: reason=queue_full (HTTP503)")

    # 场景 B：业务额度充足，全局在途满 -> global_concurrency
    app_b = create_app()
    app_b.state.settings = _settings(global_max_concurrency=1)
    app_b.state.manager = BackpressureManager(app_b.state.settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_b), base_url="http://bp"
    ) as c:
        slow = asyncio.ensure_future(
            c.post("/process/report", json={"resource": "a", "delay": 0.3})
        )
        await asyncio.sleep(0.05)
        r = await c.post("/process/export", json={"resource": "b", "delay": 0.01})
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "global_concurrency"
        await slow
    logger.info("全局层拒绝: reason=global_concurrency (HTTP503), 与业务层可区分")


async def test_global_rate_limit_spans_businesses() -> None:
    """每个业务都没超自己的额度，但合计超过全局放行上限照样被拒。"""
    manager = BackpressureManager(_settings(global_rate_limit=3))

    for i in range(3):
        business = ("report", "export", "report")[i]
        result = await manager.dispatch(business, f"r{i}", _handler, delay=0.01)
        assert result.payload["resource"] == f"r{i}"

    with pytest.raises(ThrottledError) as ei:
        await manager.dispatch("export", "r3", _handler, delay=0.01)
    assert ei.value.reason is RejectReason.GLOBAL_RATE_LIMITED
    assert ei.value.retry_after is not None and ei.value.retry_after > 0
    logger.info(
        "全局放行上限: 3 个请求分属 report/export 均未超各自额度, "
        "第 4 个 reason=global_rate_limited, retry_after=%.2f",
        ei.value.retry_after,
    )


async def test_slow_serial_traffic_still_rate_limited() -> None:
    """慢速串行：请求一个个来、闸门在间隙回到空闲，窗口内第 N+1 个仍被拒。

    窗口到点后额度恢复（不会永久锁死），本窗口拒绝计数随窗口重置。
    """
    manager = BackpressureManager(
        _settings(report_rate_limit=3, report_rate_window=0.4)
    )

    for i in range(3):
        await manager.dispatch("report", f"s{i}", _handler, delay=0.01)
        await asyncio.sleep(0.05)  # 串行间隔：期间闸门完全空闲
    entry = manager.registry._entries["report"]  # noqa: SLF001
    snap = entry.rate_limiter.snapshot("report")
    assert snap["used"] == 3 and snap["remaining"] == 0

    with pytest.raises(ThrottledError) as ei:
        await manager.dispatch("report", "s3", _handler, delay=0.01)
    assert ei.value.reason is RejectReason.RATE_LIMITED
    assert 0 < ei.value.retry_after <= 0.4
    assert entry.rate_limiter.snapshot("report")["rejected"] == 1
    logger.info(
        "慢速串行(report): 3 个间隔 50ms 的请求占满窗口, 第 4 个 "
        "reason=rate_limited, retry_after=%.3f", ei.value.retry_after,
    )

    await asyncio.sleep(0.4)  # 等窗口自然滚动过去
    result = await manager.dispatch("report", "s4", _handler, delay=0.01)
    assert result.payload["resource"] == "s4"
    # 窗口过期后旧维度已被回收，重新取当前维度的快照
    entry = manager.registry._entries["report"]  # noqa: SLF001
    snap = entry.rate_limiter.snapshot("report")
    assert snap["used"] == 1 and snap["rejected"] == 0
    logger.info("窗口到点后: report 恢复放行, 已用/拒绝计数随窗口重置")


async def test_cancel_releases_global_slot() -> None:
    """处理中被取消：全局在途名额立即归还，后续请求不受影响。"""
    manager = BackpressureManager(_settings(global_max_concurrency=1))

    task = asyncio.ensure_future(
        manager.dispatch("report", "slow", _handler, delay=5.0)
    )
    await asyncio.sleep(0.05)
    assert manager.global_limiter.active == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert manager.global_limiter.active == 0
    logger.info("取消回收: 全局在途名额 1 -> 0")

    result = await manager.dispatch("export", "after", _handler, delay=0.01)
    assert result.payload["resource"] == "after"
    assert manager.global_limiter.active == 0
    logger.info("回收后 export 立即拿到全局额度并成功, active 归零")


async def test_status_exposes_both_layers_without_consuming_quota() -> None:
    """/status 暴露全局+业务两层观测，且查询本身不消耗限流额度。"""
    app = create_app()
    app.state.settings = _settings(report_rate_limit=2, report_rate_window=60.0)
    app.state.manager = BackpressureManager(app.state.settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://bp"
    ) as c:
        for i in range(2):
            r = await c.post("/process/report", json={"resource": f"r{i}"})
            assert r.status_code == 200
        r = await c.post("/process/report", json={"resource": "r2"})
        assert r.status_code == 429
        assert r.json()["error"]["reason"] == "rate_limited"

        s1 = (await c.get("/status")).json()
        report = s1["dimensions"]["report"]
        assert report["rate"]["used"] == 2
        assert report["rate"]["remaining"] == 0
        assert report["rate"]["rejected"] == 1
        glob = s1["global"]
        assert glob["max_concurrency"] == 100
        assert glob["rate"]["used"] == 3  # 被拒的请求也在全局层计过数
        assert glob["rate"]["rejected"] == 0
        assert glob["active"] == 0

        # 再查两次 /status：计数纹丝不动，说明查询不耗额度
        s2 = (await c.get("/status")).json()
        s3 = (await c.get("/status")).json()
        assert s2["dimensions"]["report"]["rate"]["used"] == 2
        assert s3["global"]["rate"]["used"] == 3
        logger.info(
            "状态查询: report 窗口 used=2/remaining=0/rejected=1, "
            "全局 used=3; 连续查询不改变任何计数",
        )
