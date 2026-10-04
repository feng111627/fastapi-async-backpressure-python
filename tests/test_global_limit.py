"""全局容量保护层、慢速串行限流、取消回收全局额度、状态观测。

覆盖本轮新增行为：
- 全局同时处理数上限触发时的拒绝，且能与业务自己的 queue_full 区分；
- 全局单位时间放行上限触发时的拒绝（global_rate_limited）；
- 慢速串行流量下业务限流依然生效（窗口内第 N+1 个被拒，窗口滚后恢复）；
- 客户端断开后全局并发额度立即回收；
- /status 暴露全局与各业务窗口用量/被拒计数，且查询本身不占额度。
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from app.config import BusinessConfig, GlobalConfig, Settings
from app.main import create_app
from app.manager import BackpressureManager

logger = logging.getLogger("test.global_limit")


def _settings(
    *,
    global_cfg: GlobalConfig,
    businesses: dict[str, BusinessConfig],
) -> Settings:
    return Settings(
        businesses=businesses,
        default=BusinessConfig(business="default"),
        global_=global_cfg,
    )


def _biz(name: str, **over: object) -> BusinessConfig:
    base = dict(
        business=name,
        max_concurrency=8,
        max_queue=8,
        queue_timeout=1.0,
        rate_limit=1000,
        rate_window=1.0,
        processing_timeout=5.0,
        singleflight=False,
    )
    base.update(over)
    return BusinessConfig(**base)  # type: ignore[arg-type]


async def _make_client(settings: Settings):
    app = create_app()
    app.state.settings = settings
    app.state.manager = BackpressureManager(settings)
    transport = httpx.ASGITransport(app=app)
    return app, httpx.AsyncClient(transport=transport, base_url="http://bp")


async def test_global_concurrency_reject_distinct_from_business() -> None:
    """全局并发打满 -> 503/global_concurrency；业务队列满 -> 503/queue_full。"""
    settings = _settings(
        global_cfg=GlobalConfig(max_concurrency=2, rate_limit=1000),
        businesses={
            "report": _biz("report"),
            "export": _biz("export"),
        },
    )
    app, c = await _make_client(settings)
    async with c:
        async def slow(business: str) -> httpx.Response:
            return await c.post(
                f"/process/{business}", json={"resource": "g", "delay": 0.4}
            )

        # 两个不同业务各占 1 个全局名额（各自业务额度远未用完）
        leaders = [asyncio.ensure_future(slow("report")),
                   asyncio.ensure_future(slow("export"))]
        await asyncio.sleep(0.1)
        assert app.state.manager.global_limiter.active == 2

        third = await slow("report")
        assert third.status_code == 503
        assert third.json()["error"]["reason"] == "global_concurrency"
        logger.info(
            "全局并发上限: report+export 占满全局 2 名额, 第 3 个请求 "
            "HTTP503/global_concurrency（业务自身额度远未满）"
        )

        results = await asyncio.gather(*leaders)
        assert all(r.status_code == 200 for r in results)
        assert app.state.manager.global_limiter.active == 0

        # 对照：全局额度充足、业务自己打满时，原因是 queue_full
        settings2 = _settings(
            global_cfg=GlobalConfig(max_concurrency=100, rate_limit=1000),
            businesses={"report": _biz("report", max_concurrency=1, max_queue=0)},
        )
        _, c2 = await _make_client(settings2)
        async with c2:
            first = asyncio.ensure_future(
                c2.post("/process/report", json={"resource": "b", "delay": 0.3})
            )
            await asyncio.sleep(0.05)
            second = await c2.post(
                "/process/report", json={"resource": "b", "delay": 0.01}
            )
            assert second.status_code == 503
            assert second.json()["error"]["reason"] == "queue_full"
            assert (await first).status_code == 200
        logger.info(
            "原因区分: 全局满=global_concurrency, 业务满=queue_full, 可区分"
        )


async def test_global_rate_limit_reject() -> None:
    """多业务合计打满全局单位时间放行上限 -> 429/global_rate_limited。"""
    settings = _settings(
        global_cfg=GlobalConfig(max_concurrency=100, rate_limit=3, rate_window=1.0),
        businesses={
            "report": _biz("report"),
            "export": _biz("export"),
            "asset": _biz("asset"),
        },
    )
    app, c = await _make_client(settings)
    async with c:
        async def one(business: str) -> httpx.Response:
            return await c.post(
                f"/process/{business}", json={"resource": "r", "delay": 0.01}
            )

        # 三个业务轮流发，各自远未到自己的限额，但合计超过全局 3/s
        responses = []
        for business in ("report", "export", "asset", "report", "export"):
            responses.append(await one(business))

        accepted = [r for r in responses if r.status_code == 200]
        rejected = [r for r in responses if r.status_code == 429]
        assert len(accepted) == 3
        assert len(rejected) == 2
        for r in rejected:
            assert r.json()["error"]["reason"] == "global_rate_limited"
            assert float(r.headers["retry-after"]) >= 0
        logger.info(
            "全局限流: report/export/asset 合计 5 请求 -> 3 通过 + "
            "2 个 HTTP429/global_rate_limited（各业务自身限额均未触达）"
        )

        st = (await c.get("/status")).json()
        assert st["global"]["rate"]["used"] == 3
        assert st["global"]["rate"]["remaining"] == 0
        assert st["global"]["rate"]["rejected"] == 2
        logger.info("状态查询: global.rate used=3 remaining=0 rejected=2, 对得上")


async def test_slow_serial_rate_limit_and_window_recovery() -> None:
    """慢速串行：窗口内第 N+1 个被拒，空闲停顿不清零，窗口滚后恢复。"""
    settings = _settings(
        global_cfg=GlobalConfig(max_concurrency=100, rate_limit=1000),
        businesses={"report": _biz("report", rate_limit=3, rate_window=0.6)},
    )
    app, c = await _make_client(settings)
    async with c:
        # 一个一个慢慢发：每个请求都处理完、闸门回到空闲再发下一个
        for i in range(3):
            r = await c.post(
                "/process/report", json={"resource": "s", "delay": 0.02}
            )
            assert r.status_code == 200
            await asyncio.sleep(0.1)  # 空闲停顿：旧实现会在此刻偷偷清零
        logger.info("慢速串行: 3 个请求间隔发送全部成功（窗口 0.6s 内）")

        st = (await c.get("/status")).json()
        assert st["dimensions"]["report"]["rate"]["used"] == 3
        assert st["dimensions"]["report"]["rate"]["remaining"] == 0

        # 窗口内第 4 个：必须被拒，且 retry_after 与窗口剩余时间对得上
        r4 = await c.post("/process/report", json={"resource": "s", "delay": 0.02})
        assert r4.status_code == 429
        body = r4.json()
        assert body["error"]["reason"] == "rate_limited"
        retry_after = float(r4.headers["retry-after"])
        assert 0 < retry_after <= 0.6
        st = (await c.get("/status")).json()
        rate = st["dimensions"]["report"]["rate"]
        assert rate["rejected"] == 1
        assert abs(rate["retry_after"] - retry_after) < 0.2
        logger.info(
            "慢速串行: 窗口内第 4 个请求 HTTP429/rate_limited, "
            "retry_after=%.3fs, 窗口被拒计数=1", retry_after,
        )

        # 等窗口自然滚动：额度按规则恢复，不会被永久锁死；
        # 窗口计数（含被拒数）随之重置
        await asyncio.sleep(retry_after + 0.1)
        r5 = await c.post("/process/report", json={"resource": "s", "delay": 0.02})
        assert r5.status_code == 200
        st = (await c.get("/status")).json()
        rate = st["dimensions"]["report"]["rate"]
        assert rate["used"] == 1 and rate["rejected"] == 0
        logger.info("窗口滚动后: 额度恢复放行, used/rejected 计数已重置")


async def test_disconnect_releases_global_slot() -> None:
    """调用方断开：全局并发名额与业务额度都立即回收。"""
    settings = _settings(
        global_cfg=GlobalConfig(max_concurrency=1, rate_limit=1000),
        businesses={"report": _biz("report")},
    )
    app, c = await _make_client(settings)
    async with c:
        req = c.build_request(
            "POST", "/process/report", json={"resource": "d", "delay": 5.0}
        )
        task = asyncio.ensure_future(c.send(req))
        await asyncio.sleep(0.1)
        assert app.state.manager.global_limiter.active == 1

        task.cancel()
        try:
            await task
        except BaseException:
            pass
        await asyncio.sleep(0.05)
        assert app.state.manager.global_limiter.active == 0
        assert app.state.manager.registry.peek_gate("report").active == 0  # type: ignore[union-attr]
        logger.info("客户端断开: 全局 active 1 -> 0, 业务 active 1 -> 0")

        # 回收后全局唯一名额可再次使用
        r = await c.post("/process/report", json={"resource": "d", "delay": 0.02})
        assert r.status_code == 200
        logger.info("断开回收后: 新请求正常拿到全局名额并成功")


async def test_status_query_does_not_consume_quota() -> None:
    """状态查询本身不占额度、不影响限流判定。"""
    settings = _settings(
        global_cfg=GlobalConfig(max_concurrency=100, rate_limit=10),
        businesses={"report": _biz("report", rate_limit=2, rate_window=1.0)},
    )
    app, c = await _make_client(settings)
    async with c:
        for _ in range(5):
            st = (await c.get("/status")).json()
        assert st["global"]["rate"]["used"] == 0  # 查询没消耗全局额度

        r1 = await c.post("/process/report", json={"resource": "q", "delay": 0.01})
        assert r1.status_code == 200
        for _ in range(5):
            st = (await c.get("/status")).json()
        # 多次查询后用量仍是 1：观测不改变判定
        assert st["dimensions"]["report"]["rate"]["used"] == 1
        assert st["global"]["rate"]["used"] == 1

        r2 = await c.post("/process/report", json={"resource": "q", "delay": 0.01})
        assert r2.status_code == 200
        r3 = await c.post("/process/report", json={"resource": "q", "delay": 0.01})
        assert r3.status_code == 429  # 业务窗口 2 个用完，第 3 个被拒
        logger.info(
            "状态查询: 连续查询不占额度, report 窗口 2 额度用完第 3 个被拒"
        )
