"""本地负载测试：验证高并发下的限流、排队背压、取消回收与内存稳定。

用法（先启动服务）::

    uv run uvicorn main:app --host 127.0.0.1 --port 8000
    uv run python scripts/load_test.py                 # 默认 http://127.0.0.1:8000
    uv run python scripts/load_test.py http://127.0.0.1:9000

也可以直接对 ASGI app 做进程内压测（无需起服务）::

    uv run python scripts/load_test.py --in-process

脚本会分多个阶段分别覆盖：
  1. 稳态吞吐：三类业务各跑一波，确认都能成功；
  2. 突发背压：对低并发业务（export）瞬间打高并发，观察
     200/429(rate_limited)/503(queue_full)/408(queue_timeout) 分布；
  3. 同资源合并：asset 同资源高并发，确认只有 1 次真实执行；
  4. 客户端取消：发起一批长处理后立即断开，随后检查 /status 额度归零；
  5. 恢复 & 内存：连续多轮突发后 active/waiting 归零、注册表维度不膨胀。
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import BusinessConfig, GlobalConfig, Settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.manager import BackpressureManager  # noqa: E402

BUSINESSES = ("report", "export", "asset")
PHASE_RESULTS: list[tuple[str, bool, str]] = []


def record(phase: str, ok: bool, detail: str) -> None:
    PHASE_RESULTS.append((phase, ok, detail))
    flag = "PASS" if ok else "FAIL"
    print(f"[{flag}] {phase}: {detail}")


def classify(resp: httpx.Response) -> str:
    if resp.status_code == 200:
        return "200"
    try:
        reason = resp.json()["error"]["reason"]
    except Exception:
        reason = f"http_{resp.status_code}"
    return f"{resp.status_code}:{reason}"


async def _post(
    client: httpx.AsyncClient,
    business: str,
    resource: str,
    delay: float | None = None,
    fail: bool = False,
) -> httpx.Response:
    body: dict[str, Any] = {"resource": resource}
    if delay is not None:
        body["delay"] = delay
    if fail:
        body["fail"] = True
    return await client.post(f"/process/{business}", json=body)


async def get_status(
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    r = await client.get("/status")
    return r.json()


async def phase_steady(client: httpx.AsyncClient) -> None:
    """阶段 1：每类业务少量并发，全部成功。"""
    async def one(b: str) -> str:
        r = await _post(client, b, f"steady-{b}", delay=0.05)
        return classify(r)

    results = await asyncio.gather(
        *(one(b) for b in BUSINESSES for _ in range(3))
    )
    ok = all(r == "200" for r in results)
    record("稳态吞吐(report/export/asset)", ok,
           f"{len(results)} 个请求结果分布={dict(Counter(results))}")


async def phase_burst_backpressure(client: httpx.AsyncClient) -> None:
    """阶段 2：export（2 并发/2 队列/10qps）瞬时 60 并发。"""
    n = 60

    async def one(i: int) -> str:
        r = await _post(client, "export", f"bulk-{i % 5}", delay=0.4)
        return classify(r)

    results = await asyncio.gather(*(one(i) for i in range(n)))
    dist = Counter(results)
    reasons_seen = set(dist)
    # 必然出现成功与至少一种拒绝；拒绝原因必须是可区分的几类之一
    # （业务层：rate_limited/queue_full/queue_timeout；
    #   全局层：global_rate_limited/global_concurrency）
    rejected = {r for r in reasons_seen if not r.startswith("200")}
    expected_reject_prefixes = {
        "429:rate_limited", "503:queue_full", "408:queue_timeout",
        "429:global_rate_limited", "503:global_concurrency",
    }
    ok = (
        dist.get("200", 0) > 0
        and len(rejected) > 0
        and rejected.issubset(expected_reject_prefixes)
    )
    record("突发背压(export x60)", ok,
           f"结果分布={dict(dist)}，拒绝原因可区分且无 5xx 泄漏")


async def phase_singleflight(client: httpx.AsyncClient) -> None:
    """阶段 3：asset 同资源 20 并发，只应有 1 次真实执行。"""
    n = 20

    async def one() -> tuple[str, bool]:
        r = await _post(client, "asset", "HOT-RESOURCE", delay=0.3)
        if r.status_code != 200:
            return classify(r), False
        return "200", bool(r.json().get("coalesced"))

    results = await asyncio.gather(*(one() for _ in range(n)))
    statuses = Counter(s for s, _ in results)
    coalesced = sum(1 for s, c in results if s == "200" and c)
    leaders = sum(1 for s, c in results if s == "200" and not c)
    ok = statuses.get("200", 0) == n and leaders == 1 and coalesced == n - 1
    record("同资源合并(asset x20 同 resource)", ok,
           f"成功={statuses.get('200', 0)} leader={leaders} "
           f"coalesced={coalesced}（真实执行应=1）")


async def phase_cancel_recovery(client: httpx.AsyncClient) -> None:
    """阶段 4：发起长处理后立即断开，检查额度全部归还。"""
    async def fire_and_abandon() -> None:
        req = client.build_request(
            "POST", "/process/report",
            json={"resource": "abandon", "delay": 10.0},
        )
        task = asyncio.ensure_future(client.send(req))
        await asyncio.sleep(0.05)  # 确保已进入执行
        task.cancel()
        try:
            await task
        except BaseException:
            pass

    await asyncio.gather(*(fire_and_abandon() for _ in range(4)))
    # 服务器检测 TCP 断开有几十~几百毫秒延迟，轮询 status 直到归零（最多 3s）
    active = waiting = -1
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        status = await get_status(client)
        report = status["dimensions"].get("report", {})
        active = report.get("active", 0)
        waiting = report.get("waiting", 0)
        if active == 0 and waiting == 0:
            break
        await asyncio.sleep(0.1)
    ok = active == 0 and waiting == 0
    record("客户端断开取消(report x4)", ok,
           f"断开后 status: active={active} waiting={waiting}（应均为 0）")


async def phase_repeated_burst_no_growth(
    client: httpx.AsyncClient,
) -> None:
    """连续多轮突发后额度归零、注册表维度不持续增长、无对象堆积。"""
    rounds = 8

    async def one_round() -> None:
        await asyncio.gather(*(
            _post(client, "export", f"rb-{i % 6}", delay=0.2)
            for i in range(24)
        ))
        await asyncio.sleep(0.05)

    gc.collect()
    objs_before = len(gc.get_objects())
    for _ in range(rounds // 2):
        await one_round()
    await asyncio.sleep(0.3)
    gc.collect()
    objs_mid = len(gc.get_objects())
    for _ in range(rounds // 2):
        await one_round()
    # 等所有处理结束后再查状态
    await asyncio.sleep(0.6)
    gc.collect()
    objs_after = len(gc.get_objects())
    status = await get_status(client)
    active_sum = sum(d["active"] for d in status["dimensions"].values())
    waiting_sum = sum(d["waiting"] for d in status["dimensions"].values())
    dim_count = status["dimension_count"]
    inflight = sum(status.get("singleflights", {}).values())
    # 后半段对象数相对前半段允许少量波动，但不应持续大幅增长
    growth = objs_after - objs_mid
    ok = (
        active_sum == 0
        and waiting_sum == 0
        and dim_count <= len(BUSINESSES)
        and inflight == 0
        and growth < 2000
    )
    record(
        f"连续 {rounds} 轮突发后资源回收",
        ok,
        f"active={active_sum} waiting={waiting_sum} 维度数={dim_count} "
        f"singleflight在飞={inflight} GC对象 前半={objs_mid} 后半={objs_after} "
        f"增量={growth}（应接近 0）",
    )


async def run_load(base_url: str | None) -> bool:
    print("=" * 68)
    print("本地负载测试（in-process）" if base_url is None
          else f"本地负载测试 -> {base_url}")
    print("=" * 68)
    started = time.perf_counter()

    if base_url is None:
        # 进程内模式：使用压测专用的小阈值配置，便于稳定复现拒绝场景
        settings = Settings(
            businesses={
                "report": BusinessConfig(
                    business="report", max_concurrency=4, max_queue=4,
                    queue_timeout=0.4, rate_limit=40, rate_window=1.0,
                    processing_timeout=8.0, singleflight=False,
                ),
                "export": BusinessConfig(
                    business="export", max_concurrency=2, max_queue=2,
                    queue_timeout=0.3, rate_limit=10, rate_window=1.0,
                    processing_timeout=8.0, singleflight=False,
                ),
                "asset": BusinessConfig(
                    business="asset", max_concurrency=3, max_queue=4,
                    queue_timeout=1.0, rate_limit=60, rate_window=1.0,
                    processing_timeout=5.0, singleflight=True,
                ),
            },
            default=BusinessConfig(business="default"),
            # 全局层：叠在业务层之上的共享容量保护；阈值高于各业务合计，
            # 让业务层拒绝优先出现，同时防止单业务爆量吃光全局资源
            global_=GlobalConfig(
                max_concurrency=12, rate_limit=200, rate_window=1.0
            ),
        )
        app = create_app()
        app.state.settings = settings
        app.state.manager = BackpressureManager(settings)
        transport: httpx.AsyncHTTPTransport | None = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://load"
        ) as client:
            await _all_phases(client)
    else:
        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
            await _all_phases(client)

    elapsed = time.perf_counter() - started
    print("-" * 68)
    passed = sum(1 for _, ok, _ in PHASE_RESULTS if ok)
    total = len(PHASE_RESULTS)
    for phase, ok, detail in PHASE_RESULTS:
        print(f"  {'✅' if ok else '❌'} {phase}: {detail}")
    print("-" * 68)
    print(f"合计 {passed}/{total} 个阶段通过，用时 {elapsed:.2f}s")
    return passed == total


async def _all_phases(client: httpx.AsyncClient) -> None:
    gc.collect()
    await phase_steady(client)
    await phase_burst_backpressure(client)
    await phase_singleflight(client)
    await phase_cancel_recovery(client)
    await phase_repeated_burst_no_growth(client)


def main() -> None:
    parser = argparse.ArgumentParser(description="本地背压负载测试")
    parser.add_argument(
        "url", nargs="?", default=None,
        help="服务地址，如 http://127.0.0.1:8000；省略则进程内压测",
    )
    parser.add_argument(
        "--in-process", action="store_true",
        help="强制使用进程内 ASGI 压测",
    )
    args = parser.parse_args()
    url = None if args.in_process else args.url
    ok = asyncio.run(run_load(url))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
