"""本地负载测试：对运行中的服务（或现场启动的服务）打并发流量。

用法：
    uv run python scripts/load_test.py                # 自动启动 uvicorn
    uv run python scripts/load_test.py --base-url http://127.0.0.1:8000

覆盖的业务与场景（每项都会打印 PASS/FAIL）：
    1. report  同资源并发请求合并（只执行一次）
    2. compute 突发并发 -> 队列满(503 queue_full) 与排队超时(504 queue_timeout)
    3. webhook 固定调用方高速请求 -> 速率限制(429 rate_limited)
    4. report  处理中取消 -> 额度回收（stats 归零、后续请求可用）
    5. export  连续多轮突发 -> 无槽位泄漏/队列残留，且内存占用稳定
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import socket
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]

# 压测使用一套较小的固定配额，保证快速触发各类拒绝。
LOAD_ENV = {
    "BP_REPORT_MAX_CONCURRENCY": "1",
    "BP_REPORT_MAX_QUEUE": "2",
    "BP_REPORT_QUEUE_TIMEOUT": "0.3",
    "BP_REPORT_RATE_PER_SECOND": "1000",
    "BP_REPORT_RATE_BURST": "1000",
    "BP_REPORT_COALESCE": "true",
    "BP_COMPUTE_MAX_CONCURRENCY": "1",
    "BP_COMPUTE_MAX_QUEUE": "1",
    "BP_COMPUTE_QUEUE_TIMEOUT": "0.2",
    "BP_COMPUTE_RATE_PER_SECOND": "1000",
    "BP_COMPUTE_RATE_BURST": "1000",
    "BP_WEBHOOK_MAX_CONCURRENCY": "10",
    "BP_WEBHOOK_MAX_QUEUE": "0",
    "BP_WEBHOOK_QUEUE_TIMEOUT": "1",
    "BP_WEBHOOK_RATE_PER_SECOND": "20",
    "BP_WEBHOOK_RATE_BURST": "5",
    "BP_EXPORT_MAX_CONCURRENCY": "2",
    "BP_EXPORT_MAX_QUEUE": "4",
    "BP_EXPORT_QUEUE_TIMEOUT": "0.3",
    "BP_EXPORT_RATE_PER_SECOND": "1000",
    "BP_EXPORT_RATE_BURST": "1000",
}

GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
RESET = "\033[0m"


class Reporter:
    def __init__(self) -> None:
        self.results: list[tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        tag = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        line = f"[{tag}] {name}"
        if detail:
            line += f"  -- {detail}"
        print(line, flush=True)
        self.results.append((name, ok, detail))

    def section(self, title: str) -> None:
        print(f"\n{CYAN}=== {title} ==={RESET}", flush=True)

    def summary(self) -> int:
        passed = sum(1 for _, ok, _ in self.results if ok)
        failed = len(self.results) - passed
        print(f"\n{CYAN}----------------------------------------{RESET}")
        print(f"汇总: {GREEN}{passed} passed{RESET}, {RED}{failed} failed{RESET}")
        return 0 if failed == 0 else 1


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.asynccontextmanager
async def server_lifecycle(base_url: str | None):
    if base_url:
        yield base_url
        return
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    env = {**os.environ, **LOAD_ENV}
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            with contextlib.suppress(Exception):
                async with httpx.AsyncClient() as probe:
                    if (await probe.get(url + "/health")).status_code == 200:
                        break
            await asyncio.sleep(0.2)
        else:
            raise RuntimeError("server failed to start")
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def scenario_coalescing(base: str, rep: Reporter) -> None:
    rep.section("场景1 report 业务：同资源并发请求合并")
    async with httpx.AsyncClient(base_url=base, timeout=10) as c:
        n = 8
        t0 = time.perf_counter()
        res = await asyncio.gather(*[
            c.post("/work/report/load-r1", json={"duration": 0.4, "client": "lc"})
            for _ in range(n)
        ])
        elapsed = time.perf_counter() - t0
        ok_status = [r.status_code == 200 for r in res].count(True)
        stats = (await c.get("/stats")).json()["dimensions"]["report"]
        rep.check(
            "8 个并发请求全部成功且共享一次执行",
            ok_status == n
            and stats["coalesced_executions"] == 1
            and stats["coalesced_calls"] == n,
            f"成功 {ok_status}/{n}, 真实执行 {stats['coalesced_executions']}, "
            f"耗时 {elapsed:.2f}s（串行需 {0.4 * n:.1f}s）",
        )
        rep.check("合并结束后无残留占用", stats["in_flight"] == 0)


async def scenario_rejections(base: str, rep: Reporter) -> None:
    rep.section("场景2 compute 业务：突发并发 -> 队列满 / 排队超时")
    async with httpx.AsyncClient(base_url=base, timeout=10) as c:
        res = await asyncio.gather(*[
            c.post("/work/compute", json={"duration": 0.5, "client": f"u{i}"})
            for i in range(6)
        ])
        codes = Counter(r.status_code for r in res)
        errors = Counter(
            r.json().get("error") for r in res if r.status_code in (429, 503, 504)
        )
        rep.check("出现队列满快速失败 (503 queue_full)", errors.get("queue_full", 0) > 0,
                  f"状态码分布 {dict(codes)}, 拒绝分布 {dict(errors)}")
        rep.check("出现排队超时 (504 queue_timeout)", errors.get("queue_timeout", 0) > 0)
        rep.check("两类拒绝原因可区分且互不混淆",
                  errors.get("queue_full", 0) > 0 and errors.get("queue_timeout", 0) > 0)
        await asyncio.sleep(0.7)


async def scenario_rate_limit(base: str, rep: Reporter) -> None:
    rep.section("场景3 webhook 业务：单位时间请求量限制")
    async with httpx.AsyncClient(base_url=base, timeout=10) as c:
        res = await asyncio.gather(*[
            c.post("/work/webhook", json={"duration": 0.02, "client": "rate-user"})
            for _ in range(30)
        ])
        errors = Counter(
            r.json().get("error") for r in res if r.status_code in (429, 503, 504)
        )
        limited = [r for r in res if r.status_code == 429]
        rep.check("超额请求被 429 rate_limited 快速拒绝", len(limited) > 0,
                  f"拒绝分布 {dict(errors)}")
        if limited:
            ra = limited[0].headers.get("Retry-After")
            rep.check("限流响应携带 Retry-After", ra is not None, f"Retry-After={ra}")
            rep.check("限流与排队类拒绝原因不同",
                      all(r.json()["error"] == "rate_limited" for r in limited))


async def scenario_cancel_reclaim(base: str, rep: Reporter) -> None:
    rep.section("场景4 report 业务：处理中取消 -> 额度立即回收")
    async with httpx.AsyncClient(base_url=base, timeout=10) as c:
        t = asyncio.ensure_future(
            c.post("/work/report/cancel-r", json={"duration": 2.0, "client": "cx"})
        )
        await asyncio.sleep(0.3)
        before = (await c.get("/stats")).json()["dimensions"]["report"]
        t.cancel()
        with contextlib.suppress(BaseException):
            await t
        await asyncio.sleep(0.3)
        after = (await c.get("/stats")).json()["dimensions"]["report"]
        rep.check("取消前确有 1 个处理中任务", before["in_flight"] == 1, str(before))
        rep.check("取消后 in_flight 归零、无队列残留",
                  after["in_flight"] == 0 and after["waiting"] == 0, str(after))
        r = await c.post("/work/report/cancel-r",
                         json={"duration": 0.05, "client": "cx"})
        rep.check("额度回收后新请求可立即处理", r.status_code == 200, str(r.status_code))


async def scenario_sustained_pressure(base: str, rep: Reporter) -> None:
    rep.section("场景5 export 业务：连续突发压测 -> 额度不泄漏 / 内存稳定")
    import resource

    async with httpx.AsyncClient(base_url=base, timeout=10) as c:
        accepted = rejected = 0
        rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rounds = 8
        for rnd in range(rounds):
            res = await asyncio.gather(*[
                c.post("/work/export", json={"duration": 0.05, "client": f"p{rnd}-{i}"})
                for i in range(12)
            ])
            for r in res:
                if r.status_code == 200:
                    accepted += 1
                elif r.status_code in (429, 503, 504):
                    rejected += 1
            await asyncio.sleep(0.05)
            s = (await c.get("/stats")).json()["dimensions"]["export"]
            if s["in_flight"] != 0 or s["waiting"] != 0:
                rep.check(f"第 {rnd + 1} 轮后资源归零", False, str(s))
                break
        else:
            rep.check(f"{rounds} 轮突发后每轮 in_flight/waiting 均归零", True,
                      f"接受 {accepted}，拒绝 {rejected}")
        rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rep.check("压测进程 RSS 无显著增长",
                  rss_after <= rss_before + 20_000,
                  f"{rss_before} -> {rss_after} KiB")
        # 服务端内存/队列观测
        s = (await c.get("/stats")).json()["dimensions"]["export"]
        rep.check("服务端最终无残留", s["in_flight"] == 0 and s["waiting"] == 0, str(s))


async def amain(base_url: str | None) -> int:
    print(f"{CYAN}本地负载测试 (fastapi-async-backpressure){RESET}", flush=True)
    async with server_lifecycle(base_url) as base:
        print(f"目标服务: {base}", flush=True)
        rep = Reporter()
        await scenario_coalescing(base, rep)
        await scenario_rejections(base, rep)
        await scenario_rate_limit(base, rep)
        await scenario_cancel_reclaim(base, rep)
        await scenario_sustained_pressure(base, rep)
        return rep.summary()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=None,
                        help="已运行服务地址；不提供则自动本地启动 uvicorn")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(amain(args.base_url)))


if __name__ == "__main__":
    main()
