"""连续高压 + 取消之后的额度回收：多轮压测，active 必须始终归零。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.errors import RejectReason, ThrottledError
from app.gate import ConcurrencyGate

logger = logging.getLogger("test.recovery")


async def test_repeated_bursts_never_leak_slots() -> None:
    """连续 10 轮突发（含排队超时/队列满），额度和队列始终完全回收。"""
    gate = ConcurrencyGate("report", max_concurrency=4, max_queue=4, queue_timeout=0.05)

    async def job(hold: float) -> str:
        try:
            async with gate:
                await asyncio.sleep(hold)
            return "ok"
        except ThrottledError as exc:
            return exc.reason.value

    for round_no in range(1, 11):
        # 20 个请求抢 4+4 容量，处理时间故意拉长让部分排队超时
        out = await asyncio.gather(*(job(0.1) for _ in range(20)))
        assert gate.active == 0, f"round {round_no}: active leaked"
        assert gate.waiting == 0, f"round {round_no}: waiting leaked"
        logger.info(
            "第 %2d 轮突发: ok=%d queue_full=%d queue_timeout=%d "
            "结束后 active=%d waiting=%d",
            round_no,
            out.count("ok"),
            out.count(RejectReason.QUEUE_FULL.value),
            out.count(RejectReason.QUEUE_TIMEOUT.value),
            gate.active,
            gate.waiting,
        )

    assert gate.total_admitted > 0
    logger.info(
        "连续 10 轮压测结束: 累计放行=%d, 队列满=%d, 排队超时=%d, 无额度泄漏",
        gate.total_admitted,
        gate.total_queue_full,
        gate.total_queue_timeout,
    )


async def test_mixed_cancel_bursts_recover() -> None:
    """执行中取消与排队中取消混合多轮，最终额度完全回收。

    每轮：3 个运行 + 3 个排队占满 -> 取消其中 2 个排队者（排队名额释放、
    维持等待）-> 再取消全部 3 个运行者（排队者被依次唤醒后随运行任务
    结束而完成）。每轮结束 gate 必须完全归零。
    """
    gate = ConcurrencyGate("asset", max_concurrency=3, max_queue=3, queue_timeout=5)

    async def long_job() -> None:
        async with gate:
            await asyncio.sleep(5)

    for round_no in range(1, 6):
        running = [asyncio.ensure_future(long_job()) for _ in range(3)]
        waiting = [asyncio.ensure_future(gate.acquire()) for _ in range(3)]
        await asyncio.sleep(0.05)
        assert gate.active == 3 and gate.waiting == 3

        # 先取消 2 个排队中的：它们立刻放弃排队名额，3 个运行者不受影响
        for w in waiting[:2]:
            w.cancel()
        for w in waiting[:2]:
            with pytest.raises(asyncio.CancelledError):
                await w
        assert gate.active == 3 and gate.waiting == 1
        logger.info(
            "第 %d 轮: 取消 2 个排队者后 active=%d waiting=%d",
            round_no, gate.active, gate.waiting,
        )

        # 再取消 3 个运行者：剩余 1 个排队者会被 _pump 唤醒（acquire 成功），
        # 因此它不再响应取消，而是持有一个执行额度，需要正常配对 release。
        for t in running:
            t.cancel()
        for t in running:
            with pytest.raises(asyncio.CancelledError):
                await t
        await asyncio.sleep(0.05)
        assert gate.waiting == 0
        assert gate.active == (0 if waiting[2].done() and waiting[2].cancelled() else 1)
        if not waiting[2].cancelled():
            # 排队者被成功唤醒：取走结果并正常释放额度
            await waiting[2]
            gate.release()
        else:
            # 极端时序下它在唤醒前已取消：确保任务落定即可
            with pytest.raises(asyncio.CancelledError):
                await waiting[2]
        await asyncio.sleep(0.05)
        assert gate.active == 0 and gate.waiting == 0
        logger.info("第 %d 轮混合取消结束: active=0 waiting=0", round_no)
