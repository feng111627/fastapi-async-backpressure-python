"""三类耗时业务的模拟处理。

- report  报表生成：延迟可通过 ``delay`` 调整，稳定成功。
- export  数据导出：大文件导出，默认较慢，可用 ``fail=1`` 模拟失败。
- asset   资产查询：默认开启 single-flight，同资源并发请求合并。

业务层不感知限流/排队/合并，只负责真正的处理逻辑，
这样限流层可以对任意耗时处理统一收口。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

BUSINESSES = ("report", "export", "asset")


class BusinessError(Exception):
    """业务处理自身抛出的错误（区别于限流/背压错误）。"""


async def _process(
    business: str,
    resource: str,
    *,
    default_delay: float,
    delay: float | None,
    fail: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    wait = default_delay if delay is None else max(0.0, delay)
    await asyncio.sleep(wait)
    if fail:
        raise BusinessError(f"{business} processing failed for resource {resource}")
    return {
        "business": business,
        "resource": resource,
        "result_id": uuid.uuid4().hex[:8],
        "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        "served_by": "worker",
    }


async def generate_report(resource: str, *, delay: float | None = None) -> dict[str, Any]:
    """报表生成业务（默认约 0.3s）。"""
    return await _process(
        "report", resource, default_delay=0.3, delay=delay, fail=False
    )


async def export_data(
    resource: str,
    *,
    delay: float | None = None,
    fail: bool = False,
) -> dict[str, Any]:
    """数据导出业务（默认约 0.8s，可模拟失败）。"""
    return await _process(
        "export", resource, default_delay=0.8, delay=delay, fail=fail
    )


async def query_asset(resource: str, *, delay: float | None = None) -> dict[str, Any]:
    """资产查询业务（默认约 0.2s，供 single-flight 合并）。"""
    return await _process(
        "asset", resource, default_delay=0.2, delay=delay, fail=False
    )


# 业务名 -> 处理函数
HANDLERS = {
    "report": generate_report,
    "export": export_data,
    "asset": query_asset,
}
