"""HTTP 路由：统一经过 BackpressureManager 处理。

端点
----
- GET  /                         健康检查
- POST /process/{business}       通用处理入口（report/export/asset）
- GET  /status                   各维度 active/waiting/拒绝计数等观测信息

限流/背压异常映射为不同 HTTP 状态码与 ``reason`` 字段，
排队超时与直接限流可以被客户端明确区分。
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import REJECT_STATUS, RejectReason, ThrottledError
from .manager import BackpressureManager
from .services import HANDLERS

router = APIRouter()


async def _await_with_disconnect(
    request: Request, coro: object
) -> object:
    """执行业务协程；调用方中途断开时立刻取消它。

    Starlette/uvicorn 不会仅因 socket 关闭就自动取消端点任务，
    必须主动等待 ASGI 的 ``http.disconnect`` 消息。body 已在进入
    端点前被 FastAPI 读完，因此这里再 ``receive()`` 时，下一条消息
    要么是断开、要么永远不来；用它与处理任务做竞速即可。
    处理任务被取消后，其在 gate 中的 ``finally`` 会立即归还
    并发/排队额度。
    """
    task = asyncio.ensure_future(coro)  # type: ignore[arg-type]
    disconnect = asyncio.ensure_future(request.receive())
    try:
        await asyncio.wait(
            {task, disconnect}, return_when=asyncio.FIRST_COMPLETED
        )
        if task.done():
            return task.result()
        if (
            disconnect.done()
            and not disconnect.cancelled()
            and disconnect.result().get("type") == "http.disconnect"
        ):
            raise asyncio.CancelledError("client disconnected")
        # disconnect 未给出断开消息：继续等处理任务完成
        return await task
    except BaseException:
        # 无论是收到 http.disconnect，还是端点任务自身被服务器/测试客户端
        # 取消，都必须取消内部业务任务——它的 finally 负责释放并发额度。
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        raise
    finally:
        if not disconnect.done():
            disconnect.cancel()



def _manager(request: Request) -> BackpressureManager:
    return request.app.state.manager  # type: ignore[no-any-return]


class ProcessRequest(BaseModel):
    """处理请求参数。"""

    resource: str = Field(default="default", min_length=1, max_length=128)
    delay: float | None = Field(default=None, ge=0, le=60)
    fail: bool = False


class ProcessResponse(BaseModel):
    accepted: bool
    business: str
    resource: str
    coalesced: bool = False
    result: dict | None = None


def _error_response(exc: ThrottledError) -> JSONResponse:
    status = REJECT_STATUS[exc.reason]
    headers: dict[str, str] = {}
    if exc.reason is RejectReason.RATE_LIMITED and exc.retry_after is not None:
        headers["Retry-After"] = f"{max(0.0, exc.retry_after):.3f}"
    return JSONResponse(
        status_code=status,
        content={
            "accepted": False,
            "error": {
                "reason": exc.reason.value,
                "business": exc.business,
                "resource": exc.resource,
                "detail": exc.detail,
            },
        },
        headers=headers,
    )


@router.get("/")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/process/{business}")
async def process_business(
    business: str,
    payload: ProcessRequest,
    request: Request,
) -> ProcessResponse:
    handler = HANDLERS.get(business)
    if handler is None:
        return JSONResponse(  # type: ignore[return-value]
            status_code=404,
            content={
                "accepted": False,
                "error": {
                    "reason": "unknown_business",
                    "business": business,
                    "detail": "supported businesses: " + ", ".join(sorted(HANDLERS)),
                },
            },
        )

    kwargs: dict[str, object] = {}
    if payload.delay is not None:
        kwargs["delay"] = payload.delay
    if business == "export":
        kwargs["fail"] = payload.fail

    manager = _manager(request)
    try:
        outcome = await _await_with_disconnect(
            request,
            manager.dispatch(business, payload.resource, handler, **kwargs),
        )
    except ThrottledError as exc:
        # 在路由层集中记录一次，方便压测日志核对拒绝分布
        request.app.state.logger.info(
            "rejected business=%s resource=%s reason=%s detail=%s",
            exc.business,
            exc.resource,
            exc.reason.value,
            exc.detail,
        )
        return _error_response(exc)  # type: ignore[return-value]

    return ProcessResponse(
        accepted=True,
        business=business,
        resource=payload.resource,
        coalesced=outcome.coalesced,
        result=outcome.payload,
    )


@router.get("/status")
async def status(request: Request) -> dict[str, object]:
    """观测接口：每个业务维度的额度/排队/拒绝计数。"""
    manager = _manager(request)
    registry = manager.registry
    snapshot: dict[str, object] = {}
    for name, entry in registry._entries.items():  # noqa: SLF001 - 观测读取
        gate = entry.gate
        snapshot[name] = {
            "active": gate.active,
            "waiting": gate.waiting,
            "capacity": gate.capacity,
            "max_queue": gate.max_queue,
            "admitted": gate.total_admitted,
            "queue_full": gate.total_queue_full,
            "queue_timeout": gate.total_queue_timeout,
            "rate_used": entry.rate_limiter.used(name),
        }
    return {
        "dimensions": snapshot,
        "dimension_count": registry.active_dimensions(),
        "singleflights": {
            name: sf.in_flight() for name, sf in manager._singleflights.items()
        },
    }
