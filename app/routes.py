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
    content: dict[str, object] = {
        "accepted": False,
        "error": {
            "reason": exc.reason.value,
            "business": exc.business,
            "resource": exc.resource,
            "detail": exc.detail,
        },
    }
    if exc.reason in (
        RejectReason.RATE_LIMITED,
        RejectReason.GLOBAL_RATE_LIMITED,
    ) and exc.retry_after is not None:
        retry = max(0.0, exc.retry_after)
        headers["Retry-After"] = f"{retry:.3f}"
        content["error"]["retry_after"] = round(retry, 3)  # type: ignore[index]
    return JSONResponse(status_code=status, content=content, headers=headers)


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
    """观测接口：全局层 + 各业务维度的额度/排队/窗口用量/拒绝计数。

    纯只读：不申请任何额度、不重置任何计数、不影响限流判定。
    """
    manager = _manager(request)
    registry = manager.registry
    settings = manager.settings

    snapshot: dict[str, object] = {}
    for name in registry.known_dimensions():
        gate = registry.peek_gate(name)
        limiter = registry.peek_rate_limiter(name)
        cfg = settings.get(name)
        rate = (
            limiter.stats(name)
            if limiter is not None
            else None
        )
        snapshot[name] = {
            "active": gate.active if gate is not None else 0,
            "waiting": gate.waiting if gate is not None else 0,
            "capacity": gate.capacity if gate is not None else cfg.max_concurrency,
            "max_queue": gate.max_queue if gate is not None else cfg.max_queue,
            "admitted": gate.total_admitted if gate is not None else 0,
            "queue_full": gate.total_queue_full if gate is not None else 0,
            "queue_timeout": (
                gate.total_queue_timeout if gate is not None else 0
            ),
            "rate": {
                "limit": rate.limit if rate is not None else cfg.rate_limit,
                "window": rate.window if rate is not None else cfg.rate_window,
                "used": rate.used if rate is not None else 0,
                "remaining": (
                    rate.remaining if rate is not None else cfg.rate_limit
                ),
                "rejected": rate.rejected if rate is not None else 0,
                "retry_after": (
                    round(rate.retry_after, 3) if rate is not None else 0.0
                ),
            },
        }

    gl = manager.global_limiter
    grate = gl.rate_stats()
    return {
        "global": {
            "active": gl.active,
            "capacity": gl.capacity,
            "admitted": gl.total_admitted,
            "concurrency_rejected": gl.total_concurrency_rejected,
            "rate": {
                "limit": grate.limit,
                "window": grate.window,
                "used": grate.used,
                "remaining": grate.remaining,
                "rejected": grate.rejected,
                "retry_after": round(grate.retry_after, 3),
            },
        },
        "dimensions": snapshot,
        "dimension_count": registry.active_dimensions(),
        "singleflights": {
            name: sf.in_flight() for name, sf in manager._singleflights.items()
        },
    }
