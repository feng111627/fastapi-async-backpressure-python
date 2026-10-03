"""FastAPI 应用：演示并发限流、排队背压与重复请求合并。

业务端点（4 个限流域）：
- ``POST /work/report/{report_id}``  报表生成（开启同 id 请求合并）
- ``POST /work/export``              数据导出
- ``POST /work/webhook``             外部回调（慢下游）
- ``POST /work/compute``             批量计算（并发最低）

公共查询：
- ``GET  /health``   存活探针
- ``GET  /stats``    各维度当前 in-flight / waiting / 合并计数
- ``GET  /config``   当前生效配置
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import DimensionConfig, load_config
from .errors import (
    BackpressureError,
    QueueFullError,
    QueueTimeoutError,
    RateLimitedError,
)
from .manager import ThrottleManager

logger = logging.getLogger("backpressure")

# 各业务模拟耗时（秒），仅用于演示与压测。
_SIMULATED_WORK = {
    "report": 0.25,
    "export": 0.2,
    "webhook": 0.15,
    "compute": 0.35,
}


def _error_body(code: str, reason: str, detail: str) -> dict[str, object]:
    return {
        "error": code,
        "reason": reason,
        "detail": detail,
    }


def create_app(config: dict[str, DimensionConfig] | None = None) -> FastAPI:
    dimensions = config if config is not None else load_config()
    manager = ThrottleManager(dimensions)
    app = FastAPI(title="fastapi-async-backpressure", version="0.1.0")
    app.state.manager = manager
    app.state.config = dimensions

    # ---- 拒绝原因必须可区分：三种错误 -> 三种状态码/错误码 -----------------
    @app.exception_handler(QueueFullError)
    async def _queue_full(_request: Request, exc: QueueFullError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content=_error_body(
                exc.code,
                "concurrency queue is full; rejected immediately",
                str(exc),
            ),
        )

    @app.exception_handler(QueueTimeoutError)
    async def _queue_timeout(
        _request: Request, exc: QueueTimeoutError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content=_error_body(
                exc.code,
                "timed out while waiting for a concurrency slot",
                str(exc),
            ),
        )

    @app.exception_handler(RateLimitedError)
    async def _rate_limited(
        _request: Request, exc: RateLimitedError
    ) -> JSONResponse:
        headers = {"Retry-After": f"{max(1, round(exc.retry_after))}"}
        return JSONResponse(
            status_code=exc.http_status,
            content=_error_body(
                exc.code,
                "per-second request rate exceeded; rejected immediately",
                str(exc),
            ),
            headers=headers,
        )

    async def _unhandled(_request: Request, exc: Exception) -> JSONResponse:
        # 业务处理失败（含合并执行中 leader 的失败）统一成结构化 500，
        # 失败结果不被合并器缓存。
        logger.exception("work_failed: %s", exc)
        return JSONResponse(
            status_code=500,
            content=_error_body(
                "processing_failed",
                "the processing task raised an exception",
                str(exc),
            ),
        )

    # 显式注册到 ServerErrorMiddleware（装饰器方式注册 Exception 会挂在内层，
    # 500 在外层中间件被重新抛出）。
    app.add_exception_handler(Exception, _unhandled)

    async def _do_work(
        dimension: str,
        duration: float,
        resource_key: str | None,
        client: str,
        fail: bool = False,
    ) -> dict[str, object]:
        started = time.perf_counter()
        await asyncio.sleep(duration)  # 模拟耗时处理；可被取消中断
        if fail:
            raise RuntimeError("simulated processing failure")
        elapsed = time.perf_counter() - started
        logger.info(
            "work_done dimension=%s resource=%s client=%s elapsed=%.3fs",
            dimension, resource_key, client, elapsed,
        )
        return {
            "dimension": dimension,
            "resource": resource_key,
            "client": client,
            "elapsed": round(elapsed, 4),
        }

    def _register_work_route(dimension: str, path: str, with_resource: bool) -> None:
        async def _handler(request: Request) -> JSONResponse:
            body = await request.json() if (await request.body()) else {}
            duration = float(body.get("duration", _SIMULATED_WORK[dimension]))
            fail = bool(body.get("fail", False))
            client = str(body.get("client", request.client.host if request.client else "anon"))
            resource_key: str | None = None
            if with_resource:
                # 路径参数在 include_router 时统一处理，这里从 scope 取
                resource_key = request.path_params.get("report_id") or body.get(
                    "report_id"
                )
            started = time.perf_counter()

            # 把处理包成任务，与"调用方断开"事件竞速：
            # 客户端一旦断开，立即取消处理，触发队列/槽位/合并飞行的回收。
            # 注意：Request.is_disconnected() 只做一次非阻塞轮询，断连事件
            # 稍后到达时会漏检，因此这里直接阻塞在 ASGI receive 上等待
            # http.disconnect（请求体此前已被完整读取并缓存）。
            async def _watch_disconnect() -> bool:
                while True:
                    message = await request.receive()
                    if message.get("type") == "http.disconnect":
                        return True

            work_task = asyncio.ensure_future(
                manager.run(
                    dimension,
                    lambda: _do_work(
                        dimension, duration, resource_key, client, fail
                    ),
                    resource_key=resource_key,
                    rate_key=client,  # 速率限制按调用方维度
                )
            )
            disconnect_task = asyncio.ensure_future(_watch_disconnect())
            loop = asyncio.get_running_loop()
            verdict: asyncio.Future[str] = loop.create_future()

            def _on_disconnect(_t: "asyncio.Task[bool]") -> None:
                if not verdict.done() and not _t.cancelled() and _t.exception() is None:
                    if _t.result():
                        verdict.set_result("disconnect")

            def _on_work_done(_t: "asyncio.Task[dict[str, object]]") -> None:
                if not verdict.done():
                    verdict.set_result("work")

            disconnect_task.add_done_callback(_on_disconnect)
            work_task.add_done_callback(_on_work_done)

            try:
                outcome = await verdict
            except asyncio.CancelledError:
                # 处理协程本身被取消（外层取消）：按客户端断开处理。
                disconnect_task.cancel()
                work_task.cancel()
                with contextlib.suppress(BaseException):
                    await work_task
                raise

            if outcome == "disconnect":
                work_task.cancel()
                logger.info(
                    "client_disconnected dimension=%s resource=%s client=%s",
                    dimension, resource_key, client,
                )
                with contextlib.suppress(BaseException):
                    await work_task
                # 响应已无法送达，抛出取消以中断 ASGI 处理。
                raise asyncio.CancelledError("client disconnected")

            disconnect_task.cancel()
            with contextlib.suppress(BaseException):
                await disconnect_task
            if work_task.cancelled():
                raise asyncio.CancelledError
            work_exc = work_task.exception()
            if work_exc is not None:
                if isinstance(work_exc, BackpressureError):
                    logger.warning(
                        "rejected dimension=%s code=%s client=%s resource=%s",
                        dimension, work_exc.code, client, resource_key,
                    )
                    raise work_exc
                # 业务处理失败（含合并执行中 leader 的失败）：结构化 500。
                # 失败结果不会被合并器缓存，后续请求可重新真实执行。
                logger.exception("work_failed: %s", work_exc)
                return JSONResponse(
                    status_code=500,
                    content=_error_body(
                        "processing_failed",
                        "the processing task raised an exception",
                        str(work_exc),
                    ),
                )
            result = work_task.result()
            result["waited"] = round(time.perf_counter() - started - duration, 4)
            return JSONResponse(result)

        app.add_api_route(path, _handler, methods=["POST"])

    _register_work_route("report", "/work/report/{report_id}", True)
    _register_work_route("export", "/work/export", False)
    _register_work_route("webhook", "/work/webhook", False)
    _register_work_route("compute", "/work/compute", False)

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "fastapi-async-backpressure"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/stats")
    async def stats() -> dict[str, object]:
        return {"dimensions": manager.stats()}

    @app.get("/config")
    async def get_config() -> dict[str, object]:
        return {
            name: {
                "max_concurrency": c.max_concurrency,
                "max_queue": c.max_queue,
                "queue_timeout": c.queue_timeout,
                "rate_per_second": c.rate_per_second,
                "rate_burst": c.rate_burst,
                "coalesce": c.coalesce,
            }
            for name, c in dimensions.items()
        }

    return app


app = create_app()
