"""测试夹具：构造一套小配额的限流域配置，便于快速触发拒绝/超时。"""

from __future__ import annotations

import asyncio
import contextlib
import typing

import pytest_asyncio
from httpx import AsyncClient, Response
from httpx._transports.asgi import ASGITransport

from app.config import DimensionConfig
from app.main import create_app


class DisconnectTransport(ASGITransport):
    """与真实 uvicorn 一致：调用方放弃请求（取消）后，应用侧 receive()

    立即收到 ``http.disconnect``。标准 httpx ASGITransport 只在响应完成
    之后才产生 disconnect，无法模拟"处理中途客户端断开"。
    """

    def __init__(self, app, **kwargs) -> None:
        super().__init__(app, **kwargs)
        self.draining = 0

    async def handle_async_request(self, request):  # type: ignore[override]
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for k, v in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "server": (request.url.host, request.url.port),
            "client": ("127.0.0.1", 12345),
            "root_path": "",
        }

        body = b"".join([c async for c in request.stream])
        body_sent = False
        disconnected = asyncio.Event()
        drained = asyncio.Event()
        status_code: int | None = None
        response_headers: list[tuple[bytes, bytes]] = []
        body_parts: list[bytes] = []

        async def receive() -> dict[str, typing.Any]:
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            await disconnected.wait()
            return {"type": "http.disconnect"}

        async def send(message: typing.MutableMapping[str, typing.Any]) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                response_headers[:] = message.get("headers", [])
            elif message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))

        async def _run_app() -> None:
            self.draining += 1
            try:
                await self.app(scope, receive, send)
            finally:
                self.draining -= 1
                drained.set()

        app_task = asyncio.ensure_future(_run_app())

        async def _serve():
            await app_task

        serve_task = asyncio.ensure_future(_serve())
        try:
            await serve_task
        except asyncio.CancelledError:
            # 调用方放弃请求 -> 通知 disconnect；应用观察到后自行取消处理并
            # 回收资源。后台等待其完成，测试侧可通过 wait_drained() 确认。
            disconnected.set()

            async def _background_drain() -> None:
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(drained.wait(), timeout=2.0)

            asyncio.ensure_future(_background_drain())
            raise

        return Response(
            status_code,  # type: ignore[arg-type]
            headers=response_headers,
            content=b"".join(body_parts),
            request=request,
        )

    async def wait_drained(self) -> None:
        while self.draining:
            await asyncio.sleep(0.01)


def make_dimensions() -> dict[str, DimensionConfig]:
    return {
        # 合并维度：1 并发、1 排队位、0.2s 排队超时、高速率
        "report": DimensionConfig(
            name="report",
            max_concurrency=1,
            max_queue=1,
            queue_timeout=0.2,
            rate_per_second=1000.0,
            rate_burst=1000.0,
            coalesce=True,
        ),
        # 普通维度：2 并发、2 排队位、0.15s 排队超时
        "export": DimensionConfig(
            name="export",
            max_concurrency=2,
            max_queue=2,
            queue_timeout=0.15,
            rate_per_second=1000.0,
            rate_burst=1000.0,
            coalesce=False,
        ),
        # 严格速率维度（对应 /work/webhook）
        "webhook": DimensionConfig(
            name="webhook",
            max_concurrency=10,
            max_queue=0,
            queue_timeout=1.0,
            rate_per_second=2.0,
            rate_burst=2.0,
            coalesce=False,
        ),
        "compute": DimensionConfig(
            name="compute",
            max_concurrency=1,
            max_queue=1,
            queue_timeout=0.2,
            rate_per_second=1000.0,
            rate_burst=1000.0,
            coalesce=False,
        ),
    }


@pytest_asyncio.fixture
async def client(request):
    app = create_app(make_dimensions())
    # raise_app_exceptions=False：与真实 HTTP 客户端一致，服务端未捕获异常
    # 表现为 500 响应而不是在测试侧抛出。
    transport = DisconnectTransport(app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
