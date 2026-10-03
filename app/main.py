"""FastAPI 应用装配：日志、配置、背压管理器、路由。"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import load_settings
from .manager import BackpressureManager
from .routes import router
from .services import BusinessError


def _configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s | %(message)s",
    )
    logger = logging.getLogger("backpressure")
    logger.setLevel(logging.INFO)
    return logger


def create_app() -> FastAPI:
    """创建并装配应用。"""
    app = FastAPI(
        title="fastapi-async-backpressure-python",
        version="0.1.0",
        description="并发限流 + 有界排队背压 + single-flight 请求合并示例",
    )
    app.state.logger = _configure_logging()
    app.state.settings = load_settings()
    app.state.manager = BackpressureManager(app.state.settings)
    app.include_router(router)

    @app.exception_handler(BusinessError)
    async def _on_business_error(
        request: Request, exc: BusinessError
    ) -> JSONResponse:
        # 业务自身失败（如导出 fail=1）：与限流拒绝区分开，返回 502
        return JSONResponse(
            status_code=502,
            content={
                "accepted": True,
                "error": {
                    "reason": "business_failure",
                    "detail": str(exc),
                },
            },
        )

    return app


app = create_app()
