"""限流与背压相关的错误类型。"""

from __future__ import annotations


class BackpressureError(Exception):
    """所有背压/限流错误的基类。"""

    code: str = "backpressure_error"
    http_status: int = 503


class QueueFullError(BackpressureError):
    """并发队列已满，请求被立即拒绝（快速失败）。"""

    code = "queue_full"
    http_status = 503


class QueueTimeoutError(BackpressureError):
    """在队列中等待并发额度超时（排队超时）。"""

    code = "queue_timeout"
    http_status = 504


class RateLimitedError(BackpressureError):
    """超过单位时间请求量限制，被直接限流。"""

    code = "rate_limited"
    http_status = 429

    def __init__(self, retry_after: float = 0.0) -> None:
        super().__init__("rate limit exceeded")
        self.retry_after = retry_after
