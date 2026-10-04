"""拒绝原因与异常类型。

可区分的反馈，HTTP 状态码也不同，客户端/压测可以精确统计：

- RATE_LIMITED   429：超过该业务自己的单位时间请求量，立即失败（没有排队）。
- QUEUE_FULL     503：该业务并发已满且排队队列也满，立即失败（防止无限堆积）。
- QUEUE_TIMEOUT  408：进入了队列但在 ``queue_timeout`` 内没拿到并发额度。
- PROCESSING_TIMEOUT 504：已经开始处理，但处理时长超过 ``processing_timeout``。
- GLOBAL_RATE_LIMITED 429：全服务共享的单位时间放行上限被打满，立即失败。
- GLOBAL_CONCURRENCY  503：全服务共享的同时处理数上限被打满，立即失败
  （全局层不排队，避免一个业务爆量时拖累其他业务）。
"""

from __future__ import annotations

import enum


class RejectReason(str, enum.Enum):
    """请求被拒/失败的可区分原因。"""

    RATE_LIMITED = "rate_limited"
    QUEUE_FULL = "queue_full"
    QUEUE_TIMEOUT = "queue_timeout"
    PROCESSING_TIMEOUT = "processing_timeout"
    GLOBAL_RATE_LIMITED = "global_rate_limited"
    GLOBAL_CONCURRENCY = "global_concurrency"


# 各原因对应的 HTTP 状态码
REJECT_STATUS: dict[RejectReason, int] = {
    RejectReason.RATE_LIMITED: 429,
    RejectReason.QUEUE_FULL: 503,
    RejectReason.QUEUE_TIMEOUT: 408,
    RejectReason.PROCESSING_TIMEOUT: 504,
    RejectReason.GLOBAL_RATE_LIMITED: 429,
    RejectReason.GLOBAL_CONCURRENCY: 503,
}


class ThrottledError(Exception):
    """限流/背压层抛出的异常，携带可区分的拒绝原因。"""

    def __init__(
        self,
        reason: RejectReason,
        *,
        business: str,
        resource: str | None = None,
        detail: str = "",
        retry_after: float | None = None,
    ) -> None:
        self.reason = reason
        self.business = business
        self.resource = resource
        self.detail = detail or reason.value
        self.retry_after = retry_after
        super().__init__(f"[{business}] {reason.value}: {self.detail}")
