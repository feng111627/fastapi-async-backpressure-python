"""限流与背压配置。

每个"限流域"（dimension）对应一类耗时业务，包含：
- ``max_concurrency`` 同时进行的处理数上限；
- ``max_queue``       排队队列上限（满则立即拒绝）；
- ``queue_timeout``   排队最长等待秒数（超时拒绝）；
- ``rate_per_second`` 单位时间（每秒）允许的请求量；
- ``rate_burst``      允许的瞬时突发量（令牌桶容量）；
- ``coalesce``        是否对同一 resource_key 的并发请求合并执行。

所有字段均可通过环境变量覆盖，变量名形如
``BP_<DOMAIN>_<FIELD>``，例如 ``BP_REPORT_MAX_CONCURRENCY=4``。
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DimensionConfig:
    """单个限流域（业务维度）的配置。"""

    name: str
    max_concurrency: int
    max_queue: int
    queue_timeout: float
    rate_per_second: float
    rate_burst: float
    coalesce: bool = False


_DEFAULT_DIMENSIONS: tuple[DimensionConfig, ...] = (
    # 报表生成：CPU/IO 都较重，严格限制并发与排队。
    DimensionConfig(
        name="report",
        max_concurrency=2,
        max_queue=4,
        queue_timeout=2.0,
        rate_per_second=10.0,
        rate_burst=5.0,
        coalesce=True,
    ),
    # 数据导出：中等开销，允许稍多并发。
    DimensionConfig(
        name="export",
        max_concurrency=4,
        max_queue=8,
        queue_timeout=3.0,
        rate_per_second=20.0,
        rate_burst=10.0,
        coalesce=False,
    ),
    # 外部回调：慢下游，小并发、短排队。
    DimensionConfig(
        name="webhook",
        max_concurrency=3,
        max_queue=6,
        queue_timeout=1.5,
        rate_per_second=15.0,
        rate_burst=8.0,
        coalesce=False,
    ),
    # 批量计算：最重的业务，并发最少。
    DimensionConfig(
        name="compute",
        max_concurrency=1,
        max_queue=2,
        queue_timeout=1.0,
        rate_per_second=5.0,
        rate_burst=2.0,
        coalesce=False,
    ),
)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_config() -> dict[str, DimensionConfig]:
    """读取默认配置并应用环境变量覆盖。"""
    result: dict[str, DimensionConfig] = {}
    for dim in _DEFAULT_DIMENSIONS:
        prefix = f"BP_{dim.name.upper()}_"
        result[dim.name] = DimensionConfig(
            name=dim.name,
            max_concurrency=_env_int(prefix + "MAX_CONCURRENCY", dim.max_concurrency),
            max_queue=_env_int(prefix + "MAX_QUEUE", dim.max_queue),
            queue_timeout=_env_float(prefix + "QUEUE_TIMEOUT", dim.queue_timeout),
            rate_per_second=_env_float(
                prefix + "RATE_PER_SECOND", dim.rate_per_second
            ),
            rate_burst=_env_float(prefix + "RATE_BURST", dim.rate_burst),
            coalesce=_env_bool(prefix + "COALESCE", dim.coalesce),
        )
    return result
