"""限流/背压相关配置：数据结构、默认值与环境变量覆盖。

所有耗时业务共用一份按业务维度（business）区分的配置：

- ``max_concurrency``：同一业务维度允许同时执行的处理数量。
- ``max_queue``：并发额度占满后，允许排队等待的请求上限（不含正在执行的）。
  队列满时新请求立即被拒（QUEUE_FULL），不会无限堆积。
- ``queue_timeout``：排队等待额度的最长秒数；超时返回 QUEUE_TIMEOUT，
  与“直接被限流/队列满”是不同的拒绝原因。
- ``rate_limit`` / ``rate_window``：每 ``rate_window`` 秒最多放行的请求数
  （固定窗口），超限立即返回 RATE_LIMITED（不等队）。
- ``processing_timeout``：单次真实处理的最长秒数，超时抛错且结果不会被复用。
- ``singleflight``：是否对“同一业务 + 同一资源”的并发请求做合并处理。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class BusinessConfig:
    """单个业务维度的限流/背压配置。"""

    business: str
    max_concurrency: int = 8
    max_queue: int = 16
    queue_timeout: float = 5.0
    rate_limit: int = 1000
    rate_window: float = 1.0
    processing_timeout: float = 10.0
    singleflight: bool = False


# 未注册业务使用的兜底配置
DEFAULT_CONFIG = BusinessConfig(
    business="default",
    max_concurrency=8,
    max_queue=16,
    queue_timeout=5.0,
    rate_limit=1000,
    rate_window=1.0,
    processing_timeout=10.0,
    singleflight=False,
)

# 预置业务的默认配置
BUSINESS_DEFAULTS: dict[str, BusinessConfig] = {
    # 报表生成：中等并发、较短排队、每秒 20 个请求
    "report": BusinessConfig(
        business="report",
        max_concurrency=4,
        max_queue=6,
        queue_timeout=2.0,
        rate_limit=20,
        rate_window=1.0,
        processing_timeout=10.0,
        singleflight=False,
    ),
    # 数据导出：并发最低、队列最短、每秒 10 个请求
    "export": BusinessConfig(
        business="export",
        max_concurrency=2,
        max_queue=2,
        queue_timeout=1.0,
        rate_limit=10,
        rate_window=1.0,
        processing_timeout=15.0,
        singleflight=False,
    ),
    # 资产查询：开启同资源请求合并（single-flight）
    "asset": BusinessConfig(
        business="asset",
        max_concurrency=3,
        max_queue=4,
        queue_timeout=2.0,
        rate_limit=30,
        rate_window=1.0,
        processing_timeout=5.0,
        singleflight=True,
    ),
}

# 支持用 BP_{业务大写}_{字段} 覆盖默认值
_INT_FIELDS = ("max_concurrency", "max_queue", "rate_limit")
_FLOAT_FIELDS = ("queue_timeout", "rate_window", "processing_timeout")
_BOOL_FIELDS = ("singleflight",)


def _apply_env_overrides(cfg: BusinessConfig) -> BusinessConfig:
    """从环境变量读取 BP_{BUSINESS}_{FIELD} 形式的覆盖项。"""
    prefix = f"BP_{cfg.business.upper()}_"
    changes: dict[str, object] = {}
    for field in _INT_FIELDS:
        raw = os.getenv(prefix + field.upper())
        if raw is not None:
            changes[field] = int(raw)
    for field in _FLOAT_FIELDS:
        raw = os.getenv(prefix + field.upper())
        if raw is not None:
            changes[field] = float(raw)
    for field in _BOOL_FIELDS:
        raw = os.getenv(prefix + field.upper())
        if raw is not None:
            changes[field] = raw.strip().lower() in ("1", "true", "yes", "on")
    return replace(cfg, **changes) if changes else cfg


@dataclass(frozen=True)
class Settings:
    """全部业务维度的配置集合。"""

    businesses: dict[str, BusinessConfig]
    default: BusinessConfig

    def get(self, business: str) -> BusinessConfig:
        """返回业务配置，未注册则返回兜底配置的副本。"""
        cfg = self.businesses.get(business)
        if cfg is None:
            return replace(self.default, business=business)
        return cfg


def load_settings() -> Settings:
    """加载配置（默认值 + 环境变量覆盖）。"""
    businesses = {
        name: _apply_env_overrides(cfg)
        for name, cfg in BUSINESS_DEFAULTS.items()
    }
    return Settings(businesses=businesses, default=DEFAULT_CONFIG)
