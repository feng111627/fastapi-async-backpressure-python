# FastAPI 并发限流与背压示例

为耗时处理统一提供：按维度的并发数/请求量限制、有界排队与背压、
可区分的拒绝原因、取消即回收、同资源并发请求合并。

## 快速开始

```bash
uv sync
uv run python main.py                 # 启动服务
uv run pytest -q                      # 单元 + 集成测试（25 个用例）
uv run python scripts/load_test.py    # 本地负载测试（14 项检查）
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/work/report/{report_id}` | 报表生成（同 id 并发请求合并） |
| POST | `/work/export` | 数据导出 |
| POST | `/work/webhook` | 外部回调（慢下游） |
| POST | `/work/compute` | 批量计算（并发最低） |
| GET | `/stats` | 各维度 in-flight / waiting / 合并计数 |
| GET | `/config` | 当前生效的限流配置 |

请求体可选：`duration`（模拟处理秒数）、`client`（限流维度 key）、
`fail`（设为 true 模拟处理失败）。

## 拒绝反馈

- `429 rate_limited`：单位时间请求量超限（带 `Retry-After`）
- `503 queue_full`：并发队列已满，立即拒绝
- `504 queue_timeout`：排队等待槽位超时

详细设计、配置默认值与本地验证方法见 [docs/backpressure.md](docs/backpressure.md)。
