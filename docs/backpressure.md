# 并发限流与背压控制

本服务为耗时处理统一增加了一层并发限流与背压控制，保证突发高并发下服务不被
拖垮，且被拒绝或中途取消的请求不会遗留任何资源（并发额度、排队位置、合并
飞行、速率桶）。

## 1. 总体结构

控制层由三个可独立测试的组件和一个组合管理器构成：

```
请求进入
  │
  ├─ 1. 速率限制 TokenBucketRateLimiter（按调用方/维度的单位时间请求量）
  │       超限 -> 429 rate_limited（立即失败，带 Retry-After）
  │
  ├─ 2. 有界并发槽 BoundedSemaphore（同时处理数 + 排队队列）
  │       队列满 -> 503 queue_full（立即失败）
  │       排队超时 -> 504 queue_timeout
  │       调用方取消 -> 立即离开队列
  │
  └─ 3. 请求合并 Coalescer（同一资源并发请求只执行一次）
          失败/超时不缓存；无人等待时取消底层执行并释放槽位
```

代码位置：

| 文件 | 职责 |
| --- | --- |
| `app/errors.py` | 三类可区分的拒绝错误（队列满/排队超时/速率限制） |
| `app/rate_limiter.py` | 按 key 的令牌桶速率限制 + 空闲桶清扫 |
| `app/bounded_semaphore.py` | 有界并发槽：FIFO 有界队列、排队超时、取消安全 |
| `app/coalescer.py` | singleflight 请求合并 |
| `app/config.py` | 各业务维度的默认配置与环境变量覆盖 |
| `app/manager.py` | 把三层组合为统一入口 `ThrottleManager.run()` |
| `app/main.py` | FastAPI 端点、错误码映射、客户端断连监听 |

## 2. 三类拒绝原因（严格区分，不允许混淆）

| 场景 | HTTP 状态码 | 响应体 `error` | `reason` |
| --- | --- | --- | --- |
| 超过单位时间请求量，直接限流 | 429 | `rate_limited` | per-second request rate exceeded; rejected immediately |
| 并发队列已有明确上限且已满 | 503 | `queue_full` | concurrency queue is full; rejected immediately |
| 进了队列但等待并发槽超时 | 504 | `queue_timeout` | timed out while waiting for a concurrency slot |

429 响应还会带 `Retry-After`（秒，向上取整），调用方可据此退避。

## 3. 排队与背压

- **并发上限**：`max_concurrency`，同一维度同时进行的真实处理数量。
- **队列上限**：`max_queue`，超过并发上限的请求在 FIFO 队列中等待；队列长度
  达到上限后新请求**立即**得到 503，因此请求不可能在内存里无限堆积。
- **排队超时**：`queue_timeout`，队列中的请求等待超过该时间得到 504；超时
  请求立即离开队列，不占槽、不留位置。
- **取消安全**：请求在排队阶段被取消（含客户端断开）时立即出队；若取消信号
  恰好与"槽位授予"竞争，会把刚拿到的槽直接归还而不转授他人。
- **处理中断开**：端点直接监听 ASGI `http.disconnect`（
  `Request.is_disconnected()` 只做一次非阻塞轮询，会漏检迟到的断连），
  一旦客户端断开立即取消处理任务，并发槽与合并飞行随之回收。

## 4. 请求合并（singleflight）

仅对开启 `coalesce=true` 的维度（默认 `report`）生效，并需要调用方提供
`resource_key`（报表接口里即路径参数 `report_id`）：

- 同一时刻对同一 `resource_key` 的多个并发请求，只有首调（leader）真正执行
  排队 + 业务，其余请求等待并共享结果；
- **失败、超时、取消的结果一律不缓存**：flight 在共享任务结束（无论成败）
  后即被删除，后续请求会重新获得真实执行机会；
- 单个跟随者中途取消不影响其他等待者；
- 所有等待者都离开时，正在进行的共享任务会被取消，立即释放其占用的并发槽，
  避免无人消费的任务继续空跑、占用下游资源。

## 5. 业务维度、配置项与默认值

| 维度 | 端点 | max_concurrency | max_queue | queue_timeout(s) | rate_per_second | rate_burst | coalesce |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `report` | `POST /work/report/{report_id}` | 2 | 4 | 2.0 | 10 | 5 | ✅ |
| `export` | `POST /work/export` | 4 | 8 | 3.0 | 20 | 10 | ❌ |
| `webhook` | `POST /work/webhook` | 3 | 6 | 1.5 | 15 | 8 | ❌ |
| `compute` | `POST /work/compute` | 1 | 2 | 1.0 | 5 | 2 | ❌ |

字段含义：

- `max_concurrency`：同时进行的处理数量上限；
- `max_queue`：排队等待的上限，满了立即 503，杜绝无界堆积；
- `queue_timeout`：排队最长等待秒数，超时返回 504；
- `rate_per_second`：每秒补充的令牌数（平均允许的 QPS），按调用方
  （请求体 `client` 字段，缺省取连接地址）分别计数；
- `rate_burst`：令牌桶容量，允许的瞬时突发量；
- `coalesce`：是否对同一资源的并发请求合并执行。

所有字段均可通过环境变量覆盖，命名规则为 `BP_<维度大写>_<字段大写>`，例如：

```bash
BP_REPORT_MAX_CONCURRENCY=4
BP_REPORT_MAX_QUEUE=16
BP_REPORT_QUEUE_TIMEOUT=5
BP_REPORT_RATE_PER_SECOND=50
BP_REPORT_RATE_BURST=20
BP_REPORT_COALESCE=true
```

运行时可通过 `GET /config` 查看当前生效配置，通过 `GET /stats` 查看各维度的
`in_flight`、`waiting`、合并调用数/真实执行数。

## 6. 本地验证

### 6.1 启动服务

```bash
uv sync
uv run python main.py            # 或 uv run uvicorn app.main:app --reload
```

### 6.2 单元与集成测试（pytest）

```bash
uv run pytest -q
```

覆盖场景：

- 令牌桶：突发容量、补充、多 key 隔离、空闲桶清扫；
- 有界并发槽：并发上限、队列满快速失败、排队超时、FIFO 公平性、
  排队中取消、连续压力后额度不泄漏；
- 请求合并：并发只执行一次、失败不缓存、成功后不缓存、
  跟随者取消不影响他人、全部取消则中止底层执行、不同 key 独立；
- HTTP 集成：三类拒绝码互不混淆、按调用方限流、合并共享、
  **处理中取消后额度立即回收**、连续多轮突发后无槽位/队列残留。

### 6.3 本地负载测试

```bash
uv run python scripts/load_test.py
# 也可以打已经启动的服务：
uv run uvicorn app.main:app --port 8000 &
uv run python scripts/load_test.py --base-url http://127.0.0.1:8000
```

负载测试会自动用一套较小配额启动服务，并逐场景打印 `PASS/FAIL`：

1. `report`：8 个同资源并发请求只真实执行 1 次；
2. `compute`：6 个突发并发中出现 503 `queue_full` 与 504 `queue_timeout`，
   两者原因可区分；
3. `webhook`：同一调用方 30 连发，超额部分得到 429 `rate_limited` 且带
   `Retry-After`；
4. `report`：处理中取消请求后 `in_flight/waiting` 立即归零，后续请求可用；
5. `export`：8 轮、每轮 12 个突发后，每轮槽位与队列都归零，且进程 RSS
   无显著增长（无内存泄漏）。

### 6.4 手工验证（真实 TCP 断开）

```bash
# 处理中直接关闭连接，再查 stats，应看到对应维度 in_flight 归零
curl -X POST localhost:8000/work/compute -d '{"duration":10}' & sleep 0.2
# 关闭该 curl 进程后：
curl -s localhost:8000/stats
```
