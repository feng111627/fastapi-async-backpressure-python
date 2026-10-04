# FastAPI 并发限流与背压控制

在 FastAPI 服务前置一层统一的**全局容量保护 + 并发限流 + 有界排队背压 + 同资源请求合并**，
把耗时较久的处理（report / export / asset）统一管起来，使服务在突发高并发下
不被拖垮；被拒绝或中途取消的请求不会留下“额度只减不增 / 内存持续增长”等后遗症。

## 1. 能力概览

| 能力 | 说明 |
| --- | --- |
| 全局容量保护 | 全服务共享的**同时处理数上限**与**单位时间放行上限**，叠在各业务限制之上；触顶**立即拒绝不排队**，防止单个业务爆量吃光全服务资源。 |
| 单位时间请求量限制 | 按业务维度固定窗口计数，超限**快速失败**（不等队）；按真实经过的时间生效，慢速串行流量同样受限。 |
| 同时处理数量限制 | 按业务维度限制 active 处理数（信号量语义，但用显式队列实现）。 |
| 有界排队 / 背压 | 并发占满后进入**有上限**的 FIFO 队列；队列满立即拒绝，不会无限堆积。 |
| 排队超时 | 排队超过 `queue_timeout` 仍未拿到额度则失败，原因与“直接限流/队列满”区分。 |
| 取消即释放 | 排队超时、客户端断开、处理中被取消，都会立即释放全局/业务两层额度与排队名额。 |
| 同资源请求合并 | 同一时刻对同一资源的并发请求只让 1 个真正执行，其余等待并共享**成功**结果。 |
| 失败/超时不复用 | 合并执行的失败或超时结果不缓存，后续请求一定能重新获得真实执行机会。 |
| 空闲维度回收 | 完全空闲的业务维度（闸门）会被回收，长期运行/大量不同维度后注册表不膨胀；**限流窗口状态不随闸门回收**，只在窗口自然过期后清理。 |

## 2. 拒绝原因（可区分，HTTP 状态码不同）

响应体统一形如：

```json
{ "accepted": false,
  "error": { "reason": "queue_full", "business": "export",
             "resource": "bulk-1", "detail": "all 2 exec slots ..." } }
```

| reason | HTTP | 含义 |
| --- | --- | --- |
| `global_rate_limited` | **429** | 各业务合计打满全局单位时间放行上限，立即失败（带 `Retry-After` 响应头与 `retry_after` 字段）。 |
| `global_concurrency` | **503** | 各业务合计打满全局同时处理数上限，立即失败（全局层不排队）。 |
| `rate_limited` | **429** | 超过该业务自己的单位时间请求量，立即失败（带 `Retry-After` 响应头与 `retry_after` 字段）。 |
| `queue_full` | **503** | 该业务并发已满且排队队列也满，立即失败（背压）。 |
| `queue_timeout` | **408** | 进入了队列，但在 `queue_timeout` 内没排到执行额度。 |
| `processing_timeout` | **504** | 已经开始处理，但处理超过 `processing_timeout`。 |
| `business_failure` | **502** | 业务自身失败（例如导出 `fail=true`），与限流拒绝区分。 |

因此“撞到全局限制”和“撞到某个业务自己的限制”是两种不同反馈，
客户端可按 `reason` 精确区分；全局层与业务层同时生效、互不绕过。

## 3. 请求处理流程

```
                 ┌──────────────────────────────┐
  请求 ───────▶  │ 0. 全局容量保护（所有业务共享）│ ── 超量 ──▶ 429 global_rate_limited
                 │    全局限流 + 全局并发上限     │ ── 满 ────▶ 503 global_concurrency
                 │    （不排队，立即拒绝）        │
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 1. 业务固定窗口请求量检查      │ ── 超量 ──▶ 429 rate_limited（不占任何资源）
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 2.（asset）single-flight 登记  │
                 │   等待者 ─▶ 共享 leader 结果   │
                 └──────────────┬───────────────┘
                                ▼ 仅 leader / 非合并业务
                 ┌──────────────────────────────┐
                 │ 3. 并发闸门：有界 FIFO 队列    │ ── 满 ──▶ 503 queue_full
                 │    排队超时 ─▶ 408            │ ── 超时 ▶ 408 queue_timeout
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 4. processing_timeout 内执行业务│ ── 超时 ▶ 504
                 │    监听 http.disconnect，断开  │ ── 断开 ▶ 取消并释放两层额度
                 │    即取消处理任务              │
                 └──────────────┬───────────────┘
                                ▼
                        finally 归还全局 + 业务并发额度（任何异常/取消路径都归还）
```

全局层与业务层的组合规则：**先全局、后业务，两层都要通过才放行**；
任一层拒绝都立即失败，且已通过的层级额度立刻归还。single-flight 的
等待者不计真实执行，因此只占两层限流配额、不占两层并发名额。

## 4. 配置项与默认值

### 4.1 全局容量保护（`GlobalConfig`，见 `app/config.py`）

| 字段 | 含义 | 默认值 |
| --- | --- | --- |
| `max_concurrency` | 全服务同时处理数上限（各业务合计，不排队） | 10 |
| `rate_limit` | 全服务每个窗口最多放行的请求数（各业务合计） | 50 |
| `rate_window` | 全局限流计数窗口秒数（固定窗口） | 1.0 |

环境变量覆盖：`BP_GLOBAL_MAX_CONCURRENCY` / `BP_GLOBAL_RATE_LIMIT` /
`BP_GLOBAL_RATE_WINDOW`。

### 4.2 各业务维度（`BusinessConfig`）

每个业务维度一个 `BusinessConfig`（见 `app/config.py`）：

| 字段 | 含义 | 默认（兜底） | report | export | asset |
| --- | --- | --- | --- | --- | --- |
| `max_concurrency` | 同一维度同时执行的处理数 | 8 | 4 | 2 | 3 |
| `max_queue` | 并发占满后允许排队的请求数（队列上限） | 16 | 6 | 2 | 4 |
| `queue_timeout` | 排队最长等待秒数 | 5.0 | 2.0 | 1.0 | 2.0 |
| `rate_limit` | 每个窗口最多放行的请求数 | 1000 | 20 | 10 | 30 |
| `rate_window` | 计数窗口秒数（固定窗口） | 1.0 | 1.0 | 1.0 | 1.0 |
| `processing_timeout` | 单次真实处理最长秒数 | 10.0 | 10.0 | 15.0 | 5.0 |
| `singleflight` | 是否合并同资源并发请求 | false | false | false | **true** |

系统内单维度在途请求硬上限 = `max_concurrency + max_queue`
（启用 single-flight 的维度，等待 leader 的合并请求不占 gate 名额，因此可更多，
但同一资源始终只有 1 个真实执行）。

### 环境变量覆盖

格式：`BP_{业务大写}_{字段}`，例如：

```bash
export BP_EXPORT_MAX_CONCURRENCY=4
export BP_EXPORT_MAX_QUEUE=8
export BP_EXPORT_QUEUE_TIMEOUT=2.5
export BP_ASSET_RATE_LIMIT=50
export BP_REPORT_SINGLEFLIGHT=true
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

## 5. HTTP 接口

- `GET /` 健康检查。
- `POST /process/{business}`：`business` ∈ `report | export | asset`。

  ```jsonc
  // 请求体（均可选）
  { "resource": "fund-A",  // 资源标识，asset 维度按它做合并
    "delay": 0.5,          // 覆盖模拟处理耗时（秒），便于压测/演示
    "fail": false }        // 仅 export 支持，true 模拟业务失败(502)
  ```

  成功：`200 {"accepted": true, "business": ..., "resource": ...,
  "coalesced": false, "result": {...}}`，`coalesced=true` 表示该请求复用了
  同资源 leader 的结果（未真正执行）。

- `GET /status`：观测接口（**纯只读，不占额度、不影响限流判定**）：
  - `global`：全局层 `active / capacity / admitted / concurrency_rejected`，
    以及全局限流窗口 `rate.{limit, window, used, remaining, rejected, retry_after}`；
  - `dimensions.{业务}`：各业务 `active / waiting / capacity / max_queue /
    admitted / queue_full / queue_timeout`，以及业务限流窗口
    `rate.{limit, window, used, remaining, rejected, retry_after}`；
  - `rejected` 是**本窗口内**被拒数，窗口自然滚动后随计数一起重置；
  - `singleflights`：各 single-flight 在飞 key 数。

## 6. 为什么不会“额度只减不增 / 内存持续增长”

- **不用裸 `Semaphore.acquire() + wait_for` 排队**。二者组合在“超时/取消”与
  “许可恰好放行”同时发生时可能把许可泄漏给一个已经失败的请求。闸门改用
  显式 future 队列，**只在等待者尚未结束时才授予执行额度（active+1）**，
  从根上杜绝许可泄漏；释放时同步唤醒队首。
- 排队等待者由自身的 `wait_for` 驱动超时，到期即从队列摘除；客户端断开
  抛入的 `CancelledError` 走同一条摘除路径，排队名额立即回收。
- 路由层用 `request.receive()` 与处理任务竞速来监听 `http.disconnect`
  （body 已读完，下一条 ASGI 消息即断开）：客户端一旦断开就取消处理任务，
  处理任务在 gate 的 `finally` 中归还额度。任何 `BaseException` 退出路径都会
  兜底取消内部任务。
- single-flight 用**原子** `join_or_create`（首个 await 之前完成登记），
  不会出现两个 leader；leader 成功才 `succeed` 分发，失败/超时/取消即
  `fail` 广播并删除条目，结果绝不复用。等待者用 `asyncio.shield` 隔离，
  自己超时/取消不影响内部 future、leader 与其他等待者。
- 注册表在每次申请前回收 `active=0 且 waiting=0` 的空闲维度（仅回收并发
  闸门）；**限流窗口状态独立保存**，只有窗口按真实时间自然过期后才清理——
  因此慢速串行流量（请求之间闸门已空闲）不会把窗口内已用额度悄悄清零，
  窗口内第 N+1 个请求无论密集还是稀疏都会被拒，窗口滚动后额度正常恢复。

## 7. 本地验证

### 7.1 安装依赖

```bash
uv sync --group dev
```

### 7.2 单元 / 集成测试（25 个）

```bash
uv run pytest                 # 带实时日志，可看到各场景覆盖与通过情况
```

覆盖场景：

- `tests/test_rate_limit.py`：超量快速失败、窗口重置、100 并发争用计数准确；
- `tests/test_gate.py`：并发上限、队列满、排队超时、**排队中取消释放名额**、
  **执行中取消回收并发额度**、零队列快速失败；
- `tests/test_singleflight.py`：同资源合并为 1 次执行、失败不缓存、
  leader 超时后可重新执行、等待者超时/取消不影响 leader 与其他等待者；
- `tests/test_api.py`：429/503/408 原因区分、客户端断开后额度回收、
  断开回收后再次压满全部成功、single-flight 合并；
- `tests/test_global_limit.py`：**全局并发上限拒绝并与业务 queue_full 区分**、
  **全局限流拒绝（global_rate_limited）**、**慢速串行流量下窗口内第 N+1 个
  被拒且窗口滚动后恢复**、**断开后全局额度回收**、状态查询不占额度；
- `tests/test_backpressure.py`：**连续 10 轮突发**后 active/waiting 恒为 0、
  连续 5 轮“运行中取消 + 排队中取消”混合后额度完全回收。

### 7.3 本地负载测试

进程内（无需起服务，自动使用便于复现拒绝的小阈值配置）：

```bash
uv run python scripts/load_test.py --in-process
```

或对真实 uvicorn 服务：

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
uv run python scripts/load_test.py http://127.0.0.1:8000
```

分 5 个阶段并打印每阶段 PASS/FAIL 与统计：

1. 稳态吞吐（三类业务各 3 请求，全成功）；
2. 突发背压（export 瞬时 60 并发，观察 200/429/503/408 分布，原因可区分）；
3. 同资源合并（asset 20 并发同 resource → leader=1、coalesced=19）；
4. 客户端断开取消（4 个长处理发起后断开，轮询确认 active/waiting 归零）；
5. 连续 8 轮突发后：active/waiting=0、维度数不增长、single-flight 在飞=0、
   GC 对象数后半段相对前半段增量≈0。

### 7.4 分别验证全局上限与慢速串行限流

**全局上限**（把全局阈值调小，观察全局拒绝与业务拒绝的区分）：

```bash
BP_GLOBAL_MAX_CONCURRENCY=2 BP_GLOBAL_RATE_LIMIT=5 \
  uv run uvicorn main:app --host 127.0.0.1 --port 8000

# 终端 2：三个业务同时打慢请求，全局同时处理数先被打满
for b in report export asset report export; do
  curl -s -X POST "http://127.0.0.1:8000/process/$b" \
    -H 'content-type: application/json' -d '{"delay": 2.0}' &
done; wait
# 期望：前 2 个 200，其余 503 global_concurrency（业务自身额度均未触达）
# 全局窗口 5 个用完后，再发任意请求得到 429 global_rate_limited
curl -s -X POST http://127.0.0.1:8000/process/report \
  -H 'content-type: application/json' -d '{"delay": 0.1}'
curl -s http://127.0.0.1:8000/status   # 查看 global.active / global.rate.*
```

**慢速串行限流**（把某业务窗口调小，一个一个慢慢发）：

```bash
BP_REPORT_RATE_LIMIT=3 BP_REPORT_RATE_WINDOW=10 \
  uv run uvicorn main:app --host 127.0.0.1 --port 8000

# 终端 2：每隔 1 秒发 1 个（远慢于突发），10s 窗口内第 4 个必须被 429 拒绝
for i in 1 2 3 4; do
  curl -s -X POST http://127.0.0.1:8000/process/report \
    -H 'content-type: application/json' -d '{"delay": 0.05}'; echo; sleep 1
done
# 期望：前 3 个 200，第 4 个 429 rate_limited 且带 Retry-After；
# 等窗口自然滚动（约 10s）后再发即恢复 200；
# 期间可随时查 /status 看 dimensions.report.rate.{used,remaining,rejected}
```

## 8. 代码结构

```
app/
  config.py       业务/全局配置、默认值、BP_* 环境变量覆盖
  errors.py       RejectReason 枚举（含全局层两种原因）、HTTP 状态映射、ThrottledError
  rate_limit.py   固定窗口请求量限流器（窗口状态只随真实时间滚动）
  gate.py         并发闸门：显式有界 FIFO 队列 + 排队超时 + 取消安全
  global_limit.py 全服务共享容量保护：全局并发上限（不排队）+ 全局限流
  registry.py     按维度持有闸门（空闲回收）/限流器（窗口期内保留），gate_slot 生命周期
  singleflight.py 原子 join_or_create 的同资源请求合并
  services.py     report/export/asset 三类模拟耗时业务
  manager.py      全局层→业务限流→排队→合并→超时 的统一编排 BackpressureManager
  routes.py       HTTP 路由 + http.disconnect 监听 + /status 观测
  main.py         应用装配（日志 / 配置 / 管理器 / 异常处理）
scripts/
  load_test.py    本地负载测试
tests/            并发争用、拒绝、排队超时、取消、全局层、慢速串行、额度回收测试
```
