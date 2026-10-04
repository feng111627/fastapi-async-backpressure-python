# FastAPI 并发限流与背压控制

在 FastAPI 服务前置一层统一的**并发限流 + 有界排队背压 + 同资源请求合并**，
把耗时较久的处理（report / export / asset）统一管起来，使服务在突发高并发下
不被拖垮；被拒绝或中途取消的请求不会留下“额度只减不增 / 内存持续增长”等后遗症。

## 1. 能力概览

| 能力 | 说明 |
| --- | --- |
| 全局容量保护 | 全服务共享的在途数上限 + 单位时间放行上限，叠在业务限制之上，触顶**立即拒绝不排队**。 |
| 单位时间请求量限制 | 按业务维度固定窗口计数，超限**快速失败**（不等队）；按真实经过的时间生效，慢速串行同样计数。 |
| 同时处理数量限制 | 按业务维度限制 active 处理数（信号量语义，但用显式队列实现）。 |
| 有界排队 / 背压 | 并发占满后进入**有上限**的 FIFO 队列；队列满立即拒绝，不会无限堆积。 |
| 排队超时 | 排队超过 `queue_timeout` 仍未拿到额度则失败，原因与“直接限流/队列满”区分。 |
| 取消即释放 | 排队超时、客户端断开、处理中被取消，都会立即释放排队名额、并发额度与全局在途名额。 |
| 同资源请求合并 | 同一时刻对同一资源的并发请求只让 1 个真正执行，其余等待并共享**成功**结果。 |
| 失败/超时不复用 | 合并执行的失败或超时结果不缓存，后续请求一定能重新获得真实执行机会。 |
| 空闲维度回收 | 完全空闲（无执行/排队且限流窗口已过期）的业务维度会被回收，注册表不膨胀。 |

## 2. 拒绝原因（可区分，HTTP 状态码不同）

响应体统一形如：

```json
{ "accepted": false,
  "error": { "reason": "queue_full", "business": "export",
             "resource": "bulk-1", "detail": "all 2 exec slots ..." } }
```

| reason | HTTP | 含义 |
| --- | --- | --- |
| `global_rate_limited` | **429** | 所有业务合计超过全局单位时间放行上限（带 `Retry-After` 响应头）。 |
| `global_concurrency` | **503** | 所有业务合计在途处理数达到全局上限，立即失败（不排队）。 |
| `rate_limited` | **429** | 超过该业务单位时间请求量，立即失败（带 `Retry-After` 响应头）。 |
| `queue_full` | **503** | 该业务并发已满且排队队列也满，立即失败（背压）。 |
| `queue_timeout` | **408** | 进入了队列，但在 `queue_timeout` 内没排到执行额度。 |
| `processing_timeout` | **504** | 已经开始处理，但处理超过 `processing_timeout`。 |
| `business_failure` | **502** | 业务自身失败（例如导出 `fail=true`），与限流拒绝区分。 |

全局层与业务层的拒绝通过 `reason` 前缀（`global_*`）明确区分：
客户端能分辨这次是撞到全服务共享上限，还是撞到某个业务自己的额度。

因此“排队超时”和“直接被限流”是两种不同反馈，客户端可据此分别处理。

## 3. 请求处理流程

```
                 ┌──────────────────────────────┐
  请求 ───────▶  │ 0. 全局层：单位时间放行上限     │ ── 超量 ──▶ 429 global_rate_limited
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 1. 业务层：固定窗口请求量检查   │ ── 超量 ──▶ 429 rate_limited（不占任何资源）
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 2. 全局层：在途数名额（不排队） │ ── 满 ──▶ 503 global_concurrency
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 3.（asset）single-flight 登记  │
                 │   等待者 ─▶ 共享 leader 结果   │
                 └──────────────┬───────────────┘
                                ▼ 仅 leader / 非合并业务
                 ┌──────────────────────────────┐
                 │ 4. 并发闸门：有界 FIFO 队列    │ ── 满 ──▶ 503 queue_full
                 │    排队超时 ─▶ 408            │ ── 超时 ▶ 408 queue_timeout
                 └──────────────┬───────────────┘
                                ▼
                 ┌──────────────────────────────┐
                 │ 5. processing_timeout 内执行业务│ ── 超时 ▶ 504
                 │    监听 http.disconnect，断开  │ ── 断开 ▶ 取消并释放额度
                 │    即取消处理任务              │
                 └──────────────┬───────────────┘
                                ▼
                finally 归还全局在途名额 + 并发额度（任何异常/取消路径都归还）
```

两层限制**同时生效、互不绕过**：全局层统计所有业务的合计值，
业务层统计各自维度；任何一层先触顶就按该层的原因拒绝。
single-flight 的等待者只计两层放行量，不占全局在途名额与 gate 额度
（真实执行只有 leader 一个）。

## 4. 配置项与默认值

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

### 全局容量保护（`GlobalConfig`）

叠在所有业务限制之上的一层全服务共享上限，三个业务**合计**计算，
触顶立即拒绝、不排队（见 `app/global_limit.py`）：

| 字段 | 含义 | 默认 |
| --- | --- | --- |
| `max_concurrency` | 所有业务合计的在途（排队+执行）处理数上限 | 12 |
| `rate_limit` | 所有业务合计每个窗口最多放行的请求数 | 50 |
| `rate_window` | 全局计数窗口秒数（固定窗口） | 1.0 |

组合生效规则：一个请求要先过全局放行量、再过业务放行量、再占全局在途
名额，最后才进入业务自己的并发闸门排队；任何一层触顶都按该层的原因
立即拒绝。取消/断开时全局在途名额与业务额度一起立即归还。

### 环境变量覆盖

格式：`BP_{业务大写}_{字段}`，例如：

```bash
export BP_EXPORT_MAX_CONCURRENCY=4
export BP_EXPORT_MAX_QUEUE=8
export BP_EXPORT_QUEUE_TIMEOUT=2.5
export BP_ASSET_RATE_LIMIT=50
export BP_REPORT_SINGLEFLIGHT=true
# 全局层
export BP_GLOBAL_MAX_CONCURRENCY=8
export BP_GLOBAL_RATE_LIMIT=30
export BP_GLOBAL_RATE_WINDOW=1.0
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

- `GET /status`：观测接口（纯只读，不消耗任何限流额度、不影响判定）：
  - `global`：全局层 `active / max_concurrency / admitted /
    rejected_concurrency`，以及全局窗口 `rate.{used, remaining, rejected,
    reset_in}`；
  - `dimensions.{业务}`：各业务 `active / waiting / capacity / max_queue /
    admitted / queue_full / queue_timeout`，以及业务窗口
    `rate.{used, remaining, rejected, reset_in}`（窗口滚动后 `used` 与
    `rejected` 随之重置）；
  - `dimension_count` / `singleflights`：注册维度数与各 single-flight
    在飞 key 数。

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
- 注册表在每次申请前回收“无执行/排队 **且限流窗口已过期**”的空闲维度：
  窗口仍在生效的维度不会被回收，因此慢速串行流量（请求一个个来、闸门
  在间隙回到空闲）不会把窗口内已用掉的量悄悄清零；窗口过期后已用量本就
  归零，此时回收不影响任何判定。gate 等待者队列 / single-flight 条目 /
  限流窗口都随维度或请求结束而释放。
- 全局在途名额不排队（满即拒），获取/释放都是同步操作，释放放在
  `finally` 中：取消、断开、异常任何路径都会立即归还，不会只减不增。

## 7. 本地验证

### 7.1 安装依赖

```bash
uv sync --group dev
```

### 7.2 单元 / 集成测试（27 个）

```bash
uv run pytest                 # 带实时日志，可看到各场景覆盖与通过情况
```

覆盖场景：

- `tests/test_rate_limit.py`：超量快速失败、窗口重置、**慢速串行请求照样
  计数与被拒（含窗口到点恢复、拒绝计数随窗口重置）**、100 并发争用计数准确；
- `tests/test_global_limit.py`：**全局在途上限立即拒绝且不排队、全局/业务
  拒绝原因可区分（global_concurrency vs queue_full）、全局放行上限跨业务
  合计、慢速串行流量下的业务限流与窗口恢复、取消后全局名额回收、
  /status 暴露两层观测且查询不耗额度**；
- `tests/test_gate.py`：并发上限、队列满、排队超时、**排队中取消释放名额**、
  **执行中取消回收并发额度**、零队列快速失败；
- `tests/test_singleflight.py`：同资源合并为 1 次执行、失败不缓存、
  leader 超时后可重新执行、等待者超时/取消不影响 leader 与其他等待者；
- `tests/test_api.py`：429/503/408 原因区分、客户端断开后额度回收、
  断开回收后再次压满全部成功、single-flight 合并；
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

**全局在途上限**（把全局在途压到比单业务额度还小，观察 `global_concurrency`）：

```bash
BP_GLOBAL_MAX_CONCURRENCY=1 uv run uvicorn main:app --port 8000 &
curl -s -X POST localhost:8000/process/report -H 'content-type: application/json' \
     -d '{"resource":"a","delay":2}' &          # 占住唯一的全局在途名额
sleep 0.2
curl -s -X POST localhost:8000/process/export -H 'content-type: application/json' \
     -d '{"resource":"b"}'
# => 503 {"error":{"reason":"global_concurrency", ...}}（不是 queue_full）
```

**慢速串行限流**（把窗口拉长、限额调小，然后隔一会儿发一个请求）：

```bash
BP_REPORT_RATE_LIMIT=3 BP_REPORT_RATE_WINDOW=30 \
  uv run uvicorn main:app --port 8000 &
for i in 1 2 3 4; do
  curl -s -X POST localhost:8000/process/report -H 'content-type: application/json' \
       -d '{"resource":"slow","delay":0.1}'; echo; sleep 2   # 每 2s 一个
done
# 前 3 个 200，第 4 个（窗口 30s 未到）=> 429 rate_limited + Retry-After
curl -s localhost:8000/status | python -m json.tool   # 看 report 窗口 used/rejected
```

## 8. 代码结构

```
app/
  config.py       业务/全局配置、默认值、BP_* 环境变量覆盖
  errors.py       RejectReason 枚举（含全局层原因）、HTTP 状态映射、ThrottledError
  rate_limit.py   固定窗口请求量限流器（窗口用量/拒绝计数快照）
  global_limit.py 全局容量保护：合计在途上限 + 合计放行上限，触顶即拒
  gate.py         并发闸门：显式有界 FIFO 队列 + 排队超时 + 取消安全
  registry.py     按维度持有限流器/闸门，空闲回收，gate_slot 生命周期
  singleflight.py 原子 join_or_create 的同资源请求合并
  services.py     report/export/asset 三类模拟耗时业务
  manager.py      全局限流→业务限流→全局在途→排队→合并→超时 的统一编排
  routes.py       HTTP 路由 + http.disconnect 监听 + /status 两层观测
  main.py         应用装配（日志 / 配置 / 管理器 / 异常处理）
scripts/
  load_test.py    本地负载测试
tests/            并发争用、拒绝、排队超时、取消、额度回收、全局层测试
```
