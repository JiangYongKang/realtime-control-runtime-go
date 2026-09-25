# realtime-control-runtime-go

一个**本地可运行**的实时采样与控制运行时。传感数据源以接口注入（默认提供内存 Mock 源与确定性波形源），
不依赖任何外部服务、真实设备或真实凭据。系统在并发、乱序数据、下游背压、控制超时与运行期配置变更等
条件下，给出**确定、可区分、可解释**的处理结论。

## 组件边界

```
cmd/rtrctl               本地可运行入口（真实时钟 + 波形源 + 故障/热重载演示）
configs/                 示例配置（合法 v1、合法 v2、非法样例）
internal/
  clock     Clock 抽象：Real（墙钟）与 Fake（手动推进、无后台协程、可确定性测试）
  sensor    可注入数据源：Sensor 接口、MockSource（队列驱动）、PatternSource（波形/故障注入）
  sampler   被动、由时钟 tick 驱动的轮询器：确定性分类 + 有界通道状态 + 非阻塞输出
  config    配置模式、校验、JSON 解析与并发安全的版本化 Store（copy-on-write）
  control   截止时间感知的控制器与安全状态机（normal/degraded/safe + 滞后恢复）
  runtime   唯一调度器：单一时间基准，采样→决策的固定拍内顺序，组件生命周期与截止守护
  model     跨组件共享的领域类型（Sample/Envelope/Decision/SafetyState/Verdict）
```

依赖方向单一（`runtime → {sampler,control,config,sensor,clock}`），各组件均可脱离彼此独立测试。

## 快速开始

```bash
# 构建
go build ./...

# 运行 5 秒（默认读取 configs/example.json，合成正弦波数据源）
go run ./cmd/rtrctl -config configs/example.json -duration 5s

# 在 400ms 时对 pressure 通道连续注入 3 次读故障，观察进入 safe 与稳定恢复
go run ./cmd/rtrctl -config configs/example.json -duration 3s \
    -fault-at 400ms -fault-burst 3 -fault-channel pressure

# 运行期热重载：把 -config 指向的文件在重载时刻替换为内容更新、版本递增的新文件
cp configs/example.json /tmp/live.json
( sleep 0.4; cp configs/example_v2.json /tmp/live.json ) &
go run ./cmd/rtrctl -config /tmp/live.json -duration 1.5s -reload-at 600ms
```

日志会打印每个样本的关键输入（通道、序号、源时间戳、接收时间戳）与判定结论（verdict），
以及每次决策的状态、版本、输出、**判定依据 reasons** 与参与计算的最新值；状态迁移与截止超时单独高亮。

## 采样分类：确定且可区分

每个样本携带 `(通道, 单调序号 Seq, 源时间戳 Time)`。分类器在每通道上仅维护一个游标，
按固定优先级给出互斥结论：

| Verdict      | 触发条件 | 是否推进序号游标 |
|--------------|----------|------------------|
| `accepted`   | 序号更新且源时间戳未超过 `staleness`（默认 3×周期） | 是 |
| `duplicate`  | `Seq == 已接受的最后序号` | 否 |
| `out_of_order` | `Seq < 已接受的最后序号` | 否 |
| `stale`      | 序号更新，但 `now - Time > staleness` | 否（丢弃旧值） |
| `fault`      | 源显式返回读错误（`Sample.Err != nil`） | 否（重试同序号仍可被接受） |
| `dropped`    | 下游有界通道已满（背压），样本已分类但无法交付 | — |

- **缓存有上界**：每通道只保留“最新一个” envelope（外加固定序号游标），不会随乱序/历史数据增长；
  envelope 输出通道容量固定（默认 64）。
- **下游变慢不堆积、不长时间阻塞采样**：向输出通道的发送是非阻塞 `select default`，满了计入
  `dropped` 背压计数；决策侧每拍最多保留 `maxPendingEnvelopes=256` 个待处理 envelope，超出即合并
  （其效果已体现在 snapshot/stats 中）。
- **统一时间基准**：采样与决策共用 runtime 的唯一 ticker，并以注入时钟的 `Now()` 作为
  `RecvTime`/决策时间，保证所有日志与判定可在同一时间轴上对齐。

## 调度与时间基准

- **唯一调度器**：`runtime.scheduler` 持有**一个** ticker。每个周期严格按顺序执行：
  1. `sampler.Poll()`：对每个源至多做一次有界读取并分类；
  2. 非阻塞排空本拍产生的 envelope 到有界 pending 集合；
  3. 取 `Snapshot()`/`Stats()`，调用控制器做一次决策，并由截止时间守护包裹。

  单一 ticker 消除了“采样 ticker 与控制 ticker 同周期、触发顺序不确定”的问题，保证拍内
  “先采样、后决策”。
- **可控时钟**：`clock.Fake` 无后台协程，定时器/周期器在 `Advance` 内同步触发；其通道容量为 1，
  消费不及时会合并 tick（与真实 `time.Ticker` 语义一致）。定时器推进为 O(1) 算术，锁内无按周期循环。
- 测试在推进假时钟前等待 `Runtime.Ready()`（调度器已 arm ticker），避免丢失首个 tick。

## 安全状态迁移与恢复条件

状态：`normal`（全权控制）→ `degraded`（软异常，输出保持/降权）→ `safe`（硬故障，输出安全值
`safety.safeOutput`）。硬条件优先于软条件。

每个评估周期按固定顺序收集事实（都写入 `Decision.Reasons`，可解释）：

- 硬条件（进入 **safe**）：
  - `decision_deadline_exceeded`：决策未在 `safety.decisionDeadline` 预算内完成，立即强制安全输出；
  - `data_timeout:<ch>`：required 通道在 `safety.dataTimeout` 内没有新的被接受样本；
  - `no_data:<ch>`：required 通道自启动起在宽限期（=`dataTimeout`）内从未交付；
  - `consecutive_errors:<ch>`：某通道连续读故障次数达到 `safety.consecutiveFaults`。
- 软条件（进入 **degraded**）：阈值越限 `threshold_violation` 或超出 `minValue/maxValue`（`out_of_range`）。
  注意：一旦处于 safe，软条件不会把它“降级”回 degraded。
- **启动宽限**：系统刚启动、required 通道尚未来数据时，只记录 `warming:<ch>`，不计故障，避免上电闪断。

**恢复（防抖动，不因单次抖动反复切换）**：

- 离开 safe/degraded 需要连续 `safety.recoveryStableRuns` 个**全部健康**的评估周期；
  判定取所有通道中“最短健康连续拍数”，任一通道抖动都会重置恢复计数；
- 单次故障在 normal 下不会立即跳安全，必须再次累计到连续阈值。

**截止时间与迟到结果**：每个决策带单调 `generation`。超时路径调用 `ForceSafe` 并记录该代次；
被放弃的慢评估若稍后完成，会在获取锁后发现自己的 `generation <= forceGen` 而被丢弃，
**迟到结果永远不能推翻已下达的安全决策**。超时路径与在途评估不共享可写状态（值传递），无数据竞争。

## 配置兼容与生效规则

`config.Store` 是并发安全、单调版本化的持有者（copy-on-write，写锁内整体替换）：

- **校验先行**：候选配置先 `Validate()`。任何非法项（非正版本/周期、空通道、重复通道 ID、
  `min>max`、无 required 通道、非正超时/截止时间/连续故障数/恢复拍数、阈值引用未知通道、
  时长字符串非法等）都会被拒绝，**旧行为完全保持**。
- **版本单调**：候选 `version` 必须严格大于当前版本，否则返回 `ConflictError` 并拒绝（旧行为保持）。
- **原子生效、无半更新**：周期、阈值、安全策略在一次写锁内整体安装；控制器每拍从 Store 读取最新副本，
  因此并发读写下观察到的永远是某个完整版本，决策中携带其 `Version` 可追溯。Store 对写入值做深拷贝，
  调用方事后修改自己的结构体不会污染已安装配置。
- **周期热切换**：仅周期需要显式 fan-out 到调度器（原子替换 ticker）；阈值/安全策略由控制器下拍读取，
  无需半套中间状态。订阅通道为有界且合并最新值，慢订阅者不会阻塞更新。

## 背压与资源上界汇总

- 每通道状态：1 个 envelope + 序号游标（与乱序/历史长度无关）。
- envelope 输出通道：固定容量（默认 64），满则非阻塞丢弃并计数。
- 每拍 pending envelope：上限 256，超出合并。
- 决策输出通道：固定容量 32，慢观察者合并决策，不阻塞控制环。
- 源读取：对非即时源以 `context` 超时限定在一个周期内；停止时所有协程可随 context 取消退出。

## 验证方式

```bash
# 全量单测（竞态检测）
go test ./... -race -count=1

# 高重复压测（调度/并发相关用例）
go test ./internal/runtime/ -race -count=10
go test ./... -race -count=5

# 静态检查
go vet ./...
```

测试使用**可控时钟 + Mock 数据源**，覆盖：

- `clock`：定时器/周期器触发、停止/重置、tick 合并、停止后无残留定时器；
- `sampler`：accepted/duplicate/out_of_order/stale/fault 的确定性分类、空轮询不阻塞、
  背压丢弃且采样不被阻塞、周期热更新；
- `control`：阈值降级、数据超时进 safe、截止超时立即 safe、连续故障阈值、连续稳定恢复、
  单次抖动不反复切换、启动宽限、配置版本即时生效；
- `config`：校验拒绝、时长 JSON、版本冲突拒绝、深拷贝隔离、并发读写下无半更新（torn read）；
- `runtime`：端到端 normal→safe→恢复、注入慢控制器验证截止强制安全、**并发配置热重载**、
  背压，以及 `TestNoGoroutineOrClockLeak` 验证多轮启停后协程数回落、假时钟无残留定时器（配合强制 GC）。

全部用例在 `-race` 下通过。

## 配置字段

见 `configs/example.json`。时长字段（`period`、`safety.dataTimeout`、`safety.decisionDeadline`）
接受 `"100ms"`/`"1s"` 字符串或纳秒数字。

```jsonc
{
  "version": 1,                       // 必须正整数且单调递增
  "period": "100ms",                  // 采样/决策周期（唯一调度节拍）
  "channels": [
    { "id": "temperature", "minValue": -40, "maxValue": 125, "required": true }
  ],
  "threshold": { "temperature": 90 }, // 软阈值；键必须是已声明通道
  "safety": {
    "dataTimeout": "500ms",           // required 通道数据超时（也作启动宽限）
    "consecutiveFaults": 3,           // 连续读故障阈值 -> safe
    "decisionDeadline": "80ms",       // 单拍决策预算，超时 -> safe
    "recoveryStableRuns": 5,          // 恢复所需连续健康拍数（防抖）
    "safeOutput": 0                   // safe 状态的执行器输出
  }
}
```
