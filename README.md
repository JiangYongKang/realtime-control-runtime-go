# 实时采样与控制运行时（realtime-control-runtime-go）

一个**本地可运行**、不依赖任何外部服务/真实设备/真实凭据的实时采样与安全控制运行时。
传感器由可注入的数据源模拟；采样、控制决策与安全降级在并发与故障注入下保持
**行为确定、结论可区分、状态可解释、配置可追溯**。

```
go run ./cmd/rtrctl --config configs/config.example.json
```

运行期发送 `SIGHUP` 可原子热重载配置（新版本号必须严格大于当前版本），
`SIGINT/SIGTERM` 优雅退出。

---

## 1. 组件与边界

| 包 | 职责 | 关键边界 |
|---|---|---|
| `internal/clock` | 可注入时钟与周期 Ticker（`Real` / `FakeClock`） | 所有调度只依赖该接口，测试用手工推进的假时钟，时序完全确定 |
| `internal/sensor` | 样本模型、`Source` 接口、脚本源 `ScriptSource`、公式+故障注入源 `SimSource` | 不感知调度与控制；`Read(ctx)` 尊重取消 |
| `internal/sampler` | 多路按周期采样、统一时间基准、样本分类、有界缓存、非阻塞背压 | 只产出带判定结论的 `Event`，不做安全决策 |
| `internal/config` | 不可变配置快照、严格校验、原子版本化 `Manager`、JSON 编解码 | 读写分离；非法更新被拒绝且旧行为不变 |
| `internal/control` | 截止时间监测、连续异常计数、阈值校验、`NORMAL/WARMING/SAFE` 状态机、本地自定义判定的超时兜底 | 只消费 `sampler.Event`，决策带证据与原因；慢/卡死的自定义判定不能拖垮控制环 |
| `internal/runtime` | 组装上述组件、统一生命周期与原子重载入口 | 唯一持有多组件编排逻辑的地方 |
| `cmd/rtrctl` | 本地入口、配置加载、SIGHUP 重载、状态迁移打印 | 进程边界 |

数据流：

```
Sensor Source ──Read──▶ Sampler ──Event(分类+依据)──▶ Controller ──Decision(状态+证据)──▶ Drain/日志
                              ▲                              ▲
                        Fake/Real Clock               Config.Manager（原子快照）
```

---

## 2. 调度与统一时间基准

- **统一时钟**：采样时刻（`ObservedAt`）与控制评估时刻都取自同一个注入的 `clock.Clock`，
  样本自带设备时间戳 `Sample.Timestamp`，两者可比较得到 `lag`。
- **周期采样**：每个通道在各自配置的 `period` 上轮询；`FakeClock.NewTicker` 与标准库
  `time.Ticker` 语义一致——消费慢时**丢弃到点 tick 而不是排队**，通道缓冲固定为 1。
- **代际（generation）切换**：配置切换时，采样器先 `cancel` 并 **join 旧一代全部通道
  goroutine**，再启动新一代。因此同一通道永远不会有两个轮询协程并发，周期变更在单一
  原子边界生效。
- **控制周期独立**：控制环按 `safety.control_period` 固定节拍评估，与采样周期解耦。

### 2.1 样本分类（确定且可区分）

采样器对每次读取给出互斥结论，并在日志中打印输入（通道、值、设备时间戳、观测时间、lag）
与判定依据（`Detail`）：

| Outcome | 判定条件 | 处理 |
|---|---|---|
| `accepted` | 比通道最新样本更新，序号连续且未过期 | 更新该通道**单槽**缓存 |
| `duplicate` | 时间戳与序号都等于最新；或同序号重传 | 丢弃，不改变状态 |
| `stale` | 时间戳早于最新；序号回退；或早于 `观测时刻 - max_lag`（迟到/过期） | 丢弃，不改变状态 |
| `gapped` | 比最新更新但 `seq > lastSeq+1`（中间有丢失） | 值可用、刷新缓存，同时计一次异常 |
| `read_error` | 数据源返回错误（设备故障） | 计数并上报，不改变缓存 |

### 2.2 有界缓存与背压

- 每通道只保留**一个最新样本槽** + 少量计数，内存上界 = O(通道数)，与历史长度无关。
- 输出 `Event` 通道为**构造时固定容量**。下游变慢时采用非阻塞发送：满则丢弃并累加
  `DropCount`，日志打印背压丢弃——**采样协程永不因下游而阻塞，缓存永不无界增长**。

---

## 3. 安全状态迁移与恢复条件

控制器在每个控制节拍汇总所有通道证据（最新值、序号、age、连续异常数、越限标记）：

触发进入 `SAFE`（任一通道）：

| Reason | 条件 |
|---|---|
| `no_data` | 通道自启动以来从无可接受数据（且不在预热宽限内） |
| `deadline_missed` | 最新可用样本的年龄 `now - timestamp > decision_deadline` |
| `consecutive_anomaly` | 连续读错/缺口计数达到 `max_consecutive_err` |
| `threshold_violation` | 接受值超出通道 `[min_value,max_value]` 或告警带 `[warn_low,warn_high]` |
| `judge_timeout` | 注册的本地判定未在 `judge_timeout` 时限内返回，运行时兜底 |
| `judge_error` | 本地判定返回 error，运行时按错误必安全兜底 |
| `judge_unsafe` | 本地判定主动给出 `Safe=true` |

恢复 `NORMAL` 的**防抖条件**：

- 必须处于 `SAFE`；
- 连续 `recover_good_ticks` 个控制节拍**全部通道健康**（数据新鲜、无越限、无连续异常）；
- 期间任意一个不健康节拍都会把连续计数**清零重来**，因此单次抖动不会反复切换；
- 校验强制 `recover_good_ticks > max_consecutive_err`，从配置层面排除抖动风险。

`duplicate` / `stale` 样本**不改变**任何控制状态（乱序/重传/过期被显式忽略）。
每条决策日志打印：评估时间、迁移前后状态、原因、配置版本、连续健康计数、预热/兜底标记、
判定耗时、细节与每通道证据。

### 3.1 预热宽限（WARMING）

冷启动与每次热重载后，在**第一批样本到位之前**，系统不立即判 `SAFE`，而是给出一个可区分的
第三态 `WARMING`（原因 `warming`）：

- 宽限时长由 `safety.warmup_grace` 配置，**上限 30s**；缺省为 `0` 即关闭（老行为：无数据立刻
  `SAFE`）。宽限在启动和每次 `Reconfigure` 时重新开窗。
- 宽限**只覆盖"尚无任何可接受数据"**这一种情形：所有通道都还没数据时才判 `WARMING`。
- 宽限**不掩盖真实风险**：只要已经有数据，阈值越界（`threshold_violation`）、数据过期
  （`deadline_missed`）、连续异常达到阈值（`consecutive_anomaly`）都照常立刻进 `SAFE`。
- 宽限内一旦数据健康，直接转 `NORMAL`，不必等窗口走完；窗口到期仍无数据，则正常进 `SAFE`。
- `WARMING` 不累计健康节拍，也不是安全指令：它明确表达"数据尚未就绪"，与"判定不安全"可区分。

### 3.2 本地自定义判定与超时兜底

可以在启动时注册一段**本地**判定函数来接替内置策略（不依赖任何外部服务）：

```go
rt, _ := runtime.New(cfg, runtime.Options{
    // ...
    Judge: func(ctx context.Context, in control.JudgeInput) (control.Verdict, error) {
        // in.Channels：与内置策略同一套每通道证据；in.Config：当前配置快照
        return control.Verdict{Safe: false /* Detail: ... */}, nil
    },
})
```

运行时对每次评估做如下兜底，保证控制环不被慢算法拖死：

1. **硬时限**：每次判定都带 `safety.judge_timeout`（缺省/为 0 时取 `control_period`），
   配置值要求 `0 < judge_timeout <= control_period`。判定在一个**单工串行 worker** 上执行，
   全进程最多只有一次判定在飞。
2. **超时即兜底**：时限内没返回，运行时自己以 `judge_timeout` 原因强制进入 `SAFE`
   （决策上 `forced_safe=true`、`judge_timeout=true`），并取消该次调用的 `ctx`。
3. **迟到结果丢弃**：被放弃的判定即使之后算完，其结论也按序号比对后**直接丢弃**
   （计数 `LateResultCount`，日志 `late judge result discarded`），既不会改写已经下达的安全
   结论，也不会因此多触发一次状态切换。
4. **错误必安全**：判定函数返回 error 时以 `judge_error` 强制 `SAFE`；即使判定返回"放行"，
   运行时仍不掩盖已存在的越界/过期/连续异常硬证据。
5. **永久卡住**：若判定完全不响应 `ctx`，worker 会被它占住；此时后续每个节拍立即走兜底
   `SAFE`，不会排队堆积 goroutine（有界：至多一个 worker）。关闭运行时不等待该调用返回；
   响应取消的判定协程会随退出自行结束。
6. 预热期"所有通道无数据"时甚至不会调用自定义判定（没有可喂给它的证据），直接给 `WARMING`。

---

## 4. 配置兼容与生效规则

配置为不可变快照（`config.Config`），由 `Manager` 用原子指针持有，版本号 `version` 单调递增。

校验规则（任一不满足即拒绝）：

- 至少一个通道；通道 ID 非空且唯一；`period > 0`、`max_lag >= 0`；`min_value <= max_value`。
- `warn_low <= warn_high`；`control_period > 0`、`decision_deadline > 0`。
- `max_consecutive_err >= 1`、`recover_good_ticks >= 1`，且
  `recover_good_ticks > max_consecutive_err`。
- `judge_timeout` 可缺省（0）；设置时必须 `> 0` 且 `<= control_period`。
- `warmup_grace` 可缺省（0，关闭预热）；设置时必须 `>= 0` 且 `<= 30s`。
- 模拟故障参数非负；`amplitude != 0` 时 `period_s > 0`。

生效规则：

1. `Runtime.Reload` 先 `Validate`；非法/冲突配置直接返回错误，**旧版本继续生效**。
2. 版本号必须**严格递增**，回退被拒绝。
3. 通过校验后先原子安装进 `Manager`，再依次通知采样器与控制器；`Current()` 返回
   **防御性副本**，调用方修改不会污染活动快照——并发读者永远观察不到半更新状态。
4. 采样器以“停旧一代→启动新一代”方式应用新周期；控制器在版本边界重建其 Ticker。
   所有随后产生的事件/决策都加盖新版本号，可追溯。

JSON 中时长使用可读字符串（如 `"100ms"`），未知字段会被拒绝（`DisallowUnknownFields`）。

**与老配置的兼容**：`judge_timeout`、`warmup_grace` 在 JSON 中均可省略，省略即解码为 0：

- 不注册自定义判定时，`judge_timeout` 不生效，内置策略与旧版本完全一致；
- 注册了自定义判定但 `judge_timeout=0` 时，时限自动取当前 `control_period`；
- `warmup_grace=0` 关闭预热，无数据立刻进 `SAFE`——这正是升级前的默认表现。

### 4.1 新能力的可观测输出

每条控制决策日志额外携带：`warming`（是否处于预热宽限）、`forced_safe`（是否被运行时兜底
强制安全）、`judge_latency`（本次判定实际耗时）、`judge_timeout`（是否因超时放弃）。
另有点状日志：`judge timeout ...`（兜底强制）、`late judge result discarded`（迟到结论丢弃）。
可编程读取的计数器：`Controller.JudgeTimeoutCount()`、`Controller.LateResultCount()`、
`DecisionCount()`。状态迁移由 `rtrctl` 打印为 `STATE TRANSITION ...`。

---

## 5. 验证方式

```bash
go vet ./...
go test ./... -race -count=1
```

测试全部使用**可控假时钟**与模拟/脚本数据源，无需真实时间等待：

- `internal/clock`：假时钟 tick 触发、慢消费者丢 tick、停止语义。
- `internal/sensor`：脚本回放、耗尽行为、按读次序号的间歇/永久故障注入、上下文取消。
- `internal/sampler`：`accepted/duplicate/stale/gapped/read_error` 全分类；相对统一时钟的
  过期判定；5000 次无消费写入下的非阻塞背压与固定容量；真实轮询循环；重配置版本原子切换。
- `internal/control`：无数据、截止超时、连续异常阈值（单次异常不切换）、阈值越限进入 SAFE；
  重复/过期不影响状态；恢复必须连续 3 个健康节拍且被打断后重新计数；控制环重配置。
  新增：慢判定在时限内被强制 `SAFE`、被放弃的迟到结果被丢弃且不能翻盘也不产生额外切换、
  判定返回 error 必安全、永久卡住时每个节拍持续兜底；预热期不误报、超宽限进 `SAFE`、
  宽限不掩盖越界与连续异常、数据到齐即恢复、重载重新开窗、缺省关闭预热。
- `internal/config`：全部非法/冲突项拒绝、版本回退拒绝、拒绝后旧版本不变、快照隔离、
  8 个并发读者下看不到不一致快照、JSON 往返；新增 `judge_timeout`/`warmup_grace` 的边界
  校验（含 30s 上限与 `<= control_period`）、老 JSON 省略新字段时按 0 解码。
- `internal/runtime`：健康恢复 NORMAL；设备永久故障进入 SAFE 且记录读错误；
  非法/回退重载被拒、合法重载原子安装；6 协程并发混合重载；通过**栈检查**验证反复
  启停后无采样/控制协程泄漏。新增：慢/卡住的本地判定在端到端运行时里按时兜底、
  预热→宽限到期→SAFE 的完整链路、携带新参数的 6 协程并发热更新（重复 5 轮）、
  响应取消的判定协程在反复启停后零泄漏。

竞态与稳定性：测试在 `-race` 下通过（`go test ./... -race -count=N`），并对并发重载、
慢判定兜底与泄漏用例做多次重复验证。

关键判定依据均通过标准 `log` 打印（采样输入与分类、控制状态迁移与原因、配置版本切换、
背压丢弃计数），本地运行即可观察完整决策链。

---

## 6. 示例配置说明

见 `configs/config.example.json`：两路不同周期的模拟传感器、统一告警带、
350ms 决策截止、连续 2 次异常进安全、连续 3 个健康节拍恢复，
`judge_timeout=80ms`（注册本地判定时的单次时限）、`warmup_grace=500ms`（启动/重载预热）。
`sim.fail_every_n`（间歇故障）与 `sim.fail_from_read`（第 N 次读取起永久故障）
可用于手工制造故障场景，观察 WARMING / SAFE 进入与恢复全过程。
注意：示例配置本身不带自定义判定（判定函数只能在代码里通过 `runtime.Options.Judge`
注册），因此未注册时 `judge_timeout` 不生效，内置策略照常运行。
