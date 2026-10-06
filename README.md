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
| `internal/control` | 截止时间监测、连续异常计数、阈值校验、`NORMAL/SAFE` 状态机、自定义判定兜底、预热宽限 | 只消费 `sampler.Event`，决策带证据与原因；自定义判定超时由运行时强制安全 |
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
| `no_data` | 通道自启动以来从无可接受数据 |
| `deadline_missed` | 最新可用样本的年龄 `now - timestamp > decision_deadline` |
| `consecutive_anomaly` | 连续读错/缺口计数达到 `max_consecutive_err` |
| `threshold_violation` | 接受值超出通道 `[min_value,max_value]` 或告警带 `[warn_low,warn_high]` |

恢复 `NORMAL` 的**防抖条件**：

- 必须处于 `SAFE`；
- 连续 `recover_good_ticks` 个控制节拍**全部通道健康**（数据新鲜、无越限、无连续异常）；
- 期间任意一个不健康节拍都会把连续计数**清零重来**，因此单次抖动不会反复切换；
- 校验强制 `recover_good_ticks > max_consecutive_err`，从配置层面排除抖动风险。

`duplicate` / `stale` 样本**不改变**任何控制状态（乱序/重传/过期被显式忽略）。
每条决策日志打印：评估时间、迁移前后状态、原因、配置版本、连续健康计数、细节与每通道证据。

---

## 3.1 自定义控制判定（本地算法接入）

可以在启动时注册一段**本地判定逻辑**接替默认策略（`runtime.Options.Judge`，
接口 `control.Judge`）。判定输入与默认策略完全一致：同一套采样证据
（`JudgeInput.Channels`）、当前配置快照、当前状态与连续健康计数；输出一个
`Verdict{Healthy, Reason, Detail}`。状态机（SAFE 进入/恢复防抖）仍由运行时负责，
自定义判定只替换"本拍是否健康"的评估。

运行时为这段逻辑兜底（它可能很慢甚至永远卡住）：

- **判定时限**：每次调用最多等待 `safety.judge_timeout`（缺省 = `control_period`，
  且校验强制 `judge_timeout <= control_period`）。超时后运行时**自行进入 SAFE**
  （`reason=judge_timeout`、`forced_safe=true`），控制循环不会被拖死。
- **迟到结果丢弃**：被放弃的判定之后即使算完，也只会被收割并计数
  （`LateVerdictCount` + 日志 `discarded late verdict`），**不会**改写已经下达的
  安全结论，也不会因此多触发一次状态切换。
- **卡住有界**：上一次判定还没返回时不再发起新调用（`reason=judge_busy`，直接保持
  SAFE），因此即使判定永久卡死，滞留协程也至多 1 个，不会无界泄漏。
- **可观测**：每条决策日志带 `forced_safe`、`judge_dur`（单次判定实际耗时）、
  `late_verdicts` 计数；`Decision` 结构同样携带这些字段供对外输出。

## 3.2 冷启动/重载预热宽限

配置 `safety.warmup_grace`（缺省 `0` = 关闭，保持老行为）后，启动或每次热重载会
武装一个宽限窗口：第一批采样还没到位时，决策给出可区分的 **`warmup`** 结论
（`Warmup=true`，不进入 SAFE），而不是立刻因 `no_data` 判安全。

- 窗口在以下任一条件满足时结束：宽限时长到期、或所有通道都收到首批可用样本。
- **宽限不盖真实异常**：任何通道连续读错达到 `max_consecutive_err`，或已产生数据
  的通道出现越限/过期，立即按正常规则评估（可进入 SAFE）。
- 超过宽限数据仍未准备好 → 正常进入 SAFE（`no_data`）。
- 上限：`warmup_grace <= config.MaxWarmupGrace`（1 分钟），超出或负值一律拒绝。

---

## 4. 配置兼容与生效规则

配置为不可变快照（`config.Config`），由 `Manager` 用原子指针持有，版本号 `version` 单调递增。

校验规则（任一不满足即拒绝）：

- 至少一个通道；通道 ID 非空且唯一；`period > 0`、`max_lag >= 0`；`min_value <= max_value`。
- `warn_low <= warn_high`；`control_period > 0`、`decision_deadline > 0`。
- `judge_timeout` 省略/`0` 时缺省为 `control_period`；显式设置时必须
  `0 < judge_timeout <= control_period`（冲突即拒绝）。
- `warmup_grace` 省略/`0` 时关闭预热（老行为）；设置时必须
  `0 <= warmup_grace <= MaxWarmupGrace`（1 分钟）。
- `max_consecutive_err >= 1`、`recover_good_ticks >= 1`，且
  `recover_good_ticks > max_consecutive_err`。
- 模拟故障参数非负；`amplitude != 0` 时 `period_s > 0`。

生效规则：

1. `Runtime.Reload` 先 `Validate`；非法/冲突配置直接返回错误，**旧版本继续生效**。
2. 版本号必须**严格递增**，回退被拒绝。
3. 通过校验后先原子安装进 `Manager`，再依次通知采样器与控制器；`Current()` 返回
   **防御性副本**，调用方修改不会污染活动快照——并发读者永远观察不到半更新状态。
4. 采样器以“停旧一代→启动新一代”方式应用新周期；控制器在版本边界重建其 Ticker。
   所有随后产生的事件/决策都加盖新版本号，可追溯。

JSON 中时长使用可读字符串（如 `"100ms"`），未知字段会被拒绝（`DisallowUnknownFields`）。
新增字段 `judge_timeout` / `warmup_grace` 均为**可选**：老配置文件不写这两个键时按
缺省表现（判定时限 = 控制周期、预热关闭），行为与之前完全一致；两个新参数与老参数
一样参与原子热更新——非法或互相冲突（如 `judge_timeout > control_period`）的更新被
整体拒绝，旧版本继续生效。

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
  自定义判定：超时内在时限内强制 SAFE、卡死时不再发起新调用（`judge_busy`）、迟到结果被
  收割丢弃且不翻盘不多切换、限时内完成的判定按 verdict 决策并记录耗时；预热：首批数据
  缺失不误报 SAFE、首批到齐提前结束宽限、越限/连续异常不被宽限盖住、宽限到期正常进安全、
  缺省关闭。
- `internal/config`：全部非法/冲突项拒绝（含 `judge_timeout > control_period`、
  `warmup_grace` 超上限/负值）、版本回退拒绝、拒绝后旧版本不变、快照隔离、
  8 个并发读者下看不到不一致快照、JSON 往返（含新老字段、老配置缺省兼容）。
- `internal/runtime`：健康恢复 NORMAL；设备永久故障进入 SAFE 且记录读错误；
  非法/回退重载被拒、合法重载原子安装；6 协程并发混合重载；注册自定义判定后卡死判定
  被运行时强制 SAFE、迟到结果被丢弃、恢复走正常防抖；预热宽限覆盖迟到首批、宽限到期
  进安全；携带新参数的 3 轮并发热更新（合法/冲突混合）；通过**栈检查**验证反复
  启停后无采样/控制/判定协程泄漏。

竞态与稳定性：测试在 `-race` 下通过，并对并发重载与泄漏用例做多次重复验证。

关键判定依据均通过标准 `log` 打印（采样输入与分类、控制状态迁移与原因、配置版本切换、
背压丢弃计数），本地运行即可观察完整决策链。

---

## 6. 示例配置说明

见 `configs/config.example.json`：两路不同周期的模拟传感器、统一告警带、
350ms 决策截止、判定时限 100ms（= 控制周期）、预热宽限 500ms、连续 2 次异常进安全、
连续 3 个健康节拍恢复。
`sim.fail_every_n`（间歇故障）与 `sim.fail_from_read`（第 N 次读取起永久故障）
可用于手工制造故障场景，观察 SAFE 进入与恢复全过程。

本地验证自定义判定兜底：实现 `control.Judge`（例如人为 `time.Sleep` 超过
`judge_timeout` 或永久阻塞），通过 `runtime.Options.Judge` 注册后启动，
日志中可观察到 `reason=judge_timeout forced_safe=true`、判定恢复后的
`discarded late verdict` 以及 `late_verdicts` 计数增长，且状态不会因此反复切换。
