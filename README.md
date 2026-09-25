# telemetry-alert-engine-go

可在本地运行的指标采集与告警判定引擎。三个核心包：

| 包 | 职责 |
|---|---|
| `internal/metrics` | 并发安全的时间序列注册表（指标采集） |
| `internal/alert`   | 支持运行期规则变更的告警判定引擎 |
| `internal/sink`    | 带背压与放弃说明的缓冲结果写出器 |
| `internal/integration` | 并发写入 + 规则变更 + 写出失败交织的端到端测试 |

## 指标与标签模型

- 一条时间序列由 **指标名 + 标签集合（`map[string]string`）** 唯一确定。
  序列键是确定性的：标签按键名排序后拼接，与标签书写顺序无关。
- `Registry.Add(name, labels, value)` 对序列做**累加**（`Value += value`，`Count++`）。
  所有写入在同一把互斥锁内串行化，因此并发累加的结果与串行执行**逐次一致**，
  不依赖浮点求值顺序之外的任何时序假设。
- `Registry.Snapshot()` 返回按序列键排序的快照，输出确定、可复现。

### 标签基数上限

- 创建注册表时设定 `maxSeries`（`<=0` 表示不限制）。
- 取舍规则**确定且可复现**：已存在的序列永远可写；当序列数达到上限时，
  只拒绝**新**序列，不驱逐、不覆盖旧序列。
- 被拒绝的写入返回哨兵错误 `metrics.ErrCardinalityLimit`，调用方用
  `errors.Is(err, metrics.ErrCardinalityLimit)` 识别原因——绝不静默丢弃。

## 告警判定与规则变更语义

- 规则 `Rule{ID, Metric, Comparator, Threshold, For}`：当指标值满足比较条件
  （`GreaterThan` / `LessThan`）且**持续时长 ≥ `For`** 时触发。
- 判定在引擎互斥锁内串行进行，**同一时刻同一规则只有唯一结论**；
  `Fired` / `Resolved` 事件只在状态迁移时各发出一次，绝不重复。
- 指标缺失视为条件不成立（重置进度；若在告警中则发出 `Resolved`）。
- `UpsertRule` 支持采样期间变更规则，语义如下：
  - **进度保留**：`conditionSince` 与 `firing` 状态保留，已持续的时长不丢失；
  - **立即生效**：下一次 `Evaluate` 起使用**新阈值**，绝不沿用旧阈值；
  - **不重复触发**：已触发的规则变更后仍处于触发态，不会再次发出 `Fired`；
  - **自然收敛**：若新阈值下条件不再成立，下一次判定时按常规路径重置进度
    （未触发）或解除告警（已触发，发出一次 `Resolved`）。
- `RemoveRule` 删除规则并清除其全部状态。
- 引擎时钟通过 `NewEngine(now func() time.Time)` 注入，测试可替换为假时钟，
  保证判定结果可复现。

## 背压与放弃规则

` sink.Writer` 对调用方只暴露三种可区分的结果：

| 结果 | 表现 | 含义 |
|---|---|---|
| 已接受 | `Write` 返回 `nil` | 数据进入缓冲区，**保证**之后要么写出、要么出现在 `Dropped()` 中 |
| 暂时无法接受 | `Write` 返回 `sink.ErrBusy` | 缓冲区已满的背压信号，数据未接收，调用方可稍后重试 |
| 已放弃 | 出现在 `Writer.Dropped()` 并附 `Reason` | 下游持续失败（超过 `maxRetry` 次重试）后放弃，原因可解释 |

保证：

- 已接受的数据**绝不静默丢失、绝不重复写出**（写出 + 放弃 = 接受，守恒）。
- 下游恢复后**不回写已放弃的数据**，新接受的数据正常写出。
- `Close` 停止接受新数据（之后 `Write` 返回 `sink.ErrClosed`），
  并冲刷完缓冲区中所有已接受数据后才返回。
- 后台冲刷协程按序写出，单条数据重试 `maxRetry` 次仍失败才放弃并记录原因。

## 本地验证

```bash
# 编译与静态检查
go build ./... && go vet ./...

# 全部测试（含竞态检测），日志打印输入、状态变化与判定依据
go test ./... -race -v

# 只跑端到端交织场景
go test ./internal/integration/ -race -v
```

测试覆盖：

- `internal/metrics`：并发写入 vs 串行基准一致性；基数上限拒绝原因可识别、旧序列不受驱逐。
- `internal/alert`：持续时长触发/恢复各一次；规则变更后进度保留、新阈值生效、
  不重复触发；并发评估与规则变更交织下 Fired/Resolved 守恒。
- `internal/sink`：缓冲区满返回 `ErrBusy`；下游持续失败放弃并附原因；
  恢复后不回写已放弃数据；并发写出守恒且无重复。
- `internal/integration`：并发写入 + 采样期规则变更 + 下游间歇失败三者交织，
  校验指标总量等价串行基准、事件流严格交替、写出/放弃守恒。
