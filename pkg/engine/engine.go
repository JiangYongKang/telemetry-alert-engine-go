// Package engine 把指标存储、告警判定与结果写出编排为一个本地引擎：
// 指标并发写入 store，定时器周期性地对快照进行判定，判定结论严格按
// 产生顺序交给 sink；sink 返回 Busy（缓冲耗尽/下游持续失败）时，结论
// 留在引擎的待投递队列中，下次判定前优先重试，既不丢弃也不会重复
// 入队；sink 明确 Abandoned 的结论被计入 dropped 并保留原因。
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/alert"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/metrics"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/sink"
)

// IngestResult 是一次指标写入的对外结果。
type IngestResult struct {
	Accepted bool
	Reason   string // 被拒绝时为可识别原因（如 cardinality_limit）
}

// DroppedEvent 记录被 sink 确定性放弃（Abandoned）的结论及其原因，
// 用于诊断与对账：放弃是显式的，不允许静默发生。
type DroppedEvent struct {
	Event  alert.Event
	Reason sink.AbandonReason
}

// Engine 编排指标、告警与写出。
type Engine struct {
	store  *metrics.Store
	rules  *alert.Manager
	writer *sink.Writer
	tick   time.Duration

	mu      sync.Mutex
	pending []alert.Event // sink Busy 时未能入队的结论，按产生顺序保留
	dropped []DroppedEvent

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
	wg        sync.WaitGroup
}

// New 创建引擎。
func New(store *metrics.Store, rules *alert.Manager, writer *sink.Writer, tick time.Duration) *Engine {
	if tick <= 0 {
		panic("engine: tick must be positive")
	}
	return &Engine{
		store:  store,
		rules:  rules,
		writer: writer,
		tick:   tick,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Ingest 写入一条指标样本。基数上限等拒绝原因通过 IngestResult 返回，
// 调用方可明确区分“接受”与“拒绝及原因”，不存在静默丢弃。
func (e *Engine) Ingest(samp metrics.Sample) IngestResult {
	if err := e.store.Write(samp); err != nil {
		reason := "unknown"
		var clErr *metrics.CardinalityLimitError
		if errors.As(err, &clErr) {
			reason = "cardinality_limit"
		}
		return IngestResult{Accepted: false, Reason: reason}
	}
	return IngestResult{Accepted: true}
}

// UpsertRule 原子地新增或替换规则（见 alert.Manager.Upsert 语义）。
func (e *Engine) UpsertRule(r alert.Rule) { e.rules.Upsert(r) }

// RulePhases 返回规则 ID 到当前阶段的诊断视图。
func (e *Engine) RulePhases() map[string]alert.Phase { return e.rules.Snapshot() }

// RemoveRule 删除规则；若规则处于 firing，其 resolved 结论与判定结论
// 走同一条有序投递路径，保证不丢、不重。
func (e *Engine) RemoveRule(id string) bool {
	ev, ok := e.rules.Remove(id, time.Now())
	if !ok {
		return false
	}
	// 仅删除 firing 规则时才会补发 resolved；inactive/pending 规则删除
	// 返回零值事件，不产生任何投递。
	if ev.Type != "" {
		e.mu.Lock()
		e.enqueueLocked(ev)
		e.mu.Unlock()
	}
	return true
}

// Start 启动周期性采样判定循环（幂等）。
func (e *Engine) Start(ctx context.Context) {
	e.startOnce.Do(func() {
		e.wg.Add(1)
		go e.loop(ctx)
	})
}

// Shutdown 停止判定循环，尽力在 ctx 截止前把待投递结论送入 sink，
// 然后关闭写出器。返回值是最终仍无法送达的原始事件（引擎内滞留 +
// sink 交还），它们未被确认、也未被重复写出，由调用方决定后续处理。
func (e *Engine) Shutdown(ctx context.Context) ([]alert.Event, error) {
	e.stopOnce.Do(func() { close(e.stop) })
	e.wg.Wait()

	// 判定循环已停止：在截止时间前持续把 pending 喂给 sink——sink 容量
	// 有限且在途事件需要排空时间，单次尝试遇到 Busy 后必须等待空位再喂
	// 下一条，否则最后一批事件会被误判为无法送达。
	e.flushUntilDrained(ctx)

	undeliveredBytes, err := e.writer.Close(ctx)

	// 仍留在引擎 pending 中的事件 + sink 交还的字节，统一向上交还。
	e.mu.Lock()
	undelivered := make([]alert.Event, 0, len(e.pending))
	undelivered = append(undelivered, e.pending...)
	e.pending = nil
	e.mu.Unlock()
	for _, b := range undeliveredBytes {
		var ev alert.Event
		if json.Unmarshal(b, &ev) == nil {
			undelivered = append(undelivered, ev)
		}
	}
	return undelivered, err
}

// PendingCount 返回因 sink 背压暂留在引擎中的结论数。
func (e *Engine) PendingCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// flushUntilDrained 在 ctx 截止前反复尝试把 pending 送入 sink。
// 调用前判定循环必须已停止（否则可能与之竞争 e.mu；锁本身保证安全，
// 但“排空后关闭 sink”要求循环不再产生新事件）。
func (e *Engine) flushUntilDrained(ctx context.Context) {
	const poll = 2 * time.Millisecond
	timer := time.NewTimer(poll)
	defer timer.Stop()
	for {
		e.mu.Lock()
		e.tryFlushLocked()
		n := len(e.pending)
		e.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(poll)
		}
	}
}

// Dropped 返回被确定性放弃的结论记录（诊断/对账用）。
func (e *Engine) Dropped() []DroppedEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DroppedEvent, len(e.dropped))
	copy(out, e.dropped)
	return out
}

// loop 是周期判定循环。
func (e *Engine) loop(ctx context.Context) {
	defer e.wg.Done()
	defer close(e.done)

	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stop:
			return
		case at := <-ticker.C:
			e.tickOnce(at)
		}
	}
}

// tickOnce 执行一次：快照 -> 逐序列（按键排序，保证可复现）判定 ->
// 有序投递结论。
func (e *Engine) tickOnce(at time.Time) {
	snap := e.store.Snapshot()
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var produced []alert.Event
	for _, k := range keys {
		produced = append(produced, e.rules.Evaluate(k, snap[k], at)...)
	}

	e.mu.Lock()
	// 即使本 tick 没有新结论，也要给此前因 Busy 滞留的结论一次重试机会，
	// 否则下游恢复后若长时间无新事件，滞留结论会一直挂起。
	e.tryFlushLocked()
	for _, ev := range produced {
		e.enqueueLocked(ev)
	}
	e.mu.Unlock()
}

// enqueueLocked 调用时持有 e.mu。先按 FIFO 重试滞留结论，再处理新
// 结论：Accepted 则从 pending 移除；Busy 则留在/进入 pending 等待
// 下次重试；Abandoned 则从 pending 移除并记入 dropped（带原因）。
func (e *Engine) enqueueLocked(ev alert.Event) {
	// 先尝试排空此前的滞留结论，保持全局 FIFO 顺序。
	e.tryFlushLocked()
	// 新事件本身：先入 pending 作为“未确认”状态，再尝试提交。
	e.pending = append(e.pending, ev)
	e.tryFlushLocked()
}

// tryFlushLocked 按顺序尝试提交 pending 队首，直到遇到 Busy 为止。
// 永不阻塞（Submit 本身不阻塞）。调用时持有 e.mu。
func (e *Engine) tryFlushLocked() {
	for len(e.pending) > 0 {
		b, err := json.Marshal(e.pending[0])
		if err != nil {
			// JSON 序列化失败是确定性的：记录后移除，不无限重试。
			e.dropped = append(e.dropped, DroppedEvent{
				Event:  e.pending[0],
				Reason: sink.AbandonReason("marshal_error:" + err.Error()),
			})
			e.pending = e.pending[1:]
			continue
		}
		out := e.writer.Submit(b)
		switch out.Kind {
		case sink.OutcomeAccepted:
			e.pending = e.pending[1:] // 已受 sink 不丢保证，移交所有权
		case sink.OutcomeBusy:
			return // 队首未被接受，保持顺序，稍后整体重试
		case sink.OutcomeAbandoned:
			e.dropped = append(e.dropped, DroppedEvent{
				Event:  e.pending[0],
				Reason: out.Reason,
			})
			e.pending = e.pending[1:] // 终结性放弃，绝不回写
		}
	}
}
