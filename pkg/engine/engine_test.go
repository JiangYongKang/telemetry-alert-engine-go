package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/alert"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/metrics"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/sink"
)

// scriptedDownstream 可切换失败状态，并记录实际送达的事件 ID 计数。
type scriptedDownstream struct {
	mu   sync.Mutex
	fail bool
	got  []alert.Event
}

func (d *scriptedDownstream) Write(_ context.Context, events [][]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail {
		return errors.New("downstream failing")
	}
	for _, b := range events {
		var ev alert.Event
		if err := json.Unmarshal(b, &ev); err != nil {
			return err
		}
		d.got = append(d.got, ev)
	}
	return nil
}

func (d *scriptedDownstream) setFail(v bool) {
	d.mu.Lock()
	d.fail = v
	d.mu.Unlock()
}

func (d *scriptedDownstream) delivered() []alert.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]alert.Event, len(d.got))
	copy(out, d.got)
	return out
}

// TestConcurrentIngestRuleChangeSinkFailure 是核心交织场景：
//   - 多个 goroutine 持续并发累加同一计数器（基数=1），另有 goroutine
//     写超出基数上限的新序列，必须收到 cardinality_limit 拒绝；
//   - 主线程按固定 tick 驱动一条 trace 序列，并在采样中途两次改阈值；
//   - 下游在中途失败（容量故意设为 1 以制造 Busy），随后恢复；
//   - 最终送达的告警事件序列必须与手工串行基准逐条一致，且每个事件
//     恰好送达一次，无丢失、无重复、无悬挂。
func TestConcurrentIngestRuleChangeSinkFailure(t *testing.T) {
	const maxCard, counterWorkers, addsPerWorker = 3, 8, 500

	store := metrics.NewStore(maxCard)
	rules := alert.NewManager()
	down := &scriptedDownstream{}
	writer := sink.NewWriter(down,
		sink.WithCapacity(1), // 极小缓冲：下游一失败立刻产生 Busy 背压
		sink.WithBackoff(time.Millisecond, 5*time.Millisecond),
	)
	eng := New(store, rules, writer, 10*time.Millisecond)

	counterKey := metrics.LabelsFromMap(map[string]string{"series": "counter"})
	traceKey := metrics.LabelsFromMap(map[string]string{"series": "trace"})
	extraKey := metrics.LabelsFromMap(map[string]string{"series": "extra"})

	// 先把基数占满（counter / trace / extra）。
	for _, k := range []metrics.Labels{counterKey, traceKey, extraKey} {
		if r := eng.Ingest(metrics.Sample{Labels: k, Value: 1}); !r.Accepted {
			t.Fatalf("prefill %s rejected: %s", k.Key(), r.Reason)
		}
	}

	// 计数器规则阈值设成不可达：它的告警时机依赖并发调度，不纳入基准，
	// 但它全程参与评估，用于证明并发写入不影响状态机的一致性。
	eng.UpsertRule(alert.Rule{ID: "counter-guard", SeriesKey: counterKey.Key(), Op: alert.OpGT, Threshold: 1e18})
	// trace 规则：初始阈值 10，立即触发。
	eng.UpsertRule(alert.Rule{ID: "r", SeriesKey: traceKey.Key(), Op: alert.OpGT, Threshold: 10})

	now := time.Unix(1000, 0)
	tick := func() time.Time {
		now = now.Add(time.Second)
		eng.tickOnce(now)
		return now
	}

	// 并发写入与判定交织进行。
	var wg sync.WaitGroup
	for w := 0; w < counterWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < addsPerWorker; i++ {
				r := eng.Ingest(metrics.Sample{Labels: counterKey, Value: 1, Add: true})
				if !r.Accepted {
					t.Errorf("counter write rejected: %s", r.Reason)
				}
			}
		}()
	}
	// 超出基数上限的新序列：必须被明确拒绝，原因可识别，计数可对账。
	var rejected int64
	var rmu sync.Mutex
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			overflowKey := metrics.LabelsFromMap(map[string]string{"series": "overflow", "w": string(rune('a' + id))})
			for i := 0; i < 50; i++ {
				r := eng.Ingest(metrics.Sample{Labels: overflowKey, Value: 1, Add: true})
				if r.Accepted {
					t.Errorf("overflow series must be rejected: %s", overflowKey.Key())
					return
				}
				if r.Reason != "cardinality_limit" {
					t.Errorf("reject reason = %q, want cardinality_limit", r.Reason)
					return
				}
				rmu.Lock()
				rejected++
				rmu.Unlock()
			}
		}(w)
	}

	setTrace := func(v float64) {
		if r := eng.Ingest(metrics.Sample{Labels: traceKey, Value: v}); !r.Accepted {
			t.Fatalf("trace ingest rejected: %s", r.Reason)
		}
	}

	// ---- 固定脚本（串行基准见 wants） ----
	setTrace(11)
	tt := tick() // tick1: 11>10 -> fired v1
	t.Logf("tick1 输入 trace=11 阈值=10: fired(v2)（v1 为 counter-guard） at %v", tt.Unix())

	setTrace(12)
	tick() // tick2: 持续满足，无事件

	eng.UpsertRule(alert.Rule{ID: "r", SeriesKey: traceKey.Key(), Op: alert.OpGT, Threshold: 30})
	t.Log("tick3 前规则变更: 阈值 10->30 (v3)，阶段保持 firing，无事件")
	setTrace(12)
	tick() // tick3: 按新阈值 12>30 为假 -> resolved v3（不沿用旧阈值）

	setTrace(35)
	tick() // tick4: 35>30 -> fired v2

	down.setFail(true)
	t.Log("下游开始持续失败；容量=1，随后提交将得到 Busy，事件滞留引擎 pending")
	setTrace(36)
	tick() // tick5: 持续满足，无事件

	eng.UpsertRule(alert.Rule{ID: "r", SeriesKey: traceKey.Key(), Op: alert.OpGT, Threshold: 100})
	t.Log("tick6 前规则变更: 阈值 30->100 (v4)")
	setTrace(36)
	tick() // tick6: 36>100 为假 -> resolved v2；此时 sink Busy，结论滞留
	if eng.PendingCount() == 0 {
		t.Fatal("expect events stuck in engine pending during sink outage")
	}
	t.Logf("背压生效: 引擎 pending=%d，结论未丢未重，等待恢复后按序补投", eng.PendingCount())

	setTrace(5)
	tick() // tick7: 仍 inactive，无新事件

	down.setFail(false)
	t.Log("下游恢复")
	setTrace(101)
	tick() // tick8: 先补投 resolved(v2)，再 101>100 -> fired v3

	setTrace(102)
	tick() // tick9: 无事件
	setTrace(0)
	tick() // tick10: -> resolved v3

	wg.Wait()
	t.Logf("并发写入结束: 计数器累加期望=%d，overflow 拒绝=%d", counterWorkers*addsPerWorker, rejected)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	undelivered, err := eng.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown after recovery: %v", err)
	}
	if len(undelivered) != 0 {
		t.Fatalf("undelivered after recovery = %d: %+v", len(undelivered), undelivered)
	}
	if len(eng.Dropped()) != 0 {
		t.Fatalf("no event may be silently abandoned: %+v", eng.Dropped())
	}

	// 计数器并发结果必须与串行基准一致。
	var wantCounter float64 = counterWorkers * addsPerWorker
	gotCounter := store.Snapshot()[counterKey.Key()]
	t.Logf("计数器并发累加结果=%v（初值1 + %d 次 +1 => %v），基数=%d 拒绝计数=%d",
		gotCounter, counterWorkers*addsPerWorker, wantCounter+1, store.Cardinality(), store.RejectedCount())
	if gotCounter != wantCounter+1 {
		t.Fatalf("counter = %v, want %v (serial-equivalent)", gotCounter, wantCounter+1)
	}
	if store.Cardinality() != maxCard {
		t.Fatalf("cardinality = %d, want %d", store.Cardinality(), maxCard)
	}
	if store.RejectedCount() != rejected || rejected != 4*50 {
		t.Fatalf("rejected: store=%d tracked=%d expected=%d", store.RejectedCount(), rejected, 4*50)
	}

	// 送达事件与串行基准逐条核对。
	got := down.delivered()
	type want struct {
		typ alert.EventType
		ver alert.Version
		thr float64
	}
	wants := []want{
		{alert.EventFired, 2, 10},
		{alert.EventResolved, 3, 30},
		{alert.EventFired, 3, 30},
		{alert.EventResolved, 4, 100}, // tick6 产生、恢复后补投，顺序仍在 fired(v4) 之前
		{alert.EventFired, 4, 100},
		{alert.EventResolved, 4, 100},
	}
	if len(got) != len(wants) {
		for _, e := range got {
			t.Logf("实际送达: %s v%d 依据阈值=%v at=%v", e.Type, e.Version, e.Rule.Threshold, e.At.Unix())
		}
		t.Fatalf("delivered %d events, want %d", len(got), len(wants))
	}
	for i, w := range wants {
		g := got[i]
		if g.Type != w.typ || g.Version != w.ver || g.Rule.Threshold != w.thr {
			t.Fatalf("event[%d] = %s v%d thr=%v, want %s v%d thr=%v",
				i, g.Type, g.Version, g.Rule.Threshold, w.typ, w.ver, w.thr)
		}
		t.Logf("基准一致[%d]: %s version=%d 判定依据阈值=%v at=%v",
			i, g.Type, g.Version, g.Rule.Threshold, g.At.Unix())
	}

	st := writer.Stats()
	t.Logf("写出器计数: accepted=%d busy=%d abandoned=%d delivered=%d undelivered=%d",
		st.Accepted, st.Busy, st.Abandoned, st.Delivered, st.Undelivered)
	if st.Delivered != int64(len(wants)) {
		t.Fatalf("delivered stat = %d, want %d", st.Delivered, len(wants))
	}
	if st.Busy == 0 {
		t.Fatal("expected Busy backpressure signals during outage")
	}
	if st.Abandoned != 0 || st.Undelivered != 0 {
		t.Fatalf("nothing should be abandoned in recovery scenario: %+v", st)
	}
}

// TestEngineShutdownWithPersistentFailure 验证下游始终不恢复时：
// 未送达结论在 Shutdown 中显式交还（类型与数量可查），而不是静默丢失，
// 且 sink 不会在关闭后回写它们。
func TestEngineShutdownWithPersistentFailure(t *testing.T) {
	store := metrics.NewStore(2)
	rules := alert.NewManager()
	down := &scriptedDownstream{fail: true}
	writer := sink.NewWriter(down, sink.WithCapacity(1),
		sink.WithBackoff(time.Millisecond, 3*time.Millisecond))
	eng := New(store, rules, writer, 10*time.Millisecond)

	k := metrics.LabelsFromMap(map[string]string{"s": "t"})
	eng.UpsertRule(alert.Rule{ID: "r", SeriesKey: k.Key(), Op: alert.OpGT, Threshold: 10})
	eng.Ingest(metrics.Sample{Labels: k, Value: 11})
	eng.tickOnce(time.Unix(0, 1)) // fired：容量1 -> sink accepted 但无法投递

	// 再来一个恢复事件：sink 队列仍被占 -> Busy -> 滞留引擎 pending。
	eng.Ingest(metrics.Sample{Labels: k, Value: 1})
	eng.tickOnce(time.Unix(0, 2)) // resolved

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	undelivered, err := eng.Shutdown(ctx)
	if !errors.Is(err, sink.ErrUndelivered) {
		t.Fatalf("err = %v, want ErrUndelivered", err)
	}
	if len(undelivered) != 2 {
		t.Fatalf("undelivered = %d, want 2 (1 in sink + 1 in engine pending)", len(undelivered))
	}
	types := map[alert.EventType]int{}
	for _, e := range undelivered {
		types[e.Type]++
	}
	t.Logf("下游始终失败，关闭时显式交还: fired=%d resolved=%d，均未静默丢失",
		types[alert.EventFired], types[alert.EventResolved])
	if types[alert.EventFired] != 1 || types[alert.EventResolved] != 1 {
		t.Fatalf("handed-off events mismatch: %+v", types)
	}
	if len(down.delivered()) != 0 {
		t.Fatal("sink must not rewrite events after handing them off")
	}
}

// TestEngineRemoveRule 验证删除规则：firing 补发 resolved 并送达；
// inactive 删除不产生任何结论；未知规则返回 false。
func TestEngineRemoveRule(t *testing.T) {
	store := metrics.NewStore(2)
	rules := alert.NewManager()
	down := &scriptedDownstream{}
	writer := sink.NewWriter(down, sink.WithCapacity(4),
		sink.WithBackoff(time.Millisecond, 5*time.Millisecond))
	eng := New(store, rules, writer, time.Hour) // 不启动循环，手动 tick

	k := metrics.LabelsFromMap(map[string]string{"s": "a"})
	eng.Ingest(metrics.Sample{Labels: k, Value: 20})
	eng.UpsertRule(alert.Rule{ID: "hot", SeriesKey: k.Key(), Op: alert.OpGT, Threshold: 10})
	eng.UpsertRule(alert.Rule{ID: "cold", SeriesKey: k.Key(), Op: alert.OpLT, Threshold: 0})
	eng.tickOnce(time.Unix(0, 1)) // hot fired, cold inactive

	if !eng.RemoveRule("hot") {
		t.Fatal("remove existing rule must return true")
	}
	t.Log("删除 firing 规则 hot -> 补发 resolved 进入投递路径")
	if !eng.RemoveRule("cold") {
		t.Fatal("remove existing inactive rule must return true")
	}
	t.Log("删除 inactive 规则 cold -> 不产生任何结论")
	if eng.RemoveRule("nope") {
		t.Fatal("remove unknown rule must return false")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := eng.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	got := down.delivered()
	var fired, resolved int
	for _, e := range got {
		switch e.Type {
		case alert.EventFired:
			fired++
		case alert.EventResolved:
			resolved++
		}
	}
	t.Logf("最终送达: fired=%d resolved=%d（期望各 1，无零值/幽灵事件）", fired, resolved)
	if fired != 1 || resolved != 1 || len(got) != 2 {
		t.Fatalf("delivered = %+v, want exactly 1 fired + 1 resolved", got)
	}
}
