// Command demo 在本地演示并发写入、规则变更与写出背压交织的完整流程：
//
//  1. 多个 goroutine 并发累加同一时间序列（累积结果与串行一致）；
//  2. 超过基数上限的新序列写入被明确拒绝并打印原因；
//  3. 采样中途修改告警阈值，已触发不重复、进度不丢、新阈值立即生效；
//  4. 下游短暂故障产生 Busy 背压，恢复后滞留结论按序补投，不丢不重。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/alert"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/engine"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/metrics"
	"github.com/highcumontoa/telemetry-alert-engine-go/pkg/sink"
)

type printingDownstream struct {
	mu      sync.Mutex
	fail    bool
	written int
}

func (d *printingDownstream) Write(_ context.Context, events [][]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail {
		return errors.New("demo downstream temporarily unavailable")
	}
	for _, b := range events {
		var ev alert.Event
		if err := json.Unmarshal(b, &ev); err != nil {
			return err
		}
		d.written++
		fmt.Printf("  [下游已写出#%d] %-8s rule=%-8s value=%-6.0f 判定依据=%s%v(version=%d) at=%s\n",
			d.written, ev.Type, ev.RuleID, ev.Value, ev.Rule.Op, ev.Rule.Threshold, ev.Version,
			ev.At.Format("15:04:05.000"))
	}
	return nil
}

func (d *printingDownstream) setFail(v bool) {
	d.mu.Lock()
	d.fail = v
	d.mu.Unlock()
}

func waitPhase(eng *engine.Engine, want alert.Phase, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p, ok := phaseOf(eng, "cpu-hot"); ok && p == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func phaseOf(eng *engine.Engine, id string) (alert.Phase, bool) {
	p, ok := eng.RulePhases()[id]
	return p, ok
}

func waitWritten(d *printingDownstream, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		got := d.written
		d.mu.Unlock()
		if got >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func main() {
	const maxCard, workers, perWorker = 2, 8, 200

	store := metrics.NewStore(maxCard)
	rules := alert.NewManager()
	down := &printingDownstream{}
	writer := sink.NewWriter(down,
		sink.WithCapacity(1), // 容量 1：下游一故障，下一条结论立即得到 Busy
		sink.WithBackoff(time.Millisecond, 20*time.Millisecond),
	)
	eng := engine.New(store, rules, writer, 20*time.Millisecond)

	counter := metrics.LabelsFromMap(map[string]string{"metric": "counter"})
	cpu := metrics.LabelsFromMap(map[string]string{"metric": "cpu"})

	eng.UpsertRule(alert.Rule{ID: "counter-guard", SeriesKey: counter.Key(),
		Op: alert.OpGT, Threshold: 1e18})
	eng.UpsertRule(alert.Rule{ID: "cpu-hot", SeriesKey: cpu.Key(), Op: alert.OpGE,
		Threshold: 80, ForDuration: 60 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eng.Start(ctx)

	// 占满基数名额，使后续新键的拒绝结果确定可复现。
	eng.Ingest(metrics.Sample{Labels: counter, Value: 0})
	eng.Ingest(metrics.Sample{Labels: cpu, Value: 0})

	// ---- 阶段 1：并发累加 + 基数拒绝 ----
	fmt.Printf("1) 并发写入: %d 个 goroutine 各对同一计数器 Add(1) %d 次（串行基准=%d）\n",
		workers, perWorker, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				eng.Ingest(metrics.Sample{Labels: counter, Value: 1, Add: true})
			}
		}()
	}
	rejected := 0
	for i := 0; i < 3; i++ {
		overflow := metrics.LabelsFromMap(map[string]string{"metric": "new", "i": fmt.Sprint(i)})
		if r := eng.Ingest(metrics.Sample{Labels: overflow, Value: 1}); !r.Accepted {
			rejected++
			fmt.Printf("   拒绝写入 %s -> 原因: %s（可识别、未静默丢弃）\n", overflow.Key(), r.Reason)
		}
	}
	wg.Wait()

	// ---- 阶段 2：连续满足 -> fired 恰好一次 ----
	fmt.Println("2) CPU 连续 >=80 达 60ms：firing 后持续满足不得重复触发")
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(15 * time.Millisecond)
		defer t.Stop()
		v := 85.0
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				eng.Ingest(metrics.Sample{Labels: cpu, Value: v})
			}
		}
	}()
	if !waitPhase(eng, alert.PhaseFiring, time.Second) {
		panic("cpu never reached firing")
	}
	fmt.Println("   状态变化: inactive -> pending -> firing（fired 已产生，持续期间无重复结论）")
	if !waitWritten(down, 1, time.Second) {
		panic("fired not written")
	}

	// 停止后台采样输入，此后 CPU 序列值完全由主线程控制（便于解释判定依据）。
	close(stop)

	// ---- 阶段 3：下游故障中变更阈值并恢复 ----
	down.setFail(true)
	fmt.Println("3) 下游故障中变更阈值 80->99（新版 ForDuration=0）：")
	time.Sleep(40 * time.Millisecond) // 确保故障对 worker 生效
	eng.UpsertRule(alert.Rule{ID: "cpu-hot", SeriesKey: cpu.Key(), Op: alert.OpGE,
		Threshold: 99})
	// 95 < 99 -> resolved：进入容量为 1 的队列，worker 写出失败开始退避。
	eng.Ingest(metrics.Sample{Labels: cpu, Value: 95})
	if !waitPhase(eng, alert.PhaseInactive, time.Second) {
		panic("cpu not resolved under new threshold")
	}
	// 100 >= 99（ForDuration=0）-> fired：队列仍被未送达的 resolved 占住，
	// 本条必然得到 Busy 并滞留引擎，等待恢复后按序补投。
	eng.Ingest(metrics.Sample{Labels: cpu, Value: 100})
	if !waitPhase(eng, alert.PhaseFiring, time.Second) {
		panic("cpu not firing at 100")
	}
	time.Sleep(50 * time.Millisecond) // 等 worker 若干轮失败重试
	fmt.Printf("   resolved 占住队列、fired 被拒: 写出器 busy=%d（明确背压），引擎滞留=%d\n",
		writer.Stats().Busy, eng.PendingCount())

	down.setFail(false)
	fmt.Println("4) 下游恢复：resolved、fired 按序补投，均恰好写出一次")

	// 恢复后两条结论按序补投：resolved（#2）、fired（#3）。
	if !waitWritten(down, 3, 2*time.Second) {
		panic("stuck events not flushed after recovery")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	undelivered, err := eng.Shutdown(shutdownCtx)
	if err != nil {
		fmt.Printf("   shutdown: 未送达=%d err=%v\n", len(undelivered), err)
	}

	fmt.Printf("\n结果核对:\n")
	counterVal := int(store.Snapshot()[counter.Key()])
	match := "一致"
	if counterVal != workers*perWorker {
		match = "不一致"
	}
	fmt.Printf("  计数器并发累加=%d（串行基准 %d，%s）\n", counterVal, workers*perWorker, match)
	fmt.Printf("  基数=%d（上限 %d），演示拒绝=%d，store.RejectedCount=%d\n",
		store.Cardinality(), maxCard, rejected, store.RejectedCount())
	st := writer.Stats()
	fmt.Printf("  写出器: accepted=%d busy=%d abandoned=%d delivered=%d undelivered=%d\n",
		st.Accepted, st.Busy, st.Abandoned, st.Delivered, st.Undelivered)
	fmt.Printf("  确定性放弃(显式记录原因): %d 条\n", len(eng.Dropped()))
}
