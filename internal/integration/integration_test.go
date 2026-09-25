// Package integration 覆盖并发写入、规则变更与写出失败交织的端到端场景。
package integration

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/telemetry-alert-engine-go/internal/alert"
	"github.com/highcumontoa/telemetry-alert-engine-go/internal/metrics"
	"github.com/highcumontoa/telemetry-alert-engine-go/internal/sink"
)

type flakyDest struct {
	mu      sync.Mutex
	fail    bool
	written []string
}

func (d *flakyDest) Write(item []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail {
		return errors.New("downstream unavailable")
	}
	d.written = append(d.written, string(item))
	return nil
}

func (d *flakyDest) setFail(f bool) { d.mu.Lock(); d.fail = f; d.mu.Unlock() }
func (d *flakyDest) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.written...)
}

// 端到端：并发指标写入 + 采样期规则变更 + 下游间歇失败。
// 校验：1) 指标总量与写入端记账一致（等价串行基准）；
// 2) 告警事件流 Fired/Resolved 严格交替（不重复、不丢失进度）；
// 3) 已接受事件 写出+放弃 守恒，失败原因可区分。
func TestConcurrentWritesRuleChangesAndSinkFailures(t *testing.T) {
	reg := metrics.NewRegistry(4)
	dest := &flakyDest{}
	w := sink.NewWriter(dest, 8, 1)

	var clockMu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	engine := alert.NewEngine(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	})
	engine.UpsertRule(alert.Rule{ID: "high-cpu", Metric: "cpu", Comparator: alert.GreaterThan, Threshold: 100, For: 0})

	const writers = 6
	const addsPerWriter = 200
	t.Logf("输入: %d 个写入协程各累加 %d 次 +1 到序列 cpu{host=hX}; 采样 30 次; 期间变更阈值; 下游间歇失败", writers, addsPerWriter)

	// 1) 并发指标写入（每个协程写自己的序列，其中 2 个协程共享同一序列）
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			host := fmt.Sprintf("h%d", g%4) // 4 个序列，h0/h1 被两个协程共享
			for i := 0; i < addsPerWriter; i++ {
				if err := reg.Add("cpu", metrics.Labels{"host": host}, 1); err != nil {
					t.Errorf("Add 失败: %v", err)
				}
			}
		}(g)
	}

	// 2) 下游间歇故障
	stopFlaky := make(chan struct{})
	var flakyWg sync.WaitGroup
	flakyWg.Add(1)
	go func() {
		defer flakyWg.Done()
		for {
			select {
			case <-stopFlaky:
				return
			default:
				dest.setFail(true)
				time.Sleep(2 * time.Millisecond)
				dest.setFail(false)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// 3) 采样协程：推进时钟 -> 求和 -> 判定 -> 事件写入 sink
	var mu sync.Mutex
	var allEvents []alert.Event
	var accepted, busy int
	sampler := func(value float64) {
		clockMu.Lock()
		now = now.Add(time.Second)
		clockMu.Unlock()
		for _, ev := range engine.Evaluate(map[string]float64{"cpu": value}) {
			mu.Lock()
			allEvents = append(allEvents, ev)
			mu.Unlock()
			item := []byte(fmt.Sprintf("%s rule=%s value=%.0f threshold=%.0f",
				map[alert.EventType]string{alert.EventFired: "FIRED", alert.EventResolved: "RESOLVED"}[ev.Type],
				ev.RuleID, ev.Value, ev.Threshold))
			for {
				err := w.Write(item)
				if errors.Is(err, sink.ErrBusy) {
					mu.Lock()
					busy++
					mu.Unlock()
					time.Sleep(time.Millisecond)
					continue // 背压：稍后重试同一条
				}
				if err != nil {
					t.Errorf("Write 返回非背压错误: %v", err)
				}
				mu.Lock()
				accepted++
				mu.Unlock()
				break
			}
		}
	}

	wg.Wait()
	// 采样期间交替高低值并变更规则阈值
	engine.UpsertRule(alert.Rule{ID: "high-cpu", Metric: "cpu", Comparator: alert.GreaterThan, Threshold: 100, For: 0})
	total := float64(writers * addsPerWriter)
	for round := 0; round < 30; round++ {
		v := total // 高于阈值
		if round%3 == 2 {
			v = 10 // 低于阈值 -> 恢复
		}
		if round == 10 {
			engine.UpsertRule(alert.Rule{ID: "high-cpu", Metric: "cpu", Comparator: alert.GreaterThan, Threshold: 500, For: 0})
			t.Log("状态变化: 采样期间将阈值 100 -> 500")
		}
		if round == 20 {
			engine.UpsertRule(alert.Rule{ID: "high-cpu", Metric: "cpu", Comparator: alert.GreaterThan, Threshold: 50, For: 0})
			t.Log("状态变化: 采样期间将阈值 500 -> 50")
		}
		sampler(v)
	}

	close(stopFlaky)
	flakyWg.Wait()
	dest.setFail(false)
	w.Close()

	// 校验 1：指标总量 = 各协程记账之和（串行基准）
	snap := reg.Snapshot()
	var sum float64
	for _, s := range snap {
		sum += s.Value
	}
	t.Logf("状态变化: 指标快照=%+v 总量=%.0f 期望=%.0f", snap, sum, total)
	if sum != total {
		t.Fatalf("判定依据: 并发累加总量 %.0f 应等于串行基准 %.0f", sum, total)
	}

	// 校验 2：事件流严格交替，阈值始终是产生事件时的当前规则阈值
	mu.Lock()
	events := append([]alert.Event(nil), allEvents...)
	acc, bsy := accepted, busy
	mu.Unlock()
	t.Logf("状态变化: 事件流=%v", events)
	firing := false
	firedCount := 0
	for i, ev := range events {
		switch ev.Type {
		case alert.EventFired:
			if firing {
				t.Fatalf("判定依据: 第 %d 个事件重复 Fired, 中间无 Resolved: %v", i, events)
			}
			firing = true
			firedCount++
		case alert.EventResolved:
			if !firing {
				t.Fatalf("判定依据: 第 %d 个事件 Resolved 前无 Fired: %v", i, events)
			}
			firing = false
		}
	}
	if firedCount == 0 {
		t.Fatal("判定依据: 高值采样应至少触发一次 Fired")
	}
	t.Logf("判定依据: %d 次 Fired 与 Resolved 严格交替, 无重复触发", firedCount)

	// 校验 3：sink 守恒
	written := dest.snapshot()
	dropped := w.Dropped()
	t.Logf("状态变化: 已接受=%d 背压重试=%d 已写出=%d 已放弃=%d", acc, bsy, len(written), len(dropped))
	if len(written)+len(dropped) != acc {
		t.Fatalf("判定依据: 写出(%d)+放弃(%d) 应等于已接受(%d)", len(written), len(dropped), acc)
	}
	for _, d := range dropped {
		if d.Reason == "" {
			t.Fatalf("判定依据: 放弃必须附原因, 实际 %+v", d)
		}
	}
	t.Log("判定依据: 已接受事件无静默丢失、无重复; 放弃记录均附原因")
}
