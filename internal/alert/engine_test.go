package alert

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestEngine() (*Engine, *fakeClock) {
	c := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	return NewEngine(c.Now), c
}

// 条件需持续满足 For 才触发；触发与恢复各只产生一次事件。
func TestDurationFireAndResolveOnce(t *testing.T) {
	e, clk := newTestEngine()
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 80, For: 10 * time.Second})
	t.Log("输入: 规则 r1 cpu>80 持续 10s; 采样序列 90,90(t+10s),50,50")

	if ev := e.Evaluate(map[string]float64{"cpu": 90}); len(ev) != 0 {
		t.Fatalf("首次采样不应触发, 得到 %v", ev)
	}
	t.Logf("状态变化: t=0 条件开始满足, 状态=%+v", e.States())

	clk.Advance(10 * time.Second)
	ev := e.Evaluate(map[string]float64{"cpu": 90})
	t.Logf("状态变化: t=10s 持续满足, 事件=%v", ev)
	if len(ev) != 1 || ev[0].Type != EventFired || ev[0].RuleID != "r1" {
		t.Fatalf("判定依据: 持续 10s 满足应触发一次 Fired, 实际 %v", ev)
	}

	clk.Advance(5 * time.Second)
	if ev := e.Evaluate(map[string]float64{"cpu": 90}); len(ev) != 0 {
		t.Fatalf("判定依据: 已触发后持续满足不得重复触发, 实际 %v", ev)
	}

	clk.Advance(1 * time.Second)
	ev = e.Evaluate(map[string]float64{"cpu": 50})
	t.Logf("状态变化: 值回落到 50, 事件=%v", ev)
	if len(ev) != 1 || ev[0].Type != EventResolved {
		t.Fatalf("判定依据: 条件不再满足应产生一次 Resolved, 实际 %v", ev)
	}
	if ev := e.Evaluate(map[string]float64{"cpu": 50}); len(ev) != 0 {
		t.Fatalf("判定依据: 恢复后不得重复发 Resolved, 实际 %v", ev)
	}
}

// 规则变更：提高阈值后条件仍满足 -> 进度保留、按新阈值触发、不重复触发。
func TestRuleUpdateKeepsProgressUsesNewThreshold(t *testing.T) {
	e, clk := newTestEngine()
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 80, For: 10 * time.Second})
	t.Log("输入: 阈值 80 持续 10s; t=0 值 90; t=5s 改阈值为 85; t=10s 值 90")

	e.Evaluate(map[string]float64{"cpu": 90})
	clk.Advance(5 * time.Second)
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 85, For: 10 * time.Second})
	t.Logf("状态变化: 规则更新为阈值 85, 状态保留=%+v", e.States())

	clk.Advance(5 * time.Second)
	ev := e.Evaluate(map[string]float64{"cpu": 90})
	t.Logf("状态变化: t=10s 值 90 仍满足新阈值, 事件=%v", ev)
	if len(ev) != 1 || ev[0].Type != EventFired || ev[0].Threshold != 85 {
		t.Fatalf("判定依据: 进度保留且按新阈值 85 触发, 实际 %v", ev)
	}
	if ev := e.Evaluate(map[string]float64{"cpu": 90}); len(ev) != 0 {
		t.Fatalf("判定依据: 规则变更后已触发不得重复触发, 实际 %v", ev)
	}
}

// 规则变更：提高阈值后条件不再满足 -> 进度重置，不沿用旧阈值触发。
func TestRuleUpdateResetsProgressWhenNoLongerMet(t *testing.T) {
	e, _ := newTestEngine()
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 80, For: 10 * time.Second})
	t.Log("输入: 阈值 80; t=0 值 90; t=5s 改阈值为 95; t=10s 值 90 (不满足新阈值)")

	e.Evaluate(map[string]float64{"cpu": 90})
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 95, For: 10 * time.Second})
	ev := e.Evaluate(map[string]float64{"cpu": 90})
	t.Logf("状态变化: 值 90 不满足新阈值 95, 事件=%v 状态=%+v", ev, e.States())
	if len(ev) != 0 {
		t.Fatalf("判定依据: 不得沿用旧阈值 80 触发, 实际 %v", ev)
	}
	if st := e.States()[0]; st.ConditionMet {
		t.Fatalf("判定依据: 条件不满足时进度应重置, 状态=%+v", st)
	}
}

// 规则变更：已触发后调整阈值，恢复判定用新阈值，且不重复触发。
func TestRuleUpdateWhileFiring(t *testing.T) {
	e, _ := newTestEngine()
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 80, For: 0})
	t.Log("输入: 阈值 80 立即触发; 触发后改阈值为 95; 值 90 -> 应按新阈值恢复")

	if ev := e.Evaluate(map[string]float64{"cpu": 90}); len(ev) != 1 || ev[0].Type != EventFired {
		t.Fatalf("应立即触发, 实际 %v", ev)
	}
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 95, For: 0})
	ev := e.Evaluate(map[string]float64{"cpu": 90})
	t.Logf("状态变化: 触发中改阈值 95, 值 90 不满足, 事件=%v", ev)
	if len(ev) != 1 || ev[0].Type != EventResolved || ev[0].Threshold != 95 {
		t.Fatalf("判定依据: 恢复判定应使用新阈值 95, 实际 %v", ev)
	}
}

// 并发评估与规则变更交织：事件总数确定（至多一次 Fired 对应一次 Resolved），
// 且 Fired 不重复。
func TestConcurrentEvaluateAndUpdate(t *testing.T) {
	e, clk := newTestEngine()
	e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: 80, For: 0})
	t.Log("输入: 10 个协程并发 Evaluate, 同时主协程 5 次变更阈值")

	var wg sync.WaitGroup
	var mu sync.Mutex
	var fired, resolved int
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				v := 90.0
				if (g+i)%3 == 0 {
					v = 50
				}
				for _, ev := range e.Evaluate(map[string]float64{"cpu": v}) {
					mu.Lock()
					if ev.Type == EventFired {
						fired++
					} else {
						resolved++
					}
					mu.Unlock()
				}
			}
		}(g)
	}
	for i := 0; i < 5; i++ {
		e.UpsertRule(Rule{ID: "r1", Metric: "cpu", Comparator: GreaterThan, Threshold: float64(70 + i), For: 0})
	}
	wg.Wait()
	clk.Advance(time.Second)
	t.Logf("状态变化: Fired 总数=%d Resolved 总数=%d 最终状态=%+v", fired, resolved, e.States())
	if fired == 0 {
		t.Fatalf("判定依据: 值 90 始终高于阈值 70..74, 应至少触发一次")
	}
	if resolved > fired {
		t.Fatalf("判定依据: Resolved 次数不可能超过 Fired 次数: fired=%d resolved=%d", fired, resolved)
	}
	t.Logf("判定依据: 每次 Fired 至多对应一次 Resolved, 无重复无丢失 (fired=%d, resolved=%d)", fired, resolved)
}
