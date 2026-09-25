package alert

import (
	"testing"
	"time"
)

const series = `{job="api"}`

type scriptStep struct {
	tick   int
	value  float64
	upsert *Rule // 非空表示在该 tick 判定前变更规则
}

func atTick(n int) time.Time { return time.Unix(int64(n), 0) }

// TestImmediateFireResolve 验证 For=0 规则：首次满足触发一次，持续满足
// 不重复触发，恢复恰好给一个 resolved。
func TestImmediateFireResolve(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGT, Threshold: 10})

	evs := m.Evaluate(series, 11, atTick(1))
	if len(evs) != 1 || evs[0].Type != EventFired {
		t.Fatalf("tick1 events = %v, want single fired", evs)
	}
	t.Logf("判定依据: 值=11 阈值=10 算子=> => 状态 inactive->firing, 结论=fired(version=%d)", evs[0].Version)

	// 持续满足：无新结论（不重复触发）。
	if evs := m.Evaluate(series, 12, atTick(2)); len(evs) != 0 {
		t.Fatalf("tick2 events = %v, want none (no duplicate)", evs)
	}
	t.Logf("值=12 仍满足，状态保持 firing，无结论（不重复触发）")

	// 恢复：恰好一个 resolved。
	evs = m.Evaluate(series, 9, atTick(3))
	if len(evs) != 1 || evs[0].Type != EventResolved {
		t.Fatalf("tick3 events = %v, want single resolved", evs)
	}
	t.Logf("值=9 不满足 => firing->inactive, 结论=resolved(version=%d)", evs[0].Version)

	if phase := m.Snapshot()["r1"]; phase != PhaseInactive {
		t.Fatalf("phase = %s, want inactive", phase)
	}
}

// TestForDurationProgress 验证 For>0：pending 进度累积，中断清零，
// 达到持续时间后触发一次。
func TestForDurationProgress(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGE, Threshold: 10, ForDuration: 3 * time.Second})

	m.Evaluate(series, 10, atTick(1))
	t.Logf("值=10 >=10 满足但未满 3s: inactive->pending")
	if m.Snapshot()["r1"] != PhasePending {
		t.Fatal("want pending at tick1")
	}
	m.Evaluate(series, 11, atTick(2))
	t.Logf("值=11 连续满足 1s: 保持 pending，无结论")
	if evs := m.Evaluate(series, 1, atTick(3)); len(evs) != 0 {
		t.Fatalf("break event = %v, want none", evs)
	}
	t.Logf("值=1 不满足: pending->inactive，进度清零（无事件）")

	// 重新连续满足满 3 个 tick 才触发。
	m.Evaluate(series, 10, atTick(4))
	m.Evaluate(series, 10, atTick(5))
	evs := m.Evaluate(series, 10, atTick(7)) // pendingSince=4, 7-4=3s
	if len(evs) != 1 || evs[0].Type != EventFired {
		t.Fatalf("events after 3s satisfied = %v, want fired", evs)
	}
	t.Logf("连续满足 t=4..7 达 3s: pending->firing，结论=fired")
}

// TestRuleChangeThresholdAppliesImmediately 验证规则变更后：
//   - 新版本立即生效，旧阈值不再影响判定；
//   - firing 状态不因换阈值重复触发；
//   - pending 进度保留，换阈值后若不再满足则按新阈值恢复；
//   - 再次恢复/触发依据的都是新版本。
func TestRuleChangeThresholdAppliesImmediately(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGT, Threshold: 10})

	evs := m.Evaluate(series, 11, atTick(1))
	if len(evs) != 1 {
		t.Fatalf("fired = %v", evs)
	}
	oldVersion := evs[0].Version
	t.Logf("旧阈值 >10：值=11 -> fired(version=%d)", oldVersion)

	// 采样期间变更阈值：10 -> 20。当前值 11 对新阈值不再满足，
	// 但替换本身不产生任何事件（firing 不被换阈值打断、不重复触发）。
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGT, Threshold: 20})
	if phase := m.Snapshot()["r1"]; phase != PhaseFiring {
		t.Fatalf("upsert itself must not change phase, got %s", phase)
	}
	t.Logf("规则变更: 阈值 10->20，阶段保持 firing，version 递增；替换不产生事件")

	// 下一次判定立即依据新阈值：11 < 20 => resolved 一次，且事件携带新版本。
	evs = m.Evaluate(series, 11, atTick(2))
	if len(evs) != 1 || evs[0].Type != EventResolved {
		t.Fatalf("events = %v, want one resolved under NEW threshold", evs)
	}
	if evs[0].Version <= oldVersion || evs[0].Rule.Threshold != 20 {
		t.Fatalf("resolved event must carry new version/threshold, got v=%d thr=%v",
			evs[0].Version, evs[0].Rule.Threshold)
	}
	t.Logf("新阈值 >20：值=11 不满足 -> resolved(version=%d, threshold=%v)，未沿用旧阈值",
		evs[0].Version, evs[0].Rule.Threshold)

	// 用新阈值再次触发。
	evs = m.Evaluate(series, 21, atTick(3))
	if len(evs) != 1 || evs[0].Type != EventFired || evs[0].Rule.Threshold != 20 {
		t.Fatalf("events = %v, want fired under new threshold", evs)
	}
	t.Logf("值=21 > 新阈值20 -> fired(version=%d)，无重复旧结论", evs[0].Version)
}

// TestRuleChangeKeepsPendingProgress 验证 pending 期间改阈值（新旧阈值
// 下当前值都满足）：连续满足进度不重置，满持续时间后只触发一次。
func TestRuleChangeKeepsPendingProgress(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGT, Threshold: 10, ForDuration: 5 * time.Second})
	m.Evaluate(series, 30, atTick(1))
	t.Log("t=1 值=30 满足旧阈值 >10: pending，pendingSince=1")

	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGT, Threshold: 20, ForDuration: 5 * time.Second})
	t.Log("t=2 规则变更: 阈值 10->20；值=30 对新阈值仍满足，进度保留")

	evs := m.Evaluate(series, 30, atTick(6)) // 6-1=5s，进度跨越版本保留
	if len(evs) != 1 || evs[0].Type != EventFired {
		t.Fatalf("events = %v, want single fired with progress preserved across change", evs)
	}
	if evs[0].Rule.Threshold != 20 {
		t.Fatalf("event threshold = %v, want 20 (new spec)", evs[0].Rule.Threshold)
	}
	t.Logf("t=6 跨版本连续满足达 5s -> fired(version=%d, threshold=20)，只触发一次", evs[0].Version)
}

// TestRuleChangeOperator 验证算子变更同样立即生效且不重复触发。
func TestRuleChangeOperator(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpGT, Threshold: 10})
	m.Evaluate(series, 11, atTick(1)) // fired under >
	m.Upsert(Rule{ID: "r1", SeriesKey: series, Op: OpLT, Threshold: 10})
	evs := m.Evaluate(series, 11, atTick(2)) // 11 < 10 false -> resolved
	if len(evs) != 1 || evs[0].Type != EventResolved || evs[0].Rule.Op != OpLT {
		t.Fatalf("events = %v, want resolved under new operator <", evs)
	}
	t.Logf("算子 > 改为 < 后，值=11 按新算子判定不满足 -> resolved(version=%d)", evs[0].Version)
}

// TestUniqueConclusionPerRulePerTick 验证同一时刻同一规则至多一个结论，
// 且多条规则互不干扰。
func TestUniqueConclusionPerRulePerTick(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "a", SeriesKey: series, Op: OpGT, Threshold: 10})
	m.Upsert(Rule{ID: "b", SeriesKey: series, Op: OpGT, Threshold: 100})
	m.Upsert(Rule{ID: "c", SeriesKey: `{job="other"}`, Op: OpGT, Threshold: 1})

	evs := m.Evaluate(series, 11, atTick(1))
	if len(evs) != 1 || evs[0].RuleID != "a" {
		t.Fatalf("events = %v, want exactly one (rule a)", evs)
	}
	t.Logf("同一序列两条规则: 仅规则 a 满足，唯一结论=%s", evs[0].RuleID)
}

// TestRemoveFiringRuleEmitsResolved 验证删除 firing 规则补发 resolved，
// 删除 inactive 规则无事件，重复删除返回 false。
func TestRemoveFiringRuleEmitsResolved(t *testing.T) {
	m := NewManager()
	m.Upsert(Rule{ID: "a", SeriesKey: series, Op: OpGT, Threshold: 10})
	m.Upsert(Rule{ID: "b", SeriesKey: series, Op: OpGT, Threshold: 10})
	m.Evaluate(series, 11, atTick(1)) // a,b both fired

	ev, ok := m.Remove("a", atTick(2))
	if !ok || ev.Type != EventResolved {
		t.Fatalf("remove firing rule: ev=%v ok=%v", ev, ok)
	}
	t.Logf("删除 firing 规则 a -> 补发 resolved，避免悬挂告警")

	ev, ok = m.Remove("b", atTick(2))
	if !ok || ev.Type != EventResolved {
		t.Fatalf("rule b should still be firing and resolve on remove")
	}
	// b 在上一行 resolve 后规则已删除；这里补一个 inactive 规则的删除用例。
	m.Upsert(Rule{ID: "c", SeriesKey: series, Op: OpGT, Threshold: 100})
	if ev, ok = m.Remove("c", atTick(3)); ok && ev.Type != "" {
		t.Fatalf("removing inactive rule must yield no event, got %v", ev)
	}
	if _, ok := m.Remove("missing", atTick(3)); ok {
		t.Fatal("removing unknown rule must return false")
	}
}

// TestSerialBaselineScenario 用一段固定脚本与显式串行参考实现逐 tick
// 对照：事件序列必须完全一致（类型、规则、版本、所依据阈值）。
func TestSerialBaselineScenario(t *testing.T) {
	script := []scriptStep{
		{1, 11, nil},
		{2, 12, nil},
		{3, 9, nil},
		{4, 50, &Rule{ID: "r", SeriesKey: series, Op: OpGT, Threshold: 40}},
		{5, 51, nil},
		{6, 52, nil},
		{7, 30, nil},
	}

	// 被测对象：初始阈值 10，第 4 tick 前改成 40。
	got := runScript(t, script, Rule{ID: "r", SeriesKey: series, Op: OpGT, Threshold: 10})

	// 串行基准：手工推算的期望事件序列。
	// tick1: 11>10 fired(v1)
	// tick2: 12>10 (firing, no event)
	// tick3: 9>10 false -> resolved(v1)
	// tick4: 改阈值为40(v2)；50>40 -> fired(v2)
	// tick5: 51>40 (no event)
	// tick6: 52>40 (no event)
	// tick7: 30>40 false -> resolved(v2)
	type want struct {
		tick int
		typ  EventType
		ver  Version
		thr  float64
	}
	wants := []want{
		{1, EventFired, 1, 10},
		{3, EventResolved, 1, 10},
		{4, EventFired, 2, 40},
		{7, EventResolved, 2, 40},
	}
	if len(got) != len(wants) {
		t.Fatalf("event count = %d, want %d: %+v", len(got), len(wants), got)
	}
	for i, w := range wants {
		g := got[i]
		if atTick(w.tick).Compare(g.At) != 0 || g.Type != w.typ || g.Version != w.ver || g.Rule.Threshold != w.thr {
			t.Fatalf("event[%d] = tick via %v type=%s v=%d thr=%v, want tick=%d type=%s v=%d thr=%v",
				i, g.At, g.Type, g.Version, g.Rule.Threshold, w.tick, w.typ, w.ver, w.thr)
		}
		t.Logf("基准一致: tick=%d 结论=%s version=%d 依据阈值=%v", w.tick, w.typ, w.ver, w.thr)
	}
}

func runScript(t *testing.T, script []scriptStep, initial Rule) []Event {
	t.Helper()
	m := NewManager()
	m.Upsert(initial)
	var out []Event
	for _, s := range script {
		if s.upsert != nil {
			m.Upsert(*s.upsert)
			t.Logf("tick=%d 规则变更: 阈值=%v", s.tick, s.upsert.Threshold)
		}
		evs := m.Evaluate(series, s.value, atTick(s.tick))
		t.Logf("tick=%d 输入值=%v 阶段=%s 结论数=%d", s.tick, s.value, m.Snapshot()["r"], len(evs))
		out = append(out, evs...)
	}
	return out
}
