// Package alert 实现可在持续采样期间变更规则的告警判定引擎。
//
// 每条规则维护独立状态（阶段 + 连续满足起始时间）。规则定义以
// 版本化方式原子替换：自替换时刻起，所有判定一律引用新版本（含新
// 阈值/算子），旧阈值不再可能影响任何结论；状态进度（阶段、连续
// 满足起始时间）在变更中保留，因此“已持续满足”的判定不会因换阈值
// 而丢失进度或重复触发；从 firing 退出时恰好给出一个 resolved。
package alert

import (
	"fmt"
	"sync"
	"time"
)

// Operator 是阈值比较算子。
type Operator string

const (
	OpGT Operator = ">"
	OpGE Operator = ">="
	OpLT Operator = "<"
	OpLE Operator = "<="
	OpEQ Operator = "=="
)

// Rule 是一条告警规则的定义。
type Rule struct {
	ID          string
	SeriesKey   string // 所评估的时间序列键
	Op          Operator
	Threshold   float64
	ForDuration time.Duration // 需连续满足多久才触发；0 表示立即触发
}

// Version 单调递增，表示规则定义的第几版（第 1 次加入为 1）。
type Version int64

// Phase 是规则的判定阶段。
type Phase string

const (
	PhaseInactive Phase = "inactive" // 当前不满足
	PhasePending  Phase = "pending"  // 已满足但持续时间不足
	PhaseFiring   Phase = "firing"   // 已触发，尚未恢复
)

// EventType 是告警事件类型。
type EventType string

const (
	EventFired    EventType = "fired"
	EventResolved EventType = "resolved"
)

// Event 是一次告警状态转换的结论。
type Event struct {
	Type      EventType
	RuleID    string
	SeriesKey string
	Value     float64
	Rule      Rule // 产生该结论时所依据的规则定义（版本阈值）
	Version   Version
	At        time.Time
}

// ruleState 是单条规则的完整状态：定义版本 + 判定进度。
type ruleState struct {
	spec    Rule
	version Version

	phase        Phase
	pendingSince time.Time // phase == pending 时，连续满足的起始时间（零值表示尚未开始）
	lastValue    float64
	lastEvalAt   time.Time
}

// Manager 管理规则定义与其判定状态，并发安全。
type Manager struct {
	mu      sync.Mutex
	rules   map[string]*ruleState
	nextVer Version
}

// NewManager 创建空的规则管理器。
func NewManager() *Manager {
	return &Manager{rules: make(map[string]*ruleState), nextVer: 1}
}

// Upsert 原子地新增或替换规则定义。
//
// 语义：
//   - 新规则以 inactive、版本 1 加入；
//   - 已存在规则的 spec（含阈值/算子/持续时长/序列键）被整体替换，
//     版本号 +1；阶段与连续满足起始时间保留（不丢进度），后续判定
//     立即依据新定义（不沿用旧阈值）；
//   - 替换本身不产生任何事件，因此“已触发”的规则不会因调整重复触发。
func (m *Manager) Upsert(r Rule) {
	if r.ID == "" {
		panic("alert: rule ID must not be empty")
	}
	if _, ok := compareOps[r.Op]; !ok {
		panic(fmt.Sprintf("alert: unknown operator %q for rule %q", r.Op, r.ID))
	}
	if r.ForDuration < 0 {
		panic(fmt.Sprintf("alert: negative ForDuration for rule %q", r.ID))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if st, ok := m.rules[r.ID]; ok {
		st.spec = r
		st.version = m.bumpVersionLocked()
		return
	}
	m.rules[r.ID] = &ruleState{
		spec:    r,
		version: m.bumpVersionLocked(),
		phase:   PhaseInactive,
	}
}

// Remove 删除规则；若规则处于 firing，补发一个 resolved 结论，
// 保证“已触发”必然有对应“已恢复”，不会留下悬挂告警。
// 规则不存在时返回 false。
func (m *Manager) Remove(id string, at time.Time) (Event, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.rules[id]
	if !ok {
		return Event{}, false
	}
	var ev Event
	if st.phase == PhaseFiring {
		ev = Event{
			Type:      EventResolved,
			RuleID:    st.spec.ID,
			SeriesKey: st.spec.SeriesKey,
			Value:     st.lastValue,
			Rule:      st.spec,
			Version:   st.version,
			At:        at,
		}
	}
	delete(m.rules, id)
	return ev, true
}

// Evaluate 对指定时间序列的当前值推进所有相关规则的状态机。
//
// 同一次调用对同一规则至多产生一个结论，因此同一时刻对同一规则的
// 结论是唯一的。返回值中 Event 的零值不会出现；无状态转换时该规则
// 不贡献事件。
func (m *Manager) Evaluate(seriesKey string, value float64, at time.Time) []Event {
	m.mu.Lock()
	defer m.mu.Unlock()

	var events []Event
	for _, st := range m.rules {
		if st.spec.SeriesKey != seriesKey {
			continue
		}
		// 始终引用本次判定时刻最新的 spec：阈值变更后不会沿用旧阈值。
		if ev, ok := st.advanceLocked(st.spec, value, at); ok {
			events = append(events, ev)
		}
	}
	return events
}

// Snapshot 返回规则 ID 到阶段的诊断视图。
func (m *Manager) Snapshot() map[string]Phase {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Phase, len(m.rules))
	for id, st := range m.rules {
		out[id] = st.phase
	}
	return out
}

func (m *Manager) bumpVersionLocked() Version {
	v := m.nextVer
	m.nextVer++
	return v
}

// advanceLocked 在持有 m.mu 的前提下推进单条规则。
func (st *ruleState) advanceLocked(spec Rule, value float64, at time.Time) (Event, bool) {
	matched := compare(spec.Op, value, spec.Threshold)
	st.lastValue = value
	st.lastEvalAt = at

	switch st.phase {
	case PhaseInactive:
		if matched {
			if spec.ForDuration == 0 {
				st.phase = PhaseFiring
				st.pendingSince = at
				return st.eventLocked(EventFired, value, at), true
			}
			st.phase = PhasePending
			st.pendingSince = at
		}
		return Event{}, false

	case PhasePending:
		// pending 期间阈值被替换：matched 已按新定义计算。
		if !matched {
			// 恢复：进度清零，回到 inactive（pending -> inactive 无事件）。
			st.phase = PhaseInactive
			st.pendingSince = time.Time{}
			return Event{}, false
		}
		if at.Sub(st.pendingSince) >= spec.ForDuration {
			// 连续满足（跨版本）持续达到要求：恰好触发一次。
			st.phase = PhaseFiring
			return st.eventLocked(EventFired, value, at), true
		}
		return Event{}, false

	case PhaseFiring:
		if !matched {
			// 恢复：恰好给出一个 resolved。
			st.phase = PhaseInactive
			st.pendingSince = time.Time{}
			return st.eventLocked(EventResolved, value, at), true
		}
		return Event{}, false
	}
	return Event{}, false
}

func (st *ruleState) eventLocked(typ EventType, value float64, at time.Time) Event {
	return Event{
		Type:      typ,
		RuleID:    st.spec.ID,
		SeriesKey: st.spec.SeriesKey,
		Value:     value,
		Rule:      st.spec,
		Version:   st.version,
		At:        at,
	}
}

var compareOps = map[Operator]func(a, b float64) bool{
	OpGT: func(a, b float64) bool { return a > b },
	OpGE: func(a, b float64) bool { return a >= b },
	OpLT: func(a, b float64) bool { return a < b },
	OpLE: func(a, b float64) bool { return a <= b },
	OpEQ: func(a, b float64) bool { return a == b },
}

func compare(op Operator, value, threshold float64) bool {
	f, ok := compareOps[op]
	if !ok {
		// 构造时已校验，理论上不可达。
		return false
	}
	return f(value, threshold)
}
