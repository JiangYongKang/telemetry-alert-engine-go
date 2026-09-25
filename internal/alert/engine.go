// Package alert 提供支持运行期规则变更的告警判定引擎。
package alert

import (
	"sync"
	"time"
)

// Comparator 阈值比较方式。
type Comparator int

const (
	GreaterThan Comparator = iota
	LessThan
)

// Rule 是一条告警规则。
type Rule struct {
	ID         string
	Metric     string
	Comparator Comparator
	Threshold  float64
	For        time.Duration // 条件需持续满足的时长
}

// EventType 告警事件类型。
type EventType int

const (
	EventFired EventType = iota
	EventResolved
)

// Event 是一次告警状态迁移，仅在迁移时产生一次。
type Event struct {
	Type      EventType
	RuleID    string
	Value     float64
	Threshold float64
	At        time.Time
}

// State 是规则当前的判定状态（用于测试与可解释性）。
type State struct {
	RuleID        string
	Firing        bool
	ConditionMet  bool
	ConditionSince time.Time
}

// Engine 对指标快照按规则做持续时长判定。
type Engine struct {
	mu     sync.Mutex
	rules  map[string]Rule
	states map[string]*ruleState
	now    func() time.Time
}

type ruleState struct {
	firing         bool
	conditionMet   bool
	conditionSince time.Time
}

// NewEngine 创建引擎。now 用于注入时钟（测试可替换）。
func NewEngine(now func() time.Time) *Engine {
	return &Engine{
		rules:  make(map[string]Rule),
		states: make(map[string]*ruleState),
		now:    now,
	}
}

// UpsertRule 新增或更新规则。更新时保留判定进度（conditionSince 与 firing），
// 不重复触发，之后的判定立即使用新阈值；若新阈值下条件不再成立，
// 将在下一次 Evaluate 时按常规路径重置进度/解除告警。
func (e *Engine) UpsertRule(r Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules[r.ID] = r
	if _, ok := e.states[r.ID]; !ok {
		e.states[r.ID] = &ruleState{}
	}
}

// RemoveRule 删除规则并清除其状态。
func (e *Engine) RemoveRule(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.rules, id)
	delete(e.states, id)
}

// Evaluate 对每个规则用当前值做一次判定，返回本次产生的事件。
// values 为 metric 名 -> 当前值。同一时刻同一规则只产生唯一结论：
// 判定在互斥锁内串行进行，事件只在状态迁移时发出一次。
// 指标缺失视为条件不成立。
func (e *Engine) Evaluate(values map[string]float64) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	var events []Event
	for id, rule := range e.rules {
		st := e.states[id]
		v, ok := values[rule.Metric]
		met := ok && compare(rule.Comparator, v, rule.Threshold)
		if met {
			if !st.conditionMet {
				st.conditionMet = true
				st.conditionSince = now
			}
			if !st.firing && !now.Before(st.conditionSince.Add(rule.For)) {
				st.firing = true
				events = append(events, Event{Type: EventFired, RuleID: id, Value: v, Threshold: rule.Threshold, At: now})
			}
		} else {
			st.conditionMet = false
			if st.firing {
				st.firing = false
				events = append(events, Event{Type: EventResolved, RuleID: id, Value: v, Threshold: rule.Threshold, At: now})
			}
		}
	}
	return events
}

// States 返回所有规则的当前状态快照。
func (e *Engine) States() []State {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]State, 0, len(e.rules))
	for id := range e.rules {
		st := e.states[id]
		out = append(out, State{
			RuleID:         id,
			Firing:         st.firing,
			ConditionMet:   st.conditionMet,
			ConditionSince: st.conditionSince,
		})
	}
	return out
}

func compare(c Comparator, v, threshold float64) bool {
	if c == LessThan {
		return v < threshold
	}
	return v > threshold
}
