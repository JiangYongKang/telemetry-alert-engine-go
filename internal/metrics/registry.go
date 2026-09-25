// Package metrics 提供并发安全的时间序列指标注册表。
package metrics

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

// ErrCardinalityLimit 表示标签基数（时间序列数量）超过上限，写入被拒绝。
var ErrCardinalityLimit = errors.New("metrics: series cardinality limit exceeded")

// Labels 是一个时间序列的标签集合。
type Labels map[string]string

// Series 是单个时间序列的累积状态。
type Series struct {
	Name   string
	Labels Labels
	Value  float64
	Count  int64
}

// Registry 保存所有时间序列，支持并发写入。
type Registry struct {
	mu        sync.Mutex
	series    map[string]*Series
	maxSeries int
}

// NewRegistry 创建一个注册表，maxSeries 为时间序列数量上限（<=0 表示不限制）。
func NewRegistry(maxSeries int) *Registry {
	return &Registry{series: make(map[string]*Series), maxSeries: maxSeries}
}

// Add 向指定时间序列累加 value。新序列超出基数上限时返回 ErrCardinalityLimit。
// 已存在的序列永远可写；取舍规则确定：只拒绝"新"序列，不驱逐旧序列。
func (r *Registry) Add(name string, labels Labels, value float64) error {
	k := key(name, labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.series[k]
	if !ok {
		if r.maxSeries > 0 && len(r.series) >= r.maxSeries {
			return ErrCardinalityLimit
		}
		cp := make(Labels, len(labels))
		for lk, lv := range labels {
			cp[lk] = lv
		}
		s = &Series{Name: name, Labels: cp}
		r.series[k] = s
	}
	s.Value += value
	s.Count++
	return nil
}

// Snapshot 返回所有序列的确定性快照（按序列键排序）。
func (r *Registry) Snapshot() []Series {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.series))
	for k := range r.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Series, 0, len(keys))
	for _, k := range keys {
		s := r.series[k]
		cp := make(Labels, len(s.Labels))
		for lk, lv := range s.Labels {
			cp[lk] = lv
		}
		out = append(out, Series{Name: s.Name, Labels: cp, Value: s.Value, Count: s.Count})
	}
	return out
}

// key 生成序列的规范键（标签按键名排序拼接，保证确定性）。
func key(name string, labels Labels) string {
	var b strings.Builder
	b.WriteString(name)
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString("|")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}
