// Package metrics 提供带基数上限的并发指标存储。
//
// 同一时间序列（由规范化后的标签键标识）的并发写入在存储层串行化，
// 因此累积结果与串行执行等价；当活动时间序列数达到上限时，新序列的
// 写入会以可识别的错误被拒绝，而不会被静默丢弃。
package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LabelPair 是一个标签键值对。
type LabelPair struct {
	Key   string
	Value string
}

// Labels 是按键排序后的标签集合，唯一标识一条时间序列。
type Labels struct {
	pairs []LabelPair
}

// Sample 是一次指标写入请求。Add 为 true 时按计数器累加，否则按最新值覆盖。
type Sample struct {
	Labels Labels
	Value  float64
	Add    bool
}

// CardinalityLimitError 表示因时间序列基数达到上限而拒绝一次写入。
//
// 调用方可通过 errors.As 明确识别该原因，从而决定重试、告警或记录，
// 数据不会被悄悄丢弃。
type CardinalityLimitError struct {
	Limit     int
	SeriesKey string
}

func (e *CardinalityLimitError) Error() string {
	return fmt.Sprintf("cardinality limit reached: %d active series, new series %q rejected",
		e.Limit, e.SeriesKey)
}

// Is 保证错误包装后仍可被 errors.Is(err, ErrCardinalityLimit) 识别。
func (e *CardinalityLimitError) Is(target error) bool {
	_, ok := target.(*CardinalityLimitError)
	return ok
}

// Store 是并发安全的指标存储。
//
// 取舍规则（确定且可复现）：基数上限只限制“新序列”。已存在的序列
// 始终接受写入；新序列在活动序列数已达到 maxCard 时一律拒绝，与
// 到达顺序无关、不做随机淘汰，因此相同输入序列集合下结果可复现。
type Store struct {
	mu       sync.Mutex
	series   map[string]float64
	maxCard  int
	rejected int64
}

// NewStore 创建一个最多容纳 maxCard 条活动时间序列的存储。
// maxCard 必须为正数。
func NewStore(maxCard int) *Store {
	if maxCard <= 0 {
		panic(fmt.Sprintf("metrics: maxCard must be positive, got %d", maxCard))
	}
	return &Store{
		series:  make(map[string]float64),
		maxCard: maxCard,
	}
}

// LabelsFromMap 从 map 构造按键排序的标签集合。
func LabelsFromMap(m map[string]string) Labels {
	pairs := make([]LabelPair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, LabelPair{Key: k, Value: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
	return Labels{pairs: pairs}
}

// String 返回标签的规范字符串形式，如 {job="api",region="cn"}。
func (l Labels) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, p := range l.pairs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(p.Key)
		b.WriteString("=")
		b.WriteString(strconv.Quote(p.Value))
	}
	b.WriteByte('}')
	return b.String()
}

// Key 返回时间序列的规范化键。
func (l Labels) Key() string { return l.String() }

// Write 执行一次写入。
//
// 全部序列共用一把互斥锁，且“读改写”在同一临界区内完成，因此同一
// 时间序列的任意并发 Add 都被线性化，N 次累加后的结果与按某种顺序
// 串行执行完全一致。
func (s *Store) Write(samp Sample) error {
	key := samp.Labels.Key()

	s.mu.Lock()
	defer s.mu.Unlock()

	if cur, ok := s.series[key]; ok {
		// 已存在序列：永远接受，基数上限不适用于更新。
		if samp.Add {
			s.series[key] = cur + samp.Value
		} else {
			s.series[key] = samp.Value
		}
		return nil
	}

	// 新序列：达到上限则确定性拒绝。
	if len(s.series) >= s.maxCard {
		s.rejected++
		return &CardinalityLimitError{Limit: s.maxCard, SeriesKey: key}
	}

	s.series[key] = samp.Value
	return nil
}

// Snapshot 返回当前所有时间序列键到值的拷贝，调用方可在锁外长期持有。
func (s *Store) Snapshot() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]float64, len(s.series))
	for k, v := range s.series {
		out[k] = v
	}
	return out
}

// Cardinality 返回当前活动时间序列数。
func (s *Store) Cardinality() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.series)
}

// RejectedCount 返回因基数上限被拒绝的写入次数。
func (s *Store) RejectedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rejected
}
