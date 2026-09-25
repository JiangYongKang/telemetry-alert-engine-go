package metrics

import (
	"errors"
	"sync"
	"testing"
)

// TestLabelsCanonicalKey 验证标签规范化：map 顺序无关，重复构造得到同一键。
func TestLabelsCanonicalKey(t *testing.T) {
	a := LabelsFromMap(map[string]string{"region": "cn", "job": "api"})
	b := LabelsFromMap(map[string]string{"job": "api", "region": "cn"})
	if a.Key() != b.Key() {
		t.Fatalf("canonical keys differ: %q vs %q", a.Key(), b.Key())
	}
	want := `{job="api",region="cn"}`
	if a.Key() != want {
		t.Fatalf("key = %q, want %q", a.Key(), want)
	}
	t.Logf("输入标签 region=cn,job=api -> 规范化键 %s", a.Key())
}

// TestConcurrentAddMatchesSerial 验证同一时间序列的并发累加与串行基准一致。
func TestConcurrentAddMatchesSerial(t *testing.T) {
	const goroutines, perG = 16, 1000
	store := NewStore(4)
	lbl := LabelsFromMap(map[string]string{"job": "api"})

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// 混合 Add 与 Set 会使基准定义复杂，这里纯 Add 验证可加性。
				if err := store.Write(Sample{Labels: lbl, Value: 1, Add: true}); err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	got := store.Snapshot()[lbl.Key()]
	var want float64 = goroutines * perG
	t.Logf("输入: %d 个 goroutine 各 Add(1) %d 次；并发结果=%v，串行基准=%v",
		goroutines, perG, got, want)
	if got != want {
		t.Fatalf("concurrent accumulate = %v, want serial-equivalent %v", got, want)
	}
}

// TestConcurrentMixedSeries 验证多序列并发下各序列独立、互不串值。
func TestConcurrentMixedSeries(t *testing.T) {
	const seriesN, perSeries = 8, 500
	store := NewStore(seriesN)

	var wg sync.WaitGroup
	for s := 0; s < seriesN; s++ {
		key := LabelsFromMap(map[string]string{"s": string(rune('a' + s))})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSeries; i++ {
				if err := store.Write(Sample{Labels: key, Value: 2, Add: true}); err != nil {
					t.Errorf("write %s: %v", key.Key(), err)
				}
			}
		}()
	}
	wg.Wait()

	snap := store.Snapshot()
	if len(snap) != seriesN {
		t.Fatalf("cardinality = %d, want %d", len(snap), seriesN)
	}
	for k, v := range snap {
		var want float64 = 2 * perSeries
		t.Logf("序列 %s 状态: 值=%v 期望=%v", k, v, want)
		if v != want {
			t.Fatalf("series %s = %v, want %v", k, v, want)
		}
	}
}

// TestCardinalityLimitDeterministic 验证基数上限：新序列被明确拒绝且
// 原因可识别，已有序列仍可更新，拒绝计数准确，拒绝不改变状态。
func TestCardinalityLimitDeterministic(t *testing.T) {
	store := NewStore(2)
	k1 := LabelsFromMap(map[string]string{"id": "1"})
	k2 := LabelsFromMap(map[string]string{"id": "2"})
	k3 := LabelsFromMap(map[string]string{"id": "3"})

	for _, k := range []Labels{k1, k2} {
		if err := store.Write(Sample{Labels: k, Value: 1, Add: true}); err != nil {
			t.Fatalf("initial write: %v", err)
		}
	}
	t.Logf("状态: 已接受 %d 条序列，基数=%d（上限 2）", store.Cardinality(), store.Cardinality())

	// 新序列被拒绝 —— 两次，验证计数与可复现性（不做随机淘汰）。
	for i := 0; i < 2; i++ {
		err := store.Write(Sample{Labels: k3, Value: 1, Add: true})
		var clErr *CardinalityLimitError
		if !errors.As(err, &clErr) {
			t.Fatalf("attempt %d: want *CardinalityLimitError, got %v", i+1, err)
		}
		t.Logf("写入被拒绝: key=%s 原因=%q (limit=%d)", clErr.SeriesKey, clErr.Error(), clErr.Limit)
	}
	if got := store.RejectedCount(); got != 2 {
		t.Fatalf("rejected count = %d, want 2", got)
	}

	// 已存在序列始终可更新，不触达上限。
	if err := store.Write(Sample{Labels: k1, Value: 5, Add: true}); err != nil {
		t.Fatalf("existing series update rejected: %v", err)
	}
	snap := store.Snapshot()
	if snap[k1.Key()] != 6 || snap[k3.Key()] != 0 {
		t.Fatalf("state after rejection: %v (k3 must be absent)", snap)
	}
	if _, present := snap[k3.Key()]; present {
		t.Fatalf("rejected series %s must not exist in store", k3.Key())
	}
	t.Logf("拒绝后状态: %v，rejected=%d，新序列未被计入", snap, store.RejectedCount())
}

// TestCardinalityLimitRacesWithUpdates 高并发混合已存在/新序列写入，
// 验证基数永不超限：已存在序列全部接受，新序列全部以可识别错误拒绝，
// 无静默丢失。
func TestCardinalityLimitRacesWithUpdates(t *testing.T) {
	const capN, workers, tries = 4, 12, 200
	store := NewStore(capN)

	// 预填充至上限，后续“新”键的命运被唯一确定：必拒。
	existKeys := make([]Labels, capN)
	for i := range existKeys {
		existKeys[i] = LabelsFromMap(map[string]string{"id": string(rune('a' + i))})
		if err := store.Write(Sample{Labels: existKeys[i], Value: 1, Add: true}); err != nil {
			t.Fatalf("prefill: %v", err)
		}
	}

	var wg sync.WaitGroup
	var acceptedOld, rejected int64
	var mu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < tries; i++ {
				var k Labels
				isExisting := (id+i)%2 == 0
				if isExisting {
					k = existKeys[(id+i/2)%capN]
				} else {
					k = LabelsFromMap(map[string]string{"id": string(rune('a' + capN + (id+i)%capN))})
				}
				err := store.Write(Sample{Labels: k, Value: 1, Add: true})
				mu.Lock()
				if isExisting {
					if err != nil {
						t.Errorf("existing write rejected: %v", err)
					} else {
						acceptedOld++
					}
				} else {
					var clErr *CardinalityLimitError
					if !errors.As(err, &clErr) {
						t.Errorf("new series accepted unexpectedly: %s", k.Key())
					} else {
						rejected++
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if c := store.Cardinality(); c != capN {
		t.Fatalf("cardinality = %d, must never exceed cap %d", c, capN)
	}
	if rejected == 0 || store.RejectedCount() != rejected {
		t.Fatalf("rejected=%d store.RejectedCount=%d", rejected, store.RejectedCount())
	}
	t.Logf("并发混合写入结果: 已存在序列接受=%d 新序列拒绝=%d 最终基数=%d rejected计数=%d",
		acceptedOld, rejected, store.Cardinality(), store.RejectedCount())
}
