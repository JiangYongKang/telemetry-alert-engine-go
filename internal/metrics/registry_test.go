package metrics

import (
	"errors"
	"sync"
	"testing"
)

// 并发写入同一序列，结果必须与串行基准一致。
func TestConcurrentAddMatchesSerial(t *testing.T) {
	const goroutines = 8
	const addsPerGoroutine = 500
	labels := Labels{"host": "a", "dc": "east"}
	t.Logf("输入: %d 个协程, 每个累加 %d 次, 每次 +1.5, 序列标签 %v", goroutines, addsPerGoroutine, labels)

	reg := NewRegistry(0)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < addsPerGoroutine; i++ {
				if err := reg.Add("cpu_usage", labels, 1.5); err != nil {
					t.Errorf("Add 返回意外错误: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// 串行基准
	base := NewRegistry(0)
	for i := 0; i < goroutines*addsPerGoroutine; i++ {
		if err := base.Add("cpu_usage", labels, 1.5); err != nil {
			t.Fatalf("串行基准 Add 失败: %v", err)
		}
	}

	got := reg.Snapshot()
	want := base.Snapshot()
	t.Logf("状态变化: 并发快照=%+v 串行基准=%+v", got, want)
	if len(got) != 1 || len(want) != 1 {
		t.Fatalf("序列数量不符: got %d want %d", len(got), len(want))
	}
	if got[0].Value != want[0].Value || got[0].Count != want[0].Count {
		t.Fatalf("并发结果与串行基准不一致: got %+v want %+v", got[0], want[0])
	}
	t.Logf("判定依据: Value=%v Count=%v 与串行基准完全一致", got[0].Value, got[0].Count)
}

// 标签基数超限时，新序列被拒绝且原因可识别；已存在序列不受影响。
func TestCardinalityLimitRejectReason(t *testing.T) {
	reg := NewRegistry(2)
	t.Log("输入: maxSeries=2, 依次写入 s1,s2(已有),s3(新),s1(已有)")
	if err := reg.Add("m", Labels{"id": "s1"}, 1); err != nil {
		t.Fatalf("s1 写入失败: %v", err)
	}
	if err := reg.Add("m", Labels{"id": "s2"}, 1); err != nil {
		t.Fatalf("s2 写入失败: %v", err)
	}
	err := reg.Add("m", Labels{"id": "s3"}, 1)
	t.Logf("状态变化: 第三个新序列写入返回 err=%v", err)
	if !errors.Is(err, ErrCardinalityLimit) {
		t.Fatalf("判定依据: 期望 ErrCardinalityLimit, 实际 %v", err)
	}
	// 已存在序列仍可写（取舍确定：只拒新序列，不驱逐旧序列）
	if err := reg.Add("m", Labels{"id": "s1"}, 1); err != nil {
		t.Fatalf("已有序列写入应成功: %v", err)
	}
	snap := reg.Snapshot()
	t.Logf("判定依据: 快照=%+v, 序列数=%d (上限 2), s1 计数=2", snap, len(snap))
	if len(snap) != 2 {
		t.Fatalf("序列数应仍为 2, 实际 %d", len(snap))
	}
	for _, s := range snap {
		if s.Labels["id"] == "s1" && s.Count != 2 {
			t.Fatalf("s1 应被累加两次, 实际 %d", s.Count)
		}
	}
}
