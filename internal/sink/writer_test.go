package sink

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeDest 可控失败的下游，记录所有成功写出的数据。
type fakeDest struct {
	mu      sync.Mutex
	fail    bool
	written [][]byte
}

func (d *fakeDest) Write(item []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail {
		return errors.New("downstream unavailable")
	}
	cp := make([]byte, len(item))
	copy(cp, item)
	d.written = append(d.written, cp)
	return nil
}

func (d *fakeDest) setFail(f bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail = f
}

func (d *fakeDest) snapshot() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]byte, len(d.written))
	copy(out, d.written)
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// 缓冲区满时返回 ErrBusy（可识别的背压信号），已接受的数据最终全部写出。
func TestBackpressureBusySignal(t *testing.T) {
	// 下游阻塞，直到测试放行，保证缓冲区必然被填满。
	// capacity=1：item-0 被冲刷协程取走后阻塞在下游，item-1 占满缓冲区，
	// item-2/item-3 必然收到 ErrBusy。
	release := make(chan struct{})
	dest := &blockingDest{gate: release, entered: make(chan struct{})}
	w := NewWriter(dest, 1, 0)
	t.Log("输入: capacity=1, 下游阻塞, 连续写入 4 条")

	var results []error
	results = append(results, w.Write([]byte("item-0")))
	t.Logf("状态变化: Write(item-0) -> %v", results[0])
	<-dest.entered // 等冲刷协程取走 item-0 并阻塞在下游，缓冲区此时为空
	for i := 1; i < 4; i++ {
		err := w.Write([]byte(fmt.Sprintf("item-%d", i)))
		results = append(results, err)
		t.Logf("状态变化: Write(item-%d) -> %v", i, err)
	}
	if results[0] != nil || results[1] != nil {
		t.Fatalf("判定依据: 前两条应被接受, 实际 %v", results)
	}
	if !errors.Is(results[2], ErrBusy) || !errors.Is(results[3], ErrBusy) {
		t.Fatalf("判定依据: 缓冲区满后应返回 ErrBusy, 实际 %v", results[2:])
	}
	t.Log("判定依据: 调用方可区分 已接受(nil) 与 暂时无法接受(ErrBusy)")

	close(release) // 放行下游
	w.Close()
	if got := len(dest.snapshot()); got != 2 {
		t.Fatalf("判定依据: 已接受的 2 条最终必须全部写出, 实际 %d", got)
	}
	t.Log("判定依据: 已接受的数据在下游恢复后全部写出, 无丢失")
}

// blockingDest 在 gate 关闭前一直阻塞；首次进入 Write 时关闭 entered 信号。
type blockingDest struct {
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
	mu      sync.Mutex
	written [][]byte
}

func (d *blockingDest) Write(item []byte) error {
	d.once.Do(func() { close(d.entered) })
	<-d.gate
	d.mu.Lock()
	defer d.mu.Unlock()
	d.written = append(d.written, item)
	return nil
}

func (d *blockingDest) snapshot() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([][]byte(nil), d.written...)
}

// 下游持续失败时数据被放弃并附原因；下游恢复后不回写已放弃数据，新数据正常写出。
func TestDropWithReasonAndNoRewriteAfterRecovery(t *testing.T) {
	dest := &fakeDest{fail: true}
	w := NewWriter(dest, 8, 2)
	t.Log("输入: maxRetry=2, 下游先失败写入 a,b; 恢复后写入 c")

	if err := w.Write([]byte("a")); err != nil {
		t.Fatalf("a 应被接受: %v", err)
	}
	if err := w.Write([]byte("b")); err != nil {
		t.Fatalf("b 应被接受: %v", err)
	}
	waitFor(t, "a,b 被放弃并记录原因", func() bool { return len(w.Dropped()) == 2 })
	t.Logf("状态变化: 放弃记录=%+v", w.Dropped())

	dest.setFail(false)
	if err := w.Write([]byte("c")); err != nil {
		t.Fatalf("c 应被接受: %v", err)
	}
	waitFor(t, "c 被写出", func() bool { return len(dest.snapshot()) == 1 })
	w.Close()

	written := dest.snapshot()
	dropped := w.Dropped()
	t.Logf("判定依据: 已写出=%q, 已放弃=%+v", written, dropped)
	if len(written) != 1 || string(written[0]) != "c" {
		t.Fatalf("恢复后只允许写出新数据 c, 不得回写已放弃的 a,b; 实际写出 %q", written)
	}
	if len(dropped) != 2 {
		t.Fatalf("a,b 应各有一条放弃记录, 实际 %+v", dropped)
	}
	for _, d := range dropped {
		if d.Reason == "" {
			t.Fatalf("放弃必须附原因, 实际 %+v", d)
		}
	}
}

// 并发写入 + 下游间歇失败：已接受的数据要么写出要么放弃，总数守恒、无重复。
func TestConcurrentWriteConservation(t *testing.T) {
	dest := &fakeDest{}
	w := NewWriter(dest, 16, 1)
	const writers = 8
	const perWriter = 100
	t.Logf("输入: %d 个协程各写 %d 条, 下游间歇失败", writers, perWriter)

	// 间歇故障开关
	stop := make(chan struct{})
	var toggler sync.WaitGroup
	toggler.Add(1)
	go func() {
		defer toggler.Done()
		for {
			select {
			case <-stop:
				return
			default:
				dest.setFail(true)
				time.Sleep(time.Millisecond)
				dest.setFail(false)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, busy := 0, 0
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				item := []byte(fmt.Sprintf("g%d-i%d", g, i))
				err := w.Write(item)
				mu.Lock()
				if err == nil {
					accepted++
				} else if errors.Is(err, ErrBusy) {
					busy++
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	toggler.Wait()
	dest.setFail(false)
	w.Close() // Close 会冲刷完所有已接受数据

	written := dest.snapshot()
	dropped := w.Dropped()
	t.Logf("状态变化: 已接受=%d 繁忙拒绝=%d 已写出=%d 已放弃=%d", accepted, busy, len(written), len(dropped))
	if len(written)+len(dropped) != accepted {
		t.Fatalf("判定依据: 已接受数据必须守恒 写出(%d)+放弃(%d) != 接受(%d)",
			len(written), len(dropped), accepted)
	}
	seen := make(map[string]int)
	for _, b := range written {
		seen[string(b)]++
	}
	for item, n := range seen {
		if n > 1 {
			t.Fatalf("判定依据: 数据不得重复写出: %q 出现 %d 次", item, n)
		}
	}
	t.Log("判定依据: 写出+放弃=接受, 且无重复写出, 无静默丢失")
}
