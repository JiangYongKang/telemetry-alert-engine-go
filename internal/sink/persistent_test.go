package sink

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testDest 可脚本化的下游：modeOK 记录即成功；modeFail 每次失败（可带延迟）；
// modePark 阻塞直到模式被切换（模拟卡住的下游；setMode 会唤醒在途调用）。
type testDest struct {
	mu      sync.Mutex
	mode    int
	delay   time.Duration
	written []string
	changed chan struct{} // 每次 setMode 关闭并重建, 唤醒 park 中的调用
}

const (
	modeOK = iota
	modeFail
	modePark
)

func newTestDest(mode int) *testDest { return &testDest{mode: mode, changed: make(chan struct{})} }

func (d *testDest) Write(item []byte) error {
	for {
		d.mu.Lock()
		mode, delay, ch := d.mode, d.delay, d.changed
		d.mu.Unlock()
		switch mode {
		case modePark:
			<-ch // 阻塞至模式切换
			continue
		case modeFail:
			if delay > 0 {
				time.Sleep(delay)
			}
			return errors.New("downstream unavailable")
		}
		d.mu.Lock()
		d.written = append(d.written, string(item))
		d.mu.Unlock()
		return nil
	}
}

func (d *testDest) setMode(m int) {
	d.mu.Lock()
	d.mode = m
	close(d.changed)
	d.changed = make(chan struct{})
	d.mu.Unlock()
}
func (d *testDest) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.written...)
}

func journalPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queue.journal")
}

// 场景：跑到一半被硬杀 —— 5 条写入，3 条已确认写出，第 4 条卡在中途时进程死亡。
// 判定：重启后未确认的 2 条按原顺序续投，已确认的 3 条不重发，全程无重复无丢失。
func TestResumeAfterHardKillKeepsOrder(t *testing.T) {
	path := journalPath(t)
	// 下游"前 3 次成功、第 4 次永久阻塞"，模拟硬杀瞬间卡在中途
	counting := &countThenParkDest{limit: 3, parkCh: make(chan struct{})}
	w1, err := OpenPersistentWriter(counting, path, 16, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Log("输入: 依次写入 r1..r5; 下游在第 4 条写出时卡死(模拟硬杀瞬间)")
	for i := 1; i <= 5; i++ {
		if err := w1.Write([]byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatalf("Write r%d: %v", i, err)
		}
	}
	waitFor(t, "前 3 条确认写出", func() bool { return w1.Stats().Pending == 2 })
	t.Logf("状态变化: 硬杀前已确认写出=%v 积压=%d", counting.snapshot(), w1.Stats().Pending)
	// 硬杀：不 Close，直接抛弃 w1（其冲刷协程永远卡在第 4 条上）

	dest2 := newTestDest(modeOK)
	w2, err := OpenPersistentWriter(dest2, path, 16, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	t.Logf("状态变化: 重启后恢复积压=%d", w2.Stats().Pending)
	if got := w2.Stats().Pending; got != 2 {
		t.Fatalf("判定依据: 重启后应恢复 2 条积压, 实际 %d", got)
	}
	waitFor(t, "续投完成", func() bool { return len(dest2.snapshot()) == 2 })
	got := append(counting.snapshot(), dest2.snapshot()...)
	want := []string{"r1", "r2", "r3", "r4", "r5"}
	t.Logf("状态变化: 全量写出序列=%v", got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("判定依据: 写出序列应为 %v 且按序无重复, 实际 %v", want, got)
	}
	t.Log("判定依据: 已确认 3 条未重发, 未确认 2 条按原顺序续投, 无丢失无重复")
}

type countThenParkDest struct {
	mu      sync.Mutex
	limit   int
	written []string
	parkCh  chan struct{}
}

func (d *countThenParkDest) Write(item []byte) error {
	d.mu.Lock()
	if len(d.written) >= d.limit {
		d.mu.Unlock()
		<-d.parkCh
		return nil
	}
	d.written = append(d.written, string(item))
	d.mu.Unlock()
	return nil
}

func (d *countThenParkDest) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.written...)
}

// 场景：正常关停前已全部确认写出。判定：重启后一条都不重发。
func TestAckedNotResentAfterRestart(t *testing.T) {
	path := journalPath(t)
	dest1 := newTestDest(modeOK)
	w1, _ := OpenPersistentWriter(dest1, path, 16, 0)
	t.Log("输入: 写入 a1..a3, 下游健康, 正常 Close")
	for i := 1; i <= 3; i++ {
		if err := w1.Write([]byte(fmt.Sprintf("a%d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	waitFor(t, "3 条写出", func() bool { return len(dest1.snapshot()) == 3 })
	w1.Close()
	t.Logf("状态变化: 关停前写出=%v 积压=%d", dest1.snapshot(), w1.Stats().Pending)

	dest2 := newTestDest(modeOK)
	w2, _ := OpenPersistentWriter(dest2, path, 16, 0)
	defer w2.Close()
	// 给冲刷协程充分时间犯错（若重放逻辑错误会重发）
	time.Sleep(200 * time.Millisecond)
	t.Logf("状态变化: 重启后积压=%d 新写出=%v", w2.Stats().Pending, dest2.snapshot())
	if got := dest2.snapshot(); len(got) != 0 {
		t.Fatalf("判定依据: 已确认写成功的数据重启后不得重发, 实际重发 %v", got)
	}
	if s := w2.Stats(); s.Pending != 0 || s.Dropped != 0 {
		t.Fatalf("判定依据: 重启后应无积压无放弃, 实际 %+v", s)
	}
	t.Log("判定依据: 3 条已确认数据重启后零重发")
}

// 场景：下游持续失败导致放弃。判定：重启后放弃记录仍在（含原因），但绝不翻出重发。
func TestDroppedNotResentAfterRestart(t *testing.T) {
	path := journalPath(t)
	dest1 := newTestDest(modeFail)
	w1, _ := OpenPersistentWriter(dest1, path, 16, 1)
	t.Log("输入: 写入 d1,d2; 下游持续失败, maxRetry=1 -> 全部放弃")
	w1.Write([]byte("d1"))
	w1.Write([]byte("d2"))
	waitFor(t, "2 条放弃", func() bool { return len(w1.Dropped()) == 2 })
	t.Logf("状态变化: 放弃列表=%+v", w1.Dropped())
	w1.Close()

	dest2 := newTestDest(modeOK)
	w2, _ := OpenPersistentWriter(dest2, path, 16, 0)
	defer w2.Close()
	time.Sleep(200 * time.Millisecond)
	dropped := w2.Dropped()
	t.Logf("状态变化: 重启后放弃列表=%+v 新写出=%v", dropped, dest2.snapshot())
	if len(dropped) != 2 {
		t.Fatalf("判定依据: 重启后应重建 2 条放弃记录, 实际 %d", len(dropped))
	}
	for _, d := range dropped {
		if d.Reason == "" {
			t.Fatalf("判定依据: 放弃原因跨重启保留, 实际 %+v", d)
		}
	}
	if got := dest2.snapshot(); len(got) != 0 {
		t.Fatalf("判定依据: 已放弃数据重启后不得翻出重发, 实际 %v", got)
	}
	t.Log("判定依据: 2 条放弃记录及原因跨重启保留, 且零重发")
}

// 场景：下游挂着时积压 4 条, 硬杀重启。判定：积压条数/放弃统计/最老等待时长
// 跨重启连续（不归零、不倒退），下游恢复后按序清空。
func TestBacklogStatsAcrossRestart(t *testing.T) {
	path := journalPath(t)
	dest1 := newTestDest(modeFail)
	dest1.delay = 20 * time.Millisecond // 让重试占据一段时间, blocked 状态可观测
	w1, _ := OpenPersistentWriter(dest1, path, 16, 100000)
	t.Log("输入: 下游挂起, 写入 b1..b4, 观察积压后硬杀重启")
	for i := 1; i <= 4; i++ {
		w1.Write([]byte(fmt.Sprintf("b%d", i)))
	}
	waitFor(t, "积压 4 条且被下游拖住", func() bool {
		s := w1.Stats()
		return s.Pending == 4 && s.Blocked
	})
	s1 := w1.Stats()
	t.Logf("状态变化: 硬杀前积压统计=%+v", s1)
	// 硬杀：把旧下游切到 park, 抛弃 w1
	dest1.setMode(modePark)

	dest2 := newTestDest(modeFail)
	w2, _ := OpenPersistentWriter(dest2, path, 16, 100000)
	defer w2.Close()
	waitFor(t, "重启后再次被下游拖住", func() bool { return w2.Stats().Blocked })
	s2 := w2.Stats()
	t.Logf("状态变化: 重启后积压统计=%+v", s2)
	if s2.Pending != s1.Pending {
		t.Fatalf("判定依据: 积压条数跨重启连续, 重启前 %d 重启后 %d", s1.Pending, s2.Pending)
	}
	if s2.OldestAge < s1.OldestAge {
		t.Fatalf("判定依据: 最老等待时长不应倒退, 重启前 %v 重启后 %v", s1.OldestAge, s2.OldestAge)
	}
	if s2.Dropped != s1.Dropped {
		t.Fatalf("判定依据: 放弃统计跨重启连续, %d vs %d", s1.Dropped, s2.Dropped)
	}
	dest2.setMode(modeOK)
	waitFor(t, "下游恢复后清空", func() bool { return w2.Stats().Pending == 0 })
	t.Logf("状态变化: 恢复后写出=%v", dest2.snapshot())
	if got := dest2.snapshot(); fmt.Sprint(got) != fmt.Sprint([]string{"b1", "b2", "b3", "b4"}) {
		t.Fatalf("判定依据: 恢复后应按原顺序写出 b1..b4, 实际 %v", got)
	}
	t.Log("判定依据: 积压条数/最老时长/放弃统计跨重启连续, 恢复后按序清空")
}

// 场景：运维手动处置 —— 重投最老 2 条、丢弃剩余 1 条。
// 判定：重投走原顺序原路径写出; 丢弃的跨重启也不再出现。
func TestManualRequeueAndDiscard(t *testing.T) {
	path := journalPath(t)
	dest1 := newTestDest(modeFail)
	w1, _ := OpenPersistentWriter(dest1, path, 16, 0)
	t.Log("输入: 写入 m1..m3, 下游失败全部放弃; 然后 Requeue(2) + 恢复下游 + Discard(1)")
	for i := 1; i <= 3; i++ {
		w1.Write([]byte(fmt.Sprintf("m%d", i)))
	}
	waitFor(t, "3 条放弃", func() bool { return len(w1.Dropped()) == 3 })
	t.Logf("状态变化: 放弃列表=%+v", w1.Dropped())

	dest1.setMode(modeOK) // 先恢复下游再重投, 否则重投条目会再次失败
	if n := w1.Requeue(2); n != 2 {
		t.Fatalf("判定依据: Requeue(2) 应返回 2, 实际 %d", n)
	}
	t.Logf("状态变化: 重投后积压=%d 剩余放弃=%d", w1.Stats().Pending, len(w1.Dropped()))
	waitFor(t, "重投写出", func() bool { return len(dest1.snapshot()) == 2 })
	if got := dest1.snapshot(); fmt.Sprint(got) != fmt.Sprint([]string{"m1", "m2"}) {
		t.Fatalf("判定依据: 重投应按原顺序写出 m1,m2, 实际 %v", got)
	}
	if n := w1.Discard(5); n != 1 {
		t.Fatalf("判定依据: Discard(5) 只剩 1 条可丢, 应返回 1, 实际 %d", n)
	}
	t.Logf("状态变化: 丢弃后放弃列表=%+v", w1.Dropped())
	w1.Close()

	dest2 := newTestDest(modeOK)
	w2, _ := OpenPersistentWriter(dest2, path, 16, 0)
	defer w2.Close()
	time.Sleep(200 * time.Millisecond)
	t.Logf("状态变化: 重启后放弃=%v 新写出=%v", w2.Dropped(), dest2.snapshot())
	if len(w2.Dropped()) != 0 {
		t.Fatalf("判定依据: 已丢弃的记录重启后不得再出现, 实际 %+v", w2.Dropped())
	}
	if got := dest2.snapshot(); len(got) != 0 {
		t.Fatalf("判定依据: 已确认/已丢弃的数据重启后不得重发, 实际 %v", got)
	}
	t.Log("判定依据: 重投按序写出且去重, 丢弃跨重启生效")
}

// 场景：恢复补投期间下游仍挂 —— 容量上限与三种反馈照常。
// 判定：积压满时新写入返回 ErrBusy（不静默吞、不无限堆积）; 恢复后正常接受。
func TestCapacityAndFeedbackDuringRecovery(t *testing.T) {
	path := journalPath(t)
	dest1 := newTestDest(modePark) // 从一开始就卡死
	w1, _ := OpenPersistentWriter(dest1, path, 3, 0)
	t.Log("输入: 容量=3, 下游卡死, 写入 4 条")
	for i := 1; i <= 3; i++ {
		if err := w1.Write([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatalf("Write c%d 应被接受: %v", i, err)
		}
	}
	if err := w1.Write([]byte("c4")); !errors.Is(err, ErrBusy) {
		t.Fatalf("判定依据: 容量满应返回 ErrBusy, 实际 %v", err)
	}
	t.Log("状态变化: 第 4 条收到 ErrBusy 背压; 硬杀重启")
	// 硬杀：抛弃 w1（协程卡在 park 下游上）

	dest2 := newTestDest(modePark)
	w2, _ := OpenPersistentWriter(dest2, path, 3, 0)
	defer w2.Close()
	if s := w2.Stats(); s.Pending != 3 {
		t.Fatalf("判定依据: 重启后应恢复 3 条积压, 实际 %d", s.Pending)
	}
	if err := w2.Write([]byte("c4")); !errors.Is(err, ErrBusy) {
		t.Fatalf("判定依据: 恢复期间容量上限照常, 应返回 ErrBusy, 实际 %v", err)
	}
	t.Log("状态变化: 恢复期间积压占满容量, 新写入仍收到 ErrBusy")
	dest2.setMode(modeOK)
	waitFor(t, "恢复后清空", func() bool { return w2.Stats().Pending == 0 })
	if err := w2.Write([]byte("c4")); err != nil {
		t.Fatalf("判定依据: 下游恢复后应能接受, 实际 %v", err)
	}
	waitFor(t, "c4 写出", func() bool { return len(dest2.snapshot()) == 4 })
	t.Logf("状态变化: 最终写出=%v", dest2.snapshot())
	if got := dest2.snapshot(); fmt.Sprint(got) != fmt.Sprint([]string{"c1", "c2", "c3", "c4"}) {
		t.Fatalf("判定依据: 应按序写出 c1..c4, 实际 %v", got)
	}
	w2.Close()
	if err := w2.Write([]byte("c5")); !errors.Is(err, ErrClosed) {
		t.Fatalf("判定依据: 关闭后应返回 ErrClosed, 实际 %v", err)
	}
	t.Log("判定依据: 接受/繁忙/关闭三种反馈在恢复期间与恢复后均成立, 无静默丢失")
}

// 场景：日志压缩 —— 大量 ack/drop 后日志被重写, 重启后状态不变。
func TestJournalCompactionKeepsState(t *testing.T) {
	old := compactThreshold
	compactThreshold = 64 // 调小阈值让测试快速触发压缩
	defer func() { compactThreshold = old }()
	path := journalPath(t)
	dest := newTestDest(modeOK)
	w, _ := OpenPersistentWriter(dest, path, 8, 0)
	t.Logf("输入: 交替成功/失败写入超过压缩阈值 %d 条, 触发日志压缩", compactThreshold)
	failEvery := 3
	total := compactThreshold + 100
	for i := 0; i < total; i++ {
		if i%failEvery == failEvery-1 {
			dest.setMode(modeFail)
		} else {
			dest.setMode(modeOK)
		}
		for {
			err := w.Write([]byte(fmt.Sprintf("x%d", i)))
			if errors.Is(err, ErrBusy) {
				time.Sleep(time.Millisecond)
				continue
			}
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			break
		}
	}
	dest.setMode(modeOK)
	waitFor(t, "全部出队", func() bool { return w.Stats().Pending == 0 })
	droppedBefore := len(w.Dropped())
	sizeBefore, _ := os.Stat(path)
	t.Logf("状态变化: 压缩前日志大小=%d 放弃=%d", sizeBefore.Size(), droppedBefore)
	w.Close()

	dest2 := newTestDest(modeOK)
	w2, _ := OpenPersistentWriter(dest2, path, 8, 0)
	defer w2.Close()
	time.Sleep(100 * time.Millisecond)
	t.Logf("状态变化: 重启后放弃=%d 新写出=%d", len(w2.Dropped()), len(dest2.snapshot()))
	if len(w2.Dropped()) != droppedBefore {
		t.Fatalf("判定依据: 压缩+重启后放弃记录数不变, %d vs %d", droppedBefore, len(w2.Dropped()))
	}
	if got := dest2.snapshot(); len(got) != 0 {
		t.Fatalf("判定依据: 压缩后已确认数据不重发, 实际 %d 条", len(got))
	}
	// 放弃原因可统计
	var reasons []string
	for r, n := range w2.Stats().DropReasons {
		reasons = append(reasons, fmt.Sprintf("%q:%d", r, n))
	}
	t.Logf("判定依据: 压缩后状态完整, 放弃原因分布=%v", strings.Join(reasons, ","))
}
