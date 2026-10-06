package sink

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var errDownstream = errors.New("downstream unavailable")

// gateDest 可控制的下游：可整体失败、可用闸门卡住写出调用（模拟下游 hang）。
type gateDest struct {
	mu        sync.Mutex
	fail      bool
	gate      chan struct{} // 非 nil 时 Write 阻塞在闸门处
	written   []string
	attempted chan string // 每次 Write 调用都会投递（缓冲，不阻塞）
}

func newGateDest() *gateDest { return &gateDest{attempted: make(chan string, 64)} }

func (d *gateDest) Write(item []byte) error {
	d.mu.Lock()
	g, fail := d.gate, d.fail
	d.mu.Unlock()
	select {
	case d.attempted <- string(item):
	default:
	}
	if g != nil {
		<-g
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if fail {
		return errDownstream
	}
	d.written = append(d.written, string(item))
	return nil
}

func (d *gateDest) setFail(f bool) { d.mu.Lock(); d.fail = f; d.mu.Unlock() }

// block 让所有后续 Write 调用卡在闸门处。
func (d *gateDest) block() {
	d.mu.Lock()
	d.gate = make(chan struct{})
	d.mu.Unlock()
}

// release 打开闸门，之后的 Write 按当前 fail 设置返回。
func (d *gateDest) release() {
	d.mu.Lock()
	if d.gate != nil {
		close(d.gate)
		d.gate = nil
	}
	d.mu.Unlock()
}

func (d *gateDest) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.written...)
}

func newPersistent(t *testing.T, dest *gateDest, path string, capacity, maxRetry int) *PersistentWriter {
	t.Helper()
	w, err := NewPersistentWriter(dest, path, capacity, maxRetry)
	if err != nil {
		t.Fatalf("NewPersistentWriter: %v", err)
	}
	return w
}

// 场景 1：跑到一半被硬杀 —— 已收下未写出的不丢、按原顺序续投；
// 已确认写出的不重发。
func TestCrashMidwayPendingResumeInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	dest := newGateDest()

	t.Log("输入: 下游正常时写入 A,B; 随后下游 hang+失败, 写入 C,D,E; 在 C 写出中硬杀")
	w1 := newPersistent(t, dest, path, 8, 1)
	for _, s := range []string{"A", "B"} {
		if err := w1.Write([]byte(s)); err != nil {
			t.Fatalf("Write %s: %v", s, err)
		}
	}
	waitFor(t, "A,B 写出确认", func() bool { return len(dest.snapshot()) == 2 })
	t.Logf("状态变化: A,B 已写出并 ack 落盘, written=%v", dest.snapshot())

	dest.block()
	dest.setFail(true)
	for _, s := range []string{"C", "D", "E"} {
		if err := w1.Write([]byte(s)); err != nil {
			t.Fatalf("Write %s: %v", s, err)
		}
	}
	waitFor(t, "C 进入写出中", func() bool { <-dest.attempted; return true })
	t.Log("状态变化: C 写出中(卡在下游), D,E 在队列; 此刻模拟硬杀")
	w1.halt()
	dest.release()  // 硬杀后下游才有响应; stopped 实例不再落任何日志
	w1.wg.Wait()    // 确认旧实例后台协程已退出

	// 重启：C,D,E 应按原顺序补投；A,B 已确认写出不得重发。
	dest.block() // 新实例先卡住，验证恢复出的积压
	w2 := newPersistent(t, dest, path, 8, 1)
	st := w2.Stats()
	t.Logf("状态变化: 重启后积压 Pending=%d Dropped=%d Blocked=%v", st.Pending, st.Dropped, st.DownstreamBlocked)
	if st.Pending != 3 {
		t.Fatalf("判定依据: 重启后应有 3 条待补投, 实际 %d", st.Pending)
	}
	dest.setFail(false)
	dest.release()
	waitFor(t, "C,D,E 补投完成", func() bool { return len(dest.snapshot()) == 5 })
	got := dest.snapshot()
	t.Logf("状态变化: 补投完成, written=%v", got)
	want := []string{"A", "B", "C", "D", "E"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("判定依据: 写出顺序应为 %v 且 A,B 不重复, 实际 %v", want, got)
	}
	t.Log("判定依据: 已确认写出的 A,B 未重发; 未完成的 C,D,E 按原顺序续投")

	// 正常关闭后再重启：全部已确认，不得再有任何写出。
	w2.Close()
	w3 := newPersistent(t, dest, path, 8, 1)
	if st := w3.Stats(); st.Pending != 0 {
		t.Fatalf("判定依据: 正常关闭后重启应无积压, 实际 %d", st.Pending)
	}
	w3.Close()
	if got := dest.snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("判定依据: 重启后不得重发已确认数据, 实际 %v", got)
	}
	t.Log("判定依据: 正常关闭重启后无补投、无重复写出")
}

// 场景 2：已放弃的数据重启后不被翻出重发，放弃原因跨重启保留。
func TestDroppedNotResentAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	dest := newGateDest()
	dest.setFail(true)

	t.Log("输入: 下游持续失败, 写入 X,Y (maxRetry=1); 等待两条都被放弃")
	w1 := newPersistent(t, dest, path, 8, 1)
	for _, s := range []string{"X", "Y"} {
		if err := w1.Write([]byte(s)); err != nil {
			t.Fatalf("Write %s: %v", s, err)
		}
	}
	waitFor(t, "X,Y 被放弃", func() bool { return len(w1.Dropped()) == 2 })
	t.Logf("状态变化: 已放弃=%d 原因=%q", len(w1.Dropped()), w1.Dropped()[0].Reason)
	w1.halt() // 硬杀

	// 重启且下游恢复：X,Y 不得重发。
	dest.setFail(false)
	w2 := newPersistent(t, dest, path, 8, 1)
	st := w2.Stats()
	t.Logf("状态变化: 重启后 Pending=%d Dropped=%d ByReason=%v", st.Pending, st.Dropped, st.DroppedByReason)
	if st.Pending != 0 {
		t.Fatalf("判定依据: 已放弃数据不得进入补投, 实际 Pending=%d", st.Pending)
	}
	if st.Dropped != 2 || st.DroppedByReason[w1.Dropped()[0].Reason] != 2 {
		t.Fatalf("判定依据: 放弃计数与原因应跨重启保留, 实际 %+v", st)
	}
	// 写一条新数据证明链路正常，且 X,Y 仍不出现。
	if err := w2.Write([]byte("Z")); err != nil {
		t.Fatalf("Write Z: %v", err)
	}
	waitFor(t, "Z 写出", func() bool { return len(dest.snapshot()) == 1 })
	got := dest.snapshot()
	if fmt.Sprint(got) != "[Z]" {
		t.Fatalf("判定依据: 重启后只能写出新数据 Z, 已放弃的 X,Y 不得重发, 实际 %v", got)
	}
	w2.Close()
	t.Log("判定依据: 已放弃的 X,Y 重启后未重发; 放弃原因跨重启保留; 新数据 Z 正常写出")
}

// 场景 3：积压观测跨重启连续 —— 待写出条数、最老等待时长、下游阻塞标志。
func TestBacklogStatsConsistentAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	dest := newGateDest()
	dest.block() // 下游 hang：积压只增不减

	t.Log("输入: 下游 hang, 连续写入 3 条; 记录重启前后积压观测")
	w1 := newPersistent(t, dest, path, 8, 1)
	acceptTime := time.Now()
	for _, s := range []string{"m1", "m2", "m3"} {
		if err := w1.Write([]byte(s)); err != nil {
			t.Fatalf("Write %s: %v", s, err)
		}
	}
	waitFor(t, "m1 进入写出中", func() bool { <-dest.attempted; return true })
	before := w1.Stats()
	t.Logf("状态变化: 重启前 Pending=%d OldestAge=%v Blocked=%v", before.Pending, before.OldestPendingAge, before.DownstreamBlocked)
	if before.Pending != 3 {
		t.Fatalf("判定依据: 重启前 Pending 应为 3, 实际 %d", before.Pending)
	}

	w1.halt()
	dest.release() // 让旧实例卡住的那次调用返回; stopped 实例不落日志

	const down = 120 * time.Millisecond
	time.Sleep(down) // 模拟停机时长
	w2 := newPersistent(t, dest, path, 8, 1)
	dest.block() // 新实例同样先卡住，便于观测
	after := w2.Stats()
	t.Logf("状态变化: 重启后 Pending=%d OldestAge=%v (停机 %v)", after.Pending, after.OldestPendingAge, down)
	if after.Pending != before.Pending {
		t.Fatalf("判定依据: 积压条数应跨重启连续, 前=%d 后=%d", before.Pending, after.Pending)
	}
	if after.OldestPendingAge < down {
		t.Fatalf("判定依据: 最老等待时长应包含停机时间(>=%v), 实际 %v", down, after.OldestPendingAge)
	}
	if elapsed := time.Since(acceptTime); after.OldestPendingAge > elapsed+time.Second {
		t.Fatalf("判定依据: 最老等待时长不应超过自首条接受至今 %v, 实际 %v", elapsed, after.OldestPendingAge)
	}
	w2.halt()
	t.Log("判定依据: Pending 与最老等待时长跨重启连续, 未归零")
}

// 场景 4：恢复补投期间下游仍挂 —— 新写入的三种反馈与容量上限仍成立；
// 手动重投/丢弃的顺序与去重跟自动流程一致。
func TestRecoveryBackpressureAndManualOps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	dest := newGateDest()
	dest.block()
	dest.setFail(true)

	t.Log("输入: 下游 hang+失败时写入 old1,old2 后硬杀; 重启后下游仍挂, 再写 new1,new2 (容量=3)")
	w1 := newPersistent(t, dest, path, 8, 0)
	for _, s := range []string{"old1", "old2"} {
		if err := w1.Write([]byte(s)); err != nil {
			t.Fatalf("Write %s: %v", s, err)
		}
	}
	waitFor(t, "old1 进入写出中", func() bool { <-dest.attempted; return true })
	w1.halt()
	dest.release()

	// 重启：恢复出 2 条积压，下游仍然 hang+失败。
	dest.block()
	w2 := newPersistent(t, dest, path, 3, 0)
	waitFor(t, "old1 补投进入写出中", func() bool { <-dest.attempted; return true })
	if st := w2.Stats(); st.Pending != 2 {
		t.Fatalf("判定依据: 恢复积压应为 2, 实际 %d", st.Pending)
	}
	t.Log("状态变化: 重启恢复积压 Pending=2, 下游仍 hang")

	// 反馈一：容量内新写入被收下。
	if err := w2.Write([]byte("new1")); err != nil {
		t.Fatalf("判定依据: 容量内新写入应被收下, 实际 %v", err)
	}
	// 反馈二：容量(3)已满，新写入得到背压。
	if err := w2.Write([]byte("new2")); !errors.Is(err, ErrBusy) {
		t.Fatalf("判定依据: 积压占满容量时应返回 ErrBusy, 实际 %v", err)
	}
	t.Log("状态变化: new1 收下(Pending=3=容量), new2 被背压拒绝")

	// 反馈三：下游持续失败，存量按重试耗尽放弃并附原因。
	dest.release() // 闸门打开但 fail=true：old1,old2,new1 依次失败
	waitFor(t, "3 条全部放弃", func() bool { return len(w2.Dropped()) == 3 })
	st := w2.Stats()
	t.Logf("状态变化: Dropped=%d ByReason=%v Blocked=%v", st.Dropped, st.DroppedByReason, st.DownstreamBlocked)
	if st.Dropped != 3 || !st.DownstreamBlocked {
		t.Fatalf("判定依据: 应有 3 条放弃且标记下游阻塞, 实际 %+v", st)
	}
	for _, d := range w2.Dropped() {
		if d.Reason == "" {
			t.Fatalf("判定依据: 放弃必须附原因, 实际 %+v", d)
		}
	}

	// 手动重投：下游恢复后按原顺序重投，且不会重复。
	dest.setFail(false)
	if n := w2.RequeueDropped(); n != 3 {
		t.Fatalf("判定依据: 应重投 3 条, 实际 %d", n)
	}
	waitFor(t, "重投写出完成", func() bool { return len(dest.snapshot()) == 3 })
	got := dest.snapshot()
	t.Logf("状态变化: 手动重投完成, written=%v", got)
	if want := []string{"old1", "old2", "new1"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("判定依据: 重投顺序应与原接受顺序一致 %v, 实际 %v", want, got)
	}
	if n := w2.RequeueDropped(); n != 0 {
		t.Fatalf("判定依据: 已重投的条目不得重复重投, 实际又重投 %d 条", n)
	}
	t.Log("判定依据: 手动重投顺序=原顺序, 且去重(二次重投为 0)")

	// 手动丢弃：新写入两条，下游 hang 时全部丢弃。
	dest.block()
	for _, s := range []string{"d1", "d2"} {
		if err := w2.Write([]byte(s)); err != nil {
			t.Fatalf("Write %s: %v", s, err)
		}
	}
	waitFor(t, "d1 进入写出中", func() bool { <-dest.attempted; return true })
	if n := w2.DiscardPending(); n != 2 {
		t.Fatalf("判定依据: 应丢弃 2 条, 实际 %d", n)
	}
	st = w2.Stats()
	t.Logf("状态变化: 手动丢弃后 Pending=%d Dropped=%d ByReason=%v", st.Pending, st.Dropped, st.DroppedByReason)
	if st.Pending != 0 || st.DroppedByReason["manual discard"] != 2 {
		t.Fatalf("判定依据: 丢弃后 Pending=0 且原因记为 manual discard, 实际 %+v", st)
	}
	dest.setFail(false)
	dest.release() // d1 的在途调用可能成功写出一次, 但不得被记为成功/重发
	w2.Close()

	// 重启核对：被丢弃的不得翻出重发；重投成功的不得重复。
	before := len(dest.snapshot())
	w3 := newPersistent(t, dest, path, 8, 0)
	final := w3.Stats()
	t.Logf("状态变化: 再次重启 Pending=%d Dropped=%d ByReason=%v", final.Pending, final.Dropped, final.DroppedByReason)
	if final.Pending != 0 {
		t.Fatalf("判定依据: 已丢弃/已确认数据重启后不得补投, 实际 Pending=%d", final.Pending)
	}
	if final.DroppedByReason["manual discard"] != 2 {
		t.Fatalf("判定依据: 手动丢弃记录应跨重启保留, 实际 %+v", final.DroppedByReason)
	}
	w3.Close()
	if len(dest.snapshot()) != before {
		t.Fatalf("判定依据: 重启后不得有任何重发, 新增写出 %v", dest.snapshot()[before:])
	}
	t.Log("判定依据: 手动丢弃的不重发; 重投成功的不重复; 三类反馈与容量上限在恢复期成立")
}
