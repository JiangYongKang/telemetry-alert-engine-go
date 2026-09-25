package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recDownstream 是可控制失败的下游，记录实际写出的全部字节并保证
// Write 原子（全收或全不收，失败时不记录任何内容）。
type recDownstream struct {
	mu        sync.Mutex
	failUntil time.Time // 该时间之前的 Write 一律失败
	got       [][]byte
	fails     int
}

func (d *recDownstream) Write(ctx context.Context, events [][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Now().Before(d.failUntil) {
		d.fails++
		return errors.New("downstream unavailable")
	}
	for _, e := range events {
		d.got = append(d.got, append([]byte(nil), e...))
	}
	return nil
}

func (d *recDownstream) snapshot() ([][]byte, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]byte, len(d.got))
	copy(out, d.got)
	return out, d.fails
}

// TestSubmitThreeOutcomes 验证 Accepted / Busy / Abandoned(三种原因)
// 可明确区分，且计数准确。
func TestSubmitThreeOutcomes(t *testing.T) {
	down := &recDownstream{failUntil: time.Now().Add(time.Hour)} // 持续失败占住队列
	w := NewWriter(down,
		WithCapacity(2),
		WithMaxEventBytes(10),
		WithBackoff(time.Millisecond, 2*time.Millisecond),
	)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, _ = w.Close(ctx)
	}()

	// 队列容量 2，下游持续失败：前两条 Accepted，第三条立即 Busy（背压）。
	o1 := w.Submit([]byte("e1"))
	o2 := w.Submit([]byte("e2"))
	o3 := w.Submit([]byte("e3"))
	t.Logf("容量=2 且下游持续失败: 提交结果=%s,%s,%s", o1.Kind, o2.Kind, o3.Kind)
	if o1.Kind != OutcomeAccepted || o2.Kind != OutcomeAccepted || o3.Kind != OutcomeBusy {
		t.Fatalf("outcomes = %v,%v,%v, want accepted,accepted,busy", o1.Kind, o2.Kind, o3.Kind)
	}

	// 超限 -> 确定性放弃，原因 payload_too_large。
	big := w.Submit(make([]byte, 11))
	if big.Kind != OutcomeAbandoned || big.Reason != ReasonTooLarge {
		t.Fatalf("big outcome = %+v, want abandoned/%s", big, ReasonTooLarge)
	}
	t.Logf("超长事件结果=%s 原因=%s（终结性放弃，不会回写）", big.Kind, big.Reason)

	st := w.Stats()
	if st.Accepted != 2 || st.Busy != 1 || st.Abandoned != 1 {
		t.Fatalf("stats = %+v", st)
	}
	t.Logf("状态计数: %+v", st)

	// 关闭后提交 -> abandoned/sink_closed。
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	undelivered, err := w.Close(ctx)
	t.Logf("带失败下游关闭: 交还未送达=%d 条 err=%v", len(undelivered), err)
	if len(undelivered) != 2 || !errors.Is(err, ErrUndelivered) {
		t.Fatalf("undelivered=%d err=%v, want 2 + ErrUndelivered", len(undelivered), err)
	}
	after := w.Submit([]byte("e4"))
	if after.Kind != OutcomeAbandoned || after.Reason != ReasonClosed {
		t.Fatalf("post-close outcome = %+v, want abandoned/%s", after, ReasonClosed)
	}
	t.Logf("关闭后提交结果=%s 原因=%s", after.Kind, after.Reason)
}

// TestRetryThenRecoverNoLossNoDup 验证下游恢复后：全部 Accepted 事件
// 恰好送达一次（不丢、不重），顺序保持。
func TestRetryThenRecoverNoLossNoDup(t *testing.T) {
	down := &recDownstream{failUntil: time.Now().Add(80 * time.Millisecond)}
	w := NewWriter(down,
		WithCapacity(10),
		WithBackoff(2*time.Millisecond, 10*time.Millisecond),
	)

	for i := 0; i < 5; i++ {
		if o := w.Submit([]byte{byte('a' + i)}); o.Kind != OutcomeAccepted {
			t.Fatalf("submit %d: %+v", i, o)
		}
	}
	t.Log("已接受 5 条；下游失败 80ms 后恢复，期间 worker 重试且不弹队列")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.QueueLen() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if w.QueueLen() != 0 {
		t.Fatalf("queue not drained after recovery: %d", w.QueueLen())
	}

	got, fails := down.snapshot()
	t.Logf("下游恢复后: 失败尝试=%d 实际送达=%d 条", fails, len(got))
	if fails == 0 {
		t.Fatal("expected at least one failed attempt during outage")
	}
	if len(got) != 5 {
		t.Fatalf("delivered = %d, want 5 (no loss)", len(got))
	}
	seen := map[byte]int{}
	for _, b := range got {
		if len(b) != 1 {
			t.Fatalf("bad payload %v", b)
		}
		seen[b[0]]++
	}
	for i := 0; i < 5; i++ {
		if seen[byte('a'+i)] != 1 {
			t.Fatalf("payload %c delivered %d times, want exactly 1 (no dup)", 'a'+i, seen[byte('a'+i)])
		}
	}
	if string(got[0]) != "a" || string(got[4]) != "e" {
		t.Fatalf("delivery order = %v, want a..e", got)
	}
	t.Logf("送达顺序与内容: %s，每条恰好一次", string(func() []byte {
		out := make([]byte, len(got))
		for i, b := range got {
			out[i] = b[0]
		}
		return out
	}()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if undelivered, err := w.Close(ctx); err != nil || len(undelivered) != 0 {
		t.Fatalf("close after drain: undelivered=%d err=%v", len(undelivered), err)
	}
	if st := w.Stats(); st.Delivered != 5 {
		t.Fatalf("delivered stat = %d, want 5", st.Delivered)
	}
}

// TestBusyBecomesAcceptableAfterDrain 验证背压是暂时信号：下游恢复、
// 队列腾出空位后，同一数据重试可被接受，不丢任何已接受事件。
func TestBusyBecomesAcceptableAfterDrain(t *testing.T) {
	down := &recDownstream{failUntil: time.Now().Add(60 * time.Millisecond)}
	w := NewWriter(down, WithCapacity(1), WithBackoff(time.Millisecond, 5*time.Millisecond))

	o1 := w.Submit([]byte("x"))
	o2 := w.Submit([]byte("y"))
	if o1.Kind != OutcomeAccepted || o2.Kind != OutcomeBusy {
		t.Fatalf("o1=%v o2=%v", o1, o2)
	}
	t.Log("容量=1：第一条 accepted，第二条 busy（背压信号，调用方稍后重试）")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.QueueLen() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if o := w.Submit([]byte("y")); o.Kind != OutcomeAccepted {
		t.Fatalf("retry after drain = %+v, want accepted", o)
	}
	t.Log("队列排空后重试同一份数据 y -> accepted")

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && w.QueueLen() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := down.snapshot()
	if len(got) != 2 {
		t.Fatalf("delivered = %d, want 2", len(got))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = w.Close(ctx)
}

// TestCloseReturnsUndeliveredAndStopsRewrites 验证 Close 在下游持续失败
// 时把未送达事件按入队顺序交还，且此后 worker 不再把它们写出
// （不会恢复后“回写已放弃/已交还的数据”）。
func TestCloseReturnsUndeliveredAndStopsRewrites(t *testing.T) {
	down := &recDownstream{failUntil: time.Now().Add(time.Hour)}
	w := NewWriter(down, WithCapacity(5), WithBackoff(time.Millisecond, 5*time.Millisecond))
	for i := 0; i < 3; i++ {
		w.Submit([]byte{byte('1' + i)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	undelivered, err := w.Close(ctx)
	if !errors.Is(err, ErrUndelivered) || len(undelivered) != 3 {
		t.Fatalf("undelivered=%d err=%v", len(undelivered), err)
	}
	if string(undelivered[0]) != "1" || string(undelivered[2]) != "3" {
		t.Fatalf("undelivered order = %v, want 1,2,3", undelivered)
	}
	t.Logf("关闭时按序交还: %v（调用方获得所有权，sink 不再回写）", undelivered)

	// 等待超过一个退避周期，确认下游始终未收到这些事件。
	time.Sleep(40 * time.Millisecond)
	got, _ := down.snapshot()
	if len(got) != 0 {
		t.Fatalf("worker rewrote handed-off events: %v", got)
	}
	t.Log("交还后再等待 40ms：下游收到 0 条，确认不会回写")
}

// TestSubmitDoesNotMutateCallerBuffer 验证 Accepted 后调用方修改自己的
// 缓冲区不会污染已入队数据。
func TestSubmitDoesNotMutateCallerBuffer(t *testing.T) {
	down := &recDownstream{failUntil: time.Now().Add(time.Hour)}
	w := NewWriter(down, WithCapacity(5), WithBackoff(time.Millisecond, 5*time.Millisecond))
	buf := []byte("hello")
	if o := w.Submit(buf); o.Kind != OutcomeAccepted {
		t.Fatalf("submit: %+v", o)
	}
	for i := range buf {
		buf[i] = 'X'
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	undelivered, _ := w.Close(ctx)
	if string(undelivered[0]) != "hello" {
		t.Fatalf("queued payload corrupted by caller reuse: %q", undelivered[0])
	}
}
