package integration

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/telemetry-alert-engine-go/internal/alert"
	"github.com/highcumontoa/telemetry-alert-engine-go/internal/sink"
)

// parkAfterDest 前 limit 次写出成功，之后永久阻塞（模拟硬杀瞬间卡住的下游）。
type parkAfterDest struct {
	mu      sync.Mutex
	limit   int
	written []string
	parkCh  chan struct{}
}

func (d *parkAfterDest) Write(item []byte) error {
	d.mu.Lock()
	if len(d.written) >= d.limit {
		d.mu.Unlock()
		<-d.parkCh // 永不返回
		return nil
	}
	d.written = append(d.written, string(item))
	d.mu.Unlock()
	return nil
}

func (d *parkAfterDest) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.written...)
}

// 端到端：引擎判定产生事件 -> 持久写出器 -> 下游。
// 场景：事件流到一半进程被硬杀，平台重启后引擎与写出器重建。
// 判定：已确认事件不重发、未确认事件按原顺序续投、重启后新产生的事件
// 照常接受并写出，全程 写出+放弃=接受 守恒且无重复。
func TestEngineEventsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.journal")

	newEngine := func() (*alert.Engine, *time.Time) {
		now := &time.Time{}
		*now = time.Unix(1_700_000_000, 0)
		e := alert.NewEngine(func() time.Time { return *now })
		e.UpsertRule(alert.Rule{ID: "high-cpu", Metric: "cpu", Comparator: alert.GreaterThan, Threshold: 100, For: 0})
		return e, now
	}
	// 采样：交替高低值产生 Fired/Resolved 事件并写入写出器
	sample := func(e *alert.Engine, now *time.Time, w *sink.PersistentWriter, rounds, start int) []string {
		var accepted []string
		for r := 0; r < rounds; r++ {
			*now = now.Add(time.Second)
			v := 200.0
			if (start+r)%2 == 1 {
				v = 10
			}
			for _, ev := range e.Evaluate(map[string]float64{"cpu": v}) {
				item := fmt.Sprintf("%s rule=%s value=%.0f",
					map[alert.EventType]string{alert.EventFired: "FIRED", alert.EventResolved: "RESOLVED"}[ev.Type],
					ev.RuleID, ev.Value)
				if err := w.Write([]byte(item)); err != nil {
					t.Fatalf("Write 应被接受(容量充足): %v", err)
				}
				accepted = append(accepted, item)
			}
		}
		return accepted
	}

	// 第一代：写出 3 条后下游卡死，再采几轮让事件积压在日志里，然后硬杀
	engine1, now1 := newEngine()
	dest1 := &parkAfterDest{limit: 3, parkCh: make(chan struct{})}
	w1, err := sink.OpenPersistentWriter(dest1, path, 64, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Log("输入: 引擎采样产生事件流; 下游写出 3 条后卡死; 继续采样后硬杀")
	accepted1 := sample(engine1, now1, w1, 6, 0)
	waitForCond(t, "3 条确认写出", func() bool { return len(dest1.snapshot()) == 3 })
	t.Logf("状态变化: 硬杀前已接受=%d 已确认写出=%v 积压=%d",
		len(accepted1), dest1.snapshot(), w1.Stats().Pending)
	// 硬杀：不 Close，直接抛弃第一代（其冲刷协程卡在第 4 条上）

	// 第二代：平台重启，引擎与写出器重建，下游恢复
	engine2, now2 := newEngine()
	*now2 = *now1 // 时钟延续
	dest2 := &recordingDest{}
	w2, err := sink.OpenPersistentWriter(dest2, path, 64, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	restored := w2.Stats().Pending
	t.Logf("状态变化: 重启后恢复积压=%d", restored)
	if restored != len(accepted1)-3 {
		t.Fatalf("判定依据: 重启后应恢复 %d 条积压, 实际 %d", len(accepted1)-3, restored)
	}
	// 重启后继续采样，新事件照常接受
	accepted2 := sample(engine2, now2, w2, 4, 6)
	t.Logf("状态变化: 重启后新接受=%d", len(accepted2))

	want := append(append([]string(nil), accepted1...), accepted2...)
	waitForCond(t, "全部事件写出", func() bool { return len(dest2.snapshot())+3 == len(want) })
	got := append(dest1.snapshot(), dest2.snapshot()...)
	t.Logf("状态变化: 全量写出=%v", got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("判定依据: 写出序列应与接受序列完全一致(按序无重复), 期望 %v 实际 %v", want, got)
	}
	if d := w2.Dropped(); len(d) != 0 {
		t.Fatalf("判定依据: 下游恢复后不应有放弃, 实际 %+v", d)
	}
	t.Logf("判定依据: 接受=%d 写出=%d 放弃=0 守恒; 已确认不重发、未确认按序续投、新事件不丢失",
		len(want), len(got))
}

type recordingDest struct {
	mu      sync.Mutex
	written []string
}

func (d *recordingDest) Write(item []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.written = append(d.written, string(item))
	return nil
}

func (d *recordingDest) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.written...)
}

func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}
