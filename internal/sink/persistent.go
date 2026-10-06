// persistent.go 在缓冲写出语义之上叠加落盘日志：
//   - 已接受（Write 返回 nil）的数据先落盘再入队，硬杀后不丢；
//   - 重启时按 seq 重放，未确认的数据按原先后顺序续投；
//   - 已确认写成功（ack 落盘）的不重发，已放弃（drop 落盘）的不翻出；
//   - 恢复补投期间容量上限照常生效，新写入仍只有 接受/繁忙/放弃 三种结果。
package sink

import (
	"fmt"
	"sync"
	"time"
)

// compactThreshold 失效记录（ack/drop/purge）达到该数量时触发日志压缩。
// 定义为变量以便测试调小阈值。
var compactThreshold = 4096

// BacklogStats 是积压状况快照，重启前后口径一致（由日志重放重建）。
type BacklogStats struct {
	Pending     int            // 等待写出的条数
	OldestAge   time.Duration  // 最老一条已等待的时长
	Blocked     bool           // 当前是否被下游拖着（最近一次写出在失败重试中）
	Dropped     int            // 已放弃条数
	DropReasons map[string]int // 放弃原因 -> 条数
	Capacity    int            // 积压容量上限
}

// PersistentWriter 与 Writer 一样只暴露 已接受/暂时繁忙/已放弃 三种结果，
// 额外保证：接受即落盘，重启后续投；确认写成功的不重发；放弃的不翻出。
type PersistentWriter struct {
	dest     Destination
	maxRetry int
	capacity int
	j        *journal

	mu       sync.Mutex
	pending  []pendingEntry // 按 seq 升序
	dropped  []Dropped
	nextSeq  uint64
	obsolete int // 自上次压缩以来的 ack/drop/purge 记录数
	blocked  bool
	closed   bool
	wake     chan struct{}
	wg       sync.WaitGroup
	now      func() time.Time
}

type pendingEntry struct {
	seq  uint64
	ts   time.Time
	data []byte
}

// OpenPersistentWriter 打开（或创建）持久写出器并启动后台冲刷协程。
// path 指向日志文件；若已存在则重放恢复积压与放弃列表，积压按原 seq 顺序续投。
func OpenPersistentWriter(dest Destination, path string, capacity, maxRetry int) (*PersistentWriter, error) {
	j, recs, err := openJournal(path)
	if err != nil {
		return nil, err
	}
	w := &PersistentWriter{
		dest:     dest,
		maxRetry: maxRetry,
		capacity: capacity,
		j:        j,
		nextSeq:  1,
		wake:     make(chan struct{}, 1),
		now:      time.Now,
	}
	// 重放：enq 入积压，ack 出积压，drop 转入放弃列表，purge 从放弃列表抹除。
	deleted := make(map[uint64]bool)
	for _, rec := range recs {
		switch rec.Op {
		case opEnqueue:
			w.pending = append(w.pending, pendingEntry{seq: rec.Seq, ts: time.Unix(0, rec.TS), data: rec.Data})
			if rec.Seq >= w.nextSeq {
				w.nextSeq = rec.Seq + 1
			}
		case opAck:
			deleted[rec.Seq] = true
		case opDrop:
			deleted[rec.Seq] = true
			w.dropped = append(w.dropped, Dropped{Seq: rec.Seq, Item: rec.Data, Reason: rec.Reason})
		case opPurge:
			for i, d := range w.dropped {
				if d.Seq == rec.Seq {
					w.dropped = append(w.dropped[:i], w.dropped[i+1:]...)
					break
				}
			}
		}
	}
	// 压缩 pending：去掉已 ack/drop 的，保持 seq 升序。
	live := w.pending[:0]
	for _, e := range w.pending {
		if !deleted[e.seq] {
			live = append(live, e)
		}
	}
	w.pending = live
	w.wg.Add(1)
	go w.loop()
	return w, nil
}

// Write 语义同 Writer.Write：nil=已接受（已落盘），ErrBusy=积压已满，ErrClosed=已关闭。
// 恢复补投期间同样生效：重放恢复的积压占用容量，满了新写入返回 ErrBusy。
func (w *PersistentWriter) Write(item []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if len(w.pending) >= w.capacity {
		return ErrBusy
	}
	rec := journalRecord{Op: opEnqueue, Seq: w.nextSeq, TS: w.now().UnixNano(), Data: item}
	if err := w.j.append(rec); err != nil {
		return fmt.Errorf("sink: persist enqueue: %w", err)
	}
	w.pending = append(w.pending, pendingEntry{seq: rec.Seq, ts: time.Unix(0, rec.TS), data: item})
	w.nextSeq++
	w.signal()
	return nil
}

// Dropped 返回已放弃的数据及原因快照。
func (w *PersistentWriter) Dropped() []Dropped {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Dropped, len(w.dropped))
	copy(out, w.dropped)
	return out
}

// Stats 返回积压状况快照。数据全部来自日志重放所重建的状态，
// 因此重启前后口径连续，不会归零。
func (w *PersistentWriter) Stats() BacklogStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := BacklogStats{
		Pending:     len(w.pending),
		Blocked:     w.blocked,
		Dropped:     len(w.dropped),
		DropReasons: make(map[string]int),
		Capacity:    w.capacity,
	}
	if len(w.pending) > 0 {
		if age := w.now().Sub(w.pending[0].ts); age > 0 {
			s.OldestAge = age
		}
	}
	for _, d := range w.dropped {
		s.DropReasons[d.Reason]++
	}
	return s
}

// Requeue 把最老的至多 n 条已放弃数据重新排入积压队尾（新 seq、落盘 enq），
// 返回实际重投条数。重投记录与自动流程走同一条冲刷路径，顺序与去重一致。
func (w *PersistentWriter) Requeue(n int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if n > len(w.dropped) {
		n = len(w.dropped)
	}
	done := 0
	for i := 0; i < n; i++ {
		d := w.dropped[0]
		// 先落新 enq 再 purge 旧 drop：崩溃窗口内数据至多同时出现在
		// 积压与放弃列表（可见、可再处置），绝不凭空消失。
		rec := journalRecord{Op: opEnqueue, Seq: w.nextSeq, TS: w.now().UnixNano(), Data: d.Item}
		if err := w.j.append(rec); err != nil {
			break // 落盘失败则停止，已重投的部分仍然有效
		}
		if err := w.j.append(journalRecord{Op: opPurge, Seq: d.Seq}); err != nil {
			break
		}
		w.pending = append(w.pending, pendingEntry{seq: rec.Seq, ts: time.Unix(0, rec.TS), data: d.Item})
		w.nextSeq++
		w.dropped = w.dropped[1:]
		w.obsolete++
		done++
	}
	w.maybeCompactLocked()
	w.signal()
	return done
}

// Discard 永久丢弃最老的至多 n 条已放弃数据（落盘 purge，重启后不再出现），
// 返回实际丢弃条数。
func (w *PersistentWriter) Discard(n int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if n > len(w.dropped) {
		n = len(w.dropped)
	}
	done := 0
	for i := 0; i < n; i++ {
		d := w.dropped[0]
		if err := w.j.append(journalRecord{Op: opPurge, Seq: d.Seq}); err != nil {
			break
		}
		w.dropped = w.dropped[1:]
		w.obsolete++
		done++
	}
	w.maybeCompactLocked()
	return done
}

// Close 停止接受新数据，冲刷完积压后退出后台协程并关闭日志。
// 未冲刷完的积压已落盘，下次 Open 时续投。
func (w *PersistentWriter) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	w.signal()
	w.wg.Wait()
	w.j.close()
}

func (w *PersistentWriter) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// loop 顺序冲刷积压：取队首，重试 maxRetry 次仍失败则落盘 drop 并记录原因；
// 成功则落盘 ack。ack/drop 落盘后才从积压中移除，保证崩溃可恢复。
func (w *PersistentWriter) loop() {
	defer w.wg.Done()
	for {
		w.mu.Lock()
		if len(w.pending) == 0 {
			if w.closed {
				w.mu.Unlock()
				return
			}
			w.mu.Unlock()
			<-w.wake
			continue
		}
		ent := w.pending[0]
		w.mu.Unlock()

		var err error
		for attempt := 0; attempt <= w.maxRetry; attempt++ {
			if err = w.dest.Write(ent.data); err == nil {
				break
			}
			w.mu.Lock()
			w.blocked = true // 正在失败重试：积压被下游拖着
			w.mu.Unlock()
		}

		w.mu.Lock()
		if err == nil {
			w.blocked = false
			if jerr := w.j.append(journalRecord{Op: opAck, Seq: ent.seq}); jerr == nil {
				w.pending = w.pending[1:]
				w.obsolete++
			}
			// ack 落盘失败：保留在积压中，下轮重试（下游可能收到重复，
			// 但"确认成功"以 ack 落盘为准，未确认即未承诺不重发）。
		} else {
			reason := "downstream write failed after retries: " + err.Error()
			if jerr := w.j.append(journalRecord{Op: opDrop, Seq: ent.seq, Data: ent.data, Reason: reason}); jerr == nil {
				w.pending = w.pending[1:]
				w.dropped = append(w.dropped, Dropped{Seq: ent.seq, Item: ent.data, Reason: reason})
				w.obsolete++
			}
		}
		w.maybeCompactLocked()
		w.mu.Unlock()
	}
}

// maybeCompactLocked 失效记录超阈值时用当前存活记录重写日志（调用方须持锁）。
func (w *PersistentWriter) maybeCompactLocked() {
	if w.obsolete < compactThreshold {
		return
	}
	recs := make([]journalRecord, 0, len(w.pending)+len(w.dropped))
	for _, e := range w.pending {
		recs = append(recs, journalRecord{Op: opEnqueue, Seq: e.seq, TS: e.ts.UnixNano(), Data: e.data})
	}
	for _, d := range w.dropped {
		recs = append(recs, journalRecord{Op: opDrop, Seq: d.Seq, Data: d.Item, Reason: d.Reason})
	}
	if err := w.j.rewrite(recs); err == nil {
		w.obsolete = 0
	}
}
