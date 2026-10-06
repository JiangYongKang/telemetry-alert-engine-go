// persistent.go 提供跨重启续投的持久写出器：已收下未写出的数据不丢、
// 已确认写出的不重发、已放弃的不翻案，并暴露积压观测与手动处置入口。
package sink

import (
	"sort"
	"sync"
	"time"
)

// 重投标记：accept 记录的 From 字段指向被重投的已放弃条目，
// 重放时把旧条目标记为已重投（终态），保证手动重投的去重跨重启生效。
const stateRequeued = "requeued"

// Stats 是积压状况快照，重启前后口径一致（全部派生自持久日志）。
type Stats struct {
	Pending           int            // 等待写出的条数
	OldestPendingAge  time.Duration  // 最老一条已等待时长（无积压时为 0）
	DownstreamBlocked bool           // 最近一次写出尝试失败则为 true（被下游拖着）
	Dropped           int            // 已放弃总条数
	DroppedByReason   map[string]int // 已放弃条数按原因分类
}

// PersistentWriter 持久写出器。对外反馈语义与 Writer 一致：
// Write 返回 nil（收下）/ ErrBusy（暂时不收）/ ErrClosed；
// 放弃通过 Dropped()/Stats() 暴露并附原因。容量上限对恢复补投期同样生效：
// 待写出条数（含恢复补投）达到 capacity 时新写入一律 ErrBusy。
type PersistentWriter struct {
	mu       sync.Mutex
	j        *journal
	entries  map[uint64]*journalEntry // 与 journal 内容一致的内存状态表
	pending  int                      // 当前待写出条数（含恢复补投）
	dropped  []Dropped                // 已放弃列表（含原因，重放重建）
	queue    chan uint64              // 待处理序号
	capacity int
	maxRetry int
	dest     Destination
	now      func() time.Time

	blocked bool // 最近一次写出尝试是否失败
	closed  bool
	stopped bool // halt 后为 true：不再接受写入、不再落任何日志（硬杀语义）
	halted  chan struct{}
	wg      sync.WaitGroup
}

// NewPersistentWriter 打开（或创建）path 处的日志并恢复状态，
// 把上次未完成的待写出数据按原序号顺序补投，然后正常服务新写入。
// 已确认写出（ack）与已放弃（drop）的数据不会被重投。
// 若恢复出的积压超过本次给定的 capacity，积压仍会全部保留并补投，
// 但新写入在积压降到 capacity 以下之前一律得到 ErrBusy。
func NewPersistentWriter(dest Destination, path string, capacity, maxRetry int) (*PersistentWriter, error) {
	j, entries, err := openJournal(path)
	if err != nil {
		return nil, err
	}
	w := &PersistentWriter{
		j:        j,
		entries:  entries,
		capacity: capacity,
		maxRetry: maxRetry,
		dest:     dest,
		now:      time.Now,
		halted:   make(chan struct{}),
	}
	// 重建已放弃列表与待写出计数。
	for _, e := range entries {
		switch e.state {
		case opAccept:
			w.pending++
		case opDrop:
			w.dropped = append(w.dropped, Dropped{Item: e.item, Reason: e.reason})
		}
	}
	// 队列容量取 capacity 与恢复积压的较大者，保证恢复数据全部入队。
	qcap := capacity
	if w.pending > qcap {
		qcap = w.pending
	}
	w.queue = make(chan uint64, qcap)
	// 按原序号顺序补投。
	for _, seq := range replayPending(entries) {
		w.queue <- seq
	}
	w.wg.Add(1)
	go w.loop()
	return w, nil
}

// Write 收下一条数据：先落日志（fsync）再入队，保证硬杀后不丢。
// 待写出数达到容量上限返回 ErrBusy；已关闭返回 ErrClosed。
func (w *PersistentWriter) Write(item []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.stopped {
		return ErrClosed
	}
	if w.pending >= w.capacity {
		return ErrBusy
	}
	return w.acceptLocked(item, 0)
}

// acceptLocked 落一条 accept 日志并入队。from 非零表示这是对已放弃条目的手动重投。
// 调用方须持有 w.mu 且已确认容量。
func (w *PersistentWriter) acceptLocked(item []byte, from uint64) error {
	seq, err := w.j.append(record{Op: opAccept, Item: item, TS: w.now().UnixNano(), From: from})
	if err != nil {
		return err
	}
	w.entries[seq] = &journalEntry{item: item, state: opAccept, ts: w.now().UnixNano()}
	if from != 0 {
		if old, ok := w.entries[from]; ok && old.state == opDrop {
			old.state = stateRequeued
		}
	}
	w.pending++
	w.queue <- seq // 容量不变式保证不阻塞：在队条目数 <= pending <= 队列容量
	return nil
}

// Stats 返回积压状况快照。Pending / 最老等待时长 / 放弃计数与原因
// 全部派生自持久日志重放出的状态表，重启前后口径一致。
func (w *PersistentWriter) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := Stats{
		Pending:           w.pending,
		DownstreamBlocked: w.blocked,
		Dropped:           len(w.dropped),
		DroppedByReason:   make(map[string]int),
	}
	for _, d := range w.dropped {
		s.DroppedByReason[d.Reason]++
	}
	var oldest int64
	for _, e := range w.entries {
		if e.state == opAccept && (oldest == 0 || e.ts < oldest) {
			oldest = e.ts
		}
	}
	if oldest != 0 {
		if age := w.now().Sub(time.Unix(0, oldest)); age > 0 {
			s.OldestPendingAge = age
		}
	}
	return s
}

// Dropped 返回已放弃数据及原因快照。
func (w *PersistentWriter) Dropped() []Dropped {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Dropped, len(w.dropped))
	copy(out, w.dropped)
	return out
}

// RequeueDropped 把当前处于已放弃状态的数据按原序号顺序重新入队，
// 返回成功重投的条数。容量不足时剩余条目保持已放弃状态；
// 每条已放弃数据只会被重投一次（重投标记随日志持久化，重启后仍生效），
// 重投产生的新条目走与自动流程完全相同的写出/重试/放弃路径。
func (w *PersistentWriter) RequeueDropped() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.stopped {
		return 0
	}
	var seqs []uint64
	for seq, e := range w.entries {
		if e.state == opDrop {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, k int) bool { return seqs[i] < seqs[k] })
	n := 0
	for _, seq := range seqs {
		if w.pending >= w.capacity {
			break // 容量不足：剩余条目保持已放弃，下次可再试
		}
		if err := w.acceptLocked(w.entries[seq].item, seq); err != nil {
			break
		}
		n++
	}
	return n
}

// DiscardPending 把当前所有待写出数据按序号顺序标记为放弃
// （原因 "manual discard"），返回处置条数。已在写出中的条目
// 最多再完成一次既定写出，之后不会被视为成功也不会被重发。
func (w *PersistentWriter) DiscardPending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	const reason = "manual discard"
	var seqs []uint64
	for seq, e := range w.entries {
		if e.state == opAccept {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, k int) bool { return seqs[i] < seqs[k] })
	for _, seq := range seqs {
		e := w.entries[seq]
		if _, err := w.j.append(record{Op: opDrop, Seq: seq, Reason: reason}); err != nil {
			break
		}
		e.state = opDrop
		e.reason = reason
		w.dropped = append(w.dropped, Dropped{Item: e.item, Reason: reason})
		w.pending--
	}
	return len(seqs)
}

// Close 停止接受新数据，冲刷完已收下数据（全部 ack 或 drop 落盘）后关闭日志。
func (w *PersistentWriter) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	close(w.queue)
	w.wg.Wait()
	w.j.close()
}

// halt 模拟硬杀：后台协程尽快退出，不冲刷、不再落任何日志、不关闭文件。
// 仅供测试构造"跑到一半被 kill"的现场；halt 后该实例不可再使用。
func (w *PersistentWriter) halt() {
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
	close(w.halted)
}

// loop 后台冲刷协程：按序处理队列，每条最多尝试 maxRetry+1 次，
// 成功落 ack、失败落 drop（附原因），状态迁移与日志追加在同一把锁内完成。
func (w *PersistentWriter) loop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.halted:
			return
		case seq, ok := <-w.queue:
			if !ok {
				return
			}
			w.process(seq)
		}
	}
}

// process 处理一条待写出数据。被手动丢弃或已非待写出状态的条目直接跳过。
func (w *PersistentWriter) process(seq uint64) {
	w.mu.Lock()
	e, ok := w.entries[seq]
	if !ok || e.state != opAccept || w.stopped {
		w.mu.Unlock()
		return
	}
	item := e.item
	w.mu.Unlock()

	var err error
	for attempt := 0; attempt <= w.maxRetry; attempt++ {
		if err = w.dest.Write(item); err == nil {
			break
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return // 硬杀：不再落任何日志，条目保持待写出，待下次启动补投
	}
	if e.state != opAccept {
		return // 处理期间被手动丢弃：不补记成功，也不会被重发
	}
	if err == nil {
		if _, jerr := w.j.append(record{Op: opAck, Seq: seq}); jerr == nil {
			e.state = opAck
			w.pending--
		}
		w.blocked = false
		return
	}
	reason := "downstream write failed after retries: " + err.Error()
	if _, jerr := w.j.append(record{Op: opDrop, Seq: seq, Reason: reason}); jerr == nil {
		e.state = opDrop
		e.reason = reason
		w.dropped = append(w.dropped, Dropped{Item: item, Reason: reason})
		w.pending--
	}
	w.blocked = true
}
