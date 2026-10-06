// Package sink 提供带背压与放弃说明的缓冲结果写出器。
package sink

import (
	"errors"
	"sync"
)

// ErrBusy 表示缓冲区已满，调用方暂时无法写入，可稍后重试。
var ErrBusy = errors.New("sink: buffer full, try again later")

// ErrClosed 表示写出器已关闭，不再接受新数据。
var ErrClosed = errors.New("sink: writer closed")

// Destination 是下游写出目标。
type Destination interface {
	Write(item []byte) error
}

// Dropped 记录一条被放弃的数据及其原因。
// Seq 仅由 PersistentWriter 填充（日志序号，用于手动处置与重放对账）；
// 内存版 Writer 不使用该字段，恒为 0。
type Dropped struct {
	Seq    uint64
	Item   []byte
	Reason string
}

// Writer 缓冲写出器：Write 只产生"已接受"或"暂时繁忙"两种结果；
// 已接受的数据要么最终写出，要么进入 Dropped 列表并附原因，绝不静默丢失。
type Writer struct {
	buf      chan []byte
	dest     Destination
	maxRetry int

	mu      sync.Mutex
	dropped []Dropped
	closed  bool
	wg      sync.WaitGroup
}

// NewWriter 创建写出器并启动后台冲刷协程。capacity 为缓冲区容量，
// maxRetry 为单条数据对下游失败的最大重试次数，超过则放弃并记录原因。
func NewWriter(dest Destination, capacity, maxRetry int) *Writer {
	w := &Writer{
		buf:      make(chan []byte, capacity),
		dest:     dest,
		maxRetry: maxRetry,
	}
	w.wg.Add(1)
	go w.loop()
	return w
}

// Write 尝试把数据放入缓冲区。缓冲区满时返回 ErrBusy（背压信号，可稍后重试）；
// 已关闭时返回 ErrClosed。返回 nil 表示已接受，保证之后要么写出、
// 要么出现在 Dropped 中并附原因，绝不静默丢失、绝不重复写出。
func (w *Writer) Write(item []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	// 在持锁状态下做非阻塞发送：Close 也需要同一把锁才能 close(buf)，
	// 因此不存在"向已关闭通道发送"的竞态。
	select {
	case w.buf <- item:
		return nil
	default:
		return ErrBusy
	}
}

// Dropped 返回已放弃的数据及原因快照。
func (w *Writer) Dropped() []Dropped {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Dropped, len(w.dropped))
	copy(out, w.dropped)
	return out
}

// Close 停止接受新数据，冲刷完缓冲区中已接受的数据后退出后台协程。
func (w *Writer) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	close(w.buf)
	w.wg.Wait()
}

// loop 顺序冲刷缓冲区：每条数据最多尝试 maxRetry+1 次，
// 仍失败则放弃并记录原因；已放弃的数据不会回写。
func (w *Writer) loop() {
	defer w.wg.Done()
	for item := range w.buf {
		var err error
		for attempt := 0; attempt <= w.maxRetry; attempt++ {
			if err = w.dest.Write(item); err == nil {
				break
			}
		}
		if err != nil {
			w.mu.Lock()
			w.dropped = append(w.dropped, Dropped{
				Item:   item,
				Reason: "downstream write failed after retries: " + err.Error(),
			})
			w.mu.Unlock()
		}
	}
}
