// Package sink 提供带背压与确定性放弃语义的结果写出器。
//
// 写出器用有界队列衔接生产者与后台投递协程：
//
//   - Submit 永不阻塞。队列有空位 -> Accepted；队列满（下游持续失败
//     占满队列也是同一种表现）-> Busy，调用方应稍后重试同一份数据；
//     单条事件超过大小上限或写出器已关闭 -> Abandoned 并给出原因，
//     放弃是确定性的、终结性的，恢复后不会回写这些数据。
//   - 一旦 Accepted，事件受“不丢”保证约束：后台协程失败时整批原样
//     重试（不弹出队列），重试期间无法腾出空位，从而把下游持续失败
//     转化为对生产者的背压；Close 时若仍未送达，事件作为返回值显式
//     交还给调用方，绝不静默丢失。已被 Close 交还的事件不会再由
//     worker 回写（终止标记阻止提交）。
package sink

import (
	"context"
	"errors"
	"sync"
	"time"
)

// OutcomeKind 是一次提交的结果类别。
type OutcomeKind int

const (
	// OutcomeAccepted 事件已进入队列并受“不丢”保证约束。
	OutcomeAccepted OutcomeKind = iota
	// OutcomeBusy 缓冲区已满（或下游持续失败导致队列被占满），调用方可稍后重试。
	OutcomeBusy
	// OutcomeAbandoned 事件被确定性放弃，Reason 说明原因；放弃后不会再回写。
	OutcomeAbandoned
)

func (k OutcomeKind) String() string {
	switch k {
	case OutcomeAccepted:
		return "accepted"
	case OutcomeBusy:
		return "busy"
	case OutcomeAbandoned:
		return "abandoned"
	default:
		return "unknown"
	}
}

// AbandonReason 是确定性放弃的原因。
type AbandonReason string

const (
	// ReasonTooLarge：事件字节数超过配置的单条上限，重试也不可能成功。
	ReasonTooLarge AbandonReason = "payload_too_large"
	// ReasonClosed：写出器已关闭，不再接受任何新事件。
	ReasonClosed AbandonReason = "sink_closed"
)

// Outcome 是一次 Submit 的结果。
type Outcome struct {
	Kind   OutcomeKind
	Reason AbandonReason // 仅 Kind == OutcomeAbandoned 时有意义
}

// Downstream 是事件最终送达的下游接口。Write 返回 nil 表示整批已被
// 下游接受；返回非 nil 时整批均未被确认，写出器会原样重试，因此
// 下游实现需保证 Write 的原子性（要么全收，要么全不收），并应尊重
// ctx 取消信号，使 Close 的截止时间能够及时中断在途重试。
type Downstream interface {
	Write(ctx context.Context, events [][]byte) error
}

// DownstreamFunc 便于用函数实现 Downstream。
type DownstreamFunc func(ctx context.Context, events [][]byte) error

func (f DownstreamFunc) Write(ctx context.Context, events [][]byte) error {
	return f(ctx, events)
}

// Stats 是写出器的诊断计数。
type Stats struct {
	Accepted    int64
	Busy        int64
	Abandoned   int64
	Delivered   int64
	Undelivered int64 // Close 时未能送达、交还给调用方的事件数
}

const (
	defaultCapacity    = 64
	defaultMaxEventLen = 1 << 20
	defaultBaseBackoff = 10 * time.Millisecond
	defaultMaxBackoff  = time.Second
)

// Writer 是有界异步写出器。
type Writer struct {
	down        Downstream
	capacity    int
	maxEvent    int
	baseBackoff time.Duration
	maxBackoff  time.Duration

	mu         sync.Mutex
	queue      [][]byte
	closed     bool // 已开始关闭，拒绝新提交
	terminated bool // 队列所有权已移交 Close，worker 不得再提交
	closeCtx   context.Context
	stats      Stats
	wake       chan struct{} // 有新事件
	shutdown   chan struct{} // 关闭信号，令退避中的 worker 立即重试
	done       chan struct{} // worker 已退出
}

// Option 配置 Writer。
type Option func(*config)

type config struct {
	capacity    int
	maxEventLen int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// WithCapacity 设置队列容量。
func WithCapacity(n int) Option {
	return func(c *config) { c.capacity = n }
}

// WithMaxEventBytes 设置单条事件字节数上限。
func WithMaxEventBytes(n int) Option {
	return func(c *config) { c.maxEventLen = n }
}

// WithBackoff 设置失败重试的初始与最大退避时长。
func WithBackoff(base, max time.Duration) Option {
	return func(c *config) { c.baseBackoff, c.maxBackoff = base, max }
}

// NewWriter 创建写出器并启动后台投递协程。
func NewWriter(down Downstream, opts ...Option) *Writer {
	cfg := config{
		capacity:    defaultCapacity,
		maxEventLen: defaultMaxEventLen,
		baseBackoff: defaultBaseBackoff,
		maxBackoff:  defaultMaxBackoff,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.capacity <= 0 {
		panic("sink: capacity must be positive")
	}
	if cfg.maxEventLen <= 0 {
		panic("sink: maxEventLen must be positive")
	}

	w := &Writer{
		down:        down,
		capacity:    cfg.capacity,
		maxEvent:    cfg.maxEventLen,
		baseBackoff: cfg.baseBackoff,
		maxBackoff:  cfg.maxBackoff,
		wake:        make(chan struct{}, 1),
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
	}
	go w.run()
	return w
}

// Submit 尝试入队一条事件。永不阻塞，结果三选一：
//
//	Accepted  —— 已受不丢保证约束；
//	Busy      —— 缓冲耗尽（含下游持续失败），同一 payload 稍后重试；
//	Abandoned —— 超限或已关闭，终结性放弃，原因见 Reason。
func (w *Writer) Submit(event []byte) Outcome {
	if len(event) > w.maxEvent {
		w.mu.Lock()
		w.stats.Abandoned++
		w.mu.Unlock()
		return Outcome{Kind: OutcomeAbandoned, Reason: ReasonTooLarge}
	}

	w.mu.Lock()
	switch {
	case w.closed:
		w.stats.Abandoned++
		w.mu.Unlock()
		return Outcome{Kind: OutcomeAbandoned, Reason: ReasonClosed}
	case len(w.queue) >= w.capacity:
		w.stats.Busy++
		w.mu.Unlock()
		return Outcome{Kind: OutcomeBusy}
	}
	// 拷贝一份，避免调用方在事件 Accepted 后复用底层切片造成数据竞态。
	cp := append([]byte(nil), event...)
	w.queue = append(w.queue, cp)
	w.stats.Accepted++
	w.mu.Unlock()

	w.signal()
	return Outcome{Kind: OutcomeAccepted}
}

// Stats 返回当前计数快照。
func (w *Writer) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// QueueLen 返回当前排队中的事件数（主要用于测试与诊断）。
func (w *Writer) QueueLen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.queue)
}

// ErrUndelivered 表示 Close 时仍有事件未能送达。未送达事件本身通过
// Close 的第一个返回值交还，本错误仅作为显式标记，便于调用方记录。
var ErrUndelivered = errors.New("sink: closed with undelivered events")

// ErrClosed 表示对已关闭的 Writer 重复调用 Close。
var ErrClosed = errors.New("sink: writer already closed")

// Close 停止接收新事件，尝试在 ctx 截止前排空队列。
//
// 排空期间 worker 使用传入 ctx 调用下游；下游尊重取消信号时，超时
// 后在途写入会被中断，worker 停止重试，剩余事件（顺序与其 Accepted
// 先后一致）通过返回值交还，并计数 Undelivered，err 包装
// ErrUndelivered。这些事件的所有权自此转移给调用方，worker 不会再
// 将它们写向任何下游。全部送达时两个返回值均为零值。
func (w *Writer) Close(ctx context.Context) (undelivered [][]byte, err error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	w.closed = true
	w.closeCtx = ctx
	w.mu.Unlock()

	close(w.shutdown)
	w.signal()

	select {
	case <-w.done:
	case <-ctx.Done():
	}

	w.mu.Lock()
	w.terminated = true // 先夺取队列所有权，阻止任何迟到的 commit
	if len(w.queue) > 0 {
		undelivered = append(undelivered, w.queue...)
		w.queue = nil
		w.stats.Undelivered += int64(len(undelivered))
	}
	w.mu.Unlock()

	if len(undelivered) > 0 {
		return undelivered, errors.Join(ErrUndelivered, ctx.Err())
	}
	return nil, nil
}

// signal 非阻塞地唤醒 worker。
func (w *Writer) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run 是后台投递协程。
func (w *Writer) run() {
	defer close(w.done)

	backoff := w.baseBackoff
	for {
		batch := w.takeBatch()
		if len(batch) == 0 {
			if w.isClosed() {
				return
			}
			select {
			case <-w.wake:
				continue
			case <-w.shutdown:
				continue
			}
		}

		ctx, closing := w.deliveryContext()
		if err := w.down.Write(ctx, batch); err != nil {
			// 关闭排空且 ctx 已到期：停止重试，剩余队列由 Close 交还。
			if closing && ctx.Err() != nil {
				return
			}
			// 整批未确认，仍在队首（takeBatch 未弹出），退避后原样重试。
			// shutdown 时立即唤醒，改用 closeCtx 尽快走完排空。
			select {
			case <-time.After(backoff):
			case <-w.shutdown:
			}
			if backoff *= 2; backoff > w.maxBackoff {
				backoff = w.maxBackoff
			}
			continue
		}
		if !w.commitBatch(len(batch)) {
			// 队列已被 Close 接管：本批若实际到达下游也不得再动队列。
			return
		}
		backoff = w.baseBackoff
	}
}

// takeBatch 复制当前队首整批快照但不弹出。失败重试与关闭交还都依赖
// “只有下游确认后才弹出”这一保证；复制可避免与入队切片产生别名。
func (w *Writer) takeBatch() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return nil
	}
	batch := make([][]byte, len(w.queue))
	copy(batch, w.queue)
	return batch
}

// commitBatch 在下游成功确认后弹出队首 n 条；返回 false 表示队列已被
// Close 接管，调用方必须立即停止。
func (w *Writer) commitBatch(n int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminated {
		return false
	}
	if n > len(w.queue) {
		n = len(w.queue)
	}
	w.queue = w.queue[n:]
	w.stats.Delivered += int64(n)
	return true
}

func (w *Writer) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed && len(w.queue) == 0
}

func (w *Writer) deliveryContext() (context.Context, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closeCtx != nil {
		return w.closeCtx, true
	}
	return context.Background(), false
}
