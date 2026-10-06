// journal.go 提供追加式持久日志：每条已接受数据的生命周期
// （accept -> ack | drop）都先落盘再生效，崩溃后按序重放恢复。
package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// 日志记录的操作类型。
const (
	opAccept = "accept" // 数据被收下，进入待写出集合
	opAck    = "ack"    // 数据已确认写出成功
	opDrop   = "drop"   // 数据被放弃（自动重试耗尽或手动丢弃），附原因
)

// record 是日志中的一行（JSON）。Item 由 encoding/json 自动 base64。
type record struct {
	Seq    uint64 `json:"seq"`
	Op     string `json:"op"`
	Item   []byte `json:"item,omitempty"`
	Reason string `json:"reason,omitempty"`
	TS     int64  `json:"ts,omitempty"`   // accept 时间（unixnano），用于跨重启计算等待时长
	From   uint64 `json:"from,omitempty"` // 手动重投：指向被重投的已放弃条目序号
}

// journalEntry 是重放后每条数据的内存状态。
type journalEntry struct {
	item   []byte
	state  string // opAccept / opAck / opDrop
	reason string // state == opDrop 时有效
	ts     int64  // accept 时间
}

// journal 是单文件追加日志。所有 append 先写盘并 fsync 再返回，
// 保证硬杀后已确认的状态不丢；重放时按记录顺序应用，得到确定性的最终状态。
type journal struct {
	mu     sync.Mutex
	f      *os.File
	nextSeq uint64
}

// openJournal 打开（不存在则创建）日志文件并重放全部记录，
// 返回重放出的数据状态表与日志句柄。
func openJournal(path string) (*journal, map[uint64]*journalEntry, error) {
	entries := make(map[uint64]*journalEntry)
	var maxSeq uint64

	// 先以只读方式重放已有内容（文件不存在则跳过）。
	if rf, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(rf)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		line := 0
		for sc.Scan() {
			line++
			var rec record
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				rf.Close()
				return nil, nil, fmt.Errorf("journal: corrupt record at line %d: %w", line, err)
			}
			applyRecord(entries, rec)
			if rec.Seq > maxSeq {
				maxSeq = rec.Seq
			}
		}
		if err := sc.Err(); err != nil {
			rf.Close()
			return nil, nil, fmt.Errorf("journal: read: %w", err)
		}
		rf.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("journal: open for replay: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("journal: open for append: %w", err)
	}
	return &journal{f: f, nextSeq: maxSeq + 1}, entries, nil
}

// applyRecord 把一条日志记录应用到状态表（重放与运行期共用同一套状态机）。
func applyRecord(entries map[uint64]*journalEntry, rec record) {
	switch rec.Op {
	case opAccept:
		entries[rec.Seq] = &journalEntry{item: rec.Item, state: opAccept, ts: rec.TS}
		// 手动重投：同一条记录原子地把旧条目置为终态 "requeued"，
		// 保证一条已放弃数据不会被重复重投（重启后仍生效）。
		if rec.From != 0 {
			if old, ok := entries[rec.From]; ok && old.state == opDrop {
				old.state = stateRequeued
			}
		}
	case opAck:
		if e, ok := entries[rec.Seq]; ok && e.state == opAccept {
			e.state = opAck
		}
	case opDrop:
		if e, ok := entries[rec.Seq]; ok && e.state == opAccept {
			e.state = opDrop
			e.reason = rec.Reason
		}
	}
}

// append 写入一条记录并 fsync，返回分配的序号（仅 opAccept 使用返回值）。
// 调用方需自行保证与内存状态的加锁顺序一致。
func (j *journal) append(rec record) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if rec.Op == opAccept {
		rec.Seq = j.nextSeq
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("journal: encode: %w", err)
	}
	if _, err := j.f.Write(append(data, '\n')); err != nil {
		return 0, fmt.Errorf("journal: append: %w", err)
	}
	if err := j.f.Sync(); err != nil {
		return 0, fmt.Errorf("journal: sync: %w", err)
	}
	if rec.Op == opAccept {
		j.nextSeq++
	}
	return rec.Seq, nil
}

// close 关闭日志文件。
func (j *journal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}

// replayPending 按序号升序返回所有仍处于待写出状态的条目（恢复补投顺序）。
func replayPending(entries map[uint64]*journalEntry) []uint64 {
	var seqs []uint64
	for seq, e := range entries {
		if e.state == opAccept {
			seqs = append(seqs, seq)
		}
	}
	// 插入排序即可，恢复时条目数等于积压量。
	for i := 1; i < len(seqs); i++ {
		for k := i; k > 0 && seqs[k] < seqs[k-1]; k-- {
			seqs[k], seqs[k-1] = seqs[k-1], seqs[k]
		}
	}
	return seqs
}
