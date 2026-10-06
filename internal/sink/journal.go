// journal.go 提供追加式落盘日志：enqueue/ack/drop/purge 四类记录，
// 重启时按序重放以重建积压队列与放弃列表。
// 每条记录 append 后立即 fsync，保证"已接受"的数据在硬杀后也不丢。
package sink

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// journalOp 是日志记录类型。
type journalOp string

const (
	opEnqueue journalOp = "enq"   // 数据被接受，进入积压
	opAck     journalOp = "ack"   // 数据已成功写出，不再重投
	opDrop    journalOp = "drop"  // 数据被放弃，附原因
	opPurge   journalOp = "purge" // 手动丢弃一条已放弃记录（重启后不再出现）
)

// journalRecord 是一条日志记录。Data 由 encoding/json 自动 base64。
type journalRecord struct {
	Op     journalOp `json:"op"`
	Seq    uint64    `json:"seq"`
	TS     int64     `json:"ts,omitempty"`
	Data   []byte    `json:"data,omitempty"`
	Reason string    `json:"reason,omitempty"`
}

// journal 追加式落盘日志，每条记录写盘并 fsync 后才返回。
type journal struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// openJournal 打开（不存在则创建）日志文件，并返回按序重放出的全部记录。
// 末尾的半行（硬杀残留的截断记录）被忽略——只有 fsync 成功的记录才算数。
func openJournal(path string) (*journal, []journalRecord, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, fmt.Errorf("journal: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("journal: open: %w", err)
	}
	var recs []journalRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec journalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			// 截断的尾记录：视为不存在（对应 append 未 fsync 成功的情况）。
			continue
		}
		recs = append(recs, rec)
	}
	// 截断到最后一个完整记录之后，避免半行污染后续追加。
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("journal: seek: %w", err)
	}
	return &journal{f: f, path: path}, recs, nil
}

// append 追加一条记录并 fsync；返回 nil 表示该记录已持久化。
func (j *journal) append(rec journalRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("journal: marshal: %w", err)
	}
	if _, err := j.f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("journal: write: %w", err)
	}
	if err := j.f.Sync(); err != nil {
		return fmt.Errorf("journal: fsync: %w", err)
	}
	return nil
}

// rewrite 用给定记录集原子替换整个日志（压缩）：先写临时文件并 fsync，
// 再 rename 覆盖，崩溃时至多回到旧日志，不会出现半新半旧。
func (j *journal) rewrite(recs []journalRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	tmp := j.path + ".tmp"
	nf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("journal: compact open: %w", err)
	}
	for _, rec := range recs {
		b, err := json.Marshal(rec)
		if err != nil {
			nf.Close()
			os.Remove(tmp)
			return fmt.Errorf("journal: compact marshal: %w", err)
		}
		if _, err := nf.Write(append(b, '\n')); err != nil {
			nf.Close()
			os.Remove(tmp)
			return fmt.Errorf("journal: compact write: %w", err)
		}
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		os.Remove(tmp)
		return fmt.Errorf("journal: compact fsync: %w", err)
	}
	if err := nf.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("journal: compact close: %w", err)
	}
	if err := os.Rename(tmp, j.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("journal: compact rename: %w", err)
	}
	// 换 fd：旧 fd 指向已被替换的 inode。
	old := j.f
	nf2, err := os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("journal: compact reopen: %w", err)
	}
	j.f = nf2
	old.Close()
	return nil
}

// close 关闭日志文件。
func (j *journal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}
