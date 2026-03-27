package memtable

import (
	"fmt"

	"github.com/axv2655/lsm-db/internal/wal"
)

type Memtable struct {
	wal         *wal.WAL
	list        *skipList
	sizeInBytes int
	maxSize     int
}

func Open(walPath string, maxSize int, p float32, maxLevel int) (*Memtable, error) {
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("error: %w", err)
	}
	m := &Memtable{
		wal:         w,
		list:        newSkipList(p, maxLevel),
		sizeInBytes: 0,
		maxSize:     maxSize,
	}

	entries, err := w.ReadFile()
	if err != nil {
		return nil, fmt.Errorf("error reading wal: %w", err)
	}
	for _, entry := range entries {
		switch entry.Op {
		case wal.OpPut:
			if err := m.list.insert(entry.Key, entry.Value); err != nil {
				return nil, fmt.Errorf("error replaying wal put: %w", err)
			}
			m.sizeInBytes += len(entry.Key) + len(entry.Value)
		case wal.OpDelete:
			if err := m.list.delete(entry.Key); err != nil {
				return nil, fmt.Errorf("error replaying wal delete: %w", err)
			}
			m.sizeInBytes += len(entry.Key)
		}
	}

	return m, nil
}

func (m *Memtable) Put(key []byte, value []byte) error {
	entry := wal.Entry{
		Op:    wal.OpPut,
		Key:   key,
		Value: value,
	}
	err := m.wal.Append(entry)
	if err != nil {
		return fmt.Errorf("error: %w", err)
	}

	err = m.list.insert(key, value)
	if err != nil {
		return fmt.Errorf("error: %w", err)
	}
	m.sizeInBytes += len(key) + len(value)
	return nil
}

func (m *Memtable) Delete(key []byte) error {
	entry := wal.Entry{
		Op:    wal.OpDelete,
		Key:   key,
		Value: []byte{},
	}
	if err := m.wal.Append(entry); err != nil {
		return fmt.Errorf("error: %w", err)
	}

	if err := m.list.delete(key); err != nil {
		return fmt.Errorf("error: %w", err)
	}
	m.sizeInBytes += len(key)

	return nil
}

func (m *Memtable) Get(key []byte) ([]byte, error) {
	node, err := m.list.get(key)
	if err != nil {
		return nil, fmt.Errorf("error: %w", err)
	}
	return node.value, nil
}

func (m *Memtable) Close() error {
	if err := m.wal.Close(); err != nil {
		return fmt.Errorf("could not close wal: %w", err)
	}
	return nil
}

func (m *Memtable) ClearWAL(copyPath string) error {
	return m.wal.Clear(copyPath)
}

func (m *Memtable) IsFull(newKV int) bool {
	return (m.sizeInBytes + newKV) >= m.maxSize
}

func (m *Memtable) GetAll() []KVEntry {
	return m.list.GetAll()
}
