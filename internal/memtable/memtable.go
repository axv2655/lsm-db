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
	return &Memtable{
		w,
		newSkipList(p, maxLevel),
		0,
		maxSize,
	}, nil
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
	err := m.list.delete(key)
	if err != nil {
		return fmt.Errorf("error: %w", err)
	}
	m.sizeInBytes += len(key)

	entry := wal.Entry{
		Op:    wal.OpDelete,
		Key:   key,
		Value: []byte{},
	}
	err = m.wal.Append(entry)
	if err != nil {
		return fmt.Errorf("error: %w", err)
	}

	return nil
}

func (m *Memtable) Get(key []byte) ([]byte, error) {
	node, err := m.list.get(key)
	if err != nil {
		return nil, fmt.Errorf("error: %w", err)
	}
	return node.value, nil
}

func (m *Memtable) IsFull() bool {
	return m.sizeInBytes >= m.maxSize
}
